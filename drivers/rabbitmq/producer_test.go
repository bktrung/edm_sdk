package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// fakeConfirmation is a message's confirmation the test resolves by hand.
type fakeConfirmation struct {
	done  chan struct{}
	acked bool
}

func unresolved() *fakeConfirmation { return &fakeConfirmation{done: make(chan struct{})} }

func resolved(acked bool) *fakeConfirmation {
	confirmation := &fakeConfirmation{done: make(chan struct{}), acked: acked}
	close(confirmation.done)
	return confirmation
}

func (f *fakeConfirmation) Done() <-chan struct{} { return f.done }
func (f *fakeConfirmation) Acked() bool           { return f.acked }

func (f *fakeConfirmation) WaitContext(ctx context.Context) (bool, error) {
	select {
	case <-f.done:
		return f.acked, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// decideFixture is a producer and one channel whose watcher reads streams the
// test owns, with the given messages registered as a segment would register
// them.
func decideFixture(t *testing.T, messages ...outboundMessage) (*producer, *publishChannel, *watcherFixture) {
	t.Helper()
	fixture := newWatcherFixture(t)
	p := &producer{conn: fixture.conn}
	channel := &publishChannel{watcher: fixture.watcher}
	mustRegister(t, fixture.watcher, messageIDs(messages)...)
	return p, channel, fixture
}

func segmentMessage(index int, id, exchange string, confirm publishConfirmation) outboundMessage {
	return outboundMessage{index: index, exchange: exchange, publishing: amqp.Publishing{MessageId: id}, confirm: confirm}
}

// TestDecideHoldsACancelledCallsIDsUntilResolved pins the release a cancelled
// call owes its channel. Its message is still in flight, so the broker can yet
// return it; if the id were free at once, a call publishing the same id - a
// retry copy keeps its original's - could register it and be failed by that
// return. The id stays held until the confirmation resolves, and is free as
// soon as it has.
func TestDecideHoldsACancelledCallsIDsUntilResolved(t *testing.T) {
	inFlight := unresolved()
	message := segmentMessage(0, "held", "", inFlight)
	p, channel, fixture := decideFixture(t, message)
	defer fixture.shutDown(nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	failed := make(map[int]error)
	outcome := p.decide(ctx, channel, []outboundMessage{message}, nil, nil, failed)
	if !errors.Is(outcome.err, context.Canceled) || !errors.Is(failed[0], context.Canceled) {
		t.Fatalf("decide after the context ended = %v, failed %v, want context.Canceled", outcome.err, failed)
	}
	busy, live := channel.watcher.register([]string{"held"})
	if !live || busy == nil {
		t.Fatalf("register of the cancelled call's in-flight id = busy %v, live %t, want it still held", busy, live)
	}
	close(inFlight.done)
	select {
	case <-busy:
	case <-time.After(5 * time.Second): //nolint:forbidigo // bounded wait on a goroutine handoff
		t.Fatal("the id stayed held after its confirmation resolved")
	}
	mustRegister(t, channel.watcher, "held")
}

// TestDecideRepublishesWhatAnotherCallsCloseTookDown covers the calls sharing a
// channel with a message the broker refused. The close names the refused
// message, so a segment none of whose messages it names gets its undecided
// messages, and the ones it never wrote, back to publish again rather than
// failing because of someone else's message.
func TestDecideRepublishesWhatAnotherCallsCloseTookDown(t *testing.T) {
	acked := segmentMessage(0, "acked", "", resolved(true))
	lost := segmentMessage(1, "lost", "", resolved(false))
	unwritten := segmentMessage(2, "unwritten", "", nil)
	p, channel, fixture := decideFixture(t, acked, lost, unwritten)
	fixture.shutDown(&amqp.Error{Code: 404, Server: true, Reason: "NOT_FOUND - no exchange 'elsewhere' in vhost '/'"})

	failed := make(map[int]error)
	outcome := p.decide(context.Background(), channel, []outboundMessage{acked, lost}, []outboundMessage{unwritten}, classify("publish", driver.KindTransient, amqp.ErrClosed), failed)
	if outcome.err != nil || len(failed) != 0 {
		t.Fatalf("decide = %v, failed %v, want nothing failed", outcome.err, failed)
	}
	if len(outcome.republish) != 2 || outcome.republish[0].index != 1 || outcome.republish[1].index != 2 {
		t.Fatalf("republish = %+v, want the lost and the unwritten message, in order", outcome.republish)
	}
}

// TestDecideFailsWhatACloseBlamesOnIt covers the segment whose own message the
// close names: that message fails with the close, which carries the broker's
// reason and kind, and its undecided neighbours fail transient, as a lost
// channel's messages always have. Nothing is published again, because doing so
// would only close another channel.
func TestDecideFailsWhatACloseBlamesOnIt(t *testing.T) {
	refused := segmentMessage(0, "refused", "missing", resolved(false))
	neighbour := segmentMessage(1, "neighbour", "", resolved(false))
	p, channel, fixture := decideFixture(t, refused, neighbour)
	fixture.shutDown(&amqp.Error{Code: 404, Server: true, Reason: "NOT_FOUND - no exchange 'missing' in vhost '/'"})

	failed := make(map[int]error)
	outcome := p.decide(context.Background(), channel, []outboundMessage{refused, neighbour}, nil, nil, failed)
	if len(outcome.republish) != 0 || outcome.err == nil {
		t.Fatalf("decide = %+v, want the segment failed and nothing republished", outcome)
	}
	if !isAMQPCode(failed[0], 404) {
		t.Fatalf("failed[0] = %v, want the broker's 404 close", failed[0])
	}
	if !errors.Is(failed[1], amqp.ErrClosed) {
		t.Fatalf("failed[1] = %v, want the transient closed channel", failed[1])
	}
}

// TestDecideKeepsOrderOverRepublishing covers an undecided message with an
// acked one behind it. Publishing it again would put it on the queue after a
// message the call sent later, so it fails transient instead, even though the
// close blamed another call.
func TestDecideKeepsOrderOverRepublishing(t *testing.T) {
	lost := segmentMessage(0, "lost", "", resolved(false))
	acked := segmentMessage(1, "acked", "", resolved(true))
	p, channel, fixture := decideFixture(t, lost, acked)
	fixture.shutDown(&amqp.Error{Code: 404, Server: true, Reason: "NOT_FOUND - no exchange 'elsewhere' in vhost '/'"})

	failed := make(map[int]error)
	outcome := p.decide(context.Background(), channel, []outboundMessage{lost, acked}, nil, nil, failed)
	if len(outcome.republish) != 0 {
		t.Fatalf("republish = %+v, want nothing: the acked message is behind the lost one", outcome.republish)
	}
	if kind, classified := driver.Classify(failed[0]); !classified || kind != driver.KindTransient {
		t.Fatalf("failed[0] = %v, want transient", failed[0])
	}
	if _, failedAcked := failed[1]; failedAcked {
		t.Fatalf("failed = %v, want the acked message published", failed)
	}
}

func isAMQPCode(err error, code int) bool {
	var amqpErr *amqp.Error
	return errors.As(err, &amqpErr) && amqpErr.Code == code
}

// TestProducerCloseIsBoundedByItsContext proves Close waits for publishes in
// flight only as long as its context allows. A publish that never finishes -
// its confirmation held by a broker that stopped answering - must not hold the
// caller of Close past its deadline.
func TestProducerCloseIsBoundedByItsContext(t *testing.T) {
	p := &producer{conn: &conn{}}
	if !p.enter() {
		t.Fatal("enter on an open producer refused")
	}
	defer p.exit()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Close(ctx) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second): //nolint:forbidigo // bounded wait on a call that must return
		t.Fatal("Close waited for a publish that never finishes past its context")
	}
	if p.enter() {
		t.Fatal("enter on a closed producer admitted a publish")
	}
}

// TestDecideWaitsForTheCloseAFailedWriteMet covers the gap between the client
// refusing a write because its channel closed and the close notification that
// follows: the client marks the channel closed first and sends the
// notification only once the publish that met the closed channel has released
// the channel's lock. Deciding in that gap would read an open channel and fail
// the unwritten messages; deciding after it can see that the close blamed
// another call's message and hand them back to be published again.
func TestDecideWaitsForTheCloseAFailedWriteMet(t *testing.T) {
	unwritten := segmentMessage(0, "unwritten", "", nil)
	p, channel, fixture := decideFixture(t, unwritten)
	decided := make(chan segmentOutcome, 1)
	failed := make(map[int]error)
	go func() {
		decided <- p.decide(context.Background(), channel, nil, []outboundMessage{unwritten}, classify("publish", driver.KindTransient, amqp.ErrClosed), failed)
	}()
	select {
	case outcome := <-decided:
		t.Fatalf("decide returned %+v before the close was known", outcome)
	case <-time.After(50 * time.Millisecond): //nolint:forbidigo // the absence of a return has no event to wait on
	}
	fixture.shutDown(&amqp.Error{Code: 404, Server: true, Reason: "NOT_FOUND - no exchange 'elsewhere' in vhost '/'"})
	outcome := <-decided
	if outcome.err != nil || len(failed) != 0 || len(outcome.republish) != 1 {
		t.Fatalf("decide = %+v, failed %v, want the unwritten message handed back", outcome, failed)
	}
}

// TestDecideIgnoresACloseAfterEverythingWasDecided covers a close that comes
// after every message of the segment was acked: the segment has nothing left
// for the close to decide, so the call goes on to its next segment instead of
// failing the messages it has not published yet.
func TestDecideIgnoresACloseAfterEverythingWasDecided(t *testing.T) {
	acked := segmentMessage(0, "acked", "", resolved(true))
	p, channel, fixture := decideFixture(t, acked)
	fixture.shutDown(&amqp.Error{Code: 320, Server: true, Reason: "CONNECTION_FORCED - broker forced connection closure"})
	failed := make(map[int]error)
	outcome := p.decide(context.Background(), channel, []outboundMessage{acked}, nil, nil, failed)
	if outcome.err != nil || len(outcome.republish) != 0 || len(failed) != 0 {
		t.Fatalf("decide = %+v, failed %v, want the segment published and the call going on", outcome, failed)
	}
}
