package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/wire"
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

// shortStringProperty is one AMQP property whose wire form is a short string,
// with the wire header that fills it and a reader for the value the encoder
// carried through.
type shortStringProperty struct {
	name  string
	key   string
	value func(amqp.Publishing) string
}

func shortStringProperties() []shortStringProperty {
	return []shortStringProperty{
		{"CorrelationId", wire.CorrelationID, func(p amqp.Publishing) string { return p.CorrelationId }},
		{"MessageId", wire.ID, func(p amqp.Publishing) string { return p.MessageId }},
		{"Type", wire.Type, func(p amqp.Publishing) string { return p.Type }},
		{"ContentType", wire.DataContentType, func(p amqp.Publishing) string { return p.ContentType }},
	}
}

// TestAmqpPublishingRefusesOversizedShortStringProperties pins the four AMQP
// properties the client carries as short strings. AMQP prefixes one with a
// single length byte, so 255 bytes is publishable and 256 is not; the client
// finds that out while serializing the header, after the publish method frame
// is already on the wire, and shuts the whole shared connection down for the
// write error (amqp091-go writeShortstr, sendUnflushed). The refusal must
// therefore happen here, before anything is written, must classify the message
// as one the broker would never accept, and must name the property and its
// length without echoing the value.
func TestAmqpPublishingRefusesOversizedShortStringProperties(t *testing.T) {
	const shortStringLimit = 255
	for _, property := range shortStringProperties() {
		t.Run(property.name, func(t *testing.T) {
			atLimit := strings.Repeat("x", shortStringLimit)
			publishing, err := amqpPublishing(driver.OutboundMessage{
				Headers: []driver.Header{{Key: property.key, Value: []byte(atLimit)}},
			})
			if err != nil {
				t.Fatalf("amqpPublishing refused a %d-byte %s: %v", shortStringLimit, property.name, err)
			}
			if got := property.value(publishing); got != atLimit {
				t.Fatalf("%s = %d bytes, want all %d carried through", property.name, len(got), shortStringLimit)
			}

			oversized := atLimit + "x"
			_, err = amqpPublishing(driver.OutboundMessage{
				Headers: []driver.Header{{Key: property.key, Value: []byte(oversized)}},
			})
			if err == nil {
				t.Fatalf("amqpPublishing accepted a %d-byte %s", len(oversized), property.name)
			}
			if kind, classified := driver.Classify(err); !classified || kind != driver.KindTooLarge {
				t.Fatalf("classification = (%v, %t), want (too large, true)", kind, classified)
			}
			if want := fmt.Sprintf("property %s is %d bytes", property.name, len(oversized)); !strings.Contains(err.Error(), want) {
				t.Fatalf("error = %q, want it to name %q", err, want)
			}
			if strings.Contains(err.Error(), oversized) {
				t.Fatalf("error = %q, must not echo the property value", err)
			}
		})
	}
}

func TestAmqpPublishingRefusesOversizedHeaderKeys(t *testing.T) {
	atLimit := strings.Repeat("x", 243)
	oversized := atLimit + "x"
	batch := []driver.OutboundMessage{
		{Headers: []driver.Header{{Key: atLimit, Value: []byte("before")}}},
		{Headers: []driver.Header{{Key: oversized, Value: []byte("refused")}}},
		{Headers: []driver.Header{{Key: atLimit, Value: []byte("after")}}},
	}
	for index, message := range batch {
		publishing, err := amqpPublishing(message)
		if index != 1 {
			if err != nil {
				t.Fatalf("message %d with a 255-byte table key was refused: %v", index, err)
			}
			if got := publishing.Headers["cloudEvents:"+atLimit]; got != string(message.Headers[0].Value) {
				t.Fatalf("message %d header value = %v, want %s", index, got, message.Headers[0].Value)
			}
			continue
		}
		if err == nil {
			t.Fatal("amqpPublishing accepted a 256-byte table key")
		}
		if kind, classified := driver.Classify(err); !classified || kind != driver.KindTooLarge {
			t.Fatalf("classification = (%v, %t), want (too large, true)", kind, classified)
		}
		if !strings.Contains(err.Error(), "header key is 256 bytes") {
			t.Fatalf("error = %q, want the table key's byte length", err)
		}
		if strings.Contains(err.Error(), oversized) {
			t.Fatalf("error = %q, must not echo the header name", err)
		}
	}
	// A refused segment must not need a connection or channel.
	p := &producer{}
	failed := make(map[int]error)
	consumed, err := p.publishSegment(context.Background(), batch[1:2], 0, failed)
	if err != nil || consumed != 1 {
		t.Fatalf("publishSegment = (%d, %v), want (1, nil)", consumed, err)
	}
	if kind, classified := driver.Classify(failed[0]); !classified || kind != driver.KindTooLarge {
		t.Fatalf("segment refusal = (%v, %t), want (too large, true)", kind, classified)
	}
}

// TestAmqpPublishingRefusesOnlyTheOversizedMessageOfABatch covers the refusal
// being per message: an oversized property on the middle message of a batch
// refuses that message alone and leaves the messages around it encoded. That is
// what lets publishSegment record the refusal against the one index and publish
// the rest of the call.
func TestAmqpPublishingRefusesOnlyTheOversizedMessageOfABatch(t *testing.T) {
	oversized := strings.Repeat("x", 256)
	batch := []driver.OutboundMessage{
		{Destination: "orders", Headers: []driver.Header{{Key: wire.ID, Value: []byte("before")}}},
		{Destination: "orders", Headers: []driver.Header{
			{Key: wire.ID, Value: []byte("refused")},
			{Key: wire.CorrelationID, Value: []byte(oversized)},
		}},
		{Destination: "orders", Headers: []driver.Header{{Key: wire.ID, Value: []byte("after")}}},
	}
	for index, message := range batch {
		_, err := amqpPublishing(message)
		if index != 1 {
			if err != nil {
				t.Fatalf("message %d of the batch was refused with the oversized message's error: %v", index, err)
			}
			continue
		}
		if err == nil {
			t.Fatal("the oversized message of the batch was encoded")
		}
		if kind, classified := driver.Classify(err); !classified || kind != driver.KindTooLarge {
			t.Fatalf("the oversized message's error = %v, want classified too large", err)
		}
	}
}
