package rabbitmq

import (
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// sizeRefusal is the close the broker sends when a message is over its size
// limit, the one close whose reason decides a message's kind.
var sizeRefusal = &amqp.Error{
	Code:   406,
	Server: true,
	Reason: "PRECONDITION_FAILED - message size 16777217 is larger than configured max size 16777216",
}

type watcherFixture struct {
	conn    *conn
	returns chan amqp.Return
	closes  chan *amqp.Error
	watcher *publishWatcher
}

func newWatcherFixture(t *testing.T) *watcherFixture {
	t.Helper()
	fixture := &watcherFixture{
		conn:    &conn{},
		returns: make(chan amqp.Return, 16),
		closes:  make(chan *amqp.Error, 1),
	}
	fixture.watcher = startPublishWatcher(fixture.conn, fixture.returns, fixture.closes)
	return fixture
}

// shutDown closes the streams in the order the client closes them when a
// channel shuts down: the close notification first, then the returns.
func (f *watcherFixture) shutDown(reason *amqp.Error) {
	if reason != nil {
		f.closes <- reason
	}
	close(f.closes)
	close(f.returns)
}

func mustRegister(t *testing.T, watcher *publishWatcher, ids ...string) {
	t.Helper()
	busy, live := watcher.register(ids)
	if !live || busy != nil {
		t.Fatalf("register(%q) = busy %v, live %t, want the ids reserved", ids, busy, live)
	}
}

// answerSettle builds the watcher state for one registered id over the given
// streams and has it answer a settle for that id, the way the watcher's loop
// does when the request wins its select.
func answerSettle(returns chan amqp.Return, closes chan *amqp.Error, id string) watcherReply {
	state := watcherState{
		registered: map[string]*watcherRegistration{id: {}},
		returns:    returns,
		closes:     closes,
	}
	request := watcherRequest{op: watchSettle, ids: []string{id}, reply: make(chan watcherReply, 1)}
	state.answer(request)
	return <-request.reply
}

// TestPublishWatcherAnswersWithReturnsAlreadyHandedOver pins the interleaving
// that makes a settle complete. The client hands a message's return over
// before it resolves that message's confirmation, so when a call settles, the
// return can still be sitting in the watcher's buffer, and the watcher's select
// can take the settle request first. Answering before the buffer is emptied
// reports an unroutable message as published.
func TestPublishWatcherAnswersWithReturnsAlreadyHandedOver(t *testing.T) {
	returns := make(chan amqp.Return, 1)
	returns <- amqp.Return{ReplyCode: 312, ReplyText: "NO_ROUTE", MessageId: "returned"}
	reply := answerSettle(returns, make(chan *amqp.Error, 1), "returned")
	if _, ok := reply.returned["returned"]; !ok {
		t.Fatalf("settle with the return still buffered = %+v, want the return", reply)
	}
}

// TestPublishWatcherFilesAReturnUnderItsOwnID proves calls sharing a channel
// are each told only about their own messages: a return for one call's message
// fails that message, as the not-found a 312 NO_ROUTE is, and says nothing to
// the call beside it.
func TestPublishWatcherFilesAReturnUnderItsOwnID(t *testing.T) {
	fixture := newWatcherFixture(t)
	defer fixture.shutDown(nil)
	mustRegister(t, fixture.watcher, "call-a-0", "call-a-1")
	mustRegister(t, fixture.watcher, "call-b-0")
	fixture.returns <- amqp.Return{ReplyCode: 312, ReplyText: "NO_ROUTE", MessageId: "call-a-1"}

	if reply := fixture.watcher.settle([]string{"call-b-0"}); len(reply.returned) != 0 || reply.closed {
		t.Fatalf("settle of the call without a return = %+v, want nothing returned on an open channel", reply)
	}
	reply := fixture.watcher.settle([]string{"call-a-0", "call-a-1"})
	if len(reply.returned) != 1 {
		t.Fatalf("returned = %v, want only call-a-1", reply.returned)
	}
	failure := reply.returned["call-a-1"]
	if !errors.Is(failure, driver.ErrDestinationMissing) {
		t.Fatalf("returned[call-a-1] = %v, want ErrDestinationMissing", failure)
	}
	if kind, classified := driver.Classify(failure); !classified || kind != driver.KindNotFound {
		t.Fatalf("returned[call-a-1] classification = (%v, %t), want (not found, true)", kind, classified)
	}
}

// TestPublishWatcherRefusesAnIDAlreadyHeld proves two unresolved messages
// never share an id on one channel, which is what keeps a return unambiguous.
// The refusal reserves none of the ids asked for, so a call waiting for one id
// does not sit on the others, and the holder releasing its id is what wakes
// the call that was refused.
func TestPublishWatcherRefusesAnIDAlreadyHeld(t *testing.T) {
	fixture := newWatcherFixture(t)
	defer fixture.shutDown(nil)
	mustRegister(t, fixture.watcher, "shared")

	busy, live := fixture.watcher.register([]string{"fresh", "shared"})
	if !live || busy == nil {
		t.Fatalf("register of a held id = busy %v, live %t, want refused with a wait", busy, live)
	}
	mustRegister(t, fixture.watcher, "fresh")
	select {
	case <-busy:
		t.Fatal("the wait ended while the id was still held")
	default:
	}
	fixture.watcher.release([]string{"shared"})
	select {
	case <-busy:
	case <-time.After(5 * time.Second): //nolint:forbidigo // bounded wait on a goroutine handoff
		t.Fatal("releasing the held id did not end the wait")
	}
	mustRegister(t, fixture.watcher, "shared")
}

// TestPublishWatcherReportsTheClose covers what a settle reports once the
// channel has closed: the broker's reason when it gave one, which is where a
// refusal for size is recognised, and the closed publish channel this driver
// has always reported when it gave none. The client sends the reason before it
// closes the return stream, so a settle can find the closed return stream and
// the buffered reason together; one that stopped at the closed stream would
// report a refused message as a lost channel.
func TestPublishWatcherReportsTheClose(t *testing.T) {
	for _, test := range []struct {
		name         string
		reason       *amqp.Error
		want         driver.Kind
		wantSentinel error
	}{
		{name: "the broker refused a message for its size", reason: sizeRefusal, want: driver.KindTooLarge},
		{name: "the client closed without a reason", want: driver.KindTransient, wantSentinel: amqp.ErrClosed},
	} {
		t.Run(test.name, func(t *testing.T) {
			returns := make(chan amqp.Return)
			closes := make(chan *amqp.Error, 1)
			if test.reason != nil {
				closes <- test.reason
			}
			close(closes)
			close(returns)
			reply := answerSettle(returns, closes, "undecided")
			if !reply.closed {
				t.Fatalf("settle after the close = %+v, want closed", reply)
			}
			if kind, ok := driver.Classify(reply.closeErr); !ok || kind != test.want {
				t.Fatalf("Classify(closeErr) = %v, %t, want %v, true (%v)", kind, ok, test.want, reply.closeErr)
			}
			if test.wantSentinel != nil && !errors.Is(reply.closeErr, test.wantSentinel) {
				t.Fatalf("closeErr = %v, want %v", reply.closeErr, test.wantSentinel)
			}
			if test.reason != nil && !errors.Is(reply.closeErr, test.reason) {
				t.Fatalf("closeErr = %v, want the broker's own reason wrapped", reply.closeErr)
			}
		})
	}
}

// TestPublishWatcherRefusesRegisteringOnAClosedChannel proves a call is never
// handed a channel that has already closed: nothing registered there could be
// published, so the call must open a fresh channel instead.
func TestPublishWatcherRefusesRegisteringOnAClosedChannel(t *testing.T) {
	fixture := newWatcherFixture(t)
	mustRegister(t, fixture.watcher, "holder")
	fixture.shutDown(nil)
	for attempt := range 50 {
		if busy, live := fixture.watcher.register([]string{"late"}); live || busy != nil {
			t.Fatalf("attempt %d: register on a closed channel = busy %v, live %t, want refused", attempt, busy, live)
		}
	}
	fixture.watcher.release([]string{"holder"})
}

// TestPublishWatcherExitsOnceClosedAndSettled proves the watcher leaves nothing
// running behind a closed channel, and not before its last call has settled:
// exiting while a call still held ids would leave that call nobody to ask
// about the returns filed under them. The connection waits for the watcher,
// which is what makes a closed connection own no goroutine.
func TestPublishWatcherExitsOnceClosedAndSettled(t *testing.T) {
	fixture := newWatcherFixture(t)
	mustRegister(t, fixture.watcher, "last")
	fixture.returns <- amqp.Return{ReplyCode: 312, ReplyText: "NO_ROUTE", MessageId: "last"}
	fixture.shutDown(nil)
	select {
	case <-fixture.watcher.done:
		t.Fatal("the watcher exited while a call still held ids on it")
	case <-time.After(20 * time.Millisecond): //nolint:forbidigo // the absence of an exit has no event to wait on
	}
	if reply := fixture.watcher.settle([]string{"last"}); reply.returned["last"] == nil {
		t.Fatalf("settle after the close = %+v, want the return filed before it", reply)
	}
	exited := make(chan struct{})
	go func() {
		fixture.conn.detachWatch.Wait()
		close(exited)
	}()
	select {
	case <-exited:
	case <-time.After(5 * time.Second): //nolint:forbidigo // bounded wait on a goroutine exit
		t.Fatal("the watcher did not exit once its last call settled")
	}
}

// TestAMQPConfigLeavesRecoveryOff guards what a publish's handling of a closed
// channel rests on. With the client's automatic recovery off, a channel that
// shuts down resolves every confirmation it still holds as not acked, after
// the acks it had already read, and closes its return stream, which is how a
// call waiting on it learns its messages are undecided. With recovery on, the
// client keeps the channel's streams open across a reconnect instead, and a
// call waiting on a confirmation the broker will never send waits until its
// context ends. The driver owns reconnection; the client must not.
func TestAMQPConfigLeavesRecoveryOff(t *testing.T) {
	config, err := makeAMQPConfig(driver.Config{Endpoints: []string{defaultRabbitMQEndpoint}})
	if err != nil {
		t.Fatalf("makeAMQPConfig: %v", err)
	}
	if config.Recovery != nil {
		t.Fatalf("amqp.Config.Recovery = %+v, want nil: publish close handling needs client recovery off", config.Recovery)
	}
}
