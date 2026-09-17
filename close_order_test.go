package f1_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
)

// closeOrderPublishTopic is a topic this subscription does not consume, so a
// publish to it adds no delivery to the consumer. That matters more than keeping
// a measurement tidy: a delivery the driver hands over while Close is draining
// can be stranded by the wrapper's relay hop and never settled, which makes the
// consumer refuse to stop and Close return an error, an intermittent failure of
// the test rather than a fault it found. See recordingMessages for the hop.
//
// The close-order test sends its second application publish here, because that
// publish must keep Close inside its publish-idle wait. The released-consumer
// test sends one here too: not to be waited on, but because a client only owns
// a producer once something has been published, and that test needs a producer
// for Close to close.
const closeOrderPublishTopic = "orders.audit"

// closeOrderRetryInterval puts the retry copy far enough in the future that it
// is not due while the client is closing. The successor publish is the drain's
// only blocker here; a copy that became deliverable during the drain would put a
// second delivery in flight in the one window this test is pinning.
const closeOrderRetryInterval = time.Hour

// closeOrderSettleWindow bounds the one assertion in these tests that watches
// for an event instead of waiting for one: the producer close that must not
// happen while a publish is still in flight.
//
// It is the only deterministic detector for that violation. The ordering chain
// below also requires the application publish to have returned before the
// producer close, but that check reads the log after the test has released the
// publish, so whether it sees the violation depends on which goroutine the
// scheduler runs first: with the quiescence wait removed, releasing the publish
// usually beats Close's producer teardown to the log, and the chain passes.
// Watching for the event while the publish is still held has no such race,
// because the release has not happened yet when the window starts.
const closeOrderSettleWindow = 100 * time.Millisecond

// closeOrderMessageKey and closeOrderApplicationKey are the partition keys the
// two publishes carry. The assertions find each publish in the log by key and
// destination rather than by position, and the delivery carrying
// closeOrderMessageKey is what the retry copy is identified from.
const (
	closeOrderMessageKey     = "close-order"
	closeOrderApplicationKey = "close-order-second"
)

// TestCloseDrainsBeforeItClosesTheProducerAndConnection pins the shutdown
// order: Close drains every runner, waits for a publish that is still in
// flight, closes the producer, and closes the connection last.
//
// Two publishes are in flight while Close runs, and each pins one half of the
// order. The first is the core's own: the retry copy of a delivery whose
// handler failed, held by the wrapper, so the drain that owns the delivery
// cannot finish until it is released. The second is the application's, admitted
// before Close started and held too, so Close reaches the publish-idle wait
// with something still publishing; it is published to a topic this subscription
// does not consume, so it adds nothing to the drain. A producer close that ran
// before the drain would tear down a producer that still owns a delivery; one
// that ran before the publish-idle wait would tear down a producer that still
// owns a publish.
func TestCloseDrainsBeforeItClosesTheProducerAndConnection(t *testing.T) {
	ctx := t.Context()
	fixture := newRecordingFixture(t, f1.WithPublishTopics("orders.created", closeOrderPublishTopic))
	successorPublish := fixture.gate.hold(2)
	applicationPublish := fixture.gate.hold(3)
	// A fatal assertion while a hold is in flight would otherwise leave that
	// publish unresolved, and the fixture's cleanup Close would then block in its
	// drain, because the runner's only worker is the goroutine inside the held
	// publish. The test binary then times out instead of reporting the failure it
	// already has. letGo is idempotent, so this is a no-op on the passing path.
	t.Cleanup(func() {
		successorPublish.letGo()
		applicationPublish.letGo()
	})
	var attempts atomic.Int32
	subscription := recordingSubscription(
		f1.RetryConfig{MaxAttempts: 2, InitialInterval: closeOrderRetryInterval},
		f1.HandlerFunc(func(context.Context, *f1.Event) error {
			if attempts.Add(1) == 1 {
				fixture.log.record(eventHandlerFailed, "first attempt")
				return errors.New("first attempt fails")
			}
			fixture.log.record(eventHandlerHandled, "handled")
			return nil
		}),
	)
	startRecordingRunner(t, fixture, subscription)

	// The first publish is the application's original event, which the gate lets
	// through. Its delivery fails, so the core publishes the retry copy, and that
	// second publish is the one held here.
	if _, err := fixture.client.Publisher().Publish(ctx, "orders.created.v1",
		map[string]string{"id": "close-order"}, f1.WithKey(closeOrderMessageKey)); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	waitForChannel(t, fixture.log, successorPublish.started, "the retry copy was never published")

	// A second application publish, admitted while the client still accepts one,
	// is held as well. It goes to a topic this subscription does not consume, so
	// the only thing it can hold up is Close's publish-idle wait.
	applicationDone := make(chan error, 1)
	go func() {
		_, err := fixture.client.Publisher().Publish(ctx, closeOrderPublishTopic+".v1",
			map[string]string{"id": "close-order-second"}, f1.WithKey(closeOrderApplicationKey))
		applicationDone <- err
	}()
	waitForChannel(t, fixture.log, applicationPublish.started, "the application publish never reached the driver")

	closeDone := make(chan error, 1)
	go func() { closeDone <- fixture.client.Close(ctx) }()

	// The drain has begun and cannot finish while the retry copy is unpublished,
	// so Close is still inside its first phase. A producer close that arrives
	// before the drain reports itself here, rather than leaving this wait to
	// time out.
	waitForChannelWithout(t, fixture.log, fixture.log.observed(eventConsumerDrained), eventProducerClosed,
		"Producer.Close ran before Close drained the runner that still owned a delivery")
	requirePending(t, fixture.log, closeDone, "Close")

	// Releasing the core's publish settles the delivery and finishes the drain.
	// Close then reaches the publish-idle wait, where the application's publish
	// still holds it: it must not close the producer yet.
	successorPublish.letGo()
	waitForChannel(t, fixture.log, fixture.log.observed(eventConsumerStopped), "the runner never finished draining")
	requirePending(t, fixture.log, closeDone, "Close")
	requireNoEventWithin(t, fixture.log, eventProducerClosed, closeOrderSettleWindow,
		"Producer.Close ran while an application publish was still in flight")

	applicationPublish.letGo()
	if err := waitForResult(t, fixture.log, closeDone, "Close"); err != nil {
		t.Fatalf("Close() error = %v; log:%s", err, fixture.log.describe())
	}
	if err := waitForResult(t, fixture.log, applicationDone, "the application publish"); err != nil {
		t.Fatalf("application Publish() error = %v", err)
	}

	// Every entry is identified by destination and message key rather than by
	// position. The core's own publish of the retry copy is the publish of the
	// delivery's message from a destination other than the one it arrived on, and
	// the application's publish is the one carrying the second message's key.
	drain := requireEntry(t, fixture.log, eventConsumerDrained, 0, "the runner drain")
	settled := requireEntry(t, fixture.log, eventSettled, 0, "the original delivery")
	successor := requireSuccessorPublish(t, fixture.log, settled, "the retry copy")
	stop := requireEntry(t, fixture.log, eventConsumerStopped, 0, "the consumer teardown")
	application := requirePublishOfKey(t, fixture.log, closeOrderApplicationKey, "the application publish")
	producerClose := requireEntry(t, fixture.log, eventProducerClosed, 0, "the producer close")
	connClose := requireEntry(t, fixture.log, eventConnClosed, 0, "the connection close")

	requireOrdered(t, fixture.log,
		"Close must release the consumer, wait out the publishes it owns, and only then close the producer and the connection",
		drain, successor, settled, stop, application, producerClose, connClose)

	inFlight, closes := fixture.gate.producerCloseSnapshot()
	if closes == 0 {
		t.Fatal("the producer was never closed")
	}
	if inFlight != 0 {
		t.Fatalf("Producer.Close ran with %d publish(es) in flight, want 0; log:%s", inFlight, fixture.log.describe())
	}
	t.Logf("event order:%s", fixture.log.describe())
}

// closeAbandonedHandlerTimeout is the handler budget the abandoned-handler test
// gives its handler. It is short because that handler never returns: the core
// gives up on a handler that ignores cancellation after stuckAbortMultiplier of
// these, and that abandon is what settles the delivery while the handler
// goroutine is still inside it.
const closeAbandonedHandlerTimeout = 30 * time.Millisecond

// closeAbandonedKey and closeReleasedKey are the partition keys the two tests
// below publish under. The assertions identify their messages by destination and
// key, never by position in the log, for the reason recordingEvent gives.
const (
	closeAbandonedKey = "close-abandoned"
	closeReleasedKey  = "close-released"
)

// TestCloseFinishesARunnerWhileItsHandlerStillRuns pins what Close does with a
// runner whose only remaining work is a handler that will not return.
//
// The handler blocks and never looks at its context. The core gives up on it at
// the stuck threshold, abandons the delivery and settles it with a requeue nack,
// which is the settle this test holds. At that point the delivery is settled and
// out of the drain's accounting, and only the handler goroutine is still
// running. Close must still finish that runner rather than wait for a handler
// that never returns, and it must not finish it before the abandoned delivery's
// settle returned: a runner torn down without that settle leaves the delivery
// with the driver, and the consumer refuses to stop while it is outstanding.
//
// The assertion that rules the second failure out is the still-pending Close
// while the settle is held. The chain afterwards also requires the settle before
// the consumer stop, but a chain read after the fact cannot see a settle the
// runner never waited for, and only the driver would then be holding the
// evidence.
func TestCloseFinishesARunnerWhileItsHandlerStillRuns(t *testing.T) {
	ctx := t.Context()
	fixture := newRecordingFixture(t, f1.WithPublishTopics("orders.created"))
	abandonedSettle := fixture.calls.holdNext(eventNacked)
	// A failing assertion while the settle is held would otherwise leave the
	// runner's only worker blocked in it, and the fixture's cleanup Close would
	// then wait out its own drain budget. letGo is idempotent, so this is a no-op
	// on the passing path.
	t.Cleanup(abandonedSettle.letGo)

	handlerStarted := make(chan struct{})
	handlerRelease := make(chan struct{})
	handlerDone := make(chan error, 1)
	t.Cleanup(func() { close(handlerRelease) })
	var started sync.Once
	var attempts atomic.Int32
	subscription := recordingSubscription(
		f1.RetryConfig{MaxAttempts: 2, InitialInterval: time.Hour},
		f1.HandlerFunc(func(context.Context, *f1.Event) error {
			attempts.Add(1)
			started.Do(func() { close(handlerStarted) })
			<-handlerRelease
			handlerDone <- nil
			return nil
		}),
	)
	subscription.HandlerTimeout = closeAbandonedHandlerTimeout
	startRecordingRunner(t, fixture, subscription)

	if _, err := fixture.client.Publisher().Publish(ctx, "orders.created.v1",
		map[string]string{"id": "close-abandoned"}, f1.WithKey(closeAbandonedKey)); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	waitForChannel(t, fixture.log, handlerStarted, "the handler never started")

	// Close runs while the handler is still inside its first attempt, so the
	// drain has a runner to finish and a delivery it cannot settle yet.
	closeDone := make(chan error, 1)
	go func() { closeDone <- fixture.client.Close(ctx) }()
	waitForChannel(t, fixture.log, abandonedSettle.started, "the abandoned delivery was never settled")

	// The delivery is settled and the handler is still running: nothing is
	// outstanding in the drain's accounting, and the one thing left in the
	// runner is a goroutine the core has already given up on. Close must still
	// be waiting for the settle the abandon owes the delivery, and it must not be
	// waiting for the handler.
	//
	// The absence check is the structural half: the consumer cannot legitimately
	// be stopped while that settle is still on its way back, so a stop that
	// arrives now can only be a runner that moved on without it. Reading the
	// order after the fact also catches that, but that check races the settle's
	// own record, and one run in several would miss it.
	requireNoEventWithin(t, fixture.log, eventConsumerStopped, closeOrderSettleWindow,
		"the consumer was stopped while the abandoned delivery's settle was still in flight")
	requirePending(t, fixture.log, closeDone, "Close")
	requirePending(t, fixture.log, handlerDone, "the handler")

	abandonedSettle.letGo()
	if err := waitForResult(t, fixture.log, closeDone, "Close"); err != nil {
		t.Fatalf("Close() error = %v; log:%s", err, fixture.log.describe())
	}
	requirePending(t, fixture.log, handlerDone, "the handler")

	// Every assertion below identifies its message by destination and key, and
	// the lifecycle entries are the only ones of their kind.
	published := requirePublishOfKey(t, fixture.log, closeAbandonedKey, "the application publish")
	abandoned := requireEntry(t, fixture.log, eventNacked, 0, "the abandoned delivery")
	requireSameMessage(t, fixture.log, abandoned, published, "the abandoned delivery")
	stop := requireEntry(t, fixture.log, eventConsumerStopped, 0, "the consumer teardown")
	producerClose := requireEntry(t, fixture.log, eventProducerClosed, 0, "the producer close")
	connClose := requireEntry(t, fixture.log, eventConnClosed, 0, "the connection close")

	requireOrdered(t, fixture.log,
		"Close must settle the delivery it abandoned before it stops the consumer, and close the producer and the connection last",
		abandoned, stop, producerClose, connClose)

	// The abandon requeues: a delivery no handler completed must not be
	// acknowledged, and a drain that reached the abandon before the settle
	// returned would have had to hand the consumer back instead of stopping it.
	if acks := fixture.log.entries(eventSettled); len(acks) != 0 {
		t.Fatalf("the abandoned delivery was acknowledged %d time(s), want a requeue nack and no ack; log:%s", len(acks), fixture.log.describe())
	}
	if releases := fixture.log.entries(eventConsumerReleased); len(releases) != 0 {
		t.Fatalf("the consumer was released %d time(s), want the drain to settle the abandoned delivery before stopping it; log:%s", len(releases), fixture.log.describe())
	}
	// The requeued message waits for the broker, not for a consumer that is
	// draining, so the handler must not run again while the client closes.
	if got := attempts.Load(); got != 1 {
		t.Fatalf("the handler ran %d time(s), want exactly one attempt while the client closes; log:%s", got, fixture.log.describe())
	}
	t.Logf("event order:%s", fixture.log.describe())
}

// TestCloseDrainsARunnerThatReleasedItsConsumer pins what Close does with a
// runner whose consumer is already gone when the Close arrives.
//
// The runner is driven into the lane-repair path: a consumer error makes it
// release the consumer it holds and open a replacement. The release has returned
// and the runner is alive inside that replacement open when Close runs, which is
// the state this test is about, and the open is held so the state is not a
// matter of timing.
//
// Close must cancel and wait for that runner. A drain that treated a released
// consumer as nothing left to drain would let the producer and the connection
// close while the runner was still replacing it, and the runner would then be
// repairing a client that is already down.
func TestCloseDrainsARunnerThatReleasedItsConsumer(t *testing.T) {
	ctx := t.Context()
	fixture := newRecordingFixture(t, f1.WithPublishTopics("orders.created", closeOrderPublishTopic))
	subscription := recordingSubscription(
		f1.RetryConfig{MaxAttempts: 2, InitialInterval: time.Hour},
		f1.HandlerFunc(func(context.Context, *f1.Event) error { return nil }),
	)
	startRecordingRunner(t, fixture, subscription)

	// A client owns a producer only once something has been published, and the
	// producer close is one link of the order asserted below. The publish goes
	// to the declared topic this subscription does not consume, so it adds no
	// delivery to the runner.
	if _, err := fixture.client.Publisher().Publish(ctx, closeOrderPublishTopic+".v1",
		map[string]string{"id": "close-released"}, f1.WithKey(closeReleasedKey)); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	replacement := fixture.calls.holdNext(eventConsumerCreated)
	t.Cleanup(replacement.letGo)
	fixture.injectError(errors.New("the consumer failed"))

	// The runner releases the consumer it holds and opens a replacement, which
	// is the call held here. Close then runs after that release has returned and
	// while the replacement open is still in flight, which is the state this
	// test is about: the consumer is gone, the runner is not.
	waitForChannel(t, fixture.log, fixture.log.observed(eventConsumerReleased), "the runner never released its consumer")
	waitForChannel(t, fixture.log, replacement.started, "the runner never opened a replacement consumer")
	// The runner's own teardown of the replacement is a release when it saw the
	// drain before admitting that consumer and a stop when it admitted it first,
	// and both are in order. The hold is armed for either call, so what the
	// assertions below pin is not which one the runner makes.
	teardown := fixture.calls.holdNext(eventConsumerReleased, eventConsumerStopped)
	t.Cleanup(teardown.letGo)

	closeDone := make(chan error, 1)
	go func() { closeDone <- fixture.client.Close(ctx) }()

	// Close must be waiting for that runner, so the producer cannot be closed
	// while the runner is still inside its replacement open. This is the
	// structural check: it is made while the runner is held, so it cannot race
	// the drain, and a Close that treated a released consumer as nothing left to
	// drain closes the producer here.
	requireNoEventWithin(t, fixture.log, eventProducerClosed, closeOrderSettleWindow,
		"the producer was closed while the runner that had released its consumer was still opening its replacement")

	replacement.letGo()
	waitForChannel(t, fixture.log, teardown.started, "the runner never tore its replacement consumer down")
	requirePending(t, fixture.log, closeDone, "Close")

	teardown.letGo()
	if err := waitForResult(t, fixture.log, closeDone, "Close"); err != nil {
		t.Fatalf("Close() error = %v; log:%s", err, fixture.log.describe())
	}

	release := requireEntry(t, fixture.log, eventConsumerReleased, 0, "the release the runner made")
	replacementCreated := requireEntry(t, fixture.log, eventConsumerCreated, 1, "the replacement consumer")
	producerClose := requireEntry(t, fixture.log, eventProducerClosed, 0, "the producer close")
	connClose := requireEntry(t, fixture.log, eventConnClosed, 0, "the connection close")

	requireOrdered(t, fixture.log,
		"Close must drain the runner that had released its consumer, replacement and all, before it closes the producer and the connection",
		release, replacementCreated, producerClose, connClose)
	t.Logf("event order:%s", fixture.log.describe())
}
