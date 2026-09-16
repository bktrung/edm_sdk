package f1_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
)

// closeOrderPublishTopic is where the application publish that must keep Close
// inside its publish-idle wait is sent. It is a topic this subscription does not
// consume, so the publish adds no delivery to the consumer. That matters more
// than keeping this test's measurements tidy: a delivery the driver hands over
// while Close is draining can be stranded by the wrapper's relay hop and never
// settled, which makes the consumer refuse to stop and Close return an error, an
// intermittent failure of the test rather than a fault it found. See
// recordingMessages for the hop.
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
