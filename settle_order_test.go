package f1_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
)

// settleOrderMessageKey is the partition key the message these tests publish
// carries. A successor copy keeps it, which is what lets the assertions find the
// copy in the log by identity instead of by position.
const settleOrderMessageKey = "settle-order"

// TestRetryCopyIsConfirmedBeforeTheOriginalIsSettled pins the positive half of
// the settle-last invariant: the original delivery is acknowledged only after
// the retry copy that carries the work forward was durably published.
//
// The negative half is pinned elsewhere, and it is not enough on its own. A
// core that acknowledged the original first would keep that test and every
// other one green, and would lose the message whenever the successor publish
// failed. The retry copy's own delivery and settlement are asserted too, so
// this test cannot pass on a path that never settles anything.
func TestRetryCopyIsConfirmedBeforeTheOriginalIsSettled(t *testing.T) {
	ctx := t.Context()
	fixture := newRecordingFixture(t)
	var attempts atomic.Int32
	subscription := recordingSubscription(
		f1.RetryConfig{MaxAttempts: 2, InitialInterval: recordingRetryInterval},
		f1.HandlerFunc(func(_ context.Context, _ *f1.Event) error {
			if attempts.Add(1) == 1 {
				fixture.log.record(eventHandlerFailed, "first attempt")
				return errors.New("first attempt fails")
			}
			fixture.log.record(eventHandlerHandled, "handled")
			return nil
		}),
	)
	startRecordingRunner(t, fixture, subscription)

	if _, err := fixture.client.Publisher().Publish(ctx, "orders.created.v1",
		map[string]string{"id": "retry-copy"}, f1.WithKey(settleOrderMessageKey)); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	waitForChannel(t, fixture.log, fixture.log.observedNth(eventSettled, 2), "the retry copy was never settled")

	// The original delivery is the first ack, and it is identified by its
	// destination and key rather than by position, which is what the retry copy
	// is then found from.
	originalSettle := requireEntry(t, fixture.log, eventSettled, 0, "the original delivery")
	retryPublish := requireSuccessorPublish(t, fixture.log, originalSettle, "the retry copy")
	rejected := requireEntry(t, fixture.log, eventHandlerFailed, 0, "the first attempt")
	handled := requireEntry(t, fixture.log, eventHandlerHandled, 0, "the retry copy")
	retrySettle := requireEntry(t, fixture.log, eventSettled, 1, "the retry copy delivery")

	// The original publish's own record is deliberately not part of either chain:
	// the driver may hand the message to the consumer before its own Publish call
	// returns, so the first handler call, and even the retry copy's publish, can
	// precede that record legitimately.
	requireOrdered(t, fixture.log, "the original must be acknowledged only after its retry copy was confirmed",
		rejected, retryPublish, originalSettle)
	requireOrdered(t, fixture.log, "the retry copy must be delivered and settled after it was published",
		retryPublish, handled, retrySettle)
	requireSameMessage(t, fixture.log, retrySettle, retryPublish, "the retry copy")
	if got := attempts.Load(); got != 2 {
		t.Fatalf("handler ran %d time(s), want the original and its retry copy only; log:%s", got, fixture.log.describe())
	}
	t.Logf("event order:%s", fixture.log.describe())
}

// TestDeadLetterCopyIsConfirmedBeforeTheOriginalIsSettled pins the same
// invariant on the dead-letter path: when the attempt budget is spent, the
// original is acknowledged only after the dead-letter copy was confirmed. The
// two paths are separate code, and settling the original before either publish
// loses the message on a publish failure, so both are asserted.
func TestDeadLetterCopyIsConfirmedBeforeTheOriginalIsSettled(t *testing.T) {
	ctx := t.Context()
	fixture := newRecordingFixture(t)
	subscription := recordingSubscription(
		f1.RetryConfig{MaxAttempts: 1},
		f1.HandlerFunc(func(context.Context, *f1.Event) error {
			fixture.log.record(eventHandlerFailed, "no attempt left")
			return errors.New("handler always fails")
		}),
	)
	startRecordingRunner(t, fixture, subscription)

	if _, err := fixture.client.Publisher().Publish(ctx, "orders.created.v1",
		map[string]string{"id": "dead-letter-copy"}, f1.WithKey(settleOrderMessageKey)); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	// Two publishes are waited for, not identified by the order they arrive in:
	// the original's record and the copy's can land in either order, because the
	// driver may deliver the message before its own Publish call returns. Waiting
	// for both keeps the failure a comparison of two present records.
	waitForChannel(t, fixture.log, fixture.log.observedNth(eventPublished, 2), "the dead-letter copy was never published")
	waitForChannel(t, fixture.log, fixture.log.observed(eventSettled), "the original delivery was never settled")

	originalSettle := requireEntry(t, fixture.log, eventSettled, 0, "the original delivery")
	deadLetterPublish := requireSuccessorPublish(t, fixture.log, originalSettle, "the dead-letter copy")
	rejected := requireEntry(t, fixture.log, eventHandlerFailed, 0, "the only attempt")

	// As in the retry case, the original publish's record is not part of the
	// chain: the driver may deliver the message before its own Publish call
	// returns.
	requireOrdered(t, fixture.log, "the original must be acknowledged only after its dead-letter copy was confirmed",
		rejected, deadLetterPublish, originalSettle)
	t.Logf("event order:%s", fixture.log.describe())
}

func TestDiscardCallbackWaitsForAckReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fixture := newRecordingFixture(t)
		hold := fixture.calls.holdNext(eventSettled)
		t.Cleanup(hold.letGo)
		cause := errors.New("discard this event")
		subscription := recordingSubscription(f1.RetryConfig{MaxAttempts: 1},
			f1.HandlerFunc(func(context.Context, *f1.Event) error { return f1.Drop(cause) }))
		var reports []f1.Discarded
		const notified recordingEventKind = "discard callback"
		subscription.OnDiscarded = func(_ context.Context, payload f1.Discarded) {
			reports = append(reports, payload)
			fixture.log.record(notified, payload.Envelope.ID)
		}
		startRecordingRunner(t, fixture, subscription)
		body := []byte(`{"id":"discard-held"}`)
		id, err := fixture.client.Publisher().Publish(t.Context(), "orders.created.v1",
			map[string]string{"id": "discard-held"}, f1.WithKey(settleOrderMessageKey))
		if err != nil {
			t.Fatal(err)
		}
		waitForChannel(t, fixture.log, hold.started, "Ack never reached the hold")
		synctest.Wait()
		beforeReturn := len(reports)
		hold.letGo()
		synctest.Wait()
		if err := fixture.client.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if beforeReturn != 0 {
			t.Errorf("OnDiscarded calls while Ack is held = %d, want 0", beforeReturn)
		}
		if len(reports) != 1 {
			t.Fatalf("OnDiscarded calls after Ack returns = %d, want 1", len(reports))
		}
		got := reports[0]
		if got.Envelope.ID != id || string(got.Body) != string(body) || got.Reason != f1.DiscardDropped || !errors.Is(got.Err, cause) {
			t.Fatalf("discard payload = %+v, want original id/body/drop cause", got)
		}
		settled := requireEntry(t, fixture.log, eventSettled, 0, "the discarded delivery")
		discarded := requireEntry(t, fixture.log, notified, 0, "the discard callback")
		requireOrdered(t, fixture.log, "discard callback must follow Ack return", settled, discarded)
	})
}
