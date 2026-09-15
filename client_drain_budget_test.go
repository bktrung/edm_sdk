package f1

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// newBudgetTestClient builds a client on a fake clock with a runner whose
// Run goroutine has wedged before finishRunner: started is true and done
// never closes, which is the driver-side wedge shape the consumer drain
// budget exists to bound. The
// caller releases the runner through the returned function once the wait no
// longer needs to stay blocked.
func newBudgetTestClient(t *testing.T) (*Client, *Runner, *clock.Fake, func()) {
	t.Helper()
	fake := clock.NewFake(time.Unix(0, 0))
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&dispatchDriver{conn: &dispatchConn{producer: &dispatchProducer{}, consumer: newDispatchConsumer()}}),
		withClock(fake))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	runner := &Runner{
		client: client,
		subscription: Subscription{
			Name:       "orders",
			Topics:     []string{"orders.created"},
			Priorities: []Priority{PriorityHigh},
		},
		inflight: newInflightRegistry(),
		started:  true,
		done:     make(chan struct{}),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(runner.done) }) }
	t.Cleanup(release)
	return client, runner, fake, release
}

// TestConsumerDrainBudgetBoundsAWedgedRunnerDrain pins the ruling that a
// wedged driver cannot hold Client.Close past ConsumerDrainTimeout. The
// runner's done channel never closes, so without the bound this wait ends
// only when the caller's context does, and doc 02's examples pass
// context.Background(). A positive budget must end the phase itself, with
// the same deadline identity as every other bounded shutdown phase.
func TestConsumerDrainBudgetBoundsAWedgedRunnerDrain(t *testing.T) {
	client, runner, fake, release := newBudgetTestClient(t)
	client.config.Lifecycle.ConsumerDrainTimeout = 5 * time.Second

	drained := make(chan error, 1)
	go func() { drained <- client.drainRunners(context.Background(), []*Runner{runner}) }()

	waitForFakeTimer(t, fake)
	fake.Advance(5 * time.Second)

	watchdog := clock.NewReal().Timer(2 * time.Second)
	defer watchdog.Stop()
	select {
	case err := <-drained:
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "consumer drain") {
			t.Fatalf("drainRunners() error = %v, want the consumer drain phase deadline", err)
		}
	case <-watchdog.C:
		release()
		t.Fatal("drainRunners did not return when the consumer drain budget expired; the runner drain is still bounded only by the caller's context")
	}
}

// TestZeroConsumerDrainTimeoutLeavesTheCallerContextAsTheOnlyBound pins the
// zero semantics of ConsumerDrainTimeout: zero disables the bound entirely.
// No hidden fallback may stand in for it, and the caller's context must
// remain able to end the wait. Together the two halves guarantee that adding
// the field changed nothing for a caller who does not set it.
func TestZeroConsumerDrainTimeoutLeavesTheCallerContextAsTheOnlyBound(t *testing.T) {
	client, runner, fake, release := newBudgetTestClient(t)
	client.config.Lifecycle.ConsumerDrainTimeout = 0

	drained := make(chan error, 1)
	go func() { drained <- client.drainRunners(context.Background(), []*Runner{runner}) }()

	guard := clock.NewReal().Timer(100 * time.Millisecond)
	defer guard.Stop()
	select {
	case err := <-drained:
		release()
		t.Fatalf("drainRunners returned %v with a zero budget and no caller deadline", err)
	case <-guard.C:
	}

	fake.Advance(24 * time.Hour)
	for range 50 {
		fake.Advance(time.Hour)
		select {
		case err := <-drained:
			release()
			t.Fatalf("zero ConsumerDrainTimeout fell back to a hidden bound: drainRunners returned %v once the fake clock advanced", err)
		default:
		}
		runtime.Gosched()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	cancelled := make(chan error, 1)
	go func() { cancelled <- client.drainRunners(ctx, []*Runner{runner}) }()

	watchdog := clock.NewReal().Timer(2 * time.Second)
	defer watchdog.Stop()
	select {
	case err := <-cancelled:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("drainRunners() error = %v, want the caller's context deadline", err)
		}
	case <-watchdog.C:
		release()
		t.Fatal("the caller's context did not end the runner drain")
	}
}

// TestCloseConsumerDrainBudgetIsThePublicContract pins the caller-visible
// Close behavior when the consumer drain budget expires against a wedged
// runner. The helper-level tests above prove the drainRunners phase; this
// one proves the Client.Close contract on top of it: the returned error
// keeps both the phase identity and the deadline cause, the failure clears
// only the concurrent close guard while every admission guard stays up,
// driver teardown has not started, and a retried Close joins the existing
// work and completes without duplicating it.
func TestCloseConsumerDrainBudgetIsThePublicContract(t *testing.T) {
	client, runner, fake, release := newBudgetTestClient(t)
	client.config.Lifecycle.ConsumerDrainTimeout = 5 * time.Second
	client.mu.Lock()
	client.runners[runner] = struct{}{}
	client.mu.Unlock()
	// The producer handle is created lazily on the first publish. Warm it up
	// before shutdown so the teardown-count assertions below observe real
	// resources instead of a nil slot.
	if _, err := client.Publisher().Publish(context.Background(), "orders.created", "payload"); err != nil {
		t.Fatalf("warmup Publish() error = %v", err)
	}
	producer, ok := client.producerHandle.(*dispatchProducer)
	if !ok {
		t.Fatalf("producer handle is %T, want *dispatchProducer", client.producerHandle)
	}
	conn, ok := client.conn.(*dispatchConn)
	if !ok {
		t.Fatalf("connection is %T, want *dispatchConn", client.conn)
	}

	closed := make(chan error, 1)
	go func() { closed <- client.Close(context.Background()) }()
	waitForFakeTimer(t, fake)

	concurrent := make(chan error, 1)
	go func() { concurrent <- client.Close(context.Background()) }()
	guardWatchdog := clock.NewReal().Timer(2 * time.Second)
	defer guardWatchdog.Stop()
	select {
	case err := <-concurrent:
		if err == nil || !strings.Contains(err.Error(), "client is closing") {
			t.Fatalf("concurrent Close() error = %v, want the already-closing guard", err)
		}
	case <-guardWatchdog.C:
		release()
		t.Fatal("a concurrent Close was not refused while the consumer drain held the guard")
	}

	fake.Advance(5 * time.Second)

	watchdog := clock.NewReal().Timer(2 * time.Second)
	defer watchdog.Stop()
	select {
	case err := <-closed:
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "consumer drain") {
			t.Fatalf("Close() error = %v, want the consumer drain phase deadline", err)
		}
	case <-watchdog.C:
		release()
		t.Fatal("Close did not return when the consumer drain budget expired; the public close path lost its bound")
	}

	client.mu.Lock()
	closing, finished, shutdownStarted := client.closing, client.closed, client.shutdownStarted
	client.mu.Unlock()
	if closing {
		t.Fatal("the failed close left the concurrent close guard armed")
	}
	if finished {
		t.Fatal("the failed close marked the client closed")
	}
	if !shutdownStarted {
		t.Fatal("the failed clear dropped the shutdown admission guards")
	}
	if _, err := client.Subscribe(context.Background(), Subscription{Name: "late", Topics: []string{"orders.created"}, Priorities: []Priority{PriorityHigh}}); err == nil || !strings.Contains(err.Error(), "client is closing") {
		t.Fatalf("Subscribe() after a failed close error = %v, want refusal", err)
	}
	if _, err := client.Publisher().Publish(context.Background(), "orders.created", "payload"); err == nil || !strings.Contains(err.Error(), "client is closed") {
		t.Fatalf("Publish() after a failed close error = %v, want refusal", err)
	}
	if err := client.Health(context.Background()); err == nil || !strings.Contains(err.Error(), "client is closing") {
		t.Fatalf("Health() after a failed close error = %v, want refusal", err)
	}
	if producer.closeCount() != 0 || conn.closeCount() != 0 {
		t.Fatal("the failed close started tearing down driver resources during the runner drain")
	}

	release()
	retried := make(chan error, 1)
	go func() { retried <- client.Close(context.Background()) }()
	select {
	case err := <-retried:
		if err != nil {
			t.Fatalf("retry Close() error = %v, want the retry to join the existing work and complete", err)
		}
	case <-watchdog.C:
		t.Fatal("retry Close did not complete after the wedged runner was released")
	}
	if got := producer.closeCount(); got != 1 {
		t.Fatalf("producer close calls = %d, want exactly one across both Close attempts", got)
	}
	if got := conn.closeCount(); got != 1 {
		t.Fatalf("connection close calls = %d, want exactly one across both Close attempts", got)
	}
	client.mu.Lock()
	finished = client.closed
	client.mu.Unlock()
	if !finished {
		t.Fatal("the completed retry left the client open")
	}
}
