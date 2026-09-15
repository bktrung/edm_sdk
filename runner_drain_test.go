package f1

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
)

// drainProbeConsumer records how a runner released it, and what the release
// context looked like at that moment. The context is the subject: the release
// that follows a failed wait must not inherit the cancellation or the expired
// deadline that ended the wait, so a test needs to see ctx.Err() inside Stop.
type drainProbeConsumer struct {
	mu          sync.Mutex
	stops       int
	releases    int
	stopContext error
	stopErr     error
	stopStarted chan struct{}
	stopRelease chan struct{}
	// stopHonoursContext makes Stop return only once its context is done, so a
	// test can tell a release that inherits the settlement context from one
	// that runs detached.
	stopHonoursContext bool
}

func newDrainProbeConsumer() *drainProbeConsumer { return &drainProbeConsumer{} }

func (c *drainProbeConsumer) Messages() <-chan driver.InboundMessage { return nil }
func (c *drainProbeConsumer) Errors() <-chan error                   { return nil }
func (*drainProbeConsumer) Pause(...string) error                    { return nil }
func (*drainProbeConsumer) Resume(...string) error                   { return nil }
func (*drainProbeConsumer) Drain(context.Context) error              { return nil }
func (*drainProbeConsumer) Lag(context.Context) (map[string]int64, error) {
	return nil, driver.ErrUnsupported
}

func (c *drainProbeConsumer) Stop(ctx context.Context) error {
	c.mu.Lock()
	c.stops++
	started := c.stopStarted
	release := c.stopRelease
	err := c.stopErr
	honourContext := c.stopHonoursContext
	c.mu.Unlock()
	if started != nil {
		close(started)
	}
	if honourContext {
		<-ctx.Done()
	}
	if release != nil {
		<-release
	}
	c.mu.Lock()
	c.stopContext = ctx.Err()
	c.mu.Unlock()
	return err
}

func (c *drainProbeConsumer) Release(context.Context) error {
	c.mu.Lock()
	c.releases++
	c.mu.Unlock()
	return nil
}

func (c *drainProbeConsumer) probes() (stops, releases int, stopContext error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stops, c.releases, c.stopContext
}

// newDrainRunner builds the smallest runner drainAfterRun can be handed: a
// client on a fake clock, an empty in-flight registry, and a consumer it can
// release. Nothing else in the runner is touched by the drain, so a test can
// exercise it without a worker, a driver or a broker. The machine starts in
// Starting unless ready is set.
func newDrainRunner(t *testing.T, consumer driver.Consumer, ready bool) (*Runner, *Client, *clock.Fake) {
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
		client:       client,
		subscription: Subscription{Name: "orders", Topics: []string{"orders.created"}, Priorities: []Priority{PriorityHigh}},
		inflight:     newInflightRegistry(),
		lifecycle:    lifecycle.New(),
		consumer:     consumer,
	}
	if ready {
		if err := runner.lifecycle.Transition(lifecycle.Ready); err != nil {
			t.Fatal(err)
		}
	}
	return runner, client, fake
}

// TestDrainAfterRunReleasesThenCloses pins the ordinary path: nothing is in
// flight, so the wait returns at once, the release runs on the settlement
// context itself, and the runner ends Closed.
func TestDrainAfterRunReleasesThenCloses(t *testing.T) {
	consumer := newDrainProbeConsumer()
	runner, _, _ := newDrainRunner(t, consumer, true)
	if err := runner.drainAfterRun(context.Background()); err != nil {
		t.Fatalf("drainAfterRun() = %v, want nil", err)
	}
	stops, releases, stopContext := consumer.probes()
	if stops != 1 || releases != 0 {
		t.Fatalf("consumer probes = %d stops, %d releases, want 1 stop and no release", stops, releases)
	}
	if stopContext != nil {
		t.Fatalf("release context error = %v, want a live context", stopContext)
	}
	if got := runner.lifecycle.State(); got != lifecycle.Closed {
		t.Fatalf("state after an ordinary drain = %s, want closed", got)
	}
}

// TestDrainAfterRunGivesInFlightWorkItsGraceBudget pins that the release is
// held back by the wait. With a delivery still in flight and the drain budget
// unspent, the consumer must not be released; it is released as soon as the
// delivery settles. A drain that releases first turns this red.
func TestDrainAfterRunGivesInFlightWorkItsGraceBudget(t *testing.T) {
	consumer := newDrainProbeConsumer()
	runner, client, _ := newDrainRunner(t, consumer, true)
	client.config.Lifecycle.DrainTimeout = time.Minute
	id := runner.inflight.Add(driver.InboundMessage{})

	done := make(chan error, 1)
	go func() { done <- runner.drainAfterRun(context.Background()) }()

	grace := clock.NewReal().Timer(100 * time.Millisecond)
	defer grace.Stop()
	select {
	case <-grace.C:
	case err := <-done:
		t.Fatalf("drainAfterRun() = %v while a delivery was still in flight", err)
	}
	if stops, _, _ := consumer.probes(); stops != 0 {
		t.Fatal("consumer was released while a delivery was still in flight")
	}

	runner.inflight.Remove(id)
	watchdog := clock.NewReal().Timer(2 * time.Second)
	defer watchdog.Stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("drainAfterRun() = %v, want nil once the delivery settled", err)
		}
	case <-watchdog.C:
		t.Fatal("drainAfterRun did not return after the last delivery settled")
	}
	if got := runner.lifecycle.State(); got != lifecycle.Closed {
		t.Fatalf("state after the delivery settled = %s, want closed", got)
	}
}

// TestDrainAfterRunStillReleasesWhenTheWaitTimesOut pins the abort path: the
// release runs even though the wait failed, on a context that does not inherit
// the expired settlement context, and the runner ends Aborted.
func TestDrainAfterRunStillReleasesWhenTheWaitTimesOut(t *testing.T) {
	consumer := newDrainProbeConsumer()
	runner, client, fake := newDrainRunner(t, consumer, true)
	client.config.Lifecycle.DrainTimeout = 5 * time.Second
	client.config.Lifecycle.CloseTimeout = time.Minute
	runner.inflight.Add(driver.InboundMessage{})

	done := make(chan error, 1)
	go func() { done <- runner.drainAfterRun(context.Background()) }()
	waitForFakeTimer(t, fake)
	fake.Advance(5 * time.Second)

	watchdog := clock.NewReal().Timer(2 * time.Second)
	defer watchdog.Stop()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "drain phase") {
			t.Fatalf("drainAfterRun() error = %v, want the drain phase deadline", err)
		}
	case <-watchdog.C:
		t.Fatal("drainAfterRun did not return after the drain budget expired")
	}
	stops, _, stopContext := consumer.probes()
	if stops != 1 {
		t.Fatalf("consumer stops = %d, want the release to run after a failed wait", stops)
	}
	if stopContext != nil {
		t.Fatalf("release context error = %v, want the release to outlive the expired wait", stopContext)
	}
	if got := runner.lifecycle.State(); got != lifecycle.Aborted {
		t.Fatalf("state after a failed wait = %s, want aborted", got)
	}
}

// TestDrainAfterRunReleaseOutlivesACancelledParent pins the interleaving this
// path exists for: the settlement context is cancelled while a delivery is
// still in flight, so the wait fails, and the release still runs on a context
// that is not already done. Running the release on the cancelled settlement
// context instead turns this red, because the release inherits that
// cancellation and gives the broker back nothing.
func TestDrainAfterRunReleaseOutlivesACancelledParent(t *testing.T) {
	consumer := newDrainProbeConsumer()
	runner, client, _ := newDrainRunner(t, consumer, true)
	client.config.Lifecycle.DrainTimeout = time.Minute
	client.config.Lifecycle.CloseTimeout = time.Minute
	runner.inflight.Add(driver.InboundMessage{})

	// The drain waits on the runner's own settlement context, which Run cancels
	// when the runner is torn down.
	settlementCtx, cancelSettlement := context.WithCancel(context.Background())
	runner.settleCtx = settlementCtx
	done := make(chan error, 1)
	go func() { done <- runner.drainAfterRun(context.Background()) }()
	cancelSettlement()

	watchdog := clock.NewReal().Timer(2 * time.Second)
	defer watchdog.Stop()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("drainAfterRun() error = %v, want context canceled", err)
		}
	case <-watchdog.C:
		t.Fatal("drainAfterRun did not return after the settlement context was cancelled")
	}
	stops, _, stopContext := consumer.probes()
	if stops != 1 {
		t.Fatalf("consumer stops = %d, want the release to run after a cancelled wait", stops)
	}
	if stopContext != nil {
		t.Fatalf("release context error = %v, want a context detached from the cancelled parent", stopContext)
	}
	if got := runner.lifecycle.State(); got != lifecycle.Aborted {
		t.Fatalf("state after a cancelled wait = %s, want aborted", got)
	}
}

// TestDrainAfterRunSuccessPathReleaseHonoursTheSettlementContext pins the other
// half of the context rule. A release that follows a successful wait runs on
// the settlement context itself, so the cancellation that tears the runner down
// also ends a release still in progress. Only the failure path detaches, and it
// does so because its wait already ended on that same cancellation.
func TestDrainAfterRunSuccessPathReleaseHonoursTheSettlementContext(t *testing.T) {
	consumer := newDrainProbeConsumer()
	consumer.stopStarted = make(chan struct{})
	consumer.stopHonoursContext = true
	runner, client, _ := newDrainRunner(t, consumer, true)
	// Zero budgets run the release on the context itself rather than on a
	// timer-guarded copy of it, which is the shape this test needs to observe.
	client.config.Lifecycle.DrainTimeout = 0
	client.config.Lifecycle.CloseTimeout = 0

	settlementCtx, cancelSettlement := context.WithCancel(context.Background())
	runner.settleCtx = settlementCtx
	done := make(chan error, 1)
	go func() { done <- runner.drainAfterRun(context.Background()) }()
	<-consumer.stopStarted
	cancelSettlement()

	watchdog := clock.NewReal().Timer(2 * time.Second)
	defer watchdog.Stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("drainAfterRun() = %v, want nil", err)
		}
	case <-watchdog.C:
		t.Fatal("the release never saw the settlement context end: it ran on a detached context")
	}
	if _, _, stopContext := consumer.probes(); !errors.Is(stopContext, context.Canceled) {
		t.Fatalf("release context error = %v, want context canceled", stopContext)
	}
	if got := runner.lifecycle.State(); got != lifecycle.Closed {
		t.Fatalf("state after a successful drain = %s, want closed", got)
	}
}

// TestDrainAfterRunBoundsTheReleaseWithCloseTimeout pins the second budget: a
// release that never returns ends at CloseTimeout and leaves the runner
// Aborted rather than holding the drain open.
func TestDrainAfterRunBoundsTheReleaseWithCloseTimeout(t *testing.T) {
	consumer := newDrainProbeConsumer()
	consumer.stopStarted = make(chan struct{})
	consumer.stopRelease = make(chan struct{})
	defer close(consumer.stopRelease)
	runner, client, fake := newDrainRunner(t, consumer, true)
	client.config.Lifecycle.CloseTimeout = 5 * time.Second

	done := make(chan error, 1)
	go func() { done <- runner.drainAfterRun(context.Background()) }()
	<-consumer.stopStarted
	waitForFakeTimer(t, fake)
	fake.Advance(5 * time.Second)

	watchdog := clock.NewReal().Timer(2 * time.Second)
	defer watchdog.Stop()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "close phase") {
			t.Fatalf("drainAfterRun() error = %v, want the close phase deadline", err)
		}
	case <-watchdog.C:
		t.Fatal("drainAfterRun did not return after the close budget expired")
	}
	if got := runner.lifecycle.State(); got != lifecycle.Aborted {
		t.Fatalf("state after a bounded release = %s, want aborted", got)
	}
}

// TestDrainAfterRunReleaseErrorAborts pins that a release failure reaches the
// caller and leaves the runner Aborted, so a retry and an operator both see
// that the consumer was not handed back cleanly.
func TestDrainAfterRunReleaseErrorAborts(t *testing.T) {
	releaseErr := errors.New("consumer release failed")
	consumer := newDrainProbeConsumer()
	consumer.stopErr = releaseErr
	runner, _, _ := newDrainRunner(t, consumer, true)

	err := runner.drainAfterRun(context.Background())
	if !errors.Is(err, releaseErr) {
		t.Fatalf("drainAfterRun() error = %v, want %v", err, releaseErr)
	}
	if got := runner.lifecycle.State(); got != lifecycle.Aborted {
		t.Fatalf("state after a failed release = %s, want aborted", got)
	}
}

// TestDrainAfterRunRejectsNegativeDrainTimeout pins what a negative budget
// means: runWithClockTimeout refuses it before the wait starts, the drain
// treats that as a failed wait, still releases, and ends Aborted. The wait
// itself must never run: the in-flight delivery here never settles, so a drain
// that reaches WaitZero with the settlement context cannot return at all.
func TestDrainAfterRunRejectsNegativeDrainTimeout(t *testing.T) {
	consumer := newDrainProbeConsumer()
	runner, client, _ := newDrainRunner(t, consumer, true)
	client.config.Lifecycle.DrainTimeout = -time.Second
	runner.inflight.Add(driver.InboundMessage{})

	done := make(chan error, 1)
	go func() { done <- runner.drainAfterRun(context.Background()) }()

	watchdog := clock.NewReal().Timer(2 * time.Second)
	defer watchdog.Stop()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "must not be negative") {
			t.Fatalf("drainAfterRun() error = %v, want the negative-budget rejection", err)
		}
	case <-watchdog.C:
		t.Fatal("drainAfterRun did not return: the in-flight wait ran under a rejected negative budget")
	}
	stops, _, _ := consumer.probes()
	if stops != 1 {
		t.Fatalf("consumer stops = %d, want the release to run after a rejected budget", stops)
	}
	if got := runner.lifecycle.State(); got != lifecycle.Aborted {
		t.Fatalf("state after a rejected budget = %s, want aborted", got)
	}
}

// TestDrainAfterRunStartStates pins the states a drain may start from. Ready
// and Reconnecting move to Draining, Draining is accepted as a start, Failed
// is left alone, and anything else is refused before the wait or the release
// runs.
func TestDrainAfterRunStartStates(t *testing.T) {
	cases := []struct {
		name      string
		from      lifecycle.State
		wantErr   bool
		wantState lifecycle.State
	}{
		{name: "ready", from: lifecycle.Ready, wantState: lifecycle.Closed},
		{name: "reconnecting", from: lifecycle.Reconnecting, wantState: lifecycle.Closed},
		{name: "draining", from: lifecycle.Draining, wantState: lifecycle.Closed},
		{name: "failed", from: lifecycle.Failed, wantState: lifecycle.Failed},
		{name: "starting", from: lifecycle.Starting, wantErr: true},
		{name: "closed", from: lifecycle.Closed, wantErr: true},
		{name: "aborted", from: lifecycle.Aborted, wantErr: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			consumer := newDrainProbeConsumer()
			runner, _, _ := newDrainRunner(t, consumer, false)
			reachDrainStart(t, runner.lifecycle, test.from)

			err := runner.drainAfterRun(context.Background())
			if test.wantErr {
				if err == nil {
					t.Fatalf("drainAfterRun() from %s = nil, want a refused start", test.from)
				}
				if stops, releases, _ := consumer.probes(); stops != 0 || releases != 0 {
					t.Fatalf("consumer probes = %d stops, %d releases after a refused start, want none", stops, releases)
				}
				return
			}
			if err != nil {
				t.Fatalf("drainAfterRun() from %s = %v, want nil", test.from, err)
			}
			if got := runner.lifecycle.State(); got != test.wantState {
				t.Fatalf("state after draining from %s = %s, want %s", test.from, got, test.wantState)
			}
			if stops, _, _ := consumer.probes(); stops != 1 {
				t.Fatalf("consumer stops = %d after draining from %s, want 1", stops, test.from)
			}
		})
	}
}

// reachDrainStart walks a fresh machine to the state a case drains from, along
// transitions the table permits.
func reachDrainStart(t *testing.T, machine *lifecycle.Machine, from lifecycle.State) {
	t.Helper()
	if from == lifecycle.Starting {
		return
	}
	if err := machine.Transition(lifecycle.Ready); err != nil {
		t.Fatal(err)
	}
	switch from {
	case lifecycle.Ready:
	case lifecycle.Reconnecting, lifecycle.Draining, lifecycle.Failed, lifecycle.Aborted:
		if err := machine.Transition(from); err != nil {
			t.Fatal(err)
		}
	case lifecycle.Closed:
		if err := machine.Transition(lifecycle.Draining); err != nil {
			t.Fatal(err)
		}
		if err := machine.Transition(lifecycle.Closed); err != nil {
			t.Fatal(err)
		}
	}
}
