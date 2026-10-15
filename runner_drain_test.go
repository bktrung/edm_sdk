package f1

import (
	"context"
	"errors"
	"fmt"
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
	mu            sync.Mutex
	stops         int
	releases      int
	stopContext   error
	stopErr       error
	stopStarted   chan struct{}
	stopRelease   chan struct{}
	stopCompleted bool
	// releaseErr, releaseStarted, releaseRelease and releaseCompleted are the
	// same probes for Release: a test can hold the release inside the driver,
	// see that it was entered, make it fail, and know when it returned.
	releaseErr       error
	releaseStarted   chan struct{}
	releaseRelease   chan struct{}
	releaseCompleted bool
	releaseOnce      sync.Once
	messages         chan driver.InboundMessage
	errs             chan error
	// stopHonoursContext makes Stop return only once its context is done, so a
	// test can tell a release that inherits the settlement context from one
	// that runs detached.
	stopHonoursContext bool
}

func newDrainProbeConsumer() *drainProbeConsumer { return &drainProbeConsumer{} }

func (c *drainProbeConsumer) Messages() <-chan driver.InboundMessage { return c.messages }
func (c *drainProbeConsumer) Errors() <-chan error                   { return c.errs }
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
	c.stopCompleted = true
	c.mu.Unlock()
	return err
}

func (c *drainProbeConsumer) Release(context.Context) error {
	c.mu.Lock()
	c.releases++
	started := c.releaseStarted
	release := c.releaseRelease
	err := c.releaseErr
	c.mu.Unlock()
	if started != nil {
		c.releaseOnce.Do(func() { close(started) })
	}
	if release != nil {
		<-release
	}
	c.mu.Lock()
	c.releaseCompleted = true
	c.mu.Unlock()
	return err
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
	id := runner.inflight.Add()

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
	runner.inflight.Add()

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
// path exists for: the context the wait runs on is cancelled while a delivery
// is still in flight, so the wait fails, and the release still runs on a
// context that is not already done. Running the release on the cancelled
// context instead turns this red, because the release inherits that
// cancellation and gives the broker back nothing.
func TestDrainAfterRunReleaseOutlivesACancelledParent(t *testing.T) {
	consumer := newDrainProbeConsumer()
	runner, client, fake := newDrainRunner(t, consumer, true)
	client.config.Lifecycle.DrainTimeout = time.Minute
	client.config.Lifecycle.CloseTimeout = time.Minute
	runner.inflight.Add()

	// The wait runs on the runner's settlement window, which is the context the
	// runner's own teardown cancels. A caller's context that is already
	// finished is what a settlement is rescued from, so this test cancels the
	// window itself, while the delivery is still in flight.
	dead, cancelDead := context.WithCancel(context.Background())
	cancelDead()
	window := runnerSettlementContext(runner, dead)
	runner.mu.Lock()
	cancelWindow := runner.settleCancel
	runner.mu.Unlock()
	if cancelWindow == nil {
		t.Fatal("the runner installed no settlement window")
	}
	done := make(chan error, 1)
	go func() { done <- runner.drainAfterRun(context.Background()) }()
	waitForFakeTimer(t, fake)

	cancelWindow()

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
	if window.Err() != context.Canceled {
		t.Fatalf("the context the wait ran on ended with %v, want it cancelled under the wait", window.Err())
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
// the context the wait ran on, so the cancellation that tears the runner down
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

	// The drain runs on the generation context, which this test cancels while
	// the release is in progress. Zero budgets run the release on that context
	// itself rather than on a timer-guarded copy of it, which is the shape this
	// test needs to observe.
	runCtx, cancelRun := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.drainAfterRun(runCtx) }()
	<-consumer.stopStarted
	cancelRun()

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
	runner.inflight.Add()

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

// rebuildDrainContext holds the rebuild waiter before it can report, allowing
// Drain's event to reach the owner first without relying on goroutine scheduling.
type rebuildDrainContext struct {
	context.Context
	ready   func() bool
	entered chan struct{}
	resume  chan struct{}
	unblock func()
	once    sync.Once
}

func (c *rebuildDrainContext) Err() error {
	if c.ready() {
		c.once.Do(func() {
			close(c.entered)
			<-c.resume
		})
	}
	return c.Context.Err()
}

// rebuildDrainObserver holds Drain between closing drainStarted and sending its
// owner event, so the rebuilt report necessarily reaches the owner first.
type rebuildDrainObserver struct {
	entered chan struct{}
	resume  chan struct{}
}

func (o *rebuildDrainObserver) Start(ctx context.Context, event StartEvent) (context.Context, Token) {
	if event.Kind == ObserverDrain {
		close(o.entered)
		<-o.resume
	}
	return ctx, Token{Kind: event.Kind}
}

func (*rebuildDrainObserver) Finish(Token, FinishEvent) {}
func (*rebuildDrainObserver) Record(PointEvent)         {}

type rebuildDrainDriver struct {
	dispatchDriver
	conn driver.Conn
}

func (d *rebuildDrainDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return d.conn, nil
}

type rebuildDrainConn struct {
	dispatchConn
	open func(context.Context) (driver.Consumer, error)
}

func (c *rebuildDrainConn) Consumer(ctx context.Context, _ driver.ConsumerConfig) (driver.Consumer, error) {
	return c.open(ctx)
}

func drainTestGate(t *testing.T) (chan struct{}, func()) {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	return gate, release
}

// startRebuildDrainRunner uses real Run/Drain ownership with a manually pending
// reconnect. No supervisor runs this synthetic attempt: otherwise its abandon
// could release the consumer before the test reaches the held-consumer exit.
func startRebuildDrainRunner(t *testing.T, probe *drainProbeConsumer, observer Observer, open func(context.Context) (driver.Consumer, error)) (*Runner, *Client, *rebuildDrainContext, <-chan error, <-chan struct{}) {
	t.Helper()
	conn := &rebuildDrainConn{
		dispatchConn: dispatchConn{producer: &dispatchProducer{}, admin: &dispatchAdmin{}},
		open:         open,
	}
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&rebuildDrainDriver{conn: conn}), WithObserver(observer),
		withClock(clock.NewFake(time.Unix(0, 0))))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelClose()
		_ = client.Close(closeCtx)
	})
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name: "orders", Topics: []string{"orders.created"}, Concurrency: 1,
		Handlers: map[string]Handler{"orders.created": HandlerFunc(func(context.Context, *Event) error { return nil })},
	})
	if err != nil {
		t.Fatal(err)
	}
	base, cancel := context.WithCancel(context.Background())
	resume, release := drainTestGate(t)
	ctx := &rebuildDrainContext{
		Context: base, entered: make(chan struct{}), resume: resume, unblock: release,
		ready: func() bool { return runnerState(runner) == lifecycle.Reconnecting },
	}
	result := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		err := runner.Run(ctx)
		probe.mu.Lock()
		completed := probe.stopCompleted
		probe.mu.Unlock()
		if probe.stopStarted != nil && !completed {
			err = errors.Join(err, errors.New("Run returned before consumer Stop completed"))
		}
		result <- err
		close(finished)
	}()
	t.Cleanup(func() {
		release()
		cancel()
		waitForSignal(t, finished, "finished")
	})
	return runner, client, ctx, result, finished
}

func TestRunnerDrainDuringHeldConsumerRebuildWaitJoinsTerminalDrain(t *testing.T) {
	for _, order := range []string{"rebuilt-first", "drain-first"} {
		t.Run(order, func(t *testing.T) {
			probe := newDrainProbeConsumer()
			probe.errs = make(chan error, 1)
			probe.stopStarted = make(chan struct{})
			observerResume, releaseObserver := drainTestGate(t)
			observer := &rebuildDrainObserver{entered: make(chan struct{}), resume: observerResume}
			runner, client, ctx, result, finished := startRebuildDrainRunner(t, probe, observer,
				func(context.Context) (driver.Consumer, error) { return probe, nil })
			t.Cleanup(releaseObserver)
			stopResume, releaseStop := drainTestGate(t)
			probe.mu.Lock()
			probe.stopRelease = stopResume
			probe.mu.Unlock()
			waitReconnectCondition(t, func() bool { return runnerState(runner) == lifecycle.Ready })
			client.mu.Lock()
			client.conn = connReconnecting
			client.mu.Unlock()
			probe.errs <- &driver.Error{Driver: "inmem", Op: "consumer", K: driver.KindTransient, Err: errors.New("generation failed")}
			waitForSignal(t, ctx.entered, "ctx.entered")
			drained := make(chan error, 1)
			go func() { drained <- runner.Drain(context.Background()) }()
			waitForSignal(t, observer.entered, "observer.entered")
			if order == "drain-first" {
				releaseObserver()
				// The owner installs the window while handling Drain, before
				// launching teardown. Resume rebuild without waiting for Stop,
				// so joining drain can overlap the teardown goroutine's start.
				waitReconnectCondition(t, func() bool {
					runner.mu.Lock()
					defer runner.mu.Unlock()
					return runner.settleCtx != nil
				})
			}
			ctx.unblock()
			timer := clock.NewReal().Timer(2 * time.Second)
			defer timer.Stop()
			select {
			case <-probe.stopStarted:
			case <-finished:
				t.Fatalf("Run ended before terminal drain: %v", <-result)
			case <-timer.C:
				t.Fatal("terminal Stop did not start")
			}
			releaseStop()
			waitForSignal(t, finished, "finished")
			if err := <-result; err != nil {
				t.Fatalf("Run() = %v, want nil after completed Stop", err)
			}
			releaseObserver()
			if err := <-drained; err != nil {
				t.Fatalf("Drain() = %v, want nil", err)
			}
			if stops, _, stopContext := probe.probes(); stops != 1 || stopContext != nil {
				t.Fatalf("Stop probes = %d, %v, want one completed Stop on a live context", stops, stopContext)
			}
		})
	}
}

func TestRunnerDrainDuringRebuildAndAbandonSharesConsumerTeardown(t *testing.T) {
	probe := newDrainProbeConsumer()
	probe.errs = make(chan error, 1)
	observerResume, releaseObserver := drainTestGate(t)
	observer := &rebuildDrainObserver{entered: make(chan struct{}), resume: observerResume}
	runner, client, ctx, result, finished := startRebuildDrainRunner(t, probe, observer,
		func(context.Context) (driver.Consumer, error) { return probe, nil })
	t.Cleanup(releaseObserver)
	waitReconnectCondition(t, func() bool { return runnerState(runner) == lifecycle.Ready })
	client.mu.Lock()
	client.conn = connReconnecting
	client.mu.Unlock()
	probe.errs <- &driver.Error{Driver: "inmem", Op: "consumer", K: driver.KindTransient, Err: errors.New("generation failed")}
	waitForSignal(t, ctx.entered, "ctx.entered")
	// The abandon reaches the consumer first, so it owns the one teardown. The
	// terminal drain that follows joins it instead of stopping a consumer the
	// supervisor has already handed back to the connection it is leaving.
	if err := runner.abandonForReconnect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stops, releases, _ := probe.probes(); stops != 0 || releases != 1 {
		t.Fatalf("teardown calls after abandon = (%d stops, %d releases), want the abandon's one Release", stops, releases)
	}
	drained := make(chan error, 1)
	go func() { drained <- runner.Drain(context.Background()) }()
	waitForSignal(t, observer.entered, "observer.entered")
	ctx.unblock()
	waitForSignal(t, finished, "finished")
	if err := <-result; err != nil {
		t.Fatalf("Run() = %v, want nil after abandon and terminal drain", err)
	}
	releaseObserver()
	if err := <-drained; err != nil {
		t.Fatalf("Drain() = %v, want the abandon's result", err)
	}
	if stops, releases, _ := probe.probes(); stops != 0 || releases != 1 {
		t.Fatalf("teardown calls = (%d stops, %d releases), want exactly one Release and no Stop", stops, releases)
	}
	if got := runnerState(runner); got != lifecycle.Closed {
		t.Fatalf("state = %s, want closed after joining the abandon", got)
	}
}

func TestRunnerDrainDuringFailedOpenRebuildWaitDoesNotRelease(t *testing.T) {
	for _, failure := range []string{"abandoned-open", "transient-open"} {
		t.Run(failure, func(t *testing.T) {
			probe := newDrainProbeConsumer()
			probe.errs = make(chan error, 1)
			opened := make(chan struct{})
			openResume, releaseOpen := drainTestGate(t)
			observerResume, releaseObserver := drainTestGate(t)
			observer := &rebuildDrainObserver{entered: make(chan struct{}), resume: observerResume}
			opens := 0
			runner, client, ctx, result, finished := startRebuildDrainRunner(t, probe, observer,
				func(openCtx context.Context) (driver.Consumer, error) {
					opens++
					if opens == 1 {
						return probe, nil
					}
					close(opened)
					<-openResume
					if failure == "abandoned-open" {
						return nil, openCtx.Err()
					}
					return nil, &driver.Error{Driver: "inmem", Op: "consumer", K: driver.KindTransient, Err: errors.New("replacement open failed")}
				})
			t.Cleanup(func() {
				releaseOpen()
				releaseObserver()
			})
			waitReconnectCondition(t, func() bool { return runnerState(runner) == lifecycle.Ready })
			probe.errs <- &driver.Error{Driver: "inmem", Op: "consumer", K: driver.KindTransient, Err: errors.New("generation failed")}
			waitForSignal(t, opened, "opened")
			client.mu.Lock()
			client.conn = connReconnecting
			client.mu.Unlock()
			if failure == "abandoned-open" {
				if err := runner.abandonForReconnect(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			releaseOpen()
			waitForSignal(t, ctx.entered, "ctx.entered")
			stopsBefore, releasesBefore, _ := probe.probes()
			drained := make(chan error, 1)
			go func() { drained <- runner.Drain(context.Background()) }()
			// Gate rebuilt-first: the drain joins the teardown the earlier
			// repair Release already owns, whether it arrives before or after
			// Rebuilt does.
			waitForSignal(t, observer.entered, "observer.entered")
			ctx.unblock()
			waitForSignal(t, finished, "finished")
			if err := <-result; err != nil {
				t.Fatalf("Run() = %v, want nil after failed open", err)
			}
			releaseObserver()
			if err := <-drained; err != nil {
				t.Fatalf("Drain() = %v, want nil after failed open", err)
			}
			if stops, releases, _ := probe.probes(); stops != stopsBefore || releases != releasesBefore {
				t.Fatalf("teardown calls changed during failed-open wait: (%d, %d) -> (%d, %d)", stopsBefore, releasesBefore, stops, releases)
			}
		})
	}
}

func TestRunnerDrainDuringFailedOpenRebuildWaitJoinsTheOwningTeardown(t *testing.T) {
	for _, failure := range []string{"abandoned-open", "transient-open"} {
		t.Run(failure, func(t *testing.T) {
			probe := newDrainProbeConsumer()
			probe.errs = make(chan error, 1)
			opened := make(chan struct{})
			openResume, releaseOpen := drainTestGate(t)
			observerResume, releaseObserver := drainTestGate(t)
			observer := &rebuildDrainObserver{entered: make(chan struct{}), resume: observerResume}
			opens := 0
			runner, client, ctx, result, finished := startRebuildDrainRunner(t, probe, observer,
				func(openCtx context.Context) (driver.Consumer, error) {
					opens++
					if opens == 1 {
						return probe, nil
					}
					close(opened)
					<-openResume
					if failure == "abandoned-open" {
						return nil, openCtx.Err()
					}
					return nil, &driver.Error{Driver: "inmem", Op: "consumer", K: driver.KindTransient, Err: errors.New("replacement open failed")}
				})
			drainFinished := make(chan struct{})
			drainLaunched := false
			t.Cleanup(func() {
				releaseOpen()
				releaseObserver()
				ctx.unblock()
				waitForSignal(t, finished, "finished")
				if drainLaunched {
					waitForSignal(t, drainFinished, "drainFinished")
				}
				// A broken exit can leave DrainDone blocked after Run returns.
				// Only cleanup reads events then; it never competes with the owner.
				joined := make(chan struct{})
				go func() {
					_ = runner.asyncGroup.Wait()
					close(joined)
				}()
				for {
					select {
					case <-joined:
						return
					case <-runner.events:
					}
				}
			})
			waitReconnectCondition(t, func() bool { return runnerState(runner) == lifecycle.Ready })
			probe.errs <- &driver.Error{Driver: "inmem", Op: "consumer", K: driver.KindTransient, Err: errors.New("generation failed")}
			waitForSignal(t, opened, "opened")
			client.mu.Lock()
			client.conn = connReconnecting
			client.mu.Unlock()
			if failure == "abandoned-open" {
				if err := runner.abandonForReconnect(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			releaseOpen()
			waitForSignal(t, ctx.entered, "ctx.entered")
			stopsBefore, releasesBefore, _ := probe.probes()
			drained := make(chan error, 1)
			drainLaunched = true
			go func() {
				drained <- runner.Drain(context.Background())
				close(drainFinished)
			}()
			waitForSignal(t, observer.entered, "observer.entered")
			releaseObserver()
			// The owner installs the settlement window while handling Drain,
			// before it launches teardown, so waiting for it forces the drain
			// event to be handled before the rebuild waiter can report. Without
			// that, the failing open's drain-join exit would end Run first and
			// the terminal drain would never run at all.
			waitReconnectCondition(t, func() bool {
				runner.mu.Lock()
				defer runner.mu.Unlock()
				return runner.settleCtx != nil
			})
			// The terminal drain joins the teardown the repair release (or the
			// abandon, for the abandoned-open arm) already owns, so it never
			// reaches Stop.
			ctx.unblock()
			waitForSignal(t, finished, "finished")
			if err := <-result; err != nil {
				t.Fatalf("Run() = %v, want the owning teardown's result", err)
			}
			waitForSignal(t, drainFinished, "drainFinished")
			if err := <-drained; err != nil {
				t.Fatalf("Drain() = %v, want the owning teardown's result", err)
			}
			if stops, releases, _ := probe.probes(); stops != stopsBefore || releases != releasesBefore {
				t.Fatalf("teardown calls = (%d, %d), want the owning teardown's (%d, %d)", stops, releases, stopsBefore, releasesBefore)
			}
			if got := runnerState(runner); got != lifecycle.Closed {
				t.Fatalf("state = %s, want closed after joining the owning teardown", got)
			}
			// Drain has finished registering async work. In this fixture there
			// are no pending application callbacks; a stranded DrainDone sender
			// is therefore detected by joining the group, not counting goroutines.
			if err := runner.asyncGroup.Wait(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRunnerInitialOpenFailureCancelsGenerationContext(t *testing.T) {
	probe := newDrainProbeConsumer()
	openErr := errors.New("initial open failed")
	var generationCtx context.Context
	_, _, ctx, result, finished := startRebuildDrainRunner(t, probe, nil,
		func(openCtx context.Context) (driver.Consumer, error) {
			generationCtx = openCtx
			return nil, openErr
		})
	waitForSignal(t, finished, "finished")
	if err := <-result; !errors.Is(err, openErr) {
		t.Fatalf("Run() = %v, want %v", err, openErr)
	}
	if err := ctx.Context.Err(); err != nil {
		t.Fatalf("parent context error = %v, want nil", err)
	}
	if err := generationCtx.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("generation context error = %v, want canceled after Run returns", err)
	}
	select {
	case <-generationCtx.Done():
	default:
		t.Fatal("generation context Done remains open after Run returns")
	}
}

// The two results a claiming test can make a consumer produce. They are
// distinct so that a caller returning the owner's result is distinguishable
// from a caller returning its own.
var (
	errTeardownStopRefused    = errors.New("consumer stop refused")
	errTeardownReleaseRefused = errors.New("consumer release refused")
)

// sameTeardownResult fails unless both callers received the same result. A nil
// result is a result: the loser of a claim reports what the winner did, which
// is nil when the winner's teardown succeeded.
func sameTeardownResult(t *testing.T, first, second error) {
	t.Helper()
	if first == nil || second == nil {
		if first != nil || second != nil {
			t.Fatalf("teardown results = %v and %v, want the same result", first, second)
		}
		return
	}
	if !errors.Is(second, first) || !errors.Is(first, second) {
		t.Fatalf("teardown results = %v and %v, want the same result", first, second)
	}
}

// waitForTeardownResult returns the first result the channel carries, within
// the same real-time bound the suite's other waits use. It waits for a
// completion signal, not for a window in which nothing may arrive.
func waitForTeardownResult(t *testing.T, done <-chan error, what string) error {
	t.Helper()
	timer := clock.NewReal().Timer(2 * time.Second)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		t.Fatalf("%s did not return", what)
		return nil
	}
}

// waitForTeardownJoin waits until the loser has entered the owner's join,
// which the client clock shows as a second waiter for the loser's own bound on
// top of the Stop phase the owner holds. A loser that went to the driver
// itself returns from its call instead, and that is a failure here: the point
// of these tests is that it does not.
func waitForTeardownJoin(t *testing.T, fake *clock.Fake, loser <-chan error, what string) {
	t.Helper()
	timer := clock.NewReal().Timer(2 * time.Second)
	defer timer.Stop()
	ticker := clock.NewReal().Ticker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-loser:
			t.Fatalf("%s returned %v instead of joining the owner's held teardown", what, err)
		default:
		}
		if fake.NumWaiters() >= 2 {
			return
		}
		select {
		case <-timer.C:
			t.Fatalf("%s did not join the owner's held teardown (clock waiters = %d)", what, fake.NumWaiters())
		case <-ticker.C:
		}
	}
}

// TestConsumerTeardownIsOwnedByWhicheverCallerArrivesFirst pins the ownership
// rule in both arrival orders. Whichever of the drain's Stop and the abandon's
// Release reaches the consumer first runs the sequence; the other joins it
// under its own budget and reports the same result. The driver is asked to
// tear the consumer down once, not twice, whichever order the two arrive in.
func TestConsumerTeardownIsOwnedByWhicheverCallerArrivesFirst(t *testing.T) {
	t.Run("drain-first", func(t *testing.T) {
		probe := newDrainProbeConsumer()
		runner, _, fake := newDrainRunner(t, probe, true)
		// The gates are registered after the client's own cleanup, so LIFO
		// releases them before the client is closed: a held release would
		// otherwise block the close's kept-consumer retry forever.
		probe.stopStarted = make(chan struct{})
		stopResume, releaseStop := drainTestGate(t)
		probe.mu.Lock()
		probe.stopRelease = stopResume
		probe.stopErr = errTeardownStopRefused
		probe.mu.Unlock()

		drained := make(chan error, 1)
		go func() { drained <- runner.drainAfterRun(context.Background()) }()
		waitForSignal(t, probe.stopStarted, "stopStarted")

		abandoned := make(chan error, 1)
		go func() { abandoned <- runner.abandonForReconnect(context.Background()) }()
		// The abandon joins the held teardown, so it owes the client's clock a
		// second waiter for its own bound on top of the Stop phase the drain
		// holds. A loser that reached the driver itself would return instead.
		waitForTeardownJoin(t, fake, abandoned, "abandonForReconnect")

		releaseStop()
		drainedErr := waitForTeardownResult(t, drained, "drainAfterRun")
		abandonedErr := waitForTeardownResult(t, abandoned, "abandonForReconnect")
		if !errors.Is(drainedErr, errTeardownStopRefused) {
			t.Fatalf("drainAfterRun() = %v, want the owner's Stop refusal", drainedErr)
		}
		sameTeardownResult(t, drainedErr, abandonedErr)
		if stops, releases, _ := probe.probes(); stops != 1 || releases != 1 {
			t.Fatalf("teardown calls = (%d stops, %d releases), want the owner's one Stop and one Release", stops, releases)
		}
	})

	t.Run("abandon-first", func(t *testing.T) {
		probe := newDrainProbeConsumer()
		runner, client, fake := newDrainRunner(t, probe, true)
		// Registered after the client's cleanup so LIFO releases the held
		// Release before the close, which would otherwise block in it.
		probe.releaseStarted = make(chan struct{})
		releaseResume, releaseRelease := drainTestGate(t)
		probe.mu.Lock()
		probe.releaseRelease = releaseResume
		probe.releaseErr = errTeardownReleaseRefused
		probe.mu.Unlock()

		abandoned := make(chan error, 1)
		go func() { abandoned <- runner.abandonForReconnect(context.Background()) }()
		waitForSignal(t, probe.releaseStarted, "releaseStarted")

		drained := make(chan error, 1)
		go func() { drained <- runner.drainAfterRun(context.Background()) }()
		// The drain joins the held release instead of stopping the consumer the
		// abandon is handing back, so the driver sees no Stop at all.
		waitForTeardownJoin(t, fake, drained, "drainAfterRun")

		releaseRelease()
		abandonedErr := waitForTeardownResult(t, abandoned, "abandonForReconnect")
		drainedErr := waitForTeardownResult(t, drained, "drainAfterRun")
		if !errors.Is(abandonedErr, errTeardownReleaseRefused) {
			t.Fatalf("abandonForReconnect() = %v, want the owner's Release refusal", abandonedErr)
		}
		sameTeardownResult(t, abandonedErr, drainedErr)
		if stops, releases, _ := probe.probes(); stops != 0 || releases != 1 {
			t.Fatalf("teardown calls = (%d stops, %d releases), want the owner's one Release and no Stop", stops, releases)
		}
		if got := runnerState(runner); got != lifecycle.Aborted {
			t.Fatalf("state = %s, want aborted after the owner's failed Release", got)
		}
		// A failed release stays owned by its epoch, exactly once: the loser
		// joins the result and records nothing of its own.
		runner.mu.Lock()
		epoch := runner.consumerEpoch
		runner.mu.Unlock()
		client.mu.Lock()
		kept := append([]unreleasedConsumer(nil), client.unreleasedConsumers...)
		client.mu.Unlock()
		if len(kept) != 1 || kept[0].consumer != probe || kept[0].epoch != epoch {
			t.Fatalf("kept consumers = %+v, want the owner's one entry for epoch %d", kept, epoch)
		}
	})
}

// TestConsumerTeardownLoserWaitsWithinItsOwnBudget pins the loser's bound. The
// owner's Stop and the loser's join both run on the client's clock, so one
// advance reaches both budgets. The owner's Release leg is held too, so the
// owner cannot finish while the loser's own bound expires: the loser reports
// its own phase deadline, the driver sees only the owner's sequence, and the
// owner's result stays installed for the next caller.
func TestConsumerTeardownLoserWaitsWithinItsOwnBudget(t *testing.T) {
	probe := newDrainProbeConsumer()
	runner, _, fake := newDrainRunner(t, probe, true)
	// Both gates are registered after the client's cleanup, so LIFO releases
	// them before the close: a held release would otherwise block the close's
	// kept-consumer retry on a clock the test no longer advances.
	probe.stopStarted = make(chan struct{})
	stopResume, _ := drainTestGate(t)
	probe.releaseStarted = make(chan struct{})
	releaseResume, releaseRelease := drainTestGate(t)
	probe.mu.Lock()
	probe.stopRelease = stopResume
	probe.releaseRelease = releaseResume
	probe.mu.Unlock()

	drained := make(chan error, 1)
	go func() { drained <- runner.drainAfterRun(context.Background()) }()
	waitForSignal(t, probe.stopStarted, "stopStarted")

	abandoned := make(chan error, 1)
	go func() { abandoned <- runner.abandonForReconnect(context.Background()) }()
	waitForTeardownJoin(t, fake, abandoned, "abandonForReconnect")
	fake.Advance(runner.client.config.Lifecycle.CloseTimeout)

	abandonedErr := waitForTeardownResult(t, abandoned, "abandonForReconnect")
	if abandonedErr == nil || !strings.Contains(abandonedErr.Error(), "consumer teardown phase timed out") {
		t.Fatalf("abandonForReconnect() = %v, want its own phase deadline", abandonedErr)
	}
	// The owner spent its Stop budget and reached its Release leg, which is
	// held here. Its totals are therefore exactly its own sequence, and that is
	// what says the loser made no driver call of its own.
	waitForSignal(t, probe.releaseStarted, "releaseStarted")
	probe.mu.Lock()
	completed := probe.stopCompleted
	probe.mu.Unlock()
	if completed {
		t.Fatal("the held Stop returned, want the owner still inside the driver")
	}
	if stops, releases, _ := probe.probes(); stops != 1 || releases != 1 {
		t.Fatalf("teardown calls = (%d stops, %d releases), want the owner's Stop and its Release leg", stops, releases)
	}
	select {
	case err := <-drained:
		t.Fatalf("drainAfterRun() = %v while the owner was still inside its release", err)
	default:
	}

	// Letting the owner finish publishes its result, and a caller arriving now
	// joins that installed result instead of starting a second sequence.
	releaseRelease()
	drainedErr := waitForTeardownResult(t, drained, "drainAfterRun")
	if drainedErr == nil || !strings.Contains(drainedErr.Error(), "shutdown close phase timed out") {
		t.Fatalf("drainAfterRun() = %v, want the owner's Stop deadline", drainedErr)
	}
	if err := releaseRunnerConsumer(runner, context.Background()); !errors.Is(err, drainedErr) {
		t.Fatalf("releaseRunnerConsumer() = %v, want the owner's installed result %v", err, drainedErr)
	}
	if stops, releases, _ := probe.probes(); stops != 1 || releases != 1 {
		t.Fatalf("teardown calls after a joined retry = (%d stops, %d releases), want the owner's", stops, releases)
	}
}

// TestConcurrentConsumerTeardownsLeaveOneSequence starts the drain and the
// abandon together, repeatedly, and pins the invariant for whichever order
// wins: exactly one driver sequence. Both results are compared, because the
// loser has to report the winner's, and the kept-consumer entry has to be the
// winner's single one.
func TestConcurrentConsumerTeardownsLeaveOneSequence(t *testing.T) {
	const rounds = 25
	for round := range rounds {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			probe := newDrainProbeConsumer()
			probe.stopErr = errTeardownStopRefused
			probe.releaseErr = errTeardownReleaseRefused
			runner, client, _ := newDrainRunner(t, probe, true)

			start := make(chan struct{})
			drained := make(chan error, 1)
			abandoned := make(chan error, 1)
			go func() { <-start; drained <- runner.drainAfterRun(context.Background()) }()
			go func() { <-start; abandoned <- runner.abandonForReconnect(context.Background()) }()
			close(start)

			drainedErr := waitForTeardownResult(t, drained, "drainAfterRun")
			abandonedErr := waitForTeardownResult(t, abandoned, "abandonForReconnect")
			sameTeardownResult(t, drainedErr, abandonedErr)
			stops, releases, _ := probe.probes()
			if stops > 1 || releases > 1 {
				t.Fatalf("teardown calls = (%d stops, %d releases), want one sequence however the two arrived", stops, releases)
			}
			if stops+releases == 0 {
				t.Fatal("neither caller reached the driver, want one teardown sequence")
			}
			client.mu.Lock()
			kept := len(client.unreleasedConsumers)
			client.mu.Unlock()
			if kept != 1 {
				t.Fatalf("kept consumers = %d, want the owner's one entry however the two arrived", kept)
			}
		})
	}
}

// TestAbandonAfterTerminalDrainJoinsTheTeardownItMissed pins the snapshot case
// in abandonRunners: a runner that already drained is still reachable, because
// the consumer pointer is never cleared, and its late abandon has to report the
// completed teardown rather than release the consumer a second time.
func TestAbandonAfterTerminalDrainJoinsTheTeardownItMissed(t *testing.T) {
	probe := newDrainProbeConsumer()
	runner, _, _ := newDrainRunner(t, probe, true)
	if err := runner.drainAfterRun(context.Background()); err != nil {
		t.Fatalf("drainAfterRun() = %v, want nil", err)
	}
	if err := runner.abandonForReconnect(context.Background()); err != nil {
		t.Fatalf("abandonForReconnect() = %v, want the completed teardown's result", err)
	}
	if stops, releases, _ := probe.probes(); stops != 1 || releases != 0 {
		t.Fatalf("teardown calls = (%d stops, %d releases), want the one Stop the drain already made", stops, releases)
	}
}
