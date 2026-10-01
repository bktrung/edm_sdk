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

type abortTeardownConsumer struct {
	messages chan driver.InboundMessage
	errors   chan error
	opened   chan struct{}
	released chan struct{}

	mu                   sync.Mutex
	stopErr              error
	releaseErr           error
	stopCalls            int
	releaseCalls         int
	stopUntilContextDone bool
	// releaseRefusals counts Release calls that refuse and leave the consumer
	// registered on the connection, the way a driver whose teardown call fails
	// on a broken connection does. A release that is refused does not close the
	// consumer, so the connection still carries it; the next call after the
	// count is spent succeeds.
	releaseRefusals int
	// releaseUntilContextDone makes the next Release calls block until their
	// context ends, the way a driver that ignores the caller's deadline does.
	releaseUntilContextDone int
	releaseOnce             sync.Once
	openedOnce              sync.Once
}

var errAbortTeardownReleaseRefused = errors.New("consumer release refused")

func newAbortTeardownConsumer(stopErr, releaseErr error) *abortTeardownConsumer {
	return &abortTeardownConsumer{
		messages:   make(chan driver.InboundMessage, 1),
		errors:     make(chan error, 1),
		opened:     make(chan struct{}),
		released:   make(chan struct{}),
		stopErr:    stopErr,
		releaseErr: releaseErr,
	}
}

func (c *abortTeardownConsumer) Messages() <-chan driver.InboundMessage { return c.messages }
func (c *abortTeardownConsumer) Errors() <-chan error                   { return c.errors }
func (*abortTeardownConsumer) Pause(...string) error                    { return nil }
func (*abortTeardownConsumer) Resume(...string) error                   { return nil }
func (*abortTeardownConsumer) Drain(context.Context) error              { return nil }
func (*abortTeardownConsumer) Lag(context.Context) (map[string]int64, error) {
	return nil, driver.ErrUnsupported
}

func (c *abortTeardownConsumer) Stop(ctx context.Context) error {
	c.mu.Lock()
	c.stopCalls++
	waitUntilContextDone := c.stopUntilContextDone
	stopErr := c.stopErr
	c.mu.Unlock()
	if waitUntilContextDone {
		<-ctx.Done()
		return ctx.Err()
	}
	return stopErr
}

func (c *abortTeardownConsumer) Release(ctx context.Context) error {
	c.mu.Lock()
	c.releaseCalls++
	err := c.releaseErr
	refused := c.releaseRefusals > 0
	if refused {
		c.releaseRefusals--
	}
	untilContextDone := c.releaseUntilContextDone > 0
	if untilContextDone {
		c.releaseUntilContextDone--
	}
	c.mu.Unlock()
	if untilContextDone {
		<-ctx.Done()
		return ctx.Err()
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if refused {
		if err == nil {
			err = errAbortTeardownReleaseRefused
		}
		return err
	}
	c.releaseOnce.Do(func() {
		close(c.released)
		close(c.messages)
		close(c.errors)
	})
	return err
}

func (c *abortTeardownConsumer) calls() (stop, release int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopCalls, c.releaseCalls
}

type abortTeardownDriver struct {
	conn *abortTeardownConn
}

func (*abortTeardownDriver) Name() string                      { return "abort-test" }
func (*abortTeardownDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (d *abortTeardownDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return d.conn, nil
}

type abortTeardownConn struct {
	consumer *abortTeardownConsumer
	// openGate, when set, holds Consumer open until it is closed, so a test
	// can drain the runner while its consumer is still opening.
	openGate chan struct{}
}

func (*abortTeardownConn) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (*abortTeardownConn) BrokerInfo() driver.BrokerInfo     { return driver.BrokerInfo{Kind: "test"} }

func (*abortTeardownConn) Producer(context.Context, driver.ProducerConfig) (driver.Producer, error) {
	return abortTeardownProducer{}, nil
}

func (c *abortTeardownConn) Consumer(context.Context, driver.ConsumerConfig) (driver.Consumer, error) {
	c.consumer.openedOnce.Do(func() { close(c.consumer.opened) })
	if c.openGate != nil {
		<-c.openGate
	}
	return c.consumer, nil
}
func (*abortTeardownConn) Admin() driver.Admin        { return abortTeardownAdmin{} }
func (*abortTeardownConn) Ping(context.Context) error { return nil }
func (c *abortTeardownConn) Close(context.Context) error {
	select {
	case <-c.consumer.released:
		return nil
	default:
		return fmt.Errorf("connection still carries a consumer: %w", driver.ErrResourcesOutstanding)
	}
}

type abortTeardownProducer struct{}

func (abortTeardownProducer) Publish(context.Context, ...driver.OutboundMessage) error { return nil }

func (abortTeardownProducer) Close(context.Context) error { return nil }

type abortTeardownAdmin struct{}

func (abortTeardownAdmin) EnsureTopology(context.Context, driver.TopologySpec) (driver.TopologyDiff, error) {
	return driver.TopologyDiff{}, nil
}

func (abortTeardownAdmin) DescribeTopology(context.Context, []string) (driver.TopologyState, error) {
	return driver.TopologyState{}, driver.ErrUnsupported
}

func outstandingStopError(count int) error {
	return &driver.Error{
		Driver: "abort-test",
		Op:     "stop",
		K:      driver.KindFatal,
		Err:    fmt.Errorf("%w: %d outstanding messages", driver.ErrResourcesOutstanding, count),
	}
}

// TestAbandonedReleaseIsBoundedByCloseTimeout pins that a driver Release which
// waits on its context cannot hold a reconnect open: the abandon path bounds
// the call the way the runner's own teardown does, and the release then ends
// with the phase deadline.
func TestAbandonedReleaseIsBoundedByCloseTimeout(t *testing.T) {
	fake := clock.NewFake(time.Unix(950, 0))
	consumer := newAbortTeardownConsumer(nil, nil)
	consumer.releaseUntilContextDone = 1
	conn := &abortTeardownConn{consumer: consumer}
	cfg := testClientConfig(t)
	cfg.Lifecycle.CloseTimeout = 5 * time.Second
	client, err := New(context.Background(), cfg, WithDriver(&abortTeardownDriver{conn: conn}), withClock(fake))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	runner := &Runner{client: client, consumer: consumer, consumerEpoch: 1, lifecycle: lifecycle.New()}

	abandoned := make(chan error, 1)
	go func() { abandoned <- runner.abandonForReconnect(context.Background()) }()

	waitReconnectCondition(t, func() bool {
		_, releaseCalls := consumer.calls()
		return releaseCalls == 1
	})
	// The abandon owes this release a deadline on the client's clock. Waiting
	// for that waiter is what proves the call is bounded: without one the
	// release is held by its context alone, and the context here never ends.
	waitReconnectCondition(t, func() bool { return fake.NumWaiters() >= 1 })
	fake.Advance(cfg.Lifecycle.CloseTimeout)
	select {
	case err := <-abandoned:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("abandonForReconnect() error = %v, want the phase deadline", err)
		}
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("the abandoned release was not bounded by CloseTimeout")
	}
}

// TestCloseRetriesAConsumerReleaseThatFailed pins that a consumer whose
// Release failed stays on the client, so the connection still carrying it can
// be closed by a later call instead of failing every close with an
// outstanding-resource refusal.
//
// The harness refuses two releases: the one the drain makes, and the retry the
// first close makes. The first close is therefore the call that reports the
// refused release, and the second is the one that proves the retry was made
// and that the entry is dropped once it succeeds.
func TestCloseRetriesAConsumerReleaseThatFailed(t *testing.T) {
	consumer := newAbortTeardownConsumer(outstandingStopError(1), nil)
	consumer.releaseRefusals = 1
	conn := &abortTeardownConn{consumer: consumer}
	cfg := testClientConfig(t)
	cfg.Lifecycle.DrainTimeout = 20 * time.Millisecond
	cfg.Lifecycle.HandlerGrace = 0
	cfg.Lifecycle.CloseTimeout = time.Second
	client, err := New(context.Background(), cfg, WithDriver(&abortTeardownDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Concurrency:    1,
		Prefetch:       1,
		Priorities:     []Priority{PriorityMedium},
		Retry:          RetryConfig{MaxAttempts: 1},
		HandlerTimeout: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancelRun()
		_ = client.Close(context.Background())
	})
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(runCtx) }()
	openTimer := clock.NewReal().Timer(time.Second)
	defer openTimer.Stop()
	select {
	case <-consumer.opened:
	case <-openTimer.C:
		t.Fatal("consumer did not open")
	}
	waitReconnectCondition(t, func() bool {
		runner.mu.Lock()
		defer runner.mu.Unlock()
		return runner.consumer == consumer
	})
	runner.mu.Lock()
	inflight := runner.inflight
	runner.mu.Unlock()
	if inflight == nil {
		t.Fatal("runner did not initialize its in-flight registry")
	}
	inflight.Add()

	if err := client.Close(context.Background()); !errors.Is(err, errAbortTeardownReleaseRefused) {
		t.Fatalf("first Client.Close() error = %v, want the refused release", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("second Client.Close() error = %v, want nil", err)
	}
	if _, releaseCalls := consumer.calls(); releaseCalls != 2 {
		t.Fatalf("Release() calls = %d, want the drain's refused call and the retry that released", releaseCalls)
	}
	select {
	case runErr := <-runDone:
		if !errors.Is(runErr, driver.ErrResourcesOutstanding) {
			t.Fatalf("Run() error = %v, want ErrResourcesOutstanding", runErr)
		}
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("Run() did not return after the drain")
	}
}

func TestAbortedDrainTeardownReleasesWhenStopRefuses(t *testing.T) {
	consumer := newAbortTeardownConsumer(outstandingStopError(3), nil)
	runner := &Runner{consumer: consumer}

	err := stopRunnerConsumer(runner, context.Background())
	stopCalls, releaseCalls := consumer.calls()
	if releaseCalls != 1 {
		t.Fatalf("Release() calls = %d, want 1", releaseCalls)
	}
	if stopCalls != 1 {
		t.Fatalf("Stop() calls = %d, want 1 without retry", stopCalls)
	}
	if err == nil {
		t.Fatal("stopRunnerConsumer() error = nil, want Stop refusal")
	}
	if !errors.Is(err, driver.ErrResourcesOutstanding) {
		t.Fatalf("stopRunnerConsumer() error = %v, want ErrResourcesOutstanding", err)
	}
	if !strings.Contains(err.Error(), "3 outstanding messages") {
		t.Fatalf("stopRunnerConsumer() error = %v, want the outstanding count", err)
	}
}

func TestAbortedDrainTeardownReportsAFailedRelease(t *testing.T) {
	releaseErr := errors.New("consumer release failed")
	consumer := newAbortTeardownConsumer(outstandingStopError(3), releaseErr)
	runner := &Runner{consumer: consumer}

	err := stopRunnerConsumer(runner, context.Background())
	if !errors.Is(err, driver.ErrResourcesOutstanding) {
		t.Fatalf("stopRunnerConsumer() error = %v, want ErrResourcesOutstanding", err)
	}
	if !errors.Is(err, releaseErr) {
		t.Fatalf("stopRunnerConsumer() error = %v, want the Release error", err)
	}
	if !strings.Contains(err.Error(), "3 outstanding messages") {
		t.Fatalf("stopRunnerConsumer() error = %v, want the outstanding count", err)
	}
	stopCalls, releaseCalls := consumer.calls()
	if stopCalls != 1 {
		t.Fatalf("Stop() calls = %d, want 1", stopCalls)
	}
	if releaseCalls != 1 {
		t.Fatalf("Release() calls = %d, want 1", releaseCalls)
	}
}

func TestAbortedDrainTeardownReleasesAfterAnyStopFailure(t *testing.T) {
	stopErr := &driver.Error{Driver: "abort-teardown", Op: "drain", K: driver.KindTransient, Err: errors.New("leave group: connection refused")}
	consumer := newAbortTeardownConsumer(stopErr, nil)
	runner := &Runner{consumer: consumer}

	err := stopRunnerConsumer(runner, context.Background())
	if !errors.Is(err, stopErr) {
		t.Fatalf("stopRunnerConsumer() error = %v, want the Stop failure", err)
	}
	stopCalls, releaseCalls := consumer.calls()
	if stopCalls != 1 {
		t.Fatalf("Stop() calls = %d, want 1", stopCalls)
	}
	if releaseCalls != 1 {
		t.Fatalf("Release() calls = %d, want 1 so the consumer does not stay registered", releaseCalls)
	}
}

func TestSettledDrainTeardownDoesNotRelease(t *testing.T) {
	consumer := newAbortTeardownConsumer(nil, errors.New("Release must not be called"))
	runner := &Runner{consumer: consumer}

	if err := stopRunnerConsumer(runner, context.Background()); err != nil {
		t.Fatalf("stopRunnerConsumer() error = %v, want nil", err)
	}
	stopCalls, releaseCalls := consumer.calls()
	if stopCalls != 1 {
		t.Fatalf("Stop() calls = %d, want 1", stopCalls)
	}
	if releaseCalls != 0 {
		t.Fatalf("Release() calls = %d, want 0", releaseCalls)
	}
}

func TestAbortedDrainJoinsTheStopRefusalIntoTheDrainError(t *testing.T) {
	for _, viaDrain := range []bool{false, true} {
		name := "run canceled"
		if viaDrain {
			name = "drain requested"
		}
		t.Run(name, func(t *testing.T) {
			testAbortedDrainJoinsTheStopRefusal(t, viaDrain)
		})
	}
}

// testAbortedDrainJoinsTheStopRefusal ends a run whose drain times out with a
// delivery still held, either by canceling Run or by calling Drain, and checks
// that the Stop refusal reaches Run and, when Drain ended it, Drain as well.
func testAbortedDrainJoinsTheStopRefusal(t *testing.T, viaDrain bool) {
	consumer := newAbortTeardownConsumer(outstandingStopError(1), nil)
	conn := &abortTeardownConn{consumer: consumer}
	cfg := testClientConfig(t)
	cfg.Lifecycle.DrainTimeout = 20 * time.Millisecond
	cfg.Lifecycle.HandlerGrace = 0
	cfg.Lifecycle.CloseTimeout = time.Second
	client, err := New(context.Background(), cfg, WithDriver(&abortTeardownDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(context.Background()); err != nil {
			t.Errorf("Client.Close() error = %v", err)
		}
	}()

	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Concurrency:    1,
		Prefetch:       1,
		Priorities:     []Priority{PriorityMedium},
		Retry:          RetryConfig{MaxAttempts: 1},
		HandlerTimeout: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()
	openTimer := clock.NewReal().Timer(time.Second)
	defer openTimer.Stop()
	select {
	case <-consumer.opened:
	case <-openTimer.C:
		t.Fatal("consumer did not open")
	}
	runner.mu.Lock()
	inflight := runner.inflight
	runner.mu.Unlock()
	if inflight == nil {
		t.Fatal("runner did not initialize its in-flight registry")
	}
	inflight.Add()
	var drainErr error
	if viaDrain {
		drainErr = runner.Drain(context.Background())
	} else {
		cancel()
	}

	var runErr error
	runTimer := clock.NewReal().Timer(time.Second)
	defer runTimer.Stop()
	select {
	case runErr = <-runDone:
	case <-runTimer.C:
		t.Fatal("Run() did not return after the drain timeout")
	}
	if !errors.Is(runErr, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want the settle timeout", runErr)
	}
	if !errors.Is(runErr, driver.ErrResourcesOutstanding) {
		t.Fatalf("Run() error = %v, want ErrResourcesOutstanding", runErr)
	}
	if !strings.Contains(runErr.Error(), "1 outstanding messages") {
		t.Fatalf("Run() error = %v, want the outstanding count", runErr)
	}
	if viaDrain && !errors.Is(drainErr, driver.ErrResourcesOutstanding) {
		t.Fatalf("Drain() error = %v, want the Stop refusal Run reported", drainErr)
	}
	stopCalls, releaseCalls := consumer.calls()
	if stopCalls != 1 || releaseCalls != 1 {
		t.Fatalf("consumer calls = Stop %d, Release %d; want one each", stopCalls, releaseCalls)
	}
}

func TestAbortedDrainReleasesConsumerAfterStopTimeout(t *testing.T) {
	consumer := newAbortTeardownConsumer(nil, nil)
	consumer.stopUntilContextDone = true
	conn := &abortTeardownConn{consumer: consumer}
	cfg := testClientConfig(t)
	cfg.Lifecycle.DrainTimeout = 20 * time.Millisecond
	cfg.Lifecycle.HandlerGrace = 0
	cfg.Lifecycle.CloseTimeout = 20 * time.Millisecond
	client, err := New(context.Background(), cfg, WithDriver(&abortTeardownDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}

	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Concurrency:    1,
		Prefetch:       1,
		Priorities:     []Priority{PriorityMedium},
		Retry:          RetryConfig{MaxAttempts: 1},
		HandlerTimeout: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = client.Close(context.Background())
	})
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()
	openTimer := clock.NewReal().Timer(time.Second)
	defer openTimer.Stop()
	select {
	case <-consumer.opened:
	case <-openTimer.C:
		t.Fatal("consumer did not open")
	}

	waitReconnectCondition(t, func() bool {
		runner.mu.Lock()
		defer runner.mu.Unlock()
		return runner.consumer == consumer
	})
	runner.mu.Lock()
	inflight := runner.inflight
	runner.mu.Unlock()
	if inflight == nil {
		t.Fatal("runner did not initialize its in-flight registry")
	}
	inflight.Add()
	cancel()

	var runErr error
	runTimer := clock.NewReal().Timer(time.Second)
	defer runTimer.Stop()
	select {
	case runErr = <-runDone:
	case <-runTimer.C:
		t.Fatal("Run() did not return after the stop timeout")
	}
	if !errors.Is(runErr, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want the drain timeout", runErr)
	}
	stopCalls, releaseCalls := consumer.calls()
	if stopCalls != 1 {
		t.Fatalf("Stop() calls = %d, want 1", stopCalls)
	}
	if releaseCalls != 1 {
		t.Fatalf("Release() calls = %d, want 1 after Stop timed out", releaseCalls)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Client.Close() after timed-out Stop = %v, want nil", err)
	}
}

func TestCloseReleasesAConsumerThatOpenedDuringDrainAndRefusedItsRelease(t *testing.T) {
	consumer := newAbortTeardownConsumer(nil, nil)
	consumer.releaseRefusals = 1
	conn := &abortTeardownConn{consumer: consumer, openGate: make(chan struct{})}
	cfg := testClientConfig(t)
	cfg.Lifecycle.CloseTimeout = time.Second
	client, err := New(context.Background(), cfg, WithDriver(&abortTeardownDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Concurrency:    1,
		Prefetch:       1,
		Priorities:     []Priority{PriorityMedium},
		Retry:          RetryConfig{MaxAttempts: 1},
		HandlerTimeout: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	go func() { _ = runner.Run(context.Background()) }()
	openTimer := clock.NewReal().Timer(time.Second)
	defer openTimer.Stop()
	select {
	case <-consumer.opened:
	case <-openTimer.C:
		t.Fatal("consumer open did not start")
	}
	drainDone := make(chan error, 1)
	go func() { drainDone <- runner.Drain(context.Background()) }()
	waitReconnectCondition(t, func() bool {
		runner.mu.Lock()
		defer runner.mu.Unlock()
		return runner.draining
	})
	// Let the drain request reach the owner before the open completes.
	_ = clock.NewReal().Sleep(context.Background(), 20*time.Millisecond)
	close(conn.openGate)
	select {
	case err := <-drainDone:
		if !errors.Is(err, errAbortTeardownReleaseRefused) {
			t.Fatalf("Drain() error = %v, want the refused release", err)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("Drain() did not return")
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Client.Close() error = %v, want the kept consumer released and the connection closed", err)
	}
}
