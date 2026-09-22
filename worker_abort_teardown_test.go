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
)

type abortTeardownConsumer struct {
	messages chan driver.InboundMessage
	errors   chan error
	opened   chan struct{}
	released chan struct{}

	mu           sync.Mutex
	stopErr      error
	releaseErr   error
	stopCalls    int
	releaseCalls int
	releaseOnce  sync.Once
	openedOnce   sync.Once
}

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

func (c *abortTeardownConsumer) Stop(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopCalls++
	return c.stopErr
}

func (c *abortTeardownConsumer) Release(context.Context) error {
	c.mu.Lock()
	c.releaseCalls++
	err := c.releaseErr
	c.mu.Unlock()
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

type abortTeardownSettler struct{}

func (abortTeardownSettler) Ack(context.Context) error { return nil }
func (abortTeardownSettler) Nack(context.Context, driver.NackOptions) error {
	return nil
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
}

func (*abortTeardownConn) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (*abortTeardownConn) BrokerInfo() driver.BrokerInfo     { return driver.BrokerInfo{Kind: "test"} }

func (*abortTeardownConn) Producer(context.Context, driver.ProducerConfig) (driver.Producer, error) {
	return abortTeardownProducer{}, nil
}

func (c *abortTeardownConn) Consumer(context.Context, driver.ConsumerConfig) (driver.Consumer, error) {
	c.consumer.openedOnce.Do(func() { close(c.consumer.opened) })
	return c.consumer, nil
}
func (*abortTeardownConn) Admin() driver.Admin         { return abortTeardownAdmin{} }
func (*abortTeardownConn) Ping(context.Context) error  { return nil }
func (*abortTeardownConn) Close(context.Context) error { return nil }

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
	inflight.Add(driver.InboundMessage{Settle: abortTeardownSettler{}})
	cancel()

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
	stopCalls, releaseCalls := consumer.calls()
	if stopCalls != 1 || releaseCalls != 1 {
		t.Fatalf("consumer calls = Stop %d, Release %d; want one each", stopCalls, releaseCalls)
	}
}
