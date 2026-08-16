package inmem

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	//nolint:depguard // integration tests exercise the SDK through the in-memory driver
	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func (c *conn) failNextAckForTest() {
	c.mu.Lock()
	c.failNextAck = true
	c.mu.Unlock()
}

func (c *conn) failNextNackForTest() {
	c.mu.Lock()
	c.failNextNack = true
	c.mu.Unlock()
}

func (c *conn) settlementFailureCountsForTest() (ack, nack uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ackFailures, c.nackFailures
}

type settlementDriver struct {
	conn *conn
}

var _ driver.Driver = (*settlementDriver)(nil)

func (*settlementDriver) Name() string { return "inmem" }

func (d *settlementDriver) Capabilities() driver.Capabilities {
	return (Driver{}).Capabilities()
}

func (d *settlementDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	raw, err := (Driver{}).Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	d.conn = raw.(*conn)
	return raw, nil
}

func TestSettlementHookFailsExactlyOneAck(t *testing.T) {
	ctx, raw, producer := openTest(t, clock.NewReal(), driver.DestinationSpec{Name: "orders"})
	conn := raw.(*conn)
	consumer, err := raw.Consumer(ctx, driver.ConsumerConfig{
		Destinations: []string{"orders"},
		Prefetch:     1,
		Effective:    testCaps(),
	})
	require.NoError(t, err)
	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "orders", Body: []byte("one")}))

	message := receiveTest(t, consumer)
	conn.failNextAckForTest()
	err = message.Settle.Ack(ctx)
	require.Error(t, err)
	kind, classified := driver.Classify(err)
	require.True(t, classified)
	require.Equal(t, driver.KindTransient, kind)
	ackFailures, nackFailures := conn.settlementFailureCountsForTest()
	require.Equal(t, uint64(1), ackFailures)
	require.Zero(t, nackFailures)

	require.NoError(t, message.Settle.Ack(ctx))
	ackFailures, nackFailures = conn.settlementFailureCountsForTest()
	require.Equal(t, uint64(1), ackFailures)
	require.Zero(t, nackFailures)
	closeTest(t, ctx, raw, producer, consumer)
}

func TestRunnerRecoversFromInjectedAckFailure(t *testing.T) {
	handled := make(chan struct{})
	client, runner, conn := newSettlementRunner(t, func(context.Context, *f1.Event) error {
		select {
		case <-handled:
		default:
			close(handled)
		}
		return nil
	})
	conn.failNextAckForTest()

	runDone, cancelRun := runSettlementRunnerWithCancel(runner)
	publishSettlementEvent(t, client, "ack-failure")
	waitSettlementSignal(t, handled, "handler did not run")
	waitSettlementFailure(t, conn, 1, 0)
	waitInmemOutstandingZero(t, conn)
	cancelRun()
	runErr := <-runDone
	require.NoError(t, runErr)
	require.NoError(t, runner.Drain(context.Background()))
	ackFailures, nackFailures := conn.settlementFailureCountsForTest()
	require.Equal(t, uint64(1), ackFailures)
	require.Zero(t, nackFailures)
	require.NoError(t, client.Close(context.Background()))
}

func TestRunnerRecoversFromInjectedNackFailure(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	client, runner, conn := newSettlementRunner(t, func(context.Context, *f1.Event) error {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
		return nil
	})
	conn.failNextNackForTest()

	runDone := runSettlementRunner(runner)
	publishSettlementEvent(t, client, "nack-failure")
	waitSettlementSignal(t, started, "handler did not start")
	if err := runner.Drain(context.Background()); err != nil {
		t.Fatalf("Drain() = %v", err)
	}
	runErr := <-runDone
	require.NoError(t, runErr)
	close(release)

	ackFailures, nackFailures := conn.settlementFailureCountsForTest()
	require.Zero(t, ackFailures)
	require.Equal(t, uint64(1), nackFailures)
	require.NoError(t, client.Close(context.Background()))
}

func TestRunnerRedeliversAbandonedMessageAfterFreshConsumer(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	client, first, _ := newSettlementRunner(t, func(context.Context, *f1.Event) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	})

	firstDone := runSettlementRunner(first)
	publishSettlementEvent(t, client, "redelivery")
	waitSettlementSignal(t, started, "first handler did not start")
	if err := first.Drain(context.Background()); err != nil {
		t.Fatalf("first Drain() = %v", err)
	}
	require.NoError(t, <-firstDone)
	close(release)

	second, err := client.Subscribe(context.Background(), settlementSubscription(func(context.Context, *f1.Event) error {
		calls.Add(1)
		return nil
	}))
	require.NoError(t, err)
	runSettlementRunner(second)
	waitSettlementCount(t, &calls, 2, "message was not redelivered to a fresh consumer")
	require.NoError(t, client.Close(context.Background()))
}

func newSettlementRunner(t *testing.T, handler func(context.Context, *f1.Event) error) (*f1.Client, *f1.Runner, *conn) {
	t.Helper()
	t.Setenv("F1_ENV", "test")
	t.Setenv("F1_SERVICE", "settlement")
	t.Setenv("F1_INSTANCE_ID", "settlement-test")
	t.Setenv("F1_BROKER_DRIVER", "inmem")
	cfg, err := f1.LoadConfig("")
	require.NoError(t, err)
	cfg.Env = "test"
	cfg.Service = "settlement"
	cfg.InstanceID = "settlement-test"
	cfg.Broker.Driver = "inmem"
	cfg.Topology.AutoCreate = true
	cfg.Broker.DefaultPrefetch = 1
	cfg.Lifecycle.DrainTimeout = 500 * time.Millisecond
	cfg.Lifecycle.HandlerGrace = 499 * time.Millisecond
	cfg.Lifecycle.FlushTimeout = time.Second
	cfg.Lifecycle.CloseTimeout = time.Second

	driver := &settlementDriver{}
	client, err := f1.New(context.Background(), cfg,
		f1.WithDriver(driver),
		f1.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close(context.Background())
	})
	runner, err := client.Subscribe(context.Background(), settlementSubscription(handler))
	require.NoError(t, err)
	return client, runner, driver.conn
}

func settlementSubscription(handler func(context.Context, *f1.Event) error) f1.Subscription {
	return f1.Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Concurrency:    1,
		Prefetch:       1,
		Priorities:     []f1.Priority{f1.PriorityNormal},
		Retry:          f1.RetryConfig{MaxAttempts: 1},
		HandlerTimeout: 20 * time.Millisecond,
		Handlers: map[string]f1.Handler{
			"orders.created.v1": f1.HandlerFunc(handler),
		},
	}
}

func runSettlementRunner(runner *f1.Runner) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- runner.Run(context.Background())
	}()
	return done
}

func publishSettlementEvent(t *testing.T, client *f1.Client, id string) {
	t.Helper()
	t.Setenv("F1_ENV", "test")
	t.Setenv("F1_SERVICE", "settlement")
	t.Setenv("F1_INSTANCE_ID", "settlement-test")
	t.Setenv("F1_BROKER_DRIVER", "inmem")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ticker := clock.NewReal().Ticker(time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		_, err := client.Publisher().Publish(ctx, "orders.created.v1", map[string]string{"id": id})
		if err == nil {
			return
		}
		last = err
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("publish %q: last error: %v", id, last)
		}
	}
}

func waitSettlementSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	t.Setenv("F1_ENV", "test")
	t.Setenv("F1_SERVICE", "settlement")
	t.Setenv("F1_INSTANCE_ID", "settlement-test")
	t.Setenv("F1_BROKER_DRIVER", "inmem")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatal(message)
	}
}

func waitSettlementCount(t *testing.T, calls *atomic.Int32, want int32, message string) {
	t.Helper()
	t.Setenv("F1_ENV", "test")
	t.Setenv("F1_SERVICE", "settlement")
	t.Setenv("F1_INSTANCE_ID", "settlement-test")
	t.Setenv("F1_BROKER_DRIVER", "inmem")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ticker := clock.NewReal().Ticker(time.Millisecond)
	defer ticker.Stop()
	for {
		if calls.Load() >= want {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(message)
		}
	}
}

func runSettlementRunnerWithCancel(runner *f1.Runner) (<-chan error, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runner.Run(ctx)
	}()
	return done, cancel
}

func waitSettlementFailure(t *testing.T, c *conn, wantAck, wantNack uint64) {
	t.Helper()
	deadline := clock.NewReal().Timer(time.Second)
	defer deadline.Stop()
	ticker := clock.NewReal().Ticker(time.Millisecond)
	defer ticker.Stop()
	for {
		ack, nack := c.settlementFailureCountsForTest()
		if ack >= wantAck && nack >= wantNack {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("settlement failures = ack:%d nack:%d, want ack:%d nack:%d", ack, nack, wantAck, wantNack)
		}
	}
}
