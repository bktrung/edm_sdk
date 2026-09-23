package inmem

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

type reconnectDriver struct {
	conn   *conn
	shared driver.Conn
	opens  int
}

type reconnectConn struct {
	driver.Conn
}

var _ driver.Driver = (*reconnectDriver)(nil)

func (*reconnectDriver) Name() string { return "inmem" }

func (*reconnectDriver) Capabilities() driver.Capabilities {
	return (Driver{}).Capabilities()
}

func (d *reconnectDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	d.opens++
	if d.shared != nil {
		return d.shared, nil
	}
	raw, err := (Driver{}).Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	d.conn = raw.(*conn)
	d.shared = &reconnectConn{Conn: raw}
	return d.shared, nil
}

func (c *reconnectConn) Close(context.Context) error { return nil }

func (d *reconnectDriver) settlementConn() *conn { return d.conn }

// TestRunnerRedeliversAbandonedMessageAfterReconnect verifies a released delivery arrives at a new client.
func TestRunnerRedeliversAbandonedMessageAfterReconnect(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	driver := &reconnectDriver{}
	client, first, conn := newReconnectSettlementRunner(t, driver, func(context.Context, *f1.Event) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	})

	firstDone := runSettlementRunner(first)
	publishSettlementEvent(t, client, "reconnect")
	waitSettlementSignal(t, started, "first handler did not start")
	require.NoError(t, first.Drain(context.Background()))
	require.NoError(t, <-firstDone)
	close(release)
	require.Zero(t, conn.stopOutstandingFailuresForTest())
	require.NoError(t, client.Close(context.Background()))

	secondClient, second, secondConn := newReconnectSettlementRunner(t, driver, func(context.Context, *f1.Event) error {
		calls.Add(1)
		return nil
	})
	require.Same(t, conn, secondConn)
	require.Equal(t, 2, driver.opens)
	runSettlementRunner(second)
	waitSettlementCount(t, &calls, 2, "message was not redelivered after reconnect")
	waitInmemOutstandingZero(t, secondConn)
	require.Zero(t, conn.stopOutstandingFailuresForTest())
	require.NoError(t, secondClient.Close(context.Background()))
	require.NoError(t, conn.Close(context.Background()))
}

func newReconnectSettlementRunner(t *testing.T, d *reconnectDriver, handler func(context.Context, *f1.Event) error) (*f1.Client, *f1.Runner, *conn) {
	t.Helper()
	t.Setenv("F1_ENV", "test")
	t.Setenv("F1_SERVICE", "settlement")
	t.Setenv("F1_INSTANCE_ID", "settlement-test")
	t.Setenv("F1_BROKER_DRIVER", "inmem")
	cfg, err := f1.LoadConfig("")
	require.NoError(t, err)
	cfg.Topology.AutoCreate = true
	cfg.Broker.DefaultPrefetch = 1
	cfg.Lifecycle.DrainTimeout = 500 * time.Millisecond
	cfg.Lifecycle.HandlerGrace = 499 * time.Millisecond
	cfg.Lifecycle.CloseTimeout = time.Second
	client, err := f1.New(context.Background(), cfg,
		f1.WithDriver(d),
		f1.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	runner, err := client.Subscribe(context.Background(), settlementSubscription(handler))
	require.NoError(t, err)
	return client, runner, d.settlementConn()
}

func waitInmemOutstandingZero(t *testing.T, c *conn) {
	t.Helper()
	deadline := clock.NewReal().Timer(time.Second)
	defer deadline.Stop()
	ticker := clock.NewReal().Ticker(time.Millisecond)
	defer ticker.Stop()
	for {
		c.mu.Lock()
		var outstanding int
		for consumer := range c.consumers {
			outstanding += consumer.outstanding
		}
		c.mu.Unlock()
		if outstanding == 0 {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("in-memory outstanding messages = %d", outstanding)
		}
	}
}

func (c *conn) stopOutstandingFailuresForTest() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var total uint64
	for consumer := range c.consumers {
		total += consumer.stopOutstandingFailures
	}
	return total
}
