package inmem

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	//nolint:depguard // integration tests exercise the SDK through the in-memory driver
	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/testhook"
)

// TestDrainCompletesWithoutFiringTheHandlerGraceTimer pins Drain's cost to the
// work rather than to the configured grace delay.
//
// Drain arms a timer that force-cancels handlers once drainTimeout minus
// handlerGrace has elapsed. That timer waits on the runner's done channel, and
// done is closed only after the runner's worker group has been waited. Arming it
// inside that same group therefore makes the group wait for a timer that is
// waiting for the group: the runner cannot finish until the delay is spent, no
// matter how quickly the handlers returned.
//
// The clock here is never advanced, so a correct Drain returns without the timer
// ever firing. A Drain that arms the timer in the worker group cannot return at
// all and fails on the context deadline, which is the same "drain: context
// deadline exceeded" a service sees on shutdown.
//
// Determinism comes from two synchronisation points rather than from timing: the
// handler is still running when Drain starts, so the group cannot already be
// finished, and graceClock reports the exact moment the grace timer is armed, so
// the handler is released only after that.
func TestDrainCompletesWithoutFiringTheHandlerGraceTimer(t *testing.T) {
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
	// A gap wide enough to observe. The shared settlement helper leaves one
	// millisecond between these two, which hides the defect entirely.
	cfg.Lifecycle.DrainTimeout = 2 * time.Second
	cfg.Lifecycle.HandlerGrace = 200 * time.Millisecond
	cfg.Lifecycle.CloseTimeout = time.Second

	graceDelay := cfg.Lifecycle.DrainTimeout - cfg.Lifecycle.HandlerGrace
	clk := &graceClock{Fake: clock.NewFake(time.Unix(0, 0)), delay: graceDelay, armed: make(chan struct{})}
	client, err := f1.New(context.Background(), cfg,
		f1.WithDriver(&settlementDriver{}),
		testhook.ClientOption(clk).(f1.Option),
		f1.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	require.NoError(t, err)
	defer func() { _ = client.Close(context.Background()) }()

	started := make(chan struct{})
	release := make(chan struct{})
	sub := settlementSubscription(func(context.Context, *f1.Event) error {
		close(started)
		<-release
		return nil
	})
	sub.HandlerTimeout = time.Second
	runner, err := client.Subscribe(context.Background(), sub)
	require.NoError(t, err)

	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(context.Background()) }()
	publishSettlementEvent(t, client, "drain-grace")
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	drainDone := make(chan error, 1)
	go func() { drainDone <- runner.Drain(ctx) }()

	<-clk.armed
	close(release)

	require.NoError(t, <-drainDone, "Drain did not finish while its grace timer was still unfired")
	<-runDone
}

// graceClock is a fake clock that reports when a timer of one specific duration
// is armed. Counting waiters is not enough: the runner arms timers of its own,
// so a count cannot say which timer appeared.
type graceClock struct {
	*clock.Fake
	delay time.Duration
	armed chan struct{}
	once  sync.Once
}

func (c *graceClock) Timer(d time.Duration) clock.Timer {
	if d == c.delay {
		c.once.Do(func() { close(c.armed) })
	}
	return c.Fake.Timer(d)
}
