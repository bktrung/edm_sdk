package f1test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
)

// TestCancellingRunCancelsHandlersWithoutGrace pins the hard stop: cancelling
// the context passed to Run ends an in-flight handler context without waiting
// out HandlerGrace. The harness configures HandlerGrace as five seconds on the
// fake clock and this test never advances that clock, so a handler that sees
// its context end cannot have been released by the grace window; a stop that
// gave handlers their grace would stay blocked here. Runner.Drain is the
// graceful stop instead.
func TestCancellingRunCancelsHandlersWithoutGrace(t *testing.T) {
	c := NewClient(t, quietLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handlerStarted := make(chan struct{})
	handlerCanceled := make(chan struct{})
	var startedOnce, canceledOnce sync.Once

	runner, err := c.Subscribe(ctx, f1.Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Concurrency:    1,
		Prefetch:       1,
		Priorities:     []f1.Priority{f1.PriorityMedium},
		Retry:          f1.RetryConfig{MaxAttempts: 1},
		HandlerTimeout: time.Second,
		Handlers: map[string]f1.Handler{
			"orders.created.v1": f1.HandlerFunc(func(handlerCtx context.Context, _ *f1.Event) error {
				startedOnce.Do(func() { close(handlerStarted) })
				<-handlerCtx.Done()
				canceledOnce.Do(func() { close(handlerCanceled) })
				return handlerCtx.Err()
			}),
		},
	})
	require.NoError(t, err)

	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()

	c.Deliver(t, "orders.created.v1", map[string]string{"id": "order-1"})
	waitFor(t, handlerStarted, "handler did not start")

	cancel()
	waitFor(t, handlerCanceled, "handler context was not cancelled after Run's context was cancelled")

	select {
	case <-runDone:
	case <-time.After(5 * time.Second): //nolint:forbidigo // bound the wait for Run to return instead of hanging the test
		t.Fatal("runner did not stop after its context was cancelled")
	}
}
