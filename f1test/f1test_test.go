package f1test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
)

func TestClientDeliverAndCapture(t *testing.T) {
	c := NewClient(t, quietLogger())
	ctx, cancel := context.WithCancel(context.Background())
	handled := make(chan struct{})
	runner, err := c.Subscribe(ctx, f1.Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Concurrency:    1,
		Prefetch:       1,
		Priorities:     []f1.Priority{f1.PriorityNormal},
		Retry:          f1.RetryConfig{MaxAttempts: 1},
		MaxDeferrals:   1,
		HandlerTimeout: time.Second,
		Handlers: map[string]f1.Handler{
			"orders.created.v1": f1.HandlerFunc(func(context.Context, *f1.Event) error {
				close(handled)
				return nil
			}),
		},
	})
	require.NoError(t, err)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = runner.Run(ctx)
	}()
	defer func() {
		cancel()
		waitFor(t, runDone, "runner did not stop")
	}()

	c.Deliver(t, "orders.created.v1", struct {
		ID string `json:"id"`
	}{ID: "order-1"})
	waitFor(t, handled, "handler did not receive delivered event")

	published := c.Published()
	require.Len(t, published, 1)
	require.Equal(t, "orders.created.v1", published[0].EventType)
	require.Equal(t, `{"id":"order-1"}`, string(published[0].Payload))
	require.Empty(t, c.DLQ())
}

func TestClientCapturesDeadLetterCopies(t *testing.T) {
	c := NewClient(t, quietLogger())
	ctx, cancel := context.WithCancel(context.Background())
	deadLettered := make(chan struct{})
	runner, err := c.Subscribe(ctx, f1.Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Concurrency:    1,
		Prefetch:       1,
		Priorities:     []f1.Priority{f1.PriorityNormal},
		Retry:          f1.RetryConfig{MaxAttempts: 1},
		MaxDeferrals:   1,
		HandlerTimeout: time.Second,
		OnDeadLetter:   func(context.Context, f1.DeadLettered) { close(deadLettered) },
		Handlers: map[string]f1.Handler{
			"orders.created.v1": f1.HandlerFunc(func(context.Context, *f1.Event) error {
				return f1.Terminal(errors.New("invalid order"))
			}),
		},
	})
	require.NoError(t, err)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = runner.Run(ctx)
	}()
	defer func() {
		cancel()
		waitFor(t, runDone, "runner did not stop")
	}()

	c.Deliver(t, "orders.created.v1", map[string]string{"id": "order-2"})
	waitFor(t, deadLettered, "dead-letter callback did not run")

	dlq := c.DLQ()
	require.Len(t, dlq, 1)
	require.Equal(t, "orders.created.v1", dlq[0].EventType)
	require.Equal(t, f1.ReasonTerminal.String(), dlq[0].Headers["f1deathreason"])
}

func TestClientAdvanceFiresDriverRetry(t *testing.T) {
	c := NewClient(t, quietLogger())
	ctx, cancel := context.WithCancel(context.Background())
	first, second := make(chan struct{}), make(chan struct{})
	var calls int
	runner, err := c.Subscribe(ctx, f1.Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Concurrency:    1,
		Prefetch:       2,
		Priorities:     []f1.Priority{f1.PriorityNormal},
		Retry:          f1.RetryConfig{MaxAttempts: 2, Tiers: []time.Duration{time.Second}},
		MaxDeferrals:   2,
		HandlerTimeout: time.Second,
		Handlers: map[string]f1.Handler{
			"orders.created.v1": f1.HandlerFunc(func(context.Context, *f1.Event) error {
				calls++
				if calls == 1 {
					close(first)
					return errors.New("try again")
				}
				close(second)
				return nil
			}),
		},
	})
	require.NoError(t, err)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = runner.Run(ctx)
	}()
	defer func() {
		cancel()
		waitFor(t, runDone, "runner did not stop")
	}()

	c.Deliver(t, "orders.created.v1", map[string]string{"id": "order-3"})
	waitFor(t, first, "first handler attempt did not run")
	// Drain the initial publish and let the failed attempt finish its retry
	// publish before advancing the fake clock.
	_ = c.Published()
	for i := 0; i < 1000 && len(c.Published()) == 0; i++ {
		runtime.Gosched()
	}
	c.Advance(time.Second)
	waitFor(t, second, "fake-clock advance did not release the retry")
	require.Equal(t, 2, calls)
}

func Example() {
	client, err := newClient(quietLogger())
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := closeClient(client); err != nil {
			panic(err)
		}
	}()
	fmt.Println(len(client.Published()))
	// Output: 0
}

func waitFor(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatal(message)
	}
}

func quietLogger() f1.Option {
	return f1.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
}
