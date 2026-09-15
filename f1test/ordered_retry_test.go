package f1test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
)

// TestOrderedByKeyHandlesALaterSameKeyMessageBeforeARetry pins the retry limit
// of ordered mode. A fails retryably on its first attempt and B carries the
// same key, so B waits behind A on the key's lane. The retry copy is parked for
// its tier, and A's original is acknowledged as soon as that copy is stored,
// which releases the key while the copy still waits. B is therefore handled
// before the retry comes back, and the recorded order is A, B, A.
func TestOrderedByKeyHandlesALaterSameKeyMessageBeforeARetry(t *testing.T) {
	c := NewClient(t, quietLogger())
	ctx, cancel := context.WithCancel(context.Background())

	var mu sync.Mutex
	var handled []string
	handledSignal := make(chan struct{}, 4)

	runner, err := c.Subscribe(ctx, f1.Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Mode:           f1.OrderedByKey,
		Concurrency:    2,
		Prefetch:       4,
		Priorities:     []f1.Priority{f1.PriorityMedium},
		Retry:          f1.RetryConfig{MaxAttempts: 2, Tiers: []time.Duration{time.Second}},
		HandlerTimeout: time.Second,
		Handlers: map[string]f1.Handler{
			"orders.created.v1": f1.HandlerFunc(func(_ context.Context, event *f1.Event) error {
				var payload struct {
					ID string
				}
				if err := event.Decode(&payload); err != nil {
					return err
				}
				mu.Lock()
				handled = append(handled, payload.ID)
				mu.Unlock()
				handledSignal <- struct{}{}
				if payload.ID == "A" && event.Attempt() == 1 {
					return errors.New("try again")
				}
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

	c.Deliver(t, "orders.created.v1", map[string]string{"id": "A"}, f1.WithKey("key-0"))
	c.Deliver(t, "orders.created.v1", map[string]string{"id": "B"}, f1.WithKey("key-0"))

	// The retry copy waits out its tier on the driver's deferred queue, so
	// only A's first attempt and then B can run before the clock moves. B is
	// reached once the key is released, which is once the copy is stored.
	waitFor(t, handledSignal, "A was not handled")
	waitFor(t, handledSignal, "B was not handled before the retry")
	mu.Lock()
	beforeRetry := append([]string(nil), handled...)
	mu.Unlock()
	require.Equal(t, []string{"A", "B"}, beforeRetry, "the later same-key message did not run before the retry")

	c.Advance(time.Second)
	waitFor(t, handledSignal, "the retry was not released by the clock advance")
	mu.Lock()
	order := append([]string(nil), handled...)
	mu.Unlock()
	require.Equal(t, []string{"A", "B", "A"}, order)
}
