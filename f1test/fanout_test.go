package f1test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
)

func TestConsumeFanoutPerSubscription(t *testing.T) {
	for _, strict := range []bool{false, true} {
		name := "native"
		var options []f1.Option
		if strict {
			name = "strict"
			options = append(options, f1.WithStrictPortability())
		}
		t.Run("consume/fanout_per_subscription/"+name, func(t *testing.T) {
			c := NewClient(t, append(options, quietLogger())...)
			ctx, cancel := context.WithCancel(context.Background())
			firstHandled := make(chan struct{})
			secondHandled := make(chan struct{})
			first, err := c.Subscribe(ctx, f1.Subscription{
				Name:        "orders-worker",
				Topics:      []string{"orders.created"},
				Concurrency: 1,
				Prefetch:    1,
				Priorities:  []f1.Priority{f1.PriorityMedium},
				Retry:       f1.RetryConfig{MaxAttempts: 1},
				Handlers: map[string]f1.Handler{
					"orders.created.v1": f1.HandlerFunc(func(context.Context, *f1.Event) error {
						close(firstHandled)
						return nil
					}),
				},
			})
			require.NoError(t, err)
			second, err := c.Subscribe(ctx, f1.Subscription{
				Name:        "billing-worker",
				Topics:      []string{"orders.created"},
				Concurrency: 1,
				Prefetch:    1,
				Priorities:  []f1.Priority{f1.PriorityMedium},
				Retry:       f1.RetryConfig{MaxAttempts: 1},
				Handlers: map[string]f1.Handler{
					"orders.created.v1": f1.HandlerFunc(func(context.Context, *f1.Event) error {
						close(secondHandled)
						return nil
					}),
				},
			})
			require.NoError(t, err)

			firstDone := make(chan struct{})
			secondDone := make(chan struct{})
			go func() {
				defer close(firstDone)
				_ = first.Run(ctx)
			}()
			go func() {
				defer close(secondDone)
				_ = second.Run(ctx)
			}()
			defer func() {
				cancel()
				waitFor(t, firstDone, "first runner did not stop")
				waitFor(t, secondDone, "second runner did not stop")
			}()

			c.Deliver(t, "orders.created.v1", map[string]string{"id": "order-1"})
			waitFor(t, firstHandled, "first subscription did not receive the message")
			waitFor(t, secondHandled, "second subscription did not receive the message")
		})
	}
}
