package inmem

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	//nolint:depguard // integration tests exercise the SDK through the in-memory driver
	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestPublisherOnlyClientReachesSeparateSubscriber(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := publisherTopologyTestConfig()
	shared := NewShared(nil)
	publisher, err := f1.New(ctx, cfg, f1.WithDriver(shared), f1.WithPublishTopics("orders.created"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = publisher.Close(context.Background()) })

	subscriber, err := f1.New(ctx, cfg, f1.WithDriver(shared), f1.WithTopology(f1.TopologyNone))
	require.NoError(t, err)
	t.Cleanup(func() { _ = subscriber.Close(context.Background()) })

	handled := make(chan struct{})
	var handledOnce sync.Once
	runner, err := subscriber.Subscribe(ctx, f1.Subscription{
		Name:        "orders-worker",
		Topics:      []string{"orders.created"},
		Concurrency: 1,
		Prefetch:    1,
		Priorities:  []f1.Priority{f1.PriorityNormal},
		Retry:       f1.RetryConfig{MaxAttempts: 1},
		Handlers: map[string]f1.Handler{
			"orders.created.v1": f1.HandlerFunc(func(context.Context, *f1.Event) error {
				handledOnce.Do(func() { close(handled) })
				return nil
			}),
		},
	})
	require.NoError(t, err)

	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()

	_, err = publisher.Publisher().Publish(ctx, "orders.created.v1", map[string]string{"id": "o1"})
	require.NoError(t, err)
	timer := clock.NewReal().Timer(2 * time.Second)
	select {
	case <-handled:
	case <-timer.C:
		t.Fatal("separate subscriber did not receive publisher-only message")
	}

	require.NoError(t, publisher.Close(context.Background()))
	require.NoError(t, subscriber.Health(context.Background()))
	cancel()
	timer = clock.NewReal().Timer(2 * time.Second)
	select {
	case <-runDone:
	case <-timer.C:
		t.Fatal("subscriber runner did not stop")
	}
}

func publisherTopologyTestConfig() f1.Config {
	return f1.Config{
		Env:        "test",
		Service:    "orders",
		InstanceID: "publisher-topology-test",
		Broker: f1.BrokerConfig{
			Driver:          "inmem",
			ConnectTimeout:  30 * time.Second,
			DefaultPrefetch: 64,
		},
		Topology: f1.TopologyConfig{
			AutoCreate:    true,
			VerifyOnStart: true,
			Priorities:    []f1.Priority{f1.PriorityNormal},
		},
		Codec: f1.CodecConfig{
			Default:        "json",
			ContentMode:    "binary",
			MaxHeaderBytes: f1.CoreMaxHeaderBytes,
			MaxBodyBytes:   1 << 20,
		},
		Lifecycle: f1.LifecycleConfig{
			DrainTimeout:          time.Minute,
			HandlerGrace:          5 * time.Second,
			FlushTimeout:          20 * time.Second,
			CloseTimeout:          10 * time.Second,
			RebalanceDrainTimeout: 25 * time.Second,
		},
		Subscriptions: map[string]f1.SubscriptionConfig{},
	}
}
