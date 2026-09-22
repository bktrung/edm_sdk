package f1otel

import (
	"context"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func BenchmarkObserverConcurrentStartFinish(b *testing.B) {
	ctx := context.Background()
	start := f1.StartEvent{
		Kind:         f1.ObserverProcess,
		At:           time.Unix(100, 0),
		Topic:        "orders.created",
		Subscription: "orders",
	}
	finish := f1.FinishEvent{
		Kind: f1.ObserverProcess,
		At:   start.At.Add(time.Second),
	}
	received := f1.PointEvent{
		Kind:         f1.ObserverDeliveryReceived,
		At:           finish.At,
		Topic:        start.Topic,
		Subscription: start.Subscription,
	}

	b.Run("metrics_only", func(b *testing.B) {
		meterProvider := sdkmetric.NewMeterProvider()
		observer, err := New(WithMeterProvider(meterProvider))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { //nolint:contextcheck // benchmark cleanup uses a provider-owned context.
			if err := meterProvider.Shutdown(context.Background()); err != nil {
				b.Error(err)
			}
		})
		observer.Record(f1.PointEvent{Kind: f1.ObserverDriverSelected, DriverName: "inmem"}) //nolint:contextcheck // Record has no context parameter in f1.Observer.
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, token := observer.Start(ctx, start)
				observer.Record(received)      //nolint:contextcheck // Record has no context parameter in f1.Observer.
				observer.Finish(token, finish) //nolint:contextcheck // Finish has no context parameter in f1.Observer.
			}
		})
	})

	b.Run("metrics_and_tracer", func(b *testing.B) {
		meterProvider := sdkmetric.NewMeterProvider()
		tracerProvider := sdktrace.NewTracerProvider()
		observer, err := New(
			WithMeterProvider(meterProvider),
			WithTracerProvider(tracerProvider),
		)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { //nolint:contextcheck // benchmark cleanup uses provider-owned contexts.
			if err := meterProvider.Shutdown(context.Background()); err != nil {
				b.Error(err)
			}
			if err := tracerProvider.Shutdown(context.Background()); err != nil {
				b.Error(err)
			}
		})
		observer.Record(f1.PointEvent{Kind: f1.ObserverDriverSelected, DriverName: "inmem"}) //nolint:contextcheck // Record has no context parameter in f1.Observer.
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, token := observer.Start(ctx, start)
				observer.Finish(token, finish) //nolint:contextcheck // Finish has no context parameter in f1.Observer.
				observer.Record(received)      //nolint:contextcheck // Record has no context parameter in f1.Observer.
			}
		})
	})
}

func BenchmarkBatchPublishCreateSpans(b *testing.B) {
	for _, enabled := range []bool{true, false} {
		b.Run(createSpanBenchmarkName(enabled), func(b *testing.B) {
			provider := sdktrace.NewTracerProvider()
			observer, err := New(WithTracerProvider(provider), WithCreateSpans(enabled))
			if err != nil {
				b.Fatal(err)
			}
			client, err := f1.New(context.Background(), benchmarkConfig(), f1.WithObserver(observer), f1.WithDriver(inmem.New()))
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := client.Close(context.Background()); err != nil {
					b.Error(err)
				}
				if err := provider.Shutdown(context.Background()); err != nil {
					b.Error(err)
				}
			})

			messages := make([]f1.Message, 100)
			for index := range messages {
				messages[index] = f1.Message{
					EventType: "orders.created.v1",
					Payload:   map[string]int{"index": index},
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := client.Publisher().PublishBatch(context.Background(), messages); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func createSpanBenchmarkName(enabled bool) string {
	if enabled {
		return "create_on"
	}
	return "create_off"
}

func benchmarkConfig() f1.Config {
	return f1.Config{
		Env:        "bench",
		Service:    "f1otel",
		InstanceID: "bench",
		Broker: f1.BrokerConfig{
			Driver:          "inmem",
			ConnectTimeout:  30 * time.Second,
			DefaultPrefetch: 64,
		},
		Topology: f1.TopologyConfig{
			AutoCreate: true,
			Priorities: []f1.Priority{f1.PriorityHigh, f1.PriorityMedium, f1.PriorityLow},
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
			CloseTimeout:          10 * time.Second,
			RebalanceDrainTimeout: 25 * time.Second,
		},
		Subscriptions: map[string]f1.SubscriptionConfig{},
	}
}
