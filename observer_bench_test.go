package f1_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem" //nolint:depguard // benchmark measures the in-memory driver through the public client.
)

func BenchmarkObserverConsume(b *testing.B) {
	for _, concurrency := range []int{1, 8} {
		b.Run(fmt.Sprintf("Concurrency/%d", concurrency), func(b *testing.B) {
			for _, variant := range []string{"Nil", "NoOp", "Recording"} {
				b.Run(variant, func(b *testing.B) {
					benchmarkObserverConsume(b, concurrency, observerBenchmarkOpts(variant))
				})
			}
		})
	}
}

// noopObserver measures the call sites with an observer installed but no work
// done. Value receivers keep the per-call cost at the call site checks.
type noopObserver struct{}

func (noopObserver) Start(ctx context.Context, _ f1.StartEvent) (context.Context, f1.Token) {
	return ctx, f1.Token{}
}

func (noopObserver) Finish(f1.Token, f1.FinishEvent) {}

func (noopObserver) Record(f1.PointEvent) {}

// recordingObserver counts calls per method behind a mutex so concurrent
// consume and publish paths stay safe.
type recordingObserver struct {
	mu       sync.Mutex
	starts   int
	finishes int
	records  int
}

func (o *recordingObserver) Start(ctx context.Context, _ f1.StartEvent) (context.Context, f1.Token) {
	o.mu.Lock()
	o.starts++
	o.mu.Unlock()
	return ctx, f1.Token{}
}

func (o *recordingObserver) Finish(f1.Token, f1.FinishEvent) {
	o.mu.Lock()
	o.finishes++
	o.mu.Unlock()
}

func (o *recordingObserver) Record(f1.PointEvent) {
	o.mu.Lock()
	o.records++
	o.mu.Unlock()
}

func observerBenchmarkOpts(variant string) []f1.Option {
	switch variant {
	case "NoOp":
		return []f1.Option{f1.WithObserver(noopObserver{}), f1.WithBacklogPollInterval(-1)}
	case "Recording":
		return []f1.Option{f1.WithObserver(&recordingObserver{}), f1.WithBacklogPollInterval(-1)}
	default:
		return nil
	}
}

func benchmarkObserverConsume(b *testing.B, concurrency int, opts []f1.Option) {
	ctx, cancel := context.WithCancel(context.Background())
	settled := make(chan struct{}, concurrency)
	clientOpts := []f1.Option{
		f1.WithDriver(observerBenchmarkDriver{inner: inmem.New(), settled: settled}),
		f1.WithPublishTopics("orders.created"),
	}
	clientOpts = append(clientOpts, opts...)
	client, err := f1.New(ctx, observerBenchmarkConfig(), clientOpts...)
	if err != nil {
		b.Fatal(err)
	}
	runner, err := client.Subscribe(ctx, f1.Subscription{
		Name:           "observer-benchmark",
		Topics:         []string{"orders.created"},
		Mode:           f1.Unordered,
		Concurrency:    concurrency,
		Prefetch:       concurrency,
		Priorities:     []f1.Priority{f1.PriorityMedium},
		Retry:          f1.RetryConfig{MaxAttempts: 1},
		HandlerTimeout: time.Second,
		Handlers: map[string]f1.Handler{
			"orders.created": f1.HandlerFunc(func(context.Context, *f1.Event) error {
				return nil
			}),
		},
	})
	if err != nil {
		cancel()
		_ = client.Close(context.Background())
		b.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()
	publisher := client.Publisher()
	readyCtx, readyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := waitObserverBenchmarkAck(readyCtx, publisher, settled); err != nil {
		readyCancel()
		cancel()
		_ = client.Close(context.Background())
		b.Fatal(err)
	}
	readyCancel()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := publisher.Publish(ctx, "orders.created", observerBenchmarkPayload{Value: 1}); err != nil {
				b.Fatal(err)
			}
			<-settled
		}
	})
	b.StopTimer()
	cancel()
	if err := <-runDone; err != nil {
		b.Fatal(err)
	}
	if err := client.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
}

func waitObserverBenchmarkAck(ctx context.Context, publisher *f1.Publisher, settled <-chan struct{}) error {
	if _, err := publisher.Publish(ctx, "orders.created", observerBenchmarkPayload{Value: 0}); err != nil {
		return err
	}
	select {
	case <-settled:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func BenchmarkObserverPublish(b *testing.B) {
	b.Run("Single", func(b *testing.B) {
		for _, variant := range []string{"Nil", "NoOp", "Recording"} {
			b.Run(variant, func(b *testing.B) {
				benchmarkObserverPublishSingle(b, observerBenchmarkOpts(variant))
			})
		}
	})
	b.Run("Batch100", func(b *testing.B) {
		for _, variant := range []string{"Nil", "NoOp", "Recording"} {
			b.Run(variant, func(b *testing.B) {
				benchmarkObserverPublishBatch(b, 100, observerBenchmarkOpts(variant))
			})
		}
	})
}

func benchmarkObserverPublishSingle(b *testing.B, opts []f1.Option) {
	client := newObserverBenchmarkClient(b, opts)
	publisher := client.Publisher()
	ctx := context.Background()
	payload := observerBenchmarkPayload{Value: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := publisher.Publish(ctx, "orders.created", payload); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkObserverPublishBatch(b *testing.B, size int, opts []f1.Option) {
	client := newObserverBenchmarkClient(b, opts)
	publisher := client.Publisher()
	ctx := context.Background()
	messages := make([]f1.Message, size)
	for i := range messages {
		messages[i] = f1.Message{EventType: "orders.created", Payload: observerBenchmarkPayload{Value: i}}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := publisher.PublishBatch(ctx, messages); err != nil {
			b.Fatal(err)
		}
	}
}

func newObserverBenchmarkClient(b *testing.B, opts []f1.Option) *f1.Client {
	b.Helper()
	clientOpts := []f1.Option{
		f1.WithDriver(inmem.New()),
		f1.WithPublishTopics("orders.created"),
	}
	clientOpts = append(clientOpts, opts...)
	client, err := f1.New(context.Background(), observerBenchmarkConfig(), clientOpts...)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := client.Close(context.Background()); err != nil {
			b.Errorf("close benchmark client: %v", err)
		}
	})
	return client
}

func observerBenchmarkConfig() f1.Config {
	return f1.Config{
		Env:        "test",
		Service:    "observer-benchmark",
		InstanceID: "observer-benchmark",
		Broker: f1.BrokerConfig{
			Driver:          "inmem",
			ConnectTimeout:  time.Second,
			DefaultPrefetch: 64,
		},
		Topology: f1.TopologyConfig{
			AutoCreate:    true,
			VerifyOnStart: true,
			Priorities:    []f1.Priority{f1.PriorityHigh, f1.PriorityMedium, f1.PriorityLow},
		},
		Codec: f1.CodecConfig{
			Default:        "json",
			ContentMode:    "binary",
			MaxHeaderBytes: f1.CoreMaxHeaderBytes,
			MaxBodyBytes:   1 << 20,
		},
		Lifecycle: f1.LifecycleConfig{
			DrainTimeout:          2 * time.Second,
			HandlerGrace:          time.Second,
			CloseTimeout:          time.Second,
			RebalanceDrainTimeout: time.Second,
		},
		Subscriptions: map[string]f1.SubscriptionConfig{},
	}
}

type observerBenchmarkPayload struct {
	Value int `json:"value"`
}
type observerBenchmarkDriver struct {
	inner   driver.Driver
	settled chan<- struct{}
}

func (d observerBenchmarkDriver) Name() string {
	return d.inner.Name()
}

func (d observerBenchmarkDriver) Capabilities() driver.Capabilities {
	return d.inner.Capabilities()
}

func (d observerBenchmarkDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	conn, err := d.inner.Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return observerBenchmarkConn{Conn: conn, settled: d.settled}, nil
}

type observerBenchmarkConn struct {
	driver.Conn
	settled chan<- struct{}
}

func (c observerBenchmarkConn) Consumer(ctx context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	consumer, err := c.Conn.Consumer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &observerBenchmarkConsumer{Consumer: consumer, settled: c.settled}, nil
}

type observerBenchmarkConsumer struct {
	driver.Consumer
	settled chan<- struct{}
}

func (c *observerBenchmarkConsumer) Messages() <-chan driver.InboundMessage {
	messages := make(chan driver.InboundMessage)
	go func() {
		for message := range c.Consumer.Messages() {
			if message.Settle != nil {
				message.Settle = observerBenchmarkSettler{Settler: message.Settle, settled: c.settled}
			}
			messages <- message
		}
		close(messages)
	}()
	return messages
}

type observerBenchmarkSettler struct {
	driver.Settler
	settled chan<- struct{}
}

func (s observerBenchmarkSettler) Ack(ctx context.Context) error {
	err := s.Settler.Ack(ctx)
	if err == nil {
		s.settled <- struct{}{}
	}
	return err
}
