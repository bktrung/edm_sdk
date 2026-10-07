package f1_test

import (
	"context"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/f1otel"

	//nolint:depguard // the lifecycle test exercises the public client through the in-memory driver.
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"

	//nolint:depguard // provider construction is part of the f1otel lifecycle check.
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	//nolint:depguard // provider construction is part of the f1otel lifecycle check.
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const goroutineLeakModulePath = "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"

func TestClientGoroutineLeak(t *testing.T) {
	t.Run("publish-only client", func(t *testing.T) {
		assertNoClientGoroutineLeak(t, func(t *testing.T) {
			client := newGoroutineLeakClient(t)
			if _, err := client.Publisher().Publish(context.Background(), "orders.created", map[string]string{"id": "publish-only"}); err != nil {
				t.Fatal(err)
			}
			if err := closeGoroutineLeakClient(client); err != nil {
				t.Fatal(err)
			}
		})
	})
	t.Run("subscribe run drain close", func(t *testing.T) {
		assertNoClientGoroutineLeak(t, func(t *testing.T) {
			runGoroutineLeakSubscribeDrainClose(t, nil)
		})
	})
	t.Run("run canceled then close", func(t *testing.T) {
		assertNoClientGoroutineLeak(t, func(t *testing.T) {
			client := newGoroutineLeakClient(t)
			runCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			handled := make(chan struct{})
			runner, err := client.Subscribe(context.Background(), goroutineLeakSubscription(handled))
			if err != nil {
				t.Fatal(err)
			}
			runDone := make(chan error, 1)
			go func() { runDone <- runner.Run(runCtx) }()
			if _, err := client.Publisher().Publish(context.Background(), "orders.created", map[string]string{"id": "cancel"}); err != nil {
				t.Fatal(err)
			}
			waitGoroutineLeakSignal(t, handled, "handler did not observe the published event")
			cancel()
			waitGoroutineLeakRun(t, runDone)
			if err := closeGoroutineLeakClient(client); err != nil {
				t.Fatal(err)
			}
		})
	})
	t.Run("close without drain", func(t *testing.T) {
		assertNoClientGoroutineLeak(t, func(t *testing.T) {
			client := newGoroutineLeakClient(t)
			runCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			handled := make(chan struct{})
			runner, err := client.Subscribe(context.Background(), goroutineLeakSubscription(handled))
			if err != nil {
				t.Fatal(err)
			}
			runDone := make(chan error, 1)
			go func() { runDone <- runner.Run(runCtx) }()
			if _, err := client.Publisher().Publish(context.Background(), "orders.created", map[string]string{"id": "close"}); err != nil {
				t.Fatal(err)
			}
			waitGoroutineLeakSignal(t, handled, "handler did not observe the published event")
			if err := closeGoroutineLeakClient(client); err != nil {
				t.Fatal(err)
			}
			waitGoroutineLeakRun(t, runDone)
		})
	})
	t.Run("subscribe run drain close with f1otel", func(t *testing.T) {
		assertNoClientGoroutineLeak(t, func(t *testing.T) {
			meterProvider := sdkmetric.NewMeterProvider()
			tracerProvider := sdktrace.NewTracerProvider()
			observer, err := f1otel.New(
				f1otel.WithMeterProvider(meterProvider),
				f1otel.WithTracerProvider(tracerProvider),
			)
			if err != nil {
				t.Fatal(err)
			}
			runGoroutineLeakSubscribeDrainClose(t, func(t *testing.T) {
				if err := meterProvider.Shutdown(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := tracerProvider.Shutdown(context.Background()); err != nil {
					t.Fatal(err)
				}
			}, f1.WithObserver(observer))
		})
	})
}

func assertNoClientGoroutineLeak(t *testing.T, run func(*testing.T)) {
	t.Helper()
	baseline := runtime.NumGoroutine()
	run(t)

	deadline := clock.NewReal().Timer(2 * time.Second)
	defer deadline.Stop()
	poll := clock.NewReal().Ticker(5 * time.Millisecond)
	defer poll.Stop()
	for {
		current := runtime.NumGoroutine()
		if current <= baseline {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("goroutines after close = %d, baseline = %d\nmodule-owned goroutine stacks:\n%s", current, baseline, moduleGoroutineStacks())
		case <-poll.C:
		}
	}
}

func moduleGoroutineStacks() string {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	var stacks []string
	for stack := range strings.SplitSeq(string(buf[:n]), "\n\n") {
		lines := strings.Split(stack, "\n")
		for i := 1; i+1 < len(lines); i++ {
			if strings.HasPrefix(lines[i], "created by ") {
				break
			}
			source := strings.TrimSpace(lines[i+1])
			if strings.Contains(lines[i], goroutineLeakModulePath) && strings.Contains(source, ".go:") && !strings.Contains(source, "_test.go:") {
				stacks = append(stacks, stack)
				break
			}
		}
	}
	if len(stacks) == 0 {
		return "<none>"
	}
	return strings.Join(stacks, "\n\n")
}

func newGoroutineLeakClient(t *testing.T, opts ...f1.Option) *f1.Client {
	t.Helper()
	baseOpts := []f1.Option{
		f1.WithDriver(inmem.New()),
		f1.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		f1.WithPublishTopics("orders.created"),
	}
	baseOpts = append(baseOpts, opts...)
	client, err := f1.New(context.Background(), goroutineLeakConfig(), baseOpts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = closeGoroutineLeakClient(client) })
	return client
}

func goroutineLeakConfig() f1.Config {
	return f1.Config{
		Env:     "test",
		Service: "orders",
		Broker:  f1.BrokerConfig{Driver: "inmem"},
		Topology: f1.TopologyConfig{
			AutoCreate: true,
		},
	}
}

func closeGoroutineLeakClient(client *f1.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return client.Close(ctx)
}

func runGoroutineLeakSubscribeDrainClose(t *testing.T, afterClose func(*testing.T), opts ...f1.Option) {
	t.Helper()
	client := newGoroutineLeakClient(t, opts...)
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handled := make(chan struct{})
	runner, err := client.Subscribe(context.Background(), goroutineLeakSubscription(handled))
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(runCtx) }()
	if _, err := client.Publisher().Publish(context.Background(), "orders.created", map[string]string{"id": "drain"}); err != nil {
		t.Fatal(err)
	}
	waitGoroutineLeakSignal(t, handled, "handler did not observe the published event")
	if err := drainGoroutineLeakRunner(runner); err != nil {
		t.Fatal(err)
	}
	waitGoroutineLeakRun(t, runDone)
	if err := closeGoroutineLeakClient(client); err != nil {
		t.Fatal(err)
	}
	if afterClose != nil {
		afterClose(t)
	}
}

func goroutineLeakSubscription(handled chan<- struct{}) f1.Subscription {
	return f1.Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Concurrency:    1,
		Prefetch:       1,
		Priorities:     []f1.Priority{f1.PriorityMedium},
		Retry:          f1.RetryConfig{MaxAttempts: 1},
		HandlerTimeout: time.Second,
		Handlers: map[string]f1.Handler{
			"orders.created": f1.HandlerFunc(func(context.Context, *f1.Event) error {
				if handled != nil {
					close(handled)
				}
				return nil
			}),
		},
	}
}

func drainGoroutineLeakRunner(runner *f1.Runner) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return runner.Drain(ctx)
}

func waitGoroutineLeakSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	timer := clock.NewReal().Timer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatal(message)
	}
}

func waitGoroutineLeakRun(t *testing.T, runDone <-chan error) {
	t.Helper()
	timer := clock.NewReal().Timer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-runDone:
	case <-timer.C:
		t.Fatal("Run did not return")
	}
}
