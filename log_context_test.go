package f1_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	//nolint:depguard // the log context test exercises the public client through the in-memory driver.
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/f1otel"

	//nolint:depguard // provider construction is part of the log context test.
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	//nolint:depguard // provider construction is part of the log context test.
	"go.opentelemetry.io/otel/trace"
)

type capturedLog struct {
	message   string
	attrs     map[string]slog.Value
	spanValid bool
}

type logCapture struct {
	mu      sync.Mutex
	records []capturedLog
	seen    chan struct{}
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool {
	return true
}

func (c *logCapture) Handle(ctx context.Context, record slog.Record) error {
	captured := capturedLog{
		message:   record.Message,
		attrs:     make(map[string]slog.Value),
		spanValid: trace.SpanContextFromContext(ctx).IsValid(),
	}
	record.Attrs(func(attr slog.Attr) bool {
		captured.attrs[attr.Key] = attr.Value
		return true
	})

	c.mu.Lock()
	c.records = append(c.records, captured)
	c.mu.Unlock()
	select {
	case c.seen <- struct{}{}:
	default:
	}
	return nil
}

func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler {
	return c
}

func (c *logCapture) WithGroup(string) slog.Handler {
	return c
}

func (c *logCapture) waitFor(t *testing.T, message string) capturedLog {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		c.mu.Lock()
		for _, record := range c.records {
			if record.message == message {
				c.mu.Unlock()
				return record
			}
		}
		c.mu.Unlock()

		select {
		case <-c.seen:
		case <-ctx.Done():
			t.Fatalf("did not capture %q", message)
		}
	}
}

func TestLogContext(t *testing.T) {
	t.Run("stuck worker carries process span and message fields", func(t *testing.T) {
		capture := &logCapture{seen: make(chan struct{}, 32)}
		observer := newLogContextObserver(t)
		client := newLogContextClient(t, capture, observer)
		runCtx, cancelRun := context.WithCancel(context.Background())
		defer cancelRun()

		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
		handlerStarted := make(chan struct{})
		handlerDone := make(chan struct{})
		runner, err := client.Subscribe(context.Background(), f1.Subscription{
			Name:           "orders",
			Topics:         []string{"orders.created"},
			Concurrency:    1,
			Prefetch:       1,
			Priorities:     []f1.Priority{f1.PriorityMedium},
			Retry:          f1.RetryConfig{MaxAttempts: 1},
			HandlerTimeout: 20 * time.Millisecond,
			Handlers: map[string]f1.Handler{
				"orders.created": f1.HandlerFunc(func(context.Context, *f1.Event) error {
					close(handlerStarted)
					<-release
					close(handlerDone)
					return nil
				}),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		runDone := make(chan error, 1)
		go func() { runDone <- runner.Run(runCtx) }()
		defer func() {
			releaseHandler()
			cancelRun()
			closeLogContextClient(t, client)
		}()

		eventID, err := client.Publisher().Publish(context.Background(), "orders.created", map[string]string{"id": "stuck"})
		if err != nil {
			t.Fatal(err)
		}
		waitForLogContextSignal(t, handlerStarted, "handler did not start")

		record := capture.waitFor(t, "f1 stuck worker")
		if !record.spanValid {
			t.Fatal("stuck worker log has no valid span context")
		}
		if got := logContextAttr(t, record, "event_id"); got != eventID {
			t.Fatalf("stuck worker event_id = %q, want %q", got, eventID)
		}
		if got := logContextAttr(t, record, "topic"); got != "orders.created" {
			t.Fatalf("stuck worker topic = %q, want orders.created", got)
		}

		releaseHandler()
		waitForLogContextSignal(t, handlerDone, "handler did not finish after release")
		cancelRun()
		waitForLogContextRun(t, runDone)
	})

	t.Run("terminal notification panic carries parent span", func(t *testing.T) {
		capture := &logCapture{seen: make(chan struct{}, 32)}
		observer := newLogContextObserver(t)
		client := newLogContextClient(t, capture, observer)
		defer closeLogContextClient(t, client)

		tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
		t.Cleanup(func() {
			if err := tracerProvider.Shutdown(context.Background()); err != nil {
				t.Error(err)
			}
		})
		runRoot, runSpan := tracerProvider.Tracer("log-context-test").Start(context.Background(), "runner")
		defer runSpan.End()
		runCtx, cancelRun := context.WithCancel(runRoot)
		defer cancelRun()

		runner, err := client.Subscribe(context.Background(), f1.Subscription{
			Name:           "orders",
			Topics:         []string{"orders.created"},
			Concurrency:    1,
			Prefetch:       1,
			Priorities:     []f1.Priority{f1.PriorityMedium},
			Retry:          f1.RetryConfig{MaxAttempts: 1},
			HandlerTimeout: time.Second,
			Handlers: map[string]f1.Handler{
				"orders.created": f1.HandlerFunc(func(context.Context, *f1.Event) error {
					return f1.Terminal(errors.New("terminal test error"))
				}),
			},
			OnDeadLetter: func(context.Context, f1.DeadLettered) {
				panic("terminal notification test panic")
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		runDone := make(chan error, 1)
		go func() { runDone <- runner.Run(runCtx) }()

		if _, err := client.Publisher().Publish(context.Background(), "orders.created", map[string]string{"id": "panic"}); err != nil {
			t.Fatal(err)
		}
		record := capture.waitFor(t, "f1 terminal notification panicked")
		if !record.spanValid {
			t.Fatal("terminal notification log has no valid span context")
		}
		if got := logContextAttr(t, record, "kind"); got != "dead-letter" {
			t.Fatalf("terminal notification kind = %q, want dead-letter", got)
		}

		cancelRun()
		waitForLogContextRun(t, runDone)
	})
}

func newLogContextObserver(t *testing.T) f1.Observer {
	t.Helper()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() {
		if err := tracerProvider.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	observer, err := f1otel.New(f1otel.WithTracerProvider(tracerProvider))
	if err != nil {
		t.Fatal(err)
	}
	return observer
}

func newLogContextClient(t *testing.T, capture *logCapture, observer f1.Observer) *f1.Client {
	t.Helper()
	client, err := f1.New(context.Background(), f1.Config{
		Env:     "test",
		Service: "orders",
		Broker:  f1.BrokerConfig{Driver: "inmem"},
		Topology: f1.TopologyConfig{
			AutoCreate: true,
		},
	},
		f1.WithDriver(inmem.New()),
		f1.WithLogger(slog.New(capture)),
		f1.WithObserver(observer),
		f1.WithPublishTopics("orders.created"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func closeLogContextClient(t *testing.T, client *f1.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func waitForLogContextSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatal(message)
	}
}

func waitForLogContextRun(t *testing.T, runDone <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("runner returned error: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("runner did not stop")
	}
}

func logContextAttr(t *testing.T, record capturedLog, key string) string {
	t.Helper()
	value, ok := record.attrs[key]
	if !ok {
		t.Fatalf("log %q has no %s attribute", record.message, key)
	}
	return value.String()
}
