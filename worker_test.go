package f1

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/sched"
)

func TestUnknownLaneDoesNotWedgeRun(t *testing.T) {
	consumer := newDispatchConsumer()
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: &dispatchConn{
		producer: &dispatchProducer{},
		consumer: consumer,
		admin:    &dispatchAdmin{},
	}}))
	if err != nil {
		t.Fatal(err)
	}
	client.config.Lifecycle.DrainTimeout = 10 * time.Millisecond
	defer func() { _ = client.Close(context.Background()) }()

	settled := make(chan struct{})
	settler := &dispatchSettler{onSettle: func() { close(settled) }}
	runner := &Runner{client: client, subscription: pipelineTestSubscription(func(context.Context, *Event) error { return nil })}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "evt-unknown-priority",
		Source:      "/test/orders",
		Type:        "orders.created",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	consumer.messages <- retryBridgeMessage(t, envelope, settler)

	runDone := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { runDone <- runner.Run(ctx) }()
	select {
	case err := <-runDone:
		t.Fatalf("Run returned before unknown-lane message settled: %v", err)
	case <-settled:
		cancel()
	case <-oneSecondTimer(t).C:
		cancel()
		t.Fatal("unknown-lane message was not processed")
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil after fallback delivery", err)
		}
	case <-oneSecondTimer(t).C:
		t.Fatal("Run() did not return after unknown-lane delivery")
	}
}

func TestPipelineErrorCancelsTheGeneration(t *testing.T) {
	consumer := newDispatchConsumer()
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: &dispatchConn{
		producer: &dispatchProducer{},
		consumer: consumer,
		admin:    &dispatchAdmin{},
	}}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()

	runner := &Runner{client: client, subscription: Subscription{Name: "orders"}}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(context.Background()) }()

	select {
	case err := <-runDone:
		if err == nil || !strings.Contains(err.Error(), "sched: at least one lane is required") {
			t.Fatalf("Run() error = %v, want scheduler construction error", err)
		}
	case <-oneSecondTimer(t).C:
		t.Fatal("Run() did not return after the dispatch pipeline error")
	}
	if runner.runCtx == nil || runner.runCtx.Err() == nil {
		t.Fatal("generation context was not canceled after pipeline error")
	}
	consumer.mu.Lock()
	drained := consumer.drained
	consumer.mu.Unlock()
	if !drained {
		t.Fatal("fetch runner did not observe generation cancellation")
	}
}

func TestUnknownLaneRoutesToTheFallbackLane(t *testing.T) {
	var output logSink
	client, runner := newRetryBridgeRunner(t, &dispatchProducer{}, "orders.created",
		WithLogger(slog.New(slog.NewTextHandler(&output, nil))))
	defer func() { _ = client.Close(context.Background()) }()
	runner.subscription.Concurrency = 1
	runner.subscription.Prefetch = 1
	runner.subscription.Handlers = map[string]Handler{
		"orders.created": HandlerFunc(func(context.Context, *Event) error { return nil }),
	}
	runner.inflight = newInflightRegistry()
	settler := &dispatchSettler{}
	message := retryBridgeMessage(t, Envelope{
		SpecVersion: "1.0",
		ID:          "evt-fallback-lane",
		Source:      "/test/orders",
		Type:        "orders.created",
		Priority:    PriorityNormal,
		Attempt:     1,
	}, settler)
	id := runner.inflight.Add(message)
	deliveries := make(chan delivery, 1)
	deliveries <- delivery{id: id, message: message}
	close(deliveries)

	pipelineDone := make(chan error, 1)
	go func() { pipelineDone <- runDispatchPipeline(runner, context.Background(), deliveries) }()
	select {
	case err := <-pipelineDone:
		if err != nil {
			t.Fatalf("runDispatchPipeline() error = %v, want nil after fallback", err)
		}
	case <-oneSecondTimer(t).C:
		t.Fatal("runDispatchPipeline() did not process fallback-lane delivery")
	}
	if !settler.acked {
		t.Fatal("fallback-lane delivery was not processed and acked")
	}
	if got := strings.Count(output.String(), "f1 unknown delivery lane; routing to fallback lane"); got != 1 {
		t.Fatalf("unknown-lane warning count = %d, want 1; output=%q", got, output.String())
	}
	if !strings.Contains(output.String(), "lane=orders.created.normal.main") {
		t.Fatalf("unknown-lane warning = %q, want rejected lane id", output.String())
	}
}

func TestLaneFullStillAppliesBackpressure(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	runner := &Runner{
		client: &Client{options: clientOptions{clock: fake}},
		subscription: Subscription{
			Topics:      []string{"orders.created"},
			Priorities:  []Priority{PriorityHigh, PriorityNormal},
			Concurrency: 1,
			Fairness:    FairnessConfig{PrefetchFactor: 1},
			Retry:       RetryConfig{MaxAttempts: 1},
		},
	}
	scheduler, err := newRunnerScheduler(runner)
	if err != nil {
		t.Fatal(err)
	}
	topic := topicFor(runner.subscription.Topics[0])
	lane := schedulerLaneID(topic, runner.subscription.Priorities[1], 0)
	fallbackLane := schedulerLaneID(topic, runner.subscription.Priorities[0], 0)
	if fallbackLane == lane {
		t.Fatalf("fallback lane = %q, want a distinct lane", fallbackLane)
	}
	item := sched.Item{Value: "delivery"}
	capacity := 0
	for {
		err := scheduler.Enqueue(lane, item)
		if errors.Is(err, sched.ErrLaneFull) {
			break
		}
		if err != nil {
			t.Fatalf("enqueue = %v, want nil or ErrLaneFull", err)
		}
		capacity++
	}
	if capacity == 0 {
		t.Fatal("lane capacity = 0, want positive capacity")
	}
	gotLane, queued, err := enqueuePendingDelivery(runner, scheduler, &delivery{}, lane)
	if err != nil {
		t.Fatalf("enqueuePendingDelivery() error = %v, want nil", err)
	}
	if queued {
		t.Fatal("enqueuePendingDelivery() rerouted a full lane")
	}
	if gotLane != lane {
		t.Fatalf("pending lane = %q, want %q", gotLane, lane)
	}
	if got := scheduler.Depth(lane); got != capacity {
		t.Fatalf("filled lane depth = %d, want unchanged capacity %d", got, capacity)
	}
	if got := scheduler.Depth(fallbackLane); got != 0 {
		t.Fatalf("fallback lane depth = %d, want 0", got)
	}
}

func TestTruncateErrorBacksToRuneBoundary(t *testing.T) {
	const prefixBytes = deathErrorCap - 2
	value := strings.Repeat("a", prefixBytes) + "\u754c" + "suffix"
	got := truncateError(errors.New(value))

	if !utf8.ValidString(got) {
		t.Fatalf("truncateError() returned invalid UTF-8")
	}
	if len(got) > deathErrorCap {
		t.Fatalf("truncateError() length = %d, want at most %d", len(got), deathErrorCap)
	}
	if len(got) != prefixBytes {
		t.Fatalf("truncateError() length = %d, want %d", len(got), prefixBytes)
	}
}

func TestTruncateErrorKeepsExactByteCap(t *testing.T) {
	value := strings.Repeat("a", deathErrorCap)
	got := truncateError(errors.New(value))

	if len(got) != deathErrorCap {
		t.Fatalf("truncateError() length = %d, want %d", len(got), deathErrorCap)
	}
	if got != value {
		t.Fatalf("truncateError() changed ASCII text at the exact cap")
	}
}

func TestTruncateErrorKeepsOverCapAlignedBoundary(t *testing.T) {
	value := strings.Repeat("a", deathErrorCap) + "\u754c"
	got := truncateError(errors.New(value))
	want := value[:deathErrorCap]

	if len(got) != deathErrorCap {
		t.Fatalf("truncateError() length = %d, want %d", len(got), deathErrorCap)
	}
	if got != want {
		t.Fatalf("truncateError() = %q, want the first %d bytes", got, deathErrorCap)
	}
}

func pipelineTestSubscription(handler func(context.Context, *Event) error) Subscription {
	var handlers map[string]Handler
	if handler != nil {
		handlers = map[string]Handler{"orders.created": HandlerFunc(handler)}
	}
	return Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Concurrency:    1,
		Prefetch:       1,
		Priorities:     []Priority{PriorityNormal},
		Retry:          RetryConfig{MaxAttempts: 1},
		HandlerTimeout: time.Second,
		Handlers:       handlers,
	}
}

func oneSecondTimer(t *testing.T) clock.Timer {
	t.Helper()
	timer := clock.NewReal().Timer(time.Second)
	t.Cleanup(func() { timer.Stop() })
	return timer
}
