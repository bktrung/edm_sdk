package f1

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
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
		Priority:    PriorityMedium,
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
	if !strings.Contains(output.String(), "lane=orders.created.medium.main") {
		t.Fatalf("unknown-lane warning = %q, want rejected lane id", output.String())
	}
}

func TestLaneFullStillAppliesBackpressure(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	runner := &Runner{
		client: &Client{options: clientOptions{clock: fake}},
		subscription: Subscription{
			Topics:      []string{"orders.created"},
			Priorities:  []Priority{PriorityHigh, PriorityMedium},
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

// levelRecorder is a slog.Handler that keeps every record's level and message
// so a test can assert on levels rather than parse formatted output. waited
// receives each record's message, which lets a test block until the specific
// record under test arrives: the consumer loop only returns once its context
// is cancelled, so a notification record has no other completion signal.
type levelRecorder struct {
	mu      sync.Mutex
	records []slog.Record
	waited  chan string
}

func newLevelRecorder() *levelRecorder {
	return &levelRecorder{waited: make(chan string, 32)}
}

func (*levelRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *levelRecorder) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, record.Clone())
	h.mu.Unlock()
	h.waited <- record.Message
	return nil
}

func (h *levelRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *levelRecorder) WithGroup(string) slog.Handler      { return h }

func (h *levelRecorder) snapshot() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]slog.Record(nil), h.records...)
}

// waitFor blocks until a record with message arrives, so a test does not race
// the goroutine logging it. Client construction logs capability records
// through the same handler, so messages other than the wanted one are
// discarded rather than treated as the signal.
func (h *levelRecorder) waitFor(t *testing.T, message string) {
	t.Helper()
	deadline := oneSecondTimer(t).C
	for {
		select {
		case got := <-h.waited:
			if got == message {
				return
			}
		case <-deadline:
			t.Fatalf("no %q log record; records = %v", message, h.snapshot())
		}
	}
}

func recordsWithMessage(records []slog.Record, message string) []slog.Record {
	matches := make([]slog.Record, 0, 1)
	for _, record := range records {
		if record.Message == message {
			matches = append(matches, record)
		}
	}
	return matches
}

func TestConsumerNotificationLogsAtInfo(t *testing.T) {
	recorder := newLevelRecorder()
	client, runner, consumer := newLoggerConsumerRunner(t, slog.New(recorder), nil)
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- consumeRunnerErrors(runner, ctx) }()

	consumer.errs <- &driver.Error{Driver: "inmem", Op: "consumer", K: driver.KindNotification, Err: errors.New("kafka partitions assigned")}
	recorder.waitFor(t, "f1 consumer notification")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("consumeRunnerErrors() error = %v", err)
	}

	records := recorder.snapshot()
	notifications := recordsWithMessage(records, "f1 consumer notification")
	if len(notifications) != 1 {
		t.Fatalf("f1 consumer notification log records = %v, want exactly one", notifications)
	}
	if got := notifications[0].Level; got != slog.LevelInfo {
		t.Fatalf("consumer notification level = %v, want INFO", got)
	}
	for _, record := range records {
		if record.Level >= slog.LevelError {
			t.Fatalf("consumer notification produced an error record: %v", record)
		}
	}
}

func TestConsumerRealErrorStillLogsAtError(t *testing.T) {
	consumerErrors := map[string]error{
		"transient":    &driver.Error{Driver: "inmem", Op: "consumer", K: driver.KindTransient, Err: errors.New("connection lost")},
		"unclassified": errors.New("connection lost"),
	}
	for name, consumerErr := range consumerErrors {
		t.Run(name, func(t *testing.T) {
			recorder := newLevelRecorder()
			client, runner, consumer := newLoggerConsumerRunner(t, slog.New(recorder), nil)
			t.Cleanup(func() { _ = client.Close(context.Background()) })

			consumer.errs <- consumerErr
			if err := consumeRunnerErrors(runner, context.Background()); err != nil {
				t.Fatalf("consumeRunnerErrors() error = %v", err)
			}

			records := recorder.snapshot()
			failures := recordsWithMessage(records, "f1 consumer error")
			if len(failures) != 1 {
				t.Fatalf("f1 consumer error log records = %v, want exactly one", failures)
			}
			if got := failures[0].Level; got != slog.LevelError {
				t.Fatalf("consumer error level = %v, want ERROR", got)
			}
			if got := recordsWithMessage(records, "f1 consumer notification"); len(got) != 0 {
				t.Fatalf("consumer error logged as a notification: %v", got)
			}
		})
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
		Priorities:     []Priority{PriorityMedium},
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
