package f1

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
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
	conn := &dispatchConn{producer: &dispatchProducer{}, consumer: consumer, admin: &dispatchAdmin{}}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: conn}))
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
	conn.mu.Lock()
	generationCtx := conn.generationCtx
	conn.mu.Unlock()
	if generationCtx == nil || generationCtx.Err() == nil {
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
	id := runner.inflight.Add()
	deliveries := make(chan delivery, 1)
	deliveries <- delivery{id: id, message: message}
	close(deliveries)

	pipelineDone := make(chan error, 1)
	go func() {
		pipelineDone <- runDispatchPipeline(runner, context.Background(), deliveries, runner.config.Prefetch)
	}()
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

func TestTruncateErrorBoundaries(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		want  string
	}{
		{
			name:  "backs off a split rune",
			value: strings.Repeat("a", deathErrorCap-2) + "\u754c" + "suffix",
			want:  strings.Repeat("a", deathErrorCap-2),
		},
		{
			name:  "keeps text at the exact cap",
			value: strings.Repeat("a", deathErrorCap),
			want:  strings.Repeat("a", deathErrorCap),
		},
		{
			name:  "cuts at an aligned boundary over the cap",
			value: strings.Repeat("a", deathErrorCap) + "\u754c",
			want:  strings.Repeat("a", deathErrorCap),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := truncateError(errors.New(test.value))
			if !utf8.ValidString(got) {
				t.Fatalf("truncateError() returned invalid UTF-8")
			}
			if len(got) > deathErrorCap {
				t.Fatalf("truncateError() length = %d, want at most %d", len(got), deathErrorCap)
			}
			if got != test.want {
				t.Fatalf("truncateError() = %d bytes, want the first %d bytes", len(got), len(test.want))
			}
		})
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

// TestFetchRunnerDrainsWhenCancelledDuringHandOver pins the cancel path where
// the fetch loop is already inside enqueueDelivery: the lane gate holds
// deliveries back, so the hand-over can be blocked when the context fires, and
// a return that skips the drain leaves the driver's handed-over messages
// unsettled and fails Stop. It mirrors the running loop's ctx.Done case, which
// drains before returning.
func TestFetchRunnerDrainsWhenCancelledDuringHandOver(t *testing.T) {
	_, runner, consumer := newSettlementOrderingRunner(t)
	settler := &scriptedNackSettler{}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "refused-hand-over",
		Source:      "/test/orders",
		Type:        "orders.created",
		Attempt:     1,
	}
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	consumer.messages <- driver.InboundMessage{
		Destination: "f1.test.orders.created.medium",
		Headers:     headerSlice(headers),
		Body:        []byte(`{}`),
		Settle:      settler,
	}
	// Unbuffered and never read: the hand-over cannot complete, so the loop
	// blocks in enqueueDelivery's select until the cancellation reaches it.
	dispatch := make(chan delivery)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fetchRunner(runner, ctx, dispatch) }()
	// The in-flight count is the only signal that the loop is inside the
	// hand-over: enqueueDelivery adds the entry before its select, so a count
	// of one means the send is blocked. Cancelling earlier would exercise the
	// ctx.Done case above the read instead, not the refusal.
	timer := oneSecondTimer(t)
	for runner.inflight.Len() != 1 {
		select {
		case <-timer.C:
			t.Fatalf("inflight length = %d, want 1 with a hand-over in progress", runner.inflight.Len())
		default:
		}
		if err := clock.NewReal().Sleep(ctx, time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	timer = oneSecondTimer(t)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("fetchRunner() = %v, want nil after the cancellation", err)
		}
	case <-timer.C:
		t.Fatal("fetchRunner did not return after the cancellation")
	}
	consumer.mu.Lock()
	drained := consumer.drained
	consumer.mu.Unlock()
	if !drained {
		t.Fatal("fetchRunner returned without draining the consumer after a refused hand-over")
	}
	nacks, acks, outstanding := settler.state()
	if nacks != 1 || acks != 0 || outstanding {
		t.Fatalf("settlement calls = nacks %d, acks %d, outstanding %v, want one nack and nothing outstanding", nacks, acks, outstanding)
	}
	if runner.inflight.Len() != 0 {
		t.Fatalf("inflight after the refused hand-over = %d, want 0", runner.inflight.Len())
	}
}

func oneSecondTimer(t *testing.T) clock.Timer {
	t.Helper()
	timer := clock.NewReal().Timer(time.Second)
	t.Cleanup(func() { timer.Stop() })
	return timer
}

// TestRetryAndDeadLetterCopiesBothUnencodableDropOnce proves the encode twin of
// the too-large chain: a retry copy that will not fit the header cap is handed
// to the dead-letter path instead of being settled, and the dead-letter copy
// does not fit either, for a different reason than the retry copy's: it is
// built from the original envelope, so it carries no due time, and it adds the
// never-shed f1deathreason and f1deathtime on top of the cap the original
// already needs. A copy the message itself makes unbuildable is dropped: the
// original is acknowledged and never nacked, the consumer is not released, and
// one drop report replaces the hand-off failure. The subscription is not
// stopped, so no health error is recorded.
func TestRetryAndDeadLetterCopiesBothUnencodableDropOnce(t *testing.T) {
	recorder := newErrorHandlerRecorder()
	consumer := newDispatchConsumer()
	producer := &dispatchProducer{}
	client, runner := newErrorHandlerRunner(t, producer, consumer, recorder.handle)
	defer func() { _ = client.Close(context.Background()) }()

	settler := &retryBridgeSettler{}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "retry-copy-too-large",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
		// f1partitionkey is never shed, so it pins the envelope's floor.
		PartitionKey: strings.Repeat("k", 2000),
	}
	message := retryBridgeMessage(t, envelope, settler)

	// The cap is sized from this envelope rather than from a constant: the
	// encoded size moves with the timestamp format and the header names, and a
	// hard-coded cap would let the test pass or fail for the wrong reason.
	// EncodeHeaders coerces a non-positive cap up to CoreMaxHeaderBytes, so the
	// smallest cap the envelope fits is searched from 1, and "an envelope fits
	// at cap n" is monotone in n.
	limit := sort.Search(CoreMaxHeaderBytes, func(n int) bool {
		_, err := envelope.EncodeHeaders(n + 1)
		return err == nil
	}) + 1

	// Guard the premise: the original fits at limit by construction, and the
	// retry copy has to not fit, or the call below would exercise the publish
	// path instead of the one under test.
	retryCopy := envelope
	retryCopy.OriginalDest = message.Destination
	retryCopy.Attempt = envelope.Attempt + 1
	retryCopy.MaxAttempts = effectiveMaxAttempts(envelope.MaxAttempts, runner.subscription.Retry.MaxAttempts)
	due := runner.client.options.clock.Now().UTC().Add(time.Second)
	retryCopy.DueTime = &due
	if _, err := retryCopy.EncodeHeaders(limit); !errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatalf("retry copy EncodeHeaders(%d) error = %v, want %v: the premise that the copy cannot be built does not hold", limit, err, ErrEnvelopeTooLarge)
	}

	if !retryAndSettle(runner, context.Background(), message, envelope, errors.New("temporary"), &deliveryState{headerMaxBytes: limit}) {
		t.Fatal("retryAndSettle did not report the delivery settled: a copy that can never be built is dropped rather than handed back")
	}
	if settler.acks != 1 {
		t.Fatalf("Ack calls = %d, want 1: the drop acknowledges the original", settler.acks)
	}
	if len(settler.nacks) != 0 {
		t.Fatalf("Nack calls = %+v, want none: a dropped delivery is acknowledged, never handed back", settler.nacks)
	}
	producer.mu.Lock()
	published := len(producer.messages)
	producer.mu.Unlock()
	if published != 0 {
		t.Fatalf("published messages = %d, want 0: neither copy can be encoded, so neither is published", published)
	}
	consumer.mu.Lock()
	released := consumer.released
	consumer.mu.Unlock()
	if released {
		t.Fatal("consumer was released: a dropped delivery is settled, so there is nothing to hand back for redelivery")
	}

	recorder.waitForCall(t, time.Second)
	if got := recorder.count(); got != 1 {
		t.Fatalf("error handler calls = %d, want 1: the chain is reported once", got)
	}
	recorder.mu.Lock()
	call := recorder.calls[0]
	recorder.mu.Unlock()
	if !strings.HasPrefix(call.err.Error(), "f1: message dropped: event_id=") {
		t.Fatalf("error = %q, want the message-drop headline a dropped delivery is reported under", call.err)
	}
	if classified, ok := errors.AsType[*driver.Error](call.err); ok {
		t.Fatalf("error = %v, want a drop report, got the classified %s hand-off failure", call.err, classified.Op)
	}
	if !errors.Is(call.err, ErrEnvelopeTooLarge) {
		t.Fatalf("error = %v, want it to wrap %v", call.err, ErrEnvelopeTooLarge)
	}
	if call.event == nil {
		t.Fatal("event = nil, want the failed delivery's Event")
	}
	if got := call.event.ID(); got != envelope.ID {
		t.Fatalf("event ID = %q, want %q", got, envelope.ID)
	}
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health() = %v, want nil: a dropped message does not stop the subscription", err)
	}
}

// These regressions exercise runner-lifetime failure through the driver port:
// the budget is a terminal threshold, not a cap on already-admitted work.
type leftoverRunnerFixture struct {
	client   *Client
	runner   *Runner
	driver   *laneRepairDriver
	clock    *recordingClock
	consumer *laneRepairConsumer
	done     chan error
}

func newLeftoverRunnerFixture(t *testing.T, sub Subscription) *leftoverRunnerFixture {
	t.Helper()
	recorded := &recordingClock{
		Fake:         clock.NewFake(time.Unix(0, 0)),
		timerStarted: make(chan time.Duration, 128),
	}
	d := &laneRepairDriver{created: make(chan *laneRepairConsumer, 16)}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(d), withClock(recorded))
	if err != nil {
		t.Fatal(err)
	}
	runner, err := client.Subscribe(context.Background(), sub)
	if err != nil {
		_ = client.Close(context.Background())
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	fixture := &leftoverRunnerFixture{
		client: client, runner: runner, driver: d, clock: recorded, done: make(chan error, 1),
	}
	finished := make(chan struct{})
	go func() {
		fixture.done <- runner.Run(ctx)
		close(finished)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-oneSecondTimer(t).C:
			t.Error("Run did not finish during cleanup")
		}
		_ = client.Close(context.Background())
	})
	fixture.consumer = waitLaneConsumer(t, d, sub.Name)
	return fixture
}

func waitLeftoverTimer(t *testing.T, recorded *recordingClock, duration time.Duration) {
	t.Helper()
	watchdog := oneSecondTimer(t)
	for {
		select {
		case got := <-recorded.timerStarted:
			if got == duration {
				return
			}
		case <-watchdog.C:
			t.Fatalf("timer %v was not registered", duration)
		}
	}
}

func waitLeftoverSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-oneSecondTimer(t).C:
		t.Fatalf("timed out waiting for %s", description)
	}
}

func abandonLeftoverHandler(t *testing.T, fixture *leftoverRunnerFixture) {
	t.Helper()
	phase := fixture.runner.subscription.HandlerTimeout * stuckPhaseMultiplier
	waitLeftoverTimer(t, fixture.clock, phase)
	fixture.clock.Advance(phase)
	waitLeftoverTimer(t, fixture.clock, phase)
	fixture.clock.Advance(phase)
}

func assertLeftoverFailure(t *testing.T, fixture *leftoverRunnerFixture) {
	t.Helper()
	select {
	case err := <-fixture.done:
		kind, classified := driver.Classify(err)
		if err == nil || !classified || kind != driver.KindFatal {
			t.Fatalf("Run() = %v, want classified fatal leftover-budget failure", err)
		}
		if !strings.Contains(err.Error(), fixture.runner.subscription.Name) {
			t.Fatalf("Run() = %v, want subscription name", err)
		}
	case <-oneSecondTimer(t).C:
		t.Fatal("Run did not fail while leftover user functions remained blocked")
	}
	if err := fixture.client.Health(context.Background()); err == nil || !strings.Contains(err.Error(), fixture.runner.subscription.Name) {
		t.Fatalf("Health() = %v, want failed subscription %q", err, fixture.runner.subscription.Name)
	}
	select {
	case _, open := <-fixture.consumer.Messages():
		if open {
			t.Fatal("failed consumer still handed over a delivery")
		}
	default:
		t.Fatal("failed consumer intake was not stopped")
	}
}

func leftoverMessage(t *testing.T, id string, settler driver.Settler) driver.InboundMessage {
	t.Helper()
	message := laneRepairMessage(t, id, "orders.created")
	message.Settle = settler
	return message
}

func TestRunnerFailsAtLeftoverHandlerBudget(t *testing.T) {
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	sub := pipelineTestSubscription(func(context.Context, *Event) error {
		started <- struct{}{}
		<-release
		return nil
	})
	sub.Concurrency = 2
	sub.Prefetch = 2
	sub.HandlerTimeout = 10 * time.Millisecond
	fixture := newLeftoverRunnerFixture(t, sub)
	t.Cleanup(func() { close(release) })

	firstSettled := make(chan struct{})
	first := &dispatchSettler{onSettle: func() { close(firstSettled) }}
	fixture.consumer.send(leftoverMessage(t, "always-hangs", first))
	waitLeftoverSignal(t, started, "first hanging handler")
	abandonLeftoverHandler(t, fixture)
	waitLeftoverSignal(t, firstSettled, "first abandoned delivery requeue")
	first.mu.Lock()
	nacked, acked := first.nacked, first.acked
	first.mu.Unlock()
	if !nacked || acked {
		t.Fatalf("first settlement = nack %v, ack %v, want requeue", nacked, acked)
	}
	if err := fixture.client.Health(context.Background()); err != nil {
		t.Fatalf("Health below threshold = %v, want nil", err)
	}

	fixture.consumer.send(leftoverMessage(t, "always-hangs", &dispatchSettler{}))
	waitLeftoverSignal(t, started, "redelivered hanging handler")
	abandonLeftoverHandler(t, fixture)
	assertLeftoverFailure(t, fixture)
	select {
	case <-started:
		t.Fatal("a third handler started after budget failure")
	default:
	}
	shutdown := make(chan struct{})
	go func() {
		_ = fixture.runner.Drain(context.Background())
		_ = fixture.client.Close(context.Background())
		close(shutdown)
	}()
	waitLeftoverSignal(t, shutdown, "bounded Drain and Close with both handlers blocked")
}

func TestDiscardedCallbackSharesLeftoverBudget(t *testing.T) {
	for _, terminal := range []string{"discarded", "dead-letter"} {
		t.Run(terminal, func(t *testing.T) {
			handlerStarted := make(chan struct{}, 1)
			callbackStarted := make(chan struct{})
			callbackExpired := make(chan struct{})
			callbackContexts := make(chan context.Context, 1)
			release := make(chan struct{})
			sub := pipelineTestSubscription(func(_ context.Context, event *Event) error {
				if event.ID() == "handler-leftover" {
					handlerStarted <- struct{}{}
					<-release
					return nil
				}
				if terminal == "dead-letter" {
					return Terminal(errors.New("dead-letter this delivery"))
				}
				return Drop(errors.New("discard this delivery"))
			})
			sub.Concurrency = 2
			sub.Prefetch = 2
			sub.HandlerTimeout = 10 * time.Millisecond
			callback := func(ctx context.Context) {
				callbackContexts <- ctx
				close(callbackStarted)
				<-ctx.Done()
				close(callbackExpired)
				<-release
			}
			if terminal == "dead-letter" {
				sub.OnDeadLetter = func(ctx context.Context, _ DeadLettered) { callback(ctx) }
			} else {
				sub.OnDiscarded = func(ctx context.Context, _ Discarded) { callback(ctx) }
			}
			fixture := newLeftoverRunnerFixture(t, sub)
			t.Cleanup(func() { close(release) })
			requeued := make(chan struct{})
			fixture.consumer.send(leftoverMessage(t, "handler-leftover", &dispatchSettler{
				onSettle: func() { close(requeued) },
			}))
			waitLeftoverSignal(t, handlerStarted, "hanging handler")
			abandonLeftoverHandler(t, fixture)
			waitLeftoverSignal(t, requeued, "handler requeue")

			settled := make(chan struct{})
			publishedBeforeAck := make(chan bool, 1)
			settler := &dispatchSettler{onSettle: func() {
				fixture.driver.mu.Lock()
				publishedBeforeAck <- fixture.driver.publishCalls > 0
				fixture.driver.mu.Unlock()
				close(settled)
			}}
			fixture.consumer.send(leftoverMessage(t, "terminal-callback", settler))
			waitLeftoverSignal(t, settled, "terminal delivery Ack")
			settler.mu.Lock()
			acked, nacked := settler.acked, settler.nacked
			settler.mu.Unlock()
			if !acked || nacked {
				t.Fatalf("terminal settlement = ack %v, nack %v, want Ack", acked, nacked)
			}
			if got := <-publishedBeforeAck; terminal == "dead-letter" && !got {
				t.Fatal("dead-letter original was acknowledged before successor publication")
			}
			waitLeftoverSignal(t, callbackStarted, "terminal callback")
			waitLeftoverTimer(t, fixture.clock, time.Second)
			callbackCtx := <-callbackContexts
			deadline, ok := callbackCtx.Deadline()
			if want := fixture.clock.Now().Add(time.Second); !ok || !deadline.Equal(want) {
				t.Fatalf("callback Deadline() = %v, %v, want %v", deadline, ok, want)
			}
			fixture.clock.Advance(time.Second - time.Nanosecond)
			select {
			case <-callbackExpired:
				t.Fatal("callback context expired before its one-second deadline")
			default:
			}
			if err := fixture.client.Health(context.Background()); err != nil {
				t.Fatalf("Health before callback expiry = %v, want nil", err)
			}
			fixture.clock.Advance(time.Nanosecond)
			waitLeftoverSignal(t, callbackExpired, "callback context expiry")
			if err := callbackCtx.Err(); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("callback Err() after fake expiry = %v, want DeadlineExceeded", err)
			}
			assertLeftoverFailure(t, fixture)
		})
	}
}

func TestCallbackBurstStopsNewUserWorkAtThreshold(t *testing.T) {
	callbackStarted := make(chan struct{}, 3)
	callbackExpired := make(chan struct{}, 3)
	callbackReturned := make(chan struct{}, 3)
	handlerStarted := make(chan struct{}, 3)
	release := make(chan struct{})
	var releaseOnce sync.Once
	sub := pipelineTestSubscription(func(context.Context, *Event) error {
		handlerStarted <- struct{}{}
		return Drop(errors.New("discard"))
	})
	sub.HandlerTimeout = 10 * time.Millisecond
	sub.OnDiscarded = func(ctx context.Context, _ Discarded) {
		callbackStarted <- struct{}{}
		select {
		case <-release:
			return
		case <-ctx.Done():
		}
		callbackExpired <- struct{}{}
		<-release
		callbackReturned <- struct{}{}
	}
	fixture := newLeftoverRunnerFixture(t, sub)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	for _, id := range []string{"burst-a", "burst-b"} {
		settled := make(chan struct{})
		fixture.consumer.send(leftoverMessage(t, id, &dispatchSettler{onSettle: func() { close(settled) }}))
		waitLeftoverSignal(t, settled, "burst delivery Ack")
		waitLeftoverSignal(t, handlerStarted, "burst handler")
		waitLeftoverSignal(t, callbackStarted, "pre-expiry callback admission")
		waitLeftoverTimer(t, fixture.clock, time.Second)
	}
	fixture.clock.Advance(time.Second)
	waitLeftoverSignal(t, callbackExpired, "first callback expiry")
	waitLeftoverSignal(t, callbackExpired, "already-admitted second callback expiry")
	assertLeftoverFailure(t, fixture)
	releaseOnce.Do(func() { close(release) })
	waitLeftoverSignal(t, callbackReturned, "first callback return")
	waitLeftoverSignal(t, callbackReturned, "second callback return")

	// Replay the existing notification and dispatch seams after Run has failed:
	// returning leftovers must not reopen admission, even with a live parent.
	runnerNotifyDiscarded(fixture.runner, context.Background(), Discarded{})
	settled := make(chan struct{})
	dispatchMessage(fixture.runner, context.Background(),
		leftoverMessage(t, "after-threshold", &dispatchSettler{onSettle: func() { close(settled) }}),
		&Envelope{}, new(bool), &deliveryState{})
	joined := make(chan struct{})
	go func() {
		_ = fixture.runner.asyncGroup.Wait()
		close(joined)
	}()
	waitLeftoverSignal(t, joined, "late user work completion")
	select {
	case <-callbackStarted:
		t.Fatal("callback admission resumed after leftovers returned")
	default:
	}
	select {
	case <-handlerStarted:
		t.Fatal("handler admission resumed after leftovers returned")
	default:
	}
	if err := fixture.client.Health(context.Background()); err == nil {
		t.Fatal("returning leftovers cleared the failed subscription")
	}
}

func TestLeftoversSurviveConsumerGenerationRebuild(t *testing.T) {
	handlerStarted := make(chan struct{}, 1)
	callbackStarted := make(chan struct{})
	callbackExpired := make(chan struct{})
	release := make(chan struct{})
	sub := pipelineTestSubscription(func(_ context.Context, event *Event) error {
		if event.ID() == "old-generation-leftover" {
			handlerStarted <- struct{}{}
			<-release
			return nil
		}
		return Drop(errors.New("new generation callback"))
	})
	sub.Concurrency = 2
	sub.Prefetch = 2
	sub.HandlerTimeout = 10 * time.Millisecond
	sub.OnDiscarded = func(ctx context.Context, _ Discarded) {
		close(callbackStarted)
		<-ctx.Done()
		close(callbackExpired)
		<-release
	}
	fixture := newLeftoverRunnerFixture(t, sub)
	t.Cleanup(func() { close(release) })
	requeued := make(chan struct{})
	fixture.consumer.send(leftoverMessage(t, "old-generation-leftover", &dispatchSettler{
		onSettle: func() { close(requeued) },
	}))
	waitLeftoverSignal(t, handlerStarted, "old generation handler")
	abandonLeftoverHandler(t, fixture)
	waitLeftoverSignal(t, requeued, "old generation requeue")
	fixture.consumer.sendError(transientLaneError(fixture.driver))
	replacement := waitLaneConsumer(t, fixture.driver, sub.Name)
	if replacement == fixture.consumer || !fixture.consumer.released.Load() {
		t.Fatal("consumer generation was not replaced after transient failure")
	}
	if fixture.driver.openCount() != 1 {
		t.Fatal("consumer repair unexpectedly replaced the connection")
	}
	fixture.consumer = replacement
	settled := make(chan struct{})
	replacement.send(leftoverMessage(t, "new-generation-callback", &dispatchSettler{
		onSettle: func() { close(settled) },
	}))
	waitLeftoverSignal(t, settled, "new generation delivery Ack")
	waitLeftoverSignal(t, callbackStarted, "new generation callback")
	waitLeftoverTimer(t, fixture.clock, time.Second)
	fixture.clock.Advance(time.Second)
	waitLeftoverSignal(t, callbackExpired, "new generation callback expiry")
	assertLeftoverFailure(t, fixture)
}

// A callback whose context is cancelled before its own deadline is not a
// leftover: only a callback still running when that deadline passes counts. A
// transient consumer error ends the generation under the callback, and the
// runner must keep serving afterwards.
func TestCallbackCancelledByRepairIsNotCounted(t *testing.T) {
	callbackStarted := make(chan struct{}, 2)
	callbackCancelled := make(chan struct{}, 2)
	release := make(chan struct{})
	sub := pipelineTestSubscription(func(context.Context, *Event) error {
		return Drop(errors.New("discard"))
	})
	sub.Concurrency = 1
	sub.Prefetch = 1
	sub.HandlerTimeout = 10 * time.Millisecond
	sub.OnDiscarded = func(ctx context.Context, _ Discarded) {
		callbackStarted <- struct{}{}
		<-ctx.Done()
		callbackCancelled <- struct{}{}
		// Still running with its context cancelled and its deadline ahead:
		// the test holds it here until the runner has been observed serving
		// the generation that replaced this one.
		<-release
	}
	fixture := newLeftoverRunnerFixture(t, sub)
	t.Cleanup(func() { close(release) })

	settled := make(chan struct{})
	fixture.consumer.send(leftoverMessage(t, "cancelled-before-deadline", &dispatchSettler{
		onSettle: func() { close(settled) },
	}))
	waitLeftoverSignal(t, settled, "pre-repair delivery Ack")
	waitLeftoverSignal(t, callbackStarted, "callback admission")

	fixture.consumer.sendError(transientLaneError(fixture.driver))
	replacement := waitLaneConsumer(t, fixture.driver, sub.Name)
	if replacement == fixture.consumer || !fixture.consumer.released.Load() {
		t.Fatal("consumer generation was not replaced after transient failure")
	}
	waitLeftoverSignal(t, callbackCancelled, "callback cancellation")
	fixture.consumer = replacement

	settled = make(chan struct{})
	replacement.send(leftoverMessage(t, "after-cancellation", &dispatchSettler{
		onSettle: func() { close(settled) },
	}))
	waitLeftoverSignal(t, settled, "post-repair delivery Ack")
	select {
	case err := <-fixture.done:
		t.Fatalf("Run() = %v, want a runner still consuming after the repair", err)
	default:
	}
	if err := fixture.client.Health(context.Background()); err != nil {
		t.Fatalf("Health() = %v, want nil: a callback that has not reached its deadline is not a leftover", err)
	}
}

// A callback that ignores both its parent cancellation and its deadline is a
// leftover, counted when its own deadline passes and not before.
func TestCallbackOutlivingItsDeadlineAfterCancellationIsCounted(t *testing.T) {
	callbackStarted := make(chan struct{}, 2)
	release := make(chan struct{})
	sub := pipelineTestSubscription(func(context.Context, *Event) error {
		return Drop(errors.New("discard"))
	})
	sub.Concurrency = 1
	sub.Prefetch = 1
	sub.HandlerTimeout = 10 * time.Millisecond
	sub.OnDiscarded = func(context.Context, Discarded) {
		callbackStarted <- struct{}{}
		<-release
	}
	fixture := newLeftoverRunnerFixture(t, sub)
	t.Cleanup(func() { close(release) })

	settled := make(chan struct{})
	fixture.consumer.send(leftoverMessage(t, "blocking-callback", &dispatchSettler{
		onSettle: func() { close(settled) },
	}))
	waitLeftoverSignal(t, settled, "blocking delivery Ack")
	waitLeftoverSignal(t, callbackStarted, "blocking callback admission")
	waitLeftoverTimer(t, fixture.clock, time.Second)

	fixture.consumer.sendError(transientLaneError(fixture.driver))
	replacement := waitLaneConsumer(t, fixture.driver, sub.Name)
	if replacement == fixture.consumer {
		t.Fatal("consumer generation was not replaced after transient failure")
	}
	fixture.consumer = replacement

	// The cancellation alone is not counted: the runner still serves while the
	// callback is inside its one-second deadline.
	settled = make(chan struct{})
	replacement.send(leftoverMessage(t, "after-cancellation", &dispatchSettler{
		onSettle: func() { close(settled) },
	}))
	waitLeftoverSignal(t, settled, "post-repair delivery Ack")
	if err := fixture.client.Health(context.Background()); err != nil {
		t.Fatalf("Health() = %v, want nil: a parent cancellation is not a deadline the callback outlived", err)
	}

	fixture.clock.Advance(time.Second)
	assertLeftoverFailure(t, fixture)
}

// blockingStopConsumer holds the runner's own end-of-run drain in flight.
// stopRunnerConsumer reads the runner's consumer, so wrapping the fake consumer
// is what keeps the drain open while the test advances the clock.
type blockingStopConsumer struct {
	*laneRepairConsumer
	stopStarted chan struct{}
	stopRelease chan struct{}
	stopOnce    sync.Once
}

func (c *blockingStopConsumer) Stop(ctx context.Context) error {
	c.stopOnce.Do(func() { close(c.stopStarted) })
	select {
	case <-c.stopRelease:
	case <-ctx.Done():
	}
	return c.laneRepairConsumer.Stop(ctx)
}

// A runner that ends on its own end-of-run drain does not fail for a callback
// the drain left running. That drain sets only the owner's draining flag, never
// the exported one, so the lifecycle is the only signal that the runner has
// begun ending while both handler contexts are still live.
func TestRunnerOwnDrainDoesNotCountCallbackAtItsDeadline(t *testing.T) {
	callbackStarted := make(chan struct{}, 2)
	release := make(chan struct{})
	sub := pipelineTestSubscription(func(context.Context, *Event) error {
		return Drop(errors.New("discard"))
	})
	sub.Concurrency = 1
	sub.Prefetch = 1
	sub.HandlerTimeout = 10 * time.Second
	sub.OnDiscarded = func(context.Context, Discarded) {
		callbackStarted <- struct{}{}
		<-release
	}
	fixture := newLeftoverRunnerFixture(t, sub)
	t.Cleanup(func() { close(release) })

	settled := make(chan struct{})
	fixture.consumer.send(leftoverMessage(t, "callback-at-own-drain", &dispatchSettler{
		onSettle: func() { close(settled) },
	}))
	waitLeftoverSignal(t, settled, "delivery Ack")
	waitLeftoverSignal(t, callbackStarted, "callback admission")
	waitLeftoverTimer(t, fixture.clock, time.Second)

	blocking := &blockingStopConsumer{
		laneRepairConsumer: fixture.consumer,
		stopStarted:        make(chan struct{}),
		stopRelease:        make(chan struct{}),
	}
	fixture.runner.mu.Lock()
	fixture.runner.consumer = blocking
	fixture.runner.mu.Unlock()
	releaseStop := sync.OnceFunc(func() { close(blocking.stopRelease) })
	t.Cleanup(releaseStop)

	// The consumer stream closes with the callback still running, which ends the
	// run from the source side rather than through a caller's Drain.
	fixture.consumer.close()
	waitLeftoverSignal(t, blocking.stopStarted, "own drain stopping the consumer")
	waitReconnectCondition(t, func() bool { return fixture.runner.lifecycle.State() == lifecycle.Draining })

	fixture.clock.Advance(time.Second)
	select {
	case err := <-fixture.done:
		t.Fatalf("Run() = %v, want the run held open by the consumer stop", err)
	default:
	}
	releaseStop()
	select {
	case err := <-fixture.done:
		if err != nil {
			t.Fatalf("Run() = %v, want nil: a runner that ended on its own does not fail for a callback its drain left running", err)
		}
	case <-oneSecondTimer(t).C:
		t.Fatal("Run did not finish after the drain was released")
	}
	if err := fixture.client.Health(context.Background()); err != nil {
		t.Fatalf("Health() = %v, want nil", err)
	}
}
