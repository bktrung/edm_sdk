package f1

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	//nolint:depguard // consume observer tests exercise the runner through the in-memory driver.
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

type consumeRecordingObserver struct {
	mu       sync.Mutex
	records  []PointEvent
	starts   []StartEvent
	finishes []FinishEvent
	order    []string
}

type consumeObserverKey struct{}

func (o *consumeRecordingObserver) Start(_ context.Context, event StartEvent) (context.Context, Token) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.starts = append(o.starts, event)
	o.order = append(o.order, "start "+string(event.Kind))
	token := Token{Kind: event.Kind, Start: event.At, Handle: uint64(len(o.starts))}
	return context.WithValue(context.Background(), consumeObserverKey{}, len(o.starts)), token
}

func (o *consumeRecordingObserver) Finish(token Token, event FinishEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.finishes = append(o.finishes, event)
	o.order = append(o.order, "finish "+string(event.Kind)+"/"+string(event.Outcome))
	_ = token
}

func (o *consumeRecordingObserver) Record(event PointEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.records = append(o.records, event)
	o.order = append(o.order, "record "+string(event.Kind))
}

func (o *consumeRecordingObserver) snapshot() (records []PointEvent, starts []StartEvent, finishes []FinishEvent, order []string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	records = append([]PointEvent(nil), o.records...)
	starts = append([]StartEvent(nil), o.starts...)
	finishes = append([]FinishEvent(nil), o.finishes...)
	order = append([]string(nil), o.order...)
	return records, starts, finishes, order
}

func (o *consumeRecordingObserver) processFinishes() []FinishEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []FinishEvent
	for _, f := range o.finishes {
		if f.Kind == ObserverProcess {
			out = append(out, f)
		}
	}
	return out
}

func (o *consumeRecordingObserver) settlePairs() (starts []StartEvent, finishes []FinishEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, s := range o.starts {
		if s.Kind == ObserverSettle {
			starts = append(starts, s)
		}
	}
	for _, f := range o.finishes {
		if f.Kind == ObserverSettle {
			finishes = append(finishes, f)
		}
	}
	return starts, finishes
}

func newConsumeObserverClient(t *testing.T, rec Observer) *Client {
	t.Helper()
	return newConsumeObserverClientWithTopics(t, rec, "orders.created")
}

func newConsumeObserverClientWithTopics(t *testing.T, rec Observer, topics ...string) *Client {
	t.Helper()
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(inmem.New()),
		WithObserver(rec),
		WithPublishTopics(topics...),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client
}

func consumeTestSubscription(name string, handler HandlerFunc) Subscription {
	return Subscription{
		Name:           name,
		Topics:         []string{"orders.created"},
		Concurrency:    1,
		Prefetch:       4,
		Priorities:     []Priority{PriorityMedium},
		Retry:          RetryConfig{MaxAttempts: 1},
		HandlerTimeout: time.Second,
		Handlers:       map[string]Handler{"orders.created": handler},
	}
}

func startConsumeRunner(t *testing.T, client *Client, sub Subscription) (context.Context, context.CancelFunc, *Runner, chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	runner, err := client.Subscribe(ctx, sub)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runDone:
		case <-clock.NewReal().Timer(5 * time.Second).C:
		}
	})
	return ctx, cancel, runner, runDone
}

func publishConsumeOne(t *testing.T, client *Client, ctx context.Context, payload any, opts ...PublishOption) string {
	t.Helper()
	msgs := []Message{{EventType: "orders.created", Payload: payload, Opts: opts}}
	deadline := clock.NewReal().Timer(5 * time.Second)
	defer deadline.Stop()
	for {
		result, err := client.Publisher().PublishBatch(ctx, msgs)
		if err != nil {
			t.Fatalf("PublishBatch() error = %v", err)
		}
		if len(failedIndexes(result)) == 0 {
			return result.Results[0].ID
		}
		tick := clock.NewReal().Timer(10 * time.Millisecond)
		select {
		case <-deadline.C:
			tick.Stop()
			t.Fatalf("publish did not reach the topology: %v", result.Results[failedIndexes(result)[0]].Err)
			return ""
		case <-tick.C:
			tick.Stop()
		}
	}
}

func waitConsumeCondition(t *testing.T, msg string, cond func() bool) {
	t.Helper()
	deadline := clock.NewReal().Timer(5 * time.Second)
	defer deadline.Stop()
	for {
		if cond() {
			return
		}
		tick := clock.NewReal().Timer(10 * time.Millisecond)
		select {
		case <-deadline.C:
			tick.Stop()
			t.Fatal(msg)
			return
		case <-tick.C:
			tick.Stop()
		}
	}
}

func TestObserverConsumeHandledDelivery(t *testing.T) {
	rec := &consumeRecordingObserver{}
	client := newConsumeObserverClient(t, rec)
	handled := make(chan struct{})
	sub := consumeTestSubscription("orders", func(_ context.Context, _ *Event) error {
		select {
		case <-handled:
		default:
			close(handled)
		}
		return nil
	})
	ctx, _, _, _ := startConsumeRunner(t, client, sub)
	id := publishConsumeOne(t, client, ctx, map[string]string{"id": "42"})
	select {
	case <-handled:
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("handler did not run")
	}
	waitConsumeCondition(t, "consume events did not arrive", func() bool {
		_, _, _, order := rec.snapshot()
		kept := 0
		for _, entry := range order {
			switch entry {
			case "record delivery_received", "start process", "finish process/ok", "start settle", "finish settle/ok":
				kept++
			}
		}
		return kept >= 5
	})
	_, starts, finishes, order := rec.snapshot()
	var kept []string
	for _, entry := range order {
		switch entry {
		case "record delivery_received", "start process", "finish process/ok", "start settle", "finish settle/ok":
			kept = append(kept, entry)
		}
	}
	want := []string{"record delivery_received", "start process", "finish process/ok", "start settle", "finish settle/ok"}
	if len(kept) != len(want) {
		t.Fatalf("consume order = %v, want %v (full order %v)", kept, want, order)
	}
	for i := range want {
		if kept[i] != want[i] {
			t.Fatalf("consume order = %v, want %v", kept, want)
		}
	}
	var processStart *StartEvent
	for i := range starts {
		if starts[i].Kind == ObserverProcess {
			processStart = &starts[i]
			break
		}
	}
	if processStart == nil {
		t.Fatal("no process start")
	}
	if processStart.MessageID != id {
		t.Fatalf("process MessageID = %q, want %q", processStart.MessageID, id)
	}
	if processStart.EventType != "orders.created" {
		t.Fatalf("process EventType = %q, want orders.created", processStart.EventType)
	}
	if processStart.Topic != "orders.created" {
		t.Fatalf("process Topic = %q, want orders.created", processStart.Topic)
	}
	if processStart.Subscription != "orders" {
		t.Fatalf("process Subscription = %q, want orders", processStart.Subscription)
	}
	if processStart.Priority != PriorityMedium {
		t.Fatalf("process Priority = %v, want medium", processStart.Priority)
	}
	if processStart.Attempt != 1 {
		t.Fatalf("process Attempt = %d, want 1", processStart.Attempt)
	}
	if processStart.TraceParent != "" || processStart.TraceState != "" {
		t.Fatalf("process trace = %q/%q, want empty", processStart.TraceParent, processStart.TraceState)
	}
	var processFinish *FinishEvent
	for i := range finishes {
		if finishes[i].Kind == ObserverProcess {
			processFinish = &finishes[i]
			break
		}
	}
	if processFinish == nil {
		t.Fatal("no process finish")
	}
	if processFinish.Outcome != ObserverOutcomeOK || !processFinish.Terminal {
		t.Fatalf("process finish = %q terminal=%v, want ok true", processFinish.Outcome, processFinish.Terminal)
	}
	if processFinish.ErrorClass != "" {
		t.Fatalf("process class = %q, want empty", processFinish.ErrorClass)
	}
}

func TestObserverDeliveryReceivedIDs(t *testing.T) {
	rec := &consumeRecordingObserver{}
	client := newConsumeObserverClient(t, rec)
	sub := consumeTestSubscription("orders", func(context.Context, *Event) error {
		return nil
	})
	ctx, _, _, _ := startConsumeRunner(t, client, sub)

	rootID := publishConsumeOne(t, client, ctx, "root")
	correlatedID := publishConsumeOne(t, client, ctx, "correlated", WithCorrelationID("corr-explicit"))

	waitConsumeCondition(t, "delivery_received events did not arrive", func() bool {
		records, _, _, _ := rec.snapshot()
		count := 0
		for _, record := range records {
			if record.Kind == ObserverDeliveryReceived {
				count++
			}
		}
		return count >= 2
	})

	records, _, _, _ := rec.snapshot()
	received := 0
	for _, record := range records {
		if record.Kind != ObserverDeliveryReceived {
			continue
		}
		received++
		switch record.MessageID {
		case rootID:
			if record.CorrelationID != rootID {
				t.Fatalf("root delivery correlation = %q, want %q", record.CorrelationID, rootID)
			}
		case correlatedID:
			if record.CorrelationID != "corr-explicit" {
				t.Fatalf("explicit delivery correlation = %q, want corr-explicit", record.CorrelationID)
			}
		default:
			t.Fatalf("delivery_received MessageID = %q, want %q or %q", record.MessageID, rootID, correlatedID)
		}
	}
	if received != 2 {
		t.Fatalf("delivery_received count = %d, want 2", received)
	}
}

func TestObserverDeliveryReceivedIDsMissingHeaders(t *testing.T) {
	rec := &consumeRecordingObserver{}
	_, runner, _ := successorObserverRunner(t, nil, rec, nil)
	runner.inflight = newInflightRegistry()
	dispatch := make(chan delivery, 1)
	if !enqueueDelivery(runner, context.Background(), dispatch, driver.InboundMessage{}) {
		t.Fatal("enqueueDelivery returned false")
	}
	item := <-dispatch
	runner.inflight.Remove(item.id)

	records, _, _, _ := rec.snapshot()
	for _, record := range records {
		if record.Kind != ObserverDeliveryReceived {
			continue
		}
		if record.MessageID != "" || record.CorrelationID != "" {
			t.Fatalf("delivery_received identity = %q/%q, want empty fields", record.MessageID, record.CorrelationID)
		}
		return
	}
	t.Fatal("no delivery_received event")
}

type ctxValueConsumeObserver struct {
	consumeRecordingObserver
	key   any
	value any
}

func (o *ctxValueConsumeObserver) Start(ctx context.Context, event StartEvent) (context.Context, Token) {
	_, token := o.consumeRecordingObserver.Start(ctx, event)
	return context.WithValue(ctx, o.key, o.value), token
}

func TestObserverConsumeHandlerContext(t *testing.T) {
	type runnerKey struct{}
	type obsKey struct{}
	rec := &ctxValueConsumeObserver{key: obsKey{}, value: "observer-value"}
	client := newConsumeObserverClient(t, rec)
	const timeout = 2 * time.Second
	runnerCtx := context.WithValue(context.Background(), runnerKey{}, "runner-value")
	runner := &Runner{client: client, subscription: Subscription{Name: "orders", HandlerTimeout: timeout}, handlerCtx: runnerCtx}
	gotRunner := make(chan string, 1)
	gotObserver := make(chan string, 1)
	gotTimeout := make(chan time.Duration, 1)
	handlerStart := time.Now() //nolint:forbidigo // the handler deadline is wall-clock time from context.WithTimeout.
	result := invokeHandler(runner, context.Background(), HandlerFunc(func(ctx context.Context, _ *Event) error {
		runnerValue, _ := ctx.Value(runnerKey{}).(string)
		gotRunner <- runnerValue
		observerValue, _ := ctx.Value(obsKey{}).(string)
		gotObserver <- observerValue
		deadline, ok := ctx.Deadline()
		if !ok {
			gotTimeout <- -1
			return nil
		}
		gotTimeout <- deadline.Sub(handlerStart)
		return nil
	}), &Event{}, "", 0)
	if result.stuck || result.err != nil || result.panic != nil {
		t.Fatalf("invokeHandler() = %#v, want ok", result)
	}
	select {
	case value := <-gotRunner:
		if value != "runner-value" {
			t.Fatalf("handler runner value = %q, want runner-value", value)
		}
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("handler did not run")
	}
	select {
	case value := <-gotObserver:
		if value != "observer-value" {
			t.Fatalf("handler observer value = %q, want observer-value", value)
		}
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("handler did not report observer value")
	}
	select {
	case remaining := <-gotTimeout:
		if remaining < 0 {
			t.Fatal("handler ctx has no deadline")
		}
		if remaining < timeout-500*time.Millisecond || remaining > timeout+500*time.Millisecond {
			t.Fatalf("handler timeout = %v, want about %v", remaining, timeout)
		}
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("no deadline signal")
	}
	waitConsumeCondition(t, "process finish did not arrive", func() bool {
		return len(rec.processFinishes()) >= 1
	})
}

func TestObserverConsumeProcessOutcomes(t *testing.T) {
	t.Run("retryable below max", func(t *testing.T) {
		rec := &consumeRecordingObserver{}
		client := newConsumeObserverClient(t, rec)
		sub := consumeTestSubscription("orders-retryable", func(context.Context, *Event) error {
			return errors.New("boom")
		})
		sub.Retry = RetryConfig{MaxAttempts: 3, Tiers: []time.Duration{time.Millisecond}}
		ctx, _, _, _ := startConsumeRunner(t, client, sub)
		publishConsumeOne(t, client, ctx, "payload")
		waitConsumeCondition(t, "process finish did not arrive", func() bool {
			return len(rec.processFinishes()) >= 1
		})
		finish := rec.processFinishes()[0]
		if finish.Outcome != ObserverOutcomeError || finish.ErrorClass != ErrorClassRetryable || finish.Terminal {
			t.Fatalf("finish = %q/%q terminal=%v, want error/f1_retryable false", finish.Outcome, finish.ErrorClass, finish.Terminal)
		}
	})
	t.Run("terminal", func(t *testing.T) {
		rec := &consumeRecordingObserver{}
		client := newConsumeObserverClient(t, rec)
		sub := consumeTestSubscription("orders-terminal", func(context.Context, *Event) error {
			return Terminal(errors.New("boom"))
		})
		sub.Retry = RetryConfig{MaxAttempts: 3, Tiers: []time.Duration{time.Millisecond}}
		ctx, _, _, _ := startConsumeRunner(t, client, sub)
		publishConsumeOne(t, client, ctx, "payload")
		waitConsumeCondition(t, "process finish did not arrive", func() bool {
			return len(rec.processFinishes()) >= 1
		})
		finish := rec.processFinishes()[0]
		if finish.Outcome != ObserverOutcomeError || finish.ErrorClass != ErrorClassTerminal || !finish.Terminal {
			t.Fatalf("finish = %q/%q terminal=%v, want error/f1_terminal true", finish.Outcome, finish.ErrorClass, finish.Terminal)
		}
	})
	t.Run("dropped", func(t *testing.T) {
		rec := &consumeRecordingObserver{}
		client := newConsumeObserverClient(t, rec)
		sub := consumeTestSubscription("orders-dropped", func(context.Context, *Event) error {
			return Drop(errors.New("boom"))
		})
		ctx, _, _, _ := startConsumeRunner(t, client, sub)
		publishConsumeOne(t, client, ctx, "payload")
		waitConsumeCondition(t, "process finish did not arrive", func() bool {
			return len(rec.processFinishes()) >= 1
		})
		finish := rec.processFinishes()[0]
		if finish.Outcome != ObserverOutcomeError || finish.ErrorClass != ErrorClassDropped || !finish.Terminal {
			t.Fatalf("finish = %q/%q terminal=%v, want error/f1_dropped true", finish.Outcome, finish.ErrorClass, finish.Terminal)
		}
	})
	t.Run("panic", func(t *testing.T) {
		rec := &consumeRecordingObserver{}
		client := newConsumeObserverClient(t, rec)
		sub := consumeTestSubscription("orders-panic", func(context.Context, *Event) error {
			panic("boom")
		})
		ctx, _, _, _ := startConsumeRunner(t, client, sub)
		publishConsumeOne(t, client, ctx, "payload")
		waitConsumeCondition(t, "process finish did not arrive", func() bool {
			return len(rec.processFinishes()) >= 1
		})
		finish := rec.processFinishes()[0]
		if finish.Outcome != ObserverOutcomeError || finish.ErrorClass != ErrorClassPanic || !finish.Terminal {
			t.Fatalf("finish = %q/%q terminal=%v, want error/f1_panic true", finish.Outcome, finish.ErrorClass, finish.Terminal)
		}
	})
	t.Run("max attempts", func(t *testing.T) {
		rec := &consumeRecordingObserver{}
		client := newConsumeObserverClient(t, rec)
		sub := consumeTestSubscription("orders-max", func(context.Context, *Event) error {
			return errors.New("boom")
		})
		sub.Retry = RetryConfig{MaxAttempts: 1}
		ctx, _, _, _ := startConsumeRunner(t, client, sub)
		publishConsumeOne(t, client, ctx, "payload")
		waitConsumeCondition(t, "process finish did not arrive", func() bool {
			return len(rec.processFinishes()) >= 1
		})
		finish := rec.processFinishes()[0]
		if finish.Outcome != ObserverOutcomeError || finish.ErrorClass != ErrorClassMaxAttempts || !finish.Terminal {
			t.Fatalf("finish = %q/%q terminal=%v, want error/f1_max_attempts true", finish.Outcome, finish.ErrorClass, finish.Terminal)
		}
	})
}

func TestObserverConsumeStuckAbandoned(t *testing.T) {
	rec := &consumeRecordingObserver{}
	client := newConsumeObserverClient(t, rec)
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	runner := &Runner{client: client, subscription: Subscription{Name: "orders", HandlerTimeout: 10 * time.Millisecond}}
	result := invokeHandler(runner, context.Background(), HandlerFunc(func(context.Context, *Event) error {
		<-release
		return nil
	}), &Event{}, "", 0)
	select {
	case <-release:
	default:
		close(release)
	}
	if !result.stuck {
		t.Fatalf("invokeHandler() = %#v, want stuck", result)
	}
	_, starts, _, _ := rec.snapshot()
	processStarts := 0
	for _, s := range starts {
		if s.Kind == ObserverProcess {
			processStarts++
		}
	}
	processFinishes := rec.processFinishes()
	if processStarts != 1 {
		t.Fatalf("process starts = %d, want 1", processStarts)
	}
	if len(processFinishes) != 1 {
		t.Fatalf("process finishes = %d, want 1", len(processFinishes))
	}
	if processFinishes[0].Outcome != ObserverOutcomeAbandoned {
		t.Fatalf("process outcome = %q, want abandoned", processFinishes[0].Outcome)
	}
}

type flakyConsumeSettler struct {
	mu      sync.Mutex
	calls   int
	failErr error
}

func (s *flakyConsumeSettler) Ack(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls == 1 {
		return s.failErr
	}
	return nil
}

func (s *flakyConsumeSettler) Nack(context.Context, driver.NackOptions) error {
	return errors.New("unexpected nack")
}

func TestObserverConsumeTopicOverride(t *testing.T) {
	rec := &consumeRecordingObserver{}
	client := newConsumeObserverClientWithTopics(t, rec, "orders.created", "orders.custom")
	handled := make(chan struct{})
	sub := consumeTestSubscription("orders-custom", func(_ context.Context, _ *Event) error {
		select {
		case <-handled:
		default:
			close(handled)
		}
		return nil
	})
	sub.Topics = []string{"orders.custom"}
	ctx, _, _, _ := startConsumeRunner(t, client, sub)
	publishConsumeOne(t, client, ctx, map[string]string{"id": "42"}, WithTopic("orders.custom"))
	select {
	case <-handled:
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("handler did not run")
	}
	waitConsumeCondition(t, "process finish did not arrive", func() bool {
		_, _, finishes, _ := rec.snapshot()
		for _, f := range finishes {
			if f.Kind == ObserverProcess {
				return true
			}
		}
		return false
	})
	_, starts, finishes, _ := rec.snapshot()
	var processStart *StartEvent
	for i := range starts {
		if starts[i].Kind == ObserverProcess {
			processStart = &starts[i]
			break
		}
	}
	if processStart == nil {
		t.Fatal("no process start")
	}
	if processStart.Topic != "orders.custom" {
		t.Fatalf("process Topic = %q, want orders.custom", processStart.Topic)
	}
	var processFinish *FinishEvent
	for i := range finishes {
		if finishes[i].Kind == ObserverProcess {
			processFinish = &finishes[i]
			break
		}
	}
	if processFinish == nil {
		t.Fatal("no process finish")
	}
	if processFinish.Topic != "orders.custom" {
		t.Fatalf("process finish Topic = %q, want orders.custom", processFinish.Topic)
	}
}

func TestObserverConsumeExtractsTrace(t *testing.T) {
	rec := &consumeRecordingObserver{}
	client := newConsumeObserverClient(t, rec)
	handled := make(chan struct{})
	sub := consumeTestSubscription("orders-trace", func(_ context.Context, _ *Event) error {
		select {
		case <-handled:
		default:
			close(handled)
		}
		return nil
	})
	ctx, _, _, _ := startConsumeRunner(t, client, sub)
	parent := &Event{envelope: Envelope{ID: "cause-1", TraceParent: "00-abc-def-01", TraceState: "rojo=1", CorrelationID: "corr-parent"}}
	publishConsumeOne(t, client, ctx, map[string]string{"id": "43"}, WithCausedBy(parent))
	select {
	case <-handled:
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("handler did not run")
	}
	waitConsumeCondition(t, "process start did not arrive", func() bool {
		_, starts, _, _ := rec.snapshot()
		for _, s := range starts {
			if s.Kind == ObserverProcess {
				return true
			}
		}
		return false
	})
	_, starts, _, _ := rec.snapshot()
	var processStart *StartEvent
	for i := range starts {
		if starts[i].Kind == ObserverProcess {
			processStart = &starts[i]
			break
		}
	}
	if processStart == nil {
		t.Fatal("no process start")
	}
	if processStart.TraceParent != "00-abc-def-01" {
		t.Fatalf("process TraceParent = %q, want 00-abc-def-01", processStart.TraceParent)
	}
	if processStart.TraceState != "rojo=1" {
		t.Fatalf("process TraceState = %q, want rojo=1", processStart.TraceState)
	}
}

func TestObserverConsumeSettleRetry(t *testing.T) {
	rec := &consumeRecordingObserver{}
	client := newConsumeObserverClient(t, rec)
	runner := &Runner{client: client, subscription: Subscription{Name: "orders"}}
	failErr := &driver.Error{Driver: "test", Op: "ack", K: driver.KindTransient, Err: errors.New("flaky")}
	settler := &flakyConsumeSettler{failErr: failErr}
	message := driver.InboundMessage{Destination: "orders.in", Settle: settler}
	if ackDeliveryAs(runner, context.Background(), message, true, &deliveryState{}) {
		t.Fatal("first ack settled, want failure")
	}
	if !ackDeliveryAs(runner, context.Background(), message, true, &deliveryState{}) {
		t.Fatal("second ack did not settle")
	}
	starts, finishes := rec.settlePairs()
	if len(starts) != 2 || len(finishes) != 2 {
		t.Fatalf("settle pairs = %d/%d, want 2/2", len(starts), len(finishes))
	}
	if finishes[0].Outcome != ObserverOutcomeError || finishes[0].ErrorClass != ErrorClassDriverTransient {
		t.Fatalf("first settle = %q/%q, want error/driver_transient", finishes[0].Outcome, finishes[0].ErrorClass)
	}
	if finishes[1].Outcome != ObserverOutcomeOK || finishes[1].ErrorClass != "" {
		t.Fatalf("second settle = %q/%q, want ok/empty", finishes[1].Outcome, finishes[1].ErrorClass)
	}
}
