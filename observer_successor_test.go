package f1

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type successorRecordingObserver struct {
	mu          sync.Mutex
	starts      []StartEvent
	finishes    []FinishEvent
	records     []PointEvent
	order       []string
	startCtxs   []context.Context
	traceParent string
	traceState  string
	processKey  any
	processVal  any
	publishSaw  bool
}

func (o *successorRecordingObserver) Start(ctx context.Context, event StartEvent) (context.Context, Token) {
	o.mu.Lock()
	o.starts = append(o.starts, event)
	o.order = append(o.order, "start "+string(event.Kind))
	if event.Route != "" {
		o.order[len(o.order)-1] += "/" + string(event.Route)
	}
	o.startCtxs = append(o.startCtxs, ctx)
	token := Token{Kind: event.Kind, Start: event.At, Handle: uint64(len(o.starts))}
	key, val := o.processKey, o.processVal
	o.mu.Unlock()
	if event.Kind == ObserverProcess && key != nil {
		return context.WithValue(ctx, key, val), token
	}
	if event.Kind == ObserverPublish && key != nil {
		if ctx.Value(key) == val {
			o.mu.Lock()
			o.publishSaw = true
			o.mu.Unlock()
		}
	}
	return ctx, token
}

func (o *successorRecordingObserver) Finish(token Token, event FinishEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.finishes = append(o.finishes, event)
	entry := "finish " + string(event.Kind) + "/" + string(event.Outcome)
	o.order = append(o.order, entry)
	_ = token
}

func (o *successorRecordingObserver) Record(event PointEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.records = append(o.records, event)
	o.order = append(o.order, "record "+string(event.Kind))
}

func (o *successorRecordingObserver) InjectTrace(context.Context) (string, string) {
	return o.traceParent, o.traceState
}

func (o *successorRecordingObserver) snapshot() (starts []StartEvent, finishes []FinishEvent, records []PointEvent, order []string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	starts = append([]StartEvent(nil), o.starts...)
	finishes = append([]FinishEvent(nil), o.finishes...)
	records = append([]PointEvent(nil), o.records...)
	order = append([]string(nil), o.order...)
	return starts, finishes, records, order
}

func successorObserverRunner(t *testing.T, producer driver.Producer, rec Observer, handlers map[string]Handler) (*Client, *Runner, *dispatchProducer) {
	t.Helper()
	conn := &dispatchConn{admin: &dispatchAdmin{}}
	var dp *dispatchProducer
	if producer == nil {
		dp = &dispatchProducer{}
		conn.producer = dp
	} else if p, ok := producer.(*dispatchProducer); ok {
		dp = p
		conn.producer = p
	} else {
		conn.producerOverride = producer
	}
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&dispatchDriver{conn: conn}),
		WithObserver(rec),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	if handlers == nil {
		handlers = map[string]Handler{}
	}
	runner := &Runner{
		client: client,
		subscription: Subscription{
			Name:           "orders",
			Topics:         []string{"orders.created"},
			Priorities:     []Priority{PriorityHigh},
			Retry:          RetryConfig{MaxAttempts: 3, Tiers: []time.Duration{time.Second}},
			HandlerTimeout: time.Second,
			Handlers:       handlers,
		},
	}
	return client, runner, dp
}

type transientSuccessorProducer struct {
	kind  driver.Kind
	cause error
}

func (p *transientSuccessorProducer) Publish(_ context.Context, messages ...driver.OutboundMessage) error {
	failed := make(map[int]error, len(messages))
	for i := range messages {
		failed[i] = &driver.Error{Driver: "test", Op: "publish", K: p.kind, Err: p.cause}
	}
	return &driver.PublishError{Failed: failed}
}

func (p *transientSuccessorProducer) Close(context.Context) error { return nil }

func successorTestEnvelope(id string) Envelope {
	return Envelope{
		SpecVersion:   "1.0",
		ID:            id,
		Source:        "/test/orders",
		Type:          "orders.created.v1",
		Priority:      PriorityHigh,
		Attempt:       1,
		CorrelationID: "corr-" + id,
		MaxAttempts:   3,
	}
}

func TestObserverSuccessorRetryHop(t *testing.T) {
	producer := &dispatchProducer{}
	rec := &successorRecordingObserver{traceParent: "00-t2-trace-01", traceState: "t2=1"}
	handlers := map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error { return errors.New("boom") }),
	}
	_, runner, _ := successorObserverRunner(t, producer, rec, handlers)
	envelope := successorTestEnvelope("retry-hop-1")
	envelope.TraceParent = "00-inbound-01"
	envelope.TraceState = "in=1"
	message := retryBridgeMessage(t, envelope, &dispatchSettler{})
	var abandoned bool
	var out Envelope
	if !dispatchMessage(runner, context.Background(), message, &out, &abandoned, &deliveryState{}) {
		t.Fatal("dispatchMessage did not settle")
	}
	_, finishes, records, order := rec.snapshot()
	var publishStarts []StartEvent
	var publishFinishes []FinishEvent
	rec.mu.Lock()
	for _, s := range rec.starts {
		if s.Kind == ObserverPublish {
			publishStarts = append(publishStarts, s)
		}
	}
	rec.mu.Unlock()
	for _, f := range finishes {
		if f.Kind == ObserverPublish {
			publishFinishes = append(publishFinishes, f)
		}
	}
	if len(publishStarts) != 1 {
		t.Fatalf("publish starts = %d, want 1 (order %v)", len(publishStarts), order)
	}
	if publishStarts[0].Route != PublishRouteRetry {
		t.Fatalf("publish route = %q, want retry", publishStarts[0].Route)
	}
	if publishStarts[0].BatchSize != 1 {
		t.Fatalf("publish batch = %d, want 1", publishStarts[0].BatchSize)
	}
	if len(publishFinishes) != 1 || publishFinishes[0].Outcome != ObserverOutcomeOK {
		t.Fatalf("publish finishes = %+v, want one ok", publishFinishes)
	}
	var scheduled *PointEvent
	for i := range records {
		if records[i].Kind == ObserverRetryScheduled {
			scheduled = &records[i]
			break
		}
	}
	if scheduled == nil {
		t.Fatalf("no retry_scheduled (order %v)", order)
	}
	if scheduled.NextAttempt != 2 {
		t.Fatalf("NextAttempt = %d, want 2", scheduled.NextAttempt)
	}
	if scheduled.MaxAttempts != 3 {
		t.Fatalf("MaxAttempts = %d, want 3", scheduled.MaxAttempts)
	}
	if scheduled.Backoff != time.Second {
		t.Fatalf("Backoff = %v, want 1s", scheduled.Backoff)
	}
	if scheduled.ErrorClass != ErrorClassRetryable {
		t.Fatalf("class = %q, want f1_retryable", scheduled.ErrorClass)
	}
	if scheduled.MessageID != "retry-hop-1" || scheduled.CorrelationID != "corr-retry-hop-1" {
		t.Fatalf("identity = %q/%q, want retry-hop-1/corr-retry-hop-1", scheduled.MessageID, scheduled.CorrelationID)
	}
	if scheduled.Destination != message.Destination {
		t.Fatalf("retry_scheduled destination = %q, want inbound %q", scheduled.Destination, message.Destination)
	}
	if publishStarts[0].Destination != retryDestinationFor("/test/orders", "orders.created", PriorityHigh, 1, "orders") {
		t.Fatalf("retry publish destination = %q, want the retry destination", publishStarts[0].Destination)
	}
	producer.mu.Lock()
	defer producer.mu.Unlock()
	if len(producer.messages) != 1 {
		t.Fatalf("published copies = %d, want 1", len(producer.messages))
	}
	gotParent := headerValue(producer.messages[0].Headers, "traceparent")
	if gotParent != "00-t2-trace-01" {
		t.Fatalf("retry traceparent = %q, want T2", gotParent)
	}
	if gotParent == "00-inbound-01" {
		t.Fatal("retry traceparent equals inbound, want T2")
	}
	if got := headerValue(producer.messages[0].Headers, "f1id"); got != "" {
		_ = got
	}
	// Message ID and correlation survive the hop on the encoded headers.
	decoded, err := DecodeHeaders(func() map[string]string {
		m := map[string]string{}
		for _, h := range producer.messages[0].Headers {
			m[h.Key] = string(h.Value)
		}
		return m
	}())
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ID != "retry-hop-1" {
		t.Fatalf("copy ID = %q, want retry-hop-1", decoded.ID)
	}
	if decoded.CorrelationID != "corr-retry-hop-1" {
		t.Fatalf("copy correlation = %q, want corr-retry-hop-1", decoded.CorrelationID)
	}
}

func TestObserverSuccessorTerminalDeadLetter(t *testing.T) {
	producer := &dispatchProducer{}
	rec := &successorRecordingObserver{}
	handlers := map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error { return Terminal(errors.New("bad")) }),
	}
	_, runner, _ := successorObserverRunner(t, producer, rec, handlers)
	envelope := successorTestEnvelope("terminal-1")
	message := retryBridgeMessage(t, envelope, &dispatchSettler{})
	var abandoned bool
	var out Envelope
	if !dispatchMessage(runner, context.Background(), message, &out, &abandoned, &deliveryState{}) {
		t.Fatal("dispatchMessage did not settle")
	}
	_, _, records, _ := rec.snapshot()
	var decided, published *PointEvent
	for i := range records {
		switch records[i].Kind {
		case ObserverDeadLetterDecided:
			decided = &records[i]
		case ObserverDeadLetterPublished:
			published = &records[i]
		}
	}
	if decided == nil {
		t.Fatal("no dead_letter_decided")
	}
	if decided.Reason != ReasonTerminal {
		t.Fatalf("decided reason = %q, want terminal", decided.Reason)
	}
	if decided.DeadLetterDestination == "" {
		t.Fatal("decided destination empty")
	}
	wantDLQ := deadLetterDestinationFor("/test/orders", "orders.created", "orders")
	if decided.DeadLetterDestination != wantDLQ {
		t.Fatalf("decided destination = %q, want %q", decided.DeadLetterDestination, wantDLQ)
	}
	rec.mu.Lock()
	publishRoutes := []PublishRoute{}
	for _, s := range rec.starts {
		if s.Kind == ObserverPublish {
			publishRoutes = append(publishRoutes, s.Route)
		}
	}
	rec.mu.Unlock()
	if len(publishRoutes) != 1 || publishRoutes[0] != PublishRouteDeadLetter {
		t.Fatalf("publish routes = %v, want [dead_letter]", publishRoutes)
	}
	if published == nil {
		t.Fatal("no dead_letter_published")
	}
}

func TestObserverSuccessorDeadLetterPublishFailsTransient(t *testing.T) {
	cause := errors.New("broker busy")
	producer := &transientSuccessorProducer{kind: driver.KindTransient, cause: cause}
	rec := &successorRecordingObserver{}
	_, runner, _ := successorObserverRunner(t, producer, rec, nil)
	envelope := successorTestEnvelope("dlq-fail-1")
	message := retryBridgeMessage(t, envelope, &dispatchSettler{})
	state := &deliveryState{headerMaxBytes: CoreMaxHeaderBytes}
	settled := deadLetterAndSettle(runner, context.Background(), message, envelope, ReasonTerminal, errors.New("bad"), state)
	if settled {
		t.Fatal("transient dead-letter failure settled, want unsettled handoff")
	}
	_, finishes, records, _ := rec.snapshot()
	var publishFinish *FinishEvent
	for i := range finishes {
		if finishes[i].Kind == ObserverPublish {
			publishFinish = &finishes[i]
			break
		}
	}
	if publishFinish == nil {
		t.Fatal("no publish finish")
	}
	if publishFinish.Outcome != ObserverOutcomeError || publishFinish.ErrorClass != ErrorClassDriverTransient {
		t.Fatalf("publish finish = %q/%q, want error/driver_transient", publishFinish.Outcome, publishFinish.ErrorClass)
	}
	var failed *PointEvent
	var publishedCount, scheduledCount int
	for i := range records {
		switch records[i].Kind {
		case ObserverDeadLetterFailed:
			failed = &records[i]
		case ObserverDeadLetterPublished:
			publishedCount++
		case ObserverRetryScheduled:
			scheduledCount++
		}
	}
	if failed == nil {
		t.Fatal("no dead_letter_failed")
	}
	if failed.ErrorClass != ErrorClassDriverTransient {
		t.Fatalf("failed class = %q, want driver_transient", failed.ErrorClass)
	}
	if publishedCount != 0 || scheduledCount != 0 {
		t.Fatalf("published=%d scheduled=%d, want 0/0", publishedCount, scheduledCount)
	}
}

func TestObserverSuccessorRetryPublishFailsTransient(t *testing.T) {
	cause := errors.New("broker busy")
	producer := &transientSuccessorProducer{kind: driver.KindTransient, cause: cause}
	rec := &successorRecordingObserver{}
	_, runner, _ := successorObserverRunner(t, producer, rec, nil)
	envelope := successorTestEnvelope("retry-fail-1")
	message := retryBridgeMessage(t, envelope, &dispatchSettler{})
	state := &deliveryState{headerMaxBytes: CoreMaxHeaderBytes}
	if retryAndSettle(runner, context.Background(), message, envelope, errors.New("boom"), state) {
		t.Fatal("transient retry failure settled, want false")
	}
	_, finishes, records, _ := rec.snapshot()
	var publishFinish *FinishEvent
	for i := range finishes {
		if finishes[i].Kind == ObserverPublish {
			publishFinish = &finishes[i]
			break
		}
	}
	if publishFinish == nil {
		t.Fatal("no publish finish")
	}
	if publishFinish.Outcome != ObserverOutcomeError {
		t.Fatalf("publish outcome = %q, want error", publishFinish.Outcome)
	}
	for _, r := range records {
		if r.Kind == ObserverRetryScheduled {
			t.Fatal("retry_scheduled emitted on publish failure")
		}
	}
}

func TestObserverSuccessorOversizedDeadLetterDropsOnce(t *testing.T) {
	cause := errors.New("message body exceeds the broker size limit")
	producer := &transientSuccessorProducer{kind: driver.KindTooLarge, cause: cause}
	rec := &successorRecordingObserver{}
	_, runner, _ := successorObserverRunner(t, producer, rec, nil)
	envelope := successorTestEnvelope("oversized-1")
	message := retryBridgeMessage(t, envelope, &dispatchSettler{})
	state := &deliveryState{headerMaxBytes: CoreMaxHeaderBytes}
	if !deadLetterAndSettle(runner, context.Background(), message, envelope, ReasonTerminal, errors.New("bad"), state) {
		t.Fatal("oversized dead-letter copy was not contained")
	}
	_, _, records, _ := rec.snapshot()
	var failedCount, rejectedCount int
	var failed *PointEvent
	var rejected *PointEvent
	for i := range records {
		switch records[i].Kind {
		case ObserverDeadLetterFailed:
			failedCount++
			failed = &records[i]
		case ObserverPoisonRejected:
			rejectedCount++
			rejected = &records[i]
		}
	}
	if failedCount != 1 {
		t.Fatalf("dead_letter_failed count = %d, want 1", failedCount)
	}
	if failed.ErrorClass != ErrorClassDriverTooLarge {
		t.Fatalf("failed class = %q, want driver_too_large", failed.ErrorClass)
	}
	if rejectedCount != 1 {
		t.Fatalf("poison_rejected count = %d, want exactly 1", rejectedCount)
	}
	if rejected.Reason != ReasonTerminal {
		t.Fatalf("rejected reason = %q, want terminal", rejected.Reason)
	}
	if rejected.ErrorClass != ErrorClassTerminal {
		t.Fatalf("rejected class = %q, want f1_terminal", rejected.ErrorClass)
	}
}

func TestObserverSuccessorPoisonWithoutRoute(t *testing.T) {
	producer := &missingRouteProducer{}
	rec := &successorRecordingObserver{}
	client, runner := noRoutePoisonRunner(t, producer, WithObserver(rec))
	defer func() { _ = client.Close(context.Background()) }()
	settler := &dispatchSettler{}
	message := retryBridgeMessage(t, poisonEnvelope(), settler)
	if !dispatchMessage(runner, context.Background(), message, &Envelope{}, new(bool), &deliveryState{}) {
		t.Fatal("poison message was not settled")
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var rejected *PointEvent
	for i := range rec.records {
		if rec.records[i].Kind == ObserverPoisonRejected {
			rejected = &rec.records[i]
			break
		}
	}
	if rejected == nil {
		t.Fatal("no poison_rejected")
	}
	if rejected.Reason != ReasonPoison {
		t.Fatalf("reason = %q, want poison", rejected.Reason)
	}
	if rejected.ErrorClass != ErrorClassPoison {
		t.Fatalf("class = %q, want f1_poison", rejected.ErrorClass)
	}
}

type successorCtxKey struct{}

func TestObserverSuccessorSeesProcessContext(t *testing.T) {
	producer := &dispatchProducer{}
	rec := &successorRecordingObserver{processKey: successorCtxKey{}, processVal: "process-value"}
	handlers := map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error { return errors.New("boom") }),
	}
	_, runner, _ := successorObserverRunner(t, producer, rec, handlers)
	envelope := successorTestEnvelope("ctx-hop-1")
	message := retryBridgeMessage(t, envelope, &dispatchSettler{})
	var abandoned bool
	var out Envelope
	if !dispatchMessage(runner, context.Background(), message, &out, &abandoned, &deliveryState{}) {
		t.Fatal("dispatchMessage did not settle")
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if !rec.publishSaw {
		t.Fatal("Start(publish, retry) did not see the process context value")
	}
}

func TestObserverSuccessorDeadLetterTrace(t *testing.T) {
	producer := &dispatchProducer{}
	rec := &successorRecordingObserver{traceParent: "00-dl-t2-01", traceState: "dl=1"}
	handlers := map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error { return Terminal(errors.New("bad")) }),
	}
	_, runner, _ := successorObserverRunner(t, producer, rec, handlers)
	envelope := successorTestEnvelope("dl-trace-1")
	envelope.TraceParent = "00-inbound-01"
	message := retryBridgeMessage(t, envelope, &dispatchSettler{})
	var abandoned bool
	var out Envelope
	if !dispatchMessage(runner, context.Background(), message, &out, &abandoned, &deliveryState{}) {
		t.Fatal("dispatchMessage did not settle")
	}
	producer.mu.Lock()
	defer producer.mu.Unlock()
	if len(producer.messages) != 1 {
		t.Fatalf("published copies = %d, want 1", len(producer.messages))
	}
	if got := headerValue(producer.messages[0].Headers, "traceparent"); got != "00-dl-t2-01" {
		t.Fatalf("dead-letter traceparent = %q, want T2", got)
	}
	if got := headerValue(producer.messages[0].Headers, "tracestate"); got != "dl=1" {
		t.Fatalf("dead-letter tracestate = %q, want injected", got)
	}
}

func TestObserverSuccessorRetryEncodeFailureOrdering(t *testing.T) {
	producer := &dispatchProducer{}
	rec := &successorRecordingObserver{}
	_, runner, _ := successorObserverRunner(t, producer, rec, nil)
	envelope := Envelope{
		SpecVersion:  "1.0",
		ID:           "retry-encode-order",
		Source:       "/test/orders",
		Type:         "orders.created.v1",
		Priority:     PriorityHigh,
		Attempt:      1,
		PartitionKey: "kkkkkkkkkkkkkkkkkkkk",
	}
	// Grow the pin until the retry copy stops fitting but the widgets stay small.
	for len(envelope.PartitionKey) < 2000 {
		envelope.PartitionKey += envelope.PartitionKey
	}
	message := retryBridgeMessage(t, envelope, &dispatchSettler{})
	limit := sort.Search(CoreMaxHeaderBytes, func(n int) bool {
		_, err := envelope.EncodeHeaders(n + 1)
		return err == nil
	}) + 1
	retryCopy := envelope
	retryCopy.OriginalDest = message.Destination
	retryCopy.Attempt = envelope.Attempt + 1
	due := runner.client.options.clock.Now().UTC().Add(time.Second)
	retryCopy.DueTime = &due
	if _, err := retryCopy.EncodeHeaders(limit); err == nil {
		t.Fatal("premise does not hold: retry copy fits at limit")
	}
	state := &deliveryState{headerMaxBytes: limit}
	retryAndSettle(runner, context.Background(), message, envelope, errors.New("boom"), state)
	_, _, _, order := rec.snapshot()
	retryFinish := -1
	decided := -1
	dlStart := -1
	for i, entry := range order {
		switch entry {
		case "finish publish/error":
			if retryFinish < 0 {
				retryFinish = i
			}
		case "record dead_letter_decided":
			if decided < 0 {
				decided = i
			}
		case "start publish/dead_letter":
			if dlStart < 0 {
				dlStart = i
			}
		}
	}
	if retryFinish < 0 {
		t.Fatalf("no retry publish finish (order %v)", order)
	}
	if decided < 0 {
		t.Fatalf("no dead_letter_decided (order %v)", order)
	}
	if dlStart < 0 {
		t.Fatalf("no dead-letter publish start (order %v)", order)
	}
	if retryFinish >= decided || decided >= dlStart {
		t.Fatalf("order = %v, want retry finish before decided before dl start", order)
	}
}

func TestObserverSuccessorRetryClassMatchesProcess(t *testing.T) {
	producer := &dispatchProducer{}
	rec := &successorRecordingObserver{}
	driverErr := &driver.Error{Driver: "test", Op: "publish", K: driver.KindTransient, Err: errors.New("x")}
	handlers := map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error {
			return fmt.Errorf("store: %w", driverErr)
		}),
	}
	_, runner, _ := successorObserverRunner(t, producer, rec, handlers)
	envelope := successorTestEnvelope("retry-class-1")
	message := retryBridgeMessage(t, envelope, &dispatchSettler{})
	var abandoned bool
	var out Envelope
	if !dispatchMessage(runner, context.Background(), message, &out, &abandoned, &deliveryState{}) {
		t.Fatal("dispatchMessage did not settle")
	}
	_, finishes, records, _ := rec.snapshot()
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
	var scheduled *PointEvent
	for i := range records {
		if records[i].Kind == ObserverRetryScheduled {
			scheduled = &records[i]
			break
		}
	}
	if scheduled == nil {
		t.Fatal("no retry_scheduled")
	}
	if scheduled.ErrorClass != ErrorClassRetryable {
		t.Fatalf("retry class = %q, want f1_retryable", scheduled.ErrorClass)
	}
	if scheduled.ErrorClass != processFinish.ErrorClass {
		t.Fatalf("retry class = %q, process class = %q, want equal", scheduled.ErrorClass, processFinish.ErrorClass)
	}
}

func TestDeathReasonClassTable(t *testing.T) {
	for _, test := range []struct {
		reason DeathReason
		want   ErrorClass
	}{
		{ReasonDecode, ErrorClassDecode},
		{ReasonPanic, ErrorClassPanic},
		{ReasonExpired, ErrorClassExpired},
		{ReasonUnmatched, ErrorClassUnmatched},
		{ReasonMaxAttempts, ErrorClassMaxAttempts},
		{ReasonTerminal, ErrorClassTerminal},
		{ReasonPoison, ErrorClassPoison},
		{ReasonUnspecified, ErrorClassOther},
		{DeathReason("bogus"), ErrorClassOther},
	} {
		if got := deathReasonClass(test.reason); got != test.want {
			t.Fatalf("deathReasonClass(%q) = %q, want %q", test.reason, got, test.want)
		}
	}
}
