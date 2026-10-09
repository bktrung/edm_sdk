package f1

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"

	"github.com/stretchr/testify/require"
)

// publishRecordingObserver is the phase 4a recorder local to this file. The
// shared recorder stays phase 5 owned.
type publishRecordingObserver struct {
	mu       sync.Mutex
	starts   []StartEvent
	finishes []FinishEvent
	tokens   []Token
	order    []string
}

func (o *publishRecordingObserver) Start(_ context.Context, event StartEvent) (context.Context, Token) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.starts = append(o.starts, event)
	o.order = append(o.order, "start "+string(event.Kind))
	token := Token{Kind: event.Kind, Start: event.At, Handle: uint64(len(o.starts))}
	o.tokens = append(o.tokens, token)
	return context.WithValue(context.Background(), publishObserverKey{}, len(o.starts)), token
}

type publishObserverKey struct{}

func (o *publishRecordingObserver) Finish(token Token, event FinishEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.finishes = append(o.finishes, event)
	o.order = append(o.order, "finish "+string(event.Kind)+"/"+string(event.Outcome))
	o.tokens = append(o.tokens, token)
}

func (o *publishRecordingObserver) Record(PointEvent) {}

func (o *publishRecordingObserver) snapshot() (starts []StartEvent, finishes []FinishEvent, order []string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	starts = append([]StartEvent(nil), o.starts...)
	finishes = append([]FinishEvent(nil), o.finishes...)
	order = append([]string(nil), o.order...)
	return starts, finishes, order
}

// publishInjectObserver records like publishRecordingObserver and also
// implements TraceInjector with fixed values.
type publishInjectObserver struct {
	publishRecordingObserver
	traceParent string
	traceState  string
}

func (o *publishInjectObserver) Start(ctx context.Context, event StartEvent) (context.Context, Token) {
	return o.publishRecordingObserver.Start(ctx, event)
}

func (o *publishInjectObserver) Finish(token Token, event FinishEvent) {
	o.publishRecordingObserver.Finish(token, event)
}

func (o *publishInjectObserver) InjectTrace(context.Context) (string, string) {
	return o.traceParent, o.traceState
}

type failEncodeCodec struct {
	err error
}

func (c failEncodeCodec) Name() string        { return "json" }
func (c failEncodeCodec) ContentType() string { return "application/json" }
func (c failEncodeCodec) Encode(any) ([]byte, error) {
	if c.err != nil {
		return nil, c.err
	}
	return nil, errors.New("encode payload: forced failure")
}
func (c failEncodeCodec) Decode([]byte, any) error { return nil }

func TestObserverPublishSingle(t *testing.T) {
	t.Parallel()
	producer := &recordingProducer{}
	rec := &publishRecordingObserver{}
	client := newPublishClient(t, producer, WithObserver(rec))
	ctx := context.Background()
	id, err := client.Publisher().Publish(ctx, "orders.created", map[string]string{"id": "42"}, WithCorrelationID("corr-1"))
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("Publish returned an empty event ID")
	}
	starts, finishes, order := rec.snapshot()
	wantOrder := []string{"start publish", "start message_built", "finish message_built/ok", "finish publish/ok"}
	if len(order) != len(wantOrder) {
		t.Fatalf("order = %v, want %v", order, wantOrder)
	}
	for i := range wantOrder {
		if order[i] != wantOrder[i] {
			t.Fatalf("order = %v, want %v", order, wantOrder)
		}
	}
	if len(starts) != 2 || len(finishes) != 2 {
		t.Fatalf("starts = %d finishes = %d, want 2/2", len(starts), len(finishes))
	}
	publishStart := starts[0]
	if publishStart.Kind != ObserverPublish {
		t.Fatalf("publish start kind = %q, want publish", publishStart.Kind)
	}
	if publishStart.Route != PublishRoutePrimary {
		t.Fatalf("publish start route = %q, want primary", publishStart.Route)
	}
	if publishStart.BatchSize != 1 {
		t.Fatalf("publish start batch = %d, want 1", publishStart.BatchSize)
	}
	builtStart := starts[1]
	if builtStart.Kind != ObserverMessageBuilt {
		t.Fatalf("built start kind = %q, want message_built", builtStart.Kind)
	}
	if builtStart.MessageID != id {
		t.Fatalf("built start id = %q, want %q", builtStart.MessageID, id)
	}
	if builtStart.EventType != "orders.created" {
		t.Fatalf("built start type = %q, want orders.created", builtStart.EventType)
	}
	if builtStart.Topic != "orders.created" {
		t.Fatalf("built start topic = %q, want orders.created", builtStart.Topic)
	}
	if builtStart.Priority != PriorityMedium {
		t.Fatalf("built start priority = %v, want medium", builtStart.Priority)
	}
	if builtStart.Attempt != 1 {
		t.Fatalf("built start attempt = %d, want 1", builtStart.Attempt)
	}
	if builtStart.CorrelationID != "corr-1" {
		t.Fatalf("built start correlation = %q, want corr-1", builtStart.CorrelationID)
	}
	builtFinish := finishes[0]
	if builtFinish.Kind != ObserverMessageBuilt || builtFinish.Outcome != ObserverOutcomeOK {
		t.Fatalf("built finish = %q/%q, want message_built/ok", builtFinish.Kind, builtFinish.Outcome)
	}
	publishFinish := finishes[1]
	if publishFinish.Kind != ObserverPublish || publishFinish.Outcome != ObserverOutcomeOK {
		t.Fatalf("publish finish = %q/%q, want publish/ok", publishFinish.Kind, publishFinish.Outcome)
	}
	if len(publishFinish.Results) != 1 {
		t.Fatalf("publish results len = %d, want 1", len(publishFinish.Results))
	}
	if publishFinish.Results[0].ID != id || publishFinish.Results[0].ErrorClass != "" {
		t.Fatalf("publish results[0] = %+v, want ID %q empty error class", publishFinish.Results[0], id)
	}
}

func TestObserverPublishBatchPartialFailure(t *testing.T) {
	t.Parallel()
	classified := &driver.Error{Driver: "test", Op: "publish", K: driver.KindFatal, Err: errors.New("broker rejected index 1")}
	producer := &recordingProducer{publishErr: &driver.PublishError{Failed: map[int]error{1: classified}}}
	rec := &publishRecordingObserver{}
	client := newPublishClient(t, producer, WithObserver(rec))
	result, err := client.Publisher().PublishBatch(context.Background(), []Message{
		{EventType: "orders.created", Payload: "one"},
		{EventType: "orders.created", Payload: "two"},
		{EventType: "orders.created", Payload: "three"},
	})
	if err != nil {
		t.Fatalf("PublishBatch() error = %v, want nil for partial failure", err)
	}
	if got := failedIndexes(result); len(got) != 1 || got[0] != 1 {
		t.Fatalf("failed = %v, want [1]", got)
	}
	starts, finishes, order := rec.snapshot()
	if len(starts) != 4 || len(finishes) != 4 {
		t.Fatalf("starts = %d finishes = %d, want 4/4", len(starts), len(finishes))
	}
	wantOrder := []string{
		"start publish",
		"start message_built", "finish message_built/ok",
		"start message_built", "finish message_built/ok",
		"start message_built", "finish message_built/ok",
		"finish publish/error",
	}
	if len(order) != len(wantOrder) {
		t.Fatalf("order = %v, want %v", order, wantOrder)
	}
	for i := range wantOrder {
		if order[i] != wantOrder[i] {
			t.Fatalf("order = %v, want %v", order, wantOrder)
		}
	}
	var publishFinish FinishEvent
	for _, f := range finishes {
		if f.Kind == ObserverPublish {
			publishFinish = f
		}
	}
	if publishFinish.Outcome != ObserverOutcomeError {
		t.Fatalf("publish outcome = %q, want error", publishFinish.Outcome)
	}
	if publishFinish.ErrorClass != ErrorClassDriverFatal {
		t.Fatalf("publish class = %q, want driver_fatal", publishFinish.ErrorClass)
	}
	if len(publishFinish.Results) != len(result.Results) {
		t.Fatalf("observer results len = %d, want %d", len(publishFinish.Results), len(result.Results))
	}
	for i := range result.Results {
		got, want := publishFinish.Results[i], result.Results[i]
		if got.ID != want.ID {
			t.Fatalf("results[%d].ID = %q, want %q", i, got.ID, want.ID)
		}
		if got.ErrorClass != errorClassOf(want.Err) {
			t.Fatalf("results[%d].ErrorClass mismatch: %q vs %q", i, got.ErrorClass, errorClassOf(want.Err))
		}
	}
}

func TestObserverPublishBuildFailure(t *testing.T) {
	t.Parallel()
	producer := &recordingProducer{}
	rec := &publishRecordingObserver{}
	client := newPublishClient(t, producer, WithObserver(rec), WithCodec(failEncodeCodec{}))
	_, err := client.Publisher().PublishBatch(context.Background(), []Message{
		{EventType: "orders.created", Payload: "boom"},
	})
	if err == nil {
		t.Fatal("PublishBatch() error = nil, want codec failure")
	}
	_, finishes, _ := rec.snapshot()
	publishFinishes := 0
	for _, f := range finishes {
		if f.Kind == ObserverPublish {
			publishFinishes++
			if f.Outcome != ObserverOutcomeError {
				t.Fatalf("publish outcome = %q, want error", f.Outcome)
			}
		}
	}
	if publishFinishes != 1 {
		t.Fatalf("publish finishes = %d, want exactly 1", publishFinishes)
	}
}

func TestObserverPublishTraceInjection(t *testing.T) {
	t.Parallel()
	t.Run("injector values land in headers", func(t *testing.T) {
		t.Parallel()
		producer := &recordingProducer{}
		rec := &publishInjectObserver{traceParent: "00-abc-def-01", traceState: "rojo=1"}
		client := newPublishClient(t, producer, WithObserver(rec))
		if _, err := client.Publisher().Publish(context.Background(), "orders.created", "payload"); err != nil {
			t.Fatal(err)
		}
		headers := messageHeaders(producer.messages[0])
		if headers["traceparent"] != "00-abc-def-01" {
			t.Fatalf("traceparent = %q, want injected value", headers["traceparent"])
		}
		if headers["tracestate"] != "rojo=1" {
			t.Fatalf("tracestate = %q, want injected value", headers["tracestate"])
		}
	})
	t.Run("empty injector keeps caused-by copy", func(t *testing.T) {
		t.Parallel()
		producer := &recordingProducer{}
		rec := &publishInjectObserver{}
		client := newPublishClient(t, producer, WithObserver(rec))
		parent := &Event{envelope: Envelope{ID: "cause-1", TraceParent: "00-parent-parent-01", TraceState: "parent=1", CorrelationID: "corr-parent"}}
		if _, err := client.Publisher().Publish(context.Background(), "orders.created", "payload", WithCausedBy(parent)); err != nil {
			t.Fatal(err)
		}
		headers := messageHeaders(producer.messages[0])
		if headers["traceparent"] != "00-parent-parent-01" {
			t.Fatalf("traceparent = %q, want parent copy", headers["traceparent"])
		}
		if headers["tracestate"] != "parent=1" {
			t.Fatalf("tracestate = %q, want parent copy", headers["tracestate"])
		}
	})
	t.Run("injector replaces caused-by copy", func(t *testing.T) {
		t.Parallel()
		producer := &recordingProducer{}
		rec := &publishInjectObserver{traceParent: "00-new-new-01", traceState: "new=1"}
		client := newPublishClient(t, producer, WithObserver(rec))
		parent := &Event{envelope: Envelope{ID: "cause-1", TraceParent: "00-parent-parent-01", TraceState: "parent=1", CorrelationID: "corr-parent"}}
		if _, err := client.Publisher().Publish(context.Background(), "orders.created", "payload", WithCausedBy(parent)); err != nil {
			t.Fatal(err)
		}
		headers := messageHeaders(producer.messages[0])
		if headers["traceparent"] != "00-new-new-01" {
			t.Fatalf("traceparent = %q, want replacement", headers["traceparent"])
		}
		if headers["tracestate"] != "new=1" {
			t.Fatalf("tracestate = %q, want replacement", headers["tracestate"])
		}
	})
	t.Run("injection never fails publish under header pressure", func(t *testing.T) {
		t.Parallel()
		producer := &recordingProducer{}
		rec := &publishInjectObserver{traceParent: "00-abc-def-01", traceState: "rojo=1"}
		conn := &publishConn{producer: producer, info: driver.BrokerInfo{Kind: "test", Version: "1"}}
		cfg := testClientConfig(t)
		cfg.Codec.MaxHeaderBytes = 512
		client, err := New(context.Background(), cfg, WithDriver(&publishDriver{conn: conn}), WithObserver(rec))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close(context.Background()) })
		big := make([]byte, 1000)
		for i := range big {
			big[i] = 'x'
		}
		_, injectErr := client.Publisher().Publish(context.Background(), "orders.created", "payload",
			WithHeader("x-debug", string(big)))
		plainProducer := &recordingProducer{}
		plainConn := &publishConn{producer: plainProducer, info: driver.BrokerInfo{Kind: "test", Version: "1"}}
		plainClient, err := New(context.Background(), cfg, WithDriver(&publishDriver{conn: plainConn}))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = plainClient.Close(context.Background()) })
		_, plainErr := plainClient.Publisher().Publish(context.Background(), "orders.created", "payload",
			WithHeader("x-debug", string(big)))
		if (injectErr == nil) != (plainErr == nil) {
			t.Fatalf("inject err = %v, plain err = %v; injection must not change the outcome", injectErr, plainErr)
		}
	})
}

func TestObserverPublishNoObserverHeadersUnchanged(t *testing.T) {
	t.Parallel()
	fake := clock.NewFake(time.Unix(1000, 0))
	producer := &recordingProducer{}
	client := newPublishClient(t, producer, withClock(fake))
	id, err := client.Publisher().Publish(context.Background(), "orders.created", "payload",
		WithCorrelationID("corr-noobs"), WithKey("k1"))
	if err != nil {
		t.Fatal(err)
	}
	if len(producer.messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(producer.messages))
	}
	gotHeaders := messageHeaders(producer.messages[0])
	now := fake.Now().UTC()
	expectedEnvelope := Envelope{
		SpecVersion:     "1.0",
		ID:              id,
		Source:          client.source,
		Type:            "orders.created",
		Time:            now,
		DataContentType: client.options.codec.ContentType(),
		Priority:        PriorityMedium,
		Attempt:         1,
		IdempotencyKey:  id,
		CorrelationID:   "corr-noobs",
		Producer:        client.producer,
		PartitionKey:    "k1",
	}
	headerMaxBytes := effectiveHeaderLimit(client.config.Codec.MaxHeaderBytes, client.effective.MaxHeaderBytes)
	expected, err := expectedEnvelope.EncodeHeaders(headerMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(gotHeaders) != len(expected) {
		t.Fatalf("headers len = %d, want %d: got %v want %v", len(gotHeaders), len(expected), gotHeaders, expected)
	}
	keys := make([]string, 0, len(gotHeaders))
	for k := range gotHeaders {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	wantKeys := make([]string, 0, len(expected))
	for k := range expected {
		wantKeys = append(wantKeys, k)
	}
	sort.Strings(wantKeys)
	for i := range keys {
		if keys[i] != wantKeys[i] {
			t.Fatalf("header keys = %v, want %v", keys, wantKeys)
		}
		if gotHeaders[keys[i]] != expected[keys[i]] {
			t.Fatalf("header %q = %q, want %q", keys[i], gotHeaders[keys[i]], expected[keys[i]])
		}
	}
	if _, ok := gotHeaders["traceparent"]; ok {
		t.Fatal("traceparent present without injector or caused-by")
	}
}

func TestErrorClassOfTable(t *testing.T) {
	t.Parallel()
	terminal := Terminal(errors.New("terminal"))
	dropped := Drop(errors.New("dropped"))
	plain := errors.New("plain")
	for _, test := range []struct {
		name string
		err  error
		want ErrorClass
	}{
		{"nil gives empty", nil, ""},
		{"transient", &driver.Error{Driver: "test", Op: "publish", K: driver.KindTransient, Err: errors.New("x")}, ErrorClassDriverTransient},
		{"fatal", &driver.Error{Driver: "test", Op: "publish", K: driver.KindFatal, Err: errors.New("x")}, ErrorClassDriverFatal},
		{"not found", &driver.Error{Driver: "test", Op: "publish", K: driver.KindNotFound, Err: errors.New("x")}, ErrorClassDriverNotFound},
		{"too large", &driver.Error{Driver: "test", Op: "publish", K: driver.KindTooLarge, Err: errors.New("x")}, ErrorClassDriverTooLarge},
		{"permission", &driver.Error{Driver: "test", Op: "publish", K: driver.KindPermission, Err: errors.New("x")}, ErrorClassDriverPermission},
		{"notification", &driver.Error{Driver: "test", Op: "publish", K: driver.KindNotification, Err: errors.New("x")}, ErrorClassDriverNotification},
		{"terminal", terminal, ErrorClassTerminal},
		{"dropped", dropped, ErrorClassDropped},
		{"other", plain, ErrorClassOther},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := errorClassOf(test.err); got != test.want {
				t.Fatalf("errorClassOf = %q, want %q", got, test.want)
			}
		})
	}
}

// TestTerminalAndDroppedOutrankDriverKind proves the two mappers that class one
// error for paired events agree: an error carrying both a driver kind and the
// terminal or dropped marker reports the marker on the settle and successor
// finishes exactly as it does on the process finish, so one error never
// carries two classes across those events.
func TestTerminalAndDroppedOutrankDriverKind(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		err  error
		want ErrorClass
	}{
		{
			name: "terminal driver error is terminal",
			err:  Terminal(&driver.Error{Driver: "test", Op: "publish", K: driver.KindFatal, Err: errors.New("x")}),
			want: ErrorClassTerminal,
		},
		{
			name: "dropped driver error is dropped",
			err:  Drop(&driver.Error{Driver: "test", Op: "publish", K: driver.KindFatal, Err: errors.New("x")}),
			want: ErrorClassDropped,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := errorClassOf(test.err); got != test.want {
				t.Fatalf("errorClassOf = %q, want %q", got, test.want)
			}
			outcome, class, terminal := processOutcome(handlerResult{err: test.err}, nil)
			if outcome != ObserverOutcomeError || class != test.want || !terminal {
				t.Fatalf("processOutcome = %q, %q, %v; want %q, %q, true", outcome, class, terminal, ObserverOutcomeError, test.want)
			}
		})
	}
}

func TestObserverFinishWithAbandonIsNoop(t *testing.T) {
	t.Parallel()
	rec := &publishRecordingObserver{}
	client := newPublishClient(t, &recordingProducer{}, WithObserver(rec))
	start := StartEvent{Kind: ObserverPublish, At: client.options.clock.Now()}
	_, token := client.observeStart(context.Background(), start)
	guard := client.newObserverGuard(ObserverPublish, token)
	guard.finishWith(FinishEvent{Outcome: ObserverOutcomeOK, Results: []ObserverMessageResult{{ID: "x"}}})
	guard.abandon()
	rec.mu.Lock()
	finishes := len(rec.finishes)
	outcome := rec.finishes[0].Outcome
	rec.mu.Unlock()
	if finishes != 1 {
		t.Fatalf("finish count = %d, want 1", finishes)
	}
	if outcome != ObserverOutcomeOK {
		t.Fatalf("outcome = %q, want ok", outcome)
	}
}

// publishPanicBuiltObserver panics in Start(message_built) and records
// everything else like publishRecordingObserver.
type publishPanicBuiltObserver struct {
	publishRecordingObserver
}

func (o *publishPanicBuiltObserver) Start(ctx context.Context, event StartEvent) (context.Context, Token) {
	if event.Kind == ObserverMessageBuilt {
		panic("message_built start panic")
	}
	return o.publishRecordingObserver.Start(ctx, event)
}

// publishPanicInjectObserver records normally but panics in InjectTrace.
type publishPanicInjectObserver struct {
	publishRecordingObserver
}

func (o *publishPanicInjectObserver) InjectTrace(context.Context) (string, string) {
	panic("inject panic")
}

func TestObserverPublishPanicLoggedOncePerKind(t *testing.T) {
	t.Parallel()
	t.Run("start panics once", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		producer := &recordingProducer{}
		rec := &publishPanicBuiltObserver{}
		client := newPublishClient(t, producer,
			WithObserver(rec),
			WithLogger(slog.New(slog.NewTextHandler(&buf, nil))),
		)
		for i := range 5 {
			if _, err := client.Publisher().Publish(context.Background(), "orders.created", "payload"); err != nil {
				t.Fatalf("Publish() %d error = %v, want nil", i, err)
			}
		}
		quiescePublishClient(t, client)
		output := buf.String()
		if got := strings.Count(output, "f1 observer panicked"); got != 1 {
			t.Fatalf("panic log lines = %d, want 1; output=%q", got, output)
		}
		if !strings.Contains(output, "kind=message_built") || !strings.Contains(output, "phase=start") {
			t.Fatalf("panic line names the wrong call: output=%q", output)
		}
	})
	t.Run("inject panics once", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		producer := &recordingProducer{}
		rec := &publishPanicInjectObserver{}
		client := newPublishClient(t, producer,
			WithObserver(rec),
			WithLogger(slog.New(slog.NewTextHandler(&buf, nil))),
		)
		parent := &Event{envelope: Envelope{ID: "cause-1", TraceParent: "00-parent-parent-01", TraceState: "parent=1", CorrelationID: "corr-parent"}}
		for i := range 5 {
			if _, err := client.Publisher().Publish(context.Background(), "orders.created", "payload", WithCausedBy(parent)); err != nil {
				t.Fatalf("Publish() %d error = %v, want nil", i, err)
			}
		}
		quiescePublishClient(t, client)
		output := buf.String()
		if got := strings.Count(output, "f1 observer panicked"); got != 1 {
			t.Fatalf("panic log lines = %d, want 1; output=%q", got, output)
		}
		if !strings.Contains(output, "kind=message_built") || !strings.Contains(output, "phase=inject") {
			t.Fatalf("panic line names the wrong call: output=%q", output)
		}
		headers := messageHeaders(producer.messages[0])
		if headers["traceparent"] != "00-parent-parent-01" {
			t.Fatalf("traceparent = %q, want parent copy", headers["traceparent"])
		}
		if headers["tracestate"] != "parent=1" {
			t.Fatalf("tracestate = %q, want parent copy", headers["tracestate"])
		}
	})
}

type publishResultSnapshot struct {
	id    string
	class ErrorClass
}

type publishResultObserver struct {
	results  []publishResultSnapshot
	class    ErrorClass
	finishes int
}

func (*publishResultObserver) Start(ctx context.Context, event StartEvent) (context.Context, Token) {
	return ctx, Token{Kind: event.Kind, Start: event.At, Handle: 1}
}

func (o *publishResultObserver) Finish(_ Token, event FinishEvent) {
	if event.Kind != ObserverPublish {
		return
	}
	o.finishes++
	o.class = event.ErrorClass
	for _, result := range event.Results {
		o.results = append(o.results, publishResultSnapshot{id: result.ID, class: result.ErrorClass})
	}
}

func (*publishResultObserver) Record(PointEvent) {}

func TestObserverPublishResultsCarryIDAndClass(t *testing.T) {
	t.Parallel()
	fatal := &driver.Error{Driver: "test", Op: "publish", K: driver.KindFatal, Err: errors.New("broker refused")}
	permission := &driver.Error{Driver: "test", Op: "publish", K: driver.KindPermission, Err: errors.New("permission denied")}
	for _, test := range []struct {
		name       string
		publishErr error
		codecFail  bool
		methodErr  bool
		classes    []ErrorClass
		published  []bool
		topClass   ErrorClass
	}{
		{
			name:       "partial",
			publishErr: &driver.PublishError{Failed: map[int]error{1: fatal, 2: permission}},
			classes:    []ErrorClass{"", ErrorClassDriverFatal, ErrorClassDriverPermission},
			published:  []bool{true, false, false},
			topClass:   ErrorClassDriverFatal,
		},
		{
			name:       "transport",
			publishErr: fatal,
			methodErr:  true,
			classes:    []ErrorClass{ErrorClassDriverFatal, ErrorClassDriverFatal, ErrorClassDriverFatal},
			published:  []bool{false, false, false},
			topClass:   ErrorClassDriverFatal,
		},
		{
			name:       "missing partial cause",
			publishErr: &driver.PublishError{Failed: map[int]error{1: nil}},
			classes:    []ErrorClass{"", ErrorClassOther, ""},
			published:  []bool{true, false, true},
			topClass:   ErrorClassOther,
		},
		{
			name:      "unattempted codec failure",
			codecFail: true,
			methodErr: true,
			classes:   []ErrorClass{"", "", ""},
			published: []bool{false, false, false},
			topClass:  ErrorClassOther,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			rec := &publishResultObserver{}
			options := []Option{WithObserver(rec)}
			if test.codecFail {
				options = append(options, WithCodec(failEncodeCodec{}))
			}
			client := newPublishClient(t, &recordingProducer{publishErr: test.publishErr}, options...)
			_, err := client.Publisher().PublishBatch(context.Background(), []Message{
				{EventType: "orders.created", Payload: "one"},
				{EventType: "orders.created", Payload: "two"},
				{EventType: "orders.created", Payload: "three"},
			})
			if (err != nil) != test.methodErr {
				t.Fatalf("method error = %v, want error %v", err, test.methodErr)
			}
			if rec.finishes != 1 || rec.class != test.topClass {
				t.Fatalf("primary finishes/class = %d/%q, want 1/%q", rec.finishes, rec.class, test.topClass)
			}
			if len(rec.results) != len(test.classes) {
				t.Fatalf("observer results = %d, want %d", len(rec.results), len(test.classes))
			}
			for i, result := range rec.results {
				if result.class != test.classes[i] || (result.id != "") != test.published[i] {
					t.Fatalf("observer result[%d] = %+v, want class %q published %v", i, result, test.classes[i], test.published[i])
				}
			}
		})
	}
}

func TestObserverPublishBatchPreservesCallerErrors(t *testing.T) {
	t.Parallel()
	cause := errors.New("broker refused")
	fatal := &driver.Error{Driver: "test", Op: "publish", K: driver.KindFatal, Err: cause}
	for _, observed := range []bool{false, true} {
		for _, transport := range []bool{false, true} {
			t.Run(fmt.Sprintf("observed=%v/transport=%v", observed, transport), func(t *testing.T) {
				t.Parallel()
				var publishErr error = &driver.PublishError{Failed: map[int]error{1: fatal}}
				if transport {
					publishErr = fatal
				}
				rec := &publishResultObserver{}
				var options []Option
				if observed {
					options = append(options, WithObserver(rec))
				}
				client := newPublishClient(t, &recordingProducer{publishErr: publishErr}, options...)
				result, err := client.Publisher().PublishBatch(context.Background(), []Message{
					{EventType: "orders.created", Payload: "one"},
					{EventType: "orders.created", Payload: "two"},
					{EventType: "orders.created", Payload: "three"},
				})
				if transport {
					require.Same(t, fatal, err)
					if !errors.Is(err, cause) {
						t.Fatalf("transport method error lost original error/cause: %v", err)
					}
				} else if err != nil {
					t.Fatalf("partial method error = %v, want nil", err)
				}
				if len(result.Results) != 3 {
					t.Fatalf("caller results = %d, want 3", len(result.Results))
				}
				for i, message := range result.Results {
					if transport || i == 1 {
						require.Same(t, fatal, message.Err)
						if message.ID != "" || !errors.Is(message.Err, cause) {
							t.Fatalf("caller result[%d] lost original error/cause: %+v", i, message)
						}
					} else if message.ID == "" || message.Err != nil {
						t.Fatalf("caller result[%d] is not a success: %+v", i, message)
					}
				}
				if observed {
					if len(rec.results) != 3 || rec.results[1].class != ErrorClassDriverFatal {
						t.Fatalf("observer must receive a bounded class while the caller retains the error: %+v", rec.results)
					}
					for i, message := range result.Results {
						if rec.results[i].id != message.ID {
							t.Fatalf("observer/caller IDs differ at index %d", i)
						}
					}
				}
			})
		}
	}
}
