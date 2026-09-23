package f1

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

type recordingObserver struct {
	mu         sync.Mutex
	starts     uint64
	finishes   uint64
	records    uint64
	lastStart  StartEvent
	lastFinish FinishEvent
	lastPoint  PointEvent
	lastToken  Token
}

func (o *recordingObserver) Start(_ context.Context, event StartEvent) (context.Context, Token) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.starts++
	o.lastStart = event
	token := Token{Kind: event.Kind, Start: event.At, Handle: o.starts}
	o.lastToken = token
	return context.WithValue(context.Background(), observerTestKey{}, o.starts), token
}

type observerTestKey struct{}

func (o *recordingObserver) Finish(token Token, event FinishEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.finishes++
	o.lastToken = token
	o.lastFinish = event
}

func (o *recordingObserver) Record(event PointEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.records++
	o.lastPoint = event
}

func (o *recordingObserver) counts() (uint64, uint64, uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.starts, o.finishes, o.records
}

type panickingObserver struct{}

func (panickingObserver) Start(context.Context, StartEvent) (context.Context, Token) {
	panic("observer start panic")
}

func (panickingObserver) Finish(Token, FinishEvent) {
	panic("observer finish panic")
}

func (panickingObserver) Record(PointEvent) {
	panic("observer record panic")
}

type nilContextObserver struct {
	token Token
}

func (o *nilContextObserver) Start(_ context.Context, event StartEvent) (context.Context, Token) {
	o.token = Token{Kind: event.Kind, Start: event.At, Handle: 1}
	return nil, o.token
}

func (*nilContextObserver) Finish(Token, FinishEvent) {}

func (*nilContextObserver) Record(PointEvent) {}

func TestObserverRecordingCountsCalls(t *testing.T) {
	t.Parallel()
	rec := &recordingObserver{}
	client := newPublishClient(t, &recordingProducer{}, WithObserver(rec))
	baseStarts, baseFinishes, baseRecords := rec.counts()
	ctx := context.Background()
	start := StartEvent{Kind: ObserverProcess, At: client.options.clock.Now(), Topic: "orders.created"}
	if client.observer == nil {
		t.Fatal("client observer is nil")
	}
	_, token := client.observeStart(ctx, start)
	finish := FinishEvent{Kind: ObserverProcess, At: client.options.clock.Now(), Outcome: ObserverOutcomeOK}
	client.observeFinish(token, finish)
	point := PointEvent{Kind: ObserverDeliveryReceived, At: client.options.clock.Now(), Topic: "orders.created"}
	client.observeRecord(point)
	starts, finishes, records := rec.counts()
	if starts-baseStarts != 1 || finishes-baseFinishes != 1 || records-baseRecords != 1 {
		t.Fatalf("counts = %d/%d/%d, want 1/1/1", starts-baseStarts, finishes-baseFinishes, records-baseRecords)
	}
}

func TestObserverPanickingMethodsPanic(t *testing.T) {
	t.Parallel()
	var panicker panickingObserver
	for _, call := range []func(){
		func() { _, _ = panicker.Start(context.Background(), StartEvent{}) },
		func() { panicker.Finish(Token{}, FinishEvent{}) },
		func() { panicker.Record(PointEvent{}) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("observer method did not panic")
				}
			}()
			call()
		}()
	}
}

func TestObserverRecoverKeepsGoingAndLogsOncePerKind(t *testing.T) {
	t.Parallel()
	var logs logSink
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	client := newPublishClient(t, &recordingProducer{},
		WithObserver(panickingObserver{}),
		WithLogger(logger),
	)
	basePanics := strings.Count(logs.String(), "f1 observer panicked")
	ctx := context.Background()
	for _, kind := range []ObserverKind{ObserverProcess, ObserverProcess, ObserverPublish} {
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("observeStart panicked: %v", recovered)
				}
			}()
			event := StartEvent{Kind: kind, At: client.options.clock.Now()}
			if client.observer != nil {
				_, _ = client.observeStart(ctx, event)
			}
		}()
	}
	quiescePublishClient(t, client)
	output := logs.String()
	if got := strings.Count(output, "f1 observer panicked") - basePanics; got != 2 {
		t.Fatalf("panic log lines = %d, want 2; output=%q", got, output)
	}
}

func TestObserverFinishGuardAbandonsExactlyOnce(t *testing.T) {
	t.Parallel()
	t.Run("panic abandons", func(t *testing.T) {
		t.Parallel()
		rec2 := &recordingObserver{}
		client2 := newPublishClient(t, &recordingProducer{}, WithObserver(rec2))
		start2 := StartEvent{Kind: ObserverProcess, At: client2.options.clock.Now()}
		_, token2 := client2.observeStart(context.Background(), start2)
		func() {
			defer func() { _ = recover() }()
			guard := client2.newObserverGuard(ObserverProcess, token2)
			defer func() { guard.abandon() }()
			panic("boom")
		}()
		rec2.mu.Lock()
		finishes := rec2.finishes
		outcome := rec2.lastFinish.Outcome
		rec2.mu.Unlock()
		if finishes != 1 {
			t.Fatalf("finish count = %d, want 1", finishes)
		}
		if outcome != ObserverOutcomeAbandoned {
			t.Fatalf("outcome = %q, want abandoned", outcome)
		}
	})
	t.Run("explicit finish wins", func(t *testing.T) {
		t.Parallel()
		rec := &recordingObserver{}
		client := newPublishClient(t, &recordingProducer{}, WithObserver(rec))
		start := StartEvent{Kind: ObserverProcess, At: client.options.clock.Now()}
		_, token := client.observeStart(context.Background(), start)
		func() {
			guard := client.newObserverGuard(ObserverProcess, token)
			defer func() { guard.abandon() }()
			guard.finishWith(FinishEvent{Outcome: ObserverOutcomeOK})
		}()
		rec.mu.Lock()
		finishes := rec.finishes
		outcome := rec.lastFinish.Outcome
		rec.mu.Unlock()
		if finishes != 1 {
			t.Fatalf("finish count = %d, want 1", finishes)
		}
		if outcome != ObserverOutcomeOK {
			t.Fatalf("outcome = %q, want ok", outcome)
		}
	})
}

func TestObserverStartNilContextFallsBack(t *testing.T) {
	t.Parallel()
	obs := &nilContextObserver{}
	client := newPublishClient(t, &recordingProducer{}, WithObserver(obs))
	caller := context.WithValue(context.Background(), observerTestKey{}, "caller")
	event := StartEvent{Kind: ObserverProcess, At: client.options.clock.Now()}
	var got context.Context
	if client.observer != nil {
		varico, _ := client.observeStart(caller, event)
		got = varico
	} else {
		t.Fatal("client observer is nil")
	}
	if got != caller {
		t.Fatal("nil observer context did not fall back to caller context")
	}
}

func TestWithBacklogPollInterval(t *testing.T) {
	t.Parallel()
	t.Run("zero resolves to 15s", func(t *testing.T) {
		t.Parallel()
		client, err := New(context.Background(), testClientConfig(t),
			WithDriver(&testDriver{conn: &testConn{}}),
			WithBacklogPollInterval(0),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close(context.Background()) })
		if got := client.backlogPollInterval; got != 15*time.Second {
			t.Fatalf("interval = %v, want 15s", got)
		}
	})
	t.Run("1s accepted", func(t *testing.T) {
		t.Parallel()
		client, err := New(context.Background(), testClientConfig(t),
			WithDriver(&testDriver{conn: &testConn{}}),
			WithBacklogPollInterval(time.Second),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close(context.Background()) })
		if got := client.backlogPollInterval; got != time.Second {
			t.Fatalf("interval = %v, want 1s", got)
		}
	})
	t.Run("500ms rejected", func(t *testing.T) {
		t.Parallel()
		_, err := New(context.Background(), testClientConfig(t),
			WithDriver(&testDriver{conn: &testConn{}}),
			WithBacklogPollInterval(500*time.Millisecond),
		)
		if err == nil || !strings.Contains(err.Error(), "WithBacklogPollInterval") {
			t.Fatalf("error = %v, want WithBacklogPollInterval rejection", err)
		}
	})
	t.Run("minus one disables", func(t *testing.T) {
		t.Parallel()
		client, err := New(context.Background(), testClientConfig(t),
			WithDriver(&testDriver{conn: &testConn{}}),
			WithBacklogPollInterval(time.Duration(-1)),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close(context.Background()) })
		if got := client.backlogPollInterval; got >= 0 {
			t.Fatalf("interval = %v, want negative disabled value", got)
		}
	})
}

func TestWithObserverNilIsNoObserver(t *testing.T) {
	t.Parallel()
	fake := clock.NewFake(time.Unix(0, 0))
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&testDriver{conn: &testConn{}}),
		WithObserver(nil),
		withClock(fake),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	if client.observer != nil {
		t.Fatal("observer is not nil")
	}
	if client.traceInjector != nil {
		t.Fatal("injector is not nil")
	}
}
