package f1_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/f1test"
)

type observerScenario struct {
	name  string
	run   func(*testing.T, *f1test.Client, *f1test.Recorder)
	count map[f1.ObserverKind]int
	order [][]string
}

// TestObserverContract drives each scenario through the inmem public API. Decode
// failure, poison without a route, reconnect and a partial batch failure cannot
// be produced from there, so the package-internal observer tests own them.
func TestObserverContract(t *testing.T) {
	factory := inmemObserverFactory
	for _, scenario := range []observerScenario{
		{
			name:  "success",
			count: successCounts(),
			order: [][]string{
				{"start publish", "start message_built", "finish message_built", "finish publish"},
				{"record delivery_received", "start process", "finish process", "start settle", "finish settle"},
			},
			run: runSuccess,
		},
		{
			name:  "handler error retry success",
			count: retryCounts(),
			order: [][]string{
				{"record delivery_received", "start process", "finish process", "start settle", "finish settle"},
				{"finish process", "start publish", "finish publish", "record retry_scheduled", "start settle", "finish settle"},
			},
			run: runRetry,
		},
		{
			name:  "max attempts dead letter",
			count: maxAttemptsCounts(),
			order: [][]string{
				{"record delivery_received", "start process", "finish process", "start settle", "finish settle"},
				{"finish process", "record dead_letter_decided", "start publish", "finish publish", "record dead_letter_published", "start settle", "finish settle"},
			},
			run: runMaxAttempts,
		},
		{
			name:  "handler panic",
			count: panicCounts(),
			order: [][]string{
				{"record delivery_received", "start process", "finish process", "record dead_letter_decided", "start publish", "finish publish", "record dead_letter_published", "start settle", "finish settle"},
			},
			run: runHandlerPanic,
		},
		{
			name:  "unmatched event type",
			count: unmatchedCounts(),
			order: [][]string{
				{"record delivery_received", "record dead_letter_decided", "start publish", "finish publish", "record dead_letter_published", "start settle", "finish settle"},
			},
			run: runUnmatched,
		},
		{
			name:  "expired message",
			count: expiredCounts(),
			order: [][]string{
				{"record delivery_received", "record dead_letter_decided", "start publish", "finish publish", "record dead_letter_published", "start settle", "finish settle"},
			},
			run: runExpired,
		},
		{
			name:  "drain with delivery in flight",
			count: drainCounts(),
			order: [][]string{
				{"record delivery_received", "start process", "start drain", "finish process", "start settle", "finish settle", "finish drain"},
			},
			run: runDrain,
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			recorder := f1test.NewRecorder()
			client := factory(t, recorder)
			scenario.run(t, client, recorder)
			calls := waitForCounts(t, recorder, scenario.count)
			assertObserverContract(t, calls, scenario.count)
			assertObserverOrder(t, calls, scenario.order)
		})
	}
}

func TestObserverContractLockProbe(t *testing.T) {
	probe := &lockProbeObserver{recorder: f1test.NewRecorder()}
	client := inmemObserverFactory(t, probe)
	probe.client = client.Client
	run := startObserverRunner(t, client, observerSubscription("orders", "orders.created.v1", 1, func(context.Context, *f1.Event) error {
		return nil
	}, f1.Ignore))
	client.Deliver(t, "orders.created.v1", map[string]string{"id": "lock-probe"})
	calls := waitForCounts(t, probe.recorder, successCounts())
	assertObserverContract(t, calls, successCounts())
	assertObserverOrder(t, calls, [][]string{
		{"start publish", "start message_built", "finish message_built", "finish publish"},
		{"record delivery_received", "start process", "finish process", "start settle", "finish settle"},
	})
	if failures := probe.failuresSnapshot(); len(failures) != 0 {
		t.Fatalf("Health lock probe failures = %v", failures)
	}
	run.cancel()
	<-run.done
}

func TestObserverPanicsDoNotChangeSettlement(t *testing.T) {
	for _, mode := range []string{"success", "retry", "dead letter"} {
		t.Run(mode, func(t *testing.T) {
			baselineRecorder := f1test.NewRecorder()
			baselinePublished, baselineDLQ := runSettlementScenario(t, baselineRecorder, mode)

			panicRecorder := f1test.NewRecorder()
			panicPublished, panicDLQ := runSettlementScenario(t, &panickingObserver{recorder: panicRecorder}, mode)

			if got, want := normalizeCaptures(panicPublished), normalizeCaptures(baselinePublished); !reflect.DeepEqual(got, want) {
				t.Fatalf("Published() with panicking observer = %#v, baseline = %#v", got, want)
			}
			if got, want := normalizeCaptures(panicDLQ), normalizeCaptures(baselineDLQ); !reflect.DeepEqual(got, want) {
				t.Fatalf("DLQ() with panicking observer = %#v, baseline = %#v", got, want)
			}
		})
	}
}

func normalizeCaptures(captures []f1test.Captured) []f1test.Captured {
	out := make([]f1test.Captured, len(captures))
	for i, capture := range captures {
		out[i] = capture
		out[i].Payload = append([]byte(nil), capture.Payload...)
		out[i].Headers = make(map[string]string, len(capture.Headers))
		for key, value := range capture.Headers {
			switch key {
			case "id", "f1idempotencykey", "f1correlationid":
				continue
			default:
				out[i].Headers[key] = value
			}
		}
	}
	return out
}

func inmemObserverFactory(t *testing.T, observer f1.Observer) *f1test.Client {
	t.Helper()
	return f1test.NewClient(t, f1.WithObserver(observer), f1.WithBacklogPollInterval(-1))
}

func observerSubscription(name, eventType string, maxAttempts int, handler func(context.Context, *f1.Event) error, unmatched f1.UnmatchedPolicy) f1.Subscription {
	return f1.Subscription{
		Name:            name,
		Topics:          []string{"orders.created"},
		Concurrency:     1,
		Prefetch:        4,
		Priorities:      []f1.Priority{f1.PriorityMedium},
		Retry:           f1.RetryConfig{MaxAttempts: maxAttempts, Tiers: []time.Duration{time.Second}},
		HandlerTimeout:  time.Second,
		UnmatchedPolicy: unmatched,
		Handlers:        map[string]f1.Handler{eventType: f1.HandlerFunc(handler)},
	}
}

type observerRunner struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	runner *f1.Runner
}

func startObserverRunner(t *testing.T, client *f1test.Client, subscription f1.Subscription) observerRunner {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	runner, err := client.Subscribe(ctx, subscription)
	if err != nil {
		cancel()
		t.Fatalf("Subscribe() error = %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runner.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return observerRunner{ctx: ctx, cancel: cancel, done: done, runner: runner}
}

func runSuccess(t *testing.T, client *f1test.Client, recorder *f1test.Recorder) {
	startObserverRunner(t, client, observerSubscription("orders", "orders.created.v1", 1, func(context.Context, *f1.Event) error {
		return nil
	}, f1.Ignore))
	client.Deliver(t, "orders.created.v1", map[string]string{"id": "success"})
	waitForCounts(t, recorder, successCounts())
}

func runRetry(t *testing.T, client *f1test.Client, recorder *f1test.Recorder) {
	attempts := 0
	startObserverRunner(t, client, observerSubscription("orders", "orders.created.v1", 2, func(context.Context, *f1.Event) error {
		attempts++
		if attempts == 1 {
			return errors.New("retry")
		}
		return nil
	}, f1.Ignore))
	client.Deliver(t, "orders.created.v1", map[string]string{"id": "retry"})
	waitForCounts(t, recorder, map[f1.ObserverKind]int{
		f1.ObserverDriverSelected:   1,
		f1.ObserverPublish:          4,
		f1.ObserverMessageBuilt:     2,
		f1.ObserverDeliveryReceived: 1,
		f1.ObserverProcess:          2,
		f1.ObserverRetryScheduled:   1,
		f1.ObserverSettle:           2,
	})
	client.Advance(time.Second)
	waitForCounts(t, recorder, retryCounts())
}

func runMaxAttempts(t *testing.T, client *f1test.Client, recorder *f1test.Recorder) {
	startObserverRunner(t, client, observerSubscription("orders", "orders.created.v1", 2, func(context.Context, *f1.Event) error {
		return errors.New("always retry")
	}, f1.Ignore))
	client.Deliver(t, "orders.created.v1", map[string]string{"id": "max-attempts"})
	waitForCounts(t, recorder, map[f1.ObserverKind]int{
		f1.ObserverDriverSelected:   1,
		f1.ObserverPublish:          4,
		f1.ObserverMessageBuilt:     2,
		f1.ObserverDeliveryReceived: 1,
		f1.ObserverProcess:          2,
		f1.ObserverRetryScheduled:   1,
		f1.ObserverSettle:           2,
	})
	client.Advance(time.Second)
	waitForCounts(t, recorder, maxAttemptsCounts())
}

func runHandlerPanic(t *testing.T, client *f1test.Client, recorder *f1test.Recorder) {
	startObserverRunner(t, client, observerSubscription("orders", "orders.created.v1", 1, func(context.Context, *f1.Event) error {
		panic("handler panic")
	}, f1.Ignore))
	client.Deliver(t, "orders.created.v1", map[string]string{"id": "panic"})
	waitForCounts(t, recorder, panicCounts())
}

func runUnmatched(t *testing.T, client *f1test.Client, recorder *f1test.Recorder) {
	startObserverRunner(t, client, observerSubscription("orders", "other.event", 1, func(context.Context, *f1.Event) error {
		return nil
	}, f1.DeadLetter))
	client.Deliver(t, "orders.created.v1", map[string]string{"id": "unmatched"})
	waitForCounts(t, recorder, unmatchedCounts())
}

func runExpired(t *testing.T, client *f1test.Client, recorder *f1test.Recorder) {
	startObserverRunner(t, client, observerSubscription("orders", "orders.created.v1", 1, func(context.Context, *f1.Event) error {
		return nil
	}, f1.Ignore))
	client.Deliver(t, "orders.created.v1", map[string]string{"id": "expired"}, f1.WithExpiry(time.Unix(-1, 0)))
	waitForCounts(t, recorder, expiredCounts())
}

func runDrain(t *testing.T, client *f1test.Client, recorder *f1test.Recorder) {
	release := make(chan struct{})
	run := startObserverRunner(t, client, observerSubscription("orders", "orders.created.v1", 1, func(context.Context, *f1.Event) error {
		<-release
		return nil
	}, f1.Ignore))
	client.Deliver(t, "orders.created.v1", map[string]string{"id": "drain"})
	waitForCounts(t, recorder, map[f1.ObserverKind]int{
		f1.ObserverDriverSelected:   1,
		f1.ObserverPublish:          2,
		f1.ObserverMessageBuilt:     2,
		f1.ObserverDeliveryReceived: 1,
		f1.ObserverProcess:          1,
	})
	drainDone := make(chan error, 1)
	go func() { drainDone <- run.runner.Drain(context.Background()) }()
	waitForCounts(t, recorder, map[f1.ObserverKind]int{
		f1.ObserverDriverSelected:   1,
		f1.ObserverPublish:          2,
		f1.ObserverMessageBuilt:     2,
		f1.ObserverDeliveryReceived: 1,
		f1.ObserverProcess:          1,
		f1.ObserverDrain:            1,
	})
	close(release)
	if err := <-drainDone; err != nil {
		t.Fatalf("Runner.Drain() error = %v", err)
	}
	<-run.done
	waitForCounts(t, recorder, drainCounts())
}

func runSettlementScenario(t *testing.T, observer f1.Observer, mode string) ([]f1test.Captured, []f1test.Captured) {
	t.Helper()
	client := inmemObserverFactory(t, observer)
	var counts map[f1.ObserverKind]int
	switch mode {
	case "success":
		startObserverRunner(t, client, observerSubscription("orders", "orders.created.v1", 1, func(context.Context, *f1.Event) error {
			return nil
		}, f1.Ignore))
		client.Deliver(t, "orders.created.v1", map[string]string{"id": mode})
		counts = successCounts()
	case "retry":
		attempts := 0
		startObserverRunner(t, client, observerSubscription("orders", "orders.created.v1", 2, func(context.Context, *f1.Event) error {
			attempts++
			if attempts == 1 {
				return errors.New("retry")
			}
			return nil
		}, f1.Ignore))
		client.Deliver(t, "orders.created.v1", map[string]string{"id": mode})
		partial := map[f1.ObserverKind]int{
			f1.ObserverDriverSelected:   1,
			f1.ObserverPublish:          4,
			f1.ObserverMessageBuilt:     2,
			f1.ObserverDeliveryReceived: 1,
			f1.ObserverProcess:          2,
			f1.ObserverRetryScheduled:   1,
			f1.ObserverSettle:           2,
		}
		waitForCounts(t, observerRecorder(observer), partial)
		client.Advance(time.Second)
		counts = retryCounts()
	case "dead letter":
		startObserverRunner(t, client, observerSubscription("orders", "orders.created.v1", 1, func(context.Context, *f1.Event) error {
			return errors.New("dead letter")
		}, f1.Ignore))
		client.Deliver(t, "orders.created.v1", map[string]string{"id": mode})
		counts = deadLetterCounts()
	default:
		t.Fatalf("unknown settlement scenario %q", mode)
	}
	waitForCounts(t, observerRecorder(observer), counts)
	return client.Published(), client.DLQ()
}

func observerRecorder(observer f1.Observer) *f1test.Recorder {
	switch observer := observer.(type) {
	case *f1test.Recorder:
		return observer
	case *panickingObserver:
		return observer.recorder
	default:
		panic("observer does not expose a recorder")
	}
}

type panickingObserver struct {
	recorder *f1test.Recorder
}

func (o *panickingObserver) Start(ctx context.Context, event f1.StartEvent) (context.Context, f1.Token) {
	o.recorder.Start(ctx, event)
	panic("observer start")
}

func (o *panickingObserver) Finish(token f1.Token, event f1.FinishEvent) {
	o.recorder.Finish(token, event)
	panic("observer finish")
}

func (o *panickingObserver) Record(event f1.PointEvent) {
	o.recorder.Record(event)
	panic("observer record")
}

type lockProbeObserver struct {
	recorder *f1test.Recorder
	client   *f1.Client
	mu       sync.Mutex
	failures []error
}

func (o *lockProbeObserver) Start(ctx context.Context, event f1.StartEvent) (context.Context, f1.Token) {
	gotCtx, token := o.recorder.Start(ctx, event)
	o.probeHealth()
	return gotCtx, token
}

func (o *lockProbeObserver) Finish(token f1.Token, event f1.FinishEvent) {
	o.recorder.Finish(token, event)
}

func (o *lockProbeObserver) Record(event f1.PointEvent) {
	o.recorder.Record(event)
	o.probeHealth()
}

func (o *lockProbeObserver) probeHealth() {
	if o.client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = o.client.Health(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		o.mu.Lock()
		o.failures = append(o.failures, ctx.Err())
		o.mu.Unlock()
	}
}

func (o *lockProbeObserver) failuresSnapshot() []error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]error(nil), o.failures...)
}

func waitForCounts(t *testing.T, recorder *f1test.Recorder, want map[f1.ObserverKind]int) []f1test.ObserverCall {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var got []f1test.ObserverCall
	if err := recorder.Wait(ctx, func(calls []f1test.ObserverCall) bool {
		if !hasAtLeastCounts(calls, want) {
			return false
		}
		got = calls
		return true
	}); err != nil {
		t.Fatalf("waiting for observer calls: %v; calls = %#v", err, recorder.Calls())
	}
	return got
}

func hasAtLeastCounts(calls []f1test.ObserverCall, want map[f1.ObserverKind]int) bool {
	got := countKinds(calls)
	for kind, count := range want {
		if got[kind] < count {
			return false
		}
	}
	return true
}

func assertObserverContract(t *testing.T, calls []f1test.ObserverCall, want map[f1.ObserverKind]int) {
	t.Helper()
	got := countKinds(calls)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("observer kind counts = %#v, want %#v; calls = %#v", got, want, calls)
	}
	started := make(map[f1.Token]bool)
	finished := make(map[f1.Token]bool)
	deliveryReceived := 0
	processStarted := 0
	processFinished := 0
	settleStarted := 0
	settleFinished := 0
	for _, call := range calls {
		switch call.Op {
		case f1test.ObserverOpStart:
			if started[call.Token] {
				t.Fatalf("token %#v started more than once", call.Token)
			}
			started[call.Token] = true
			switch call.Kind {
			case f1.ObserverProcess:
				if processStarted >= deliveryReceived {
					t.Fatalf("process start occurred before delivery_received: %#v", calls)
				}
				processStarted++
			case f1.ObserverSettle:
				if processStarted > 0 && settleStarted >= processFinished {
					t.Fatalf("settle start occurred before process finish: %#v", calls)
				}
				settleStarted++
			}
		case f1test.ObserverOpFinish:
			if !started[call.Token] {
				t.Fatalf("finish for token %#v occurred before its start", call.Token)
			}
			if finished[call.Token] {
				t.Fatalf("token %#v finished more than once", call.Token)
			}
			finished[call.Token] = true
			switch call.Kind {
			case f1.ObserverProcess:
				processFinished++
				if processFinished > processStarted {
					t.Fatalf("process finish occurred before process start: %#v", calls)
				}
			case f1.ObserverSettle:
				settleFinished++
				if settleFinished > settleStarted {
					t.Fatalf("settle finish occurred before settle start: %#v", calls)
				}
			}
		case f1test.ObserverOpRecord:
			if call.Kind == f1.ObserverDeliveryReceived {
				deliveryReceived++
			}
		}
	}
	for token := range started {
		if !finished[token] {
			t.Fatalf("start token %#v has no finish", token)
		}
	}
	for token := range finished {
		if !started[token] {
			t.Fatalf("finish token %#v has no start", token)
		}
	}
}

func assertObserverOrder(t *testing.T, calls []f1test.ObserverCall, sequences [][]string) {
	t.Helper()
	labels := make([]string, len(calls))
	for i, call := range calls {
		switch call.Op {
		case f1test.ObserverOpStart:
			labels[i] = "start " + string(call.Kind)
		case f1test.ObserverOpFinish:
			labels[i] = "finish " + string(call.Kind)
		case f1test.ObserverOpRecord:
			labels[i] = "record " + string(call.Kind)
		}
	}
	for _, sequence := range sequences {
		next := 0
		for _, label := range labels {
			if next < len(sequence) && label == sequence[next] {
				next++
			}
		}
		if next != len(sequence) {
			t.Fatalf("observer order missing %q in %v; calls = %#v", sequence[next:], labels, calls)
		}
	}
}

func countKinds(calls []f1test.ObserverCall) map[f1.ObserverKind]int {
	got := make(map[f1.ObserverKind]int)
	for _, call := range calls {
		got[call.Kind]++
	}
	return got
}

func successCounts() map[f1.ObserverKind]int {
	return map[f1.ObserverKind]int{
		f1.ObserverDriverSelected:   1,
		f1.ObserverPublish:          2,
		f1.ObserverMessageBuilt:     2,
		f1.ObserverDeliveryReceived: 1,
		f1.ObserverProcess:          2,
		f1.ObserverSettle:           2,
	}
}

func retryCounts() map[f1.ObserverKind]int {
	return map[f1.ObserverKind]int{
		f1.ObserverDriverSelected:   1,
		f1.ObserverPublish:          4,
		f1.ObserverMessageBuilt:     2,
		f1.ObserverDeliveryReceived: 2,
		f1.ObserverProcess:          4,
		f1.ObserverRetryScheduled:   1,
		f1.ObserverSettle:           4,
	}
}

func maxAttemptsCounts() map[f1.ObserverKind]int {
	return map[f1.ObserverKind]int{
		f1.ObserverDriverSelected:      1,
		f1.ObserverPublish:             6,
		f1.ObserverMessageBuilt:        2,
		f1.ObserverDeliveryReceived:    2,
		f1.ObserverProcess:             4,
		f1.ObserverRetryScheduled:      1,
		f1.ObserverDeadLetterDecided:   1,
		f1.ObserverDeadLetterPublished: 1,
		f1.ObserverSettle:              4,
	}
}

func panicCounts() map[f1.ObserverKind]int {
	return deadLetterCounts()
}

func deadLetterCounts() map[f1.ObserverKind]int {
	return map[f1.ObserverKind]int{
		f1.ObserverDriverSelected:      1,
		f1.ObserverPublish:             4,
		f1.ObserverMessageBuilt:        2,
		f1.ObserverDeliveryReceived:    1,
		f1.ObserverProcess:             2,
		f1.ObserverDeadLetterDecided:   1,
		f1.ObserverDeadLetterPublished: 1,
		f1.ObserverSettle:              2,
	}
}

func unmatchedCounts() map[f1.ObserverKind]int {
	return deadLetterCountsNoProcess()
}

func expiredCounts() map[f1.ObserverKind]int {
	return deadLetterCountsNoProcess()
}

func deadLetterCountsNoProcess() map[f1.ObserverKind]int {
	return map[f1.ObserverKind]int{
		f1.ObserverDriverSelected:      1,
		f1.ObserverPublish:             4,
		f1.ObserverMessageBuilt:        2,
		f1.ObserverDeliveryReceived:    1,
		f1.ObserverDeadLetterDecided:   1,
		f1.ObserverDeadLetterPublished: 1,
		f1.ObserverSettle:              2,
	}
}

func drainCounts() map[f1.ObserverKind]int {
	counts := successCounts()
	counts[f1.ObserverDrain] = 2
	return counts
}
