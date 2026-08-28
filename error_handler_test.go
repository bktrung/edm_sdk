package f1

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// errorHandlerRecorder captures every WithErrorHandler invocation for
// assertion, in call order, safe for concurrent use. Since the handler runs
// on its own goroutine (runnerNotifyError never waits on it), tests observe
// a call through notified rather than by reading count immediately after
// triggering it.
type errorHandlerRecorder struct {
	mu    sync.Mutex
	calls []struct {
		event *Event
		err   error
	}
	release  <-chan struct{}
	notified chan struct{}
}

func newErrorHandlerRecorder() *errorHandlerRecorder {
	return &errorHandlerRecorder{notified: make(chan struct{}, 8)}
}

func (r *errorHandlerRecorder) handle(_ context.Context, event *Event, err error) {
	if r.release != nil {
		<-r.release
	}
	r.mu.Lock()
	r.calls = append(r.calls, struct {
		event *Event
		err   error
	}{event, err})
	r.mu.Unlock()
	select {
	case r.notified <- struct{}{}:
	default:
	}
}

func (r *errorHandlerRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

// waitForCall blocks until the handler has recorded at least one call, or
// fails the test after timeout.
func (r *errorHandlerRecorder) waitForCall(t *testing.T, timeout time.Duration) {
	t.Helper()
	if r.count() > 0 {
		return
	}
	timer := clock.NewReal().Timer(timeout)
	defer timer.Stop()
	select {
	case <-r.notified:
	case <-timer.C:
		t.Fatal("error handler was not invoked in time")
	}
}

func newErrorHandlerRunner(t *testing.T, producer driver.Producer, consumer *dispatchConsumer, handler func(context.Context, *Event, error)) (*Client, *Runner) {
	t.Helper()
	options := []Option{WithDriver(&dispatchDriver{conn: &dispatchConn{producer: dispatchProducerOrNil(producer), consumer: consumer, admin: &dispatchAdmin{}}})}
	if handler != nil {
		options = append(options, WithErrorHandler(handler))
	}
	client, err := New(context.Background(), testClientConfig(t), options...)
	if err != nil {
		t.Fatal(err)
	}
	runner := &Runner{
		client: client,
		subscription: Subscription{
			Name:       "orders",
			Topics:     []string{"orders.created"},
			Priorities: []Priority{PriorityHigh},
			Retry:      RetryConfig{MaxAttempts: 3, Tiers: []time.Duration{time.Second}},
		},
		consumer:   consumer,
		asyncGroup: new(errgroup.Group),
	}
	return client, runner
}

// dispatchProducerOrNil lets tests that only exercise the consumer-error
// path pass a nil producer without New rejecting it: dispatchConn.Producer
// is only invoked once a publish is attempted.
func dispatchProducerOrNil(producer driver.Producer) *dispatchProducer {
	if producer == nil {
		return &dispatchProducer{}
	}
	if p, ok := producer.(*dispatchProducer); ok {
		return p
	}
	return &dispatchProducer{}
}

// TestErrorHandlerReceivesDriverError proves a driver-level error read from
// Consumer.Errors() reaches WithErrorHandler, with a nil Event since the
// error belongs to no single message.
func TestErrorHandlerReceivesDriverError(t *testing.T) {
	t.Parallel()
	recorder := newErrorHandlerRecorder()
	consumer := newDispatchConsumer()
	client, runner := newErrorHandlerRunner(t, nil, consumer, recorder.handle)
	defer func() { _ = client.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	runner.cancel = cancel
	driverErr := errors.New("connection lost")
	consumer.errs <- driverErr

	if err := consumeRunnerErrors(runner, ctx); err != nil {
		t.Fatalf("consumeRunnerErrors() error = %v", err)
	}
	recorder.waitForCall(t, time.Second)
	if got := recorder.count(); got != 1 {
		t.Fatalf("error handler calls = %d, want 1", got)
	}
	recorder.mu.Lock()
	call := recorder.calls[0]
	recorder.mu.Unlock()
	if call.event != nil {
		t.Fatalf("event = %+v, want nil for a connection-level error", call.event)
	}
	if !errors.Is(call.err, driverErr) {
		t.Fatalf("error = %v, want it to wrap %v", call.err, driverErr)
	}
}

// TestErrorHandlerReceivesSuccessorHandoffFailure proves a terminal retry or
// dead-letter hand-off failure reaches WithErrorHandler with a non-nil
// Event identifying the delivery whose successor could not be published.
func TestErrorHandlerReceivesSuccessorHandoffFailure(t *testing.T) {
	t.Parallel()
	recorder := newErrorHandlerRecorder()
	consumer := newDispatchConsumer()
	client, runner := newErrorHandlerRunner(t, nil, consumer, recorder.handle)
	defer func() { _ = client.Close(context.Background()) }()
	client.producerHandle = &retryBridgeFailingProducer{}

	settler := &retryBridgeSettler{}
	envelope := Envelope{
		SpecVersion:     "1.0",
		ID:              "handoff-failure",
		Source:          "/test/orders",
		Type:            "orders.created.v1",
		DataContentType: "application/protobuf",
		Priority:        PriorityHigh,
		Attempt:         1,
	}
	message := retryBridgeMessage(t, envelope, settler)
	if retryAndSettle(runner, context.Background(), message, envelope, errors.New("temporary")) {
		t.Fatal("retry successor hand-off was reported as successful")
	}
	recorder.waitForCall(t, time.Second)
	if got := recorder.count(); got != 1 {
		t.Fatalf("error handler calls = %d, want 1", got)
	}
	recorder.mu.Lock()
	call := recorder.calls[0]
	recorder.mu.Unlock()
	if call.event == nil {
		t.Fatal("event = nil, want the failed delivery's Event")
	}
	if got := call.event.ID(); got != envelope.ID {
		t.Fatalf("event ID = %q, want %q", got, envelope.ID)
	}
	if err := call.event.Decode(&map[string]any{}); err == nil || err.Error() != "f1: event codec is unavailable" {
		t.Fatalf("event.Decode() error = %v, want unavailable-codec error", err)
	}
	if _, ok := errors.AsType[*driver.Error](call.err); !ok {
		t.Fatalf("error = %v, want a classified *driver.Error", call.err)
	}
}

// TestErrorHandlerNotInvokedForHandlerError proves an error returned by a
// subscription's own handler function never reaches WithErrorHandler: it
// already flows through the retry ladder, and reporting it here too would
// double-report the same failure.
func TestErrorHandlerNotInvokedForHandlerError(t *testing.T) {
	t.Parallel()
	recorder := newErrorHandlerRecorder()
	producer := &dispatchProducer{}
	consumer := newDispatchConsumer()
	client, runner := newErrorHandlerRunner(t, producer, consumer, recorder.handle)
	defer func() { _ = client.Close(context.Background()) }()
	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error {
			return errors.New("handler failed")
		}),
	}
	runner.subscription.HandlerTimeout = time.Second

	settler := &dispatchSettler{}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "handler-error",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	message := retryBridgeMessage(t, envelope, settler)
	if !dispatchMessage(runner, context.Background(), message, &Envelope{}, new(bool)) {
		t.Fatal("delivery with a failing handler was not settled via the retry ladder")
	}
	if len(producer.messages) != 1 {
		t.Fatalf("retry copies = %d, want 1: the handler error should have produced a retry successor", len(producer.messages))
	}
	// The correct code path never spawns a notification goroutine for a
	// handler error, so there is nothing to await; this grace window only
	// guards against a wrongly introduced async call racing past the check.
	grace := clock.NewReal().Timer(50 * time.Millisecond)
	defer grace.Stop()
	select {
	case <-recorder.notified:
		t.Fatal("error handler was invoked for a handler error")
	case <-grace.C:
	}
	if got := recorder.count(); got != 0 {
		t.Fatalf("error handler calls = %d, want 0: a handler error must not reach WithErrorHandler", got)
	}
}

// TestErrorHandlerDoesNotBlockRunner proves a slow WithErrorHandler callback
// cannot stall the runner: consumeRunnerErrors must return as soon as it has
// dispatched the notification, not wait for the handler to finish.
func TestErrorHandlerDoesNotBlockRunner(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	recorder := newErrorHandlerRecorder()
	recorder.release = release
	consumer := newDispatchConsumer()
	client, runner := newErrorHandlerRunner(t, nil, consumer, recorder.handle)
	defer func() { _ = client.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	runner.cancel = cancel
	consumer.errs <- errors.New("connection lost")

	returned := make(chan error, 1)
	go func() { returned <- consumeRunnerErrors(runner, ctx) }()

	returnDeadline := clock.NewReal().Timer(time.Second)
	defer returnDeadline.Stop()
	select {
	case err := <-returned:
		if err != nil {
			t.Fatalf("consumeRunnerErrors() error = %v", err)
		}
	case <-returnDeadline.C:
		t.Fatal("consumeRunnerErrors did not return while the error handler was still blocked")
	}
	if got := recorder.count(); got != 0 {
		t.Fatalf("error handler calls = %d before release, want 0", got)
	}

	close(release)
	recorder.waitForCall(t, 2*time.Second)
}

func TestErrorHandlerNotificationsAreBoundedAndDroppable(t *testing.T) {
	defaultOutput := captureProcessDefault(t)
	started := make(chan struct{}, 4)
	finished := make(chan struct{}, 4)
	release := make(chan struct{})
	var releaseOnce sync.Once
	var active atomic.Int32
	client, runner := newLoggerHandoffRunner(t, nil, func(context.Context, *Event, error) {
		active.Add(1)
		started <- struct{}{}
		<-release
		active.Add(-1)
		finished <- struct{}{}
	})
	defer func() {
		releaseOnce.Do(func() { close(release) })
		_ = client.Close(context.Background())
	}()
	runner.subscription.Concurrency = 2

	runnerNotifyError(runner, context.Background(), nil, errors.New("first"))
	runnerNotifyError(runner, context.Background(), nil, errors.New("second"))
	for range 2 {
		timer := clock.NewReal().Timer(time.Second)
		select {
		case <-started:
		case <-timer.C:
			t.Fatal("error handler did not fill its concurrency bound")
		}
		timer.Stop()
	}
	runnerNotifyError(runner, context.Background(), nil, errors.New("dropped"))
	timer := clock.NewReal().Timer(50 * time.Millisecond)
	select {
	case <-started:
		t.Fatal("error handler exceeded its concurrency bound")
	case <-timer.C:
	}
	timer.Stop()
	if got := active.Load(); got != 2 {
		t.Fatalf("active error handlers = %d, want 2", got)
	}
	if got := defaultOutput.String(); !strings.Contains(got, "error handler notification dropped") || !strings.Contains(got, "dropped") {
		t.Fatalf("process default output = %q, want dropped notification", got)
	}

	releaseOnce.Do(func() { close(release) })
	for range 2 {
		timer := clock.NewReal().Timer(time.Second)
		select {
		case <-finished:
		case <-timer.C:
			t.Fatal("error handler did not finish after release")
		}
		timer.Stop()
	}
}
