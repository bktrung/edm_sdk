package f1

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// panicTimerClock panics on one selected timer construction while preserving
// the fake clock for every other operation.
type panicTimerClock struct {
	*clock.Fake
	panicNext atomic.Bool
}

func (c *panicTimerClock) Timer(d time.Duration) clock.Timer {
	if c.panicNext.CompareAndSwap(true, false) {
		panic("worker error path test timer panic")
	}
	return c.Fake.Timer(d)
}

type recordingRequeueSettler struct {
	*dispatchSettler
	requeue atomic.Bool
}

func (s *recordingRequeueSettler) Nack(ctx context.Context, options driver.NackOptions) error {
	s.requeue.Store(options.Requeue)
	return s.dispatchSettler.Nack(ctx, options)
}

// TestProcessDeliveryRecoversDispatchPanic proves that a panic in the
// delivery path is dead-lettered and the original delivery is acknowledged.
func TestProcessDeliveryRecoversDispatchPanic(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	panicClock := &panicTimerClock{Fake: fake}
	observer := &consumeRecordingObserver{}
	producer := &dispatchProducer{}
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&dispatchDriver{conn: &dispatchConn{producer: producer}}),
		WithObserver(observer),
		withClock(panicClock),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	release := make(chan struct{})
	handlerDone := make(chan struct{})
	releaseHandler := func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}
	t.Cleanup(releaseHandler)
	runner := &Runner{
		client: client,
		subscription: Subscription{
			Name:           "orders",
			HandlerTimeout: time.Second,
			Handlers: map[string]Handler{
				"orders.created": HandlerFunc(func(context.Context, *Event) error {
					<-release
					close(handlerDone)
					return nil
				}),
			},
		},
		inflight: newInflightRegistry(),
	}
	envelope := Envelope{SpecVersion: "1.0", ID: "panic-dispatch", Source: "/test/orders", Type: "orders.created", Attempt: 1}
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	settler := &dispatchSettler{}
	message := driver.InboundMessage{
		Destination: "f1.test.orders.created.medium",
		Headers:     headerSlice(headers),
		Body:        []byte(`{}`),
		Settle:      settler,
	}
	id := runner.inflight.Add()
	panicClock.panicNext.Store(true)
	processDelivery(runner, context.Background(), delivery{id: id, message: message})
	releaseHandler()
	select {
	case <-handlerDone:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("handler did not exit after the recovered delivery panic")
	}

	producer.mu.Lock()
	messages := append([]driver.OutboundMessage(nil), producer.messages...)
	producer.mu.Unlock()
	if len(messages) != 1 {
		t.Fatalf("successor messages = %d, want one dead-letter message", len(messages))
	}
	if got, want := messages[0].Destination, "f1.test.orders.created.dlq.orders"; got != want {
		t.Fatalf("dead-letter destination = %q, want %q", got, want)
	}
	if got, want := headerValue(messages[0].Headers, "f1deathreason"), ReasonPanic.String(); got != want {
		t.Fatalf("dead-letter reason = %q, want %q", got, want)
	}
	settler.mu.Lock()
	acked, nacked := settler.acked, settler.nacked
	settler.mu.Unlock()
	if !acked || nacked {
		t.Fatalf("settlement = acked %t nacked %t, want acked true and nacked false", acked, nacked)
	}
	records, _, _, _ := observer.snapshot()
	found := false
	for _, record := range records {
		if record.Kind != ObserverDeadLetterPublished {
			continue
		}
		found = true
		if record.Reason != ReasonPanic {
			t.Fatalf("dead-letter observer reason = %q, want panic", record.Reason)
		}
	}
	if !found {
		t.Fatal("dead-letter observer event was not recorded")
	}
}

// TestProcessDeliveryKeepsPhaseTwoDrainUntilStuck proves that a draining
// runner ignores parent cancellation during the second stuck phase, then
// requeues the abandoned delivery at the cumulative threshold.
func TestProcessDeliveryKeepsPhaseTwoDrainUntilStuck(t *testing.T) {
	const timeout = 10 * time.Millisecond
	fake := clock.NewFake(time.Unix(0, 0))
	producer := &dispatchProducer{}
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&dispatchDriver{conn: &dispatchConn{producer: producer}}),
		withClock(fake),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	started := make(chan struct{})
	release := make(chan struct{})
	handlerDone := make(chan struct{})
	releaseHandler := func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}
	t.Cleanup(releaseHandler)
	settled := make(chan struct{})
	settler := &recordingRequeueSettler{dispatchSettler: &dispatchSettler{onSettle: func() { close(settled) }}}
	runner := &Runner{
		client: client,
		subscription: Subscription{
			Name:           "orders",
			HandlerTimeout: timeout,
			Handlers: map[string]Handler{
				"orders.created": HandlerFunc(func(context.Context, *Event) error {
					close(started)
					<-release
					close(handlerDone)
					return nil
				}),
			},
		},
		settleCtx: context.Background(),
		inflight:  newInflightRegistry(),
	}
	envelope := Envelope{SpecVersion: "1.0", ID: "stuck-drain", Source: "/test/orders", Type: "orders.created", Attempt: 1}
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	message := driver.InboundMessage{
		Destination: "f1.test.orders.created.medium",
		Headers:     headerSlice(headers),
		Body:        []byte(`{}`),
		Settle:      settler,
	}
	id := runner.inflight.Add()
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		processDelivery(runner, parent, delivery{id: id, message: message})
		close(done)
	}()

	select {
	case <-started:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("handler did not start")
	}
	fake.BlockUntil(1)
	fake.Advance(2 * timeout)
	fake.BlockUntil(1)

	runner.mu.Lock()
	runner.draining = true
	runner.mu.Unlock()
	cancel()
	// A runner that obeyed the cancellation would settle as soon as it saw it,
	// so a short real window is enough to catch it.
	guard := clock.NewReal().Timer(100 * time.Millisecond)
	select {
	case <-settled:
		guard.Stop()
		t.Fatal("delivery settled before the second stuck phase threshold")
	case <-guard.C:
	}

	fake.Advance(2 * timeout)
	select {
	case <-done:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("stuck delivery did not finish after the second phase")
	}
	releaseHandler()
	select {
	case <-handlerDone:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("stuck handler did not exit after release")
	}

	settler.mu.Lock()
	acked, nacked := settler.acked, settler.nacked
	settler.mu.Unlock()
	if acked || !nacked {
		t.Fatalf("settlement = acked %t nacked %t, want acked false and nacked true", acked, nacked)
	}
	if !settler.requeue.Load() {
		t.Fatal("abandoned delivery nack did not request requeue")
	}
	producer.mu.Lock()
	messageCount := len(producer.messages)
	producer.mu.Unlock()
	if messageCount != 0 {
		t.Fatalf("successor messages = %d, want none for an abandoned delivery", messageCount)
	}
}
