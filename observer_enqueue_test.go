package f1

import (
	"context"
	"errors"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/testhook"
)

func TestObserverInboundEnqueueFields(t *testing.T) {
	enqueuedAt := time.Unix(100, 0)
	fake := clock.NewFake(enqueuedAt)
	rec := &consumeRecordingObserver{}
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(testhook.Driver(fake)),
		WithObserver(rec),
		WithPublishTopics("orders.created"),
		WithBacklogPollInterval(-1),
		withClock(fake),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	ctx, _, runner, _ := startConsumeRunner(t, client, consumeTestSubscription("orders", func(context.Context, *Event) error {
		return nil
	}))
	waitOwnerReady(t, runner)

	runner.mu.Lock()
	consumer := runner.consumer
	runner.mu.Unlock()
	if consumer == nil {
		t.Fatal("runner consumer is nil")
	}
	if err := consumer.Pause(); err != nil {
		t.Fatal(err)
	}
	id := publishConsumeOne(t, client, ctx, map[string]string{"id": "enqueue"})
	fake.Advance(3 * time.Second)
	if err := consumer.Resume(); err != nil {
		t.Fatal(err)
	}
	waitConsumeCondition(t, "enqueue observer events did not arrive", func() bool {
		records, starts, _, _ := rec.snapshot()
		for _, record := range records {
			if record.Kind == ObserverDeliveryReceived {
				for _, start := range starts {
					if start.Kind == ObserverProcess && start.MessageID == id {
						return true
					}
				}
			}
		}
		return false
	})

	records, starts, _, _ := rec.snapshot()
	var received *PointEvent
	for i := range records {
		if records[i].Kind == ObserverDeliveryReceived {
			received = &records[i]
			break
		}
	}
	if received == nil {
		t.Fatal("no delivery_received event")
	}
	if !received.EnqueuedAt.Equal(enqueuedAt) {
		t.Fatalf("delivery EnqueuedAt = %v, want %v", received.EnqueuedAt, enqueuedAt)
	}
	if received.EnqueuedAtSource != EnqueuedAtBroker {
		t.Fatalf("delivery EnqueuedAtSource = %q, want broker", received.EnqueuedAtSource)
	}
	var process *StartEvent
	for i := range starts {
		if starts[i].Kind == ObserverProcess && starts[i].MessageID == id {
			process = &starts[i]
			break
		}
	}
	if process == nil {
		t.Fatal("no process start for published message")
	}
	if !process.EnqueuedAt.Equal(enqueuedAt) {
		t.Fatalf("process EnqueuedAt = %v, want %v", process.EnqueuedAt, enqueuedAt)
	}
	if process.EnqueuedAtSource != EnqueuedAtBroker {
		t.Fatalf("process EnqueuedAtSource = %q, want broker", process.EnqueuedAtSource)
	}
}

func TestObserverSuccessorPreservesEnqueueFields(t *testing.T) {
	enqueuedAt := time.Unix(200, 123)
	producer := &dispatchProducer{}
	rec := &successorRecordingObserver{}
	handlers := map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error {
			return errors.New("retry")
		}),
	}
	_, runner, _ := successorObserverRunner(t, producer, rec, handlers)

	envelope := successorTestEnvelope("enqueue-retry")
	message := retryBridgeMessage(t, envelope, &dispatchSettler{})
	message.EnqueuedAt = enqueuedAt
	message.EnqueuedAtSource = driver.EnqueueSourceBroker
	var abandoned bool
	var out Envelope
	if !dispatchMessage(runner, context.Background(), message, &out, &abandoned) {
		t.Fatal("dispatchMessage did not settle")
	}

	_, _, records, _ := rec.snapshot()
	var retry *PointEvent
	for i := range records {
		if records[i].Kind == ObserverRetryScheduled {
			retry = &records[i]
			break
		}
	}
	if retry == nil {
		t.Fatal("no retry_scheduled event")
	}
	if !retry.EnqueuedAt.Equal(enqueuedAt) {
		t.Fatalf("retry EnqueuedAt = %v, want %v", retry.EnqueuedAt, enqueuedAt)
	}
	if retry.EnqueuedAtSource != EnqueuedAtBroker {
		t.Fatalf("retry EnqueuedAtSource = %q, want broker", retry.EnqueuedAtSource)
	}
}

func TestObserverDeadLetterPreservesEnqueueFields(t *testing.T) {
	enqueuedAt := time.Unix(400, 0)
	fake := clock.NewFake(enqueuedAt)
	rec := &consumeRecordingObserver{}
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(testhook.Driver(fake)),
		WithObserver(rec),
		WithPublishTopics("orders.created"),
		WithBacklogPollInterval(-1),
		withClock(fake),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	ctx, _, runner, _ := startConsumeRunner(t, client, consumeTestSubscription("orders", func(context.Context, *Event) error {
		return Terminal(errors.New("terminal"))
	}))
	waitOwnerReady(t, runner)
	runner.mu.Lock()
	consumer := runner.consumer
	runner.mu.Unlock()
	if consumer == nil {
		t.Fatal("runner consumer is nil")
	}
	if err := consumer.Pause(); err != nil {
		t.Fatal(err)
	}
	id := publishConsumeOne(t, client, ctx, map[string]string{"id": "dead-letter"})
	fake.Advance(3 * time.Second)
	if err := consumer.Resume(); err != nil {
		t.Fatal(err)
	}
	waitConsumeCondition(t, "dead-letter observer events did not arrive", func() bool {
		records, _, _, _ := rec.snapshot()
		var decided, published bool
		for _, record := range records {
			if record.MessageID != id {
				continue
			}
			switch record.Kind {
			case ObserverDeadLetterDecided:
				decided = true
			case ObserverDeadLetterPublished:
				published = true
			}
		}
		return decided && published
	})

	records, _, _, _ := rec.snapshot()
	var decided, published *PointEvent
	for i := range records {
		if records[i].MessageID != id {
			continue
		}
		switch records[i].Kind {
		case ObserverDeadLetterDecided:
			decided = &records[i]
		case ObserverDeadLetterPublished:
			published = &records[i]
		}
	}
	if decided == nil || published == nil {
		t.Fatalf("dead-letter events = decided %v, published %v", decided != nil, published != nil)
	}
	for name, event := range map[string]*PointEvent{
		"dead_letter_decided":   decided,
		"dead_letter_published": published,
	} {
		if !event.EnqueuedAt.Equal(enqueuedAt) {
			t.Errorf("%s EnqueuedAt = %v, want %v", name, event.EnqueuedAt, enqueuedAt)
		}
		if event.EnqueuedAtSource != EnqueuedAtBroker {
			t.Errorf("%s EnqueuedAtSource = %q, want broker", name, event.EnqueuedAtSource)
		}
	}
}

func TestObserverEnqueueFieldsMapZeroAndSources(t *testing.T) {
	stamp := time.Unix(300, 0)
	for _, test := range []struct {
		name       string
		enqueuedAt time.Time
		source     driver.EnqueueSource
		wantAt     time.Time
		wantSource EnqueuedAtSource
	}{
		{name: "zero producer", source: driver.EnqueueSourceProducer, wantSource: EnqueuedAtUnknown},
		{name: "unknown", enqueuedAt: stamp, wantAt: stamp, wantSource: EnqueuedAtUnknown},
		{name: "producer", enqueuedAt: stamp, source: driver.EnqueueSourceProducer, wantAt: stamp, wantSource: EnqueuedAtProducer},
		{name: "broker", enqueuedAt: stamp, source: driver.EnqueueSourceBroker, wantAt: stamp, wantSource: EnqueuedAtBroker},
		{name: "out of range", enqueuedAt: stamp, source: driver.EnqueueSource(99), wantAt: stamp, wantSource: EnqueuedAtUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			rec := &consumeRecordingObserver{}
			_, runner, _ := successorObserverRunner(t, &dispatchProducer{}, rec, nil)
			runner.inflight = newInflightRegistry()
			dispatch := make(chan delivery, 1)
			message := driver.InboundMessage{
				EnqueuedAt:       test.enqueuedAt,
				EnqueuedAtSource: test.source,
			}
			if !enqueueDelivery(runner, context.Background(), dispatch, message) {
				t.Fatal("enqueueDelivery returned false")
			}
			item := <-dispatch
			runner.inflight.Remove(item.id)
			records, _, _, _ := rec.snapshot()

			var received *PointEvent
			for i := range records {
				if records[i].Kind == ObserverDeliveryReceived {
					received = &records[i]
					break
				}
			}
			if received == nil {
				t.Fatal("no delivery_received event")
			}
			if !received.EnqueuedAt.Equal(test.wantAt) {
				t.Fatalf("EnqueuedAt = %v, want %v", received.EnqueuedAt, test.wantAt)
			}
			if received.EnqueuedAtSource != test.wantSource {
				t.Fatalf("EnqueuedAtSource = %q, want %q", received.EnqueuedAtSource, test.wantSource)
			}
		})
	}
}
