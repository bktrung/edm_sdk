package f1

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestObserverSettleCarriesTopicAndPriority(t *testing.T) {
	rec := &consumeRecordingObserver{}
	client := newConsumeObserverClient(t, rec)
	sub := consumeTestSubscription("orders", func(context.Context, *Event) error {
		return nil
	})
	sub.Priorities = []Priority{PriorityHigh}
	ctx, _, _, _ := startConsumeRunner(t, client, sub)
	publishConsumeOne(t, client, ctx, "payload", WithPriority(PriorityHigh))
	waitConsumeCondition(t, "settle event did not arrive", func() bool {
		starts, finishes := rec.settlePairs()
		return len(starts) == 1 && len(finishes) == 1
	})

	starts, finishes := rec.settlePairs()
	if starts[0].Priority != PriorityHigh {
		t.Fatalf("settle start priority = %v, want high", starts[0].Priority)
	}
	if starts[0].Topic != "orders.created" {
		t.Fatalf("settle start topic = %q, want orders.created", starts[0].Topic)
	}
	if finishes[0].Priority != PriorityHigh {
		t.Fatalf("settle finish priority = %v, want high", finishes[0].Priority)
	}
	if finishes[0].Topic != "orders.created" {
		t.Fatalf("settle finish topic = %q, want orders.created", finishes[0].Topic)
	}
}

func TestObserverPrimaryPublishFinishCarriesTopicAndPriority(t *testing.T) {
	t.Run("single topic", func(t *testing.T) {
		rec := &publishRecordingObserver{}
		client := newPublishClient(t, &recordingProducer{}, WithObserver(rec))
		_, err := client.Publisher().PublishBatch(context.Background(), []Message{{
			EventType: "orders.created",
			Payload:   "payload",
			Opts:      []PublishOption{WithPriority(PriorityHigh)},
		}})
		if err != nil {
			t.Fatal(err)
		}

		_, finishes, _ := rec.snapshot()
		for _, finish := range finishes {
			if finish.Kind != ObserverPublish {
				continue
			}
			if finish.Topic != "orders.created" {
				t.Fatalf("publish finish topic = %q, want orders.created", finish.Topic)
			}
			if finish.Priority != PriorityHigh {
				t.Fatalf("publish finish priority = %v, want high", finish.Priority)
			}
			return
		}
		t.Fatal("no publish finish event")
	})

	t.Run("mixed topics", func(t *testing.T) {
		rec := &publishRecordingObserver{}
		client := newPublishClient(t, &recordingProducer{}, WithObserver(rec))
		_, err := client.Publisher().PublishBatch(context.Background(), []Message{
			{EventType: "orders.created", Payload: "one", Opts: []PublishOption{WithPriority(PriorityHigh)}},
			{EventType: "payments.created", Payload: "two", Opts: []PublishOption{WithPriority(PriorityHigh)}},
		})
		if err != nil {
			t.Fatal(err)
		}

		_, finishes, _ := rec.snapshot()
		for _, finish := range finishes {
			if finish.Kind != ObserverPublish {
				continue
			}
			if finish.Topic != "" || finish.Priority != 0 {
				t.Fatalf("mixed publish finish = topic %q priority %v, want zero fields", finish.Topic, finish.Priority)
			}
			return
		}
		t.Fatal("no publish finish event")
	})
}

// TestObserverDeliveryReceivedCarriesLogicalTopicAndPriority checks the first
// delivery and its retry: the retry arrives on a retry destination and must
// still report the logical topic.
func TestObserverDeliveryReceivedCarriesLogicalTopicAndPriority(t *testing.T) {
	rec := &consumeRecordingObserver{}
	client := newConsumeObserverClient(t, rec)
	var attempts atomic.Int32
	sub := consumeTestSubscription("orders", func(context.Context, *Event) error {
		if attempts.Add(1) == 1 {
			return errors.New("retry once")
		}
		return nil
	})
	sub.Priorities = []Priority{PriorityHigh}
	sub.Retry = RetryConfig{MaxAttempts: 2, Tiers: []time.Duration{time.Millisecond}}
	ctx, _, _, _ := startConsumeRunner(t, client, sub)
	publishConsumeOne(t, client, ctx, "payload", WithPriority(PriorityHigh))
	waitConsumeCondition(t, "retry delivery received events did not arrive", func() bool {
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
	var received []PointEvent
	for _, record := range records {
		if record.Kind == ObserverDeliveryReceived {
			received = append(received, record)
		}
	}
	if len(received) < 2 {
		t.Fatalf("delivery received count = %d, want at least 2", len(received))
	}
	for i, delivery := range received[:2] {
		if delivery.Topic != "orders.created" {
			t.Fatalf("delivery %d topic = %q, want orders.created", i, delivery.Topic)
		}
		if delivery.Priority != PriorityHigh {
			t.Fatalf("delivery %d priority = %v, want high", i, delivery.Priority)
		}
	}
}
