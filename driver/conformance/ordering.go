package conformance

import (
	"fmt"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func init() { registerGroup("ordering", runOrdering) }

func runOrdering(group *groupContext) {
	group.Check("initial same-key prefetch retains receipt order", func(t *testing.T) {
		const destination = "ordering.initial"
		producer := newProducer(t, group, profileDestination(group, destination), driver.ProducerConfig{Effective: group.effective})
		consumer := orderingConsumer(t, group, destination, 3, false)
		publishKeyed(t, group, producer, destination, "k", "one", "two", "three")
		waitForSaturation(t, group, destination, 3, 0)
		assertOrderedBodies(t, group, consumer, "one", "two", "three")
		addOrderingEvent(group, "ordering-initial", "one,two,three", destination)
	})

	group.Check("same-key receipt order survives irregular prefetch refills", func(t *testing.T) {
		const destination = "ordering.refill"
		producer := newProducer(t, group, profileDestination(group, destination), driver.ProducerConfig{Effective: group.effective})
		consumer := orderingConsumer(t, group, destination, 3, false)
		publishKeyed(t, group, producer, destination, "k", "one", "two", "three", "four", "five", "six")
		waitForSaturation(t, group, destination, 3, 3)
		first := receiveMessages(t, group, consumer, 3)
		settleMessage(t, group, first[2])
		settleMessage(t, group, first[0])
		settleMessage(t, group, first[1])
		remaining := receiveMessages(t, group, consumer, 3)
		assertBodies(t, remaining, "four", "five", "six")
		for _, message := range remaining {
			settleMessage(t, group, message)
		}
		addOrderingEvent(group, "ordering-refill", "one,two,three,four,five,six", destination)
	})

	group.Check("independent keys retain independent receipt sequences", func(t *testing.T) {
		const destination = "ordering.keys"
		producer := newProducer(t, group, profileDestination(group, destination), driver.ProducerConfig{Effective: group.effective})
		consumer := orderingConsumer(t, group, destination, 4, false)
		publishKeyed(t, group, producer, destination, "a", "a1", "a2")
		publishKeyed(t, group, producer, destination, "b", "b1", "b2")
		messages := receiveMessages(t, group, consumer, 4)
		sequences := map[string][]string{}
		for _, message := range messages {
			sequences[string(message.Key)] = append(sequences[string(message.Key)], string(message.Body))
			settleMessage(t, group, message)
		}
		if fmt.Sprint(sequences["a"]) != "[a1 a2]" || fmt.Sprint(sequences["b"]) != "[b1 b2]" {
			t.Fatalf("per-key sequences=%v", sequences)
		}
		addOrderingEvent(group, "ordering-keys", "per-key", destination)
	})

	group.Check("distinct keys progress while another key is unsettled", func(t *testing.T) {
		const destination = "ordering.progress"
		producer := newProducer(t, group, profileDestination(group, destination), driver.ProducerConfig{Effective: group.effective})
		consumer := orderingConsumer(t, group, destination, 2, false)
		publishKeyed(t, group, producer, destination, "held", "held")
		held := receiveMessages(t, group, consumer, 1)[0]
		publishKeyed(t, group, producer, destination, "free", "free")
		free := receiveMessages(t, group, consumer, 1)[0]
		if string(free.Key) != "free" || string(free.Body) != "free" {
			t.Fatalf("distinct key did not progress: key=%q body=%q", free.Key, free.Body)
		}
		settleMessage(t, group, free)
		settleMessage(t, group, held)
		addOrderingEvent(group, "ordering-progress", "independent", destination)
	})

	group.Check("requeued key returns before an undelivered successor", func(t *testing.T) {
		const destination = "ordering.requeue"
		producer := newProducer(t, group, profileDestination(group, destination), driver.ProducerConfig{Effective: group.effective})
		consumer := orderingConsumer(t, group, destination, 1, false)
		publishKeyed(t, group, producer, destination, "k", "one", "two")
		first := receiveMessages(t, group, consumer, 1)[0]
		if err := first.Settle.Nack(group.ctx, driver.NackOptions{Requeue: true}); err != nil {
			t.Fatal(err)
		}
		redelivered := receiveMessages(t, group, consumer, 1)[0]
		if string(redelivered.Body) != "one" {
			t.Fatalf("redelivered body=%q, want one", redelivered.Body)
		}
		settleMessage(t, group, redelivered)
		next := receiveMessages(t, group, consumer, 1)[0]
		if string(next.Body) != "two" {
			t.Fatalf("successor body=%q, want two", next.Body)
		}
		settleMessage(t, group, next)
		addOrderingEvent(group, "ordering-requeue", "one,two", destination)
	})

	group.Check("nil-key messages deliver exactly once", func(t *testing.T) {
		const destination = "ordering.nil"
		producer := newProducer(t, group, profileDestination(group, destination), driver.ProducerConfig{Effective: group.effective})
		consumer := orderingConsumer(t, group, destination, 1, false)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: destination, Body: []byte("nil")}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessages(t, group, consumer, 1)[0]
		if message.Key != nil || string(message.Body) != "nil" {
			t.Fatalf("nil-key message=%+v", message)
		}
		settleMessage(t, group, message)
		waitForStable(t, group, "nil-key message to remain delivered once", func() (bool, string) {
			view := inspectDestination(t, group, destination)
			return view.Ready == 0 && view.Unsettled == 0, fmt.Sprintf("view=%+v", view)
		})
		addOrderingEvent(group, "ordering-nil", "delivered", destination)
	})

	group.Check("exclusive ordering request creates a usable consumer", func(t *testing.T) {
		const destination = "ordering.exclusive"
		producer := newProducer(t, group, profileDestination(group, destination), driver.ProducerConfig{Effective: group.effective})
		consumer := orderingConsumer(t, group, destination, 2, true)
		publishKeyed(t, group, producer, destination, "k", "one", "two")
		assertOrderedBodies(t, group, consumer, "one", "two")
		addOrderingEvent(group, "ordering-exclusive", "usable", destination)
	})

	group.Check("zero-value producer configuration preserves ordered receipt", func(t *testing.T) {
		const destination = "ordering.zero"
		producer := newProducer(t, group, profileDestination(group, destination), driver.ProducerConfig{
			RequireDurableAck: true,
			Effective:         group.effective,
		})
		consumer := orderingConsumer(t, group, destination, 1, true)
		publishKeyed(t, group, producer, destination, "k", "one", "two")
		assertOrderedBodiesOneAtATime(t, group, consumer, "one", "two")
		addOrderingEvent(group, "ordering-zero", "one,two", destination)
	})
}

func orderingConsumer(t *testing.T, group *groupContext, destination string, prefetch int, exclusive bool) driver.Consumer {
	t.Helper()
	scopedDestination := profileDestination(group, destination)
	if _, err := group.conn.Admin().EnsureTopology(group.ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: scopedDestination}}, Effective: group.effective,
	}); err != nil {
		t.Fatal(err)
	}
	cfg, logical := profileConsumerConfig(group, driver.ConsumerConfig{
		Destinations: []string{destination}, Prefetch: prefetch, Exclusive: exclusive, Effective: group.effective,
	})
	consumer, err := group.conn.Consumer(group.ctx, cfg)
	if err != nil {
		t.Fatalf("Consumer(%q): %v", destination, err)
	}
	wrapped := newProfileConsumer(group, consumer, logical)
	t.Cleanup(func() {
		if err := wrapped.Stop(group.ctx); err != nil {
			t.Errorf("stop ordering consumer %q: %v", destination, err)
		}
	})
	return wrapped
}

func publishKeyed(t *testing.T, group *groupContext, producer driver.Producer, destination, key string, bodies ...string) {
	t.Helper()
	for _, body := range bodies {
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: destination, Key: []byte(key), Body: []byte(body)}); err != nil {
			t.Fatalf("Publish(%q, %q): %v", destination, body, err)
		}
	}
}

func receiveMessages(t *testing.T, group *groupContext, consumer driver.Consumer, count int) []driver.InboundMessage {
	t.Helper()
	messages := make([]driver.InboundMessage, 0, count)
	for range count {
		messages = append(messages, receiveMessage(t, group, consumer))
	}
	return messages
}

func assertOrderedBodies(t *testing.T, group *groupContext, consumer driver.Consumer, want ...string) {
	t.Helper()
	messages := receiveMessages(t, group, consumer, len(want))
	assertBodies(t, messages, want...)
	for _, message := range messages {
		settleMessage(t, group, message)
	}
}

func assertOrderedBodiesOneAtATime(t *testing.T, group *groupContext, consumer driver.Consumer, want ...string) {
	t.Helper()
	for _, body := range want {
		message := receiveMessages(t, group, consumer, 1)[0]
		if string(message.Body) != body {
			t.Fatalf("body=%q want=%q", message.Body, body)
		}
		settleMessage(t, group, message)
	}
}

func assertBodies(t *testing.T, messages []driver.InboundMessage, want ...string) {
	t.Helper()
	if len(messages) != len(want) {
		t.Fatalf("received %d messages, want %d", len(messages), len(want))
	}
	for i, message := range messages {
		if string(message.Body) != want[i] {
			t.Fatalf("message %d body=%q want=%q", i, message.Body, want[i])
		}
	}
}

func settleMessage(t *testing.T, group *groupContext, message driver.InboundMessage) {
	t.Helper()
	if message.Settle == nil {
		t.Fatal("ordered message has nil settler")
	}
	if err := message.Settle.Ack(group.ctx); err != nil {
		t.Fatal(err)
	}
}

func waitForSaturation(t *testing.T, group *groupContext, destination string, unsettled, ready int64) {
	t.Helper()
	waitFor(t, group, "ordered prefetch saturation", func() (bool, string) {
		view := inspectDestination(t, group, destination)
		return view.Unsettled == unsettled && view.Ready == ready, fmt.Sprintf("view=%+v", view)
	})
}

func addOrderingEvent(group *groupContext, id, outcome, destination string) {
	group.vector.Add(BehaviorEvent{ID: id, Outcome: outcome, FinalDestination: destination})
}
