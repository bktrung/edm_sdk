package conformance

import (
	"fmt"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func init() { registerGroup("ordering", runOrdering) }

// runOrdering runs the ordering group.
//
// Four checks here publish with an explicit key on purpose and then require
// several of those records unsettled at once, and each derives that count from
// the driver's declared consumer scaling rather than hard-coding it. A driver
// declaring ScalingPartitionBound admits one unsettled delivery per partition
// and one key reaches one partition, so the most such a check may ask of it is
// the number of distinct keys it publishes. A driver declaring ScalingFree
// keeps the count the checks were written with.
//
// The waiver is a loss and not a simplification, and the next reader must not
// mistake it for one: on a partition-bound driver this group no longer shows
// that same-key receipt order survives a prefetch window, because there is no
// window to survive. What it still shows is that the records of one key arrive
// in the order they were published, with one delivery of that key in hand at a
// time.
func runOrdering(group *groupContext) {
	group.Check("initial same-key prefetch retains receipt order", func(t *testing.T) {
		const destination = "ordering.initial"
		producer := newProducer(t, group, profileDestination(group, destination), driver.ProducerConfig{Effective: group.effective})
		consumer := orderingConsumer(t, group, destination, 3, false)
		publishKeyed(t, group, producer, destination, "k", "one", "two", "three")
		// One key reaches one partition, so a partition-bound driver admits one of
		// the three records and leaves the other two ready, where a free-scaling
		// driver takes all three.
		outstanding := deliveryCeiling(group.factoryCapabilities, 1, 3)
		waitForSaturation(t, group, destination, outstanding, 3-outstanding)
		assertOrderedBodies(t, group, consumer, outstanding, "one", "two", "three")
		addOrderingEvent(group, "ordering-initial", "one,two,three", destination)
	})

	group.Check("same-key receipt order survives irregular prefetch refills", func(t *testing.T) {
		const destination = "ordering.refill"
		producer := newProducer(t, group, profileDestination(group, destination), driver.ProducerConfig{Effective: group.effective})
		consumer := orderingConsumer(t, group, destination, 3, false)
		bodies := []string{"one", "two", "three", "four", "five", "six"}
		publishKeyed(t, group, producer, destination, "k", bodies...)
		// One key reaches one partition, so a partition-bound driver holds one of
		// the six records unsettled and leaves the other five ready, where a
		// free-scaling driver holds three of each.
		outstanding := deliveryCeiling(group.factoryCapabilities, 1, 3)
		waitForSaturation(t, group, destination, outstanding, len(bodies)-outstanding)
		first := receiveMessages(t, group, consumer, outstanding)
		// Settle the newest delivery first and the rest in receipt order. The
		// check's instrument is that a window refills irregularly and that receipt
		// order survives the irregularity; a driver admitting one delivery per
		// partition has no window to refill, and settling the one delivery it holds
		// is what admits the next.
		settleMessage(t, group, first[len(first)-1])
		settleMessages(t, group, first[:len(first)-1])
		remaining := receiveMessagesWithin(t, group, consumer, len(bodies)-outstanding, outstanding)
		assertBodies(t, remaining, bodies[outstanding:]...)
		addOrderingEvent(group, "ordering-refill", "one,two,three,four,five,six", destination)
	})

	group.Check("independent keys retain independent receipt sequences", func(t *testing.T) {
		const destination = "ordering.keys"
		producer := newProducer(t, group, profileDestination(group, destination), driver.ProducerConfig{Effective: group.effective})
		consumer := orderingConsumer(t, group, destination, 4, false)
		publishKeyed(t, group, producer, destination, "a", "a1", "a2")
		publishKeyed(t, group, producer, destination, "b", "b1", "b2")
		// Two keys reach two partitions, so a partition-bound driver holds two of
		// the four records and cannot be handed the other two until it settles
		// those, where a free-scaling driver holds all four.
		outstanding := deliveryCeiling(group.factoryCapabilities, 2, 4)
		messages := receiveMessagesWithin(t, group, consumer, 4, outstanding)
		sequences := map[string][]string{}
		for _, message := range messages {
			sequences[string(message.Key)] = append(sequences[string(message.Key)], string(message.Body))
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
		// One key reaches one partition, so a partition-bound driver holds one of
		// the two records at once and is asked for them one at a time.
		outstanding := deliveryCeiling(group.factoryCapabilities, 1, 2)
		assertOrderedBodies(t, group, consumer, outstanding, "one", "two")
		addOrderingEvent(group, "ordering-exclusive", "usable", destination)
	})

	group.Check("zero-value producer configuration preserves ordered receipt", func(t *testing.T) {
		const destination = "ordering.zero"
		producer := newProducer(t, group, profileDestination(group, destination), driver.ProducerConfig{
			Effective: group.effective,
		})
		consumer := orderingConsumer(t, group, destination, 1, true)
		publishKeyed(t, group, producer, destination, "k", "one", "two")
		assertOrderedBodies(t, group, consumer, 1, "one", "two")
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

// assertOrderedBodies receives len(want) deliveries in receipt order, settling
// each one once ceiling are in hand, and fails unless the bodies arrive in want
// order. A ceiling at or above len(want) holds every delivery before settling
// any, which is what a driver declaring ScalingFree is asked for.
func assertOrderedBodies(t *testing.T, group *groupContext, consumer driver.Consumer, ceiling int, want ...string) {
	t.Helper()
	messages := receiveMessagesWithin(t, group, consumer, len(want), ceiling)
	assertBodies(t, messages, want...)
}

// receiveMessagesWithin receives count deliveries in receipt order and returns
// them in the order they were received, settling each one once ceiling are in
// hand so that a driver admitting one delivery per partition is never asked to
// hold more than it can.
func receiveMessagesWithin(t *testing.T, group *groupContext, consumer driver.Consumer, count, ceiling int) []driver.InboundMessage {
	t.Helper()
	messages := make([]driver.InboundMessage, 0, count)
	held := make([]driver.InboundMessage, 0, ceiling)
	for range count {
		message := receiveMessages(t, group, consumer, 1)[0]
		messages = append(messages, message)
		held = append(held, message)
		if len(held) == ceiling {
			settleMessages(t, group, held)
			held = held[:0]
		}
	}
	settleMessages(t, group, held)
	return messages
}

// settleMessages acknowledges deliveries in the order given.
func settleMessages(t *testing.T, group *groupContext, messages []driver.InboundMessage) {
	t.Helper()
	for _, message := range messages {
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

func waitForSaturation(t *testing.T, group *groupContext, destination string, unsettled, ready int) {
	t.Helper()
	waitFor(t, group, "ordered prefetch saturation", func() (bool, string) {
		view := inspectDestination(t, group, destination)
		return view.Unsettled == int64(unsettled) && view.Ready == int64(ready), fmt.Sprintf("view=%+v", view)
	})
}

func addOrderingEvent(group *groupContext, id, outcome, destination string) {
	group.vector.add(BehaviorEvent{ID: id, Outcome: outcome, FinalDestination: destination})
}
