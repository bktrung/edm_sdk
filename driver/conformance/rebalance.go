package conformance

import (
	"errors"
	"fmt"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func init() {
	registerGroup("rebalance", runRebalance)
}

func runRebalance(group *groupContext) {
	group.Check("adding a consumer distributes new work to both consumers", func(t *testing.T) {
		producer := newProducer(t, group, "rebalance.scale-up", driver.ProducerConfig{Effective: group.effective})
		first := newConsumer(t, group, "rebalance.scale-up", 1)
		second := newConsumer(t, group, "rebalance.scale-up", 1)
		if err := producer.Publish(group.ctx,
			driver.OutboundMessage{Destination: "rebalance.scale-up", Body: []byte("first")},
			driver.OutboundMessage{Destination: "rebalance.scale-up", Body: []byte("second")},
		); err != nil {
			t.Fatal(err)
		}
		ackMessage(t, group, receiveMessage(t, group, first))
		ackMessage(t, group, receiveMessage(t, group, second))
		group.vector.Add(BehaviorEvent{ID: "rebalance-scale-up", Outcome: "distributed", FinalDestination: "rebalance.scale-up"})
	})

	group.Check("draining a consumer reassigns new work to a survivor", func(t *testing.T) {
		producer := newProducer(t, group, "rebalance.reassign", driver.ProducerConfig{Effective: group.effective})
		departing := newConsumer(t, group, "rebalance.reassign", 1)
		survivor := newConsumer(t, group, "rebalance.reassign", 1)
		if err := departing.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		publishCount(t, group, producer, "rebalance.reassign", 4)
		ackAll(t, group, survivor, 4)
		waitForStable(t, group, "drained consumer to remain without reassigned work", func() (bool, string) {
			select {
			case message, ok := <-departing.Messages():
				if !ok {
					return false, "Messages channel closed"
				}
				return false, fmt.Sprintf("received %q", message.Destination)
			default:
				return true, "no delivery to drained consumer"
			}
		})
		group.vector.Add(BehaviorEvent{ID: "rebalance-reassign", Outcome: "reassigned", FinalDestination: "rebalance.reassign"})
	})

	group.Check("drained consumer requeues in-flight work to one survivor", func(t *testing.T) {
		producer := newProducer(t, group, "rebalance.in-flight", driver.ProducerConfig{Effective: group.effective})
		first := newConsumer(t, group, "rebalance.in-flight", 1)
		second := newConsumer(t, group, "rebalance.in-flight", 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.in-flight", Body: []byte("once")}); err != nil {
			t.Fatal(err)
		}
		inFlight, departing, survivor := receiveFromEither(t, group, "in-flight message", first, second)
		if err := departing.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		if err := inFlight.Settle.Nack(group.ctx, driver.NackOptions{Requeue: true}); err != nil {
			t.Fatalf("Nack(requeue) error = %v", err)
		}
		redelivered := receiveMessage(t, group, survivor)
		if string(redelivered.Body) != "once" {
			t.Fatalf("redelivered body = %q, want once", redelivered.Body)
		}
		ackMessage(t, group, redelivered)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.in-flight", Body: []byte("next")}); err != nil {
			t.Fatal(err)
		}
		next := receiveMessage(t, group, survivor)
		if string(next.Body) != "next" {
			t.Fatalf("next body = %q, want next", next.Body)
		}
		ackMessage(t, group, next)
		waitFor(t, group, "requeued in-flight message to settle once", func() (bool, string) {
			view := inspectDestination(t, group, "rebalance.in-flight")
			return view.Ready == 0 && view.Unsettled == 0, fmt.Sprintf("view=%+v", view)
		})
		group.vector.Add(BehaviorEvent{ID: "rebalance-in-flight", Outcome: "redelivered", AttemptCount: 2, FinalDestination: "rebalance.in-flight"})
	})

	group.Check("redelivery count advances only when available", func(t *testing.T) {
		producer := newProducer(t, group, "rebalance.delivery-count", driver.ProducerConfig{Effective: group.effective})
		firstConsumer := newConsumer(t, group, "rebalance.delivery-count", 1)
		secondConsumer := newConsumer(t, group, "rebalance.delivery-count", 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.delivery-count"}); err != nil {
			t.Fatal(err)
		}
		first, departing, survivor := receiveFromEither(t, group, "delivery-count message", firstConsumer, secondConsumer)
		if err := departing.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		if err := first.Settle.Nack(group.ctx, driver.NackOptions{Requeue: true}); err != nil {
			t.Fatal(err)
		}
		redelivery := receiveMessage(t, group, survivor)
		if group.effective.NativeDeliveryCount {
			if redelivery.DeliveryCount <= first.DeliveryCount {
				t.Fatalf("redelivery DeliveryCount = %d, first = %d", redelivery.DeliveryCount, first.DeliveryCount)
			}
		} else if redelivery.DeliveryCount != -1 {
			t.Fatalf("DeliveryCount = %d, want -1 when unavailable", redelivery.DeliveryCount)
		}
		ackMessage(t, group, redelivery)
		group.vector.Add(BehaviorEvent{ID: "rebalance-delivery-count", Outcome: "redelivered", AttemptCount: 2, FinalDestination: "rebalance.delivery-count"})
	})

	group.Check("each consumer keeps its own prefetch budget after joining", func(t *testing.T) {
		producer := newProducer(t, group, "rebalance.prefetch", driver.ProducerConfig{Effective: group.effective})
		const firstPrefetch = 1
		const secondPrefetch = 2
		first := newConsumer(t, group, "rebalance.prefetch", firstPrefetch)
		second := newConsumer(t, group, "rebalance.prefetch", secondPrefetch)
		publishCount(t, group, producer, "rebalance.prefetch", 4)
		waitFor(t, group, "per-consumer prefetch budgets to saturate", func() (bool, string) {
			view := inspectDestination(t, group, "rebalance.prefetch")
			return view.Unsettled == 3 && view.Ready == 1, fmt.Sprintf("view=%+v", view)
		})

		initial := make([]driver.InboundMessage, 0, firstPrefetch+secondPrefetch)
		firstOutstanding, secondOutstanding := 0, 0
		for range firstPrefetch + secondPrefetch {
			message, owner, _ := receiveFromEither(t, group, "prefetch delivery", first, second)
			initial = append(initial, message)
			switch owner {
			case first:
				firstOutstanding++
			case second:
				secondOutstanding++
			default:
				t.Fatalf("prefetch delivery came from an unknown consumer")
			}
		}
		if firstOutstanding > firstPrefetch {
			t.Fatalf("first consumer held %d unsettled messages, prefetch=%d", firstOutstanding, firstPrefetch)
		}
		if secondOutstanding > secondPrefetch {
			t.Fatalf("second consumer held %d unsettled messages, prefetch=%d", secondOutstanding, secondPrefetch)
		}
		for _, message := range initial {
			ackMessage(t, group, message)
		}
		last, _, _ := receiveFromEither(t, group, "prefetch delivery", first, second)
		ackMessage(t, group, last)
		waitFor(t, group, "prefetch destination to drain after all messages are acknowledged", func() (bool, string) {
			view := inspectDestination(t, group, "rebalance.prefetch")
			return view.Ready == 0 && view.Unsettled == 0, fmt.Sprintf("view=%+v", view)
		})
		group.vector.Add(BehaviorEvent{ID: "rebalance-prefetch", Outcome: "bounded", FinalDestination: "rebalance.prefetch"})
	})

	group.Check("repeating a departure leaves redistribution stable", func(t *testing.T) {
		producer := newProducer(t, group, "rebalance.repeat", driver.ProducerConfig{Effective: group.effective})
		departing := newConsumer(t, group, "rebalance.repeat", 1)
		survivor := newConsumer(t, group, "rebalance.repeat", 1)
		if err := departing.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		if err := departing.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.repeat"}); err != nil {
			t.Fatal(err)
		}
		ackMessage(t, group, receiveMessage(t, group, survivor))
		group.vector.Add(BehaviorEvent{ID: "rebalance-repeat", Outcome: "stable", FinalDestination: "rebalance.repeat"})
	})

	group.Check("delivery made before a membership change remains settleable", func(t *testing.T) {
		producer := newProducer(t, group, "rebalance.settle", driver.ProducerConfig{Effective: group.effective})
		first := newConsumer(t, group, "rebalance.settle", 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.settle"}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, first)
		_ = newConsumer(t, group, "rebalance.settle", 1)
		if err := message.Settle.Ack(group.ctx); err != nil {
			if kind, classified := driver.Classify(err); !classified {
				t.Fatalf("Ack() error = %v, want success or classified error", err)
			} else {
				t.Logf("Ack() after membership change returned classified %s error: %v", kind, err)
				if cleanupErr := message.Settle.Nack(group.ctx, driver.NackOptions{}); cleanupErr != nil && !isAlreadySettled(cleanupErr) {
					t.Fatalf("Nack() after classified Ack() error = %v", cleanupErr)
				}
			}
		}
		waitFor(t, group, "pre-change delivery settlement to clear", func() (bool, string) {
			view := inspectDestination(t, group, "rebalance.settle")
			return view.Unsettled == 0, fmt.Sprintf("view=%+v", view)
		})
		group.vector.Add(BehaviorEvent{ID: "rebalance-settle", Outcome: "handled", FinalDestination: "rebalance.settle"})
	})

	group.Check("a key is never delivered concurrently to two consumers", func(t *testing.T) {
		producerA := newProducer(t, group, "rebalance.key-a", driver.ProducerConfig{Effective: group.effective})
		producerB := newProducer(t, group, "rebalance.key-b", driver.ProducerConfig{Effective: group.effective})
		departing := newConsumerFor(t, group, driver.ConsumerConfig{
			Destinations: []string{"rebalance.key-a", "rebalance.key-b"}, Prefetch: 4, Effective: group.effective,
		})
		if err := producerA.Publish(group.ctx,
			driver.OutboundMessage{Destination: "rebalance.key-a", Key: []byte("K")},
			driver.OutboundMessage{Destination: "rebalance.key-a", Key: []byte("L")},
		); err != nil {
			t.Fatal(err)
		}
		var keyK, keyL driver.InboundMessage
		for range 2 {
			message := receiveMessage(t, group, departing)
			switch string(message.Key) {
			case "K":
				keyK = message
			case "L":
				keyL = message
			default:
				t.Fatalf("in-flight key = %q, want K or L", message.Key)
			}
		}
		if keyK.Settle == nil || keyL.Settle == nil {
			t.Fatalf("in-flight deliveries = K:%#v L:%#v, want both keys", keyK, keyL)
		}
		if err := departing.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		survivor := newConsumerFor(t, group, driver.ConsumerConfig{
			Destinations: []string{"rebalance.key-a", "rebalance.key-b"}, Prefetch: 1, Effective: group.effective,
		})
		if err := producerA.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.key-a", Key: []byte("K")}); err != nil {
			t.Fatal(err)
		}
		if err := producerB.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.key-b", Body: []byte("control")}); err != nil {
			t.Fatal(err)
		}
		var control driver.InboundMessage
		var beforeControl []driver.InboundMessage
		waitFor(t, group, "control destination delivery", func() (bool, string) {
			select {
			case message, ok := <-survivor.Messages():
				if !ok {
					return false, "Messages channel closed"
				}
				if message.Destination != "rebalance.key-b" {
					beforeControl = append(beforeControl, message)
					return false, fmt.Sprintf("received destination=%q; waiting for control", message.Destination)
				}
				control = message
				return true, "received control destination"
			default:
				return false, "no control message"
			}
		})
		ackMessage(t, group, control)
		waitForStable(t, group, "non-owner consumer to avoid concurrent keyed delivery", func() (bool, string) {
			select {
			case message, ok := <-survivor.Messages():
				if !ok {
					return false, "Messages channel closed"
				}
				return false, fmt.Sprintf("received destination=%q key=%q", message.Destination, message.Key)
			default:
				return true, "no concurrent keyed delivery"
			}
		})
		for _, message := range beforeControl {
			if err := message.Settle.Nack(group.ctx, driver.NackOptions{Requeue: true}); err != nil {
				t.Fatalf("requeue pre-control message: %v", err)
			}
		}
		ackMessage(t, group, keyK)
		duplicate := receiveMessage(t, group, survivor)
		if string(duplicate.Key) != "K" {
			t.Fatalf("reassigned key = %q, want K", duplicate.Key)
		}
		ackMessage(t, group, duplicate)
		ackMessage(t, group, keyL)
		group.vector.Add(BehaviorEvent{ID: "rebalance-key-exclusive", Outcome: "exclusive", FinalDestination: "rebalance.key-a"})
	})

	group.Check("settled key reassigns after its holder drains", func(t *testing.T) {
		producer := newProducer(t, group, "rebalance.affinity", driver.ProducerConfig{Effective: group.effective})
		departing := newConsumer(t, group, "rebalance.affinity", 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.affinity", Key: []byte("order")}); err != nil {
			t.Fatal(err)
		}
		ackMessage(t, group, receiveMessage(t, group, departing))
		if err := departing.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		survivor := newConsumer(t, group, "rebalance.affinity", 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.affinity", Key: []byte("order")}); err != nil {
			t.Fatal(err)
		}
		ackMessage(t, group, receiveMessage(t, group, survivor))
		group.vector.Add(BehaviorEvent{ID: "rebalance-settled-key", Outcome: "reassigned", FinalDestination: "rebalance.affinity"})
	})

	group.Check("joining consumer does not take already unsettled work", func(t *testing.T) {
		producer := newProducer(t, group, "rebalance.join-in-flight", driver.ProducerConfig{Effective: group.effective})
		first := newConsumer(t, group, "rebalance.join-in-flight", 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.join-in-flight", Body: []byte("old")}); err != nil {
			t.Fatal(err)
		}
		old := receiveMessage(t, group, first)
		joining := newConsumer(t, group, "rebalance.join-in-flight", 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.join-in-flight", Body: []byte("control")}); err != nil {
			t.Fatal(err)
		}
		control := receiveMessage(t, group, joining)
		if string(control.Body) != "control" {
			t.Fatalf("joining consumer received %q, want control", control.Body)
		}
		ackMessage(t, group, control)
		ackMessage(t, group, old)
		group.vector.Add(BehaviorEvent{ID: "rebalance-join-in-flight", Outcome: "preserved", FinalDestination: "rebalance.join-in-flight"})
	})

	group.Check("joining an idle destination receives newly published work", func(t *testing.T) {
		producer := newProducer(t, group, "rebalance.idle-join", driver.ProducerConfig{Effective: group.effective})
		departing := newConsumer(t, group, "rebalance.idle-join", 1)
		if err := departing.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		joining := newConsumer(t, group, "rebalance.idle-join", 1)
		publishCount(t, group, producer, "rebalance.idle-join", 4)
		ackAll(t, group, joining, 4)
		group.vector.Add(BehaviorEvent{ID: "rebalance-idle-join", Outcome: "joined", FinalDestination: "rebalance.idle-join"})
	})
}

func isAlreadySettled(err error) bool {
	return errors.Is(err, driver.ErrAlreadySettled)
}

func receiveFromEither(
	t *testing.T,
	group *groupContext,
	what string,
	first, second driver.Consumer,
) (driver.InboundMessage, driver.Consumer, driver.Consumer) {
	t.Helper()
	var received driver.InboundMessage
	var owner, other driver.Consumer
	waitFor(t, group, what, func() (bool, string) {
		select {
		case message, ok := <-first.Messages():
			if !ok {
				return false, "first Messages channel closed"
			}
			received, owner, other = message, first, second
			return true, "received by first consumer"
		case message, ok := <-second.Messages():
			if !ok {
				return false, "second Messages channel closed"
			}
			received, owner, other = message, second, first
			return true, "received by second consumer"
		default:
			return false, "no delivery"
		}
	})
	return received, owner, other
}
