package conformance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func init() {
	registerGroup("consume", runConsume)
}

func runConsume(group *groupContext) {
	group.Check("delivered message preserves portable identity", func(t *testing.T) {
		producer := newProducer(t, group, profileDestination(group, "consume.identity"), driver.ProducerConfig{Effective: group.effective})
		wantHeaders := []driver.Header{{Key: "x-trace", Value: []byte("trace-1")}, {Key: "x-empty", Value: nil}}
		if err := producer.Publish(group.ctx, driver.OutboundMessage{
			Destination: "consume.identity", Key: []byte("order-42"), Headers: wantHeaders, Body: []byte("body"),
		}); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		message := receiveMessage(t, group, newConsumer(t, group, profileDestination(group, "consume.identity"), 1))
		if message.Destination != "consume.identity" {
			t.Fatalf("Destination = %q, want %q", message.Destination, "consume.identity")
		}
		if !bytes.Equal(message.Key, []byte("order-42")) || !bytes.Equal(message.Body, []byte("body")) {
			t.Fatalf("message identity = key %q body %q", message.Key, message.Body)
		}
		if len(message.Headers) != len(wantHeaders) {
			t.Fatalf("Headers = %#v, want %d headers", message.Headers, len(wantHeaders))
		}
		if got := headerByKey(t, message.Headers, "x-trace"); !bytes.Equal(got.Value, []byte("trace-1")) {
			t.Fatalf("x-trace = %q, want %q", got.Value, "trace-1")
		}
		if got := headerByKey(t, message.Headers, "x-empty"); len(got.Value) != 0 {
			t.Fatalf("x-empty length = %d, want 0", len(got.Value))
		}
		if message.Settle == nil || message.Ref == (driver.BrokerRef{}) || message.ReceivedAt.IsZero() {
			t.Fatalf("delivery metadata = settle:%v ref:%+v received:%v", message.Settle != nil, message.Ref, message.ReceivedAt)
		}
		ackMessage(t, group, message)
		group.vector.add(BehaviorEvent{ID: "identity", Outcome: "ok", FinalDestination: "consume.identity"})
	})

	group.Check("delivery count uses minus one when unavailable", func(t *testing.T) {
		producer := newProducer(t, group, profileDestination(group, "consume.delivery-count"), driver.ProducerConfig{Effective: group.effective})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.delivery-count"}); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		message := receiveMessage(t, group, newConsumer(t, group, profileDestination(group, "consume.delivery-count"), 1))
		if group.effective.NativeDeliveryCount {
			if message.DeliveryCount < 0 {
				t.Fatalf("DeliveryCount = %d, want a usable count", message.DeliveryCount)
			}
		} else if message.DeliveryCount != -1 {
			t.Fatalf("DeliveryCount = %d, want -1 when unavailable", message.DeliveryCount)
		}
		ackMessage(t, group, message)
		group.vector.add(BehaviorEvent{ID: "delivery-count", Outcome: "ok", FinalDestination: "consume.delivery-count"})
	})

	group.Check("one consumer receives every configured destination", func(t *testing.T) {
		producerA := newProducer(t, group, profileDestination(group, "consume.dest-a"), driver.ProducerConfig{Effective: group.effective})
		producerB := newProducer(t, group, profileDestination(group, "consume.dest-b"), driver.ProducerConfig{Effective: group.effective})
		if err := producerA.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.dest-a", Body: []byte("a")}); err != nil {
			t.Fatalf("Publish(a) error = %v", err)
		}
		if err := producerB.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.dest-b", Body: []byte("b")}); err != nil {
			t.Fatalf("Publish(b) error = %v", err)
		}
		consumer := newConsumerFor(t, group, driver.ConsumerConfig{
			Destinations: []string{"consume.dest-a", "consume.dest-b"}, Prefetch: 2, Effective: group.effective,
		})
		seen := map[string]bool{}
		for range 2 {
			message := receiveMessage(t, group, consumer)
			seen[message.Destination] = true
			ackMessage(t, group, message)
		}
		if !seen["consume.dest-a"] || !seen["consume.dest-b"] {
			t.Fatalf("destinations received = %v, want both destinations", seen)
		}
		group.vector.add(BehaviorEvent{ID: "multi-destination", Outcome: "ok", FinalDestination: "consume.dest-a,consume.dest-b"})
	})

	group.Check("prefetch saturates at the subscription budget", func(t *testing.T) {
		producerA := newPlacedProducer(t, group, "consume.prefetch-total-a", driver.ProducerConfig{Effective: group.effective})
		producerB := newPlacedProducer(t, group, "consume.prefetch-total-b", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumerFor(t, group, driver.ConsumerConfig{
			Destinations: []string{"consume.prefetch-total-a", "consume.prefetch-total-b"}, Prefetch: 4,
			PerDestination: map[string]int{"consume.prefetch-total-a": 2, "consume.prefetch-total-b": 2}, Effective: group.effective,
		})
		publishPlacedCount(t, group, producerA, "consume.prefetch-total-a", 4)
		publishPlacedCount(t, group, producerB, "consume.prefetch-total-b", 4)
		waitFor(t, group, "prefetch total to saturate", func() (bool, string) {
			views := inspectDestinations(t, group, "consume.prefetch-total-a", "consume.prefetch-total-b")
			total := views[0].Unsettled + views[1].Unsettled
			return total == 4, fmt.Sprintf("unsettled total=%d; views=%#v", total, views)
		})
		waitForStable(t, group, "prefetch destination shares to stay within bounds", func() (bool, string) {
			views := inspectDestinations(t, group, "consume.prefetch-total-a", "consume.prefetch-total-b")
			for _, view := range views {
				if view.Unsettled > 2 {
					return false, fmt.Sprintf("unsettled by destination=%#v", views)
				}
			}
			return true, fmt.Sprintf("unsettled by destination=%#v", views)
		})
		ackAll(t, group, consumer, 8)
		group.vector.add(BehaviorEvent{ID: "prefetch-total", Outcome: "ok", FinalDestination: "consume.prefetch-total"})
	})

	group.Check("prefetch caps admitted deliveries below destination capacity sum", func(t *testing.T) {
		const destinationA = "consume.prefetch-cap-a"
		const destinationB = "consume.prefetch-cap-b"
		producerA := newPlacedProducer(t, group, destinationA, driver.ProducerConfig{Effective: group.effective})
		producerB := newPlacedProducer(t, group, destinationB, driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumerFor(t, group, driver.ConsumerConfig{
			Destinations: []string{destinationA, destinationB}, Prefetch: 2,
			PerDestination: map[string]int{destinationA: 2, destinationB: 2}, Effective: group.effective,
		})
		publishPlacedCount(t, group, producerA, destinationA, 4)
		publishPlacedCount(t, group, producerB, destinationB, 4)
		held := []driver.InboundMessage{
			receiveMessage(t, group, consumer),
			receiveMessage(t, group, consumer),
		}
		// Broker transport credit may exceed SDK admission, so only
		// Messages and the settlers held by the caller prove this cap.
		assertNoDelivery(t, group, consumer, "delivery beyond aggregate Prefetch=2")
		for range 6 {
			ackMessage(t, group, held[0])
			held[0] = held[1]
			held[1] = receiveMessage(t, group, consumer)
			assertNoDelivery(t, group, consumer, "delivery beyond aggregate Prefetch=2 after ACK/refill")
		}
		for _, message := range held {
			ackMessage(t, group, message)
		}
		group.vector.add(BehaviorEvent{ID: "prefetch-admission-cap", Outcome: "ok", FinalDestination: destinationA + "," + destinationB})
	})

	group.Check("prefetch applies each destination share", func(t *testing.T) {
		producerA := newPlacedProducer(t, group, "consume.prefetch-share-a", driver.ProducerConfig{Effective: group.effective})
		producerB := newPlacedProducer(t, group, "consume.prefetch-share-b", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumerFor(t, group, driver.ConsumerConfig{
			Destinations: []string{"consume.prefetch-share-a", "consume.prefetch-share-b"}, Prefetch: 4,
			PerDestination: map[string]int{"consume.prefetch-share-a": 1, "consume.prefetch-share-b": 3}, Effective: group.effective,
		})
		publishPlacedCount(t, group, producerA, "consume.prefetch-share-a", 3)
		publishPlacedCount(t, group, producerB, "consume.prefetch-share-b", 5)
		waitFor(t, group, "prefetch destination shares to saturate", func() (bool, string) {
			views := inspectDestinations(t, group, "consume.prefetch-share-a", "consume.prefetch-share-b")
			return views[0].Unsettled == 1 && views[1].Unsettled == 3,
				fmt.Sprintf("unsettled by destination=%d/%d", views[0].Unsettled, views[1].Unsettled)
		})
		ackAll(t, group, consumer, 8)
		group.vector.add(BehaviorEvent{ID: "prefetch-share", Outcome: "ok", FinalDestination: "consume.prefetch-share"})
	})

	group.Check("pause stops only the requested destination", func(t *testing.T) {
		producerA := newProducer(t, group, profileDestination(group, "consume.pause-a"), driver.ProducerConfig{Effective: group.effective})
		producerB := newProducer(t, group, profileDestination(group, "consume.pause-b"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumerFor(t, group, driver.ConsumerConfig{
			Destinations: []string{"consume.pause-a", "consume.pause-b"}, Prefetch: 2, Effective: group.effective,
		})
		if err := consumer.Pause("consume.pause-a"); err != nil {
			t.Fatalf("Pause(a) error = %v", err)
		}
		if err := producerA.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.pause-a"}); err != nil {
			t.Fatalf("Publish(a) error = %v", err)
		}
		if err := producerB.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.pause-b"}); err != nil {
			t.Fatalf("Publish(b) error = %v", err)
		}
		message := receiveMessage(t, group, consumer)
		if message.Destination != "consume.pause-b" {
			t.Fatalf("received destination = %q, want b while a is paused", message.Destination)
		}
		ackMessage(t, group, message)
		waitForStable(t, group, "paused destination to remain empty", func() (bool, string) {
			select {
			case message, ok := <-consumer.Messages():
				if !ok {
					return false, "Messages channel closed"
				}
				return false, fmt.Sprintf("received paused destination %q", message.Destination)
			default:
				return true, "no paused message"
			}
		})
		if err := consumer.Resume("consume.pause-a"); err != nil {
			t.Fatalf("Resume(a) error = %v", err)
		}
		ackMessage(t, group, receiveMessage(t, group, consumer))
		group.vector.add(BehaviorEvent{ID: "pause-isolated", Outcome: "ok", FinalDestination: "consume.pause-a,consume.pause-b"})
	})

	group.Check("empty pause stops every destination and resume restarts delivery", func(t *testing.T) {
		producerA := newProducer(t, group, profileDestination(group, "consume.pause-all-a"), driver.ProducerConfig{Effective: group.effective})
		producerB := newProducer(t, group, profileDestination(group, "consume.pause-all-b"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumerFor(t, group, driver.ConsumerConfig{
			Destinations: []string{"consume.pause-all-a", "consume.pause-all-b"}, Prefetch: 2, Effective: group.effective,
		})
		if err := consumer.Pause(); err != nil {
			t.Fatalf("Pause() error = %v", err)
		}
		if err := producerA.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.pause-all-a"}); err != nil {
			t.Fatalf("Publish(a) error = %v", err)
		}
		if err := producerB.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.pause-all-b"}); err != nil {
			t.Fatalf("Publish(b) error = %v", err)
		}
		waitForStable(t, group, "empty Pause to stop every destination", func() (bool, string) {
			select {
			case message, ok := <-consumer.Messages():
				if !ok {
					return false, "Messages channel closed"
				}
				return false, fmt.Sprintf("received paused destination %q", message.Destination)
			default:
				return true, "no delivery while paused"
			}
		})
		if err := consumer.Resume(); err != nil {
			t.Fatalf("Resume() error = %v", err)
		}
		seen := map[string]bool{}
		for range 2 {
			message := receiveMessage(t, group, consumer)
			seen[message.Destination] = true
			ackMessage(t, group, message)
		}
		if !seen["consume.pause-all-a"] || !seen["consume.pause-all-b"] {
			t.Fatalf("destinations after Resume() = %v, want both destinations", seen)
		}
		group.vector.add(BehaviorEvent{ID: "pause-all", Outcome: "ok", FinalDestination: "consume.pause-all-a,consume.pause-all-b"})
	})

	group.Check("repeated pause is idempotent", func(t *testing.T) {
		producerA := newProducer(t, group, profileDestination(group, "consume.pause-repeat-a"), driver.ProducerConfig{Effective: group.effective})
		producerB := newProducer(t, group, profileDestination(group, "consume.pause-repeat-b"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumerFor(t, group, driver.ConsumerConfig{
			Destinations: []string{"consume.pause-repeat-a", "consume.pause-repeat-b"}, Prefetch: 2, Effective: group.effective,
		})
		if err := consumer.Pause("consume.pause-repeat-a"); err != nil {
			t.Fatalf("first Pause() error = %v", err)
		}
		if err := consumer.Pause("consume.pause-repeat-a"); err != nil {
			t.Fatalf("second Pause() error = %v", err)
		}
		if err := producerA.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.pause-repeat-a"}); err != nil {
			t.Fatalf("Publish(a) error = %v", err)
		}
		if err := producerB.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.pause-repeat-b"}); err != nil {
			t.Fatalf("Publish(b) error = %v", err)
		}
		message := receiveMessage(t, group, consumer)
		if message.Destination != "consume.pause-repeat-b" {
			t.Fatalf("repeated Pause() allowed delivery of %q", message.Destination)
		}
		ackMessage(t, group, message)
		waitForStable(t, group, "repeatedly paused destination to remain empty", func() (bool, string) {
			select {
			case message, ok := <-consumer.Messages():
				if !ok {
					return false, "Messages channel closed"
				}
				return false, fmt.Sprintf("received paused destination %q", message.Destination)
			default:
				return true, "no paused message"
			}
		})
		if err := consumer.Resume("consume.pause-repeat-a"); err != nil {
			t.Fatalf("Resume() error = %v", err)
		}
		ackMessage(t, group, receiveMessage(t, group, consumer))
		group.vector.add(BehaviorEvent{ID: "pause-idempotent", Outcome: "ok", FinalDestination: "consume.pause-repeat-a,consume.pause-repeat-b"})
	})

	group.Check("resume delivers every message that arrived while paused", func(t *testing.T) {
		producer := newProducer(t, group, profileDestination(group, "consume.resume"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "consume.resume"), 3)
		if err := consumer.Pause("consume.resume"); err != nil {
			t.Fatalf("Pause() error = %v", err)
		}
		publishCount(t, group, producer, "consume.resume", 3)
		if err := consumer.Resume("consume.resume"); err != nil {
			t.Fatalf("Resume() error = %v", err)
		}
		ackAll(t, group, consumer, 3)
		if got := inspectDestination(t, group, "consume.resume").Ready; got != 0 {
			t.Fatalf("Ready after resume = %d, want 0", got)
		}
		group.vector.add(BehaviorEvent{ID: "resume-lossless", Outcome: "ok", FinalDestination: "consume.resume"})
	})

	group.Check("paused destination stays within its prefetch share", func(t *testing.T) {
		producer := newPlacedProducer(t, group, "consume.pause-bound", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "consume.pause-bound"), 2)
		publishPlacedCount(t, group, producer, "consume.pause-bound", 2)
		first := receiveMessage(t, group, consumer)
		second := receiveMessage(t, group, consumer)
		waitFor(t, group, "prefetch saturation before pause", func() (bool, string) {
			view := inspectDestination(t, group, "consume.pause-bound")
			return view.Unsettled == 2, fmt.Sprintf("unsettled=%d", view.Unsettled)
		})
		if err := consumer.Pause("consume.pause-bound"); err != nil {
			t.Fatalf("Pause() error = %v", err)
		}
		publishCount(t, group, producer, "consume.pause-bound", 5)
		waitForStable(t, group, "paused unsettled to stay within prefetch share", func() (bool, string) {
			view := inspectDestination(t, group, "consume.pause-bound")
			return view.Unsettled <= 2, fmt.Sprintf("unsettled=%d", view.Unsettled)
		})
		if err := first.Settle.Ack(group.ctx); err != nil {
			t.Fatalf("Ack(first) error = %v", err)
		}
		if err := second.Settle.Ack(group.ctx); err != nil {
			t.Fatalf("Ack(second) error = %v", err)
		}
		group.vector.add(BehaviorEvent{ID: "pause-bound", Outcome: "ok", FinalDestination: "consume.pause-bound"})
	})

	group.Check("errors remains open across lifecycle controls", func(t *testing.T) {
		_ = newProducer(t, group, profileDestination(group, "consume.errors"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "consume.errors"), 1)
		select {
		case _, ok := <-consumer.Errors():
			if !ok {
				t.Fatal("Errors() was closed before Stop")
			}
		default:
		}
		if err := consumer.Pause("consume.errors"); err != nil {
			t.Fatalf("Pause() error = %v", err)
		}
		if err := consumer.Resume("consume.errors"); err != nil {
			t.Fatalf("Resume() error = %v", err)
		}
		if err := consumer.Drain(group.ctx); err != nil {
			t.Fatalf("Drain() error = %v", err)
		}
		select {
		case _, ok := <-consumer.Errors():
			if !ok {
				t.Fatal("Errors() was closed after lifecycle controls")
			}
		default:
		}
		group.vector.add(BehaviorEvent{ID: "errors-open", Outcome: "ok", FinalDestination: "consume.errors"})
	})

	group.Check("StartEarliest includes retained messages for a new group", func(t *testing.T) {
		destination := "consume.earliest"
		groupName := "consume-earliest-new-" + group.runID + "-" + group.profile.String()
		maintenance := group.maintenance(t)
		purgeAndCleanupTopologyDestinations(t, maintenance, group.ctx, destination)
		producer := newProducer(t, group, profileDestination(group, destination), driver.ProducerConfig{Effective: group.effective})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: destination, Body: []byte("retained")}); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		consumer := newConsumerFor(t, group, driver.ConsumerConfig{
			Group: groupName, Destinations: []string{destination}, Prefetch: 1,
			StartAt: driver.StartEarliest, Effective: group.effective,
		})
		message := receiveMessage(t, group, consumer)
		if string(message.Body) != "retained" {
			t.Fatalf("StartEarliest body = %q, want retained", message.Body)
		}
		ackMessage(t, group, message)
		group.vector.add(BehaviorEvent{ID: "start-earliest", Outcome: "ok", FinalDestination: "consume.earliest"})
	})

	group.Check("StartAt does not reposition an existing group", func(t *testing.T) {
		destination := "consume.existing-group"
		groupName := "consume-existing-" + group.runID + "-" + group.profile.String()
		maintenance := group.maintenance(t)
		purgeAndCleanupTopologyDestinations(t, maintenance, group.ctx, destination)
		producer := newProducer(t, group, profileDestination(group, destination), driver.ProducerConfig{Effective: group.effective})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: destination, Body: []byte("first")}); err != nil {
			t.Fatalf("Publish(first) error = %v", err)
		}
		firstConsumer := newConsumerFor(t, group, driver.ConsumerConfig{
			Group: groupName, Destinations: []string{destination}, Prefetch: 1,
			StartAt: driver.StartEarliest, Effective: group.effective,
		})
		first := receiveMessage(t, group, firstConsumer)
		ackMessage(t, group, first)
		if err := firstConsumer.Stop(group.ctx); err != nil {
			t.Fatalf("Stop(first consumer) error = %v", err)
		}
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: destination, Body: []byte("second")}); err != nil {
			t.Fatalf("Publish(second) error = %v", err)
		}
		secondConsumer := newConsumerFor(t, group, driver.ConsumerConfig{
			Group: groupName, Destinations: []string{destination}, Prefetch: 1,
			StartAt: driver.StartEarliest, Effective: group.effective,
		})
		second := receiveMessage(t, group, secondConsumer)
		if string(second.Body) != "second" {
			t.Fatalf("existing group was repositioned to body %q", second.Body)
		}
		ackMessage(t, group, second)
		group.vector.add(BehaviorEvent{ID: "start-existing", Outcome: "ok", FinalDestination: "consume.existing-group"})
	})

	group.Check("two groups on one destination each receive every message", func(t *testing.T) {
		if group.effective.Fanout != driver.FanoutAtConsume {
			group.Skip(t, "two groups on one destination each receive every message", "groups on one destination compete under publish fanout; independent groups are a consume-fanout obligation")
			return
		}
		destination := "consume.two-groups"
		groupName := "consume-two-groups-" + group.runID + "-" + group.profile.String()
		purgeAndCleanupTopologyDestinations(t, group.maintenance(t), group.ctx, destination)
		producer := newPlacedProducer(t, group, destination, driver.ProducerConfig{Effective: group.effective})
		consumers := []driver.Consumer{
			newConsumerFor(t, group, driver.ConsumerConfig{
				Group: groupName + "-a", Destinations: []string{destination}, Prefetch: 2,
				StartAt: driver.StartEarliest, Effective: group.effective,
			}),
			newConsumerFor(t, group, driver.ConsumerConfig{
				Group: groupName + "-b", Destinations: []string{destination}, Prefetch: 2,
				StartAt: driver.StartEarliest, Effective: group.effective,
			}),
		}
		if err := producer.Publish(group.ctx,
			driver.OutboundMessage{Destination: destination, Key: []byte(placementKey(0)), Body: []byte("shared")},
			driver.OutboundMessage{Destination: destination, Key: []byte(placementKey(1)), Body: []byte("keyed")},
		); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		var held []driver.InboundMessage
		for _, consumer := range consumers {
			held = append(held, receiveEveryBody(t, group, consumer, []string{"shared", "keyed"})...)
		}
		// Nothing is settled until every group has received every body. Settling
		// in one group releases that group's claim on the key, and a driver that
		// keeps one claim per key instead of one per group hands a keyed message
		// to the other group only after that release, which is the defect this
		// check exists for.
		for _, message := range held {
			ackMessage(t, group, message)
		}
		group.vector.add(BehaviorEvent{ID: "two-groups", Outcome: "ok", FinalDestination: destination})
	})

	group.Check("a detached group receives a message acked while it was away", func(t *testing.T) {
		if group.effective.Fanout != driver.FanoutAtConsume {
			group.Skip(t, "a detached group receives a message acked while it was away", "groups on one destination compete under publish fanout; independent groups are a consume-fanout obligation")
			return
		}
		destination := "consume.detached-group"
		groupName := "consume-detached-group-" + group.runID + "-" + group.profile.String()
		purgeAndCleanupTopologyDestinations(t, group.maintenance(t), group.ctx, destination)
		producer := newProducer(t, group, profileDestination(group, destination), driver.ProducerConfig{Effective: group.effective})
		attached := newConsumerFor(t, group, driver.ConsumerConfig{
			Group: groupName + "-a", Destinations: []string{destination}, Prefetch: 1,
			StartAt: driver.StartEarliest, Effective: group.effective,
		})
		away := newConsumerFor(t, group, driver.ConsumerConfig{
			Group: groupName + "-b", Destinations: []string{destination}, Prefetch: 1,
			StartAt: driver.StartEarliest, Effective: group.effective,
		})
		if err := away.Stop(group.ctx); err != nil {
			t.Fatalf("Stop(away consumer) error = %v", err)
		}
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: destination, Body: []byte("while-away")}); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		ackMessage(t, group, receiveMessage(t, group, attached))
		returned := newConsumerFor(t, group, driver.ConsumerConfig{
			Group: groupName + "-b", Destinations: []string{destination}, Prefetch: 1,
			StartAt: driver.StartEarliest, Effective: group.effective,
		})
		message := receiveMessage(t, group, returned)
		if string(message.Body) != "while-away" {
			t.Fatalf("rejoined consumer body = %q, want while-away", message.Body)
		}
		ackMessage(t, group, message)
		group.vector.add(BehaviorEvent{ID: "detached-group", Outcome: "ok", FinalDestination: destination})
	})

	group.Check("zero-value consumer configuration delivers", func(t *testing.T) {
		producer := newProducer(t, group, profileDestination(group, "consume.zero"), driver.ProducerConfig{Effective: group.effective})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.zero", Body: []byte("default")}); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		consumer := newConsumerFor(t, group, driver.ConsumerConfig{Destinations: []string{"consume.zero"}})
		message := receiveMessage(t, group, consumer)
		if string(message.Body) != "default" {
			t.Fatalf("zero-value consumer body = %q, want default", message.Body)
		}
		ackMessage(t, group, message)
		group.vector.add(BehaviorEvent{ID: "zero-value", Outcome: "ok", FinalDestination: "consume.zero"})
	})

	group.Check("canceled consumer creation returns context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(group.ctx)
		producer := newProducer(t, group, profileDestination(group, "consume.cancel-create"), driver.ProducerConfig{Effective: group.effective})
		cancel()
		cfg, _ := profileConsumerConfig(group, driver.ConsumerConfig{Destinations: []string{"consume.cancel-create"}})
		_, err := group.conn.Consumer(ctx, cfg)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Consumer() error = %v, want context.Canceled", err)
		}
		if kind, ok := driver.Classify(err); !ok || kind != driver.KindTransient {
			t.Fatalf("Consumer() classification = (%v, %t), want transient", kind, ok)
		}
		publishCount(t, group, producer, "consume.cancel-create", 3)
		consumer := newConsumer(t, group, profileDestination(group, "consume.cancel-create"), 1)
		ackAll(t, group, consumer, 3)
		group.vector.add(BehaviorEvent{ID: "cancel-consumer", Outcome: "cancelled", FinalDestination: "consume.cancel-create"})
	})

	group.Check("canceled Drain returns without changing consumer state", func(t *testing.T) {
		producer := newProducer(t, group, profileDestination(group, "consume.cancel-drain"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "consume.cancel-drain"), 1)
		ctx, cancel := context.WithCancel(group.ctx)
		cancel()
		assertCancelled(t, "Drain", consumer.Drain(ctx))
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.cancel-drain", Body: []byte("after-cancel")}); err != nil {
			t.Fatalf("Publish() after canceled Drain() error = %v", err)
		}
		message := receiveMessage(t, group, consumer)
		if string(message.Body) != "after-cancel" {
			t.Fatalf("message after canceled Drain() = %q, want %q", message.Body, "after-cancel")
		}
		ackMessage(t, group, message)
		group.vector.add(BehaviorEvent{ID: "cancel-drain", Outcome: "cancelled", FinalDestination: "consume.cancel-drain"})
	})

	group.Check("canceled Lag returns context cancellation", func(t *testing.T) {
		_ = newProducer(t, group, profileDestination(group, "consume.cancel-lag"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "consume.cancel-lag"), 1)
		ctx, cancel := context.WithCancel(group.ctx)
		cancel()
		_, err := consumer.Lag(ctx)
		assertCancelled(t, "Lag", err)
		group.vector.add(BehaviorEvent{ID: "cancel-lag", Outcome: "cancelled", FinalDestination: "consume.cancel-lag"})
	})

	group.Check("canceled Stop returns context cancellation", func(t *testing.T) {
		producer := newProducer(t, group, profileDestination(group, "consume.cancel-stop"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "consume.cancel-stop"), 1)
		ctx, cancel := context.WithCancel(group.ctx)
		cancel()
		assertCancelled(t, "Stop", consumer.Stop(ctx))
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.cancel-stop", Body: []byte("after-cancel")}); err != nil {
			t.Fatalf("Publish() after canceled Stop() error = %v", err)
		}
		message := receiveMessage(t, group, consumer)
		if string(message.Body) != "after-cancel" {
			t.Fatalf("message after canceled Stop() = %q, want %q", message.Body, "after-cancel")
		}
		ackMessage(t, group, message)
		group.vector.add(BehaviorEvent{ID: "cancel-stop", Outcome: "cancelled", FinalDestination: "consume.cancel-stop"})
	})
	group.Check("canceled Release returns transient and leaves consumer usable", func(t *testing.T) {
		name := "consume.cancel-release"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
		ctx, cancel := context.WithCancel(group.ctx)
		cancel()
		assertCancelled(t, "Release", consumer.Release(ctx))
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("after-cancel")}); err != nil {
			t.Fatalf("Publish() after cancelled Release() = %v", err)
		}
		message := receiveMessage(t, group, consumer)
		if string(message.Body) != "after-cancel" {
			t.Fatalf("message after cancelled Release() = %q, want %q", message.Body, "after-cancel")
		}
		ackMessage(t, group, message)
		group.vector.add(BehaviorEvent{ID: "cancel-release", Outcome: "cancelled", FinalDestination: name})
	})
}

func newConsumerFor(t *testing.T, group *groupContext, cfg driver.ConsumerConfig) driver.Consumer {
	t.Helper()
	cfg, logical := profileConsumerConfig(group, cfg)
	consumer, err := group.conn.Consumer(group.ctx, cfg)
	if err != nil {
		t.Fatalf("Consumer(%v): %v", cfg.Destinations, err)
	}
	wrapped := newProfileConsumer(group, consumer, logical)
	t.Cleanup(func() {
		if err := wrapped.Stop(group.ctx); err != nil {
			t.Errorf("stop consumer %v: %v", cfg.Destinations, err)
		}
	})
	return wrapped
}

// receiveEveryBody receives from consumer until every body in wanted has
// arrived and returns each delivery unsettled. Delivery is at-least-once, so
// the two-group checks assert that a body arrived rather than that it arrived
// once, and the caller settles what it holds. Settling happens outside this
// helper because a group that settles mid-check releases the claim it holds on
// a key, which the second group's delivery must not depend on.
func receiveEveryBody(t *testing.T, group *groupContext, consumer driver.Consumer, wanted []string) []driver.InboundMessage {
	t.Helper()
	remaining := make(map[string]bool, len(wanted))
	for _, body := range wanted {
		remaining[body] = true
	}
	var received []driver.InboundMessage
	for len(remaining) > 0 {
		message := receiveMessage(t, group, consumer)
		received = append(received, message)
		delete(remaining, string(message.Body))
	}
	return received
}

func publishCount(t *testing.T, group *groupContext, producer driver.Producer, destination string, count int) {
	t.Helper()
	messages := make([]driver.OutboundMessage, count)
	for i := range messages {
		messages[i] = driver.OutboundMessage{Destination: destination, Body: fmt.Appendf(nil, "%s-%d", destination, i)}
	}
	if err := producer.Publish(group.ctx, messages...); err != nil {
		t.Fatalf("Publish(%q, %d messages) error = %v", destination, count, err)
	}
}

// placementPartitions is the partition count a check declares when it needs
// several records of one destination outstanding at once. A driver that admits
// one delivery per partition holds at most one unsettled record per partition,
// so a check that requires n records in hand needs n partitions under the
// records it publishes.
const placementPartitions = 4

// placementKey returns the key the index-th record of a placed publish carries.
// The keys are load-bearing rather than decorative: a batch published with no
// key is the partitioner's to place and reaches a single partition, so a check
// that needs records apart has to say so with a key, and distinct keys only
// reach distinct partitions if the partitioner hashes them apart. These do,
// measured against the Kafka fixture through the driver's own producer: the
// default murmur2 partitioner places key-0 to key-3 on four distinct partitions
// of a placementPartitions destination, and their first two and first three on
// distinct partitions of a two- and a three-partition destination as well. The
// indexes cycle because a check may publish more records than a destination has
// partitions to hold them.
func placementKey(index int) string {
	return fmt.Sprintf("key-%d", index%placementPartitions)
}

// newPlacedProducer returns a producer over destination, declared with
// placementPartitions partitions first so that records keyed with placementKey
// reach distinct partitions. The declaration has to come before anything else
// creates the destination, where a check's consumer or its producer would: a
// declaration that arrives afterwards finds the destination existing and leaves
// its partition count alone.
func newPlacedProducer(t *testing.T, group *groupContext, destination string, config driver.ProducerConfig) driver.Producer {
	t.Helper()
	warmTopology(t, group, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Partitions: placementPartitions}},
		Effective:    group.effective,
	})
	return newProducer(t, group, profileDestination(group, destination), config)
}

// publishPlacedCount publishes count messages to destination, each keyed so that
// a partition-bound driver places them on distinct partitions and admits them
// together, with the bodies publishCount gives. The destination must have been
// declared through newPlacedProducer.
func publishPlacedCount(t *testing.T, group *groupContext, producer driver.Producer, destination string, count int) {
	t.Helper()
	messages := make([]driver.OutboundMessage, count)
	for i := range messages {
		messages[i] = driver.OutboundMessage{
			Destination: destination,
			Key:         []byte(placementKey(i)),
			Body:        fmt.Appendf(nil, "%s-%d", destination, i),
		}
	}
	if err := producer.Publish(group.ctx, messages...); err != nil {
		t.Fatalf("Publish(%q, %d messages) error = %v", destination, count, err)
	}
}

func inspectDestinations(t *testing.T, group *groupContext, destinations ...string) []BrokerView {
	t.Helper()
	views := make([]BrokerView, 0, len(destinations))
	for _, destination := range destinations {
		views = append(views, inspectDestination(t, group, destination))
	}
	return views
}

func ackAll(t *testing.T, group *groupContext, consumer driver.Consumer, count int) {
	t.Helper()
	for range count {
		ackMessage(t, group, receiveMessage(t, group, consumer))
	}
}
