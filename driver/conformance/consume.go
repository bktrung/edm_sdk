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
		producer := newProducer(t, group, "consume.identity", driver.ProducerConfig{Effective: group.effective})
		wantHeaders := []driver.Header{{Key: "x-trace", Value: []byte("trace-1")}, {Key: "x-empty", Value: nil}}
		if err := producer.Publish(group.ctx, driver.OutboundMessage{
			Destination: "consume.identity", Key: []byte("order-42"), Headers: wantHeaders, Body: []byte("body"),
		}); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		message := receiveMessage(t, group, newConsumer(t, group, "consume.identity", 1))
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
		group.vector.Add(BehaviorEvent{ID: "identity", Outcome: "ok", FinalDestination: "consume.identity"})
	})

	group.Check("delivery count uses minus one when unavailable", func(t *testing.T) {
		producer := newProducer(t, group, "consume.delivery-count", driver.ProducerConfig{Effective: group.effective})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.delivery-count"}); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		message := receiveMessage(t, group, newConsumer(t, group, "consume.delivery-count", 1))
		if group.effective.NativeDeliveryCount {
			if message.DeliveryCount < 0 {
				t.Fatalf("DeliveryCount = %d, want a usable count", message.DeliveryCount)
			}
		} else if message.DeliveryCount != -1 {
			t.Fatalf("DeliveryCount = %d, want -1 when unavailable", message.DeliveryCount)
		}
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "delivery-count", Outcome: "ok", FinalDestination: "consume.delivery-count"})
	})

	group.Check("one consumer receives every configured destination", func(t *testing.T) {
		producerA := newProducer(t, group, "consume.dest-a", driver.ProducerConfig{Effective: group.effective})
		producerB := newProducer(t, group, "consume.dest-b", driver.ProducerConfig{Effective: group.effective})
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
		group.vector.Add(BehaviorEvent{ID: "multi-destination", Outcome: "ok", FinalDestination: "consume.dest-a,consume.dest-b"})
	})

	group.Check("prefetch saturates at the subscription budget", func(t *testing.T) {
		producerA := newProducer(t, group, "consume.prefetch-total-a", driver.ProducerConfig{Effective: group.effective})
		producerB := newProducer(t, group, "consume.prefetch-total-b", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumerFor(t, group, driver.ConsumerConfig{
			Destinations: []string{"consume.prefetch-total-a", "consume.prefetch-total-b"}, Prefetch: 4,
			PerDestination: map[string]int{"consume.prefetch-total-a": 2, "consume.prefetch-total-b": 2}, Effective: group.effective,
		})
		publishCount(t, group, producerA, "consume.prefetch-total-a", 4)
		publishCount(t, group, producerB, "consume.prefetch-total-b", 4)
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
		group.vector.Add(BehaviorEvent{ID: "prefetch-total", Outcome: "ok", FinalDestination: "consume.prefetch-total"})
	})

	group.Check("prefetch applies each destination share", func(t *testing.T) {
		producerA := newProducer(t, group, "consume.prefetch-share-a", driver.ProducerConfig{Effective: group.effective})
		producerB := newProducer(t, group, "consume.prefetch-share-b", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumerFor(t, group, driver.ConsumerConfig{
			Destinations: []string{"consume.prefetch-share-a", "consume.prefetch-share-b"}, Prefetch: 4,
			PerDestination: map[string]int{"consume.prefetch-share-a": 1, "consume.prefetch-share-b": 3}, Effective: group.effective,
		})
		publishCount(t, group, producerA, "consume.prefetch-share-a", 3)
		publishCount(t, group, producerB, "consume.prefetch-share-b", 5)
		waitFor(t, group, "prefetch destination shares to saturate", func() (bool, string) {
			views := inspectDestinations(t, group, "consume.prefetch-share-a", "consume.prefetch-share-b")
			return views[0].Unsettled == 1 && views[1].Unsettled == 3,
				fmt.Sprintf("unsettled by destination=%d/%d", views[0].Unsettled, views[1].Unsettled)
		})
		ackAll(t, group, consumer, 8)
		group.vector.Add(BehaviorEvent{ID: "prefetch-share", Outcome: "ok", FinalDestination: "consume.prefetch-share"})
	})

	group.Check("pause stops only the requested destination", func(t *testing.T) {
		producerA := newProducer(t, group, "consume.pause-a", driver.ProducerConfig{Effective: group.effective})
		producerB := newProducer(t, group, "consume.pause-b", driver.ProducerConfig{Effective: group.effective})
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
		group.vector.Add(BehaviorEvent{ID: "pause-isolated", Outcome: "ok", FinalDestination: "consume.pause-a,consume.pause-b"})
	})

	group.Check("repeated pause is idempotent", func(t *testing.T) {
		producerA := newProducer(t, group, "consume.pause-repeat-a", driver.ProducerConfig{Effective: group.effective})
		producerB := newProducer(t, group, "consume.pause-repeat-b", driver.ProducerConfig{Effective: group.effective})
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
		group.vector.Add(BehaviorEvent{ID: "pause-idempotent", Outcome: "ok", FinalDestination: "consume.pause-repeat-a,consume.pause-repeat-b"})
	})

	group.Check("resume delivers every message that arrived while paused", func(t *testing.T) {
		producer := newProducer(t, group, "consume.resume", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, "consume.resume", 3)
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
		group.vector.Add(BehaviorEvent{ID: "resume-lossless", Outcome: "ok", FinalDestination: "consume.resume"})
	})

	group.Check("paused destination stays within its prefetch share", func(t *testing.T) {
		producer := newProducer(t, group, "consume.pause-bound", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, "consume.pause-bound", 2)
		publishCount(t, group, producer, "consume.pause-bound", 2)
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
		group.vector.Add(BehaviorEvent{ID: "pause-bound", Outcome: "ok", FinalDestination: "consume.pause-bound"})
	})

	group.Check("errors remains open across lifecycle controls", func(t *testing.T) {
		_ = newProducer(t, group, "consume.errors", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, "consume.errors", 1)
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
		group.vector.Add(BehaviorEvent{ID: "errors-open", Outcome: "ok", FinalDestination: "consume.errors"})
	})

	group.Check("StartLatest excludes retained messages for a new group", func(t *testing.T) {
		producer := newProducer(t, group, "consume.latest", driver.ProducerConfig{Effective: group.effective})
		controlProducer := newProducer(t, group, "consume.latest-control", driver.ProducerConfig{Effective: group.effective})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.latest", Body: []byte("retained")}); err != nil {
			t.Fatalf("Publish(retained) error = %v", err)
		}
		consumer := newConsumerFor(t, group, driver.ConsumerConfig{
			Group: "consume-latest-new-" + group.profile.String(), Destinations: []string{"consume.latest", "consume.latest-control"}, Prefetch: 2,
			StartAt: driver.StartLatest, Effective: group.effective,
		})
		if err := controlProducer.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.latest-control", Body: []byte("control")}); err != nil {
			t.Fatalf("Publish(control) error = %v", err)
		}
		var control driver.InboundMessage
		waitFor(t, group, "StartLatest control delivery", func() (bool, string) {
			select {
			case message, ok := <-consumer.Messages():
				if !ok {
					return false, "Messages channel closed"
				}
				control = message
				return true, fmt.Sprintf("received destination=%q body=%q", message.Destination, message.Body)
			default:
				return false, "no control message"
			}
		})
		if control.Destination != "consume.latest-control" {
			t.Fatalf("StartLatest delivered retained destination %q before control", control.Destination)
		}
		ackMessage(t, group, control)
		waitForStable(t, group, "StartLatest to exclude retained messages", func() (bool, string) {
			select {
			case message, ok := <-consumer.Messages():
				if !ok {
					return false, "Messages channel closed"
				}
				return false, fmt.Sprintf("received retained destination=%q body=%q", message.Destination, message.Body)
			default:
				return true, "no retained message"
			}
		})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.latest", Body: []byte("new")}); err != nil {
			t.Fatalf("Publish(new) error = %v", err)
		}
		message := receiveMessage(t, group, consumer)
		if string(message.Body) != "new" {
			t.Fatalf("StartLatest body = %q, want new", message.Body)
		}
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "start-latest", Outcome: "ok", FinalDestination: "consume.latest"})
	})

	group.Check("StartEarliest includes retained messages for a new group", func(t *testing.T) {
		producer := newProducer(t, group, "consume.earliest", driver.ProducerConfig{Effective: group.effective})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.earliest", Body: []byte("retained")}); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		consumer := newConsumerFor(t, group, driver.ConsumerConfig{
			Group: "consume-earliest-new-" + group.profile.String(), Destinations: []string{"consume.earliest"}, Prefetch: 1,
			StartAt: driver.StartEarliest, Effective: group.effective,
		})
		message := receiveMessage(t, group, consumer)
		if string(message.Body) != "retained" {
			t.Fatalf("StartEarliest body = %q, want retained", message.Body)
		}
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "start-earliest", Outcome: "ok", FinalDestination: "consume.earliest"})
	})

	group.Check("StartAt does not reposition an existing group", func(t *testing.T) {
		producer := newProducer(t, group, "consume.existing-group", driver.ProducerConfig{Effective: group.effective})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.existing-group", Body: []byte("first")}); err != nil {
			t.Fatalf("Publish(first) error = %v", err)
		}
		firstConsumer := newConsumerFor(t, group, driver.ConsumerConfig{
			Group: "consume-existing", Destinations: []string{"consume.existing-group"}, Prefetch: 1,
			StartAt: driver.StartEarliest, Effective: group.effective,
		})
		first := receiveMessage(t, group, firstConsumer)
		ackMessage(t, group, first)
		if err := firstConsumer.Stop(group.ctx); err != nil {
			t.Fatalf("Stop(first consumer) error = %v", err)
		}
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.existing-group", Body: []byte("second")}); err != nil {
			t.Fatalf("Publish(second) error = %v", err)
		}
		secondConsumer := newConsumerFor(t, group, driver.ConsumerConfig{
			Group: "consume-existing", Destinations: []string{"consume.existing-group"}, Prefetch: 1,
			StartAt: driver.StartEarliest, Effective: group.effective,
		})
		second := receiveMessage(t, group, secondConsumer)
		if string(second.Body) != "second" {
			t.Fatalf("existing group was repositioned to body %q", second.Body)
		}
		ackMessage(t, group, second)
		group.vector.Add(BehaviorEvent{ID: "start-existing", Outcome: "ok", FinalDestination: "consume.existing-group"})
	})

	group.Check("zero-value consumer configuration delivers", func(t *testing.T) {
		producer := newProducer(t, group, "consume.zero", driver.ProducerConfig{Effective: group.effective})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.zero", Body: []byte("default")}); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		consumer := newConsumerFor(t, group, driver.ConsumerConfig{Destinations: []string{"consume.zero"}})
		message := receiveMessage(t, group, consumer)
		if string(message.Body) != "default" {
			t.Fatalf("zero-value consumer body = %q, want default", message.Body)
		}
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "zero-value", Outcome: "ok", FinalDestination: "consume.zero"})
	})

	group.Check("canceled consumer creation returns context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(group.ctx)
		producer := newProducer(t, group, "consume.cancel-create", driver.ProducerConfig{Effective: group.effective})
		cancel()
		_, err := group.conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"consume.cancel-create"}})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Consumer() error = %v, want context.Canceled", err)
		}
		if kind, ok := driver.Classify(err); !ok || kind != driver.KindTransient {
			t.Fatalf("Consumer() classification = (%v, %t), want transient", kind, ok)
		}
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.cancel-create", Body: []byte("after-cancel")}); err != nil {
			t.Fatalf("Publish() after canceled Consumer() error = %v", err)
		}
		consumer := newConsumer(t, group, "consume.cancel-create", 1)
		message := receiveMessage(t, group, consumer)
		if string(message.Body) != "after-cancel" {
			t.Fatalf("message after canceled Consumer() = %q, want %q", message.Body, "after-cancel")
		}
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "cancel-consumer", Outcome: "cancelled", FinalDestination: "consume.cancel-create"})
	})

	group.Check("canceled Drain returns without changing consumer state", func(t *testing.T) {
		producer := newProducer(t, group, "consume.cancel-drain", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, "consume.cancel-drain", 1)
		ctx, cancel := context.WithCancel(group.ctx)
		cancel()
		if err := consumer.Drain(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Drain() error = %v, want context.Canceled", err)
		}
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.cancel-drain", Body: []byte("after-cancel")}); err != nil {
			t.Fatalf("Publish() after canceled Drain() error = %v", err)
		}
		message := receiveMessage(t, group, consumer)
		if string(message.Body) != "after-cancel" {
			t.Fatalf("message after canceled Drain() = %q, want %q", message.Body, "after-cancel")
		}
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "cancel-drain", Outcome: "cancelled", FinalDestination: "consume.cancel-drain"})
	})

	group.Check("canceled Lag returns context cancellation", func(t *testing.T) {
		_ = newProducer(t, group, "consume.cancel-lag", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, "consume.cancel-lag", 1)
		ctx, cancel := context.WithCancel(group.ctx)
		cancel()
		if _, err := consumer.Lag(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Lag() error = %v, want context.Canceled", err)
		}
		group.vector.Add(BehaviorEvent{ID: "cancel-lag", Outcome: "cancelled", FinalDestination: "consume.cancel-lag"})
	})

	group.Check("canceled Stop returns context cancellation", func(t *testing.T) {
		producer := newProducer(t, group, "consume.cancel-stop", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, "consume.cancel-stop", 1)
		ctx, cancel := context.WithCancel(group.ctx)
		cancel()
		if err := consumer.Stop(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Stop() error = %v, want context.Canceled", err)
		}
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "consume.cancel-stop", Body: []byte("after-cancel")}); err != nil {
			t.Fatalf("Publish() after canceled Stop() error = %v", err)
		}
		message := receiveMessage(t, group, consumer)
		if string(message.Body) != "after-cancel" {
			t.Fatalf("message after canceled Stop() = %q, want %q", message.Body, "after-cancel")
		}
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "cancel-stop", Outcome: "cancelled", FinalDestination: "consume.cancel-stop"})
	})
}

func newConsumerFor(t *testing.T, group *groupContext, cfg driver.ConsumerConfig) driver.Consumer {
	t.Helper()
	consumer, err := group.conn.Consumer(group.ctx, cfg)
	if err != nil {
		t.Fatalf("Consumer(%v): %v", cfg.Destinations, err)
	}
	t.Cleanup(func() {
		if err := consumer.Stop(group.ctx); err != nil {
			t.Errorf("stop consumer %v: %v", cfg.Destinations, err)
		}
	})
	return consumer
}

func publishCount(t *testing.T, group *groupContext, producer driver.Producer, destination string, count int) {
	t.Helper()
	messages := make([]driver.OutboundMessage, count)
	for i := range messages {
		messages[i] = driver.OutboundMessage{Destination: destination, Body: []byte(fmt.Sprintf("%s-%d", destination, i))}
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
