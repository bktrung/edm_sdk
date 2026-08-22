package conformance

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func init() {
	registerGroup("settle", runSettle)
}

func runSettle(group *groupContext) {
	group.Check("ack removes delivered message from broker view", func(t *testing.T) {
		producer := newProducer(t, group, "settle.ack", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, "settle.ack", 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "settle.ack"}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		if err := message.Settle.Ack(group.ctx); err != nil {
			t.Fatal(err)
		}
		waitFor(t, group, "Ack to remove the message", func() (bool, string) {
			view := inspectDestination(t, group, "settle.ack")
			return view.Ready == 0 && view.Unsettled == 0, fmt.Sprintf("view=%+v", view)
		})
		group.vector.Add(BehaviorEvent{ID: "ack-visible", Outcome: "ack", FinalDestination: "settle.ack"})
	})

	group.Check("requeue nack returns the message for redelivery", func(t *testing.T) {
		producer := newProducer(t, group, "settle.requeue", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, "settle.requeue", 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "settle.requeue"}); err != nil {
			t.Fatal(err)
		}
		first := receiveMessage(t, group, consumer)
		// Pause first: dispatch runs synchronously inside Nack, and with this the
		// sole consumer and its prefetch slot free, an unpaused destination would
		// redeliver before the inspector ever sees the requeued message as Ready.
		if err := consumer.Pause("settle.requeue"); err != nil {
			t.Fatalf("Pause() error = %v", err)
		}
		if err := first.Settle.Nack(group.ctx, driver.NackOptions{Requeue: true}); err != nil {
			t.Fatal(err)
		}
		waitFor(t, group, "requeue nack to return the message", func() (bool, string) {
			view := inspectDestination(t, group, "settle.requeue")
			return view.Ready+view.Unsettled == 1, fmt.Sprintf("view=%+v", view)
		})
		if err := consumer.Resume("settle.requeue"); err != nil {
			t.Fatalf("Resume() error = %v", err)
		}
		second := receiveMessage(t, group, consumer)
		if group.effective.NativeDeliveryCount && second.DeliveryCount < 1 {
			t.Fatalf("redelivery count=%d, want at least 1", second.DeliveryCount)
		}
		if err := second.Settle.Ack(group.ctx); err != nil {
			t.Fatal(err)
		}
		waitFor(t, group, "redelivered message to settle", func() (bool, string) {
			view := inspectDestination(t, group, "settle.requeue")
			return view.Ready == 0 && view.Unsettled == 0, fmt.Sprintf("view=%+v", view)
		})
		group.vector.Add(BehaviorEvent{ID: "nack-requeue", Outcome: "redelivered", AttemptCount: 2, FinalDestination: "settle.requeue"})
	})

	group.Check("zero-value nack does not requeue", func(t *testing.T) {
		producer := newProducer(t, group, "settle.discard", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, "settle.discard", 2)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "settle.discard"}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		if err := message.Settle.Nack(group.ctx, driver.NackOptions{}); err != nil {
			t.Fatal(err)
		}
		// Positive control: a second publish must still arrive promptly, proving
		// the dispatch loop is live for this destination during the window that
		// follows.
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "settle.discard"}); err != nil {
			t.Fatal(err)
		}
		control := receiveMessage(t, group, consumer)
		waitForStable(t, group, "discarded message to stay absent", func() (bool, string) {
			view := inspectDestination(t, group, "settle.discard")
			return view.Ready == 0 && view.Unsettled == 1, fmt.Sprintf("view=%+v", view)
		})
		if err := control.Settle.Ack(group.ctx); err != nil {
			t.Fatal(err)
		}
		group.vector.Add(BehaviorEvent{ID: "nack-discard", Outcome: "discarded", FinalDestination: "settle.discard"})
	})

	group.Check("discard nack does not deliver after settlement", func(t *testing.T) {
		producer := newProducer(t, group, "settle.no-redelivery", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, "settle.no-redelivery", 2)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "settle.no-redelivery"}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		if err := message.Settle.Nack(group.ctx, driver.NackOptions{Requeue: false}); err != nil {
			t.Fatal(err)
		}
		// Positive control: publish and drain a second message before the
		// stable-empty read, proving the channel is actively delivering rather
		// than merely stalled.
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "settle.no-redelivery"}); err != nil {
			t.Fatal(err)
		}
		control := receiveMessage(t, group, consumer)
		if err := control.Settle.Ack(group.ctx); err != nil {
			t.Fatal(err)
		}
		waitForStable(t, group, "settled message to stay out of Messages", func() (bool, string) {
			select {
			case redelivered, ok := <-consumer.Messages():
				if !ok {
					return false, "Messages channel closed"
				}
				return false, fmt.Sprintf("received %q", redelivered.Destination)
			default:
				return true, "no redelivery"
			}
		})
		group.vector.Add(BehaviorEvent{ID: "no-redelivery", Outcome: "discarded", FinalDestination: "settle.no-redelivery"})
	})

	group.Check("acked message is never redelivered", func(t *testing.T) {
		producer := newProducer(t, group, "settle.ack-no-redelivery", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, "settle.ack-no-redelivery", 2)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "settle.ack-no-redelivery"}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		if err := message.Settle.Ack(group.ctx); err != nil {
			t.Fatal(err)
		}
		// Positive control: a second publish must still arrive, proving delivery
		// is live during the window that follows.
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "settle.ack-no-redelivery"}); err != nil {
			t.Fatal(err)
		}
		control := receiveMessage(t, group, consumer)
		waitForStable(t, group, "acked message to stay out of Messages", func() (bool, string) {
			select {
			case redelivered, ok := <-consumer.Messages():
				if !ok {
					return false, "Messages channel closed"
				}
				return false, fmt.Sprintf("received unexpected redelivery destination=%q", redelivered.Destination)
			default:
				return true, "no redelivery"
			}
		})
		if err := control.Settle.Ack(group.ctx); err != nil {
			t.Fatal(err)
		}
		group.vector.Add(BehaviorEvent{ID: "ack-no-redelivery", Outcome: "ack", FinalDestination: "settle.ack-no-redelivery"})
	})

	group.Check("settling an already-acked message stays safe after a clean stop", func(t *testing.T) {
		// A clean Stop requires zero outstanding messages, so this can only ever
		// reach the message through its already-settled path; it does not exercise
		// a stop-while-unsettled state (that needs Drain, from a different group).
		producer := newProducer(t, group, "settle.after-stop", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, "settle.after-stop", 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "settle.after-stop"}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		if err := message.Settle.Ack(group.ctx); err != nil {
			t.Fatal(err)
		}
		if err := consumer.Stop(group.ctx); err != nil {
			t.Fatal(err)
		}
		err := message.Settle.Ack(group.ctx)
		if !errors.Is(err, driver.ErrAlreadySettled) {
			t.Fatalf("settle after stop error=%v, want ErrAlreadySettled", err)
		}
		if kind, ok := driver.Classify(err); !ok || kind != driver.KindFatal {
			t.Fatalf("settle after stop classification=(%v,%t), want fatal", kind, ok)
		}
		group.vector.Add(BehaviorEvent{ID: "settle-after-stop", Outcome: "already-settled", FinalDestination: "settle.after-stop"})
	})

	group.Check("ack then ack is already settled", func(t *testing.T) {
		assertDoubleSettlement(t, group, "settle.double-ack",
			func(message driver.InboundMessage) error { return message.Settle.Ack(group.ctx) },
			func(message driver.InboundMessage) error { return message.Settle.Ack(group.ctx) },
			BrokerView{Ready: 0, Unsettled: 0})
		group.vector.Add(BehaviorEvent{ID: "double-ack", Outcome: "already-settled", FinalDestination: "settle.double-ack"})
	})

	group.Check("ack then nack is already settled", func(t *testing.T) {
		assertDoubleSettlement(t, group, "settle.ack-nack",
			func(message driver.InboundMessage) error { return message.Settle.Ack(group.ctx) },
			func(message driver.InboundMessage) error {
				return message.Settle.Nack(group.ctx, driver.NackOptions{Requeue: true})
			},
			BrokerView{Ready: 0, Unsettled: 0})
		group.vector.Add(BehaviorEvent{ID: "ack-nack", Outcome: "already-settled", FinalDestination: "settle.ack-nack"})
	})

	group.Check("nack then ack is already settled", func(t *testing.T) {
		assertDoubleSettlement(t, group, "settle.nack-ack",
			func(message driver.InboundMessage) error { return message.Settle.Nack(group.ctx, driver.NackOptions{}) },
			func(message driver.InboundMessage) error { return message.Settle.Ack(group.ctx) },
			BrokerView{Ready: 0, Unsettled: 0})
		group.vector.Add(BehaviorEvent{ID: "nack-ack", Outcome: "already-settled", FinalDestination: "settle.nack-ack"})
	})

	group.Check("nack then nack is already settled", func(t *testing.T) {
		// The first nack requeues onto the sole consumer, which still has a free
		// prefetch slot: dispatch runs synchronously inside Nack, so by the time
		// it returns the redelivered copy is already sitting unsettled, not Ready.
		assertDoubleSettlement(t, group, "settle.double-nack",
			func(message driver.InboundMessage) error {
				return message.Settle.Nack(group.ctx, driver.NackOptions{Requeue: true})
			},
			func(message driver.InboundMessage) error {
				return message.Settle.Nack(group.ctx, driver.NackOptions{Requeue: true})
			},
			BrokerView{Ready: 0, Unsettled: 1})
		group.vector.Add(BehaviorEvent{ID: "double-nack", Outcome: "already-settled", FinalDestination: "settle.double-nack"})
	})

	group.Check("out-of-order settlement accounts for every message", func(t *testing.T) {
		producer := newProducer(t, group, "settle.out-of-order", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, "settle.out-of-order", 3)
		publishCount(t, group, producer, "settle.out-of-order", 3)
		messages := []driver.InboundMessage{
			receiveMessage(t, group, consumer),
			receiveMessage(t, group, consumer),
			receiveMessage(t, group, consumer),
		}
		for _, index := range []int{1, 0, 2} {
			if err := messages[index].Settle.Ack(group.ctx); err != nil {
				t.Fatal(err)
			}
		}
		waitFor(t, group, "out-of-order settlements to clear the view", func() (bool, string) {
			view := inspectDestination(t, group, "settle.out-of-order")
			return view.Ready == 0 && view.Unsettled == 0, fmt.Sprintf("view=%+v", view)
		})
		group.vector.Add(BehaviorEvent{ID: "out-of-order", Outcome: "ack", FinalDestination: "settle.out-of-order"})
	})

	group.Check("out-of-order requeue and ack lose nothing", func(t *testing.T) {
		producer := newProducer(t, group, "settle.mixed-order", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, "settle.mixed-order", 3)
		publishCount(t, group, producer, "settle.mixed-order", 3)
		messages := []driver.InboundMessage{
			receiveMessage(t, group, consumer),
			receiveMessage(t, group, consumer),
			receiveMessage(t, group, consumer),
		}
		// Pause first: dispatch runs synchronously inside Nack, and with a free
		// prefetch slot an unpaused destination would redeliver the requeued
		// message before the inspector ever sees it as Ready.
		if err := consumer.Pause("settle.mixed-order"); err != nil {
			t.Fatalf("Pause() error = %v", err)
		}
		if err := messages[1].Settle.Nack(group.ctx, driver.NackOptions{Requeue: true}); err != nil {
			t.Fatal(err)
		}
		if err := messages[0].Settle.Ack(group.ctx); err != nil {
			t.Fatal(err)
		}
		if err := messages[2].Settle.Nack(group.ctx, driver.NackOptions{}); err != nil {
			t.Fatal(err)
		}
		waitFor(t, group, "mixed settlements to expose one requeue", func() (bool, string) {
			view := inspectDestination(t, group, "settle.mixed-order")
			return view.Ready+view.Unsettled == 1, fmt.Sprintf("view=%+v", view)
		})
		if err := consumer.Resume("settle.mixed-order"); err != nil {
			t.Fatalf("Resume() error = %v", err)
		}
		redelivery := receiveMessage(t, group, consumer)
		if err := redelivery.Settle.Ack(group.ctx); err != nil {
			t.Fatal(err)
		}
		waitFor(t, group, "mixed settlements to finish", func() (bool, string) {
			view := inspectDestination(t, group, "settle.mixed-order")
			return view.Ready == 0 && view.Unsettled == 0, fmt.Sprintf("view=%+v", view)
		})
		group.vector.Add(BehaviorEvent{ID: "mixed-order", Outcome: "redelivered", AttemptCount: 2, FinalDestination: "settle.mixed-order"})
	})

	group.Check("settlement after failed stop is safe", func(t *testing.T) {
		producer := newProducer(t, group, "settle.stop", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, "settle.stop", 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "settle.stop"}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		if err := consumer.Stop(group.ctx); err == nil {
			t.Fatal("Stop() succeeded with an outstanding message")
		}
		if err := message.Settle.Ack(group.ctx); err != nil {
			t.Fatal(err)
		}
		if err := consumer.Stop(group.ctx); err != nil {
			t.Fatal(err)
		}
		group.vector.Add(BehaviorEvent{ID: "stop-after-settlement", Outcome: "ack", FinalDestination: "settle.stop"})
	})

	group.Check("distinct messages settle concurrently", func(t *testing.T) {
		producer := newProducer(t, group, "settle.concurrent", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, "settle.concurrent", 4)
		publishCount(t, group, producer, "settle.concurrent", 4)
		messages := make([]driver.InboundMessage, 4)
		for i := range messages {
			messages[i] = receiveMessage(t, group, consumer)
		}
		errorsCh := make(chan error, len(messages))
		var waitGroup sync.WaitGroup
		for _, message := range messages {
			message := message
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				errorsCh <- message.Settle.Ack(group.ctx)
			}()
		}
		waitGroup.Wait()
		close(errorsCh)
		for err := range errorsCh {
			if err != nil {
				t.Fatal(err)
			}
		}
		waitFor(t, group, "concurrent settlements to clear the view", func() (bool, string) {
			view := inspectDestination(t, group, "settle.concurrent")
			return view.Ready == 0 && view.Unsettled == 0, fmt.Sprintf("view=%+v", view)
		})
		group.vector.Add(BehaviorEvent{ID: "concurrent", Outcome: "ack", FinalDestination: "settle.concurrent"})
	})

	group.Check("cancelled ack leaves the message unsettled", func(t *testing.T) {
		producer := newProducer(t, group, "settle.cancel-ack", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, "settle.cancel-ack", 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "settle.cancel-ack"}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		ctx, cancel := context.WithCancel(group.ctx)
		cancel()
		if err := message.Settle.Ack(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Ack() error=%v, want context.Canceled", err)
		}
		waitForStable(t, group, "cancelled ack to leave the message unsettled", func() (bool, string) {
			view := inspectDestination(t, group, "settle.cancel-ack")
			return view.Unsettled == 1, fmt.Sprintf("view=%+v", view)
		})
		// Positive control: a live ack on the same message must still succeed,
		// proving the cancelled call above did not settle it.
		if err := message.Settle.Ack(group.ctx); err != nil {
			t.Fatal(err)
		}
		group.vector.Add(BehaviorEvent{ID: "cancel-ack", Outcome: "cancelled", FinalDestination: "settle.cancel-ack"})
	})

	group.Check("cancelled nack leaves the message unsettled", func(t *testing.T) {
		producer := newProducer(t, group, "settle.cancel-nack", driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, "settle.cancel-nack", 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "settle.cancel-nack"}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		ctx, cancel := context.WithCancel(group.ctx)
		cancel()
		if err := message.Settle.Nack(ctx, driver.NackOptions{Requeue: true}); !errors.Is(err, context.Canceled) {
			t.Fatalf("Nack() error=%v, want context.Canceled", err)
		}
		waitForStable(t, group, "cancelled nack to leave the message unsettled", func() (bool, string) {
			view := inspectDestination(t, group, "settle.cancel-nack")
			return view.Unsettled == 1, fmt.Sprintf("view=%+v", view)
		})
		// Positive control: a live nack on the same message must still succeed
		// and redeliver, proving the cancelled call above did not settle it.
		if err := message.Settle.Nack(group.ctx, driver.NackOptions{Requeue: true}); err != nil {
			t.Fatal(err)
		}
		redelivery := receiveMessage(t, group, consumer)
		if err := redelivery.Settle.Ack(group.ctx); err != nil {
			t.Fatal(err)
		}
		group.vector.Add(BehaviorEvent{ID: "cancel-nack", Outcome: "cancelled", AttemptCount: 2, FinalDestination: "settle.cancel-nack"})
	})
}

// assertDoubleSettlement publishes one message, settles it with first, then
// asserts second is rejected as already-settled and that the rejected call
// left the broker view exactly where first left it. wantAfterFirst is the
// caller-known outcome of first alone (before second ever runs), so the
// baseline can never absorb second's effect the way a post-hoc read would.
func assertDoubleSettlement(t *testing.T, group *groupContext, destination string, first, second func(driver.InboundMessage) error, wantAfterFirst BrokerView) {
	t.Helper()
	producer := newProducer(t, group, destination, driver.ProducerConfig{Effective: group.effective})
	consumer := newConsumer(t, group, destination, 1)
	if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: destination}); err != nil {
		t.Fatal(err)
	}
	message := receiveMessage(t, group, consumer)
	if err := first(message); err != nil {
		t.Fatal(err)
	}
	waitFor(t, group, "first settlement to reach its expected view", func() (bool, string) {
		got := inspectDestination(t, group, destination)
		return got == wantAfterFirst, fmt.Sprintf("view=%+v, want=%+v", got, wantAfterFirst)
	})
	err := second(message)
	if !errors.Is(err, driver.ErrAlreadySettled) {
		t.Fatalf("second settlement error=%v, want ErrAlreadySettled", err)
	}
	if kind, ok := driver.Classify(err); !ok || kind != driver.KindFatal {
		t.Fatalf("second settlement classification=(%v,%t), want fatal", kind, ok)
	}
	waitForStable(t, group, "already-settled delivery to leave the broker view unchanged", func() (bool, string) {
		got := inspectDestination(t, group, destination)
		return got == wantAfterFirst, fmt.Sprintf("view=%+v, want=%+v", got, wantAfterFirst)
	})
	// A requeuing first settlement leaves a redelivered copy outstanding; drain
	// and ack it so the consumer can stop cleanly at test end.
	if wantAfterFirst.Unsettled > 0 {
		redelivery := receiveMessage(t, group, consumer)
		if err := redelivery.Settle.Ack(group.ctx); err != nil {
			t.Fatal(err)
		}
	}
}
