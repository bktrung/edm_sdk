package conformance

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func init() { registerGroup("failure", runFailure) }

func runFailure(group *groupContext) {
	group.Check("transient publish failure is classified", func(t *testing.T) {
		name := "failure.publish.transient"
		producer := newProducer(t, group, name, driver.ProducerConfig{Effective: group.effective})
		injectFailure(t, group, FaultPublishFailure)
		err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name})
		assertFaultError(t, err, driver.KindTransient, true)
		group.vector.Add(BehaviorEvent{ID: "failure-publish-transient", Outcome: "transient", FinalDestination: name})
	})

	group.Check("publish recovers after a transient outage", func(t *testing.T) {
		name := "failure.publish.recovery"
		producer := newProducer(t, group, name, driver.ProducerConfig{Effective: group.effective})
		injectFailure(t, group, FaultPublishFailure)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("failed")}); err == nil {
			t.Fatal("outage Publish() error = nil")
		}
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("recovered")}); err != nil {
			t.Fatalf("Publish() after recovery = %v", err)
		}
		if got := inspectDestination(t, group, name).Ready; got != 1 {
			t.Fatalf("Ready after recovery = %d, want 1", got)
		}
		group.vector.Add(BehaviorEvent{ID: "failure-publish-recovery", Outcome: "recovered", FinalDestination: name})
	})

	group.Check("failed publish leaves no phantom message", func(t *testing.T) {
		name := "failure.publish.no-phantom"
		producer := newProducer(t, group, name, driver.ProducerConfig{Effective: group.effective})
		injectFailure(t, group, FaultPublishFailure)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("lost")}); err == nil {
			t.Fatal("Publish() error = nil")
		}
		waitForStable(t, group, "failed publish to leave no phantom message", func() (bool, string) {
			view := inspectDestination(t, group, name)
			return view.Ready == 0 && view.Unsettled == 0, fmt.Sprintf("view=%+v", view)
		})
		group.vector.Add(BehaviorEvent{ID: "failure-publish-no-phantom", Outcome: "absent", FinalDestination: name})
	})

	group.Check("connection fault reports a transient error", func(t *testing.T) {
		name := "failure.errors.classified"
		_ = newProducer(t, group, name, driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, name, 1)
		injectFailure(t, group, FaultConnectionDrop)
		err := receiveFailureError(t, group, consumer)
		assertFaultError(t, err, driver.KindTransient, true)
		group.vector.Add(BehaviorEvent{ID: "failure-errors-classified", Outcome: "transient", FinalDestination: name})
	})

	group.Check("transient error does not close Errors", func(t *testing.T) {
		name := "failure.errors.open"
		_ = newProducer(t, group, name, driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, name, 1)
		injectFailure(t, group, FaultConnectionDrop)
		_ = receiveFailureError(t, group, consumer)
		injectFailure(t, group, FaultConnectionDrop)
		second := receiveFailureError(t, group, consumer)
		assertFaultError(t, second, driver.KindTransient, true)
		group.vector.Add(BehaviorEvent{ID: "failure-errors-open", Outcome: "open", FinalDestination: name})
	})

	group.Check("transient error does not close Messages", func(t *testing.T) {
		name := "failure.messages.open"
		producer := newProducer(t, group, name, driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, name, 1)
		injectFailure(t, group, FaultConnectionDrop)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("control")}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		if string(message.Body) != "control" {
			t.Fatalf("body=%q, want control", message.Body)
		}
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "failure-messages-open", Outcome: "delivered", FinalDestination: name})
	})

	group.Check("unread Errors do not block delivery", func(t *testing.T) {
		name := "failure.errors.unread"
		producer := newProducer(t, group, name, driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, name, 1)
		injectFailureAsync(t, group, FaultConnectionDrop)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("unblocked")}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "failure-errors-unread", Outcome: "unblocked", FinalDestination: name})
	})

	group.Check("connection drop redelivers an unsettled message", func(t *testing.T) {
		name := "failure.redelivery.once"
		producer := newProducer(t, group, name, driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, name, 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("redeliver")}); err != nil {
			t.Fatal(err)
		}
		first := receiveMessage(t, group, consumer)
		injectFailure(t, group, FaultConnectionDrop)
		second := receiveMessage(t, group, consumer)
		if string(second.Body) != string(first.Body) {
			t.Fatalf("redelivery body=%q, want %q", second.Body, first.Body)
		}
		ackMessage(t, group, second)
		group.vector.Add(BehaviorEvent{ID: "failure-redelivery-once", Outcome: "redelivered", AttemptCount: 2, FinalDestination: name})
	})

	group.Check("delivery failure causes no duplicate after recovery", func(t *testing.T) {
		name := "failure.redelivery.no-duplicate"
		producer := newProducer(t, group, name, driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, name, 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("unique")}); err != nil {
			t.Fatal(err)
		}
		first := receiveMessage(t, group, consumer)
		injectFailure(t, group, FaultDeliveryFailure)
		second := receiveMessage(t, group, consumer)
		if string(second.Body) != string(first.Body) {
			t.Fatalf("redelivery body=%q, want %q", second.Body, first.Body)
		}
		ackMessage(t, group, second)
		waitFor(t, group, "redelivered message to settle", func() (bool, string) {
			view := inspectDestination(t, group, name)
			return view.Ready == 0 && view.Unsettled == 0, fmt.Sprintf("view=%+v", view)
		})
		assertNoDelivery(t, group, consumer, "duplicate delivery after recovery")
		group.vector.Add(BehaviorEvent{ID: "failure-redelivery-no-duplicate", Outcome: "at-least-once", FinalDestination: name})
	})

	group.Check("delivery count survives a delivery fault", func(t *testing.T) {
		name := "failure.redelivery.count"
		producer := newProducer(t, group, name, driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, name, 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name}); err != nil {
			t.Fatal(err)
		}
		first := receiveMessage(t, group, consumer)
		injectFailure(t, group, FaultDeliveryFailure)
		second := receiveMessage(t, group, consumer)
		if group.effective.NativeDeliveryCount {
			if second.DeliveryCount <= first.DeliveryCount {
				t.Fatalf("redelivery count=%d, first=%d", second.DeliveryCount, first.DeliveryCount)
			}
		} else if second.DeliveryCount != -1 {
			t.Fatalf("DeliveryCount=%d, want -1 when unavailable", second.DeliveryCount)
		}
		ackMessage(t, group, second)
		group.vector.Add(BehaviorEvent{ID: "failure-redelivery-count", Outcome: "advanced", FinalDestination: name})
	})

	group.Check("stale settlement after connection drop is fatal", func(t *testing.T) {
		name := "failure.settlement.fatal"
		producer := newProducer(t, group, name, driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, name, 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name}); err != nil {
			t.Fatal(err)
		}
		first := receiveMessage(t, group, consumer)
		injectFailure(t, group, FaultConnectionDrop)
		err := first.Settle.Ack(group.ctx)
		assertFaultError(t, err, driver.KindFatal, false)
		ackMessage(t, group, receiveMessage(t, group, consumer))
		group.vector.Add(BehaviorEvent{ID: "failure-settlement-fatal", Outcome: "fatal", FinalDestination: name})
	})

	group.Check("recovered delivery remains settleable", func(t *testing.T) {
		name := "failure.settlement.recovered"
		producer := newProducer(t, group, name, driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, name, 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name}); err != nil {
			t.Fatal(err)
		}
		_ = receiveMessage(t, group, consumer)
		injectFailure(t, group, FaultConnectionDrop)
		second := receiveMessage(t, group, consumer)
		if err := second.Settle.Ack(group.ctx); err != nil {
			t.Fatalf("Ack() after recovery = %v", err)
		}
		waitFor(t, group, "recovered settlement", func() (bool, string) {
			view := inspectDestination(t, group, name)
			return view.Ready == 0 && view.Unsettled == 0, fmt.Sprintf("view=%+v", view)
		})
		group.vector.Add(BehaviorEvent{ID: "failure-settlement-recovered", Outcome: "settled", FinalDestination: name})
	})

	group.Check("fatal publish failure is non-retryable", func(t *testing.T) {
		name := "failure.publish.fatal"
		producer := newProducer(t, group, name, driver.ProducerConfig{Effective: group.effective})
		injectFailure(t, group, FaultFatalPublish)
		err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name})
		assertFaultError(t, err, driver.KindFatal, false)
		group.vector.Add(BehaviorEvent{ID: "failure-publish-fatal", Outcome: "fatal", FinalDestination: name})
	})

	group.Check("stale settlement preserves its sentinel", func(t *testing.T) {
		name := "failure.settlement.sentinel"
		producer := newProducer(t, group, name, driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, name, 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name}); err != nil {
			t.Fatal(err)
		}
		first := receiveMessage(t, group, consumer)
		injectFailure(t, group, FaultConnectionDrop)
		err := first.Settle.Nack(group.ctx, driver.NackOptions{Requeue: true})
		if !errors.Is(err, driver.ErrAlreadySettled) {
			t.Fatalf("stale settlement error=%v, want ErrAlreadySettled", err)
		}
		ackMessage(t, group, receiveMessage(t, group, consumer))
		group.vector.Add(BehaviorEvent{ID: "failure-settlement-sentinel", Outcome: "wrapped", FinalDestination: name})
	})

	group.Check("repeating a fault sequence is deterministic", func(t *testing.T) {
		first := runDeterministicFaultSequence(t, group, "failure.deterministic.a")
		second := runDeterministicFaultSequence(t, group, "failure.deterministic.b")
		if first != second {
			t.Fatalf("fault sequence outcomes differ: first=%q second=%q", first, second)
		}
		group.vector.Add(BehaviorEvent{ID: "failure-deterministic", Outcome: "stable", FinalDestination: "failure.deterministic"})
	})
}

func injectFailure(t *testing.T, group *groupContext, kind FaultKind) {
	t.Helper()
	if group.inject == nil {
		t.Fatal("fault injector is not configured")
	}
	if err := group.inject(group.ctx, kind); err != nil {
		t.Fatalf("inject %s: %v", kind, err)
	}
}

func injectFailureAsync(t *testing.T, group *groupContext, kind FaultKind) {
	t.Helper()
	if group.inject == nil {
		t.Fatal("fault injector is not configured")
	}
	done := make(chan error, 1)
	go func() { done <- group.inject(group.ctx, kind) }()
	ctx, cancel := context.WithTimeout(group.ctx, waitTimeout)
	defer cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("inject %s: %v", kind, err)
		}
	case <-ctx.Done():
		t.Fatalf("inject %s blocked with unread Errors", kind)
	}
}

func receiveFailureError(t *testing.T, group *groupContext, consumer driver.Consumer) error {
	t.Helper()
	var received error
	waitFor(t, group, "fault error", func() (bool, string) {
		select {
		case err, ok := <-consumer.Errors():
			if !ok {
				return false, "Errors channel closed"
			}
			received = err
			return true, fmt.Sprintf("error=%v", err)
		default:
			return false, "no fault error"
		}
	})
	return received
}

func assertFaultError(t *testing.T, err error, wantKind driver.Kind, wantRetryable bool) {
	t.Helper()
	if err == nil {
		t.Fatalf("error=nil, want %s", wantKind)
	}
	var classified driver.ClassifiedError
	if !errors.As(err, &classified) {
		t.Fatalf("error %v is not classified, want %v", err, wantKind)
	}
	if classified.Kind() != wantKind {
		t.Fatalf("error classification=%v, want %v", classified.Kind(), wantKind)
	}
	if classified.Retryable() != wantRetryable {
		t.Fatalf("error Retryable()=%v, want %v", classified.Retryable(), wantRetryable)
	}
}

func runDeterministicFaultSequence(t *testing.T, group *groupContext, name string) string {
	t.Helper()
	producer := newProducer(t, group, name, driver.ProducerConfig{Effective: group.effective})
	consumer := newConsumer(t, group, name, 1)
	injectFailure(t, group, FaultPublishFailure)
	failed := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("failed")})
	kind, classified := driver.Classify(failed)
	assertFaultError(t, failed, driver.KindTransient, true)
	if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("ok")}); err != nil {
		t.Fatal(err)
	}
	message := receiveMessage(t, group, consumer)
	ackMessage(t, group, message)
	if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("delivery")}); err != nil {
		t.Fatal(err)
	}
	_ = receiveMessage(t, group, consumer)
	injectFailure(t, group, FaultDeliveryFailure)
	redelivery := receiveMessage(t, group, consumer)
	ackMessage(t, group, redelivery)
	deliveryError := receiveFailureError(t, group, consumer)
	deliveryKind, deliveryClassified := driver.Classify(deliveryError)
	injectFailure(t, group, FaultConnectionDrop)
	connectionError := receiveFailureError(t, group, consumer)
	connectionKind, connectionClassified := driver.Classify(connectionError)
	return fmt.Sprintf("%v/%t/%t|%v/%t|%v/%t", kind, classified, failed != nil, deliveryKind, deliveryClassified, connectionKind, connectionClassified)
}
