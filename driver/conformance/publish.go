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
	registerGroup("publish", runPublish)
}

func runPublish(group *groupContext) {
	group.Check("confirmed publish is visible in the destination", func(t *testing.T) {
		producer := newProducer(t, group, "publish.visible", driver.ProducerConfig{RequireDurableAck: true, Effective: group.effective})
		publish := driver.OutboundMessage{Destination: "publish.visible", Body: []byte("visible")}
		if err := producer.Publish(group.ctx, publish); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		view := inspectDestination(t, group, "publish.visible")
		if view.Ready != 1 {
			t.Fatalf("Ready = %d, want 1", view.Ready)
		}
		group.vector.Add(BehaviorEvent{ID: "confirmed-visible", Outcome: "ok", FinalDestination: "publish.visible"})
	})

	group.Check("zero-value publish config accepts a normal message", func(t *testing.T) {
		producer := newProducer(t, group, "publish.zero", driver.ProducerConfig{})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "publish.zero", Body: []byte("default")}); err != nil {
			t.Fatalf("Publish() with zero-value config error = %v", err)
		}
		if got := inspectDestination(t, group, "publish.zero").Ready; got != 1 {
			t.Fatalf("Ready = %d, want 1", got)
		}
		group.vector.Add(BehaviorEvent{ID: "zero-value", Outcome: "ok", FinalDestination: "publish.zero"})
	})

	group.Check("flush is safe after a confirmed publish", func(t *testing.T) {
		producer := newProducer(t, group, "publish.flush", driver.ProducerConfig{RequireDurableAck: true, Effective: group.effective})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "publish.flush", Body: []byte("flush")}); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		before := inspectDestination(t, group, "publish.flush").Ready
		if err := producer.Flush(group.ctx); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}
		if got := inspectDestination(t, group, "publish.flush").Ready; got != before {
			t.Fatalf("Ready after Flush = %d, want unchanged at %d", got, before)
		}
		group.vector.Add(BehaviorEvent{ID: "flush-safe", Outcome: "ok", FinalDestination: "publish.flush"})
	})

	group.Check("partial publish reports only the missing message", func(t *testing.T) {
		producer := newProducer(t, group, "publish.partial", driver.ProducerConfig{RequireDurableAck: true, Effective: group.effective})
		err := producer.Publish(group.ctx,
			driver.OutboundMessage{Destination: "publish.partial", Body: []byte("accepted")},
			driver.OutboundMessage{Destination: "publish.missing", Body: []byte("rejected")},
		)
		var publishErr *driver.PublishError
		if !errors.As(err, &publishErr) {
			t.Fatalf("Publish() error = %v, want *driver.PublishError", err)
		}
		if len(publishErr.Failed) != 1 {
			t.Fatalf("failed messages = %d, want 1", len(publishErr.Failed))
		}
		if _, ok := publishErr.Failed[1]; !ok {
			t.Fatalf("failed indexes = %v, want index 1", publishErr.Failed)
		}
		if got := inspectDestination(t, group, "publish.partial").Ready; got != 1 {
			t.Fatalf("accepted Ready = %d, want 1", got)
		}
		group.vector.Add(BehaviorEvent{ID: "partial-failure", Outcome: "partial", FinalDestination: "publish.partial"})
	})

	group.Check("publishing only to a missing destination is classified", func(t *testing.T) {
		producer := newProducer(t, group, "publish.missing-only", driver.ProducerConfig{RequireDurableAck: true, Effective: group.effective})
		err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "publish.no-such-destination"})
		if err == nil {
			t.Fatal("Publish() error = nil, want missing-destination error")
		}
		if !errors.Is(err, driver.ErrDestinationMissing) {
			t.Fatalf("Publish() error = %v, want ErrDestinationMissing", err)
		}
		if kind, classified := driver.Classify(err); !classified || kind != driver.KindNotFound {
			t.Fatalf("Publish() classification = (%v, %t), want (not found, true)", kind, classified)
		}
		group.vector.Add(BehaviorEvent{ID: "missing-destination", Outcome: "not_found", FinalDestination: "publish.no-such-destination"})
	})

	group.Check("total publish failure reports every message", func(t *testing.T) {
		producer := newProducer(t, group, "publish.total", driver.ProducerConfig{RequireDurableAck: true, Effective: group.effective})
		err := producer.Publish(group.ctx,
			driver.OutboundMessage{Destination: "publish.total.missing-a"},
			driver.OutboundMessage{Destination: "publish.total.missing-b"},
		)
		var publishErr *driver.PublishError
		if !errors.As(err, &publishErr) {
			t.Fatalf("Publish() error = %v, want *driver.PublishError", err)
		}
		if len(publishErr.Failed) != 2 {
			t.Fatalf("failed messages = %d, want 2", len(publishErr.Failed))
		}
		for index, failure := range publishErr.Failed {
			if !errors.Is(failure, driver.ErrDestinationMissing) {
				t.Fatalf("failed[%d] = %v, want ErrDestinationMissing", index, failure)
			}
			if kind, classified := driver.Classify(failure); !classified || kind != driver.KindNotFound {
				t.Fatalf("failed[%d] classification = (%v, %t), want (not found, true)", index, kind, classified)
			}
		}
		group.vector.Add(BehaviorEvent{ID: "total-failure", Outcome: "failed", FinalDestination: "publish.total"})
	})

	group.Check("empty publish is a no-op", func(t *testing.T) {
		producer := newProducer(t, group, "publish.empty", driver.ProducerConfig{RequireDurableAck: true, Effective: group.effective})
		if err := producer.Publish(group.ctx); err != nil {
			t.Fatalf("empty Publish() error = %v", err)
		}
		if got := inspectDestination(t, group, "publish.empty").Ready; got != 0 {
			t.Fatalf("Ready after empty Publish = %d, want 0", got)
		}
		group.vector.Add(BehaviorEvent{ID: "empty-publish", Outcome: "ok", FinalDestination: "publish.empty"})
	})

	group.Check("cancelled context prevents publishing", func(t *testing.T) {
		producer := newProducer(t, group, "publish.cancelled", driver.ProducerConfig{RequireDurableAck: true, Effective: group.effective})
		ctx, cancel := context.WithCancel(group.ctx)
		cancel()
		err := producer.Publish(ctx, driver.OutboundMessage{Destination: "publish.cancelled"})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Publish() error = %v, want context.Canceled", err)
		}
		if kind, classified := driver.Classify(err); !classified || kind != driver.KindTransient {
			t.Fatalf("Publish() classification = (%v, %t), want (transient, true)", kind, classified)
		}
		if got := inspectDestination(t, group, "publish.cancelled").Ready; got != 0 {
			t.Fatalf("Ready after cancelled Publish = %d, want 0", got)
		}
		group.vector.Add(BehaviorEvent{ID: "cancelled", Outcome: "cancelled", FinalDestination: "publish.cancelled"})
	})

	group.Check("body bytes survive publish", func(t *testing.T) {
		producer := newProducer(t, group, "publish.body", driver.ProducerConfig{RequireDurableAck: true, Effective: group.effective})
		body := []byte{0, 1, 2, 255}
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "publish.body", Body: body}); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		consumer := newConsumer(t, group, "publish.body", 2)
		message := receiveMessage(t, group, consumer)
		if string(message.Body) != string(body) {
			t.Fatalf("Body = %v, want %v", message.Body, body)
		}
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "body-fidelity", Outcome: "ok", FinalDestination: "publish.body"})
	})

	group.Check("headers survive publish", func(t *testing.T) {
		producer := newProducer(t, group, "publish.headers", driver.ProducerConfig{RequireDurableAck: true, Effective: group.effective})
		headers := []driver.Header{{Key: "x-trace", Value: []byte("trace-1")}, {Key: "x-empty", Value: nil}}
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "publish.headers", Headers: headers}); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		consumer := newConsumer(t, group, "publish.headers", 2)
		message := receiveMessage(t, group, consumer)
		if len(message.Headers) != len(headers) {
			t.Fatalf("Headers = %#v, want %d headers", message.Headers, len(headers))
		}
		trace := headerByKey(t, message.Headers, "x-trace")
		empty := headerByKey(t, message.Headers, "x-empty")
		if string(trace.Value) != "trace-1" {
			t.Fatalf("x-trace value = %q, want %q", trace.Value, "trace-1")
		}
		if len(empty.Value) != 0 {
			t.Fatalf("x-empty value length = %d, want 0", len(empty.Value))
		}
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "header-fidelity", Outcome: "ok", FinalDestination: "publish.headers"})
	})

	group.Check("routing key survives publish", func(t *testing.T) {
		producer := newProducer(t, group, "publish.key", driver.ProducerConfig{RequireDurableAck: true, Effective: group.effective})
		key := []byte("order-42")
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "publish.key", Key: key}); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		consumer := newConsumer(t, group, "publish.key", 2)
		message := receiveMessage(t, group, consumer)
		if string(message.Key) != string(key) {
			t.Fatalf("Key = %q, want %q", message.Key, key)
		}
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "key-fidelity", Outcome: "ok", FinalDestination: "publish.key"})
	})

	group.Check("message and header inputs are not mutated by publish", func(t *testing.T) {
		producer := newProducer(t, group, "publish.clone", driver.ProducerConfig{RequireDurableAck: true, Effective: group.effective})
		message := driver.OutboundMessage{
			Destination: "publish.clone",
			Key:         []byte("original-key"),
			Headers:     []driver.Header{{Key: "x", Value: []byte("original-value")}},
			Body:        []byte("original-body"),
		}
		want := fmt.Sprintf("%s|%s|%s", message.Key, message.Headers[0].Value, message.Body)
		if err := producer.Publish(group.ctx, message); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		message.Key[0] = 'X'
		message.Headers[0].Value[0] = 'X'
		message.Body[0] = 'X'
		consumer := newConsumer(t, group, "publish.clone", 2)
		got := receiveMessage(t, group, consumer)
		actual := fmt.Sprintf("%s|%s|%s", got.Key, got.Headers[0].Value, got.Body)
		if actual != want {
			t.Fatalf("published message = %q, want %q", actual, want)
		}
		ackMessage(t, group, got)
		group.vector.Add(BehaviorEvent{ID: "input-copy", Outcome: "ok", FinalDestination: "publish.clone"})
	})

	group.Check("over-limit publish is rejected", func(t *testing.T) {
		caps := group.effective
		producer := newProducer(t, group, "publish.limits", driver.ProducerConfig{RequireDurableAck: true, Effective: group.effective})
		if caps.MaxMessageBytes > 0 {
			err := producer.Publish(group.ctx, driver.OutboundMessage{
				Destination: "publish.limits",
				Body:        make([]byte, caps.MaxMessageBytes+1),
			})
			assertTooLargePublish(t, err, 0)
			group.vector.Add(BehaviorEvent{ID: "message-too-large", Outcome: "too_large", FinalDestination: "publish.limits"})
		}
		if caps.MaxHeaderBytes > 0 {
			err := producer.Publish(group.ctx, driver.OutboundMessage{
				Destination: "publish.limits",
				Headers:     []driver.Header{{Key: "x-over-limit", Value: make([]byte, caps.MaxHeaderBytes+1)}},
			})
			assertTooLargePublish(t, err, 0)
			group.vector.Add(BehaviorEvent{ID: "headers-too-large", Outcome: "too_large", FinalDestination: "publish.limits"})
		}
		if got := inspectDestination(t, group, "publish.limits").Ready; got != 0 {
			t.Fatalf("Ready after rejected publishes = %d, want 0", got)
		}
	})
	group.Check("concurrent publishes are all confirmed", func(t *testing.T) {
		producer := newProducer(t, group, "publish.concurrent", driver.ProducerConfig{RequireDurableAck: true, Effective: group.effective})
		const count = 8
		var wg sync.WaitGroup
		start := make(chan struct{})
		ready := make(chan struct{}, count)
		errs := make(chan error, count)
		for i := 0; i < count; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				ready <- struct{}{}
				<-start
				errs <- producer.Publish(group.ctx, driver.OutboundMessage{
					Destination: "publish.concurrent",
					Body:        []byte(fmt.Sprintf("message-%d", i)),
				})
			}()
		}
		for i := 0; i < count; i++ {
			<-ready
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent Publish() error = %v", err)
			}
		}
		if got := inspectDestination(t, group, "publish.concurrent").Ready; got != count {
			t.Fatalf("Ready = %d, want %d", got, count)
		}
		group.vector.Add(BehaviorEvent{ID: "concurrent-publish", Outcome: "ok", FinalDestination: "publish.concurrent"})
	})

}

func assertTooLargePublish(t *testing.T, err error, index int) {
	t.Helper()
	var publishErr *driver.PublishError
	if !errors.As(err, &publishErr) {
		t.Fatalf("Publish() error = %v, want *driver.PublishError", err)
	}
	failure, ok := publishErr.Failed[index]
	if !ok {
		t.Fatalf("failed indexes = %v, want index %d", publishErr.Failed, index)
	}
	if kind, classified := driver.Classify(failure); !classified || kind != driver.KindTooLarge {
		t.Fatalf("failed message classification = (%v, %t), want (too large, true)", kind, classified)
	}
}
