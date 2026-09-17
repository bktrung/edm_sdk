package conformance

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

const drainPromptTimeout = time.Second

func init() {
	registerGroup("drain", runDrain)
}

func runDrain(group *groupContext) {
	group.Check("drain returns without waiting for outstanding work", func(t *testing.T) {
		producer := newProducer(t, group, profileDestination(group, "drain.prompt"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "drain.prompt"), 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "drain.prompt"}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		done := make(chan error, 1)
		go func() { done <- consumer.Drain(group.ctx) }()
		deadline, cancel := context.WithTimeout(group.ctx, drainPromptTimeout)
		defer cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Drain() error = %v", err)
			}
		case <-deadline.Done():
			ackMessage(t, group, message)
			t.Fatal("Drain() waited for outstanding work")
		}
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "drain-prompt", Outcome: "ok", FinalDestination: "drain.prompt"})
	})

	group.Check("drained consumer receives no new messages", func(t *testing.T) {
		producer := newProducer(t, group, profileDestination(group, "drain.no-new"), driver.ProducerConfig{Effective: group.effective})
		drained := newConsumer(t, group, profileDestination(group, "drain.no-new"), 1)
		if err := drained.Drain(group.ctx); err != nil {
			t.Fatalf("Drain() error = %v", err)
		}
		survivor := newConsumer(t, group, profileDestination(group, "drain.no-new"), 1)
		publishCount(t, group, producer, "drain.no-new", 4)
		ackAll(t, group, survivor, 4)
		waitForStable(t, group, "drained consumer to stay empty while survivor receives", func() (bool, string) {
			select {
			case message, ok := <-drained.Messages():
				if !ok {
					return false, "Messages channel closed"
				}
				return false, fmt.Sprintf("received %q", message.Destination)
			default:
				return true, "no delivery to drained consumer"
			}
		})
		group.vector.Add(BehaviorEvent{ID: "drain-no-new", Outcome: "withheld", FinalDestination: "drain.no-new"})
	})

	group.Check("outstanding delivery remains settleable after drain", func(t *testing.T) {
		producer := newProducer(t, group, profileDestination(group, "drain.settleable"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "drain.settleable"), 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "drain.settleable"}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		if err := consumer.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "drain-settleable", Outcome: "acked", FinalDestination: "drain.settleable"})
	})

	group.Check("drain leaves Messages open", func(t *testing.T) {
		producer := newProducer(t, group, profileDestination(group, "drain.messages-open"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "drain.messages-open"), 1)
		if err := consumer.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		survivor := newConsumer(t, group, profileDestination(group, "drain.messages-open"), 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "drain.messages-open", Body: []byte("control")}); err != nil {
			t.Fatal(err)
		}
		ackMessage(t, group, receiveMessage(t, group, survivor))
		waitForStable(t, group, "drained Messages channel to remain open", func() (bool, string) {
			select {
			case message, ok := <-consumer.Messages():
				if !ok {
					return false, "Messages channel closed"
				}
				return false, fmt.Sprintf("received %q", message.Destination)
			default:
				return true, "Messages channel open without delivery"
			}
		})
		group.vector.Add(BehaviorEvent{ID: "drain-messages-open", Outcome: "open", FinalDestination: "drain.messages-open"})
	})

	group.Check("settlement clears broker unsettled count after drain", func(t *testing.T) {
		producer := newProducer(t, group, profileDestination(group, "drain.unsettled"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "drain.unsettled"), 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "drain.unsettled"}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		if err := consumer.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		waitFor(t, group, "drain to retain the in-flight broker delivery", func() (bool, string) {
			view := inspectDestination(t, group, "drain.unsettled")
			return view.Unsettled == 1, fmt.Sprintf("view=%+v", view)
		})
		ackMessage(t, group, message)
		waitFor(t, group, "drained settlement to clear broker unsettled count", func() (bool, string) {
			view := inspectDestination(t, group, "drain.unsettled")
			return view.Unsettled == 0, fmt.Sprintf("view=%+v", view)
		})
		group.vector.Add(BehaviorEvent{ID: "drain-unsettled", Outcome: "cleared", FinalDestination: "drain.unsettled"})
	})

	group.Check("drain cycle accounts for every published message", func(t *testing.T) {
		producer := newProducer(t, group, profileDestination(group, "drain.accounting"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "drain.accounting"), 1)
		if err := producer.Publish(group.ctx,
			driver.OutboundMessage{Destination: "drain.accounting", Body: []byte("in-flight")},
			driver.OutboundMessage{Destination: "drain.accounting", Body: []byte("ready")},
		); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		if err := consumer.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		ackMessage(t, group, message)
		waitFor(t, group, "drain cycle to account for settled and ready messages", func() (bool, string) {
			view := inspectDestination(t, group, "drain.accounting")
			return view.Ready == 1 && view.Unsettled == 0, fmt.Sprintf("view=%+v", view)
		})
		group.vector.Add(BehaviorEvent{ID: "drain-accounting", Outcome: "lossless", FinalDestination: "drain.accounting"})
	})

	group.Check("stop refuses outstanding work as a fatal error", func(t *testing.T) {
		producer := newProducer(t, group, profileDestination(group, "drain.refusal"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "drain.refusal"), 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "drain.refusal"}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		err := consumer.Stop(group.ctx)
		if err == nil || errors.Is(err, driver.ErrDrainTimeout) {
			t.Fatalf("Stop() error = %v, want fatal refusal distinct from ErrDrainTimeout", err)
		}
		if kind, classified := driver.Classify(err); !classified || kind != driver.KindFatal {
			t.Fatalf("Stop() classification = (%v, %t), want (fatal, true)", kind, classified)
		}
		if !strings.Contains(err.Error(), "stop") {
			t.Fatalf("Stop() error = %v, want operation name", err)
		}
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "drain-stop-refusal", Outcome: "fatal", FinalDestination: "drain.refusal"})
	})

	group.Check("expired stop deadline returns drain timeout", func(t *testing.T) {
		_ = newProducer(t, group, profileDestination(group, "drain.timeout"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "drain.timeout"), 1)
		// inmem has no asynchronous flush window. An already-expired deadline
		// is a fixture-only probe of ErrDrainTimeout identity; real drivers must
		// prove expiration while their final flush is in progress.
		ctx, cancel := context.WithTimeout(group.ctx, 0)
		defer cancel()
		<-ctx.Done()
		err := consumer.Stop(ctx)
		if !errors.Is(err, driver.ErrDrainTimeout) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Stop() error = %v, want ErrDrainTimeout and context deadline", err)
		}
		if kind, classified := driver.Classify(err); !classified || kind != driver.KindTransient {
			t.Fatalf("Stop() classification = (%v, %t), want (transient, true)", kind, classified)
		}
		group.vector.Add(BehaviorEvent{ID: "drain-timeout", Outcome: "timeout", FinalDestination: "drain.timeout"})
	})

	group.Check("stop refusal and timeout remain distinguishable", func(t *testing.T) {
		producer := newProducer(t, group, profileDestination(group, "drain.distinguishable"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "drain.distinguishable"), 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "drain.distinguishable"}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		refusal := consumer.Stop(group.ctx)
		ctx, cancel := context.WithTimeout(group.ctx, 0)
		defer cancel()
		<-ctx.Done()
		timeout := consumer.Stop(ctx)
		if errors.Is(refusal, driver.ErrDrainTimeout) || !errors.Is(timeout, driver.ErrDrainTimeout) {
			t.Fatalf("Stop() errors refusal=%v timeout=%v, want distinct timeout sentinel", refusal, timeout)
		}
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "drain-errors-distinct", Outcome: "distinct", FinalDestination: "drain.distinguishable"})
	})

	group.Check("drain is idempotent", func(t *testing.T) {
		_ = newProducer(t, group, profileDestination(group, "drain.idempotent"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "drain.idempotent"), 1)
		if err := consumer.Drain(group.ctx); err != nil {
			t.Fatalf("first Drain() error = %v", err)
		}
		if err := consumer.Drain(group.ctx); err != nil {
			t.Fatalf("second Drain() error = %v", err)
		}
		group.vector.Add(BehaviorEvent{ID: "drain-idempotent", Outcome: "ok", FinalDestination: "drain.idempotent"})
	})

	group.Check("stop is idempotent", func(t *testing.T) {
		_ = newProducer(t, group, profileDestination(group, "drain.stop-idempotent"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "drain.stop-idempotent"), 1)
		if err := consumer.Stop(group.ctx); err != nil {
			t.Fatalf("first Stop() error = %v", err)
		}
		if err := consumer.Stop(group.ctx); err != nil {
			t.Fatalf("second Stop() error = %v", err)
		}
		group.vector.Add(BehaviorEvent{ID: "drain-stop-idempotent", Outcome: "ok", FinalDestination: "drain.stop-idempotent"})
	})

	group.Check("stop completes a settled drain cycle", func(t *testing.T) {
		producer := newProducer(t, group, profileDestination(group, "drain.stop"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "drain.stop"), 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "drain.stop"}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		if err := consumer.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		ackMessage(t, group, message)
		if err := consumer.Stop(group.ctx); err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
		group.vector.Add(BehaviorEvent{ID: "drain-stop", Outcome: "stopped", FinalDestination: "drain.stop"})
	})

	group.Check("successful stop closes Messages", func(t *testing.T) {
		_ = newProducer(t, group, profileDestination(group, "drain.messages-closed"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "drain.messages-closed"), 1)
		if err := consumer.Stop(group.ctx); err != nil {
			t.Fatal(err)
		}
		waitFor(t, group, "Messages to close after Stop", func() (bool, string) {
			select {
			case _, ok := <-consumer.Messages():
				return !ok, "Messages channel remains open"
			default:
				return false, "Messages channel remains open"
			}
		})
		group.vector.Add(BehaviorEvent{ID: "drain-messages-closed", Outcome: "closed", FinalDestination: "drain.messages-closed"})
	})

	group.Check("release redelivers outstanding work", func(t *testing.T) {
		producer := newProducer(t, group, profileDestination(group, "drain.release"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "drain.release"), 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "drain.release", Body: []byte("release")}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		if err := consumer.Release(group.ctx); err != nil {
			t.Fatalf("Release() error = %v", err)
		}
		receiver := newConsumer(t, group, profileDestination(group, "drain.release"), 1)
		redelivered := receiveMessage(t, group, receiver)
		if string(redelivered.Body) != string(message.Body) {
			t.Fatalf("redelivered body = %q, want %q", redelivered.Body, message.Body)
		}
		ackMessage(t, group, redelivered)
		group.vector.Add(BehaviorEvent{ID: "drain-release-redelivery", Outcome: "redelivered", FinalDestination: "drain.release"})

		// An acknowledged message must not carry an outstanding earlier one
		// away with it. Release hands back every delivery it did not settle,
		// so the group has to be able to receive the earlier body again after
		// rejoining. The acknowledged body is allowed to arrive a second time:
		// a driver that commits contiguously cannot commit past the hole the
		// earlier body leaves, and at-least-once delivery permits the repeat.
		ordered := "drain.release-after-ack"
		orderedProducer := newPlacedProducer(t, group, ordered, driver.ProducerConfig{Effective: group.effective})
		orderedGroup := "drain-release-after-ack-" + group.runID + "-" + group.profile.String()
		holder := newConsumerFor(t, group, driver.ConsumerConfig{
			Group: orderedGroup, Destinations: []string{ordered}, Prefetch: 2,
			StartAt: driver.StartEarliest, Effective: group.effective,
		})
		if err := orderedProducer.Publish(group.ctx,
			driver.OutboundMessage{Destination: ordered, Key: []byte(placementKey(0)), Body: []byte("release-unsettled")},
			driver.OutboundMessage{Destination: ordered, Key: []byte(placementKey(1)), Body: []byte("release-acknowledged")},
		); err != nil {
			t.Fatalf("Publish(release after a later ack) error = %v", err)
		}
		outstanding := receiveMessage(t, group, holder)
		later := receiveMessage(t, group, holder)
		ackMessage(t, group, later)
		if err := holder.Release(group.ctx); err != nil {
			t.Fatalf("Release() after a later ack error = %v", err)
		}
		rejoiner := newConsumerFor(t, group, driver.ConsumerConfig{
			Group: orderedGroup, Destinations: []string{ordered}, Prefetch: 1,
			StartAt: driver.StartEarliest, Effective: group.effective,
		})
		returned := false
		for range 2 {
			candidate := receiveMessage(t, group, rejoiner)
			ackMessage(t, group, candidate)
			if string(candidate.Body) == string(outstanding.Body) {
				returned = true
				break
			}
		}
		if !returned {
			t.Fatalf("release did not return the unsettled body %q to the group", outstanding.Body)
		}
		group.vector.Add(BehaviorEvent{ID: "drain-release-after-ack", Outcome: "redelivered", FinalDestination: ordered})
	})

	group.Check("release with no outstanding work closes Messages", func(t *testing.T) {
		_ = newProducer(t, group, profileDestination(group, "drain.release-closed"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "drain.release-closed"), 1)
		if err := consumer.Release(group.ctx); err != nil {
			t.Fatalf("Release() error = %v", err)
		}
		waitFor(t, group, "Messages to close after Release", func() (bool, string) {
			select {
			case _, ok := <-consumer.Messages():
				return !ok, "Messages channel remains open"
			default:
				return false, "Messages channel remains open"
			}
		})
		group.vector.Add(BehaviorEvent{ID: "drain-release-closed", Outcome: "closed", FinalDestination: "drain.release-closed"})
	})

	group.Check("release is idempotent", func(t *testing.T) {
		_ = newProducer(t, group, profileDestination(group, "drain.release-idempotent"), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, "drain.release-idempotent"), 1)
		if err := consumer.Release(group.ctx); err != nil {
			t.Fatalf("first Release() error = %v", err)
		}
		if err := consumer.Release(group.ctx); err != nil {
			t.Fatalf("second Release() error = %v", err)
		}
		if err := consumer.Stop(group.ctx); err != nil {
			t.Fatalf("Stop() after Release error = %v", err)
		}
		stopped := newConsumer(t, group, profileDestination(group, "drain.release-idempotent"), 1)
		if err := stopped.Stop(group.ctx); err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
		if err := stopped.Release(group.ctx); err != nil {
			t.Fatalf("Release() after Stop error = %v", err)
		}
		group.vector.Add(BehaviorEvent{ID: "drain-release-idempotent", Outcome: "ok", FinalDestination: "drain.release-idempotent"})
	})
}
