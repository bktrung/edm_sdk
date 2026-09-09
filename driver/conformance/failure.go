package conformance

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

const privateCloseDestination = "failure.close.private"

func init() { registerGroup("failure", runFailure) }

func runFailure(group *groupContext) {
	group.Check("cancelled Driver Open returns transient without a connection", func(t *testing.T) {
		ctx, cancel := context.WithCancel(group.ctx)
		cancel()
		conn, err := group.drv.Open(ctx, group.cfg)
		assertCancelled(t, "Driver.Open", err)
		if conn != nil {
			_ = conn.Close(group.ctx)
			t.Fatal("Driver.Open() returned a connection after cancellation")
		}
		group.vector.Add(BehaviorEvent{ID: "cancel-open", Outcome: "cancelled", FinalDestination: "driver"})
	})

	group.Check("cancelled Ping returns transient", func(t *testing.T) {
		ctx, cancel := context.WithCancel(group.ctx)
		cancel()
		assertCancelled(t, "Ping", group.conn.Ping(ctx))
		group.vector.Add(BehaviorEvent{ID: "cancel-ping", Outcome: "cancelled", FinalDestination: "connection"})
	})

	group.Check("cancelled Conn Close returns transient and leaves admission open", func(t *testing.T) {
		conn, err := group.drv.Open(group.ctx, group.cfg)
		if err != nil {
			t.Fatalf("Driver.Open(): %v", err)
		}
		defer func() { _ = conn.Close(group.ctx) }()
		ctx, cancel := context.WithCancel(group.ctx)
		cancel()
		assertCancelled(t, "Conn.Close", conn.Close(ctx))
		producer, err := conn.Producer(group.ctx, driver.ProducerConfig{})
		if err != nil {
			t.Fatalf("Producer() after cancelled Conn.Close() = %v", err)
		}
		if err := producer.Close(group.ctx); err != nil {
			t.Fatalf("Producer.Close() after cancelled Conn.Close() = %v", err)
		}
		if err := conn.Close(group.ctx); err != nil {
			t.Fatalf("Conn.Close() cleanup = %v", err)
		}
		group.vector.Add(BehaviorEvent{ID: "cancel-conn-close", Outcome: "cancelled", FinalDestination: "connection"})
	})

	group.Check("failed Close keeps admission closed and remains retryable", func(t *testing.T) {
		conn, inject := newPrivateFailureConnection(t, group)
		assertPrivateCloseFailure(t, group, conn, inject, profileDestination(group, privateCloseDestination), 100*time.Millisecond, group.ctx)

		conn, inject = newPrivateFailureConnection(t, group)
		assertPrivateCloseFailure(t, group, conn, inject, profileDestination(group, privateCloseDestination), 0, group.ctx)
		group.vector.Add(BehaviorEvent{ID: "failure-close-admission", Outcome: "closed-and-retryable", FinalDestination: "failure.close"})
	})

	group.Check("rejected Close leaves admission open", func(t *testing.T) {
		conn, _ := newPrivateFailureConnection(t, group)
		admin := conn.Admin()
		destination := profileDestination(group, privateCloseDestination)
		if _, err := admin.EnsureTopology(group.ctx, profileTopologySpec(group, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: destination}},
			Scope:        []string{"failure.close."},
			Effective:    group.effective,
		})); err != nil {
			t.Fatalf("declare precondition destination: %v", err)
		}
		producer, err := conn.Producer(group.ctx, driver.ProducerConfig{})
		if err != nil {
			t.Fatalf("Producer() before precondition Close: %v", err)
		}
		if err := conn.Close(group.ctx); !errors.Is(err, driver.ErrResourcesOutstanding) {
			t.Fatalf("Close() with an open producer = %v, want ErrResourcesOutstanding", err)
		}
		if next, err := conn.Producer(group.ctx, driver.ProducerConfig{}); err != nil {
			t.Fatalf("Producer() after rejected Close: %v", err)
		} else {
			_ = next.Close(group.ctx)
		}
		if _, err := admin.DescribeTopology(group.ctx, []string{destination}); err != nil {
			t.Fatalf("Admin.DescribeTopology() after rejected Close: %v", err)
		}
		if err := producer.Close(group.ctx); err != nil {
			t.Fatalf("close precondition producer: %v", err)
		}
		if err := conn.Close(group.ctx); err != nil {
			t.Fatalf("Close() after resources closed: %v", err)
		}
		group.vector.Add(BehaviorEvent{ID: "failure-close-precondition", Outcome: "admission-open", FinalDestination: "failure.close"})
	})

	group.Check("transient publish failure is classified", func(t *testing.T) {
		name := "failure.publish.transient"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		injectFailure(t, group, FaultPublishFailure)
		err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name})
		assertFaultError(t, err, driver.KindTransient)
		group.vector.Add(BehaviorEvent{ID: "failure-publish-transient", Outcome: "transient", FinalDestination: name})
	})

	group.Check("publish recovers after a transient outage", func(t *testing.T) {
		name := "failure.publish.recovery"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
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
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
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
		_ = newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
		injectFailure(t, group, FaultConnectionDrop)
		err := receiveFailureError(t, group, consumer)
		assertFaultError(t, err, driver.KindTransient)
		group.vector.Add(BehaviorEvent{ID: "failure-errors-classified", Outcome: "transient", FinalDestination: name})
	})

	group.Check("transient error does not close Errors", func(t *testing.T) {
		name := "failure.errors.open"
		_ = newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
		injectFailure(t, group, FaultConnectionDrop)
		_ = receiveFailureError(t, group, consumer)
		injectFailure(t, group, FaultConnectionDrop)
		second := receiveFailureError(t, group, consumer)
		assertFaultError(t, second, driver.KindTransient)
		group.vector.Add(BehaviorEvent{ID: "failure-errors-open", Outcome: "open", FinalDestination: name})
	})

	group.Check("transient error does not close Messages", func(t *testing.T) {
		name := "failure.messages.open"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
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
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
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
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
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
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
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
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
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
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name}); err != nil {
			t.Fatal(err)
		}
		first := receiveMessage(t, group, consumer)
		injectFailure(t, group, FaultConnectionDrop)
		err := first.Settle.Ack(group.ctx)
		assertFaultError(t, err, driver.KindFatal)
		ackMessage(t, group, receiveMessage(t, group, consumer))
		group.vector.Add(BehaviorEvent{ID: "failure-settlement-fatal", Outcome: "fatal", FinalDestination: name})
	})

	group.Check("recovered delivery remains settleable", func(t *testing.T) {
		name := "failure.settlement.recovered"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
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
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		injectFailure(t, group, FaultFatalPublish)
		err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name})
		assertFaultError(t, err, driver.KindFatal)
		group.vector.Add(BehaviorEvent{ID: "failure-publish-fatal", Outcome: "fatal", FinalDestination: name})
	})

	group.Check("stale settlement preserves its sentinel", func(t *testing.T) {
		name := "failure.settlement.sentinel"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
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

	group.Check("lane channel closure reports a transient error", func(t *testing.T) {
		producer, consumer, first, second := newLaneCloseFixture(t, group, "errors")
		primeLaneClose(t, group, producer, consumer, first, second)
		injectFailure(t, group, FaultLaneChannelClose)
		err := receiveFailureError(t, group, consumer)
		assertFaultError(t, err, driver.KindTransient)
		assertLaneClosed(t, group, producer, consumer, first)
		group.vector.Add(BehaviorEvent{ID: "failure-lane-close-errors", Outcome: "transient", FinalDestination: first})
	})

	group.Check("lane channel closure leaves Messages open", func(t *testing.T) {
		producer, consumer, first, second := newLaneCloseFixture(t, group, "messages")
		primeLaneClose(t, group, producer, consumer, first, second)
		injectFailure(t, group, FaultLaneChannelClose)
		err := receiveFailureError(t, group, consumer)
		assertFaultError(t, err, driver.KindTransient)
		assertLaneClosed(t, group, producer, consumer, first)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: second, Body: []byte("survivor")}); err != nil {
			t.Fatalf("surviving lane publish: %v", err)
		}
		message := receiveMessage(t, group, consumer)
		if message.Destination != second {
			t.Fatalf("surviving lane destination=%q, want %q", message.Destination, second)
		}
		ackMessage(t, group, message)
		assertNoDelivery(t, group, consumer, "lane close Messages after surviving delivery")
		group.vector.Add(BehaviorEvent{ID: "failure-lane-close-messages", Outcome: "open-survivor", FinalDestination: second})
	})

	group.Check("lane channel closure permits clean Stop", func(t *testing.T) {
		producer, consumer, first, second := newLaneCloseFixture(t, group, "stop")
		primeLaneClose(t, group, producer, consumer, first, second)
		injectFailure(t, group, FaultLaneChannelClose)
		err := receiveFailureError(t, group, consumer)
		assertFaultError(t, err, driver.KindTransient)
		assertLaneClosed(t, group, producer, consumer, first)
		if err := consumer.Stop(group.ctx); err != nil {
			t.Fatalf("lane close Stop: %v", err)
		}
		if _, ok := <-consumer.Messages(); ok {
			t.Fatal("lane close Messages remained open after Stop")
		}
		if _, ok := <-consumer.Errors(); ok {
			t.Fatal("lane close Errors remained open after Stop")
		}
		group.vector.Add(BehaviorEvent{ID: "failure-lane-close-stop", Outcome: "stopped", FinalDestination: first})
	})
}

func assertPrivateCloseFailure(t *testing.T, group *groupContext, conn driver.Conn, inject FaultInjector, destination string, closeTimeout time.Duration, activeCtx context.Context) {
	t.Helper()
	admin := conn.Admin()
	if _, err := admin.EnsureTopology(activeCtx, profileTopologySpec(group, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination}},
		Scope:        []string{"failure.close."},
		Effective:    group.effective,
	})); err != nil {
		t.Fatalf("declare close destination: %v", err)
	}
	if err := inject(activeCtx, FaultCloseFailure); err != nil {
		t.Fatalf("inject %s: %v", FaultCloseFailure, err)
	}
	closeCtx := activeCtx
	if closeTimeout > 0 {
		var cancel context.CancelFunc
		closeCtx, cancel = context.WithTimeout(activeCtx, closeTimeout)
		defer cancel()
	}
	if err := conn.Close(closeCtx); err == nil {
		t.Fatal("Close() after injected failure returned nil")
	}
	if producer, err := conn.Producer(activeCtx, driver.ProducerConfig{}); err == nil {
		_ = producer.Close(activeCtx)
		t.Fatal("Producer() succeeded after Close teardown began")
	}
	if _, err := admin.DescribeTopology(activeCtx, []string{destination}); err == nil {
		t.Error("Admin.DescribeTopology() succeeded after Close teardown began")
	}
	if consumer, err := conn.Consumer(activeCtx, driver.ConsumerConfig{
		Destinations: []string{destination},
	}); err == nil {
		_ = consumer.Stop(activeCtx)
		t.Fatal("Consumer() succeeded after Close teardown began")
	}
	if err := conn.Close(activeCtx); err != nil {
		t.Fatalf("retry Close() = %v", err)
	}
}

func newPrivateFailureConnection(t *testing.T, group *groupContext) (driver.Conn, FaultInjector) {
	t.Helper()
	if group.drv == nil || group.injectFactory == nil {
		t.Fatal("private failure connection requires driver and fault injector factory")
	}
	conn, err := group.drv.Open(group.ctx, group.cfg)
	if err != nil {
		t.Fatalf("open private failure connection: %v", err)
	}
	inject, err := group.injectFactory(conn)
	if err != nil {
		_ = conn.Close(context.Background())
		t.Fatalf("create private fault injector: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(context.Background()); err != nil && !errors.Is(err, driver.ErrResourcesOutstanding) {
			t.Errorf("close private failure connection: %v", err)
		}
	})
	return conn, inject
}

func assertLaneClosed(t *testing.T, group *groupContext, producer driver.Producer, consumer driver.Consumer, destination string) {
	t.Helper()
	if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: destination, Body: []byte("closed")}); err != nil {
		t.Fatalf("closed lane publish: %v", err)
	}
	assertNoDelivery(t, group, consumer, fmt.Sprintf("closed lane %q after channel closure", destination))
}

func newLaneCloseFixture(t *testing.T, group *groupContext, suffix string) (driver.Producer, driver.Consumer, string, string) {
	t.Helper()
	first := "failure.lane-close." + suffix + ".first"
	second := "failure.lane-close." + suffix + ".second"
	if _, err := group.conn.Admin().EnsureTopology(group.ctx, profileTopologySpec(group, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: first}, {Name: second}},
		Effective:    group.effective,
	})); err != nil {
		t.Fatalf("EnsureTopology lane close: %v", err)
	}
	producer, err := group.conn.Producer(group.ctx, driver.ProducerConfig{Effective: group.effective})
	if err != nil {
		t.Fatalf("Producer lane close: %v", err)
	}
	t.Cleanup(func() {
		if err := producer.Close(group.ctx); err != nil {
			t.Errorf("close lane close producer: %v", err)
		}
	})
	for _, destination := range []string{first, second} {
		t.Cleanup(func() {
			if err := purgeIfSupported(group.ctx, group.conn, profileDestination(group, destination)); err != nil {
				t.Errorf("purge lane close destination %q: %v", destination, err)
			}
		})
	}
	consumer := newConsumerFor(t, group, driver.ConsumerConfig{
		Destinations: []string{first, second},
		Prefetch:     2,
		PerDestination: map[string]int{
			first:  1,
			second: 1,
		},
		Effective: group.effective,
	})
	return &profileProducer{group: group, producer: producer, scoped: true}, consumer, first, second
}

func primeLaneClose(t *testing.T, group *groupContext, producer driver.Producer, consumer driver.Consumer, first, second string) {
	t.Helper()
	if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: first, Body: []byte("first-before")}); err != nil {
		t.Fatalf("lane close first publish: %v", err)
	}
	firstMessage := receiveMessage(t, group, consumer)
	if firstMessage.Destination != first {
		t.Fatalf("first lane destination=%q, want %q", firstMessage.Destination, first)
	}
	ackMessage(t, group, firstMessage)
	if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: second, Body: []byte("second-before")}); err != nil {
		t.Fatalf("lane close second publish: %v", err)
	}
	secondMessage := receiveMessage(t, group, consumer)
	if secondMessage.Destination != second {
		t.Fatalf("second lane destination=%q, want %q", secondMessage.Destination, second)
	}
	ackMessage(t, group, secondMessage)
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

func assertFaultError(t *testing.T, err error, wantKind driver.Kind) {
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
}

func runDeterministicFaultSequence(t *testing.T, group *groupContext, name string) string {
	t.Helper()
	producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
	consumer := newConsumer(t, group, profileDestination(group, name), 1)
	injectFailure(t, group, FaultPublishFailure)
	failed := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("failed")})
	kind, classified := driver.Classify(failed)
	assertFaultError(t, failed, driver.KindTransient)
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
