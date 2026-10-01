package conformance

import (
	"context"
	"fmt"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

const (
	deferredDelay     = 500 * time.Millisecond
	deferredMargin    = 500 * time.Millisecond
	deferredLateBound = deferredDelay*2/5 + deferredMargin

	// publishOrderCount is how many messages the publish-order check publishes, in
	// order, to one destination.
	publishOrderCount = 20

	// publishOrderName is the destination the publish-order check declares, with
	// publishOrderPartitions partitions. The count belongs to the check rather than
	// to the warm-up: a driver owes publish order within a destination's partition,
	// so two partitions would be two ordering units and the assertion would stop
	// meaning what it says.
	publishOrderName       = "deferred.publish-order"
	publishOrderPartitions = 1
)

func init() { registerGroup("deferred", runDeferred) }

// deferredDestinations is the topology the deferred group declares.
func deferredDestinations(group *groupContext) []driver.DestinationSpec {
	destinations := []driver.DestinationSpec{
		{Name: "deferred.bounded-late", Delay: deferredDelay},
		{Name: publishOrderName, Partitions: publishOrderPartitions, Delay: deferredDelay},
		{Name: "deferred.destination-delay", Delay: deferredDelay},
		{Name: "deferred.destination-delay.control"},
		{Name: "deferred.zero"},
		{Name: "deferred.auxiliary", Delay: deferredDelay},
		{Name: "deferred.auxiliary.control"},
		{Name: "deferred.ready", Delay: deferredDelay},
		{Name: "deferred.ready.control"},
		{Name: "deferred.release", Delay: deferredDelay},
		{Name: "deferred.release.control"},
	}
	if group.deadline != nil {
		destinations = append(destinations,
			driver.DestinationSpec{Name: "deferred.deadline", Delay: deferredDelay},
			driver.DestinationSpec{Name: "deferred.deadline.control"},
		)
	}
	return destinations
}

// scopeDelays keys delays - logical destination names, as a check wrote them in
// its topology - by the physical name a record carries. A destination a check
// declares no delay for is left out, which is how the consumer built over it is
// told that destination defers nothing.
func scopeDelays(group *groupContext, delays map[string]time.Duration) map[string]time.Duration {
	if len(delays) == 0 {
		return nil
	}
	scoped := make(map[string]time.Duration, len(delays))
	for destination, delay := range delays {
		if delay > 0 {
			scoped[profileDestination(group, destination)] = delay
		}
	}
	if len(scoped) == 0 {
		return nil
	}
	return scoped
}

// warmDeferredTopology pays Kafka's creation and metadata propagation cost before deferred timing checks.
func warmDeferredTopology(group *groupContext) {
	destinations := deferredDestinations(group)
	warmTopology(group.t, group, driver.TopologySpec{
		Destinations: destinations,
		Effective:    group.effective,
	})
}

func runDeferred(group *groupContext) {
	warmDeferredTopology(group)
	group.Check("deferred delivery is bounded in lateness", func(t *testing.T) {
		name := "deferred.bounded-late"
		producer := newDeferredProducer(t, group, profileDestination(group, name), deferredDelay)
		consumer := deferredConsumer(t, group, []string{name}, map[string]time.Duration{name: deferredDelay}, 1)
		earliestDue := deferredNow(group).Add(deferredDelay)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("bounded")}); err != nil {
			t.Fatal(err)
		}
		// Acceptance can happen anywhere inside Publish. Bracket that instant so
		// publication latency does not spend the driver's lateness allowance.
		latestDue := deferredNow(group).Add(deferredDelay)
		latestDelivery := latestDue.Add(deferredLateBound)
		// Half the bound leaves margin for the ReceivedAt assertion to reject late
		// delivery rather than comparing the fixture's release instant to itself.
		advanceDeferredTo(group, latestDue.Add(deferredLateBound/2))
		message := receiveBefore(t, group, consumer, latestDelivery, "bounded deferred delivery")
		if message.ReceivedAt.Before(earliestDue) || message.ReceivedAt.After(latestDelivery) {
			t.Fatalf("ReceivedAt=%s, want between %s and %s", message.ReceivedAt, earliestDue, latestDelivery)
		}
		ackMessage(t, group, message)
		group.vector.add(BehaviorEvent{ID: "deferred-bounded-late", Outcome: "bounded", FinalDestination: name})
	})

	group.Check("destination delay delivers a destination's messages in publish order", func(t *testing.T) {
		name := publishOrderName
		// Declared here as well as in the group's warm-up, with the same partition
		// count, because the count is what makes this destination one ordering unit.
		producer := newDeferredProducerFor(t, group, driver.DestinationSpec{
			Name: profileDestination(group, name), Partitions: publishOrderPartitions, Delay: deferredDelay,
		})
		consumer := deferredConsumer(t, group, []string{name}, map[string]time.Duration{name: deferredDelay}, publishOrderCount)
		// Every message is due the declared destination delay after its own publish
		// instant, so due times along a destination's partition do not decrease and the
		// order a driver owes equals the order it received. A driver that releases a
		// later message ahead of an earlier one fails here even though every message
		// still arrives at an instant it is entitled to pick.
		for i := range publishOrderCount {
			body := fmt.Sprintf("order-%02d", i)
			if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte(body)}); err != nil {
				t.Fatal(err)
			}
		}
		// The release and the waits below are anchored to the instant after the last
		// publish rather than to one instant read before the loop. A broker clock
		// spends wall time in the loop, and the last message is the one its delay puts
		// furthest out, so a bound taken before the loop would expire while a correct
		// driver was still publishing.
		advanceDeferredTo(group, deferredNow(group).Add(deferredDelay))
		deadline := deferredNow(group).Add(deferredDelay + deferredLateBound)
		for i := range publishOrderCount {
			message := receiveBefore(t, group, consumer, deadline, "publish-order delivery")
			if want := fmt.Sprintf("order-%02d", i); string(message.Body) != want {
				t.Fatalf("delivery %d is body %q, want %q: a destination's messages were not delivered in publish order", i, message.Body, want)
			}
			ackMessage(t, group, message)
		}
		group.vector.add(BehaviorEvent{ID: "deferred-publish-order", Outcome: "in-order", FinalDestination: name})
	})

	group.Check("destination delay holds a message until it is due", func(t *testing.T) {
		name := "deferred.destination-delay"
		control := "deferred.destination-delay.control"
		producer := newDeferredProducer(t, group, profileDestination(group, name), deferredDelay)
		controlProducer := newProducer(t, group, profileDestination(group, control), driver.ProducerConfig{Effective: group.effective})
		consumer := deferredConsumer(t, group, []string{name, control}, map[string]time.Duration{name: deferredDelay}, 2)
		publishedAt := deferredNow(group)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("destination-delay")}); err != nil {
			t.Fatal(err)
		}
		if err := controlProducer.Publish(group.ctx, driver.OutboundMessage{Destination: control, Body: []byte("control")}); err != nil {
			t.Fatal(err)
		}
		controlMessage := receiveMessage(t, group, consumer)
		if controlMessage.Destination != control {
			t.Fatalf("first delivery destination=%q, want control %q", controlMessage.Destination, control)
		}
		ackMessage(t, group, controlMessage)
		due := publishedAt.Add(deferredDelay)
		assertNoDeliveryBefore(t, group, consumer, due, "message before its destination delay")
		advanceDeferredTo(group, due)
		message := receiveBefore(t, group, consumer, due.Add(deferredLateBound), "destination-delay delivery")
		if message.ReceivedAt.Before(due) {
			t.Fatalf("ReceivedAt=%s, want at or after %s", message.ReceivedAt, due)
		}
		ackMessage(t, group, message)
		group.vector.add(BehaviorEvent{ID: "deferred-destination-delay", Outcome: "nominal-delay", FinalDestination: name})
	})

	group.Check("zero destination delay delivers immediately", func(t *testing.T) {
		name := "deferred.zero"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := deferredConsumer(t, group, []string{name}, nil, 1)
		publishedAt := deferredNow(group)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("zero")}); err != nil {
			t.Fatal(err)
		}
		// The deadline bounds a broker suite's wait and a fixture suite never reads it:
		// receiveBefore spends its own waitTimeout on a delivery the fixture clock has
		// already released, so both suites give this delivery the same 5s.
		message := receiveBefore(t, group, consumer, realNow().Add(waitTimeout), "zero-value delivery")
		if string(message.Body) != "zero" {
			t.Fatalf("body=%q, want zero", message.Body)
		}
		if message.ReceivedAt.After(publishedAt.Add(deferredMargin)) {
			t.Fatalf("zero-value delivery at %s, want no later than %s", message.ReceivedAt, publishedAt.Add(deferredMargin))
		}
		ackMessage(t, group, message)
		group.vector.add(BehaviorEvent{ID: "deferred-zero", Outcome: "immediate", FinalDestination: name})
	})

	group.Check("Auxiliary counts not-yet-due messages separately", func(t *testing.T) {
		name := "deferred.auxiliary"
		control := "deferred.auxiliary.control"
		producer := newDeferredProducer(t, group, profileDestination(group, name), deferredDelay)
		controlProducer := newProducer(t, group, profileDestination(group, control), driver.ProducerConfig{Effective: group.effective})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name}); err != nil {
			t.Fatal(err)
		}
		if err := controlProducer.Publish(group.ctx, driver.OutboundMessage{Destination: control}); err != nil {
			t.Fatal(err)
		}
		waitFor(t, group, "deferred Auxiliary and Ready counts", func() (bool, string) {
			view := inspectDestination(t, group, name)
			controlView := inspectDestination(t, group, control)
			return view.Auxiliary == 1 && controlView.Ready == 1,
				fmt.Sprintf("deferred=%+v control=%+v", view, controlView)
		})
		group.vector.add(BehaviorEvent{ID: "deferred-auxiliary", Outcome: "counted", FinalDestination: name})
	})

	group.Check("Ready excludes Auxiliary messages and preserves total depth", func(t *testing.T) {
		name := "deferred.ready"
		control := "deferred.ready.control"
		producer := newDeferredProducer(t, group, profileDestination(group, name), deferredDelay)
		controlProducer := newProducer(t, group, profileDestination(group, control), driver.ProducerConfig{Effective: group.effective})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name}); err != nil {
			t.Fatal(err)
		}
		if err := controlProducer.Publish(group.ctx, driver.OutboundMessage{Destination: control}); err != nil {
			t.Fatal(err)
		}
		waitFor(t, group, "deferred Ready excludes Auxiliary", func() (bool, string) {
			view := inspectDestination(t, group, name)
			controlView := inspectDestination(t, group, control)
			return view.Ready == 0 && view.Ready+view.Auxiliary == 1 && controlView.Ready == 1,
				fmt.Sprintf("deferred=%+v control=%+v", view, controlView)
		})
		group.vector.add(BehaviorEvent{ID: "deferred-ready", Outcome: "excludes-auxiliary", FinalDestination: name})
	})

	group.Check("Auxiliary messages become Ready at their due time", func(t *testing.T) {
		name := "deferred.release"
		control := "deferred.release.control"
		producer := newDeferredProducer(t, group, profileDestination(group, name), deferredDelay)
		controlProducer := newProducer(t, group, profileDestination(group, control), driver.ProducerConfig{Effective: group.effective})
		due := deferredNow(group).Add(deferredDelay)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("release")}); err != nil {
			t.Fatal(err)
		}
		if err := controlProducer.Publish(group.ctx, driver.OutboundMessage{Destination: control}); err != nil {
			t.Fatal(err)
		}
		controlView := inspectDestination(t, group, control)
		if controlView.Ready != 1 {
			t.Fatalf("control view=%+v, want one ready message", controlView)
		}
		initial := inspectDestination(t, group, name)
		if initial.Ready != 0 || initial.Auxiliary != 1 {
			t.Fatalf("initial deferred view=%+v, want ready=0 auxiliary=1", initial)
		}
		advanceDeferredTo(group, due)
		waitFor(t, group, "deferred message to become ready", func() (bool, string) {
			view := inspectDestination(t, group, name)
			return view.Ready == 1 && view.Auxiliary == 0 && view.Ready+view.Auxiliary == 1, fmt.Sprintf("view=%+v", view)
		})
		group.vector.add(BehaviorEvent{ID: "deferred-release", Outcome: "ready", FinalDestination: name})
	})

	group.Check("parked delivery does not consume an ack deadline", func(t *testing.T) {
		if group.deadline == nil {
			group.Skip(t, "parked delivery does not consume an ack deadline", "deadline fixture is not configured")
			return
		}
		name := "deferred.deadline"
		control := "deferred.deadline.control"
		producer := newDeferredProducer(t, group, profileDestination(group, name), deferredDelay)
		controlProducer := newProducer(t, group, profileDestination(group, control), driver.ProducerConfig{Effective: group.effective})
		cfg, logical := profileConsumerConfig(group, driver.ConsumerConfig{
			Destinations: []string{name, control}, Prefetch: 2,
			Delays:    scopeDelays(group, map[string]time.Duration{name: deferredDelay}),
			Effective: group.effective,
		})
		rawConsumer, err := group.deadline.Consumer(group.ctx, 10*time.Millisecond, cfg)
		if err != nil {
			t.Fatal(err)
		}
		consumer := newProfileConsumer(group, rawConsumer, logical)
		t.Cleanup(func() {
			if err := consumer.Stop(group.ctx); err != nil {
				t.Errorf("stop deadline consumer: %v", err)
			}
		})
		if err := controlProducer.Publish(group.ctx, driver.OutboundMessage{Destination: control, Body: []byte("control")}); err != nil {
			t.Fatal(err)
		}
		controlMessage := receiveMessage(t, group, consumer)
		if controlMessage.Destination != control {
			t.Fatalf("control delivery destination=%q, want %q", controlMessage.Destination, control)
		}
		group.deadline.Advance(20 * time.Millisecond)
		redelivery := receiveMessage(t, group, consumer)
		if redelivery.Destination != control || string(redelivery.Body) != "control" {
			t.Fatalf("redelivery=%+v, want control message", redelivery)
		}
		if group.effective.NativeDeliveryCount && redelivery.DeliveryCount <= controlMessage.DeliveryCount {
			t.Fatalf("control redelivery count=%d, first=%d", redelivery.DeliveryCount, controlMessage.DeliveryCount)
		}
		ackMessage(t, group, redelivery)
		due := deferredNow(group).Add(deferredDelay)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("parked")}); err != nil {
			t.Fatal(err)
		}
		assertNoDeliveryBefore(t, group, consumer, due, "parked message during ack deadline")
		advanceDeferredTo(group, due)
		message := receiveBefore(t, group, consumer, due.Add(deferredLateBound), "parked delivery after due time")
		if message.Destination != name || message.ReceivedAt.Before(due) {
			t.Fatalf("delivery=%+v, want destination %q at or after %s", message, name, due)
		}
		// DeliveryCount is a zero-based redelivery counter, so one observed
		// delivery must still report zero redeliveries.
		if group.effective.NativeDeliveryCount && message.DeliveryCount != 0 {
			t.Fatalf("parked delivery count=%d, want zero redeliveries for one delivery", message.DeliveryCount)
		}
		ackMessage(t, group, message)
		assertNoDelivery(t, group, consumer, "parked message redelivered after ack deadline")
		group.vector.add(BehaviorEvent{ID: "deferred-deadline", Outcome: "not-consumed", FinalDestination: name})
	})
}

func newDeferredProducer(t *testing.T, group *groupContext, destination string, delay time.Duration) driver.Producer {
	t.Helper()
	return newDeferredProducerFor(t, group, driver.DestinationSpec{Name: destination, Delay: delay})
}

// newDeferredProducerFor is newDeferredProducer for a check that declares its own
// destination spec, because a spec carries more than a name and a delay: the
// publish-order check pins a partition count through it.
func newDeferredProducerFor(t *testing.T, group *groupContext, spec driver.DestinationSpec) driver.Producer {
	t.Helper()
	if _, err := group.conn.Admin().EnsureTopology(group.ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{spec}, Effective: group.effective,
	}); err != nil {
		t.Fatalf("EnsureTopology(%q): %v", spec.Name, err)
	}
	producer, err := group.conn.Producer(group.ctx, driver.ProducerConfig{Effective: group.effective})
	if err != nil {
		t.Fatalf("Producer(%q): %v", spec.Name, err)
	}
	t.Cleanup(func() {
		if err := producer.Close(group.ctx); err != nil {
			t.Errorf("close producer %q: %v", spec.Name, err)
		}
	})
	t.Cleanup(func() {
		if err := purgeIfSupported(group.ctx, group.conn, spec.Name); err != nil {
			t.Errorf("purge destination %q: %v", spec.Name, err)
		}
	})
	return &profileProducer{group: group, producer: producer, scoped: spec.Name != unprofileDestination(group, spec.Name)}
}

// deferredConsumer builds a consumer over destinations, with delays giving the
// delay each deferred destination declares, keyed by the logical name the check
// wrote. A destination absent from delays defers nothing, which is the answer a
// control destination needs rather than a gap in it.
func deferredConsumer(t *testing.T, group *groupContext, destinations []string, delays map[string]time.Duration, prefetch int) driver.Consumer {
	t.Helper()
	scoped, logical := profileDestinations(group, destinations)
	consumer, err := group.conn.Consumer(group.ctx, driver.ConsumerConfig{
		Destinations: scoped, Prefetch: prefetch,
		Delays:    scopeDelays(group, delays),
		Effective: group.effective,
	})
	if err != nil {
		t.Fatalf("Consumer(%v): %v", scoped, err)
	}
	wrapped := newProfileConsumer(group, consumer, logical)
	t.Cleanup(func() {
		if err := wrapped.Stop(group.ctx); err != nil {
			t.Errorf("stop consumer %v: %v", scoped, err)
		}
	})
	return wrapped
}

func deferredNow(group *groupContext) time.Time {
	if group.deadline != nil {
		return group.deadline.Now()
	}
	return realNow()
}

func realNow() time.Time {
	//nolint:forbidigo // real-broker fixtures have no injectable clock.
	return time.Now()
}

func advanceDeferredTo(group *groupContext, target time.Time) {
	if group.deadline == nil {
		return
	}
	if delta := target.Sub(group.deadline.Now()); delta > 0 {
		group.deadline.Advance(delta)
	}
}

func assertNoDelivery(t *testing.T, group *groupContext, consumer driver.Consumer, what string) {
	t.Helper()
	assertNoDeliveryFor(t, group, consumer, stabilityWindow, what)
}

// assertNoDeliveryBefore asserts silence until due, for at most the stability
// window. The window ends at due: a delivery after due is on time, and a slow
// Publish that spends part of the delay must not turn it into an early one.
func assertNoDeliveryBefore(t *testing.T, group *groupContext, consumer driver.Consumer, due time.Time, what string) {
	t.Helper()
	window := min(stabilityWindow, due.Sub(deferredNow(group)))
	if window <= 0 {
		return
	}
	assertNoDeliveryFor(t, group, consumer, window, what)
}

func assertNoDeliveryFor(t *testing.T, group *groupContext, consumer driver.Consumer, window time.Duration, what string) {
	t.Helper()
	waitForStableFor(t, group, what, window, func() (bool, string) {
		select {
		case message, ok := <-consumer.Messages():
			if !ok {
				return false, "Messages channel closed"
			}
			if message.Settle != nil {
				_ = message.Settle.Ack(group.ctx)
			}
			return false, fmt.Sprintf("received destination=%q", message.Destination)
		default:
			return true, "no delivery"
		}
	})
}

func receiveBefore(t *testing.T, group *groupContext, consumer driver.Consumer, deadline time.Time, what string) driver.InboundMessage {
	t.Helper()
	// A broker deadline is wall-clock time, so its remainder is a real budget. A fixture
	// deadline is an instant on the fixture's own clock: the deferred checks advance that
	// clock past the due instant before calling here, so the delivery is already owed and
	// this wait is for wall time to hand it over. waitTimeout is the budget the rest of the
	// harness gives a delivery. time.Until on a fixture instant instead spends wall time
	// the fixture clock never spent, so a deschedule between the caller computing the
	// deadline and this wait starting shortens the budget by the length of the deschedule,
	// to nothing on a loaded machine.
	timeout := waitTimeout
	if group.deadline == nil {
		timeout = time.Until(deadline)
		if timeout <= 0 {
			timeout = time.Millisecond
		}
	}
	ctx, cancel := context.WithTimeout(group.ctx, timeout)
	defer cancel()
	select {
	case message, ok := <-consumer.Messages():
		if !ok {
			t.Fatalf("%s: Messages channel closed", what)
		}
		return message
	case <-ctx.Done():
		t.Fatalf("%s: %v", what, ctx.Err())
		return driver.InboundMessage{}
	}
}
