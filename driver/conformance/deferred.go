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

	// deferredOrderDelay is the delay the due-order check's destination declares.
	// A due time has to sit inside the band its destination's own delay sets for
	// it, because a driver is entitled to refuse one outside that band, so due
	// times deferredDelay*4 apart from their own publish instants cannot share a
	// destination declaring deferredDelay. See the check for why they are that
	// far apart and why they are not further.
	deferredOrderDelay = deferredDelay * 6
)

func init() { registerGroup("deferred", runDeferred) }

// warmDeferredTopology pays Kafka's creation and metadata propagation cost before deferred timing checks.
func warmDeferredTopology(group *groupContext) {
	destinations := []driver.DestinationSpec{
		{Name: "deferred.never-early", Delay: deferredDelay},
		{Name: "deferred.never-early.control"},
		{Name: "deferred.bounded-late", Delay: deferredDelay},
		{Name: "deferred.band", Delay: deferredDelay},
		{Name: "deferred.due-order", Delay: deferredOrderDelay},
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
	warmTopology(group.t, group, driver.TopologySpec{
		Destinations: destinations,
		Effective:    group.effective,
	})
}

func runDeferred(group *groupContext) {
	warmDeferredTopology(group)
	group.Check("explicit due time is never delivered early", func(t *testing.T) {
		deferred := "deferred.never-early"
		control := "deferred.never-early.control"
		producer := newDeferredProducer(t, group, profileDestination(group, deferred), deferredDelay)
		controlProducer := newProducer(t, group, profileDestination(group, control), driver.ProducerConfig{Effective: group.effective})
		consumer := deferredConsumer(t, group, []string{deferred, control}, 2)
		publishedAt := deferredNow(group)
		due := publishedAt.Add(deferredDelay)
		if err := controlProducer.Publish(group.ctx, driver.OutboundMessage{Destination: control, Body: []byte("control")}); err != nil {
			t.Fatal(err)
		}
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: deferred, Body: []byte("deferred"), DelayUntil: due}); err != nil {
			t.Fatal(err)
		}
		controlMessage := receiveMessage(t, group, consumer)
		if controlMessage.Destination != control {
			t.Fatalf("first delivery destination=%q, want control %q", controlMessage.Destination, control)
		}
		ackMessage(t, group, controlMessage)
		assertNoDelivery(t, group, consumer, "deferred message before its due time")
		advanceDeferredTo(group, due)
		message := receiveBefore(t, group, consumer, due.Add(deferredLateBound), "deferred message at its due time")
		if message.Destination != deferred || message.ReceivedAt.Before(due) {
			t.Fatalf("delivery=%+v, want destination %q at or after %s", message, deferred, due)
		}
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "deferred-never-early", Outcome: "on-time", FinalDestination: deferred})
	})

	group.Check("deferred delivery is bounded in lateness", func(t *testing.T) {
		name := "deferred.bounded-late"
		producer := newDeferredProducer(t, group, profileDestination(group, name), deferredDelay)
		consumer := deferredConsumer(t, group, []string{name}, 1)
		due := deferredNow(group).Add(deferredDelay)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("bounded"), DelayUntil: due}); err != nil {
			t.Fatal(err)
		}
		advanceDeferredTo(group, due.Add(deferredLateBound))
		message := receiveBefore(t, group, consumer, due.Add(deferredLateBound), "bounded deferred delivery")
		if message.ReceivedAt.Before(due) || message.ReceivedAt.After(due.Add(deferredLateBound)) {
			t.Fatalf("ReceivedAt=%s, want between %s and %s", message.ReceivedAt, due, due.Add(deferredLateBound))
		}
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "deferred-bounded-late", Outcome: "bounded", FinalDestination: name})
	})

	group.Check("each in-band due time is delivered", func(t *testing.T) {
		name := "deferred.band"
		producer := newDeferredProducer(t, group, profileDestination(group, name), deferredDelay)
		consumer := deferredConsumer(t, group, []string{name}, 3)
		base := deferredNow(group)
		// The offsets ascend, and that order is load-bearing rather than tidy: a driver
		// that parks every deferred message of a destination in one queue releases them
		// in publish order, because a per-message TTL expires only at the head of a queue.
		// Ascending due times keep the head the earliest one, so each message still leaves
		// at its own instant and a violation stays invisible here. Reordering these offsets
		// therefore changes what this check covers: a nearer due time published after a
		// farther one needs its own check.
		offsets := []time.Duration{deferredDelay * 4 / 5, deferredDelay, deferredDelay * 6 / 5}
		dues := make(map[string]time.Time, len(offsets))
		for i, offset := range offsets {
			body := fmt.Sprintf("band-%d", i)
			publishedAt := deferredNow(group)
			due := publishedAt.Add(offset)
			assertDeferredBand(t, publishedAt, due)
			dues[body] = due
			if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte(body), DelayUntil: due}); err != nil {
				t.Fatal(err)
			}
		}
		for i, offset := range offsets {
			due := base.Add(offset)
			advanceDeferredTo(group, due.Add(deferredLateBound))
			message := receiveBefore(t, group, consumer, due.Add(deferredLateBound), "in-band deferred delivery")
			due, ok := dues[string(message.Body)]
			if !ok {
				t.Fatalf("unexpected body %q", message.Body)
			}
			if string(message.Body) != fmt.Sprintf("band-%d", i) {
				t.Fatalf("received body %q at due boundary %s, want band-%d", message.Body, due, i)
			}
			if message.ReceivedAt.Before(due) {
				t.Fatalf("body %q arrived at %s before %s", message.Body, message.ReceivedAt, due)
			}
			if message.ReceivedAt.After(due.Add(deferredLateBound)) {
				t.Fatalf("body %q arrived at %s after its bound %s", message.Body, message.ReceivedAt, due.Add(deferredLateBound))
			}
			delete(dues, string(message.Body))
			ackMessage(t, group, message)
		}
		if len(dues) != 0 {
			t.Fatalf("undelivered in-band messages: %v", dues)
		}
		group.vector.Add(BehaviorEvent{ID: "deferred-band", Outcome: "all-delivered", FinalDestination: name})
	})

	group.Check("a nearer due time published after a farther one is delivered in due order", func(t *testing.T) {
		name := "deferred.due-order"
		producer := newDeferredProducer(t, group, profileDestination(group, name), deferredOrderDelay)
		consumer := deferredConsumer(t, group, []string{name}, 2)
		// Publish order here is the opposite of due order, and that inversion is the
		// whole instrument: a driver that parks every deferred message of one
		// destination behind a single released head, or that holds a message waiting
		// for its own due time behind a later-due one it read first, releases the
		// nearer message only once the farther one is owed. The band check above
		// publishes its offsets in due order, which leaves an earlier due time at the
		// head on exactly those drivers, so it cannot see the fault and this check
		// exists.
		//
		// The separation between the two due times, not the assertions, is what makes
		// the fault visible. Held behind the farther message, the nearer one arrives
		// at the farther due time, 2s past its own and far outside deferredLateBound,
		// so the band assertion below cannot pass by accident. Due times closer
		// together than the band would let a driver release both late and still put
		// each inside its own band, which reads as green.
		//
		// Each due time is deferredDelay*4 or deferredDelay*8 from its own publish
		// instant, and the destination declares deferredOrderDelay. Both offsets sit
		// inside the band a destination's own delay sets for a due time, which is what
		// keeps the check measuring delivery order rather than measuring a driver
		// correctly refusing a due time it does not owe. Both also sit outside the
		// narrower window assertDeferredBand enforces, so that helper is not the one
		// for this check and is not called here: it constrains a due time to within a
		// fifth of the destination delay, and two due times that close together cannot
		// be separated by more than the band.
		fartherDue := deferredNow(group).Add(deferredDelay * 8)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("farther"), DelayUntil: fartherDue}); err != nil {
			t.Fatal(err)
		}
		nearerDue := deferredNow(group).Add(deferredDelay * 4)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("nearer"), DelayUntil: nearerDue}); err != nil {
			t.Fatal(err)
		}
		owed := []struct {
			body string
			due  time.Time
		}{
			{body: "nearer", due: nearerDue},
			{body: "farther", due: fartherDue},
		}
		// Each step releases the message that step's due time is owed to, and both due
		// times fall at or before last, the last instant a delivery can legitimately
		// arrive by. Waiting against last rather than against each due time's own
		// bound is what makes a driver that releases the two together report a
		// delivery at the wrong instant, rather than report a delivery that never
		// came.
		last := fartherDue.Add(deferredLateBound)
		received := make(map[string]driver.InboundMessage, len(owed))
		for _, step := range owed {
			// Advance to half the bound past the due time, not onto it. A fixture stamps
			// ReceivedAt with the instant its clock was advanced to, so advancing to
			// step.due.Add(deferredLateBound) left the late assertion below comparing that
			// instant with itself: equality passed, so the assertion held whatever the driver
			// did, and it moved with any later edit to the advance target or to how a fixture
			// stamps ReceivedAt. Half keeps the release triggered by a clock strictly past the
			// due time and lands ReceivedAt deferredLateBound/2 inside the bound, which gives
			// the assertion margin to fail on rather than an equality to pass by. Any fraction
			// below one still leaves the farther message unreleased at this step: its due time
			// is deferredDelay*4 later and deferredLateBound is far short of that separation.
			advanceDeferredTo(group, step.due.Add(deferredLateBound/2))
			message := receiveBefore(t, group, consumer, last, "due-order deferred delivery")
			received[string(message.Body)] = message
			ackMessage(t, group, message)
		}
		for _, want := range owed {
			message, ok := received[want.body]
			if !ok {
				t.Fatalf("body %q owed at %s was never delivered", want.body, want.due)
			}
			if message.ReceivedAt.Before(want.due) {
				t.Fatalf("body %q arrived at %s before its due time %s", want.body, message.ReceivedAt, want.due)
			}
			if message.ReceivedAt.After(want.due.Add(deferredLateBound)) {
				t.Fatalf("body %q arrived at %s after its bound %s", want.body, message.ReceivedAt, want.due.Add(deferredLateBound))
			}
		}
		if nearer, farther := received["nearer"], received["farther"]; !nearer.ReceivedAt.Before(farther.ReceivedAt) {
			t.Fatalf("body %q arrived at %s and body %q at %s: deliveries arrived in publish order, not due order",
				"nearer", nearer.ReceivedAt, "farther", farther.ReceivedAt)
		}
		group.vector.Add(BehaviorEvent{ID: "deferred-due-order", Outcome: "due-order", FinalDestination: name})
	})

	group.Check("destination delay supplies a zero due time", func(t *testing.T) {
		name := "deferred.destination-delay"
		control := "deferred.destination-delay.control"
		producer := newDeferredProducer(t, group, profileDestination(group, name), deferredDelay)
		controlProducer := newProducer(t, group, profileDestination(group, control), driver.ProducerConfig{Effective: group.effective})
		consumer := deferredConsumer(t, group, []string{name, control}, 2)
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
		assertNoDelivery(t, group, consumer, "zero due time before destination delay")
		due := publishedAt.Add(deferredDelay)
		advanceDeferredTo(group, due)
		message := receiveBefore(t, group, consumer, due.Add(deferredLateBound), "destination-delay delivery")
		if message.ReceivedAt.Before(due) {
			t.Fatalf("ReceivedAt=%s, want at or after %s", message.ReceivedAt, due)
		}
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "deferred-destination-delay", Outcome: "nominal-delay", FinalDestination: name})
	})

	group.Check("zero destination delay and zero due time deliver immediately", func(t *testing.T) {
		name := "deferred.zero"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := deferredConsumer(t, group, []string{name}, 1)
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
		group.vector.Add(BehaviorEvent{ID: "deferred-zero", Outcome: "immediate", FinalDestination: name})
	})

	group.Check("Auxiliary counts not-yet-due messages separately", func(t *testing.T) {
		name := "deferred.auxiliary"
		control := "deferred.auxiliary.control"
		producer := newDeferredProducer(t, group, profileDestination(group, name), deferredDelay)
		controlProducer := newProducer(t, group, profileDestination(group, control), driver.ProducerConfig{Effective: group.effective})
		due := deferredNow(group).Add(deferredDelay)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, DelayUntil: due}); err != nil {
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
		group.vector.Add(BehaviorEvent{ID: "deferred-auxiliary", Outcome: "counted", FinalDestination: name})
	})

	group.Check("Ready excludes Auxiliary messages and preserves total depth", func(t *testing.T) {
		name := "deferred.ready"
		control := "deferred.ready.control"
		producer := newDeferredProducer(t, group, profileDestination(group, name), deferredDelay)
		controlProducer := newProducer(t, group, profileDestination(group, control), driver.ProducerConfig{Effective: group.effective})
		due := deferredNow(group).Add(deferredDelay)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, DelayUntil: due}); err != nil {
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
		group.vector.Add(BehaviorEvent{ID: "deferred-ready", Outcome: "excludes-auxiliary", FinalDestination: name})
	})

	group.Check("Auxiliary messages become Ready at their due time", func(t *testing.T) {
		name := "deferred.release"
		control := "deferred.release.control"
		producer := newDeferredProducer(t, group, profileDestination(group, name), deferredDelay)
		controlProducer := newProducer(t, group, profileDestination(group, control), driver.ProducerConfig{Effective: group.effective})
		due := deferredNow(group).Add(deferredDelay)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("release"), DelayUntil: due}); err != nil {
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
		group.vector.Add(BehaviorEvent{ID: "deferred-release", Outcome: "ready", FinalDestination: name})
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
			Destinations: []string{name, control}, Prefetch: 2, Effective: group.effective,
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
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("parked"), DelayUntil: due}); err != nil {
			t.Fatal(err)
		}
		assertNoDelivery(t, group, consumer, "parked message during ack deadline")
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
		group.vector.Add(BehaviorEvent{ID: "deferred-deadline", Outcome: "not-consumed", FinalDestination: name})
	})
}

func newDeferredProducer(t *testing.T, group *groupContext, destination string, delay time.Duration) driver.Producer {
	t.Helper()
	if _, err := group.conn.Admin().EnsureTopology(group.ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Delay: delay}}, Effective: group.effective,
	}); err != nil {
		t.Fatalf("EnsureTopology(%q): %v", destination, err)
	}
	producer, err := group.conn.Producer(group.ctx, driver.ProducerConfig{Effective: group.effective})
	if err != nil {
		t.Fatalf("Producer(%q): %v", destination, err)
	}
	t.Cleanup(func() {
		if err := producer.Close(group.ctx); err != nil {
			t.Errorf("close producer %q: %v", destination, err)
		}
	})
	t.Cleanup(func() {
		if err := purgeIfSupported(group.ctx, group.conn, destination); err != nil {
			t.Errorf("purge destination %q: %v", destination, err)
		}
	})
	return &profileProducer{group: group, producer: producer, scoped: destination != unprofileDestination(group, destination)}
}

func deferredConsumer(t *testing.T, group *groupContext, destinations []string, prefetch int) driver.Consumer {
	t.Helper()
	scoped, logical := profileDestinations(group, destinations)
	consumer, err := group.conn.Consumer(group.ctx, driver.ConsumerConfig{
		Destinations: scoped, Prefetch: prefetch, Effective: group.effective,
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

func assertDeferredBand(t *testing.T, publishedAt, due time.Time) {
	t.Helper()
	delay := due.Sub(publishedAt)
	if delay < deferredDelay*4/5 || delay > deferredDelay*6/5 {
		t.Fatalf("DelayUntil offset=%s, want within [%s, %s]", delay, deferredDelay*4/5, deferredDelay*6/5)
	}
}

func assertNoDelivery(t *testing.T, group *groupContext, consumer driver.Consumer, what string) {
	t.Helper()
	waitForStable(t, group, what, func() (bool, string) {
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
