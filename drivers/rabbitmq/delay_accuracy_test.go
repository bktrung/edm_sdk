package rabbitmq

import (
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// TestDelayAccuracyMatchesTheParkLadder holds the accuracy the driver declares
// to the parking ladder it is derived from. The declaration is read from
// Capabilities rather than recomputed here, so a hand edit to the declaration
// is caught as well as a drift in the derivation: the two are checked against
// each other nowhere else, which leaves a false number shipping green.
//
// The ladder is not frozen by this. The derivation recomputes the declared
// bound from the rungs, so an edited ladder moves the declaration along with it
// and every assertion here still holds.
func TestDelayAccuracyMatchesTheParkLadder(t *testing.T) {
	accuracy := Driver{}.Capabilities().DelayAccuracy

	first := parkRungs[0]
	last := parkRungs[len(parkRungs)-1]
	if accuracy.Floor != first || accuracy.MaxDelay != last {
		t.Fatalf("declared accuracy = %+v, want floor %s from the first rung and max delay %s from the last", accuracy, first, last)
	}

	// Every rung sits within one relative step of the rung below it. That is
	// the declaration's own promise: a delay parked in a rung above the one it
	// rounded out of is late by at most that step, which the relative covers
	// for every delay in the band and not only for the rungs shipped here.
	for i := range len(parkRungs) - 1 {
		previous, rung := parkRungs[i], parkRungs[i+1]
		if float64(rung) > float64(previous)*(1+accuracy.Relative) {
			t.Errorf("rung %s is more than %v times the rung below it %s, so the declared relative %v is too small", rung, 1+accuracy.Relative, previous, accuracy.Relative)
		}
	}

	// The edges of the declared band, where the ladder rounds a delay up to a
	// rung and the lateness is the whole gap: a delay of one nanosecond and a
	// delay at the first rung, both parked in that first rung, then a delay one
	// nanosecond above every rung below the top one.
	delays := make([]time.Duration, 0, len(parkRungs)+1)
	delays = append(delays, 1, first)
	for _, rung := range parkRungs[:len(parkRungs)-1] {
		delays = append(delays, rung+1)
	}
	for _, delay := range delays {
		parked := parkRung(delay)
		if parked == 0 {
			t.Errorf("parkRung(%s) = 0, want a rung at or below the declared maximum %s", delay, last)
			continue
		}
		bound := max(accuracy.Floor, time.Duration(float64(delay)*accuracy.Relative))
		if lateness := parked - delay; lateness > bound {
			t.Errorf("parkRung(%s) = %s, late by %s, want at most %s", delay, parked, lateness, bound)
		}
	}

	// Above the top rung the driver declares no bound, and the routing agrees:
	// the delay belongs on the per-message expiration path, which has no rung.
	if parked := parkRung(last + 1); parked != 0 {
		t.Errorf("parkRung(%s) = %s, want 0: the declared bound ends at %s", last+1, parked, last)
	}
}

func TestFixedParkQueueName(t *testing.T) {
	if got := fixedParkQueueName("orders.retry.2", 5*time.Second); got != "orders.retry.2.park.fixed-5000ms" {
		t.Fatalf("fixedParkQueueName(5s) = %q, want %q", got, "orders.retry.2.park.fixed-5000ms")
	}
	if got := fixedParkQueueName("orders.retry.2", 1500*time.Microsecond); got != "orders.retry.2.park.fixed-2ms" {
		t.Fatalf("fixedParkQueueName(1500us) = %q, want %q", got, "orders.retry.2.park.fixed-2ms")
	}
}

func TestFixedParkQueuePartsRoundTrip(t *testing.T) {
	name := fixedParkQueueName("orders.retry.2", 5*time.Second)
	destination, delay, ok := parkQueueParts(name)
	if !ok || destination != "orders.retry.2" || delay != 5*time.Second {
		t.Fatalf("parkQueueParts(%q) = (%q, %s, %t), want (%q, %s, %t)", name, destination, delay, ok, "orders.retry.2", 5*time.Second, true)
	}
}

func TestFixedParkQueuePartsRejectsNonParkingNames(t *testing.T) {
	cases := []string{
		"topology.prune.park.eligible",
		"x.park.fixed-",
		"x.park.fixed-0ms",
		"x.park.fixed-05000ms",
		"x.park.fixed-5000",
		"x.park.fixed-abcms",
	}
	for _, name := range cases {
		if destination, delay, ok := parkQueueParts(name); ok {
			t.Errorf("parkQueueParts(%q) = (%q, %s, true), want ok=false", name, destination, delay)
		}
	}
}

func TestParkQueueNamesForFixedAndLadder(t *testing.T) {
	fixed := driver.DestinationSpec{Name: "orders.retry.2", Delay: 5 * time.Second, FixedDelay: true}
	names := parkQueueNamesFor(fixed)
	if len(names) != 2 {
		t.Fatalf("parkQueueNamesFor(fixed) = %v, want 2 names", names)
	}
	if names[0] != "orders.retry.2.park.fixed-5000ms" || names[1] != "orders.retry.2.park" {
		t.Fatalf("parkQueueNamesFor(fixed) = %v, want [fixed-5000ms park]", names)
	}
	ladder := driver.DestinationSpec{Name: "orders.retry.2", Delay: 5 * time.Second}
	ladderNames := parkQueueNamesFor(ladder)
	if len(ladderNames) != 9 {
		t.Fatalf("parkQueueNamesFor(ladder) = %v, want 9 names", ladderNames)
	}
}

func TestFixedParkTargetRouting(t *testing.T) {
	p := &producer{
		clock: clock.NewReal(),
		conn: &conn{
			deferred: map[string]time.Duration{"d": 5 * time.Second},
			fixed:    map[string]time.Duration{"d": 5 * time.Second},
		},
	}
	due := time.Now().Add(5 * time.Second) //nolint:forbidigo // remaining delay is measured at publish time
	_, routingKey, expiration := p.target(driver.OutboundMessage{Destination: "d", DelayUntil: due})
	if routingKey != "d.park.fixed-5000ms" {
		t.Fatalf("target() routingKey = %q, want %q", routingKey, "d.park.fixed-5000ms")
	}
	if expiration != "" {
		t.Fatalf("target() expiration = %q, want empty in the fixed queue", expiration)
	}
	farDue := time.Now().Add(10 * time.Second) //nolint:forbidigo // remaining delay is measured at publish time
	_, farKey, farExpiration := p.target(driver.OutboundMessage{Destination: "d", DelayUntil: farDue})
	if farKey != "d.park" {
		t.Fatalf("target() routingKey = %q, want %q", farKey, "d.park")
	}
	if farExpiration == "" {
		t.Fatal("target() expiration is empty above the fixed delay, want a per-message TTL")
	}
}
