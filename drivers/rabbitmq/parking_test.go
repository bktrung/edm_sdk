package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// TestParkQueueNameCarriesNoDelay proves a destination has one parking queue
// name whatever its delay, which is what lets a changed retry delay reuse the
// queue instead of declaring another beside it.
func TestParkQueueNameCarriesNoDelay(t *testing.T) {
	if got := parkQueueName("orders.retry.2"); got != "orders.retry.2.park" {
		t.Fatalf("parkQueueName() = %q, want %q", got, "orders.retry.2.park")
	}
}

func TestParkQueueParts(t *testing.T) {
	cases := []struct {
		name        string
		destination string
		ok          bool
	}{
		{name: "orders.retry.2.park", destination: "orders.retry.2", ok: true},
		// The suffix is read from the end, so a destination whose own name
		// contains ".park." resolves to itself rather than to its prefix.
		{name: "orders.park.eligible.park", destination: "orders.park.eligible", ok: true},
		{name: "topology.prune.park.eligible"},
		{name: "orders.parked"},
		{name: ".park"},
		{name: "orders"},
	}
	for _, test := range cases {
		destination, ok := parkQueueParts(test.name)
		if ok != test.ok || destination != test.destination && test.ok {
			t.Errorf("parkQueueParts(%q) = (%q, %t), want (%q, %t)", test.name, destination, ok, test.destination, test.ok)
		}
	}
}

func TestParkingOfNamesOneQueuePerDelayedDestination(t *testing.T) {
	c := &conn{deferred: map[string]time.Duration{"orders.retry.2": 5 * time.Second}}
	if got, ok := c.parkingOf("orders.retry.2"); !ok || got != "orders.retry.2.park" {
		t.Fatalf("parkingOf(delayed destination) = (%q, %t), want the parking queue", got, ok)
	}
	if got, ok := c.parkingOf("orders.other"); ok {
		t.Fatalf("parkingOf(undeclared destination) = (%q, true), want no parking queue", got)
	}
}

// TestProducerTargetRoutesByDestinationDelay proves a parked message carries
// its destination's delay as its expiration, rounded up to whole milliseconds,
// since the parking queue has no TTL of its own.
func TestProducerTargetRoutesByDestinationDelay(t *testing.T) {
	p := &producer{conn: &conn{deferred: map[string]time.Duration{"d": 1500 * time.Microsecond}}}
	if exchange, routingKey, expiration := p.target(driver.OutboundMessage{Destination: "d"}); exchange != "" || routingKey != "d.park" || expiration != "2" {
		t.Fatalf("target(delayed) = (%q, %q, %q), want the default exchange, the parking queue and expiration 2", exchange, routingKey, expiration)
	}
	if exchange, routingKey, expiration := p.target(driver.OutboundMessage{Destination: "plain"}); exchange != "" || routingKey != "plain" || expiration != "" {
		t.Fatalf("target(plain) = (%q, %q, %q), want the destination queue and no expiration", exchange, routingKey, expiration)
	}
	if exchange, routingKey, expiration := p.target(driver.OutboundMessage{Destination: "orders", EntryPoint: true}); exchange != "orders" || routingKey != "" || expiration != "" {
		t.Fatalf("target(entry point) = (%q, %q, %q), want the exchange and no expiration", exchange, routingKey, expiration)
	}
}

func TestTopologyNoneRoutesDelayedMessagesToTheParkingQueue(t *testing.T) {
	c := &conn{deferred: map[string]time.Duration{}}
	admin := &adminOperations{conn: c}
	spec := driver.TopologySpec{
		Policy:       driver.TopologyNone,
		Destinations: []driver.DestinationSpec{{Name: "d", Kind: driver.DestRetry, Durable: true, Delay: 5 * time.Second}},
	}
	if _, err := admin.ensureTopology(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	p := &producer{conn: c}
	if _, routingKey, expiration := p.target(driver.OutboundMessage{Destination: "d"}); routingKey != "d.park" || expiration != "5000" {
		t.Fatalf("target() = (%q, %q), want the parking queue the operator provisions for this spec and expiration 5000", routingKey, expiration)
	}
}

// TestEnsureTopologyRefusesDelayAboveTheLongestExpiration proves a delay no
// message expiration can carry is refused under every policy, before any
// routing state is recorded, rather than parked and released early.
func TestEnsureTopologyRefusesDelayAboveTheLongestExpiration(t *testing.T) {
	longest := time.Duration(maxParkDelayMillis) * time.Millisecond
	if !parkDelayFits(longest) || parkDelayFits(longest+time.Nanosecond) {
		t.Fatalf("parkDelayFits must accept %s and refuse anything longer, which rounds up past the limit", longest)
	}
	tooLong := longest + time.Millisecond
	for _, policy := range []driver.TopologyPolicy{driver.TopologyDeclare, driver.TopologyVerify, driver.TopologyNone} {
		c := &conn{deferred: map[string]time.Duration{}}
		admin := &adminOperations{conn: c}
		_, err := admin.ensureTopology(context.Background(), driver.TopologySpec{
			Policy:       policy,
			Destinations: []driver.DestinationSpec{{Name: "d", Durable: true, Delay: tooLong}},
		})
		var driverErr *driver.Error
		if !errors.As(err, &driverErr) || driverErr.K != driver.KindFatal {
			t.Fatalf("policy %v: ensureTopology() error = %v, want a fatal driver error", policy, err)
		}
		if _, recorded := c.parkingOf("d"); recorded {
			t.Fatalf("policy %v: a refused delay was recorded for routing", policy)
		}
	}
}
