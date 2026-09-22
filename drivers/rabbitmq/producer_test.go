package rabbitmq

import (
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestProducerTargetUsesClockForRemainingDelay(t *testing.T) {
	now := time.Date(2030, time.January, 2, 3, 4, 5, 0, time.UTC)
	fake := clock.NewFake(now)
	producer := &producer{
		clock: fake,
		conn: &conn{
			deferred: map[string]time.Duration{"d": 5 * time.Second},
			fixed:    map[string]time.Duration{"d": 5 * time.Second},
		},
	}
	due := now.Add(10 * time.Second)
	_, routingKey, expiration := producer.target(driver.OutboundMessage{Destination: "d", DelayUntil: due})
	if routingKey != "d.park" || expiration != "10000" {
		t.Fatalf("target() = routing key %q, expiration %q; want d.park and 10000", routingKey, expiration)
	}

	fake.Advance(6 * time.Second)
	_, routingKey, expiration = producer.target(driver.OutboundMessage{Destination: "d", DelayUntil: due})
	if routingKey != "d.park.fixed-5000ms" || expiration != "" {
		t.Fatalf("target() after fake advance = routing key %q, expiration %q; want fixed queue and no expiration", routingKey, expiration)
	}
}
