package conformance

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func assertCancelled(t *testing.T, operation string, err error) {
	t.Helper()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("%s() error=%v, want context.Canceled", operation, err)
	}
	if kind, ok := driver.Classify(err); !ok || kind != driver.KindTransient {
		t.Fatalf("%s() classification=(%v,%t), want transient,true", operation, kind, ok)
	}
}

func (g *groupContext) maintenance(t *testing.T) driver.Maintenance {
	t.Helper()
	maintenance, ok := g.conn.Admin().(driver.Maintenance)
	if !ok {
		g.Skip(t, g.currentCheck, "driver.Maintenance is not implemented")
	}
	return maintenance
}

const (
	waitTimeout  = 5 * time.Second
	waitInterval = 5 * time.Millisecond

	// The stability window only needs to outlast the next dispatch, which is the mechanism
	// that can violate the current empty-channel and upper-bound assertions.
	stabilityWindow = 250 * time.Millisecond
)

func newProducer(t *testing.T, group *groupContext, destination string, config driver.ProducerConfig) driver.Producer {
	t.Helper()
	if _, err := group.conn.Admin().EnsureTopology(group.ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination}},
		Effective:    group.effective,
	}); err != nil {
		t.Fatalf("EnsureTopology(%q): %v", destination, err)
	}
	producer, err := group.conn.Producer(group.ctx, config)
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
	return producer
}

func newConsumer(t *testing.T, group *groupContext, destination string, prefetch int) driver.Consumer {
	t.Helper()
	consumer, err := group.conn.Consumer(group.ctx, driver.ConsumerConfig{
		Destinations: []string{destination}, Prefetch: prefetch, Effective: group.effective,
	})
	if err != nil {
		t.Fatalf("Consumer(%q): %v", destination, err)
	}
	t.Cleanup(func() {
		if err := consumer.Stop(group.ctx); err != nil {
			t.Errorf("stop consumer %q: %v", destination, err)
		}
	})
	return consumer
}

func headerByKey(t *testing.T, headers []driver.Header, key string) driver.Header {
	t.Helper()
	for _, header := range headers {
		if header.Key == key {
			return header
		}
	}
	t.Fatalf("header %q not found in %#v", key, headers)
	return driver.Header{}
}

func inspectDestination(t *testing.T, group *groupContext, destination string) BrokerView {
	t.Helper()
	view, err := group.inspect(group.ctx, destination)
	if err != nil {
		t.Fatalf("Inspect(%q): %v", destination, err)
	}
	return view
}

func receiveMessage(t *testing.T, group *groupContext, consumer driver.Consumer) driver.InboundMessage {
	t.Helper()
	var received driver.InboundMessage
	waitFor(t, group, "published message", func() (bool, string) {
		select {
		case message, ok := <-consumer.Messages():
			if !ok {
				return false, "Messages channel closed"
			}
			received = message
			return true, fmt.Sprintf("received destination=%q", message.Destination)
		default:
			return false, "no message"
		}
	})
	return received
}

// waitFor retries condition until it holds or the deadline passes, and reports
// the last observed value on failure.
func waitFor(t *testing.T, group *groupContext, what string, condition func() (bool, string)) {
	t.Helper()
	waitForUntil(t, group, what, condition, false)
}

func waitForStable(t *testing.T, group *groupContext, what string, condition func() (bool, string)) {
	t.Helper()
	waitForUntil(t, group, what, condition, true)
}

func waitForUntil(t *testing.T, group *groupContext, what string, condition func() (bool, string), stable bool) {
	t.Helper()
	timeout := waitTimeout
	if stable {
		timeout = stabilityWindow
	}
	deadline, cancel := context.WithTimeout(group.ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(waitInterval) //nolint:forbidigo // conformance is stdlib-only and cannot import internal/clock
	defer ticker.Stop()
	var lastObservation string
	for {
		holds, observation := condition()
		lastObservation = observation
		if !holds {
			if stable {
				t.Fatalf("%s violated; last observation: %s", what, lastObservation)
			}
		} else if !stable {
			return
		}

		select {
		case <-deadline.Done():
			if stable {
				return
			}
			t.Fatalf("%s timed out after %s; last observation: %s", what, timeout, lastObservation)
		case <-ticker.C:
		}
	}
}

func ackMessage(t *testing.T, group *groupContext, message driver.InboundMessage) {
	t.Helper()
	if message.Settle == nil {
		t.Fatal("published message has nil settler")
	}
	if err := message.Settle.Ack(group.ctx); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
}
