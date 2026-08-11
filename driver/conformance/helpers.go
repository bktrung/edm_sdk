package conformance

import (
	"fmt"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

const (
	waitTimeout  = 5 * time.Second
	waitInterval = 10 * time.Millisecond
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
		if _, err := group.conn.Admin().Purge(group.ctx, destination); err != nil {
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
	deadline := time.NewTimer(waitTimeout)
	defer deadline.Stop()
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
		case <-deadline.C:
			t.Fatalf("%s timed out after %s; last observation: %s", what, waitTimeout, lastObservation)
		case <-time.After(waitInterval):
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
