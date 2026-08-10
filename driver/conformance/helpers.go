package conformance

import (
	"context"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
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
	ctx, cancel := context.WithTimeout(group.ctx, 5*time.Second)
	defer cancel()
	select {
	case message := <-consumer.Messages():
		return message
	case <-ctx.Done():
		t.Fatal("timed out waiting for published message")
		return driver.InboundMessage{}
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
