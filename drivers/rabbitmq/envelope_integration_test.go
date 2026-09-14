//go:build integration

package rabbitmq

import (
	"bytes"
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// TestEnvelopeRoundTripPreservesKeyAndAttributes publishes through the driver
// and reads back through the driver, asserting every value survives byte for
// byte. Two of them are the ones a lossy mapping quietly rewrites: the ordering
// identity, which a fanout exchange gives no routing key to carry, and a
// sub-second UTC time, which the AMQP timestamp property truncates to whole
// seconds and re-renders in the consuming process's zone.
func TestEnvelopeRoundTripPreservesKeyAndAttributes(t *testing.T) {
	requireBroker(t)

	const queue = "rabbitmq-driver-envelope"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("raw broker connection: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("raw channel: %v", err)
	}
	_, _ = rawChannel.QueueDelete(queue, false, false, false)
	if _, err := rawChannel.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
		t.Fatalf("QueueDeclare(%q): %v", queue, err)
	}
	t.Cleanup(func() { _, _ = rawChannel.QueueDelete(queue, false, false, false) })

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	producer, err := conn.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}

	const wantTime = "2026-08-13T22:30:00.123456789Z"
	wantKey := []byte("order-42")
	wantHeaders := []driver.Header{
		{Key: "id", Value: []byte("evt-1")},
		{Key: "time", Value: []byte(wantTime)},
		{Key: "type", Value: []byte("orders.created.v1")},
		{Key: "x-trace", Value: []byte("trace-1")},
	}
	if err := producer.Publish(ctx, driver.OutboundMessage{
		Destination: queue,
		Key:         wantKey,
		Headers:     wantHeaders,
		Body:        []byte("body"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{queue}, Prefetch: 1})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	var message driver.InboundMessage
	select {
	case message = <-consumer.Messages():
	case <-ctx.Done():
		t.Fatal("no delivery")
	}

	if !bytes.Equal(message.Key, wantKey) {
		t.Fatalf("Key = %q, want %q", message.Key, wantKey)
	}
	if len(message.Headers) != len(wantHeaders) {
		t.Fatalf("Headers = %#v, want %d", message.Headers, len(wantHeaders))
	}
	for _, want := range wantHeaders {
		got, ok := headerByKey(message.Headers, want.Key)
		if !ok {
			t.Fatalf("header %q missing from %#v", want.Key, message.Headers)
		}
		if !bytes.Equal(got, want.Value) {
			t.Fatalf("header %q = %q, want %q", want.Key, got, want.Value)
		}
	}
	if message.DeliveryCount != 0 {
		t.Fatalf("DeliveryCount = %d, want 0 on a first delivery", message.DeliveryCount)
	}

	if err := message.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := consumer.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func headerByKey(headers []driver.Header, key string) ([]byte, bool) {
	for _, header := range headers {
		if header.Key == key {
			return header.Value, true
		}
	}
	return nil, false
}
