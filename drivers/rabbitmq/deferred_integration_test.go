//go:build integration

package rabbitmq

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// deleteParkQueues removes a destination and its parking queue, so a test
// leaves the fixture as it found it.
func deleteParkQueues(channel *amqp.Channel, destination string) {
	_, _ = channel.QueueDelete(parkQueueName(destination), false, false, false)
	_, _ = channel.QueueDelete(destination, false, false, false)
}

// TestDelayedDestinationFiresAtItsDelay proves a message published to a
// delayed destination arrives once the destination's delay has passed, and no
// later than its expiration plus the broker's dead-letter hop.
func TestDelayedDestinationFiresAtItsDelay(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const destination = "rabbitmq-driver-fixed-delay-fire"
	const delay = 5 * time.Second

	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	deleteParkQueues(rawChannel, destination)
	t.Cleanup(func() {
		deleteParkQueues(rawChannel, destination)
		_ = rawChannel.Close()
	})

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: delay}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	deliveries, err := rawChannel.Consume(destination, "fixed-delay-fire", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	producer, err := conn.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(ctx) })

	publishedAt := time.Now() //nolint:forbidigo // the timing assertion uses the real broker clock
	if err := producer.Publish(ctx, driver.OutboundMessage{
		Destination: destination,
		Body:        []byte("fixed"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	deadlineCtx, deadlineCancel := context.WithDeadline(ctx, publishedAt.Add(6500*time.Millisecond))
	defer deadlineCancel()
	select {
	case delivery := <-deliveries:
		receivedAt := time.Now() //nolint:forbidigo // the timing assertion uses the real broker clock
		elapsed := receivedAt.Sub(publishedAt)
		t.Logf("publish-to-receive = %s", elapsed)
		if elapsed < 5*time.Second || elapsed >= 6500*time.Millisecond {
			t.Fatalf("publish-to-receive = %s, want at least 5s and below 6.5s", elapsed)
		}
		if string(delivery.Body) != "fixed" {
			t.Fatalf("body = %q, want fixed", delivery.Body)
		}
		if err := delivery.Ack(false); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-deadlineCtx.Done():
		t.Fatalf("delivery did not arrive by %s: %v", publishedAt.Add(6500*time.Millisecond), deadlineCtx.Err())
	}
}
