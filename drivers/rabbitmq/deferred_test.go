package rabbitmq

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// TestDeferredPublishUsesRemainingDelay verifies that a due time shorter than
// the destination's nominal delay is not restarted from the nominal delay.
func TestDeferredPublishUsesRemainingDelay(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const destination = "rabbitmq-driver-deferred-remaining"

	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	t.Cleanup(func() {
		_, _ = rawChannel.QueueDelete(destination+".park", false, false, false)
		_, _ = rawChannel.QueueDelete(destination, false, false, false)
		_ = rawChannel.Close()
	})

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: time.Second}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	deliveries, err := rawChannel.Consume(destination, "deferred-remaining", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	producer, err := conn.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(ctx) })

	publishedAt := time.Now() //nolint:forbidigo // the timing assertion uses the real broker clock
	due := publishedAt.Add(300 * time.Millisecond)
	if err := producer.Publish(ctx, driver.OutboundMessage{
		Destination: destination,
		DelayUntil:  due,
		Body:        []byte("remaining"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	earlyCtx, earlyCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer earlyCancel()
	select {
	case <-deliveries:
		t.Fatalf("delivery arrived early at %s, due %s", time.Now(), due) //nolint:forbidigo // the timing assertion uses the real broker clock
	case <-earlyCtx.Done():
		if err := earlyCtx.Err(); err != context.DeadlineExceeded {
			t.Fatalf("early-delivery check: %v", err)
		}
	}
	deadlineCtx, deadlineCancel := context.WithDeadline(ctx, due.Add(500*time.Millisecond))
	defer deadlineCancel()
	select {
	case delivery := <-deliveries:
		receivedAt := time.Now() //nolint:forbidigo // the timing assertion uses the real broker clock
		if receivedAt.Before(due) || receivedAt.After(due.Add(500*time.Millisecond)) {
			t.Fatalf("delivery at %s, want between %s and %s", receivedAt, due, due.Add(500*time.Millisecond))
		}
		if string(delivery.Body) != "remaining" {
			t.Fatalf("body = %q, want remaining", delivery.Body)
		}
		if err := delivery.Ack(false); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-deadlineCtx.Done():
		t.Fatalf("delivery did not arrive by %s: %v", due.Add(500*time.Millisecond), deadlineCtx.Err())
	}
}
