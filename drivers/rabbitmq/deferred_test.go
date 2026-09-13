package rabbitmq

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// deleteParkQueues removes a destination and every parking queue it parks in,
// so a test leaves the fixture as it found it whichever rung a message landed
// in. The ladder is read from parkQueueNames rather than listed here, so a rung
// added to the ladder is cleaned up by every test that uses this.
func deleteParkQueues(channel *amqp.Channel, destination string) {
	for _, parkName := range parkQueueNames(destination) {
		_, _ = channel.QueueDelete(parkName, false, false, false)
	}
	_, _ = channel.QueueDelete(destination, false, false, false)
}

// TestDeferredDueOrderSurvivesReversedPublishOrder is the regression test for
// the head-of-line defect: a nearer due time published after a farther one to
// the same destination is owed at its own due time, not at the instant the
// message ahead of it is released.
//
// It reads arrivals from the destination itself rather than through the
// driver's consumer, because the property under test is when the broker
// dead-letters a parked message, and a consumer-side delay would blur it. The
// nearer message is published second and its due time is measured from its own
// publish instant, which is the shape the core produces when a retry with a
// short Retry-After follows one with a longer delay in the same destination.
//
// The failure this guards against is unbounded rather than subtle: on a single
// per-message-TTL park queue the nearer message leaves with the farther one,
// seconds late, and with the ladder collapsed onto one rung it leaves at that
// rung, which is later still.
func TestDeferredDueOrderSurvivesReversedPublishOrder(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const destination = "rabbitmq-driver-deferred-due-order"
	const lateBound = 700 * time.Millisecond

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
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: 2 * time.Second}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	deliveries, err := rawChannel.Consume(destination, "deferred-due-order", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	producer, err := conn.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(ctx) })

	// The farther due time is published first, which is the order the defect
	// cannot honour. The two clock reads and the read budget below are wall
	// time on purpose: this is the one test whose subject is real broker
	// timing, and a fake clock cannot expire a parked message. The same
	// suppression, for the same reason, is on the delivery test above.
	farDue := time.Now().Add(2 * time.Second) //nolint:forbidigo // real broker timing is the subject
	if err := producer.Publish(ctx, driver.OutboundMessage{
		Destination: destination,
		DelayUntil:  farDue,
		Body:        []byte("far"),
	}); err != nil {
		t.Fatalf("Publish(far): %v", err)
	}
	nearDue := time.Now().Add(500 * time.Millisecond) //nolint:forbidigo // real broker timing is the subject
	if err := producer.Publish(ctx, driver.OutboundMessage{
		Destination: destination,
		DelayUntil:  nearDue,
		Body:        []byte("near"),
	}); err != nil {
		t.Fatalf("Publish(near): %v", err)
	}
	if !nearDue.Before(farDue) {
		t.Fatalf("due times %s and %s are not ordered, so this run cannot show the defect", nearDue, farDue)
	}

	received := make(map[string]time.Time, 2)
	arrived := make([]string, 0, 2)
	for range 2 {
		deadline := time.Until(farDue.Add(lateBound))
		select {
		case delivery := <-deliveries:
			if err := delivery.Ack(false); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			received[string(delivery.Body)] = time.Now() //nolint:forbidigo // real broker timing is the subject
			arrived = append(arrived, string(delivery.Body))
		case <-time.After(deadline): //nolint:forbidigo // real broker timing is the subject
			t.Fatalf("after %s only %v had arrived, want %q by %s and %q by %s",
				deadline, arrived, "near", nearDue.Add(lateBound), "far", farDue.Add(lateBound))
		}
	}
	for body, due := range map[string]time.Time{"near": nearDue, "far": farDue} {
		arrived, ok := received[body]
		if !ok {
			t.Fatalf("body %q did not arrive, want it at %s", body, due)
		}
		if arrived.Before(due) {
			t.Fatalf("body %q arrived at %s, before its due time %s", body, arrived, due)
		}
		if arrived.After(due.Add(lateBound)) {
			t.Fatalf("body %q arrived at %s, %s late against a %s bound", body, arrived, arrived.Sub(due), lateBound)
		}
	}
	if !received["near"].Before(received["far"]) {
		t.Fatalf("near arrived at %s and far at %s, want the nearer due time first", received["near"], received["far"])
	}
}

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
		deleteParkQueues(rawChannel, destination)
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
