//go:build integration

package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// producerFreeChannel hands a test the channel the producer is holding free,
// which is the channel its next publish runs on: a test replaces the
// confirmation or return stream the driver would read from it, or attaches a
// second listener to the channel itself, before that publish starts.
func producerFreeChannel(t *testing.T, ctx context.Context, p *producer) *publishChannel {
	t.Helper()
	channel, err := p.acquire(ctx)
	if err != nil {
		t.Fatalf("taking the producer's free channel: %v", err)
	}
	p.release(channel)
	return channel
}

// awaitChannelClosed waits for the driver's close of a channel to reach the
// client. The close is started before the publish that discarded the channel
// returns, but its wait is bounded by that publish's context: a window whose
// context has already ended gives up on the broker rather than waiting for it,
// and one of the ways a channel dies is the caller's context ending, so the
// close is allowed to finish after the publish that discarded it does.
func awaitChannelClosed(t *testing.T, ctx context.Context, channel *amqp.Channel) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond) //nolint:forbidigo // a discarded channel gives off no event of its own
	defer ticker.Stop()
	deadline := time.NewTimer(5 * time.Second) //nolint:forbidigo // bounded live-broker channel-close guard
	defer deadline.Stop()
	for !channel.IsClosed() {
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("a discarded channel stayed open")
		case <-ctx.Done():
			t.Fatalf("waiting for a discarded channel to close: %v", ctx.Err())
		}
	}
}

// TestProducerHeldChannelDoesNotBlockPublish proves a publish waits for a free
// channel and not for the producer: one channel held out of the pool for the
// duration of another call must not stop that call from being published and
// confirmed. A producer with a single channel has nothing free to hand out,
// and one publish at a time is the serialisation the pool replaces.
func TestProducerHeldChannelDoesNotBlockPublish(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-pool-held"
	conn, rawChannel := openProducerFixture(t, ctx, queue)
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	rawProducer, err := conn.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = rawProducer.Close(context.Background()) })
	deliveries, err := rawChannel.Consume(queue, "producer-pool-held", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	p := rawProducer.(*producer)
	held, err := p.acquire(ctx)
	if err != nil {
		t.Fatalf("taking a channel out of the pool: %v", err)
	}
	defer p.release(held)
	publishCtx, cancelPublish := context.WithTimeout(ctx, 5*time.Second)
	defer cancelPublish()
	if err := rawProducer.Publish(publishCtx, driver.OutboundMessage{
		Destination: queue,
		Headers:     []driver.Header{{Key: "id", Value: []byte("held-0")}},
		Body:        []byte("published-while-a-channel-is-held"),
	}); err != nil {
		t.Fatalf("Publish while a channel is held = %v, want nil", err)
	}
	select {
	case delivery := <-deliveries:
		if string(delivery.Body) != "published-while-a-channel-is-held" {
			t.Fatalf("delivered body = %q, want published-while-a-channel-is-held", delivery.Body)
		}
		if err := delivery.Ack(false); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("receiving the published message: %v", ctx.Err())
	}
}

// TestProducerPublishesBeyondPoolSize proves the pool bounds channels and not
// publishes: twice as many concurrent publishes as the pool holds channels
// must all be confirmed, and every message must arrive exactly once.
func TestProducerPublishesBeyondPoolSize(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-pool-beyond"
	const publishers = 2 * publishChannelPoolSize
	conn, rawChannel := openProducerFixture(t, ctx, queue)
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	rawProducer, err := conn.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = rawProducer.Close(context.Background()) })
	deliveries, err := rawChannel.Consume(queue, "producer-pool-beyond", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	publishErrs := make([]error, publishers)
	var group sync.WaitGroup
	for index := range publishers {
		group.Add(1)
		go func() {
			defer group.Done()
			publishErrs[index] = rawProducer.Publish(ctx, driver.OutboundMessage{
				Destination: queue,
				Headers:     []driver.Header{{Key: "id", Value: fmt.Appendf(nil, "beyond-%d", index)}},
				Body:        fmt.Appendf(nil, "beyond-body-%d", index),
			})
		}()
	}
	group.Wait()
	var failures []error
	for index, err := range publishErrs {
		if err != nil {
			failures = append(failures, fmt.Errorf("concurrent publish %d: %w", index, err))
		}
	}
	if err := errors.Join(failures...); err != nil {
		t.Fatalf("publishes beyond the pool size: %v", err)
	}

	// Each message is its own window, and those publishes run on different
	// channels, so they can reach the queue in any order: what is promised is
	// one copy of each message, not a global order.
	received := make(map[string]int, publishers)
	for range publishers {
		select {
		case delivery := <-deliveries:
			received[string(delivery.Body)]++
			if err := delivery.Ack(false); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("received %d of %d messages: %v", len(received), publishers, ctx.Err())
		}
	}
	for index := range publishers {
		if want := fmt.Sprintf("beyond-body-%d", index); received[want] != 1 {
			t.Fatalf("delivered body %q %d times, want exactly once", want, received[want])
		}
	}
}

// TestProducerDiscardedChannelIsClosedAndReplaced proves a window that died
// takes only its channel with it: the channel is closed, and the producer
// stays usable on a channel of its own. The window is cancelled mid-flight,
// the way a caller whose deadline ran out cancels it.
func TestProducerDiscardedChannelIsClosedAndReplaced(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-pool-discard"
	conn, rawChannel := openProducerFixture(t, ctx, queue)
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	rawProducer, err := conn.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = rawProducer.Close(context.Background()) })
	deliveries, err := rawChannel.Consume(queue, "producer-pool-discard", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	p := rawProducer.(*producer)
	discarded := producerFreeChannel(t, ctx, p)
	// The window reads confirmations from a stream the test owns and never
	// feeds, so it cannot finish until its own context ends.
	discarded.confirms = make(chan amqp.Confirmation, publishWindowSize)
	messages := make([]driver.OutboundMessage, 4)
	for index := range messages {
		messages[index] = driver.OutboundMessage{
			Destination: queue,
			Headers:     []driver.Header{{Key: "id", Value: fmt.Appendf(nil, "discard-%d", index)}},
			Body:        fmt.Appendf(nil, "discard-body-%d", index),
		}
	}
	publishCtx, cancelPublish := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- rawProducer.Publish(publishCtx, messages...) }()
	awaitUnconfirmedDeliveries(t, ctx, deliveries, len(messages))
	cancelPublish()

	var publishErr *driver.PublishError
	select {
	case err := <-done:
		if !errors.As(err, &publishErr) {
			t.Fatalf("cancelled window Publish error = %v, want *driver.PublishError", err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled window Publish error = %v, want context.Canceled", err)
		}
	case <-ctx.Done():
		t.Fatalf("cancelled window Publish result: %v", ctx.Err())
	}
	if len(publishErr.Failed) != len(messages) {
		t.Fatalf("failed indexes = %v, want every index of the cancelled window", publishErr.Failed)
	}
	awaitChannelClosed(t, ctx, discarded.channel)

	replacementCtx, cancelReplacement := context.WithTimeout(ctx, 5*time.Second)
	defer cancelReplacement()
	if err := rawProducer.Publish(replacementCtx, driver.OutboundMessage{
		Destination: queue,
		Headers:     []driver.Header{{Key: "id", Value: []byte("discard-replacement")}},
		Body:        []byte("discard-replacement"),
	}); err != nil {
		t.Fatalf("Publish after a discarded channel: %v", err)
	}
	replacement := producerFreeChannel(t, ctx, p)
	if replacement == discarded {
		t.Fatal("the publish after the discard ran on the discarded channel")
	}
	select {
	case delivery := <-deliveries:
		if string(delivery.Body) != "discard-replacement" {
			t.Fatalf("delivered body = %q, want discard-replacement", delivery.Body)
		}
		if err := delivery.Ack(false); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("receiving the replacement message: %v", ctx.Err())
	}
}
