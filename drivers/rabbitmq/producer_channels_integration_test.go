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

// openSharedProducer declares queue, opens a producer on it, and hands the test
// both the driver's view and the producer itself.
func openSharedProducer(t *testing.T, ctx context.Context, queue string) (driver.Conn, *amqp.Channel, *producer) {
	t.Helper()
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
	t.Cleanup(func() { _ = rawProducer.Close(context.WithoutCancel(ctx)) })
	return conn, rawChannel, rawProducer.(*producer)
}

func outbound(destination, id, body string) driver.OutboundMessage {
	return driver.OutboundMessage{
		Destination: destination,
		Headers:     []driver.Header{{Key: "id", Value: []byte(id)}},
		Body:        []byte(body),
	}
}

// openChannels returns the channels the producer holds in its slots.
func openChannels(p *producer) []*publishChannel {
	p.mu.Lock()
	defer p.mu.Unlock()
	var channels []*publishChannel
	for _, channel := range p.channels {
		if channel != nil {
			channels = append(channels, channel)
		}
	}
	return channels
}

// TestProducerCallsShareChannels proves the channel count bounds channels and
// not calls: many more concurrent calls than the producer has channels are all
// confirmed, every message arrives exactly once, and the producer holds no
// more than its fixed set of channels to do it.
func TestProducerCallsShareChannels(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-shared-calls"
	const publishers = 8 * publishChannelCount
	_, rawChannel, p := openSharedProducer(t, ctx, queue)
	deliveries, err := rawChannel.Consume(queue, "producer-shared-calls", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	publishErrs := make([]error, publishers)
	var group sync.WaitGroup
	for index := range publishers {
		group.Go(func() {
			publishErrs[index] = p.Publish(ctx, outbound(queue, fmt.Sprintf("shared-%d", index), fmt.Sprintf("shared-body-%d", index)))
		})
	}
	group.Wait()
	if err := errors.Join(publishErrs...); err != nil {
		t.Fatalf("concurrent publishes: %v", err)
	}
	if channels := openChannels(p); len(channels) > publishChannelCount {
		t.Fatalf("producer holds %d channels, want at most %d", len(channels), publishChannelCount)
	}
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
		if want := fmt.Sprintf("shared-body-%d", index); received[want] != 1 {
			t.Fatalf("delivered body %q %d times, want exactly once", want, received[want])
		}
	}
}

// TestProducerCancelledPublishKeepsItsChannel proves a caller giving up costs
// nothing but its own call. Its messages still in flight fail with the
// context's error, as undecided messages always have, and the channel they
// were published on stays in use: the client resolves each message's
// confirmation on its own, so nothing a later call reads can belong to the
// cancelled one, and no channel has to be thrown away to guarantee it.
func TestProducerCancelledPublishKeepsItsChannel(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-cancel-keeps"
	const batch = 5000
	_, rawChannel, p := openSharedProducer(t, ctx, queue)
	deliveries, err := rawChannel.Consume(queue, "producer-cancel-keeps", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	messages := make([]driver.OutboundMessage, batch)
	for index := range messages {
		messages[index] = outbound(queue, fmt.Sprintf("cancel-%d", index), fmt.Sprintf("cancel-body-%d", index))
	}
	before := openChannels(p)
	publishCtx, cancelPublish := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- p.Publish(publishCtx, messages...) }()
	awaitUnconfirmedDeliveries(t, ctx, deliveries, 1)
	cancelPublish()

	var result error
	select {
	case result = <-done:
	case <-ctx.Done():
		t.Fatalf("cancelled Publish result: %v", ctx.Err())
	}
	var publishErr *driver.PublishError
	if !errors.As(result, &publishErr) || !errors.Is(result, context.Canceled) {
		t.Fatalf("cancelled Publish error = %v, want a *driver.PublishError carrying context.Canceled", result)
	}
	for index, failure := range publishErr.Failed {
		if kind, classified := driver.Classify(failure); !classified || kind != driver.KindTransient {
			t.Fatalf("failed[%d] = %v, kind %v, want transient", index, failure, kind)
		}
	}
	for _, channel := range append(before, openChannels(p)...) {
		if channel.channel.IsClosed() {
			t.Fatal("a channel closed because a caller cancelled its publish")
		}
	}
	if err := p.Publish(ctx, outbound(queue, "after-cancel", "after-cancel")); err != nil {
		t.Fatalf("Publish after a cancelled call: %v", err)
	}
}

// TestProducerClosedChannelIsReplaced proves a channel the broker or the
// connection closed takes nothing else with it: the calls that land on its
// slot afterwards publish on a fresh channel, and the closed channel's watcher
// exits once nothing is registered on it.
func TestProducerClosedChannelIsReplaced(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-closed-replaced"
	_, rawChannel, p := openSharedProducer(t, ctx, queue)
	deliveries, err := rawChannel.Consume(queue, "producer-closed-replaced", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	closed := openChannels(p)[0]
	if err := closed.channel.Close(); err != nil {
		t.Fatalf("closing the producer's channel: %v", err)
	}
	// Every slot is visited within one round of the round robin, so this many
	// calls reach the slot whose channel closed.
	const calls = 2 * publishChannelCount
	for index := range calls {
		if err := p.Publish(ctx, outbound(queue, fmt.Sprintf("replaced-%d", index), "replaced")); err != nil {
			t.Fatalf("Publish %d after a channel closed: %v", index, err)
		}
	}
	for _, channel := range openChannels(p) {
		if channel == closed {
			t.Fatal("the closed channel is still in the producer's slots")
		}
	}
	select {
	case <-closed.watcher.done:
	case <-ctx.Done():
		t.Fatalf("the closed channel's watcher did not exit: %v", ctx.Err())
	}
	awaitUnconfirmedDeliveries(t, ctx, deliveries, calls)
}

// TestProducerReturnsReachTheirOwnCalls proves a return fails the call whose
// message the broker could not route, and only that call, when calls share
// channels. Half the calls publish to a destination that does not exist; each
// must be told not-found, and each of the others must be told published. The
// rounds repeat because which calls share a channel, and how their returns and
// acks interleave, is decided by the scheduler and the broker.
func TestProducerReturnsReachTheirOwnCalls(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-shared-returns"
	const missing = "rabbitmq-driver-producer-shared-returns-missing"
	const publishers = 4 * publishChannelCount
	_, _, p := openSharedProducer(t, ctx, queue)
	for round := range 5 {
		assertOwnOutcomes(t, ctx, p, publishers, queue, missing, func(index int) string {
			return fmt.Sprintf("returns-%d-%d", round, index)
		})
	}
}

// TestProducerSharedIDKeepsEachCallsOutcome proves two unresolved messages
// never share an id on one channel. A retry or dead-letter successor copies
// the id of the delivery it came from, so the same id can be in flight from
// two calls at once; a basic.return names its message only by that id, so two
// such messages on one channel would let a return fail the call that was
// published and pass the one that was not. Every call here uses one id, half
// of them towards a destination that does not exist.
func TestProducerSharedIDKeepsEachCallsOutcome(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-shared-id"
	const missing = "rabbitmq-driver-producer-shared-id-missing"
	const publishers = 4 * publishChannelCount
	_, _, p := openSharedProducer(t, ctx, queue)
	for range 5 {
		assertOwnOutcomes(t, ctx, p, publishers, queue, missing, func(int) string { return "shared-id" })
	}
}

// assertOwnOutcomes runs publishers concurrent one-message calls, the odd ones
// towards missing, and checks each call was told its own outcome.
func assertOwnOutcomes(t *testing.T, ctx context.Context, p *producer, publishers int, queue, missing string, id func(int) string) {
	t.Helper()
	publishErrs := make([]error, publishers)
	var group sync.WaitGroup
	for index := range publishers {
		destination := queue
		if index%2 == 1 {
			destination = missing
		}
		group.Go(func() {
			publishErrs[index] = p.Publish(ctx, outbound(destination, id(index), "outcome"))
		})
	}
	group.Wait()
	for index, err := range publishErrs {
		if index%2 == 0 {
			if err != nil {
				t.Fatalf("call %d to the existing queue: %v, want published", index, err)
			}
			continue
		}
		if !errors.Is(err, driver.ErrDestinationMissing) {
			t.Fatalf("call %d to the missing destination: %v, want ErrDestinationMissing", index, err)
		}
	}
}

// TestProducerSizeRefusalOnASharedChannel proves a message refused for its
// size fails as too large and takes no other call with it. The broker closes
// the channel it refused the message on; the close names the limit, which says
// the other calls' messages in flight on that channel were not the refused
// one, so they are published again on another channel and every one of those
// calls succeeds. The calls publish batches so that some are certainly in
// flight on the refused message's channel when it closes.
func TestProducerSizeRefusalOnASharedChannel(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-shared-size"
	assertBystandersPublished(t, ctx, queue, func(index int) driver.OutboundMessage {
		message := outbound(queue, fmt.Sprintf("size-%d", index), "refused")
		message.Body = make([]byte, oversizedBodyBytes)
		return message
	}, driver.KindTooLarge)
}

// TestProducerMissingExchangeOnASharedChannel is the same property for the
// other close a single publish causes: a publish to an exchange that does not
// exist closes its channel with a 404 naming the exchange. That call fails
// with the broker's reason, transient as a closed publish channel has always
// been, and the calls sharing its channel are published again elsewhere and
// succeed. Several bad publishes run so that most channels are closed at least
// once while the batches are in flight.
func TestProducerMissingExchangeOnASharedChannel(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-shared-missing-exchange"
	const exchange = "rabbitmq-driver-producer-shared-missing-exchange-absent"
	assertBystandersPublished(t, ctx, queue, func(index int) driver.OutboundMessage {
		return driver.OutboundMessage{
			Destination: exchange,
			EntryPoint:  true,
			Headers:     []driver.Header{{Key: "id", Value: fmt.Appendf(nil, "absent-%d", index)}},
			Body:        []byte("to a missing exchange"),
		}
	}, driver.KindTransient)
}

// assertBystandersPublished runs batch calls to queue alongside calls whose
// one message the broker refuses by closing its channel, and checks every
// batch call is published in full, with each message delivered, while every
// refused call fails with the broker's close as want.
func assertBystandersPublished(t *testing.T, ctx context.Context, queue string, refused func(int) driver.OutboundMessage, want driver.Kind) {
	t.Helper()
	const bystanders = 8 * publishChannelCount
	const batch = 200
	const culprits = 2 * publishChannelCount
	_, rawChannel, p := openSharedProducer(t, ctx, queue)
	deliveries, err := rawChannel.Consume(queue, "producer-bystanders", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	bystanderErrs := make([]error, bystanders)
	culpritErrs := make([]error, culprits)
	var group sync.WaitGroup
	for call := range bystanders {
		messages := make([]driver.OutboundMessage, batch)
		for index := range messages {
			messages[index] = outbound(queue, fmt.Sprintf("bystander-%d-%d", call, index), fmt.Sprintf("bystander-%d-%d", call, index))
		}
		group.Go(func() { bystanderErrs[call] = p.Publish(ctx, messages...) })
	}
	for index := range culprits {
		group.Go(func() { culpritErrs[index] = p.Publish(ctx, refused(index)) })
	}
	group.Wait()
	for call, err := range bystanderErrs {
		if err != nil {
			t.Fatalf("bystander call %d = %v, want published: another call's refused message failed it", call, err)
		}
	}
	for index, err := range culpritErrs {
		if kind, classified := driver.Classify(err); !classified || kind != want {
			t.Fatalf("refused call %d = %v, kind %v, want %v", index, err, kind, want)
		}
	}
	// A republished message may have reached the queue before the close as
	// well, so a message can arrive twice; none may be missing.
	received := make(map[string]bool, bystanders*batch)
	for len(received) < bystanders*batch {
		select {
		case delivery := <-deliveries:
			received[string(delivery.Body)] = true
			if err := delivery.Ack(false); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("received %d of %d distinct messages: %v", len(received), bystanders*batch, ctx.Err())
		}
	}
	if err := p.Publish(ctx, outbound(queue, "after-refusals", "after-refusals")); err != nil {
		t.Fatalf("Publish after the refusals: %v", err)
	}
}

// TestProducerCloseWithPublishesInFlight proves a Close that runs while calls
// are still waiting for confirmations lets those calls return, and that once
// the connection closes every channel watcher has exited: the producer leaves
// nothing running. The bound Close's context puts on its wait is covered
// without a broker, by TestProducerCloseIsBoundedByItsContext.
func TestProducerCloseWithPublishesInFlight(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-close-in-flight"
	const batch = 5000
	conn, rawChannel, p := openSharedProducer(t, ctx, queue)
	deliveries, err := rawChannel.Consume(queue, "producer-close-in-flight", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	var calls sync.WaitGroup
	for call := range publishChannelCount {
		messages := make([]driver.OutboundMessage, batch)
		for index := range messages {
			messages[index] = outbound(queue, fmt.Sprintf("close-%d-%d", call, index), "in-flight")
		}
		calls.Go(func() { _ = p.Publish(ctx, messages...) })
	}
	awaitUnconfirmedDeliveries(t, ctx, deliveries, 1)
	watchers := make([]*publishWatcher, 0, publishChannelCount)
	for _, channel := range openChannels(p) {
		watchers = append(watchers, channel.watcher)
	}
	closeCtx, cancelClose := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancelClose()
	_ = p.Close(closeCtx)
	returned := make(chan struct{})
	go func() {
		calls.Wait()
		close(returned)
	}()
	select {
	case <-returned:
	case <-ctx.Done():
		t.Fatalf("publishes in flight did not return after Close: %v", ctx.Err())
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("Conn.Close: %v", err)
	}
	for index, watcher := range watchers {
		select {
		case <-watcher.done:
		case <-ctx.Done():
			t.Fatalf("watcher %d still running after the connection closed: %v", index, ctx.Err())
		}
	}
}
