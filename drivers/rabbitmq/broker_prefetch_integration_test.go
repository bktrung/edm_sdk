//go:build integration

package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

const brokerPrefetchSafetyValue = 64

// TestBrokerPrefetchHeldQuorumDeliveries records RabbitMQ's quorum behavior
// when a consumer with broker prefetch holds deliveries that no handler has
// handled. The blocked handler receives one delivery and leaves it unsettled;
// closing the consumer then measures the broker's count after redelivery.
func TestBrokerPrefetchHeldQuorumDeliveries(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := (Driver{}).Open(ctx, driver.Config{
		Endpoints:     []string{defaultEndpoint},
		DriverOptions: map[string]string{brokerPrefetchOption: "64"},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Logf("RabbitMQ broker version = %s", conn.BrokerInfo().Version)

	const queue = "rabbitmq-broker-prefetch-safety"
	rawChannel := declareBrokerPrefetchQueue(t, queue)
	publishBrokerPrefetchMessages(t, ctx, rawChannel, queue, brokerPrefetchSafetyValue)
	sdkConsumer, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{queue}, Prefetch: 1})
	if err != nil {
		_ = rawChannel.Close()
		_ = conn.Close(ctx)
		t.Fatalf("Consumer: %v", err)
	}
	rabbitConsumer := sdkConsumer.(*consumer)
	if len(rabbitConsumer.lanes) != 1 || rabbitConsumer.lanes[0].prefetch != brokerPrefetchSafetyValue {
		t.Fatalf("lane prefetch = %#v, want %d", rabbitConsumer.lanes, brokerPrefetchSafetyValue)
	}
	handlerBlock := make(chan struct{})
	handlerDone := make(chan struct{})
	var closeHandler sync.Once
	t.Cleanup(func() {
		closeHandler.Do(func() { close(handlerBlock) })
		_ = sdkConsumer.Release(context.Background())
		_ = conn.Close(context.Background())
	})
	firstC := make(chan driver.InboundMessage, 1)
	go func() {
		firstC <- <-sdkConsumer.Messages()
		<-handlerBlock
		close(handlerDone)
	}()

	var first driver.InboundMessage
	select {
	case first = <-firstC:
	case <-ctx.Done():
		t.Fatalf("waiting for held delivery: %v", ctx.Err())
	}
	t.Logf("first held delivery before close: count=%d", first.DeliveryCount)
	if first.DeliveryCount < 0 {
		t.Fatalf("held delivery has no native delivery count")
	}
	// One delivery is SDK-admitted. The next waits at admission while the
	// remaining transport credit fills pending, without registering settlers.
	waitForwarderState(t, "all broker-prefetched deliveries", func() bool {
		lane := rabbitConsumer.lanes[0]
		lane.mu.Lock()
		emitting := lane.emitting
		lane.mu.Unlock()
		return len(lane.pending)+emitting == brokerPrefetchSafetyValue-1
	})
	select {
	case message := <-sdkConsumer.Messages():
		t.Fatalf("broker prefetch bypassed SDK cap: %q", message.Body)
	default:
	}
	rabbitConsumer.mu.Lock()
	outstanding := rabbitConsumer.outstanding
	rabbitConsumer.mu.Unlock()
	if outstanding != 1 {
		t.Fatalf("SDK-admitted unsettled deliveries = %d, want 1", outstanding)
	}
	t.Logf("broker-held deliveries: %d; SDK-admitted deliveries: %d", brokerPrefetchSafetyValue, outstanding)
	if err := sdkConsumer.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}
	closeHandler.Do(func() { close(handlerBlock) })
	<-handlerDone
	if err := rawChannel.Close(); err != nil {
		t.Fatalf("Close publisher channel: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("Close consumer connection: %v", err)
	}

	reopened, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial reopen: %v", err)
	}
	defer reopened.Close()
	reopenedChannel, err := reopened.Channel()
	if err != nil {
		t.Fatalf("Channel reopen: %v", err)
	}
	defer reopenedChannel.Close()
	if err := reopenedChannel.Qos(1, 0, false); err != nil {
		t.Fatalf("Qos reopen: %v", err)
	}
	redeliveries, err := reopenedChannel.Consume(queue, "broker-prefetch-safety-second", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume reopen: %v", err)
	}
	select {
	case delivery := <-redeliveries:
		secondCount := deliveryCount(delivery, true)
		t.Logf("redelivered after close: count=%d headers=%v", secondCount, delivery.Headers)
		if secondCount <= first.DeliveryCount {
			t.Fatalf("redelivery count=%d, first=%d; headers=%v", secondCount, first.DeliveryCount, delivery.Headers)
		}
		if err := delivery.Ack(false); err != nil {
			t.Fatalf("Ack redelivery: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("waiting for redelivery: %v", ctx.Err())
	}
}

func TestBrokerPrefetchDeliversEveryMessageOnce(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := (Driver{}).Open(ctx, driver.Config{
		Endpoints:     []string{defaultEndpoint},
		DriverOptions: map[string]string{brokerPrefetchOption: "64"},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	const queue = "rabbitmq-broker-prefetch-delivery"
	rawChannel := declareBrokerPrefetchQueue(t, queue)
	publishBrokerPrefetchMessages(t, ctx, rawChannel, queue, brokerPrefetchSafetyValue)
	if err := rawChannel.Close(); err != nil {
		t.Fatalf("Close publisher channel: %v", err)
	}

	consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{queue}, Prefetch: 1})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = consumer.Stop(context.Background()) }()
	seen := make(map[string]int, brokerPrefetchSafetyValue)
	for range brokerPrefetchSafetyValue {
		select {
		case message := <-consumer.Messages():
			seen[string(message.Body)]++
			if err := message.Settle.Ack(ctx); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("waiting for message: %v", ctx.Err())
		}
	}
	if len(seen) != brokerPrefetchSafetyValue {
		t.Fatalf("received %d unique messages, want %d", len(seen), brokerPrefetchSafetyValue)
	}
	for body, count := range seen {
		if count != 1 {
			t.Fatalf("message %q delivered %d times, want once", body, count)
		}
	}
}

func declareBrokerPrefetchQueue(t *testing.T, queue string) *amqp.Channel {
	t.Helper()
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	rawChannel, err := raw.Channel()
	if err != nil {
		_ = raw.Close()
		t.Fatalf("Channel: %v", err)
	}
	_, _ = rawChannel.QueueDelete(queue, false, false, false)
	if _, err := rawChannel.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
		_ = rawChannel.Close()
		_ = raw.Close()
		t.Fatalf("QueueDeclare: %v", err)
	}
	t.Cleanup(func() {
		cleanupChannel, cleanupErr := raw.Channel()
		if cleanupErr == nil {
			_, _ = cleanupChannel.QueueDelete(queue, false, false, false)
			_ = cleanupChannel.Close()
		}
		_ = raw.Close()
	})
	return rawChannel
}

func publishBrokerPrefetchMessages(t *testing.T, ctx context.Context, channel *amqp.Channel, queue string, count int) {
	t.Helper()
	for i := range count {
		if err := channel.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         []byte(fmt.Sprintf("held-%d", i)),
		}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
}

func TestConsumerAggregateAdmissionAcrossDestinations(t *testing.T) {
	for _, brokerPrefetch := range []string{"", "8"} {
		t.Run("broker-prefetch-"+brokerPrefetch, func(t *testing.T) {
			requireBroker(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			options := map[string]string{}
			if brokerPrefetch != "" {
				options[brokerPrefetchOption] = brokerPrefetch
			}
			public, err := (Driver{}).Open(ctx, driver.Config{
				Endpoints: []string{defaultEndpoint}, DriverOptions: options,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = public.Close(context.Background()) })
			firstQueue := "rabbitmq-aggregate-admission-first-" + brokerPrefetch
			secondQueue := "rabbitmq-aggregate-admission-second-" + brokerPrefetch
			firstChannel := declareBrokerPrefetchQueue(t, firstQueue)
			secondChannel := declareBrokerPrefetchQueue(t, secondQueue)
			publishBrokerPrefetchMessages(t, ctx, firstChannel, firstQueue, 4)
			publishBrokerPrefetchMessages(t, ctx, secondChannel, secondQueue, 4)
			sdk, err := public.Consumer(ctx, driver.ConsumerConfig{
				Destinations:   []string{firstQueue, secondQueue},
				Prefetch:       2,
				PerDestination: map[string]int{firstQueue: 2, secondQueue: 2},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = sdk.Release(context.Background()) })
			read := func() driver.InboundMessage {
				select {
				case message := <-sdk.Messages():
					return message
				case <-ctx.Done():
					t.Fatalf("waiting for admitted delivery: %v", ctx.Err())
					return driver.InboundMessage{}
				}
			}
			assertSaturated := func() {
				t.Helper()
				select {
				case message := <-sdk.Messages():
					t.Fatalf("aggregate cap exceeded by delivery from %q", message.Destination)
				case <-time.After(30 * time.Millisecond): //nolint:forbidigo // observe the absence of broker-backed admission
				}
			}
			first, second := read(), read()
			assertSaturated()
			if err := first.Settle.Ack(ctx); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			refill := read()
			assertSaturated()
			if err := sdk.Drain(ctx); err != nil {
				t.Fatalf("Drain at saturated admission: %v", err)
			}
			for _, message := range []driver.InboundMessage{second, refill} {
				if err := message.Settle.Ack(ctx); err != nil {
					t.Fatalf("Ack after Drain: %v", err)
				}
			}
			select {
			case message := <-sdk.Messages():
				t.Fatalf("delivery admitted after Drain: %q", message.Body)
			default:
			}
			if err := sdk.Stop(ctx); err != nil {
				t.Fatalf("Stop after saturated Drain: %v", err)
			}
		})
	}
}
