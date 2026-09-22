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
	if got := cap(rabbitConsumer.messages); got != brokerPrefetchSafetyValue {
		t.Fatalf("messages channel capacity = %d, want %d", got, brokerPrefetchSafetyValue)
	}
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
	held := make([]driver.InboundMessage, 0, brokerPrefetchSafetyValue)
	for range brokerPrefetchSafetyValue - 1 {
		select {
		case message := <-sdkConsumer.Messages():
			held = append(held, message)
		case <-ctx.Done():
			t.Fatalf("waiting for all held deliveries: got %d of %d: %v", len(held)+1, brokerPrefetchSafetyValue, ctx.Err())
		}
	}
	t.Logf("held deliveries before close: %d", len(held)+1)
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
