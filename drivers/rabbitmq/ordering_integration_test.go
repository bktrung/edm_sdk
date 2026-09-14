//go:build integration

package rabbitmq

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

const (
	liveOrderingQueue    = "f1.test.rabbitmq-ordering"
	liveOrderingMessages = 48
)

func TestExclusiveConsumerPreservesKeyOrderOnQuorum(t *testing.T) {
	requireBroker(t)
	runLiveOrderingScenario(t)
}

func runLiveOrderingScenario(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })
	_, _ = rawChannel.QueueDelete(liveOrderingQueue, false, false, false)
	if _, err := rawChannel.QueueDeclare(liveOrderingQueue, true, false, false, false, amqp.Table{
		"x-queue-type": "quorum",
	}); err != nil {
		t.Fatalf("QueueDeclare: %v", err)
	}
	t.Cleanup(func() { _, _ = rawChannel.QueueDelete(liveOrderingQueue, false, false, false) })

	broker, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = broker.Close(context.Background()) })
	producer, err := broker.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(context.Background()) })

	consumerConfig := driver.ConsumerConfig{
		Destinations:   []string{liveOrderingQueue},
		Prefetch:       1,
		PerDestination: map[string]int{liveOrderingQueue: 1},
		Exclusive:      true,
	}
	first, err := broker.Consumer(ctx, consumerConfig)
	if err != nil {
		t.Fatalf("Consumer(first): %v", err)
	}
	consumers := []driver.Consumer{first}
	t.Cleanup(func() {
		for _, consumer := range consumers {
			_ = consumer.Stop(context.Background())
		}
	})

	messages := liveOrderingSequence()
	for _, message := range messages {
		if err := producer.Publish(ctx, message); err != nil {
			t.Fatalf("Publish(%q): %v", message.Body, err)
		}
	}

	deliveries := make([]driver.InboundMessage, 0, len(messages))
	for range messages {
		message := receiveLiveMessage(t, ctx, consumers[0])
		deliveries = append(deliveries, message)
		if err := acknowledgeLiveMessage(ctx, message); err != nil {
			t.Fatalf("Ack(%q): %v", message.Body, err)
		}
	}

	previous := make(map[string]int)
	var orderErr error
	for _, message := range deliveries {
		key, sequence, err := parseOrderingMessage(message)
		if err != nil {
			if orderErr == nil {
				orderErr = err
			}
			continue
		}
		if want := previous[key] + 1; sequence != want && orderErr == nil {
			orderErr = fmt.Errorf("key %q sequence=%d, want %d", key, sequence, want)
		}
		previous[key] = sequence
	}
	if orderErr != nil {
		t.Fatalf("ordering assertion: %v", orderErr)
	}

	for _, consumer := range consumers {
		if err := consumer.Stop(ctx); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	}
}

func acknowledgeLiveMessage(ctx context.Context, message driver.InboundMessage) error {
	if message.Settle == nil {
		return fmt.Errorf("message %q has nil settler", message.Body)
	}
	return message.Settle.Ack(ctx)
}

func liveOrderingSequence() []driver.OutboundMessage {
	keys := []string{"alpha", "beta", "gamma", "delta"}
	counts := make(map[string]int, len(keys))
	messages := make([]driver.OutboundMessage, 0, liveOrderingMessages)
	for i := range liveOrderingMessages {
		key := "ordered"
		if i >= 8 {
			key = keys[(i-8)%len(keys)]
		}
		counts[key]++
		messages = append(messages, driver.OutboundMessage{
			Destination: liveOrderingQueue,
			Key:         []byte(key),
			Body:        fmt.Appendf(nil, "%s:%d", key, counts[key]),
		})
	}
	return messages
}

func receiveLiveMessage(t *testing.T, ctx context.Context, consumer driver.Consumer) driver.InboundMessage {
	t.Helper()
	select {
	case message := <-consumer.Messages():
		return message
	case <-ctx.Done():
		t.Fatalf("receive delivery: %v", ctx.Err())
		return driver.InboundMessage{}
	}
}

func parseOrderingMessage(message driver.InboundMessage) (string, int, error) {
	key, sequence, ok := strings.Cut(string(message.Body), ":")
	if !ok || key == "" {
		return "", 0, fmt.Errorf("invalid ordering body %q", message.Body)
	}
	value, err := strconv.Atoi(sequence)
	if err != nil {
		return "", 0, fmt.Errorf("invalid ordering body %q: %w", message.Body, err)
	}
	return key, value, nil
}
