package rabbitmq

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestConsumerPauseResume(t *testing.T) {
	requireBroker(t)
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("raw broker connection: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("raw channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })

	const queueA = "rabbitmq-driver-consumer-a"
	const queueB = "rabbitmq-driver-consumer-b"
	for _, name := range []string{queueA, queueB} {
		_, _ = rawChannel.QueueDelete(name, false, false, false)
		if _, err := rawChannel.QueueDeclare(name, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
			t.Fatalf("QueueDeclare(%q): %v", name, err)
		}
		t.Cleanup(func() { _, _ = rawChannel.QueueDelete(name, false, false, false) })
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	sdkConsumer, err := conn.Consumer(ctx, driver.ConsumerConfig{
		Destinations:   []string{queueA, queueB},
		Prefetch:       2,
		PerDestination: map[string]int{queueA: 1, queueB: 1},
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}

	if err := sdkConsumer.Pause(queueA); err != nil {
		t.Fatalf("Pause(a): %v", err)
	}
	if err := sdkConsumer.Pause(queueA); err != nil {
		t.Fatalf("repeated Pause(a): %v", err)
	}
	publish := func(queue, body string) {
		t.Helper()
		if err := rawChannel.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         []byte(body),
		}); err != nil {
			t.Fatalf("Publish(%q): %v", queue, err)
		}
	}
	publish(queueA, "a-1")
	publish(queueB, "b-1")

	receiveCtx, receiveCancel := context.WithTimeout(ctx, 5*time.Second)
	defer receiveCancel()
	select {
	case message := <-sdkConsumer.Messages():
		if message.Destination != queueB || string(message.Body) != "b-1" {
			t.Fatalf("received while a paused = %q/%q, want b-1", message.Destination, message.Body)
		}
		if err := message.Settle.Ack(ctx); err != nil {
			t.Fatalf("Ack(b-1): %v", err)
		}
	case <-receiveCtx.Done():
		t.Fatal("timed out waiting for positive control from destination b")
	}

	publish(queueB, "b-2")
	select {
	case message := <-sdkConsumer.Messages():
		if message.Destination != queueB || string(message.Body) != "b-2" {
			t.Fatalf("second active delivery = %q/%q, want b-2", message.Destination, message.Body)
		}
		if err := message.Settle.Ack(ctx); err != nil {
			t.Fatalf("Ack(b-2): %v", err)
		}
	case <-receiveCtx.Done():
		t.Fatal("timed out waiting for second active delivery")
	}

	select {
	case message := <-sdkConsumer.Messages():
		t.Fatalf("received paused destination = %q/%q", message.Destination, message.Body)
	case <-time.After(100 * time.Millisecond): //nolint:forbidigo // test timeout is intentionally wall-clock based
	}

	if err := sdkConsumer.Resume(queueA); err != nil {
		t.Fatalf("Resume(a): %v", err)
	}
	for _, want := range []string{"a-1"} {
		select {
		case message := <-sdkConsumer.Messages():
			if message.Destination != queueA || string(message.Body) != want {
				t.Fatalf("resumed delivery = %q/%q, want a/%q", message.Destination, message.Body, want)
			}
			if err := message.Settle.Ack(ctx); err != nil {
				t.Fatalf("Ack(%q): %v", want, err)
			}
		case <-receiveCtx.Done():
			t.Fatalf("timed out waiting for resumed delivery %q", want)
		}
	}
	if err := sdkConsumer.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsumerDrainAfterCreationContextCancellation(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const queue = "rabbitmq-driver-consumer-cancel-drain"

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}

	consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{queue}, Prefetch: 1})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	cancel()

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	if err := consumer.Drain(drainCtx); err != nil {
		t.Fatalf("Drain after creation context cancellation: %v", err)
	}
	if err := consumer.Stop(drainCtx); err != nil {
		t.Fatalf("Stop after Drain: %v", err)
	}
}
