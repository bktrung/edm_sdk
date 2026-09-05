package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver/conformance"
)

func TestPauseReasonSet(t *testing.T) {
	reasons := []pauseReason{pauseReasonDeferred, pauseReasonPrefetch, pauseReasonUserPaused}
	orders := [][]pauseReason{
		{reasons[0], reasons[1], reasons[2]},
		{reasons[0], reasons[2], reasons[1]},
		{reasons[1], reasons[0], reasons[2]},
		{reasons[1], reasons[2], reasons[0]},
		{reasons[2], reasons[0], reasons[1]},
		{reasons[2], reasons[1], reasons[0]},
	}
	for _, order := range orders {
		set := make(pauseReasonSet)
		for index, reason := range order {
			if got := set.add(reason); got != (index == 0) {
				t.Fatalf("add(%q) becameNonEmpty=%t, want %t for order %v", reason, got, index == 0, order)
			}
		}
		if set.add(order[0]) {
			t.Fatalf("adding an existing reason became non-empty for order %v", order)
		}
		for index := len(order) - 1; index >= 0; index-- {
			if got := set.remove(order[index]); got != (index == 0) {
				t.Fatalf("remove(%q) becameEmpty=%t, want %t for order %v", order[index], got, index == 0, order)
			}
		}
		if set.remove(pauseReasonDeferred) {
			t.Fatalf("removing an absent reason became empty for order %v", order)
		}
	}

	set := make(pauseReasonSet)
	set.add(pauseReasonDeferred)
	set.add(pauseReasonPrefetch)
	if set.remove(pauseReasonDeferred) {
		t.Fatal("removing one of two reasons became empty")
	}
	if !set.remove(pauseReasonPrefetch) {
		t.Fatal("removing the second reason did not become empty")
	}
}

func TestConsumerIntake(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "consumer-intake")
	group := kafkaTestTopic(t, "consumer-intake-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 1)

	producer, err := connection.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true, Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	defer func() { _ = producer.Close(context.Background()) }()
	if err := producer.Publish(ctx, driver.OutboundMessage{
		Destination: topic,
		Key:         []byte("key"),
		Headers:     []driver.Header{{Key: "header", Value: []byte("value")}},
		Body:        []byte("body"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	consumer, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{topic}, Prefetch: 1, Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() { closeKafkaConsumer(consumer) })
	message := receiveKafkaMessage(t, consumer)
	if message.Destination != topic || !bytes.Equal(message.Key, []byte("key")) || !bytes.Equal(message.Body, []byte("body")) {
		t.Fatalf("message identity = destination:%q key:%q body:%q", message.Destination, message.Key, message.Body)
	}
	if len(message.Headers) != 1 || message.Headers[0].Key != "header" || !bytes.Equal(message.Headers[0].Value, []byte("value")) {
		t.Fatalf("message headers = %#v", message.Headers)
	}
	if message.Ref.Partition != 0 || message.Ref.Offset != 0 || message.Ref.Tag != 0 || message.Ref.Raw != "" {
		t.Fatalf("BrokerRef = %+v, want partition 0 offset 0 and zero tag/raw", message.Ref)
	}
	if message.DeliveryCount != -1 {
		t.Fatalf("DeliveryCount = %d, want -1", message.DeliveryCount)
	}
	if message.ReceivedAt.IsZero() {
		t.Fatal("ReceivedAt is zero")
	}
	if message.Settle == nil {
		t.Fatal("Settle is nil")
	}
	if err := message.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := consumer.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestConsumerPauseResume(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	first := kafkaTestTopic(t, "consumer-pause-first")
	second := kafkaTestTopic(t, "consumer-pause-second")
	group := kafkaTestTopic(t, "consumer-pause-group")
	cleanupKafkaTopics(t, admin, first, second)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, first, 1)
	createKafkaTopic(t, admin, ctx, second, 1)
	producer, err := connection.Producer(ctx, driver.ProducerConfig{Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	defer func() { _ = producer.Close(context.Background()) }()
	consumer, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{first, second}, Prefetch: 2,
		PerDestination: map[string]int{first: 1, second: 1}, Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() { closeKafkaConsumer(consumer) })

	if err := consumer.Pause(first); err != nil {
		t.Fatalf("Pause(first): %v", err)
	}
	publishKafkaMessage(t, producer, ctx, first, "first-paused")
	publishKafkaMessage(t, producer, ctx, second, "second-active")
	active := receiveKafkaMessage(t, consumer)
	if active.Destination != second {
		t.Fatalf("active delivery destination = %q, want %q", active.Destination, second)
	}
	if err := consumer.Pause(second); err != nil {
		t.Fatalf("Pause(second): %v", err)
	}
	if err := active.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(active): %v", err)
	}
	expectNoKafkaMessage(t, consumer, 300*time.Millisecond)
	if err := consumer.Resume(second); err != nil {
		t.Fatalf("Resume(second): %v", err)
	}
	if err := consumer.Resume(first); err != nil {
		t.Fatalf("Resume(first): %v", err)
	}
	paused := receiveKafkaMessage(t, consumer)
	if paused.Destination != first {
		t.Fatalf("resumed delivery destination = %q, want %q", paused.Destination, first)
	}
	if err := paused.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(resumed): %v", err)
	}

	if err := consumer.Pause(); err != nil {
		t.Fatalf("Pause(all): %v", err)
	}
	publishKafkaMessage(t, producer, ctx, first, "first-all-paused")
	publishKafkaMessage(t, producer, ctx, second, "second-all-paused")
	expectNoKafkaMessage(t, consumer, 300*time.Millisecond)
	if err := consumer.Resume(); err != nil {
		t.Fatalf("Resume(all): %v", err)
	}
	seen := map[string]bool{}
	for range 2 {
		message := receiveKafkaMessage(t, consumer)
		seen[message.Destination] = true
		if err := message.Settle.Ack(ctx); err != nil {
			t.Fatalf("Ack(all): %v", err)
		}
	}
	if !seen[first] || !seen[second] {
		t.Fatalf("destinations after Resume() = %v, want both destinations", seen)
	}
	if err := consumer.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestConsumerGroupIdentityIsPerConsumer(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "consumer-group-identity")
	cleanupKafkaTopics(t, admin, topic)
	createKafkaTopic(t, admin, ctx, topic, 1)
	producer, err := connection.Producer(ctx, driver.ProducerConfig{Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	defer func() { _ = producer.Close(context.Background()) }()
	publishKafkaMessage(t, producer, ctx, topic, "retained")

	first, err := connection.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{topic}, Prefetch: 1, Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Consumer A: %v", err)
	}
	t.Cleanup(func() { closeKafkaConsumer(first) })
	firstConcrete := first.(*consumer)
	cleanupKafkaGroups(t, admin, firstConcrete.group)
	firstMessage := receiveKafkaMessage(t, first)
	if err := firstMessage.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(A): %v", err)
	}

	second, err := connection.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{topic}, Prefetch: 1, Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Consumer B: %v", err)
	}
	t.Cleanup(func() { closeKafkaConsumer(second) })
	secondConcrete := second.(*consumer)
	cleanupKafkaGroups(t, admin, secondConcrete.group)
	lag, err := second.Lag(ctx)
	if err != nil {
		t.Fatalf("Lag(B): %v", err)
	}
	if lag[topic] != 1 {
		t.Fatalf("Lag(B)[%q] = %d, want 1", topic, lag[topic])
	}
	if err := first.Stop(ctx); err != nil {
		t.Fatalf("Stop(A): %v", err)
	}
	secondMessage := receiveKafkaMessage(t, second)
	if err := secondMessage.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(B): %v", err)
	}
	if err := second.Stop(ctx); err != nil {
		t.Fatalf("Stop(B): %v", err)
	}
}

func TestConsumerStopRefusesOutstanding(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "consumer-stop-outstanding")
	group := kafkaTestTopic(t, "consumer-stop-outstanding-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 1)

	producer, err := connection.Producer(ctx, driver.ProducerConfig{Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	defer func() { _ = producer.Close(context.Background()) }()
	publishKafkaMessage(t, producer, ctx, topic, "outstanding")

	consumer, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{topic}, Prefetch: 1, Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() { closeKafkaConsumer(consumer) })
	message := receiveKafkaMessage(t, consumer)

	if err := consumer.Stop(ctx); !errors.Is(err, driver.ErrResourcesOutstanding) {
		t.Fatalf("Stop() error = %v, want ErrResourcesOutstanding", err)
	}
	if err := message.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := consumer.Stop(ctx); err != nil {
		t.Fatalf("Stop after Ack: %v", err)
	}
}

func TestConsumerPrefetchBound(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	first := kafkaTestTopic(t, "consumer-prefetch-first")
	second := kafkaTestTopic(t, "consumer-prefetch-second")
	group := kafkaTestTopic(t, "consumer-prefetch-group")
	cleanupKafkaTopics(t, admin, first, second)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, first, 1)
	createKafkaTopic(t, admin, ctx, second, 1)
	producer, err := connection.Producer(ctx, driver.ProducerConfig{Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	defer func() { _ = producer.Close(context.Background()) }()
	publishKafkaCount(t, producer, ctx, first, 4)
	publishKafkaCount(t, producer, ctx, second, 4)
	consumer, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{first, second}, Prefetch: 2,
		PerDestination: map[string]int{first: 1, second: 1}, Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() { closeKafkaConsumer(consumer) })
	inspect, err := kafkaInspector(connection)
	if err != nil {
		t.Fatalf("Inspector: %v", err)
	}
	waitForKafkaPrefetch(t, inspect, ctx, first, second)

	messages := []driver.InboundMessage{receiveKafkaMessage(t, consumer), receiveKafkaMessage(t, consumer)}
	if messages[0].Destination == messages[1].Destination {
		t.Fatalf("initial deliveries = %q and %q, want both destinations", messages[0].Destination, messages[1].Destination)
	}
	firstMessage := messages[0]
	if err := firstMessage.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(first): %v", err)
	}
	released := receiveKafkaMessage(t, consumer)
	if released.Destination != firstMessage.Destination {
		t.Fatalf("delivery after settling %q = %q, want released destination", firstMessage.Destination, released.Destination)
	}
	if err := released.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(released): %v", err)
	}
	if err := messages[1].Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(second): %v", err)
	}
	secondReleased := receiveKafkaMessage(t, consumer)
	for secondReleased.Destination != messages[1].Destination {
		if err := secondReleased.Settle.Ack(ctx); err != nil {
			t.Fatalf("Ack(interleaved release): %v", err)
		}
		secondReleased = receiveKafkaMessage(t, consumer)
	}
	if err := secondReleased.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(second released): %v", err)
	}
	if err := consumer.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	for deliveries := range 8 {
		firstView, err := inspect(ctx, first)
		if err != nil {
			t.Fatalf("Inspect(first): %v", err)
		}
		secondView, err := inspect(ctx, second)
		if err != nil {
			t.Fatalf("Inspect(second): %v", err)
		}
		if firstView.Unsettled+secondView.Unsettled == 0 {
			break
		}
		message := receiveKafkaMessage(t, consumer)
		if err := message.Settle.Ack(ctx); err != nil {
			t.Fatalf("Ack(remaining): %v", err)
		}
		if deliveries == 7 {
			t.Fatal("prefetch consumer retained unsettled messages after draining")
		}
	}
	if err := consumer.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func closeKafkaConsumer(value driver.Consumer) {
	value.(*consumer).client.Close()
}

func receiveKafkaMessage(t *testing.T, consumer driver.Consumer) driver.InboundMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	select {
	case message, ok := <-consumer.Messages():
		if !ok {
			t.Fatal("Messages closed before delivery")
		}
		return message
	case <-ctx.Done():
		t.Fatalf("receive message: %v", ctx.Err())
		return driver.InboundMessage{}
	}
}

func expectNoKafkaMessage(t *testing.T, consumer driver.Consumer, duration time.Duration) {
	t.Helper()
	timer := time.NewTimer(duration) //nolint:forbidigo // broker pause assertions need a wall-clock silence window
	defer timer.Stop()
	select {
	case message, ok := <-consumer.Messages():
		if ok {
			t.Fatalf("received message while paused: destination=%q body=%q", message.Destination, message.Body)
		}
		t.Fatal("Messages closed while consumer was paused")
	case <-timer.C:
	}
}

func publishKafkaMessage(t *testing.T, producer driver.Producer, ctx context.Context, destination, body string) {
	t.Helper()
	if err := producer.Publish(ctx, driver.OutboundMessage{Destination: destination, Body: []byte(body)}); err != nil {
		t.Fatalf("Publish(%q): %v", destination, err)
	}
}

func publishKafkaCount(t *testing.T, producer driver.Producer, ctx context.Context, destination string, count int) {
	t.Helper()
	for index := range count {
		publishKafkaMessage(t, producer, ctx, destination, fmt.Sprintf("%s-%d", destination, index))
	}
}

func waitForKafkaPrefetch(t *testing.T, inspect func(context.Context, string) (conformance.BrokerView, error), ctx context.Context, first, second string) {
	t.Helper()
	deadline, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond) //nolint:forbidigo // broker polling needs a bounded wall-clock retry
	defer ticker.Stop()
	for {
		firstView, firstErr := inspect(ctx, first)
		secondView, secondErr := inspect(ctx, second)
		if firstErr == nil && secondErr == nil && firstView.Unsettled == 1 && secondView.Unsettled == 1 {
			return
		}
		select {
		case <-deadline.Done():
			t.Fatalf("prefetch did not saturate: first=%+v err=%v second=%+v err=%v", firstView, firstErr, secondView, secondErr)
		case <-ticker.C:
		}
	}
}
