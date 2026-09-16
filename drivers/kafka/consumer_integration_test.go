//go:build integration

package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver/conformance"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestPauseReasonSet(t *testing.T) {
	reasons := []pauseReason{pauseReasonDeferred, pauseReasonPrefetch, pauseReasonAckGap, pauseReasonUserPaused}
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

func openKafkaConsumerTest(t *testing.T, options map[string]string) (context.Context, *conn, *kadm.Client) {
	t.Helper()
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	opened, err := (Driver{}).Open(ctx, driver.Config{
		Endpoints:     []string{kafkaEndpoint},
		ClientID:      "f1-kafka-consumer-test",
		DriverOptions: options,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	connection, ok := opened.(*conn)
	if !ok {
		t.Fatalf("Open() returned %T, want *conn", opened)
	}
	t.Cleanup(func() { _ = connection.Close(context.Background()) })
	return ctx, connection, kadm.NewClient(connection.client)
}

func TestConsumerAckGapPause(t *testing.T) {
	ctx, connection, admin := openKafkaConsumerTest(t, map[string]string{"kafka.maxAckGap": "1"})
	topic := kafkaTestTopic(t, "consumer-ack-gap")
	group := kafkaTestTopic(t, "consumer-ack-gap-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 1)

	producer, err := connection.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true, Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	defer func() { _ = producer.Close(context.Background()) }()
	publishKafkaCount(t, producer, ctx, topic, 3)

	consumerValue, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{topic}, Prefetch: 3, Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() { closeKafkaConsumer(consumerValue) })
	concrete := consumerValue.(*consumer)
	messages := []driver.InboundMessage{
		receiveKafkaMessage(t, consumerValue),
		receiveKafkaMessage(t, consumerValue),
		receiveKafkaMessage(t, consumerValue),
	}
	if err := messages[2].Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(high offset): %v", err)
	}
	concrete.mu.Lock()
	_, paused := concrete.pauseReasons[topic][pauseReasonAckGap]
	concrete.mu.Unlock()
	if !paused {
		t.Fatalf("ack-gap pause reason missing after out-of-order Ack")
	}
	if err := messages[0].Settle.Nack(ctx, driver.NackOptions{Requeue: true}); err != nil {
		t.Fatalf("Nack(first offset): %v", err)
	}
	redelivered := receiveKafkaMessage(t, consumerValue)
	if redelivered.Ref.Offset != messages[0].Ref.Offset {
		t.Fatalf("redelivery offset = %d, want %d", redelivered.Ref.Offset, messages[0].Ref.Offset)
	}
	if err := redelivered.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(redelivery): %v", err)
	}
	concrete.mu.Lock()
	_, paused = concrete.pauseReasons[topic][pauseReasonAckGap]
	concrete.mu.Unlock()
	if paused {
		t.Fatalf("ack-gap pause reason remained after gap returned to bound")
	}
	if err := messages[1].Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(middle offset): %v", err)
	}
	if err := consumerValue.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestConsumerRequeueRedeliversInRun(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "consumer-requeue")
	group := kafkaTestTopic(t, "consumer-requeue-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 1)
	producer, err := connection.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true, Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	defer func() { _ = producer.Close(context.Background()) }()
	publishKafkaMessage(t, producer, ctx, topic, "requeue")

	consumer, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{topic}, Prefetch: 1, Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() { closeKafkaConsumer(consumer) })
	first := receiveKafkaMessage(t, consumer)
	if err := consumer.Pause(topic); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if err := first.Settle.Nack(ctx, driver.NackOptions{Requeue: true}); err != nil {
		t.Fatalf("Nack(requeue): %v", err)
	}
	expectNoKafkaMessage(t, consumer, 200*time.Millisecond)
	if err := consumer.Resume(topic); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	redelivered := receiveKafkaMessage(t, consumer)
	if redelivered.Ref.Offset != first.Ref.Offset || !bytes.Equal(redelivered.Body, first.Body) {
		t.Fatalf("redelivery = offset %d body %q, want offset %d body %q", redelivered.Ref.Offset, redelivered.Body, first.Ref.Offset, first.Body)
	}
	if err := redelivered.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(redelivery): %v", err)
	}
	if err := consumer.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestConsumerDefersFutureRecordWithPositiveControl(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	deferred := kafkaTestTopic(t, "consumer-deferred-control")
	control := kafkaTestTopic(t, "consumer-deferred-control-positive")
	group := kafkaTestTopic(t, "consumer-deferred-control-group")
	cleanupKafkaTopics(t, admin, deferred, control)
	cleanupKafkaGroups(t, admin, group)
	const delay = 2 * time.Second
	if _, err := connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{
			{Name: deferred, Delay: delay},
			{Name: control},
		},
		Effective: connection.Capabilities(),
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	producer, err := connection.Producer(ctx, driver.ProducerConfig{Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(context.Background()) })
	consumer, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{deferred, control}, Prefetch: 2,
		Delays:    map[string]time.Duration{deferred: delay},
		Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() { closeKafkaConsumer(consumer) })

	const controlBody = "control"
	if err := producer.Publish(ctx,
		driver.OutboundMessage{Destination: control, Body: []byte(controlBody)},
		driver.OutboundMessage{Destination: control, Body: []byte(controlBody)},
	); err != nil {
		t.Fatalf("Publish(control): %v", err)
	}
	controlMessage := receiveKafkaMessage(t, consumer)
	if controlMessage.Destination != control {
		t.Fatalf("first delivery destination = %q, want control %q", controlMessage.Destination, control)
	}
	if err := controlMessage.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(control): %v", err)
	}
	due := kafkaNow().Add(delay)
	if err := producer.Publish(ctx, driver.OutboundMessage{
		Destination: deferred, Body: []byte("deferred"), DelayUntil: due,
	}); err != nil {
		t.Fatalf("Publish(deferred): %v", err)
	}
	controlMessage = receiveKafkaMessage(t, consumer)
	if controlMessage.Destination != control || string(controlMessage.Body) != controlBody {
		t.Fatalf("second delivery = %+v, want control %q", controlMessage, controlBody)
	}
	if err := controlMessage.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(second control): %v", err)
	}
	expectNoKafkaMessage(t, consumer, 150*time.Millisecond)
	deferredMessage := receiveKafkaMessageBefore(t, consumer, due.Add(2*time.Second))
	if deferredMessage.Destination != deferred || deferredMessage.ReceivedAt.Before(due) {
		t.Fatalf("deferred delivery = %+v, want %q at or after %s", deferredMessage, deferred, due)
	}
	if err := deferredMessage.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(deferred): %v", err)
	}
}

func TestConsumerLoneDeferredRecordArrivesAtDueTime(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "consumer-deferred-lone")
	group := kafkaTestTopic(t, "consumer-deferred-lone-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	const delay = 25 * time.Second
	if _, err := connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: topic, Delay: delay}},
		Effective:    connection.Capabilities(),
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	producer, err := connection.Producer(ctx, driver.ProducerConfig{Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(context.Background()) })
	consumer, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{topic}, Prefetch: 1,
		Delays:    map[string]time.Duration{topic: delay},
		Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() { closeKafkaConsumer(consumer) })

	due := kafkaNow().Add(delay)
	if err := producer.Publish(ctx, driver.OutboundMessage{
		Destination: topic, Body: []byte("lone"), DelayUntil: due,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	deadlineCtx, deadlineCancel := context.WithDeadline(context.Background(), due.Add(5*time.Second))
	defer deadlineCancel()
	var message driver.InboundMessage
receive:
	for {
		select {
		case message = <-consumer.Messages():
			break receive
		case err := <-consumer.Errors():
			kind, classified := driver.Classify(err)
			if classified && kind == driver.KindNotification {
				continue
			}
			t.Fatalf("consumer error: %v", err)
		case <-deadlineCtx.Done():
			t.Fatalf("receive message before deadline: %v", deadlineCtx.Err())
		}
	}
	if message.ReceivedAt.Before(due) {
		t.Fatalf("delivery at %s, want at or after %s", message.ReceivedAt, due)
	}
	if err := message.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

func TestConsumerRequeueWaitsBehindDeferredRecord(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "consumer-deferred-requeue")
	group := kafkaTestTopic(t, "consumer-deferred-requeue-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	const delay = 500 * time.Millisecond
	if _, err := connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: topic, Delay: delay}},
		Effective:    connection.Capabilities(),
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	producer, err := connection.Producer(ctx, driver.ProducerConfig{Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(context.Background()) })
	consumerValue, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{topic}, Prefetch: 2,
		Delays:    map[string]time.Duration{topic: delay},
		Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() { closeKafkaConsumer(consumerValue) })
	consumer := consumerValue.(*consumer)

	firstDue := kafkaNow().Add(delay)
	if err := producer.Publish(ctx, driver.OutboundMessage{
		Destination: topic, Body: []byte("first"), DelayUntil: firstDue,
	}); err != nil {
		t.Fatalf("Publish(first): %v", err)
	}
	first := receiveKafkaMessage(t, consumerValue)
	secondDue := kafkaNow().Add(delay)
	if err := producer.Publish(ctx, driver.OutboundMessage{
		Destination: topic, Body: []byte("second"), DelayUntil: secondDue,
	}); err != nil {
		t.Fatalf("Publish(second): %v", err)
	}
	waitForDeferredPause(t, consumer, topic)
	if err := first.Settle.Nack(ctx, driver.NackOptions{Requeue: true}); err != nil {
		t.Fatalf("Nack(first): %v", err)
	}
	expectNoKafkaMessage(t, consumerValue, 150*time.Millisecond)

	seen := map[int64]driver.InboundMessage{}
	for range 2 {
		message := receiveKafkaMessageBefore(t, consumerValue, secondDue.Add(2*time.Second))
		seen[message.Ref.Offset] = message
		if err := message.Settle.Ack(ctx); err != nil {
			t.Fatalf("Ack(offset %d): %v", message.Ref.Offset, err)
		}
	}
	if _, ok := seen[first.Ref.Offset]; !ok {
		t.Fatalf("redelivery offsets = %v, want first offset %d", seen, first.Ref.Offset)
	}
}

func TestConsumerCommittedPrefixRestartsAtBase(t *testing.T) {
	ctx, firstConnection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "consumer-prefix-restart")
	group := kafkaTestTopic(t, "consumer-prefix-restart-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 1)
	producer, err := firstConnection.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true, Effective: firstConnection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	defer func() { _ = producer.Close(context.Background()) }()
	publishKafkaCount(t, producer, ctx, topic, 3)

	firstConsumer, err := firstConnection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{topic}, Prefetch: 3, Effective: firstConnection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer before restart: %v", err)
	}
	firstConcrete := firstConsumer.(*consumer)
	firstMessages := []driver.InboundMessage{
		receiveKafkaMessage(t, firstConsumer),
		receiveKafkaMessage(t, firstConsumer),
		receiveKafkaMessage(t, firstConsumer),
	}
	if err := firstMessages[0].Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(prefix): %v", err)
	}
	firstConcrete.client.Close()

	secondOpened, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{kafkaEndpoint}, ClientID: "f1-kafka-prefix-restart"})
	if err != nil {
		t.Fatalf("Open after restart: %v", err)
	}
	secondConnection := secondOpened.(*conn)
	t.Cleanup(func() { _ = secondConnection.Close(context.Background()) })
	secondConsumer, err := secondConnection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{topic}, Prefetch: 3, Effective: secondConnection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer after restart: %v", err)
	}
	t.Cleanup(func() { closeKafkaConsumer(secondConsumer) })
	redelivered := receiveKafkaMessage(t, secondConsumer)
	if redelivered.Ref.Offset != 1 {
		t.Fatalf("first offset after restart = %d, want committed base 1", redelivered.Ref.Offset)
	}
	if err := redelivered.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(first redelivery): %v", err)
	}
	remaining := receiveKafkaMessage(t, secondConsumer)
	if remaining.Ref.Offset != 2 {
		t.Fatalf("second offset after restart = %d, want 2", remaining.Ref.Offset)
	}
	if err := remaining.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(second redelivery): %v", err)
	}
	if err := secondConsumer.Stop(ctx); err != nil {
		t.Fatalf("Stop after restart: %v", err)
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
	if message.Ref.Partition != 0 || message.Ref.Offset != 0 || message.Ref.Tag != 0 || message.Ref.Raw != topic+"/0/0" {
		t.Fatalf("BrokerRef = %+v, want partition 0 offset 0, zero tag, and raw %q", message.Ref, topic+"/0/0")
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

func assertExclusiveConsumerRefused(t *testing.T, err error) {
	t.Helper()
	kind, classified := driver.Classify(err)
	if err == nil || !classified || kind != driver.KindFatal || !strings.Contains(strings.ToLower(err.Error()), "exclusive") {
		t.Fatalf("exclusive consumer error=%v kind=%v classified=%t", err, kind, classified)
	}
}

func cleanupLifecycleConsumer(t *testing.T, ctx context.Context, value driver.Consumer) {
	t.Helper()
	t.Cleanup(func() {
		if err := value.Stop(ctx); err != nil {
			t.Errorf("cleanup Stop: %v", err)
		}
	})
}

func TestConsumerExclusiveAdmission(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "consumer-exclusive-admission")
	otherTopic := kafkaTestTopic(t, "consumer-exclusive-disjoint")
	cleanupKafkaTopics(t, admin, topic, otherTopic)
	createKafkaTopic(t, admin, ctx, topic, 1)
	createKafkaTopic(t, admin, ctx, otherTopic, 1)

	newConsumer := func(t *testing.T, destination string, exclusive bool) driver.Consumer {
		t.Helper()
		value, err := connection.Consumer(ctx, driver.ConsumerConfig{
			Destinations: []string{destination},
			Prefetch:     1,
			Exclusive:    exclusive,
			Effective:    connection.Capabilities(),
		})
		if err != nil {
			t.Fatalf("Consumer(exclusive=%t): %v", exclusive, err)
		}
		cleanupLifecycleConsumer(t, ctx, value)
		return value
	}

	newMultiConsumer := func(t *testing.T, destinations []string, exclusive bool) driver.Consumer {
		t.Helper()
		value, err := connection.Consumer(ctx, driver.ConsumerConfig{
			Destinations: destinations,
			Prefetch:     len(destinations),
			Exclusive:    exclusive,
			Effective:    connection.Capabilities(),
		})
		if err != nil {
			t.Fatalf("Consumer(destinations=%v, exclusive=%t): %v", destinations, exclusive, err)
		}
		cleanupLifecycleConsumer(t, ctx, value)
		return value
	}
	t.Run("exclusive refuses competing exclusive", func(t *testing.T) {
		first := newConsumer(t, topic, true)
		_, err := connection.Consumer(ctx, driver.ConsumerConfig{
			Destinations: []string{topic},
			Prefetch:     1,
			Exclusive:    true,
			Effective:    connection.Capabilities(),
		})
		assertExclusiveConsumerRefused(t, err)

		if err := first.Stop(ctx); err != nil {
			t.Fatalf("Stop(first): %v", err)
		}
		replacement := newConsumer(t, topic, true)
		if err := replacement.Stop(ctx); err != nil {
			t.Fatalf("Stop(replacement): %v", err)
		}
	})

	t.Run("active exclusive refuses incoming nonexclusive", func(t *testing.T) {
		newConsumer(t, topic, true)
		_, err := connection.Consumer(ctx, driver.ConsumerConfig{
			Destinations: []string{topic},
			Prefetch:     1,
			Effective:    connection.Capabilities(),
		})
		assertExclusiveConsumerRefused(t, err)
	})

	t.Run("active nonexclusive refuses incoming exclusive", func(t *testing.T) {
		newConsumer(t, topic, false)
		_, err := connection.Consumer(ctx, driver.ConsumerConfig{
			Destinations: []string{topic},
			Prefetch:     1,
			Exclusive:    true,
			Effective:    connection.Capabilities(),
		})
		assertExclusiveConsumerRefused(t, err)
	})

	t.Run("multi-destination overlap is refused", func(t *testing.T) {
		newMultiConsumer(t, []string{topic, otherTopic}, true)
		_, err := connection.Consumer(ctx, driver.ConsumerConfig{
			Destinations: []string{otherTopic},
			Prefetch:     1,
			Effective:    connection.Capabilities(),
		})
		assertExclusiveConsumerRefused(t, err)
	})

	t.Run("nonexclusive consumers may share a destination", func(t *testing.T) {
		newConsumer(t, topic, false)
		newConsumer(t, topic, false)
	})

	t.Run("exclusive consumers may use disjoint destinations", func(t *testing.T) {
		newConsumer(t, topic, true)
		newConsumer(t, otherTopic, true)
	})

	t.Run("separate connections admit independent claims", func(t *testing.T) {
		opened, err := (Driver{}).Open(ctx, driver.Config{
			Endpoints: []string{kafkaEndpoint},
			ClientID:  "f1-kafka-exclusive-separate-connection",
		})
		if err != nil {
			t.Fatalf("Open(second connection): %v", err)
		}
		secondConnection := opened.(*conn)
		t.Cleanup(func() { _ = secondConnection.Close(ctx) })

		newConsumer(t, topic, true)
		second, err := secondConnection.Consumer(ctx, driver.ConsumerConfig{
			Destinations: []string{topic},
			Prefetch:     1,
			Exclusive:    true,
			Effective:    secondConnection.Capabilities(),
		})
		if err != nil {
			t.Fatalf("Consumer(second connection): %v", err)
		}
		cleanupLifecycleConsumer(t, ctx, second)
	})
}

func TestConsumerExclusiveClaimReleased(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "consumer-exclusive-release")
	cleanupKafkaTopics(t, admin, topic)
	createKafkaTopic(t, admin, ctx, topic, 1)

	newConsumer := func(t *testing.T) driver.Consumer {
		t.Helper()
		value, err := connection.Consumer(ctx, driver.ConsumerConfig{
			Destinations: []string{topic},
			Prefetch:     1,
			Exclusive:    true,
			Effective:    connection.Capabilities(),
		})
		if err != nil {
			t.Fatalf("exclusive Consumer: %v", err)
		}
		cleanupLifecycleConsumer(t, ctx, value)
		return value
	}

	t.Run("stop releases claim", func(t *testing.T) {
		first := newConsumer(t)
		if err := first.Stop(ctx); err != nil {
			t.Fatalf("Stop(first): %v", err)
		}
		replacement := newConsumer(t)
		if err := replacement.Stop(ctx); err != nil {
			t.Fatalf("Stop(replacement): %v", err)
		}
	})

	t.Run("release releases claim", func(t *testing.T) {
		first := newConsumer(t)
		if err := first.Release(ctx); err != nil {
			t.Fatalf("Release(first): %v", err)
		}
		replacement := newConsumer(t)
		if err := replacement.Stop(ctx); err != nil {
			t.Fatalf("Stop(replacement): %v", err)
		}
	})

	t.Run("concurrent construction admits one", func(t *testing.T) {
		type result struct {
			value driver.Consumer
			err   error
		}
		const attempts = 8
		results := make(chan result, attempts)
		var group sync.WaitGroup
		group.Add(attempts)
		for range attempts {
			go func() {
				defer group.Done()
				value, err := connection.Consumer(ctx, driver.ConsumerConfig{
					Destinations: []string{topic},
					Prefetch:     1,
					Exclusive:    true,
					Effective:    connection.Capabilities(),
				})
				results <- result{value: value, err: err}
			}()
		}
		group.Wait()
		close(results)

		var admitted []driver.Consumer
		for result := range results {
			if result.err != nil {
				assertExclusiveConsumerRefused(t, result.err)
				continue
			}
			cleanupLifecycleConsumer(t, ctx, result.value)
			admitted = append(admitted, result.value)
		}
		if len(admitted) != 1 {
			t.Fatalf("concurrent exclusive consumers admitted = %d, want 1", len(admitted))
		}
		if err := admitted[0].Stop(ctx); err != nil {
			t.Fatalf("Stop(admitted): %v", err)
		}
		replacement := newConsumer(t)
		if err := replacement.Stop(ctx); err != nil {
			t.Fatalf("Stop(after concurrent admission): %v", err)
		}
	})
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

func TestConsumerPerDestinationShareRefillsOnSettlement(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	first := kafkaTestTopic(t, "consumer-refill-first")
	second := kafkaTestTopic(t, "consumer-refill-second")
	group := kafkaTestTopic(t, "consumer-refill-group")
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
		Group: group, Destinations: []string{first, second}, Prefetch: 4,
		PerDestination: map[string]int{first: 1, second: 3}, Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() { closeKafkaConsumer(consumer) })
	publishKafkaCount(t, producer, ctx, first, 3)
	publishKafkaCount(t, producer, ctx, second, 5)
	inspect, err := kafkaInspector(connection)
	if err != nil {
		t.Fatalf("Inspector: %v", err)
	}
	waitForKafkaShares(t, inspect, ctx, map[string]int{first: 1, second: 3})
	// Explicit initial 1/3 categorization asserting share saturation.
	initial := make(map[string][]driver.InboundMessage)
	for range 4 {
		msg := receiveKafkaMessage(t, consumer)
		initial[msg.Destination] = append(initial[msg.Destination], msg)
	}
	if len(initial[first]) != 1 || len(initial[second]) != 3 {
		t.Fatalf("initial deliveries by destination: first=%d second=%d, want 1 and 3", len(initial[first]), len(initial[second]))
	}

	// Settle exactly one message on each destination.
	if err := initial[first][0].Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(first initial): %v", err)
	}
	if err := initial[second][0].Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(second initial): %v", err)
	}

	// Assert immediate per-destination refill: one further message delivered from each.
	deadline := kafkaNow().Add(2 * time.Second)
	refills := make(map[string]driver.InboundMessage)
	for range 2 {
		msg := receiveKafkaMessageBefore(t, consumer, deadline)
		refills[msg.Destination] = msg
	}
	refilledFirst, hasFirst := refills[first]
	if !hasFirst {
		t.Fatalf("further message not delivered on %q after initial settlement", first)
	}
	refilledSecond, hasSecond := refills[second]
	if !hasSecond {
		t.Fatalf("further message not delivered on %q after initial settlement", second)
	}

	// Settle destination second's refilled message and its remaining initial messages.
	if err := refilledSecond.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(second refilled): %v", err)
	}
	for i, msg := range initial[second][1:] {
		if err := msg.Settle.Ack(ctx); err != nil {
			t.Fatalf("Ack(initial second %d): %v", i+1, err)
		}
	}

	// Drain the remaining message on second so that second is completely exhausted on the broker.
	secondFinal := receiveKafkaMessageBefore(t, consumer, deadline)
	if secondFinal.Destination != second {
		t.Fatalf("received %q, want final %q", secondFinal.Destination, second)
	}
	if err := secondFinal.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(second final): %v", err)
	}

	// Destination second is now exhausted on the broker and idle/unpaused.
	// Only now acknowledge first's refilled message. Its final message must be delivered promptly.
	// Without FetchMaxWait configured, franz-go issues a fetch request for the exhausted
	// destination second which Kafka holds for 5 seconds, starving first and failing this 2s deadline.
	if err := refilledFirst.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(first refilled): %v", err)
	}
	firstFinal := receiveKafkaMessageBefore(t, consumer, kafkaNow().Add(2*time.Second))
	if firstFinal.Destination != first {
		t.Fatalf("received %q, want final %q", firstFinal.Destination, first)
	}
	if err := firstFinal.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(first final): %v", err)
	}

	if err := consumer.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func waitForKafkaShares(t *testing.T, inspect func(context.Context, string) (conformance.BrokerView, error), ctx context.Context, shares map[string]int) {
	t.Helper()
	deadline, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond) //nolint:forbidigo // broker polling needs a bounded wall-clock retry
	defer ticker.Stop()
	for {
		allMatch := true
		for destination, share := range shares {
			view, err := inspect(ctx, destination)
			if err != nil || view.Unsettled != int64(share) {
				allMatch = false
				break
			}
		}
		if allMatch {
			return
		}
		select {
		case <-deadline.Done():
			t.Fatalf("prefetch shares did not saturate to %v", shares)
		case <-ticker.C:
		}
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

func receiveKafkaMessageBefore(t *testing.T, consumer driver.Consumer, deadline time.Time) driver.InboundMessage {
	t.Helper()
	timeout := time.Until(deadline)
	if timeout <= 0 {
		timeout = time.Millisecond
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	select {
	case message, ok := <-consumer.Messages():
		if !ok {
			t.Fatal("Messages closed before delivery")
		}
		return message
	case <-ctx.Done():
		t.Fatalf("receive message before deadline: %v", ctx.Err())
		return driver.InboundMessage{}
	}
}

func waitForDeferredPause(t *testing.T, value driver.Consumer, destination string) {
	t.Helper()
	consumer := value.(*consumer)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond) //nolint:forbidigo // broker polling needs a bounded wall-clock retry
	defer ticker.Stop()
	for {
		consumer.mu.Lock()
		_, paused := consumer.pauseReasons[destination][pauseReasonDeferred]
		consumer.mu.Unlock()
		if paused {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("deferred pause was not set: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestConsumerReleaseAbandonsUnsettledAndCloses(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "consumer-release-abandons")
	group := kafkaTestTopic(t, "consumer-release-abandons-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 1)

	producer, err := connection.Producer(ctx, driver.ProducerConfig{Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	defer func() { _ = producer.Close(context.Background()) }()
	publishKafkaMessage(t, producer, ctx, topic, "unsettled-payload")

	csm, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{topic}, Prefetch: 1, Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer closeKafkaConsumer(csm)

	message := receiveKafkaMessage(t, csm)
	if string(message.Body) != "unsettled-payload" {
		t.Fatalf("received body = %q, want %q", message.Body, "unsettled-payload")
	}

	// Release must not refuse on outstanding settlers, and must not commit.
	if err := csm.Release(ctx); err != nil {
		t.Fatalf("Release() error = %v", err)
	}

	// Messages must be closed after Release.
	select {
	case _, ok := <-csm.Messages():
		if ok {
			t.Fatal("Messages remains open after Release")
		}
	case <-time.After(5 * time.Second): //nolint:forbidigo // bounded wait for Messages channel close
		t.Fatal("timed out waiting for Messages to close after Release")
	}

	// A second consumer in the same group must receive the abandoned work.
	receiver, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{topic}, Prefetch: 1, Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Receiver Consumer: %v", err)
	}
	defer closeKafkaConsumer(receiver)

	redelivered := receiveKafkaMessage(t, receiver)
	if string(redelivered.Body) != "unsettled-payload" {
		t.Fatalf("redelivered body = %q, want %q", redelivered.Body, "unsettled-payload")
	}
	if err := redelivered.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := receiver.Stop(ctx); err != nil {
		t.Fatalf("Stop after Ack: %v", err)
	}
}

func TestConsumerReleaseIdempotencyAndMutualStopSafety(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "consumer-release-idempotent")
	group := kafkaTestTopic(t, "consumer-release-idempotent-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 1)

	// Release twice then Stop.
	csm1, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{topic}, Prefetch: 1, Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer 1: %v", err)
	}
	defer closeKafkaConsumer(csm1)

	if err := csm1.Release(ctx); err != nil {
		t.Fatalf("first Release() error = %v", err)
	}
	if err := csm1.Release(ctx); err != nil {
		t.Fatalf("second Release() error = %v", err)
	}
	if err := csm1.Stop(ctx); err != nil {
		t.Fatalf("Stop() after Release error = %v", err)
	}

	// Stop then Release.
	csm2, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{topic}, Prefetch: 1, Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer 2: %v", err)
	}
	defer closeKafkaConsumer(csm2)

	if err := csm2.Stop(ctx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := csm2.Release(ctx); err != nil {
		t.Fatalf("Release() after Stop error = %v", err)
	}
}

func TestConsumerDrainSaturatedEmitDoesNotDeliverAfterDrain(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "consumer-drain-saturated")
	group := kafkaTestTopic(t, "consumer-drain-saturated-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 1)

	producer, err := connection.Producer(ctx, driver.ProducerConfig{Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	defer func() { _ = producer.Close(context.Background()) }()

	csm, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{topic}, Prefetch: 1, Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer closeKafkaConsumer(csm)

	c := csm.(*consumer)

	// Saturate the emit path by pre-filling the delivery channel buffer to capacity.
	// The test intentionally never reads from Messages prior to or during Drain.
	c.messages <- driver.InboundMessage{Destination: topic, Body: []byte("prefilled")}

	// Publish a broker record. The consumer polls it and calls emit, where it parks
	// on the blocking send c.messages <- message because the buffer is saturated.
	publishKafkaMessage(t, producer, ctx, topic, "parked-record")

	// Wait until the sender is parked in emit: settler is tracked, buffer is full.
	waitForKafkaConsumerState(t, c, "sender parked mid-send", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.settlers) == 1 && len(c.messages) == cap(c.messages)
	})

	drainCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	drainErr := csm.Drain(drainCtx)

	// Read only the prefilled message that was placed before Drain.
	select {
	case msg, ok := <-c.messages:
		if !ok || string(msg.Body) != "prefilled" {
			t.Fatalf("expected prefilled message, got %v (ok=%t)", msg, ok)
		}
	default:
		t.Fatal("expected prefilled message in delivery channel")
	}

	// Verify that Messages yields nothing new after Drain returned.
	select {
	case msg := <-c.messages:
		t.Fatalf("observed delivery after Drain returned: %s", msg.Body)
	case <-time.After(100 * time.Millisecond): //nolint:forbidigo // bounded assertion that Messages yields nothing
	}

	if drainErr != nil {
		t.Fatalf("Drain() error = %v", drainErr)
	}
}

func TestConsumerReleaseFencesLateSettlement(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "consumer-release-fenced")
	group := kafkaTestTopic(t, "consumer-release-fenced-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 1)

	producer, err := connection.Producer(ctx, driver.ProducerConfig{Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	defer func() { _ = producer.Close(context.Background()) }()
	publishKafkaMessage(t, producer, ctx, topic, "fenced-payload")

	csm, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{topic}, Prefetch: 1, Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer closeKafkaConsumer(csm)

	message := receiveKafkaMessage(t, csm)
	if string(message.Body) != "fenced-payload" {
		t.Fatalf("received body = %q, want %q", message.Body, "fenced-payload")
	}

	if err := csm.Release(ctx); err != nil {
		t.Fatalf("Release() error = %v", err)
	}

	// Late settlement must be fenced and rejected.
	if err := message.Settle.Ack(ctx); !errors.Is(err, ErrRevoked) {
		t.Fatalf("late Ack() error = %v, want ErrRevoked", err)
	}

	// A second consumer in the same group must receive the abandoned work intact.
	receiver, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{topic}, Prefetch: 1, Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Receiver Consumer: %v", err)
	}
	defer closeKafkaConsumer(receiver)

	redelivered := receiveKafkaMessage(t, receiver)
	if string(redelivered.Body) != "fenced-payload" {
		t.Fatalf("redelivered body = %q, want %q", redelivered.Body, "fenced-payload")
	}
	if err := redelivered.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := receiver.Stop(ctx); err != nil {
		t.Fatalf("Stop after Ack: %v", err)
	}
}

func waitForKafkaConsumerState(t *testing.T, consumer *consumer, description string, check func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond) //nolint:forbidigo // state polling needs a bounded wall-clock retry
	defer ticker.Stop()
	for {
		if check() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s: %v", description, ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestConsumerStaticMembershipOptionInspection(t *testing.T) {
	conn := &conn{
		clientOpts: []kgo.Opt{noDialKafkaOption()},
	}
	cfg := driver.ConsumerConfig{Group: "group", Destinations: []string{"topic"}}

	t.Run("static membership true with non-empty instance id", func(t *testing.T) {
		conn.staticMembership = true
		conn.instanceID = "worker-1"
		opts, err := consumerClientOpts(conn, cfg, "group", nil)
		if err != nil {
			t.Fatalf("consumerClientOpts error = %v", err)
		}
		cl, err := kgo.NewClient(opts...)
		if err != nil {
			t.Fatalf("NewClient error = %v", err)
		}
		defer cl.Close()
		vals := cl.OptValues(kgo.InstanceID)
		if len(vals) != 2 || vals[0] != "worker-1" || vals[1] != true {
			t.Fatalf("kgo.InstanceID = %v, want [worker-1 true]", vals)
		}
	})

	t.Run("static membership false with non-empty instance id", func(t *testing.T) {
		conn.staticMembership = false
		conn.instanceID = "worker-1"
		opts, err := consumerClientOpts(conn, cfg, "group", nil)
		if err != nil {
			t.Fatalf("consumerClientOpts error = %v", err)
		}
		cl, err := kgo.NewClient(opts...)
		if err != nil {
			t.Fatalf("NewClient error = %v", err)
		}
		defer cl.Close()
		vals := cl.OptValues(kgo.InstanceID)
		if len(vals) == 2 && vals[1] == true {
			t.Fatalf("kgo.InstanceID unexpectedly enabled when staticMembership is false: %v", vals)
		}
	})

	t.Run("static membership true with empty instance id", func(t *testing.T) {
		conn.staticMembership = true
		conn.instanceID = ""
		opts, err := consumerClientOpts(conn, cfg, "group", nil)
		if err != nil {
			t.Fatalf("consumerClientOpts error = %v", err)
		}
		cl, err := kgo.NewClient(opts...)
		if err != nil {
			t.Fatalf("NewClient error = %v", err)
		}
		defer cl.Close()
		vals := cl.OptValues(kgo.InstanceID)
		if len(vals) == 2 && vals[1] == true {
			t.Fatalf("kgo.InstanceID unexpectedly enabled when instanceID is empty: %v", vals)
		}
	})
}

func TestConsumerRevokeWaitsForSettlerInsideBound(t *testing.T) {
	key := partitionKey{destination: "topic", partition: 0}
	tracker := newAckTracker(0, 1)
	if err := tracker.Track(0); err != nil {
		t.Fatalf("Track: %v", err)
	}

	c := &consumer{
		rebalanceDrainTimeout: 500 * time.Millisecond,
		clock:                 clock.NewReal(),
		trackers:              map[partitionKey]*ackTracker{key: tracker},
		activeGenerations:     map[partitionKey]uint64{key: 1},
		fenced:                make(map[partitionKey]bool),
		settlers:              make(map[*settler]struct{}),
		settlerCh:             make(chan struct{}, 1),
		unsettled:             map[string]int{"topic": 1},
		budgets:               map[string]int{"topic": 1},
		pauseReasons:          make(map[string]pauseReasonSet),
	}
	s := &settler{
		owner:   c,
		record:  &kgo.Record{Topic: "topic", Partition: 0, Offset: 0},
		tracker: tracker,
		key:     key,
	}
	c.settlers[s] = struct{}{}

	revokeDone := make(chan struct{})
	start := time.Now() //nolint:forbidigo // timing test measuring duration
	go func() {
		c.onPartitionsRevoked(context.Background(), nil, map[string][]int32{"topic": {0}})
		close(revokeDone)
	}()

	// Handshake: wait until onPartitionsRevoked enters wait (fenced is set).
	waitForKafkaConsumerState(t, c, "partition to become fenced", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.fenced[key]
	})

	// Settle inside the bound.
	c.completeSettlement(s, false)

	select {
	case <-revokeDone:
	case <-time.After(1 * time.Second): //nolint:forbidigo // bounded wait for revoke completion
		t.Fatal("onPartitionsRevoked timed out waiting for settler settlement")
	}

	elapsed := time.Since(start) //nolint:forbidigo // timing test duration assertion
	if elapsed >= 450*time.Millisecond {
		t.Fatalf("revoke waited too long: %v, want return immediately after settlement", elapsed)
	}

	// Verify tracker is dropped and Ack after drop returns ErrRevoked.
	if err := tracker.Ack(0, nil); !errors.Is(err, ErrRevoked) {
		t.Fatalf("Ack after drop = %v, want ErrRevoked", err)
	}
}

func TestConsumerRevokeTombstonesAfterBound(t *testing.T) {
	key := partitionKey{destination: "topic", partition: 0}
	tracker := newAckTracker(0, 1)
	if err := tracker.Track(0); err != nil {
		t.Fatalf("Track: %v", err)
	}

	c := &consumer{
		rebalanceDrainTimeout: 60 * time.Millisecond,
		clock:                 clock.NewReal(),
		trackers:              map[partitionKey]*ackTracker{key: tracker},
		activeGenerations:     map[partitionKey]uint64{key: 1},
		fenced:                make(map[partitionKey]bool),
		settlers:              make(map[*settler]struct{}),
		settlerCh:             make(chan struct{}, 1),
		unsettled:             map[string]int{"topic": 1},
		budgets:               map[string]int{"topic": 1},
		pauseReasons:          make(map[string]pauseReasonSet),
	}
	s := &settler{
		owner:   c,
		record:  &kgo.Record{Topic: "topic", Partition: 0, Offset: 0},
		tracker: tracker,
		key:     key,
	}
	c.settlers[s] = struct{}{}

	start := time.Now() //nolint:forbidigo // timing test measuring duration
	revokeDone := make(chan struct{})
	go func() {
		c.onPartitionsRevoked(context.Background(), nil, map[string][]int32{"topic": {0}})
		close(revokeDone)
	}()

	// Outer bound: 400ms is well under 25s, so mutation 3 fails on its first run.
	select {
	case <-revokeDone:
	case <-time.After(400 * time.Millisecond): //nolint:forbidigo // bounded wait for revoke timeout
		t.Fatal("onPartitionsRevoked did not return after configured 60ms bound")
	}

	elapsed := time.Since(start) //nolint:forbidigo // timing test duration assertion
	if elapsed < 50*time.Millisecond || elapsed > 350*time.Millisecond {
		t.Fatalf("onPartitionsRevoked elapsed = %v, want ~60ms", elapsed)
	}

	// Verify tracker is tombstoned after bound expired.
	if err := tracker.Ack(0, nil); !errors.Is(err, ErrRevoked) {
		t.Fatalf("Ack after bound expired = %v, want ErrRevoked", err)
	}
}

func TestWaitForSettlersTimesOutOnFakeClock(t *testing.T) {
	key := partitionKey{destination: "topic", partition: 0}
	waitForFakeTimer := func(t *testing.T, fake *clock.Fake) {
		t.Helper()
		guard := clock.NewReal().Timer(time.Second)
		defer guard.Stop()
		for fake.NumWaiters() == 0 {
			select {
			case <-guard.C:
				t.Fatal("waitForSettlers did not register its fake timer")
			default:
				runtime.Gosched()
			}
		}
	}

	t.Run("settlement releases the wait", func(t *testing.T) {
		fake := clock.NewFake(time.Unix(0, 0))
		s := &settler{key: key}
		c := &consumer{
			clock:     fake,
			settlerCh: make(chan struct{}, 1),
			settlers:  map[*settler]struct{}{s: {}},
		}
		done := make(chan struct{})
		go func() {
			c.waitForSettlers(map[partitionKey]struct{}{key: {}}, time.Second)
			close(done)
		}()
		waitForFakeTimer(t, fake)

		c.mu.Lock()
		delete(c.settlers, s)
		c.mu.Unlock()
		c.settlerCh <- struct{}{}

		select {
		case <-done:
		case <-clock.NewReal().Timer(time.Second).C:
			t.Fatal("waitForSettlers did not return after settlers cleared")
		}
	})

	t.Run("timeout advances the fake clock", func(t *testing.T) {
		fake := clock.NewFake(time.Unix(0, 0))
		s := &settler{key: key}
		c := &consumer{
			clock:     fake,
			settlerCh: make(chan struct{}, 1),
			settlers:  map[*settler]struct{}{s: {}},
		}
		done := make(chan struct{})
		go func() {
			c.waitForSettlers(map[partitionKey]struct{}{key: {}}, 60*time.Millisecond)
			close(done)
		}()
		waitForFakeTimer(t, fake)

		select {
		case <-done:
			t.Fatal("waitForSettlers returned before fake clock advance")
		default:
		}
		fake.Advance(60 * time.Millisecond)

		select {
		case <-done:
		case <-clock.NewReal().Timer(time.Second).C:
			t.Fatal("waitForSettlers did not return after fake clock advance")
		}
	})
}

func TestConsumerLostDropsImmediatelyWithoutWaiting(t *testing.T) {
	key := partitionKey{destination: "topic", partition: 0}
	tracker := newAckTracker(0, 1)
	if err := tracker.Track(0); err != nil {
		t.Fatalf("Track: %v", err)
	}

	c := &consumer{
		rebalanceDrainTimeout: 5 * time.Second,
		trackers:              map[partitionKey]*ackTracker{key: tracker},
		activeGenerations:     map[partitionKey]uint64{key: 1},
		fenced:                make(map[partitionKey]bool),
		settlers:              make(map[*settler]struct{}),
		settlerCh:             make(chan struct{}, 1),
		unsettled:             map[string]int{"topic": 1},
		budgets:               map[string]int{"topic": 1},
		pauseReasons:          make(map[string]pauseReasonSet),
	}
	s := &settler{
		owner:   c,
		record:  &kgo.Record{Topic: "topic", Partition: 0, Offset: 0},
		tracker: tracker,
		key:     key,
	}
	c.settlers[s] = struct{}{}

	start := time.Now() //nolint:forbidigo // timing test measuring duration
	c.onPartitionsLost(context.Background(), nil, map[string][]int32{"topic": {0}})
	elapsed := time.Since(start) //nolint:forbidigo // timing test duration assertion

	if elapsed > 100*time.Millisecond {
		t.Fatalf("onPartitionsLost took %v, want immediate return without waiting for drain timeout", elapsed)
	}

	if err := tracker.Ack(0, nil); !errors.Is(err, ErrRevoked) {
		t.Fatalf("Ack after lost = %v, want ErrRevoked", err)
	}
}

func TestConsumerBufferedRecordFencedAfterReassignment(t *testing.T) {
	key := partitionKey{destination: "topic", partition: 0}
	c := &consumer{
		trackers:              make(map[partitionKey]*ackTracker),
		activeGenerations:     make(map[partitionKey]uint64),
		assignmentGenerations: make(map[partitionKey]uint64),
		fenced:                make(map[partitionKey]bool),
		recordGenerations:     make(map[*kgo.Record]uint64),
		settlers:              make(map[*settler]struct{}),
		messages:              make(chan driver.InboundMessage, 10),
		budgets:               map[string]int{"topic": 10},
		unsettled:             map[string]int{"topic": 0},
		pauseReasons:          make(map[string]pauseReasonSet),
		clock:                 clock.NewFake(time.Unix(0, 0)),
	}

	// 1. Initial assignment at generation 1.
	c.onPartitionsAssigned(context.Background(), nil, map[string][]int32{"topic": {0}})
	if c.activeGenerations[key] != 1 {
		t.Fatalf("active generation = %d, want 1", c.activeGenerations[key])
	}

	// 2. Poll fetches record at generation 1.
	record := &kgo.Record{Topic: "topic", Partition: 0, Offset: 42, Value: []byte("payload")}
	c.tagRecord(record)

	// 3. Partition is revoked and reassigned at generation 2.
	c.onPartitionsRevoked(context.Background(), nil, map[string][]int32{"topic": {0}})
	c.onPartitionsAssigned(context.Background(), nil, map[string][]int32{"topic": {0}})
	if c.activeGenerations[key] != 2 {
		t.Fatalf("active generation after reassignment = %d, want 2", c.activeGenerations[key])
	}

	// 4. Pending buffer holds the old-generation record.
	pending := []*kgo.Record{record}
	if !c.flushPending(&pending) {
		t.Fatal("flushPending returned false")
	}

	// 5. The old-generation record must be discarded from pending and not emitted.
	if len(pending) != 0 {
		t.Fatalf("pending length = %d, want 0 (stale record discarded)", len(pending))
	}
	if len(c.messages) != 0 {
		t.Fatalf("delivered messages = %d, want 0 (fenced from delivery)", len(c.messages))
	}
}

func TestConsumerUnassignedRecordIsDiscarded(t *testing.T) {
	key := partitionKey{destination: "topic", partition: 0}
	c := &consumer{
		trackers:          make(map[partitionKey]*ackTracker),
		activeGenerations: make(map[partitionKey]uint64),
		fenced:            make(map[partitionKey]bool),
		recordGenerations: make(map[*kgo.Record]uint64),
		settlers:          make(map[*settler]struct{}),
		messages:          make(chan driver.InboundMessage, 1),
		budgets:           map[string]int{"topic": 10},
		unsettled:         map[string]int{"topic": 0},
		pauseReasons:      make(map[string]pauseReasonSet),
		clock:             clock.NewFake(time.Unix(0, 0)),
	}
	pending := []*kgo.Record{{Topic: key.destination, Partition: key.partition, Offset: 7, Value: []byte("unassigned")}}
	c.tagRecord(pending[0])
	if got, tagged := c.recordGenerations[pending[0]]; !tagged || got != 0 {
		t.Fatalf("unassigned record tag = (%d, %t), want (0, true)", got, tagged)
	}
	if !c.flushPending(&pending) {
		t.Fatal("flushPending returned false")
	}
	if len(pending) != 0 {
		t.Fatalf("pending length = %d, want 0 for unassigned record", len(pending))
	}
	if len(c.trackers) != 0 {
		t.Fatalf("trackers = %d, want 0 for unassigned record", len(c.trackers))
	}
	if len(c.activeGenerations) != 0 {
		t.Fatalf("active generations = %d, want 0 for unassigned record", len(c.activeGenerations))
	}
	if len(c.messages) != 0 {
		t.Fatalf("delivered messages = %d, want 0 for unassigned record", len(c.messages))
	}
}

func TestConsumerUnassignedRecordCannotReviveAfterAssignment(t *testing.T) {
	key := partitionKey{destination: "topic", partition: 0}
	record := &kgo.Record{Topic: key.destination, Partition: key.partition, Offset: 7, Value: []byte("unassigned")}
	c := &consumer{
		trackers:              make(map[partitionKey]*ackTracker),
		activeGenerations:     make(map[partitionKey]uint64),
		assignmentGenerations: make(map[partitionKey]uint64),
		fenced:                make(map[partitionKey]bool),
		recordGenerations:     make(map[*kgo.Record]uint64),
		settlers:              make(map[*settler]struct{}),
		messages:              make(chan driver.InboundMessage, 1),
		budgets:               map[string]int{"topic": 1},
		unsettled:             map[string]int{"topic": 0},
		pauseReasons:          make(map[string]pauseReasonSet),
		clock:                 clock.NewFake(time.Unix(0, 0)),
	}
	c.tagRecord(record)
	if got, tagged := c.recordGenerations[record]; !tagged || got != 0 {
		t.Fatalf("unassigned record tag = (%d, %t), want (0, true)", got, tagged)
	}
	c.onPartitionsAssigned(context.Background(), nil, map[string][]int32{"topic": {0}})
	if got := c.activeGenerations[key]; got != 1 {
		t.Fatalf("active generation = %d, want 1", got)
	}

	pending := []*kgo.Record{record}
	if !c.flushPending(&pending) {
		t.Fatal("flushPending returned false")
	}
	if len(pending) != 0 {
		t.Fatalf("pending length = %d, want 0 after assignment", len(pending))
	}
	if len(c.messages) != 0 {
		t.Fatalf("delivered messages = %d, want 0 after assignment", len(c.messages))
	}
	if len(c.trackers) != 0 {
		t.Fatalf("trackers = %d, want 0 after assignment", len(c.trackers))
	}
	if got := c.activeGenerations[key]; got != 1 {
		t.Fatalf("active generation after discard = %d, want 1", got)
	}
}

func TestConsumerOnePartitionChangeDoesNotResetDestinationAccounting(t *testing.T) {
	key0 := partitionKey{destination: "topic", partition: 0}
	key1 := partitionKey{destination: "topic", partition: 1}
	tracker0 := newAckTracker(0, 1)
	tracker1 := newAckTracker(0, 1)

	for i := range int64(3) {
		_ = tracker0.Track(i)
	}
	for i := range int64(4) {
		_ = tracker1.Track(i)
	}

	c := &consumer{
		trackers:          map[partitionKey]*ackTracker{key0: tracker0, key1: tracker1},
		activeGenerations: map[partitionKey]uint64{key0: 1, key1: 1},
		fenced:            make(map[partitionKey]bool),
		settlers:          make(map[*settler]struct{}),
		unsettled:         map[string]int{"topic": 7},
		budgets:           map[string]int{"topic": 10},
		pauseReasons:      make(map[string]pauseReasonSet),
	}

	// Drop only partition 0.
	c.dropTracker("topic", 0)

	if got := c.unsettled["topic"]; got != 4 {
		t.Fatalf("destination unsettled after dropping partition 0 = %d, want 4 (not reset to 0)", got)
	}
	if c.trackers[key1] != tracker1 {
		t.Fatal("partition 1 tracker was unexpectedly removed")
	}
	if err := tracker1.Ack(0, nil); err != nil {
		t.Fatalf("partition 1 Ack: %v, want success", err)
	}
}

func TestConsumerNotificationDoesNotDisplaceQueuedFailure(t *testing.T) {
	fatalCause := errors.New("fatal consumer failure")
	const errorCapacity = 8
	c := &consumer{errors: make(chan error, errorCapacity)}
	c.sendError(classify("consumer", driver.KindFatal, fatalCause))
	for sequence := range errorCapacity - 1 {
		c.sendError(classify("consumer", driver.KindTransient, fmt.Errorf("transient consumer failure %d", sequence)))
	}

	c.sendRebalanceError("assigned", map[string][]int32{"topic": {0}})
	fatalSurvived := false
	notificationObserved := false
	for range errorCapacity {
		err := <-c.Errors()
		kind, classified := driver.Classify(err)
		if classified && kind == driver.KindFatal {
			fatalSurvived = true
		}
		if classified && kind == driver.KindNotification {
			notificationObserved = true
		}
	}
	if !fatalSurvived {
		t.Fatal("queued fatal error was displaced by a notification")
	}
	if notificationObserved {
		t.Fatal("full error channel delivered a notification")
	}

	c.sendRebalanceError("assigned", map[string][]int32{"topic": {0}})
	select {
	case err := <-c.Errors():
		kind, classified := driver.Classify(err)
		if !classified || kind != driver.KindNotification {
			t.Fatalf("empty error channel received (%v, %t), want notification", kind, classified)
		}
	default:
		t.Fatal("notification was not delivered when error channel had capacity")
	}
}

func TestConsumerNotificationsObservableAndQuietOnClose(t *testing.T) {
	c := &consumer{
		errors:       make(chan error, 8),
		pauseReasons: make(map[string]pauseReasonSet),
		trackers:     make(map[partitionKey]*ackTracker),
		fenced:       make(map[partitionKey]bool),
		settlers:     make(map[*settler]struct{}),
		clock:        clock.NewFake(time.Unix(0, 0)),
	}
	c.onPartitionsAssigned(context.Background(), nil, map[string][]int32{"topic": {0}})
	assertNotification := func(name string) {
		t.Helper()
		select {
		case err := <-c.Errors():
			kind, classified := driver.Classify(err)
			if !classified || kind != driver.KindNotification {
				t.Fatalf("expected %s notification kind, got %v (kind=%v classified=%t)", name, err, kind, classified)
			}
			if !strings.Contains(err.Error(), name) {
				t.Fatalf("expected %s notification message, got %v", name, err)
			}
		default:
			t.Fatalf("expected %s notification on Errors", name)
		}
	}
	assertNotification("assigned")

	c.onPartitionsRevoked(context.Background(), nil, map[string][]int32{"topic": {0}})
	assertNotification("revoked")

	c.onPartitionsLost(context.Background(), nil, map[string][]int32{"topic": {0}})
	assertNotification("lost")

	// Quiet on close:
	c.mu.Lock()
	c.stopped = true
	c.mu.Unlock()

	c.sendRebalanceError("assigned", map[string][]int32{"topic": {0}})
	c.sendRebalanceError("revoked", map[string][]int32{"topic": {0}})
	c.sendRebalanceError("lost", map[string][]int32{"topic": {0}})
	select {
	case err := <-c.Errors():
		t.Fatalf("unexpected error received after close: %v", err)
	default:
		// Quiet!
	}
}

func TestConsumerRealRebalanceOwnershipTransfer(t *testing.T) {
	requireBroker(t)
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "rebalance-real")
	group := kafkaTestTopic(t, "rebalance-real-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 2)
	t.Cleanup(func() {
		cleanupKafkaTopics(t, admin, topic)
		cleanupKafkaGroups(t, admin, group)
	})

	manualClient, err := kgo.NewClient(append([]kgo.Opt(nil), append(connection.clientOpts, kgo.RecordPartitioner(kgo.ManualPartitioner()))...)...)
	if err != nil {
		t.Fatalf("ManualPartitioner client: %v", err)
	}
	defer manualClient.Close()

	produceSync := func(part int32, body string) {
		record := &kgo.Record{Topic: topic, Partition: part, Value: []byte(body)}
		res := manualClient.ProduceSync(ctx, record)
		if err := res.FirstErr(); err != nil {
			t.Fatalf("ProduceSync partition %d (%s): %v", part, body, err)
		}
	}

	// Seed both partitions with test sequence:
	// offset 0: settled lower
	// offset 1: unsettled abandoned
	// offset 2: later body
	produceSync(0, "p0-settled-lower")
	produceSync(0, "p0-unsettled-abandoned")

	produceSync(1, "p1-settled-lower")
	produceSync(1, "p1-unsettled-abandoned")

	// Consumer 1 config with 200ms drain timeout.
	cfg1 := driver.ConsumerConfig{
		Group:        group,
		Destinations: []string{topic},
		Prefetch:     10,
		Effective:    connection.Capabilities(),
	}
	conn1 := &conn{
		client:                connection.client,
		clientOpts:            connection.clientOpts,
		driverOptions:         map[string]string{"kafka.staticMembership": "false"},
		rebalanceDrainTimeout: 200 * time.Millisecond,
		caps:                  connection.caps,
		info:                  connection.info,
		consumers:             make(map[*consumer]struct{}),
	}
	c1Raw, err := newConsumer(ctx, conn1, cfg1)
	if err != nil {
		t.Fatalf("Consumer 1: %v", err)
	}
	defer closeKafkaConsumer(c1Raw)

	var (
		p0Lower, p1Lower     driver.InboundMessage
		p0Abandon, p1Abandon driver.InboundMessage
	)

	initCtx, initCancel := context.WithTimeout(ctx, 15*time.Second)
	defer initCancel()

	for p0Lower.Body == nil || p1Lower.Body == nil || p0Abandon.Body == nil || p1Abandon.Body == nil {
		select {
		case msg := <-c1Raw.Messages():
			body := string(msg.Body)
			switch body {
			case "p0-settled-lower":
				p0Lower = msg
				if err := msg.Settle.Ack(ctx); err != nil {
					t.Fatalf("Ack(p0Lower): %v", err)
				}
			case "p1-settled-lower":
				p1Lower = msg
				if err := msg.Settle.Ack(ctx); err != nil {
					t.Fatalf("Ack(p1Lower): %v", err)
				}
			case "p0-unsettled-abandoned":
				p0Abandon = msg
			case "p1-unsettled-abandoned":
				p1Abandon = msg
			}
		case <-initCtx.Done():
			t.Fatalf("timed out receiving initial records on C1: %v (p0L=%v, p1L=%v, p0A=%v, p1A=%v)",
				initCtx.Err(), p0Lower.Body != nil, p1Lower.Body != nil, p0Abandon.Body != nil, p1Abandon.Body != nil)
		}
	}

	// Start Consumer 2 in the same group to force cooperative rebalance.
	cfg2 := driver.ConsumerConfig{
		Group:        group,
		Destinations: []string{topic},
		Prefetch:     10,
		Effective:    connection.Capabilities(),
	}
	conn2 := &conn{
		client:                connection.client,
		clientOpts:            connection.clientOpts,
		driverOptions:         map[string]string{"kafka.staticMembership": "false"},
		rebalanceDrainTimeout: 200 * time.Millisecond,
		caps:                  connection.caps,
		info:                  connection.info,
		consumers:             make(map[*consumer]struct{}),
	}
	c2Raw, err := newConsumer(ctx, conn2, cfg2)
	if err != nil {
		t.Fatalf("Consumer 2: %v", err)
	}
	defer closeKafkaConsumer(c2Raw)

	c2Ctx, c2Cancel := context.WithTimeout(ctx, 15*time.Second)
	defer c2Cancel()

	var c2FirstMsg driver.InboundMessage
	for c2FirstMsg.Body == nil {
		select {
		case msg := <-c2Raw.Messages():
			c2FirstMsg = msg
		case <-c2Raw.Errors():
		case <-c1Raw.Errors():
		case <-c2Ctx.Done():
			t.Fatalf("C2 timed out waiting for reassigned message: %v", c2Ctx.Err())
		}
	}
	reassignedPart := c2FirstMsg.Ref.Partition
	stayingPart := int32(1 - reassignedPart)

	wantAbandonBody := fmt.Sprintf("p%d-unsettled-abandoned", reassignedPart)
	if string(c2FirstMsg.Body) != wantAbandonBody {
		t.Fatalf("C2 first message = %q, want abandoned body %q (proves survivor receives abandoned body, not settled lower offset)",
			string(c2FirstMsg.Body), wantAbandonBody)
	}

	// Identify the held messages on C1.
	var departingMsg, stayingMsg driver.InboundMessage
	if reassignedPart == 0 {
		departingMsg = p0Abandon
		stayingMsg = p1Abandon
	} else {
		departingMsg = p1Abandon
		stayingMsg = p0Abandon
	}

	// 1. Prove departing owner's settle after tombstoning returns error wrapping ErrRevoked.
	departingErr := departingMsg.Settle.Ack(ctx)
	if departingErr == nil || !errors.Is(departingErr, ErrRevoked) {
		t.Fatalf("departing settler Ack = %v, want error wrapping ErrRevoked", departingErr)
	}

	// 2. Survivor settles the abandoned body.
	if err := c2FirstMsg.Settle.Ack(ctx); err != nil {
		t.Fatalf("C2 Ack abandoned body: %v", err)
	}
	// 3. Now publish each partition's later body after rebalance ownership is determined.
	produceSync(reassignedPart, fmt.Sprintf("p%d-later-body", reassignedPart))
	produceSync(stayingPart, fmt.Sprintf("p%d-later-body", stayingPart))

	// Survivor receives later body on the reassigned partition (proves later body not skipped).
	var c2SecondMsg driver.InboundMessage
	for c2SecondMsg.Body == nil {
		select {
		case msg := <-c2Raw.Messages():
			c2SecondMsg = msg
		case <-c2Raw.Errors():
		case <-c1Raw.Errors():
		case <-c2Ctx.Done():
			t.Fatalf("C2 timed out waiting for later body: %v", c2Ctx.Err())
		}
	}
	wantLaterBody := fmt.Sprintf("p%d-later-body", reassignedPart)
	if string(c2SecondMsg.Body) != wantLaterBody {
		t.Fatalf("C2 second message = %q, want later body %q", string(c2SecondMsg.Body), wantLaterBody)
	}
	if err := c2SecondMsg.Settle.Ack(ctx); err != nil {
		t.Fatalf("C2 Ack later body: %v", err)
	}
	// 4. Prove unaffected partition on C1 continues progressing through cooperative revoke.
	if err := stayingMsg.Settle.Ack(ctx); err != nil {
		t.Fatalf("C1 Ack on unaffected partition: %v", err)
	}
	var c1LaterMsg driver.InboundMessage
	c1Ctx, c1Cancel := context.WithTimeout(ctx, 10*time.Second)
	defer c1Cancel()
	for c1LaterMsg.Body == nil {
		select {
		case msg := <-c1Raw.Messages():
			c1LaterMsg = msg
		case <-c1Raw.Errors():
		case <-c2Raw.Errors():
		case <-c1Ctx.Done():
			t.Fatalf("C1 timed out waiting for later message on unaffected partition: %v", c1Ctx.Err())
		}
	}
	wantStayingLater := fmt.Sprintf("p%d-later-body", stayingPart)
	if string(c1LaterMsg.Body) != wantStayingLater {
		t.Fatalf("C1 unaffected partition message = %q, want %q", string(c1LaterMsg.Body), wantStayingLater)
	}
	if err := c1LaterMsg.Settle.Ack(ctx); err != nil {
		t.Fatalf("C1 Ack later message on unaffected partition: %v", err)
	}
}

func TestConsumerRealRebalanceNoGoroutineOrTimerLeak(t *testing.T) {
	requireBroker(t)
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "rebalance-leak")
	group := kafkaTestTopic(t, "rebalance-leak-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 2)
	t.Cleanup(func() {
		cleanupKafkaTopics(t, admin, topic)
		cleanupKafkaGroups(t, admin, group)
	})

	cfg := driver.ConsumerConfig{
		Group:        group,
		Destinations: []string{topic},
		Prefetch:     1,
		Effective:    connection.Capabilities(),
	}

	c1, err := connection.Consumer(ctx, cfg)
	if err != nil {
		t.Fatalf("Consumer 1: %v", err)
	}
	defer closeKafkaConsumer(c1)

	// Wait for C1 to join and stabilize
	waitForKafkaConsumerState(t, c1.(*consumer), "C1 to join", func() bool {
		c1.(*consumer).mu.Lock()
		defer c1.(*consumer).mu.Unlock()
		return len(c1.(*consumer).activeGenerations) > 0
	})

	// Measure baseline goroutines
	time.Sleep(100 * time.Millisecond) //nolint:forbidigo // stabilization wait before baseline leak check
	baselineGoroutines := runtime.NumGoroutine()

	// Run 4 join/leave rebalance cycles
	const cycles = 4
	counts := make([]int, 0, cycles)
	for range cycles {
		c2, err := connection.Consumer(ctx, cfg)
		if err != nil {
			t.Fatalf("Consumer C2: %v", err)
		}
		waitForKafkaConsumerState(t, c2.(*consumer), "C2 to join", func() bool {
			c2.(*consumer).mu.Lock()
			defer c2.(*consumer).mu.Unlock()
			return len(c2.(*consumer).activeGenerations) > 0
		})
		closeKafkaConsumer(c2)
		time.Sleep(100 * time.Millisecond) //nolint:forbidigo // bounded stabilization wait per rebalance cycle
		counts = append(counts, runtime.NumGoroutine())
	}

	finalGoroutines := runtime.NumGoroutine()
	t.Logf("Rebalance leak proof: baseline=%d, cycles=%v, final=%d", baselineGoroutines, counts, finalGoroutines)

	// Check for monotone growth across cycles
	strictlyIncreasing := true
	for i := 1; i < len(counts); i++ {
		if counts[i] <= counts[i-1] {
			strictlyIncreasing = false
			break
		}
	}
	if strictlyIncreasing && len(counts) > 2 {
		t.Fatalf("goroutine count exhibited monotone growth across rebalance cycles: %v", counts)
	}
	// Ensure final is not significantly higher than baseline (allow up to 2 for background runtime noise)
	if diff := finalGoroutines - baselineGoroutines; diff > 3 {
		t.Fatalf("goroutine count grew from %d to %d (delta %d)", baselineGoroutines, finalGoroutines, diff)
	}
}

func TestConsumerCallerHeldTransferIsExclusive(t *testing.T) {
	requireBroker(t)
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "caller-held-transfer")
	group := kafkaTestTopic(t, "caller-held-transfer-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 1)
	t.Cleanup(func() {
		cleanupKafkaTopics(t, admin, topic)
		cleanupKafkaGroups(t, admin, group)
	})

	connection.rebalanceDrainTimeout = 200 * time.Millisecond
	producer, err := connection.Producer(ctx, driver.ProducerConfig{
		RequireDurableAck: true,
		Effective:         connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(context.Background()) })
	publishKafkaMessage(t, producer, ctx, topic, "caller-held")

	cfg := driver.ConsumerConfig{
		Group:        group,
		Destinations: []string{topic},
		Prefetch:     1,
		Effective:    connection.Capabilities(),
	}
	source, err := newConsumer(ctx, connection, cfg)
	if err != nil {
		t.Fatalf("source Consumer: %v", err)
	}
	t.Cleanup(func() { source.client.Close() })

	var held driver.InboundMessage
	receiveCtx, receiveCancel := context.WithTimeout(ctx, 15*time.Second)
	defer receiveCancel()
	for held.Body == nil {
		select {
		case held = <-source.Messages():
		case err := <-source.Errors():
			if kind, classified := driver.Classify(err); classified && kind != driver.KindNotification {
				t.Fatalf("source error: %v", err)
			}
		case <-receiveCtx.Done():
			t.Fatalf("source timed out receiving caller-held message: %v", receiveCtx.Err())
		}
	}
	if string(held.Body) != "caller-held" {
		t.Fatalf("source body = %q, want caller-held", held.Body)
	}

	target, err := newConsumer(ctx, connection, cfg)
	if err != nil {
		t.Fatalf("target Consumer: %v", err)
	}
	t.Cleanup(func() { target.client.Close() })

	// Drive the ownership move through the production callbacks. A
	// single-partition topic leaves the assignor free to hand the partition
	// to either member, so waiting for its choice would make this test depend
	// on which member wins rather than on the transfer.
	source.onPartitionsRevoked(ctx, nil, map[string][]int32{topic: {0}})
	target.onPartitionsAssigned(ctx, nil, map[string][]int32{topic: {0}})

	targetCtx, targetCancel := context.WithTimeout(ctx, 15*time.Second)
	defer targetCancel()
	var replacement driver.InboundMessage
	for replacement.Body == nil {
		select {
		case replacement = <-target.Messages():
		case err := <-target.Errors():
			if kind, classified := driver.Classify(err); classified && kind != driver.KindNotification {
				t.Fatalf("target error: %v", err)
			}
		case <-targetCtx.Done():
			t.Fatalf("target timed out receiving transferred message: %v", targetCtx.Err())
		}
	}
	if string(replacement.Body) != "caller-held" || replacement.Ref.Offset != held.Ref.Offset {
		t.Fatalf("target replacement = body %q offset %d, want body %q offset %d",
			replacement.Body, replacement.Ref.Offset, held.Body, held.Ref.Offset)
	}

	// The transfer detaches the settler's settlement authority but keeps its
	// destination slot charged: the caller still holds the delivery, and a
	// delivery occupies a slot from admission until terminal settlement. The
	// slot is released exactly once, by the settlement below.
	waitForKafkaConsumerState(t, source, "source settler detached with slot held", func() bool {
		source.mu.Lock()
		defer source.mu.Unlock()
		return len(source.settlers) == 0 && source.unsettled[topic] == 1
	})
	target.mu.Lock()
	targetUnsettled := target.unsettled[topic]
	targetSettlers := len(target.settlers)
	target.mu.Unlock()
	if targetUnsettled != 1 || targetSettlers != 1 {
		t.Fatalf("target ownership = unsettled %d settlers %d, want 1 and 1", targetUnsettled, targetSettlers)
	}
	if err := held.Settle.Ack(ctx); err == nil || !errors.Is(err, ErrRevoked) {
		t.Fatalf("source Ack = %v, want ErrRevoked", err)
	}
	waitForKafkaConsumerState(t, source, "revoked settlement released the slot once", func() bool {
		source.mu.Lock()
		defer source.mu.Unlock()
		return source.unsettled[topic] == 0 && len(source.settlers) == 0
	})
	if err := replacement.Settle.Ack(ctx); err != nil {
		t.Fatalf("target Ack: %v", err)
	}
	waitForKafkaConsumerState(t, target, "target prefetch released", func() bool {
		target.mu.Lock()
		defer target.mu.Unlock()
		return target.unsettled[topic] == 0 && len(target.settlers) == 0
	})

	noDuplicateCtx, noDuplicateCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer noDuplicateCancel()
	select {
	case duplicate := <-target.Messages():
		t.Fatalf("target received duplicate offset %d body %q", duplicate.Ref.Offset, duplicate.Body)
	case <-noDuplicateCtx.Done():
	}
}

// TestConsumerHandoffStaysQueuedUntilAnEligibleOwnerAppears covers requeuing
// directly. A delivery the caller still holds is transferred while the only
// candidates are a fenced member and a draining one, so the handoff stays in
// the source's queue instead of reaching either, and the next assigned owner
// receives it.
func TestConsumerHandoffStaysQueuedUntilAnEligibleOwnerAppears(t *testing.T) {
	requireBroker(t)
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "fenced-handoff")
	group := kafkaTestTopic(t, "fenced-handoff-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 1)
	t.Cleanup(func() {
		cleanupKafkaTopics(t, admin, topic)
		cleanupKafkaGroups(t, admin, group)
	})

	connection.rebalanceDrainTimeout = 200 * time.Millisecond
	producer, err := connection.Producer(ctx, driver.ProducerConfig{
		RequireDurableAck: true,
		Effective:         connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(context.Background()) })
	publishKafkaMessage(t, producer, ctx, topic, "fenced-handoff")

	cfg := driver.ConsumerConfig{
		Group:        group,
		Destinations: []string{topic},
		Prefetch:     1,
		Effective:    connection.Capabilities(),
	}
	source, err := newConsumer(ctx, connection, cfg)
	if err != nil {
		t.Fatalf("source Consumer: %v", err)
	}
	t.Cleanup(func() { source.client.Close() })

	var held driver.InboundMessage
	receiveCtx, receiveCancel := context.WithTimeout(ctx, 15*time.Second)
	defer receiveCancel()
	for held.Body == nil {
		select {
		case held = <-source.Messages():
		case err := <-source.Errors():
			if kind, classified := driver.Classify(err); classified && kind != driver.KindNotification {
				t.Fatalf("source error: %v", err)
			}
		case <-receiveCtx.Done():
			t.Fatalf("source timed out receiving the delivery: %v", receiveCtx.Err())
		}
	}
	if string(held.Body) != "fenced-handoff" {
		t.Fatalf("source body = %q, want fenced-handoff", held.Body)
	}

	// The first candidate is fenced after an assignment, so the dispatch guard skips it,
	// and it would refuse the record inside emit anyway: the fenced half of that guard is
	// defence in depth behind isRecordStaleLocked, and the handoff requeues either way.
	// The second is assigned and reachable but draining, so the handoff is offered to it,
	// refused by emit, and returned to the queue. What this test pins is that observable
	// contract: neither candidate receives it, the source keeps it queued, and the next
	// assigned owner gets it.
	key := partitionKey{destination: topic, partition: 0}
	fenced := &consumer{
		conn:              connection,
		messages:          make(chan driver.InboundMessage, 4),
		fenced:            map[partitionKey]bool{key: true},
		activeGenerations: map[partitionKey]uint64{key: 1},
	}
	draining := &consumer{
		conn:              connection,
		messages:          make(chan driver.InboundMessage, 4),
		draining:          true,
		activeGenerations: map[partitionKey]uint64{key: 1},
	}
	connection.mu.Lock()
	connection.consumers[fenced] = struct{}{}
	connection.consumers[draining] = struct{}{}
	connection.mu.Unlock()
	t.Cleanup(func() {
		connection.mu.Lock()
		delete(connection.consumers, fenced)
		delete(connection.consumers, draining)
		connection.mu.Unlock()
	})

	// The caller holds the delivery, so the transfer queues the handoff for the
	// key's next owner rather than delivering it.
	source.onPartitionsRevoked(ctx, nil, map[string][]int32{topic: {0}})
	source.redriveHandoffs(key)

	waitForKafkaConsumerState(t, source, "handoff queued while no candidate can own it", func() bool {
		source.mu.Lock()
		defer source.mu.Unlock()
		return len(source.handoffs[key]) == 1
	})
	if len(fenced.messages) != 0 {
		t.Fatalf("fenced target received %d handoff messages, want none", len(fenced.messages))
	}
	if len(draining.messages) != 0 {
		t.Fatalf("draining target received %d handoff messages, want none", len(draining.messages))
	}

	second, err := newConsumer(ctx, connection, cfg)
	if err != nil {
		t.Fatalf("second Consumer: %v", err)
	}
	t.Cleanup(func() { second.client.Close() })
	second.onPartitionsAssigned(ctx, nil, map[string][]int32{topic: {0}})

	secondCtx, secondCancel := context.WithTimeout(ctx, 15*time.Second)
	defer secondCancel()
	var replacement driver.InboundMessage
	for replacement.Body == nil {
		select {
		case replacement = <-second.Messages():
		case err := <-second.Errors():
			if kind, classified := driver.Classify(err); classified && kind != driver.KindNotification {
				t.Fatalf("second consumer error: %v", err)
			}
		case <-secondCtx.Done():
			t.Fatalf("second consumer timed out receiving the queued handoff: %v", secondCtx.Err())
		}
	}
	if replacement.Ref.Offset != held.Ref.Offset {
		t.Fatalf("second consumer offset = %d, want %d", replacement.Ref.Offset, held.Ref.Offset)
	}
	waitForKafkaConsumerState(t, source, "queued handoff drained to the assigned owner", func() bool {
		source.mu.Lock()
		defer source.mu.Unlock()
		return len(source.handoffs[key]) == 0
	})
	if len(fenced.messages) != 0 || len(draining.messages) != 0 {
		t.Fatalf("ineligible targets received %d and %d handoff messages, want none",
			len(fenced.messages), len(draining.messages))
	}
}

// queuedHandoffOffsets counts the handoff offsets a consumer still has queued for one
// destination.
func queuedHandoffOffsets(consumer *consumer, destination string) int {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	queued := 0
	for key, offsets := range consumer.handoffs {
		if key.destination == destination {
			queued += len(offsets)
		}
	}
	return queued
}

// TestConsumerQueuedHandoffDispatchesWhenCapacityFrees covers the capacity trigger directly: two
// caller-held deliveries are transferred while the only eligible target is at its prefetch
// budget, so exactly one handoff is taken and the other stays queued. Freeing one slot on that
// target is enough to have the queued one taken. Without the trigger the record has no path back
// at all, because its offset reservation keeps the broker's own redelivery filtered.
func TestConsumerQueuedHandoffDispatchesWhenCapacityFrees(t *testing.T) {
	requireBroker(t)
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "capacity-trigger")
	group := kafkaTestTopic(t, "capacity-trigger-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 2)
	t.Cleanup(func() {
		cleanupKafkaTopics(t, admin, topic)
		cleanupKafkaGroups(t, admin, group)
	})

	connection.rebalanceDrainTimeout = 200 * time.Millisecond
	producer, err := connection.Producer(ctx, driver.ProducerConfig{
		RequireDurableAck: true,
		Effective:         connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(context.Background()) })
	publishKafkaMessage(t, producer, ctx, topic, "trigger-a")
	publishKafkaMessage(t, producer, ctx, topic, "trigger-b")

	cfg := driver.ConsumerConfig{
		Group:        group,
		Destinations: []string{topic},
		Prefetch:     2,
		Effective:    connection.Capabilities(),
	}
	source, err := newConsumer(ctx, connection, cfg)
	if err != nil {
		t.Fatalf("source Consumer: %v", err)
	}
	t.Cleanup(func() { source.client.Close() })

	held := make([]driver.InboundMessage, 0, 2)
	receiveCtx, receiveCancel := context.WithTimeout(ctx, 15*time.Second)
	defer receiveCancel()
	for len(held) < 2 {
		select {
		case message := <-source.Messages():
			held = append(held, message)
		case err := <-source.Errors():
			if kind, classified := driver.Classify(err); classified && kind != driver.KindNotification {
				t.Fatalf("source error: %v", err)
			}
		case <-receiveCtx.Done():
			t.Fatalf("source timed out after %d deliveries: %v", len(held), receiveCtx.Err())
		}
	}

	targetCfg := cfg
	targetCfg.Prefetch = 1
	target, err := newConsumer(ctx, connection, targetCfg)
	if err != nil {
		t.Fatalf("target Consumer: %v", err)
	}
	t.Cleanup(func() { target.client.Close() })
	target.onPartitionsAssigned(ctx, nil, map[string][]int32{topic: {0, 1}})

	// Both caller-held copies are transferred, so both offsets queue as handoffs while the
	// target holds its single slot.
	source.onPartitionsRevoked(ctx, nil, map[string][]int32{topic: {0, 1}})

	firstCtx, firstCancel := context.WithTimeout(ctx, 15*time.Second)
	defer firstCancel()
	var first driver.InboundMessage
	for first.Body == nil {
		select {
		case first = <-target.Messages():
		case err := <-target.Errors():
			if kind, classified := driver.Classify(err); classified && kind != driver.KindNotification {
				t.Fatalf("target error: %v", err)
			}
		case <-firstCtx.Done():
			t.Fatalf("target timed out taking the first handoff: %v", firstCtx.Err())
		}
	}
	waitForKafkaConsumerState(t, source, "one handoff stays queued while the target is full", func() bool {
		return queuedHandoffOffsets(source, topic) == 1
	})

	// Freeing the target's single slot is the crossing the trigger exists for. Every
	// production release holds c.mu while it writes the destination's charge, so the test
	// takes the same lock, and the dispatch the release spawns blocks on it exactly as it
	// does on every production release path.
	target.mu.Lock()
	target.releaseSlotsLocked(topic, 1)
	target.mu.Unlock()

	secondCtx, secondCancel := context.WithTimeout(ctx, 15*time.Second)
	defer secondCancel()
	var second driver.InboundMessage
	for second.Body == nil {
		select {
		case second = <-target.Messages():
		case err := <-target.Errors():
			if kind, classified := driver.Classify(err); classified && kind != driver.KindNotification {
				t.Fatalf("target error: %v", err)
			}
		case <-secondCtx.Done():
			t.Fatalf("queued handoff was not dispatched after capacity freed: %v", secondCtx.Err())
		}
	}
	if first.Ref.Offset == second.Ref.Offset {
		t.Fatalf("both handoffs delivered offset %d, want one each", first.Ref.Offset)
	}
	delivered := map[int64]bool{first.Ref.Offset: true, second.Ref.Offset: true}
	for _, message := range held {
		if !delivered[message.Ref.Offset] {
			t.Fatalf("held offset %d was never redelivered to the target", message.Ref.Offset)
		}
	}
	waitForKafkaConsumerState(t, source, "no handoff is left queued", func() bool {
		return queuedHandoffOffsets(source, topic) == 0
	})
}

// TestConsumerSelfTransferRegrantsSettlement covers a cooperative generation
// bump that hands a revoked partition back to the member that transferred it:
// the delivery the caller still holds keeps its one live settlement path, the
// queued handoff is cleared instead of delivered twice, and the destination
// slot is released exactly once, by that settlement.
func TestConsumerSelfTransferRegrantsSettlement(t *testing.T) {
	requireBroker(t)
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "self-transfer")
	group := kafkaTestTopic(t, "self-transfer-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 2)
	t.Cleanup(func() {
		cleanupKafkaTopics(t, admin, topic)
		cleanupKafkaGroups(t, admin, group)
	})

	connection.rebalanceDrainTimeout = 200 * time.Millisecond
	// One record per partition, so the delivery held on partition 0 and the
	// untouched delivery on partition 1 are both real.
	manualClient, err := kgo.NewClient(append([]kgo.Opt(nil), append(connection.clientOpts, kgo.RecordPartitioner(kgo.ManualPartitioner()))...)...)
	if err != nil {
		t.Fatalf("ManualPartitioner client: %v", err)
	}
	t.Cleanup(manualClient.Close)
	for partition, body := range map[int32]string{0: "lower", 1: "higher"} {
		record := &kgo.Record{Topic: topic, Partition: partition, Value: []byte(body)}
		if err := manualClient.ProduceSync(ctx, record).FirstErr(); err != nil {
			t.Fatalf("ProduceSync partition %d: %v", partition, err)
		}
	}

	cfg := driver.ConsumerConfig{
		Group:        group,
		Destinations: []string{topic},
		Prefetch:     2,
		Effective:    connection.Capabilities(),
	}
	source, err := newConsumer(ctx, connection, cfg)
	if err != nil {
		t.Fatalf("source Consumer: %v", err)
	}
	t.Cleanup(func() { source.client.Close() })

	held := make(map[int32]driver.InboundMessage)
	receiveCtx, receiveCancel := context.WithTimeout(ctx, 20*time.Second)
	defer receiveCancel()
	for len(held) < 2 {
		select {
		case message := <-source.Messages():
			held[message.Ref.Partition] = message
		case err := <-source.Errors():
			if kind, classified := driver.Classify(err); classified && kind != driver.KindNotification {
				t.Fatalf("source error: %v", err)
			}
		case <-receiveCtx.Done():
			t.Fatalf("source timed out holding both deliveries: %v", receiveCtx.Err())
		}
	}
	lower := held[0]

	// Revoke only partition 0, then hand it straight back: the production
	// callbacks of a cooperative bump that keeps the partition on this member.
	source.onPartitionsRevoked(ctx, nil, map[string][]int32{topic: {0}})
	source.onPartitionsAssigned(ctx, nil, map[string][]int32{topic: {0}})

	returned, ok := lower.Settle.(*settler)
	if !ok {
		t.Fatalf("settler type = %T, want *settler", lower.Settle)
	}
	source.mu.Lock()
	live := len(source.settlers)
	_, attached := source.settlers[returned]
	queued := len(source.handoffs[partitionKey{destination: topic, partition: 0}])
	charged := source.unsettled[topic]
	tracker := returned.tracker
	source.mu.Unlock()
	if live != 2 {
		t.Fatalf("source settlers after self-transfer = %d, want the returned and the kept delivery", live)
	}
	if !attached || returned.tombstoned || tracker == nil {
		t.Fatalf("returned delivery not re-granted: attached=%v tombstoned=%v tracker=%v",
			attached, returned.tombstoned, tracker)
	}
	if queued != 0 {
		t.Fatalf("queued handoffs after self-transfer = %d, want the copy cleared", queued)
	}
	if charged != 2 {
		t.Fatalf("source slot accounting = %d charged, want 2 until terminal settlement", charged)
	}
	if err := lower.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack of the returned partition: %v", err)
	}
	source.mu.Lock()
	afterAck := source.unsettled[topic]
	source.mu.Unlock()
	if afterAck != 1 {
		t.Fatalf("slot accounting after one settlement = %d, want 1 (one release per delivery)", afterAck)
	}
	if err := held[1].Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack of the kept partition: %v", err)
	}
	waitForKafkaConsumerState(t, source, "both slots released", func() bool {
		source.mu.Lock()
		defer source.mu.Unlock()
		return source.unsettled[topic] == 0 && len(source.settlers) == 0
	})

	noDuplicateCtx, noDuplicateCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer noDuplicateCancel()
	select {
	case duplicate := <-source.Messages():
		t.Fatalf("source received duplicate offset %d body %q", duplicate.Ref.Offset, duplicate.Body)
	case <-noDuplicateCtx.Done():
	}
}

// TestConsumerTransferredDeliveryReleasesOneSlot covers the detached case: a
// delivery transferred to another consumer keeps its destination slot until the
// caller's revoked settlement completes, and that settlement releases exactly
// one slot. The delta is asserted rather than the absolute count, so a
// concurrent rebalance cannot mask a double release.
func TestConsumerTransferredDeliveryReleasesOneSlot(t *testing.T) {
	requireBroker(t)
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "detached-slot")
	group := kafkaTestTopic(t, "detached-slot-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 2)
	t.Cleanup(func() {
		cleanupKafkaTopics(t, admin, topic)
		cleanupKafkaGroups(t, admin, group)
	})

	connection.rebalanceDrainTimeout = 200 * time.Millisecond
	manualClient, err := kgo.NewClient(append([]kgo.Opt(nil), append(connection.clientOpts, kgo.RecordPartitioner(kgo.ManualPartitioner()))...)...)
	if err != nil {
		t.Fatalf("ManualPartitioner client: %v", err)
	}
	t.Cleanup(manualClient.Close)
	for partition, body := range map[int32]string{0: "moved", 1: "kept"} {
		record := &kgo.Record{Topic: topic, Partition: partition, Value: []byte(body)}
		if err := manualClient.ProduceSync(ctx, record).FirstErr(); err != nil {
			t.Fatalf("ProduceSync partition %d: %v", partition, err)
		}
	}

	cfg := driver.ConsumerConfig{
		Group:        group,
		Destinations: []string{topic},
		Prefetch:     2,
		Effective:    connection.Capabilities(),
	}
	source, err := newConsumer(ctx, connection, cfg)
	if err != nil {
		t.Fatalf("source Consumer: %v", err)
	}
	t.Cleanup(func() { source.client.Close() })

	held := make(map[int32]driver.InboundMessage)
	receiveCtx, receiveCancel := context.WithTimeout(ctx, 20*time.Second)
	defer receiveCancel()
	for len(held) < 2 {
		select {
		case message := <-source.Messages():
			held[message.Ref.Partition] = message
		case err := <-source.Errors():
			if kind, classified := driver.Classify(err); classified && kind != driver.KindNotification {
				t.Fatalf("source error: %v", err)
			}
		case <-receiveCtx.Done():
			t.Fatalf("source timed out holding both deliveries: %v", receiveCtx.Err())
		}
	}

	target, err := newConsumer(ctx, connection, cfg)
	if err != nil {
		t.Fatalf("target Consumer: %v", err)
	}
	t.Cleanup(func() { target.client.Close() })

	// Transfer partition 0 to the other consumer through the production
	// callbacks and settle the source's copy afterwards.
	source.onPartitionsRevoked(ctx, nil, map[string][]int32{topic: {0}})
	target.onPartitionsAssigned(ctx, nil, map[string][]int32{topic: {0}})

	moved := held[0]
	source.mu.Lock()
	settler, ok := moved.Settle.(*settler)
	detached := ok && settler.tombstoned
	before := source.unsettled[topic]
	source.mu.Unlock()
	if !detached {
		t.Fatalf("source settler for the moved partition is not detached")
	}
	if before < 1 {
		t.Fatalf("source slot accounting = %d charged after the transfer, want the moved delivery still charged", before)
	}
	if err := moved.Settle.Ack(ctx); err == nil || !errors.Is(err, ErrRevoked) {
		t.Fatalf("source Ack = %v, want ErrRevoked", err)
	}
	source.mu.Lock()
	after := source.unsettled[topic]
	source.mu.Unlock()
	if before-after != 1 {
		t.Fatalf("revoked settlement released %d slots, want exactly 1", before-after)
	}
	if err := held[1].Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack of the kept partition: %v", err)
	}
}

func TestConsumerLeaveSelectsMembershipPath(t *testing.T) {
	tests := []struct {
		name       string
		static     bool
		instanceID string
	}{
		{name: "static instance", static: true, instanceID: "worker-1"},
		{name: "dynamic member", instanceID: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wantErr := errors.New("leave failed")
			var (
				staticCalls  int
				dynamicCalls int
				gotGroup     string
				gotInstance  string
			)
			c := &consumer{
				group:            "group",
				staticMembership: tc.static,
				instanceID:       tc.instanceID,
				errors:           make(chan error, 1),
				leaveRequestC:    make(chan struct{}, 1),
				leaveStopC:       make(chan struct{}),
				leaveFinished:    make(chan struct{}),
				leaveLoopDone:    make(chan struct{}),
			}
			c.leaveStatic = func(_ context.Context, group, instanceID string) error {
				staticCalls++
				gotGroup = group
				gotInstance = instanceID
				return wantErr
			}
			c.leaveDynamic = func(_ context.Context) error {
				dynamicCalls++
				return wantErr
			}
			c.leaveFn = c.leaveGroup

			if err := c.requestLeaveAndWait(context.Background()); !errors.Is(err, wantErr) {
				t.Fatalf("requestLeaveAndWait error = %v, want %v", err, wantErr)
			}
			<-c.leaveLoopDone
			wantStatic := tc.static && tc.instanceID != ""
			expectedStatic := 0
			expectedDynamic := 1
			if wantStatic {
				expectedStatic = 1
				expectedDynamic = 0
			}
			if staticCalls != expectedStatic {
				t.Fatalf("static operation calls = %d, want %d", staticCalls, expectedStatic)
			}
			if dynamicCalls != expectedDynamic {
				t.Fatalf("dynamic operation calls = %d, want %d", dynamicCalls, expectedDynamic)
			}
			if wantStatic && (gotGroup != c.group || gotInstance != tc.instanceID) {
				t.Fatalf("static operation arguments = group %q instance %q, want group %q instance %q",
					gotGroup, gotInstance, c.group, tc.instanceID)
			}
			select {
			case err := <-c.errors:
				if !errors.Is(err, wantErr) {
					t.Fatalf("Errors() = %v, want %v", err, wantErr)
				}
			default:
				t.Fatal("leave error was not reported")
			}
		})
	}
}

func TestConsumerLeaveTeardownCancelsBoundedOperation(t *testing.T) {
	client, err := kgo.NewClient(noDialKafkaOption())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	started := make(chan struct{})
	canceled := make(chan struct{})
	connection := &conn{
		consumers: make(map[*consumer]struct{}),
	}
	c := &consumer{
		conn:                  connection,
		client:                client,
		group:                 "group",
		destinations:          []string{"topic"},
		budgets:               map[string]int{"topic": 1},
		errors:                make(chan error, 1),
		messages:              make(chan driver.InboundMessage, 1),
		pollDone:              make(chan struct{}),
		stopDone:              make(chan struct{}),
		forwarderStopC:        make(chan struct{}),
		leaveRequestC:         make(chan struct{}, 1),
		leaveStopC:            make(chan struct{}),
		leaveFinished:         make(chan struct{}),
		leaveLoopDone:         make(chan struct{}),
		rebalanceDrainTimeout: time.Hour,
		leaveFn: func(ctx context.Context, _ leaveRequest) error {
			close(started)
			<-ctx.Done()
			close(canceled)
			return ctx.Err()
		},
		cancelPoll: func() {},
	}

	drainCtx, cancelDrain := context.WithCancel(context.Background())
	drainDone := make(chan error, 1)
	go func() {
		drainDone <- c.Drain(drainCtx)
	}()
	waitForKafkaConsumerState(t, c, "Drain to enter draining state", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.draining
	})
	cancelDrain()
	if err := <-drainDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Drain() error = %v, want context cancellation", err)
	}

	c.requestLeave()
	select {
	case <-started:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("leave operation did not start")
	}

	teardownCtx, cancelTeardown := context.WithTimeout(context.Background(), time.Second)
	defer cancelTeardown()
	teardownDone := make(chan struct{})
	go func() {
		c.closeTeardown(teardownCtx)
		close(teardownDone)
	}()
	select {
	case <-canceled:
	case <-teardownCtx.Done():
		t.Fatal("leave context was not canceled by teardown")
	}
	select {
	case <-teardownDone:
	case <-teardownCtx.Done():
		t.Fatal("teardown exceeded its context bound")
	}
}

func TestConsumerTransferMarkersKeepExactOffsets(t *testing.T) {
	key := partitionKey{destination: "topic", partition: 0}
	target := &consumer{
		messages:          make(chan driver.InboundMessage, 1),
		activeGenerations: map[partitionKey]uint64{key: 1},
		fenced:            make(map[partitionKey]bool),
		recordGenerations: make(map[*kgo.Record]uint64),
	}
	lower := &kgo.Record{Topic: key.destination, Partition: key.partition, Offset: 4}
	exact := &kgo.Record{Topic: key.destination, Partition: key.partition, Offset: 5}
	target.recordGenerations[lower] = 1
	target.recordGenerations[exact] = 1

	target.suppressSettledTransfer(key, exact.Offset)

	if target.isRecordStale(lower) {
		t.Fatal("lower offset was suppressed by a higher settled offset")
	}
	if !target.isRecordStale(exact) {
		t.Fatal("exact settled offset was not suppressed")
	}
	target.discardStaleRecord(exact)
	if !target.isRecordStale(exact) {
		t.Fatal("exact transfer marker was cleared after one stale record")
	}
}

// TestConsumerBudgetRefusedRecordsResumeOnAck pins the stall a deadline-bounded
// poll creates. That poll returns every record franz-go has buffered in one
// batch, admission takes only what the prefetch budget allows, and the loop
// holds the rest in its own pending queue. franz-go has already handed those
// records over and advanced its cursors past them, so the broker will never
// offer them again: the settlement that frees the budget is the only event that
// can bring them back, and without one the loop waits on a broker that has
// nothing left to send while the records it is holding are ready to deliver.
func TestConsumerBudgetRefusedRecordsResumeOnAck(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	ready := kafkaTestTopic(t, "consumer-pending-ready")
	parked := kafkaTestTopic(t, "consumer-pending-parked")
	group := kafkaTestTopic(t, "consumer-pending-group")
	cleanupKafkaTopics(t, admin, ready, parked)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, ready, 1)
	createKafkaTopic(t, admin, ctx, parked, 1)
	// A record on parked stays due for an hour, which keeps the poll loop in
	// its deadline-bounded poll for the whole test. A one-record poll cannot
	// build the batch this needs: a destination that reaches its budget pauses,
	// and franz-go strips what is still buffered for a paused topic back to the
	// broker rather than handing it over.
	const parkedDelay = time.Hour
	if _, err := connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{
			{Name: parked, Delay: parkedDelay},
			{Name: ready},
		},
		Effective: connection.Capabilities(),
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	producer, err := connection.Producer(ctx, driver.ProducerConfig{Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(context.Background()) })
	consumerValue, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{parked, ready}, Prefetch: 2,
		PerDestination: map[string]int{parked: 1, ready: 1},
		Delays:         map[string]time.Duration{parked: parkedDelay},
		Effective:      connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() { closeKafkaConsumer(consumerValue) })

	publishKafkaMessage(t, producer, ctx, parked, "parked")
	waitForDeferredPause(t, consumerValue, parked)

	// Keep the ready lane paused while its records are published, so franz-go
	// issues the fetch for that partition only after the resume and returns all
	// four records in the one batch the bounded poll takes.
	if err := consumerValue.Pause(ready); err != nil {
		t.Fatalf("Pause(%q): %v", ready, err)
	}
	publishKafkaCount(t, producer, ctx, ready, 4)
	if err := consumerValue.Resume(ready); err != nil {
		t.Fatalf("Resume(%q): %v", ready, err)
	}

	// Settle each delivery as it arrives, the way a worker does. The first ack
	// frees the only ready slot; the other three were already refused, so only
	// that ack can start them moving again.
	for delivered := 1; delivered <= 4; delivered++ {
		message := receiveKafkaMessageBefore(t, consumerValue, kafkaNow().Add(3*time.Second))
		if message.Destination != ready {
			t.Fatalf("delivery %d destination = %q, want %q", delivered, message.Destination, ready)
		}
		if err := message.Settle.Ack(ctx); err != nil {
			t.Fatalf("Ack(delivery %d): %v", delivered, err)
		}
	}
	if err := consumerValue.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
