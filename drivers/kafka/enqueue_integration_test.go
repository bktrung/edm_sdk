//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestKafkaInboundLogAppendTimeReportsBrokerSource(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "enqueue-log-append")
	group := kafkaTestTopic(t, "enqueue-log-append-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopicWithTimestampType(t, admin, ctx, topic, "LogAppendTime")

	before := clock.NewReal().Now().Add(-time.Second)
	record := &kgo.Record{Topic: topic, Value: []byte("log-append"), Timestamp: before.Add(-time.Hour)}
	if err := connection.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		t.Fatalf("ProduceSync: %v", err)
	}
	after := clock.NewReal().Now().Add(time.Second)

	consumer, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group:        group,
		Destinations: []string{topic},
		Prefetch:     1,
		StartAt:      driver.StartEarliest,
		Effective:    connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Release(context.Background()) })
	message := receiveKafkaMessage(t, consumer)
	if message.EnqueuedAt.Before(before) || message.EnqueuedAt.After(after) {
		t.Fatalf("EnqueuedAt = %v, want within [%v, %v]", message.EnqueuedAt, before, after)
	}
	if message.EnqueuedAtSource != driver.EnqueueSourceBroker {
		t.Fatalf("EnqueuedAtSource = %v, want broker", message.EnqueuedAtSource)
	}
	if err := message.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := consumer.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestKafkaInboundCreateTimeReportsProducerSource(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "enqueue-create")
	group := kafkaTestTopic(t, "enqueue-create-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopicWithTimestampType(t, admin, ctx, topic, "CreateTime")

	producerAt := time.Date(2026, time.January, 2, 3, 4, 5, 678000000, time.UTC)
	record := &kgo.Record{Topic: topic, Value: []byte("create-time"), Timestamp: producerAt}
	if err := connection.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		t.Fatalf("ProduceSync: %v", err)
	}

	consumer, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group:        group,
		Destinations: []string{topic},
		Prefetch:     1,
		StartAt:      driver.StartEarliest,
		Effective:    connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Release(context.Background()) })
	message := receiveKafkaMessage(t, consumer)
	if !message.EnqueuedAt.Equal(producerAt) {
		t.Fatalf("EnqueuedAt = %v, want %v", message.EnqueuedAt, producerAt)
	}
	if message.EnqueuedAtSource != driver.EnqueueSourceProducer {
		t.Fatalf("EnqueuedAtSource = %v, want producer", message.EnqueuedAtSource)
	}
	if err := message.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := consumer.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func createKafkaTopicWithTimestampType(t *testing.T, admin *kadm.Client, ctx context.Context, name, timestampType string) {
	t.Helper()
	value := kadm.StringPtr(timestampType)
	response, err := admin.CreateTopic(ctx, 1, -1, map[string]*string{"message.timestamp.type": value}, name)
	if err != nil {
		t.Fatalf("CreateTopic(%q): %v", name, err)
	}
	if response.Err != nil {
		t.Fatalf("CreateTopic(%q) response: %v", name, response.Err)
	}
	waitKafkaTopicVisible(t, ctx, admin, name)
}
