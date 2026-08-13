package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestProducerConfirmAndReturn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-queue"
	conn, rawChannel := openProducerFixture(t, ctx, queue)
	defer func() { _ = rawChannel.Close() }()

	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	producer, err := conn.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	deliveries, err := rawChannel.Consume(queue, "producer-test", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	publishTime := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	if err := producer.Publish(ctx, driver.OutboundMessage{
		Destination: queue,
		Headers: []driver.Header{
			{Key: "id", Value: []byte("event-1")},
			{Key: "time", Value: []byte(publishTime.Format(time.RFC3339Nano))},
			{Key: "type", Value: []byte("orders.created")},
			{Key: "datacontenttype", Value: []byte("application/json")},
			{Key: "f1correlationid", Value: []byte("corr-1")},
			{Key: "f1partitionkey", Value: []byte("order-1")},
			{Key: "custom", Value: []byte("value")},
		},
		Body: []byte("payload"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case delivery := <-deliveries:
		if string(delivery.Body) != "payload" {
			t.Fatalf("Body = %q, want payload", delivery.Body)
		}
		if delivery.DeliveryMode != amqp.Persistent {
			t.Fatalf("DeliveryMode = %d, want persistent", delivery.DeliveryMode)
		}
		if delivery.MessageId != "event-1" || delivery.Type != "orders.created" || delivery.ContentType != "application/json" || delivery.CorrelationId != "corr-1" {
			t.Fatalf("AMQP properties = id %q type %q content type %q correlation %q", delivery.MessageId, delivery.Type, delivery.ContentType, delivery.CorrelationId)
		}
		if got := string(headerValue(delivery.Headers["cloudEvents:f1partitionkey"])); got != "order-1" {
			t.Fatalf("partition header = %q, want order-1", got)
		}
		if got := string(headerValue(delivery.Headers["cloudEvents:custom"])); got != "value" {
			t.Fatalf("custom header = %q, want value", got)
		}
		if err := delivery.Ack(false); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("receive: %v", ctx.Err())
	}

	err = producer.Publish(ctx, driver.OutboundMessage{Destination: queue + "-missing"})
	if err == nil {
		t.Fatal("missing Publish error = nil")
	}
	if !errors.Is(err, driver.ErrDestinationMissing) {
		t.Fatalf("missing Publish error = %v, want ErrDestinationMissing", err)
	}
	if err := producer.Close(ctx); err != nil {
		t.Fatalf("Producer.Close: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("Conn.Close: %v", err)
	}
}

func TestProducerPublishesToDeclaredFanoutExchange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const exchange = "rabbitmq-driver-producer-exchange"
	const queueA = "rabbitmq-driver-producer-fanout-a"
	const queueB = "rabbitmq-driver-producer-fanout-b"
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
	_ = rawChannel.ExchangeDelete(exchange, false, false)
	for _, queue := range []string{queueA, queueB} {
		_, _ = rawChannel.QueueDelete(queue, false, false, false)
	}
	t.Cleanup(func() {
		_ = rawChannel.ExchangeDelete(exchange, false, false)
		for _, queue := range []string{queueA, queueB} {
			_, _ = rawChannel.QueueDelete(queue, false, false, false)
		}
	})

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	spec := driver.TopologySpec{
		Exchanges: []driver.ExchangeSpec{{Name: exchange, Kind: "fanout", Durable: true}},
		Destinations: []driver.DestinationSpec{
			{Name: queueA, Durable: true},
			{Name: queueB, Durable: true},
		},
		Bindings: []driver.BindingSpec{
			{Source: exchange, Destination: queueA},
			{Source: exchange, Destination: queueB},
		},
	}
	if _, err := conn.Admin().EnsureTopology(ctx, spec); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	producer, err := conn.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	consume := func(queue, tag string) <-chan amqp.Delivery {
		t.Helper()
		messages, consumeErr := rawChannel.Consume(queue, tag, false, false, false, false, nil)
		if consumeErr != nil {
			t.Fatalf("Consume(%q): %v", queue, consumeErr)
		}
		return messages
	}
	deliveriesA := consume(queueA, "producer-fanout-a")
	deliveriesB := consume(queueB, "producer-fanout-b")
	if err := producer.Publish(ctx, driver.OutboundMessage{Destination: exchange, Body: []byte("fanout")}); err != nil {
		t.Fatalf("Publish exchange: %v", err)
	}
	for name, deliveries := range map[string]<-chan amqp.Delivery{"a": deliveriesA, "b": deliveriesB} {
		select {
		case delivery := <-deliveries:
			if string(delivery.Body) != "fanout" {
				t.Fatalf("queue %s body = %q, want fanout", name, delivery.Body)
			}
			if err := delivery.Ack(false); err != nil {
				t.Fatalf("queue %s Ack: %v", name, err)
			}
		case <-ctx.Done():
			t.Fatalf("queue %s receive: %v", name, ctx.Err())
		}
	}
	if err := producer.Close(ctx); err != nil {
		t.Fatalf("Producer.Close: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("Conn.Close: %v", err)
	}
}

func openProducerFixture(t *testing.T, ctx context.Context, queue string) (driver.Conn, *amqp.Channel) {
	t.Helper()
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	channel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	_, _ = channel.QueueDelete(queue, false, false, false)
	t.Cleanup(func() {
		_, _ = channel.QueueDelete(queue, false, false, false)
		_ = channel.Close()
	})
	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	return conn, channel
}
