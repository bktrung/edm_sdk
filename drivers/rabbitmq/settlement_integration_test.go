//go:build integration

package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestRabbitMQSettlementRequeueIncrementsDeliveryCount(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, consumer, channel := newSettlementFixture(t, ctx, "rabbitmq-driver-settlement-requeue")

	if err := channel.PublishWithContext(ctx, "", "rabbitmq-driver-settlement-requeue", false, false, amqp.Publishing{Body: []byte("body")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	first := receiveSettlement(t, ctx, consumer)
	if first.DeliveryCount != 0 {
		t.Fatalf("first DeliveryCount = %d, want 0", first.DeliveryCount)
	}
	if err := first.Settle.Nack(ctx, driver.NackOptions{Requeue: true, CountAsFailure: true}); err != nil {
		t.Fatalf("Nack(requeue): %v", err)
	}
	// Exact values, not "greater than": a driver that cannot read the broker's
	// counter and returns a constant on Redelivered satisfies an inequality, and
	// this assertion is the only thing standing between that and a green suite.
	second := receiveSettlement(t, ctx, consumer)
	if second.DeliveryCount != 1 {
		t.Fatalf("first redelivery DeliveryCount = %d, want 1", second.DeliveryCount)
	}
	if err := second.Settle.Nack(ctx, driver.NackOptions{Requeue: true, CountAsFailure: true}); err != nil {
		t.Fatalf("Nack(requeue) second: %v", err)
	}
	third := receiveSettlement(t, ctx, consumer)
	if third.DeliveryCount != 2 {
		t.Fatalf("second redelivery DeliveryCount = %d, want 2", third.DeliveryCount)
	}
	if err := third.Settle.Nack(ctx, driver.NackOptions{}); err != nil {
		t.Fatalf("Nack(discard): %v", err)
	}
	if err := consumer.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestRabbitMQSettlementConcurrentCallsSingleFlight(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, consumer, channel := newSettlementFixture(t, ctx, "rabbitmq-driver-settlement-concurrent")

	if err := channel.PublishWithContext(ctx, "", "rabbitmq-driver-settlement-concurrent", false, false, amqp.Publishing{Body: []byte("body")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	message := receiveSettlement(t, ctx, consumer)
	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			results <- message.Settle.Ack(ctx)
		}()
	}
	close(start)

	var successes, alreadySettled int
	for range 2 {
		err := <-results
		switch {
		case err == nil:
			successes++
		case errors.Is(err, driver.ErrAlreadySettled):
			alreadySettled++
		default:
			t.Fatalf("concurrent settlement error = %v", err)
		}
	}
	if successes != 1 || alreadySettled != 1 {
		t.Fatalf("concurrent settlement results = successes %d, already-settled %d, want 1, 1", successes, alreadySettled)
	}
	if err := consumer.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestRabbitMQReleaseRequeuesOutstandingDelivery(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, consumer, channel := newSettlementFixture(t, ctx, "rabbitmq-driver-release-requeue")

	if err := channel.PublishWithContext(ctx, "", "rabbitmq-driver-release-requeue", false, false, amqp.Publishing{Body: []byte("body")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	first := receiveSettlement(t, ctx, consumer)
	if err := consumer.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}

	second, err := conn.Consumer(ctx, driver.ConsumerConfig{
		Destinations: []string{"rabbitmq-driver-release-requeue"},
		Prefetch:     1,
		PerDestination: map[string]int{
			"rabbitmq-driver-release-requeue": 1,
		},
	})
	if err != nil {
		t.Fatalf("second Consumer: %v", err)
	}
	redelivered := receiveSettlement(t, ctx, second)
	if string(redelivered.Body) != string(first.Body) {
		t.Fatalf("redelivered body = %q, want %q", redelivered.Body, first.Body)
	}
	if err := redelivered.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack redelivery: %v", err)
	}
	if err := second.Stop(ctx); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestRabbitMQReleaseClosesAndIsIdempotent(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, consumer, _ := newSettlementFixture(t, ctx, "rabbitmq-driver-release-idempotent")

	if err := consumer.Release(ctx); err != nil {
		t.Fatalf("first Release: %v", err)
	}
	if err := consumer.Release(ctx); err != nil {
		t.Fatalf("second Release: %v", err)
	}
	select {
	case _, ok := <-consumer.Messages():
		if ok {
			t.Fatal("Messages remained open after Release")
		}
	case <-ctx.Done():
		t.Fatalf("waiting for Messages close: %v", ctx.Err())
	}
	if err := consumer.Stop(ctx); err != nil {
		t.Fatalf("Stop after Release: %v", err)
	}

	stopped, err := conn.Consumer(ctx, driver.ConsumerConfig{
		Destinations: []string{"rabbitmq-driver-release-idempotent"},
		Prefetch:     1,
	})
	if err != nil {
		t.Fatalf("stopped Consumer: %v", err)
	}
	if err := stopped.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := stopped.Release(ctx); err != nil {
		t.Fatalf("Release after Stop: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func newSettlementFixture(t *testing.T, ctx context.Context, queue string) (driver.Conn, driver.Consumer, *amqp.Channel) {
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
	t.Cleanup(func() {
		_, _ = channel.QueueDelete(queue, false, false, false)
		_ = channel.Close()
	})
	_, _ = channel.QueueDelete(queue, false, false, false)
	if _, err := channel.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
		t.Fatalf("QueueDeclare: %v", err)
	}
	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{queue}, Prefetch: 1, PerDestination: map[string]int{queue: 1}})
	if err != nil {
		_ = conn.Close(ctx)
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() {
		_ = consumer.Stop(ctx)
		_ = conn.Close(ctx)
	})
	return conn, consumer, channel
}

func receiveSettlement(t *testing.T, ctx context.Context, consumer driver.Consumer) driver.InboundMessage {
	t.Helper()
	select {
	case message := <-consumer.Messages():
		return message
	case <-ctx.Done():
		t.Fatalf("receive: %v", ctx.Err())
		return driver.InboundMessage{}
	}
}
