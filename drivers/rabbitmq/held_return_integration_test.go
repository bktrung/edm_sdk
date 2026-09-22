//go:build integration

package rabbitmq

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestHeldReturnVariants(t *testing.T) {
	requireBroker(t)
	logRabbitMQBrokerVersion(t)
	for _, variant := range []string{"close", "nack-multiple", "nack-each", "reject-each"} {
		t.Run(variant, func(t *testing.T) {
			runHeldReturnVariant(t, variant)
		})
	}
}

func runHeldReturnVariant(t *testing.T, variant string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queue := "rabbitmq-held-return-" + variant
	publisher := declareBrokerPrefetchQueue(t, queue)
	publishBrokerPrefetchMessages(t, ctx, publisher, queue, brokerPrefetchSafetyValue)
	if err := publisher.Qos(brokerPrefetchSafetyValue, 0, false); err != nil {
		t.Fatalf("Qos: %v", err)
	}

	consumerTag := "held-return-" + variant
	deliveries, err := publisher.Consume(queue, consumerTag, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	held := make([]amqp.Delivery, 0, brokerPrefetchSafetyValue)
	for range brokerPrefetchSafetyValue {
		select {
		case delivery := <-deliveries:
			held = append(held, delivery)
		case <-ctx.Done():
			t.Fatalf("waiting for held delivery %d: %v", len(held)+1, ctx.Err())
		}
	}

	switch variant {
	case "close":
		if err := publisher.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	case "nack-multiple":
		if err := publisher.Cancel(consumerTag, false); err != nil {
			t.Fatalf("Cancel: %v", err)
		}
		if err := publisher.Nack(held[len(held)-1].DeliveryTag, true, true); err != nil {
			t.Fatalf("Nack(multiple): %v", err)
		}
		if err := publisher.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	case "nack-each":
		if err := publisher.Cancel(consumerTag, false); err != nil {
			t.Fatalf("Cancel: %v", err)
		}
		for _, delivery := range held {
			if err := publisher.Nack(delivery.DeliveryTag, false, true); err != nil {
				t.Fatalf("Nack(%d): %v", delivery.DeliveryTag, err)
			}
		}
		if err := publisher.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	case "reject-each":
		if err := publisher.Cancel(consumerTag, false); err != nil {
			t.Fatalf("Cancel: %v", err)
		}
		for _, delivery := range held {
			if err := publisher.Reject(delivery.DeliveryTag, true); err != nil {
				t.Fatalf("Reject(%d): %v", delivery.DeliveryTag, err)
			}
		}
		if err := publisher.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	default:
		t.Fatalf("unknown variant %q", variant)
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
	if err := reopenedChannel.Qos(brokerPrefetchSafetyValue, 0, false); err != nil {
		t.Fatalf("Qos reopen: %v", err)
	}
	redeliveries, err := reopenedChannel.Consume(queue, "held-return-second-"+variant, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume reopen: %v", err)
	}
	deliveryCount, acquiredCount := -1, -1
	deliveryCounts := make(map[int]int)
	acquiredCounts := make(map[int]int)
	for index := range brokerPrefetchSafetyValue {
		select {
		case delivery := <-redeliveries:
			currentDeliveryCount := heldReturnHeaderCount(delivery.Headers, "x-delivery-count")
			currentAcquiredCount := heldReturnHeaderCount(delivery.Headers, "x-acquired-count")
			if index == 0 {
				deliveryCount = currentDeliveryCount
				acquiredCount = currentAcquiredCount
			}
			deliveryCounts[currentDeliveryCount]++
			acquiredCounts[currentAcquiredCount]++
			if err := delivery.Ack(false); err != nil {
				t.Fatalf("Ack redelivery: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("waiting for redelivery %d: %v", index+1, ctx.Err())
		}
	}
	t.Logf(
		"variant=%s x-delivery-count=%d x-acquired-count=%d redeliveries=%d distributions=(delivery:%v acquired:%v)",
		variant,
		deliveryCount,
		acquiredCount,
		brokerPrefetchSafetyValue,
		deliveryCounts,
		acquiredCounts,
	)
}

func heldReturnHeaderCount(headers amqp.Table, name string) int {
	count, ok := headerCount(headers[name])
	if !ok {
		return -1
	}
	return count
}

func TestHeldReturnDeliveryLimit(t *testing.T) {
	requireBroker(t)
	logRabbitMQBrokerVersion(t)
	for _, variant := range []string{"nack-each", "close"} {
		t.Run(variant, func(t *testing.T) {
			runHeldReturnDeliveryLimitVariant(t, variant)
		})
	}
}

func runHeldReturnDeliveryLimitVariant(t *testing.T, variant string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queue := "rabbitmq-held-return-limit-" + variant
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() {
		cleanupChannel, cleanupErr := raw.Channel()
		if cleanupErr == nil {
			_, _ = cleanupChannel.QueueDelete(queue, false, false, false)
			_ = cleanupChannel.Close()
		}
		_ = raw.Close()
	})
	setupChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	_, _ = setupChannel.QueueDelete(queue, false, false, false)
	if _, err := setupChannel.QueueDeclare(queue, true, false, false, false, amqp.Table{
		"x-queue-type":     "quorum",
		"x-delivery-limit": int32(1),
	}); err != nil {
		_ = setupChannel.Close()
		t.Fatalf("QueueDeclare: %v", err)
	}
	if err := setupChannel.Confirm(false); err != nil {
		_ = setupChannel.Close()
		t.Fatalf("Confirm: %v", err)
	}
	confirmations := setupChannel.NotifyPublish(make(chan amqp.Confirmation, brokerPrefetchSafetyValue))
	publishBrokerPrefetchMessages(t, ctx, setupChannel, queue, brokerPrefetchSafetyValue)
	for index := range brokerPrefetchSafetyValue {
		select {
		case confirmation := <-confirmations:
			if !confirmation.Ack {
				_ = setupChannel.Close()
				t.Fatalf("Publish confirmation %d was nack", index)
			}
		case <-ctx.Done():
			_ = setupChannel.Close()
			t.Fatalf("waiting for publish confirmation %d: %v", index+1, ctx.Err())
		}
	}
	ready, err := setupChannel.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		_ = setupChannel.Close()
		t.Fatalf("QueueDeclarePassive before cycles: %v", err)
	}
	t.Logf("limit variant=%s ready-before-publish-close=%d", variant, ready.Messages)
	if err := setupChannel.Close(); err != nil {
		t.Fatalf("Close setup channel: %v", err)
	}

	cycleCounts := make([]int, 0, 3)
	for range 3 {
		channel, err := raw.Channel()
		if err != nil {
			t.Fatalf("Channel cycle: %v", err)
		}
		if err := channel.Qos(brokerPrefetchSafetyValue, 0, false); err != nil {
			_ = channel.Close()
			t.Fatalf("Qos cycle: %v", err)
		}
		consumerTag := "held-return-limit-" + variant
		deliveries, err := channel.Consume(queue, consumerTag, false, false, false, false, nil)
		if err != nil {
			_ = channel.Close()
			t.Fatalf("Consume cycle: %v", err)
		}
		held := make([]amqp.Delivery, 0, brokerPrefetchSafetyValue)
		cycleCtx, cycleCancel := context.WithTimeout(ctx, 2*time.Second)
	cycleRead:
		for len(held) < brokerPrefetchSafetyValue {
			select {
			case delivery, ok := <-deliveries:
				if !ok {
					break cycleRead
				}
				held = append(held, delivery)
			case <-cycleCtx.Done():
				break cycleRead
			}
		}
		cycleCancel()
		cycleCounts = append(cycleCounts, len(held))
		if variant == "nack-each" {
			if err := channel.Cancel(consumerTag, false); err != nil {
				_ = channel.Close()
				t.Fatalf("Cancel cycle: %v", err)
			}
			for _, delivery := range held {
				if err := delivery.Nack(false, true); err != nil {
					_ = channel.Close()
					t.Fatalf("Nack(%d): %v", delivery.DeliveryTag, err)
				}
			}
		}
		if err := channel.Close(); err != nil {
			t.Fatalf("Close cycle: %v", err)
		}
	}
	t.Logf("limit variant=%s cycle-deliveries=%v", variant, cycleCounts)

	drainChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel drain: %v", err)
	}
	defer drainChannel.Close()
	inspect, err := drainChannel.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclarePassive: %v", err)
	}
	t.Logf("limit variant=%s cycles=3 ready-before-drain=%d", variant, inspect.Messages)
	if inspect.Messages == 0 {
		t.Logf("limit variant=%s drained=0", variant)
		return
	}
	if err := drainChannel.Qos(brokerPrefetchSafetyValue, 0, false); err != nil {
		t.Fatalf("Qos drain: %v", err)
	}
	deliveries, err := drainChannel.Consume(queue, "held-return-limit-drain-"+variant, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume drain: %v", err)
	}
	drained := 0
	for index := range inspect.Messages {
		select {
		case delivery := <-deliveries:
			drained++
			if err := delivery.Ack(false); err != nil {
				t.Fatalf("Ack drain %d: %v", index, err)
			}
		case <-ctx.Done():
			t.Fatalf("waiting for drain delivery %d: %v", index+1, ctx.Err())
		}
	}
	t.Logf("limit variant=%s drained=%d", variant, drained)
}

func logRabbitMQBrokerVersion(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open for broker version: %v", err)
	}
	t.Logf("RabbitMQ broker version = %s", conn.BrokerInfo().Version)
	_ = conn.Close(ctx)
}
