package rabbitmq

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestInboundMessageReceivedAtIsReceiptTime(t *testing.T) {
	receiptAt := time.Date(2030, time.January, 2, 3, 4, 5, 6, time.UTC)
	timestamp := receiptAt.Add(-time.Hour)
	delivery := amqp.Delivery{
		Timestamp: timestamp,
		Headers: amqp.Table{
			"cloudEvents:specversion": "1.0",
			"cloudEvents:id":          "event-1",
			"cloudEvents:source":      "/test",
			"cloudEvents:type":        "test.event",
		},
	}

	consumer := &consumer{clock: clock.NewFake(receiptAt)}
	message := consumer.inboundMessage("orders", delivery, nil, false, false)
	if !message.ReceivedAt.Equal(receiptAt) {
		t.Fatalf("ReceivedAt = %s, want fake receipt time %s", message.ReceivedAt, receiptAt)
	}

	var timeHeader string
	for _, header := range message.Headers {
		if header.Key == "time" {
			timeHeader = string(header.Value)
			break
		}
	}
	gotTimestamp, err := time.Parse(time.RFC3339Nano, timeHeader)
	if err != nil {
		t.Fatalf("time header = %q: %v", timeHeader, err)
	}
	if !gotTimestamp.Equal(timestamp) {
		t.Fatalf("time header = %s, want %s", gotTimestamp, timestamp)
	}

	message = consumer.inboundMessage("orders", amqp.Delivery{}, nil, false, false)
	if !message.ReceivedAt.Equal(receiptAt) {
		t.Fatalf("zero-timestamp ReceivedAt = %s, want fake receipt time %s", message.ReceivedAt, receiptAt)
	}
}
