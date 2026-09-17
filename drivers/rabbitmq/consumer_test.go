package rabbitmq

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestInboundMessageReceivedAtIsReceiptTime(t *testing.T) {
	timestamp := time.Now().Add(-time.Hour) //nolint:forbidigo // the test compares receipt time with the real clock around the call
	delivery := amqp.Delivery{
		Timestamp: timestamp,
		Headers: amqp.Table{
			"cloudEvents:specversion": "1.0",
			"cloudEvents:id":          "event-1",
			"cloudEvents:source":      "/test",
			"cloudEvents:type":        "test.event",
		},
	}

	before := time.Now() //nolint:forbidigo // the test compares receipt time with the real clock around the call
	message := inboundMessage("orders", delivery, nil, false)
	after := time.Now() //nolint:forbidigo // the test compares receipt time with the real clock around the call
	if message.ReceivedAt.Before(before) || message.ReceivedAt.After(after) {
		t.Fatalf("ReceivedAt = %s, want between %s and %s", message.ReceivedAt, before, after)
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

	before = time.Now() //nolint:forbidigo // the test compares receipt time with the real clock around the call
	message = inboundMessage("orders", amqp.Delivery{}, nil, false)
	after = time.Now() //nolint:forbidigo // the test compares receipt time with the real clock around the call
	if message.ReceivedAt.Before(before) || message.ReceivedAt.After(after) {
		t.Fatalf("zero-timestamp ReceivedAt = %s, want between %s and %s", message.ReceivedAt, before, after)
	}
}
