package rabbitmq

import (
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestInboundMessageEnqueueTime(t *testing.T) {
	brokerAt := time.UnixMilli(1_724_123_456_789)
	int32BrokerAt := time.UnixMilli(1_234_567_000)
	producerAt := time.Date(2026, time.January, 2, 3, 4, 5, 678_000_000, time.UTC)
	deliveryAt := time.Unix(1_724_123_456, 0)

	tests := []struct {
		name       string
		trust      bool
		headers    amqp.Table
		deliveryAt time.Time
		wantAt     time.Time
		wantSource driver.EnqueueSource
	}{
		{
			name:       "trusted broker milliseconds win",
			trust:      true,
			headers:    amqp.Table{"timestamp_in_ms": brokerAt.UnixMilli(), "cloudEvents:time": producerAt.Format(time.RFC3339Nano)},
			deliveryAt: deliveryAt,
			wantAt:     brokerAt,
			wantSource: driver.EnqueueSourceBroker,
		},
		{
			name:       "trusted broker int32 milliseconds",
			trust:      true,
			headers:    amqp.Table{"timestamp_in_ms": int32(1_234_567_000)},
			wantAt:     int32BrokerAt,
			wantSource: driver.EnqueueSourceBroker,
		},
		{
			name:       "trusted without broker header uses producer",
			trust:      true,
			headers:    amqp.Table{"cloudEvents:time": producerAt.Format(time.RFC3339Nano)},
			deliveryAt: deliveryAt,
			wantAt:     producerAt,
			wantSource: driver.EnqueueSourceProducer,
		},
		{
			name:       "untrusted broker header is ignored",
			trust:      false,
			headers:    amqp.Table{"timestamp_in_ms": brokerAt.UnixMilli()},
			deliveryAt: deliveryAt,
			wantAt:     deliveryAt,
			wantSource: driver.EnqueueSourceProducer,
		},
		{
			name:       "cloud events time beats delivery timestamp",
			trust:      false,
			headers:    amqp.Table{"cloudEvents:time": producerAt.Format(time.RFC3339Nano)},
			deliveryAt: deliveryAt,
			wantAt:     producerAt,
			wantSource: driver.EnqueueSourceProducer,
		},
		{
			name:       "delivery timestamp uses seconds",
			deliveryAt: deliveryAt,
			wantAt:     deliveryAt,
			wantSource: driver.EnqueueSourceProducer,
		},
		{
			name:       "invalid broker header falls through",
			trust:      true,
			headers:    amqp.Table{"timestamp_in_ms": "not-a-timestamp", "cloudEvents:time": producerAt.Format(time.RFC3339Nano)},
			deliveryAt: deliveryAt,
			wantAt:     producerAt,
			wantSource: driver.EnqueueSourceProducer,
		},
		{
			name:       "nothing is unknown",
			wantSource: driver.EnqueueSourceUnknown,
		},
	}

	receiptAt := time.Date(2030, time.January, 2, 3, 4, 5, 0, time.UTC)
	consumer := &consumer{clock: clock.NewFake(receiptAt)}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			message := consumer.inboundMessage("orders", amqp.Delivery{Headers: tc.headers, Timestamp: tc.deliveryAt}, nil, false, tc.trust)
			if !message.EnqueuedAt.Equal(tc.wantAt) {
				t.Fatalf("EnqueuedAt = %s, want %s", message.EnqueuedAt, tc.wantAt)
			}
			if message.EnqueuedAtSource != tc.wantSource {
				t.Fatalf("EnqueuedAtSource = %q, want %q", message.EnqueuedAtSource, tc.wantSource)
			}
			if tc.wantAt.IsZero() && message.ReceivedAt.Equal(message.EnqueuedAt) {
				t.Fatalf("ReceivedAt = EnqueuedAt = %s; receipt time must not become enqueue time", message.ReceivedAt)
			}
		})
	}
}

func TestResolveTrustBrokerTimestamp(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    bool
		wantErr bool
	}{
		{name: "true", value: "true", want: true},
		{name: "trimmed false", value: " FALSE ", want: false},
		{name: "empty defaults false", value: "", want: false},
		{name: "invalid names key", value: "yes-please", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveTrustBrokerTimestamp(map[string]string{"rabbitmq.trustBrokerTimestamp": tc.value})
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "rabbitmq.trustBrokerTimestamp") {
					t.Fatalf("resolveTrustBrokerTimestamp() error = %v, want key in error", err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("resolveTrustBrokerTimestamp() = %t, %v, want %t", got, err, tc.want)
			}
		})
	}
}
