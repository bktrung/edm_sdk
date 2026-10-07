package rabbitmq

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestAMQPHeadersAcceptTypedCloudEventValues(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{name: "nil", value: nil},
		{name: "bool", value: true},
		{name: "int", value: int(1)},
		{name: "int8", value: int8(-2)},
		{name: "int16", value: int16(-3)},
		{name: "int32", value: int32(-4)},
		{name: "int64", value: int64(-5)},
		{name: "uint", value: uint(6)},
		{name: "uint8", value: uint8(7)},
		{name: "uint16", value: uint16(8)},
		{name: "uint32", value: uint32(9)},
		{name: "uint64", value: uint64(10)},
		{name: "float32", value: float32(1.5)},
		{name: "float64", value: float64(2.5)},
		{name: "decimal", value: amqp.Decimal{Scale: 2, Value: 12345}},
		{name: "string", value: "value"},
		{name: "bytes", value: []byte("bytes")},
		{name: "time", value: time.Date(2026, 8, 5, 11, 0, 0, 0, time.UTC)},
		{name: "nested table", value: amqp.Table{"nested": "value"}},
		{name: "array", value: []any{true, int32(2), "three"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("amqpHeaders panicked: %v", recovered)
				}
			}()

			headers := amqpHeaders(amqp.Delivery{
				Headers: amqp.Table{"cloudEvents:typed": tt.value},
			})
			if len(headers) != 1 {
				t.Fatalf("got %d headers, want one", len(headers))
			}
			if headers[0].Key != "typed" {
				t.Fatalf("header key = %q, want %q", headers[0].Key, "typed")
			}
		})
	}
}
