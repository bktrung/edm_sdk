package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDriverCapabilities(t *testing.T) {
	caps := Driver{}.Capabilities()
	if !caps.PerMessageAck || caps.NativeDelay || !caps.NativeDLQ {
		t.Fatalf("capabilities = %#v, want per-message ack, no native delay, and native DLQ", caps)
	}
	if caps.NativePriority != driver.PriorityStrict || caps.NativePriorityLevels != 32 {
		t.Fatalf("priority capabilities = %#v, want strict 32 levels", caps)
	}
}

func TestDriverOpenPingClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}, ConnectTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if got := conn.BrokerInfo().Kind; got != "rabbitmq" {
		t.Fatalf("BrokerInfo().Kind = %q, want rabbitmq", got)
	}
	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestClassifyAMQPNotFound(t *testing.T) {
	err := classifyAMQP("consume", driver.KindTransient, &amqp.Error{Code: 404, Reason: "not found"})
	if !errors.Is(err, driver.ErrDestinationMissing) {
		t.Fatalf("error = %v, want ErrDestinationMissing", err)
	}
	kind, ok := driver.Classify(err)
	if !ok || kind != driver.KindNotFound {
		t.Fatalf("Classify(%v) = %v, %t, want not_found, true", err, kind, ok)
	}
}
