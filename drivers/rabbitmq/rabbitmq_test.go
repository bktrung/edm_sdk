package rabbitmq

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestDriverCapabilities(t *testing.T) {
	caps := Driver{}.Capabilities()
	if !caps.PerMessageAck || caps.NativeDelay || !caps.NativeDLQ {
		t.Fatalf("capabilities = %#v, want per-message ack, no native delay, and native DLQ", caps)
	}
	if caps.NativePriority != driver.PriorityStrict || caps.NativePriorityLevels != 32 {
		t.Fatalf("priority capabilities = %#v, want strict 32 levels", caps)
	}
	if caps.Fanout != driver.FanoutAtPublish || caps.OrderedByKey {
		t.Fatalf("routing capabilities = %#v, want publish fanout and unordered keys", caps)
	}
}

func TestConfiguredQueueKind(t *testing.T) {
	cases := []struct {
		name    string
		options map[string]string
		want    queueKind
		ok      bool
	}{
		{name: "default", want: queueKindQuorum, ok: true},
		{name: "quorum", options: map[string]string{"rabbitmq.queueType": "quorum"}, want: queueKindQuorum, ok: true},
		{name: "classic", options: map[string]string{"rabbitmq.queueType": "classic"}, want: queueKindClassic, ok: true},
		{name: "case and spaces", options: map[string]string{"rabbitmq.queueType": " CLASSIC "}, want: queueKindClassic, ok: true},
		{name: "unsupported", options: map[string]string{"rabbitmq.queueType": "stream"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := configuredQueueKind(tc.options)
			if tc.ok {
				if err != nil || got != tc.want {
					t.Fatalf("configuredQueueKind() = %q, %v, want %q", got, err, tc.want)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "quorum, classic") {
				t.Fatalf("configuredQueueKind() error = %v, want supported queue list", err)
			}
		})
	}
}

func TestQueueKindCapabilities(t *testing.T) {
	if got := capabilitiesForQueueKind(queueKindQuorum).NativeDeliveryCount; !got {
		t.Fatal("quorum capabilities NativeDeliveryCount = false, want true")
	}
	if got := capabilitiesForQueueKind(queueKindClassic).NativeDeliveryCount; got {
		t.Fatal("classic capabilities NativeDeliveryCount = true, want false")
	}
}

func TestConnectionCapabilitiesFollowQueueType(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cases := []struct {
		name       string
		options    map[string]string
		wantNative bool
	}{
		{name: "default quorum", wantNative: true},
		{name: "classic", options: map[string]string{"rabbitmq.queueType": "classic"}, wantNative: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := (Driver{}).Open(ctx, driver.Config{
				Endpoints: []string{defaultEndpoint}, DriverOptions: tc.options,
			})
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			defer func() { _ = conn.Close(ctx) }()
			if got := conn.Capabilities().NativeDeliveryCount; got != tc.wantNative {
				t.Fatalf("NativeDeliveryCount = %t, want %t", got, tc.wantNative)
			}
		})
	}
}

func TestOpenRejectsSCRAM(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := (Driver{}).Open(ctx, driver.Config{SASL: &driver.SASLConfig{Mechanism: "scram-sha-256"}})
	if err == nil {
		t.Fatal("Open() error = nil, want unsupported SASL error")
	}
	if !strings.Contains(err.Error(), "PLAIN, AMQPLAIN, EXTERNAL") {
		t.Fatalf("Open() error = %v, want supported mechanism list", err)
	}
	if kind, ok := driver.Classify(err); !ok || kind != driver.KindFatal {
		t.Fatalf("Classify(Open error) = %v, %t, want fatal, true", kind, ok)
	}
}

func TestDriverOpenPingClose(t *testing.T) {
	requireBroker(t)
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
