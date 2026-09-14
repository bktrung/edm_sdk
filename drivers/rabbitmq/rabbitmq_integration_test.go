//go:build integration

// Tests in this file open a connection to the RabbitMQ fixture, so the file
// carries the integration tag. The rest of the package's connection tests need no
// broker, including the ones that dial a listener they start themselves, and they
// live in rabbitmq_test.go.
package rabbitmq

import (
	"context"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestConnectionCapabilitiesFollowQueueType(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cases := []struct {
		name          string
		options       map[string]string
		wantNative    bool
		wantNativeDLQ bool
	}{
		{name: "default quorum", wantNative: true, wantNativeDLQ: true},
		{name: "classic", options: map[string]string{"rabbitmq.queueType": "classic"}, wantNative: false, wantNativeDLQ: false},
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
			if got := conn.Capabilities().NativeDLQ; got != tc.wantNativeDLQ {
				t.Fatalf("NativeDLQ = %t, want %t", got, tc.wantNativeDLQ)
			}
		})
	}
}

func TestOpenAllowsPlaintextLoopback(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open() error = %v, want loopback plaintext connection", err)
	}
	defer func() { _ = conn.Close(ctx) }()
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

type expiredDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c expiredDeadlineContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

func TestCloseTimeoutRemainsRetryable(t *testing.T) {
	requireBroker(t)
	connContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	publicConn, err := (Driver{}).Open(connContext, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	rabbitConn := publicConn.(*conn)

	closeErr := publicConn.Close(expiredDeadlineContext{
		Context:  context.Background(),
		deadline: time.Unix(0, 0),
	})
	if closeErr == nil {
		t.Fatal("Close() error = nil, want underlying close timeout")
	}
	if rabbitConn.closed {
		t.Fatal("connection marked closed after failed underlying close; a later Close cannot retry")
	}
	if retryErr := publicConn.Close(context.Background()); retryErr != nil {
		t.Fatalf("retry Close() error = %v, want success after retry", retryErr)
	}
}
