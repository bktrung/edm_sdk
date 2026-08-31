package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net"
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
	if caps.Fanout != driver.FanoutAtPublish || !caps.OrderedByKey {
		t.Fatalf("routing capabilities = %#v, want publish fanout and ordered keys", caps)
	}
}

func TestCopyBrokerInfoClonesExtra(t *testing.T) {
	source := driver.BrokerInfo{
		Extra: map[string]string{"region": "saigon"},
	}

	got := copyBrokerInfo(source)
	if got.Extra["region"] != "saigon" {
		t.Errorf("copyBrokerInfo().Extra[region] = %q, want saigon", got.Extra["region"])
	}

	got.Extra["region"] = "singapore"
	if source.Extra["region"] != "saigon" {
		t.Errorf("mutating copied Extra changed source: got %q, want saigon", source.Extra["region"])
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
	quorum := capabilitiesForQueueKind(queueKindQuorum)
	if !quorum.NativeDeliveryCount {
		t.Fatal("quorum capabilities NativeDeliveryCount = false, want true")
	}
	if !quorum.NativeDLQ {
		t.Fatal("quorum capabilities NativeDLQ = false, want true")
	}
	classic := capabilitiesForQueueKind(queueKindClassic)
	if classic.NativeDeliveryCount {
		t.Fatal("classic capabilities NativeDeliveryCount = true, want false")
	}
	if classic.NativeDLQ {
		t.Fatal("classic capabilities NativeDLQ = true, want false")
	}
}

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

func TestExpirationMillisClampsLargeDelay(t *testing.T) {
	if got := expirationMillis(100 * 365 * 24 * time.Hour); got != "2147483647" {
		t.Fatalf("expirationMillis(100 years) = %q, want 2147483647", got)
	}
}

func TestOpenRejectsSCRAM(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}, SASL: &driver.SASLConfig{Mechanism: "scram-sha-256"}})
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

func TestOpenRejectsEmptyEndpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := (Driver{}).Open(ctx, driver.Config{})
	if err == nil || !errors.Is(err, errMissingEndpoints) {
		t.Fatalf("Open() error = %v, want named empty-endpoint error", err)
	}
	if kind, ok := driver.Classify(err); !ok || kind != driver.KindFatal {
		t.Fatalf("Classify(Open error) = %v, %t, want fatal, true", kind, ok)
	}
}

func TestOpenRefusesPlaintextRemoteBeforeDialing(t *testing.T) {
	listener, accepted := listenerForOpenAttempt(t)
	endpoint := fmt.Sprintf("amqp://0.0.0.0:%d/", listener.Addr().(*net.TCPAddr).Port)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{endpoint}, ConnectTimeout: time.Second})
	if err == nil {
		t.Fatal("Open() error = nil, want plaintext endpoint refusal")
	}
	if receivedOpenAttempt(accepted) {
		t.Fatal("Open() attempted a connection before refusing the plaintext remote endpoint")
	}
	if !strings.Contains(err.Error(), "amqps://") {
		t.Fatalf("Open() error = %v, want plaintext endpoint refusal", err)
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

func TestOpenAllowsAmqpsEndpoint(t *testing.T) {
	for _, test := range []struct {
		name string
		tls  *driver.TLSConfig
	}{
		{name: "without TLS block"},
		{name: "with TLS block", tls: &driver.TLSConfig{Enabled: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener, accepted := listenerForOpenAttempt(t)
			endpoint := fmt.Sprintf("amqps://0.0.0.0:%d/", listener.Addr().(*net.TCPAddr).Port)
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()

			_, _ = (Driver{}).Open(ctx, driver.Config{Endpoints: []string{endpoint}, TLS: test.tls})
			if !receivedOpenAttempt(accepted) {
				t.Fatal("Open() refused the amqps endpoint before dialing")
			}
		})
	}
}

func listenerForOpenAttempt(t *testing.T) (net.Listener, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	accepted := make(chan struct{}, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- struct{}{}
		_ = conn.Close()
	}()
	return listener, accepted
}

func receivedOpenAttempt(accepted <-chan struct{}) bool {
	select {
	case <-accepted:
		return true
	case <-time.After(100 * time.Millisecond): //nolint:forbidigo // bounded socket acceptance observation
		return false
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
