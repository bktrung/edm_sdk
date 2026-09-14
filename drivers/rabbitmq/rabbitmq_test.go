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

// TestClassifyAMQPTransportLoss covers the close codes amqp091-go raises on the
// client when the transport dies. The library marks those with Server false,
// and the caller's fallback cannot decide them: the Qos, Confirm and topology
// paths pass fatal, so a connection lost while a consumer reopens would end the
// subscription. A code in the same range that the broker sent stays fatal,
// because the broker sends those for protocol misuse by the client.
func TestClassifyAMQPTransportLoss(t *testing.T) {
	for _, test := range []struct {
		name        string
		err         error
		fallback    driver.Kind
		want        driver.Kind
		wantMissing bool
	}{
		{
			name:     "client frame error with a fatal fallback",
			err:      &amqp.Error{Code: 501, Reason: "connection reset by peer"},
			fallback: driver.KindFatal,
			want:     driver.KindTransient,
		},
		{
			name:     "client channel error with a fatal fallback",
			err:      amqp.ErrClosed,
			fallback: driver.KindFatal,
			want:     driver.KindTransient,
		},
		{
			name:     "client channel error with a not found fallback",
			err:      amqp.ErrClosed,
			fallback: driver.KindNotFound,
			want:     driver.KindTransient,
		},
		{
			name:     "wrapped client channel error",
			err:      fmt.Errorf("publish: %w", amqp.ErrClosed),
			fallback: driver.KindFatal,
			want:     driver.KindTransient,
		},
		{
			name:     "client syntax error with a fatal fallback",
			err:      amqp.ErrSyntax,
			fallback: driver.KindFatal,
			want:     driver.KindTransient,
		},
		{
			name:     "client command invalid with a fatal fallback",
			err:      amqp.ErrCommandInvalid,
			fallback: driver.KindFatal,
			want:     driver.KindTransient,
		},
		{
			name:     "server frame error with a transient fallback",
			err:      &amqp.Error{Code: 501, Reason: "frame could not be parsed", Server: true},
			fallback: driver.KindTransient,
			want:     driver.KindFatal,
		},
		{
			name:     "server channel error with a transient fallback",
			err:      &amqp.Error{Code: 504, Reason: "channel error", Server: true},
			fallback: driver.KindTransient,
			want:     driver.KindFatal,
		},
		{
			name:     "server connection forced",
			err:      &amqp.Error{Code: 320, Reason: "CONNECTION_FORCED", Server: true},
			fallback: driver.KindTransient,
			want:     driver.KindTransient,
		},
		{
			name:        "destination missing",
			err:         &amqp.Error{Code: 404, Reason: "NOT_FOUND"},
			fallback:    driver.KindTransient,
			want:        driver.KindNotFound,
			wantMissing: true,
		},
		{
			name:     "client credentials refusal",
			err:      amqp.ErrCredentials,
			fallback: driver.KindTransient,
			want:     driver.KindPermission,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := classifyAMQP("consume", test.fallback, test.err)
			kind, ok := driver.Classify(err)
			if !ok || kind != test.want {
				t.Fatalf("Classify(%v) = %v, %t, want %v, true", err, kind, ok, test.want)
			}
			if got := errors.Is(err, driver.ErrDestinationMissing); got != test.wantMissing {
				t.Fatalf("errors.Is(%v, ErrDestinationMissing) = %t, want %t", err, got, test.wantMissing)
			}
		})
	}
}
