package kafka

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

const (
	defaultKafkaEndpoint = "localhost:19092"
	kafkaEndpointEnv     = "F1_KAFKA_ENDPOINT"
)

var kafkaEndpoint = resolveKafkaEndpoint()

func resolveKafkaEndpoint() string {
	if endpoint := os.Getenv(kafkaEndpointEnv); endpoint != "" {
		return endpoint
	}
	return defaultKafkaEndpoint
}

func TestResolveKafkaEndpoint(t *testing.T) {
	tests := []struct {
		name       string
		configured string
		want       string
	}{
		{name: "default", want: "localhost:19092"},
		{name: "configured", configured: "localhost:29092", want: "localhost:29092"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(kafkaEndpointEnv, tc.configured)
			if got := resolveKafkaEndpoint(); got != tc.want {
				t.Fatalf("resolveKafkaEndpoint() = %q, want %q", got, tc.want)
			}
		})
	}
}

var brokerProbe struct {
	once   sync.Once
	reason string
}

// requireBroker probes the Kafka fixture once per package and fails the calling
// test when it is unreachable. There is no skip branch: a test that needs this
// fixture lives in an _integration_test.go file, so the only honest outcomes for
// it are a pass and a failure that names the missing fixture. The probe runs once
// per package, so one dial decides the whole suite.
func requireBroker(t *testing.T) {
	t.Helper()
	brokerProbe.once.Do(func() {
		conn, err := net.DialTimeout("tcp", kafkaEndpoint, 2*time.Second)
		if err != nil {
			brokerProbe.reason = err.Error()
			return
		}
		_ = conn.Close()
	})
	if brokerProbe.reason != "" {
		t.Fatalf("Kafka fixture unreachable (%s); start it with `make kafka-up`", brokerProbe.reason)
	}
}

// assertFatalOpenError checks the shape of an Open refusal: a classified fatal
// error, tagged with the open operation, carrying the expected text. It lives in
// this file rather than beside the tests that use it because tls_test.go needs it
// and tls_test.go is not an integration file, so a definition in an integration
// file would leave the broker-free build unable to compile.
func assertFatalOpenError(t *testing.T, err error, wantText string) {
	t.Helper()
	if err == nil {
		t.Fatal("Open() error = nil, want classified fatal error")
	}
	if !strings.Contains(err.Error(), wantText) {
		t.Fatalf("Open() error = %v, want text %q", err, wantText)
	}
	var classified *driver.Error
	if !errors.As(err, &classified) {
		t.Fatalf("Open() error = %T, want *driver.Error", err)
	}
	if classified.Op != "open" {
		t.Errorf("Open() error Op = %q, want open", classified.Op)
	}
	if classified.Kind() != driver.KindFatal {
		t.Errorf("Open() error Kind = %v, want fatal", classified.Kind())
	}
}

// noDialKafkaOption returns the option a test hands to kgo.NewClient when it
// builds a client to inspect its resolved options or to drive a teardown path
// whose network calls are stubbed out. franz-go dials a seed broker from a
// background metadata fetch, so a client built from a compiled-in seed reaches
// whatever another lane happens to be running on the default port, from a test
// that never needs a broker. Every such site shares this one option so the
// decision cannot be right at three call sites and wrong at a fourth.
func noDialKafkaOption() kgo.Opt {
	return kgo.Dialer(func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("test client never dials")
	})
}
