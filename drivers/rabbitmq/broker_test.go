package rabbitmq

import (
	"net"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	// Local fixture credentials are intentional and never used for production endpoints.
	defaultRabbitMQEndpoint = "amqp://guest:guest@localhost:5672/" //nolint:gosec // test fixture endpoint
	rabbitMQEndpointEnv     = "F1_RABBITMQ_ENDPOINT"
)

var defaultEndpoint = resolveRabbitMQEndpoint()

func resolveRabbitMQEndpoint() string {
	if endpoint := os.Getenv(rabbitMQEndpointEnv); endpoint != "" {
		return endpoint
	}
	return defaultRabbitMQEndpoint
}

func brokerAddress(endpoint string) string {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Hostname() == "" {
		return ""
	}
	port := parsed.Port()
	if port == "" {
		port = "5672"
		if parsed.Scheme == "amqps" {
			port = "5671"
		}
	}
	return net.JoinHostPort(parsed.Hostname(), port)
}

func TestResolveRabbitMQEndpoint(t *testing.T) {
	tests := []struct {
		name       string
		configured string
		want       string
	}{
		{name: "default", want: defaultRabbitMQEndpoint},
		{name: "configured", configured: "amqp://localhost:25672/", want: "amqp://localhost:25672/"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(rabbitMQEndpointEnv, tc.configured)
			if got := resolveRabbitMQEndpoint(); got != tc.want {
				t.Fatalf("resolveRabbitMQEndpoint() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBrokerAddressRejectsInvalidEndpoint(t *testing.T) {
	endpoint := "amqp://" + "review-user" + ":" + "review-secret" + "@%zz"
	if got := brokerAddress(endpoint); got != "" {
		t.Fatalf("brokerAddress() = %q, want empty address", got)
	}
}

func TestBrokerAddressUsesAMQPDefaultPort(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		want     string
	}{
		{name: "amqp", endpoint: "amqp://localhost/", want: "localhost:5672"},
		{name: "amqps", endpoint: "amqps://localhost/", want: "localhost:5671"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := brokerAddress(tc.endpoint); got != tc.want {
				t.Fatalf("brokerAddress(%q) = %q, want %q", tc.endpoint, got, tc.want)
			}
		})
	}
}

var brokerProbe struct {
	once   sync.Once
	reason string
}

// requireBroker probes the RabbitMQ fixture once per package and fails the
// calling test when it is unreachable. There is no skip branch: a test that needs
// this fixture lives in an _integration_test.go file, so the only honest outcomes
// for it are a pass and a failure that names the missing fixture. The probe runs
// once per package, so one dial decides the whole suite.
func requireBroker(t *testing.T) {
	t.Helper()
	brokerProbe.once.Do(func() {
		conn, err := net.DialTimeout("tcp", brokerAddress(defaultEndpoint), 2*time.Second)
		if err != nil {
			brokerProbe.reason = err.Error()
			return
		}
		_ = conn.Close()
	})
	if brokerProbe.reason != "" {
		t.Fatalf("RabbitMQ fixture unreachable (%s); start it with `make broker-up`", brokerProbe.reason)
	}
}
