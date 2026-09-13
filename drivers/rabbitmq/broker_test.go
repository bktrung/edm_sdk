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
	requireBrokerEnv        = "F1_REQUIRE_RABBITMQ"
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

// requireBroker skips the calling test when the local fixture is unreachable,
// or fails it when requireBrokerEnv is set. Under -short it decides before the
// probe, so the short suite excludes broker-backed tests without opening a
// connection. The probe runs once per package.
func requireBroker(t *testing.T) {
	t.Helper()
	if testing.Short() {
		if os.Getenv(requireBrokerEnv) != "" {
			t.Fatalf("-short and %s contradict each other: the short suite excludes broker-backed tests", requireBrokerEnv)
		}
		t.Skip("short suite excludes broker-backed tests; run `make test-rabbitmq` to require the RabbitMQ fixture")
	}
	brokerProbe.once.Do(func() {
		conn, err := net.DialTimeout("tcp", brokerAddress(defaultEndpoint), 2*time.Second)
		if err != nil {
			brokerProbe.reason = err.Error()
			return
		}
		_ = conn.Close()
	})
	if brokerProbe.reason == "" {
		return
	}
	if os.Getenv(requireBrokerEnv) != "" {
		t.Fatalf("%s is set and the RabbitMQ fixture is unreachable: %s", requireBrokerEnv, brokerProbe.reason)
	}
	t.Skipf("RabbitMQ fixture unreachable (%s); run `make test-rabbitmq` to require it", brokerProbe.reason)
}
