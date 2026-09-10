package kafka

import (
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	defaultKafkaEndpoint = "localhost:19092"
	kafkaEndpointEnv     = "F1_KAFKA_ENDPOINT"
	requireBrokerEnv     = "F1_REQUIRE_KAFKA"
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

// requireBroker skips the calling test when the local fixture is unreachable,
// or fails it when requireBrokerEnv is set. The probe runs once per package.
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
	if brokerProbe.reason == "" {
		return
	}
	if os.Getenv(requireBrokerEnv) != "" {
		t.Fatalf("%s is set and the Kafka fixture is unreachable: %s", requireBrokerEnv, brokerProbe.reason)
	}
	t.Skipf("Kafka fixture unreachable (%s); run `make test-kafka` to require it", brokerProbe.reason)
}
