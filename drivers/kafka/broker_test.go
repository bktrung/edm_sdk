package kafka

import (
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	kafkaEndpoint    = "localhost:19092"
	requireBrokerEnv = "F1_REQUIRE_KAFKA"
)

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
