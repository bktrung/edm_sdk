package rabbitmq

import (
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

// requireBrokerEnv names the variable that turns an unreachable fixture from a
// skip into a failure. The repository-wide test command must stay runnable on a
// machine with no broker, while the driver's own target must never report
// success for a suite that did not run.
const requireBrokerEnv = "F1_REQUIRE_RABBITMQ"

var brokerProbe struct {
	once   sync.Once
	reason string
}

// requireBroker skips the calling test when the local fixture is unreachable,
// or fails it when requireBrokerEnv is set. The probe runs once per package.
func requireBroker(t *testing.T) {
	t.Helper()
	brokerProbe.once.Do(func() {
		conn, err := net.DialTimeout("tcp", "localhost:5672", 2*time.Second)
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
