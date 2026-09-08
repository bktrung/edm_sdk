package driver

import (
	"context"
	"time"
)

// Driver is the entry point of a broker adapter.
//
// Implementations must be stateless value types; all state lives in the Conn
// returned by Open. Implementations must be safe for concurrent use.
type Driver interface {
	// Name returns the stable registry key: "kafka", "rabbitmq", or "inmem".
	Name() string

	// Capabilities describes what the driver can do natively. It must not
	// depend on a live connection. When a capability depends on broker
	// version, report the pessimistic value here and refine it through Conn.
	Capabilities() Capabilities

	// Open establishes a usable connection. It must not return until the
	// connection is usable or ctx expires. Transient failures during Open are
	// retried internally up to cfg.ConnectTimeout.
	Open(ctx context.Context, cfg Config) (Conn, error)
}

// Conn is a live broker connection and the factory for driver resources.
// Implementations must be safe for concurrent use.
type Conn interface {
	// Capabilities returns the connected broker's capabilities. It may reduce,
	// but must not increase, any capability reported by Driver.Capabilities.
	Capabilities() Capabilities

	// BrokerInfo returns free-form broker metadata for logs and diagnostics.
	BrokerInfo() BrokerInfo

	// Producer creates a producer using cfg.
	Producer(ctx context.Context, cfg ProducerConfig) (Producer, error)

	// Consumer creates a consumer using cfg.
	Consumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error)

	// Admin returns the topology administration surface.
	Admin() Admin

	// Ping performs a lightweight liveness check for readiness probes.
	Ping(ctx context.Context) error

	// Close releases resources. It must not be called before all Producers and
	// Consumers created from it have been closed. Behaviour otherwise is
	// undefined; drivers should return ErrResourcesOutstanding.
	Close(ctx context.Context) error
}

// BrokerInfo contains free-form broker metadata for logs and diagnostics.
type BrokerInfo struct {
	Kind    string            // broker type, such as "kafka" or "rabbitmq"
	Version string            // best-effort broker version
	Nodes   []string          // broker nodes, when known
	Extra   map[string]string // driver-specific metadata
}

// Display renders "kind version" for logs.
//
// Core code must use Display rather than reading Version directly.
func (b BrokerInfo) Display() string {
	switch {
	case b.Kind == "":
		return b.Version
	case b.Version == "":
		return b.Kind
	default:
		return b.Kind + " " + b.Version
	}
}

// Producer publishes messages and waits for durable broker acknowledgement.
// Implementations must be safe for concurrent use.
type Producer interface {
	// Publish sends messages and must not return nil until the broker has
	// durably acknowledged every message. Partial failure returns a
	// PublishError carrying per-message results, so the core retries only the
	// failed subset. Publish must be safe for concurrent use.
	Publish(ctx context.Context, msgs ...OutboundMessage) error

	// Flush waits until all buffered messages are acknowledged.
	Flush(ctx context.Context) error

	// Close releases producer resources.
	Close(ctx context.Context) error
}

// Consumer delivers messages from a set of destinations.
// Implementations must be safe for concurrent use.
type Consumer interface {
	// Messages yields delivered messages. The channel is closed only after Stop
	// completes. Drivers must not close it on transient errors.
	Messages() <-chan InboundMessage

	// Errors yields asynchronous driver errors: connection loss, rebalance
	// notifications, and transport decode failures. The core logs and counts
	// these; fatal ones stop only the subscription that received the error.
	Errors() <-chan error

	// Pause stops delivery without leaving the consumer group. Accumulation must
	// remain within the destination's prefetch share; callers own outage policy.
	// An empty destinations list applies to every destination held by the consumer.
	Pause(destinations ...string) error

	// Resume restarts delivery after Pause. An empty destinations list applies to
	// every destination held by the consumer.
	Resume(destinations ...string) error

	// Drain stops fetching new messages while keeping outstanding messages
	// settleable. Messages yields nothing new after Drain returns.
	Drain(ctx context.Context) error

	// Stop performs final settlement and then closes Messages. It must not be
	// called before every delivered message has been settled. Stop must flush
	// committed positions before returning; if ctx expires first, it returns
	// ErrDrainTimeout.
	Stop(ctx context.Context) error

	// Release abandons every outstanding delivery WITHOUT settling it and then
	// closes the consumer. The broker redelivers, to the same consumer group,
	// whatever this consumer was given and did not settle. Release is the port
	// verb for "give these back"; Stop is the verb for "I am finished with
	// these", and the two must not be conflated.
	//
	// Release makes NO durability promise about committed positions. That is the
	// difference from Stop, and it is why it is a separate verb rather than a
	// flag: a driver that flushed positions here would commit past a message it
	// is deliberately handing back.
	//
	// On a consumer with nothing outstanding Release behaves as Stop does.
	// Release is idempotent, and on an already-released or already-stopped
	// consumer it returns nil.
	//
	// A driver that genuinely cannot return unsettled deliveries returns
	// ErrUnsupported.
	Release(ctx context.Context) error

	// Lag reports per-destination backlog. Drivers that cannot determine lag
	// return ErrUnsupported.
	Lag(ctx context.Context) (map[string]int64, error)
}

// Admin provisions and inspects broker destinations.
// Implementations must be safe for concurrent use.
type Admin interface {
	// EnsureTopology creates missing destinations and reports the diff. It must
	// be idempotent and must not delete. It must also report in-scope
	// destinations that exist on the broker but are absent from the spec in
	// TopologyDiff.Orphaned.
	EnsureTopology(ctx context.Context, spec TopologySpec) (TopologyDiff, error)

	// DescribeTopology reads current topology state for topology diff output.
	DescribeTopology(ctx context.Context, names []string) (TopologyState, error)
}

// Maintenance provides optional destructive destination operations.
// Implementations must be safe for concurrent use.
type Maintenance interface {
	// Purge empties a destination and keeps it.
	Purge(ctx context.Context, destination string) (int64, error)

	// Prune is the only deletion operation in the port and reports the result
	// for each requested name.
	//
	// A destination is prunable only when it is empty, its driver-managed
	// auxiliary destinations are empty, and no consumer is attached. Drivers
	// must recheck these guards before each delete and delete auxiliaries first.
	Prune(ctx context.Context, names []string) ([]PruneResult, error)
}

// Config contains connection settings shared by all drivers.
type Config struct {
	Endpoints             []string      // broker endpoints
	ClientID              string        // stable client identity
	InstanceID            string        // stable group instance identity
	RebalanceDrainTimeout time.Duration // bound for in-flight settlement during rebalance drain
	ConnectTimeout        time.Duration // timeout for Open retries
	TLS                   *TLSConfig    // nil disables TLS
	SASL                  *SASLConfig   // nil disables SASL
	// DriverOptions carries driver-specific knobs untouched by the core.
	DriverOptions map[string]string
}

// TLSConfig contains the portable subset of TLS settings exposed by the port.
type TLSConfig struct {
	Enabled            bool   // whether TLS is enabled
	CAFile             string // trusted CA file
	CertFile           string // client certificate file
	KeyFile            string // client key file
	InsecureSkipVerify bool   // never true outside a test fixture
}

// SASLConfig selects a SASL mechanism and carries credentials.
// Empty Mechanism means no SASL. Credentials must not be logged.
type SASLConfig struct {
	Mechanism string // SASL mechanism, such as "plain" or "scram-sha-256"
	Username  string
	Password  string
}

// ProducerConfig configures a Producer.
type ProducerConfig struct {
	// RequireDurableAck must be honored. The core always sets it true in v1.
	RequireDurableAck bool

	// Effective is the capability set selected by the core.
	Effective Capabilities
}

// ConsumerConfig configures a Consumer over one subscription's destinations.
type ConsumerConfig struct {
	Group        string   // consumer group or queue-set identity
	Destinations []string // main and retry lanes for this subscription

	// Prefetch is the subscription-wide in-flight budget. The core divides it
	// across destinations with a minimum of one per lane and passes each lane's
	// result in PerDestination. Prefetch below the lane count is rejected by
	// the core rather than silently changing the budget.
	Prefetch       int
	PerDestination map[string]int

	Exclusive bool          // request single-active-consumer semantics
	StartAt   StartPosition // Earliest or Latest for a new group only

	// Effective is the capability set selected by the core.
	Effective Capabilities
}

// StartPosition controls where a new consumer group begins. It never
// affects an existing group.
type StartPosition int

const (
	// StartEarliest starts a new group at the earliest available message.
	StartEarliest StartPosition = iota
	// StartLatest starts a new group at messages published after group creation.
	StartLatest
)
