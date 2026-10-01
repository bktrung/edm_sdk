package driver

import (
	"context"
	"log/slog"
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
	Kind    string            // Kind is the broker type, such as "kafka" or "rabbitmq".
	Version string            // Version is the best-effort broker version.
	Nodes   []string          // Nodes lists broker nodes when known.
	Extra   map[string]string // Extra contains driver-specific metadata.
}

// Display returns the broker kind and version as "kind version", omitting
// either part when it is empty.
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
	// durably acknowledged every message. On partial failure it returns a
	// [PublishError] with per-message results. Publish must be safe for
	// concurrent use.
	Publish(ctx context.Context, msgs ...OutboundMessage) error

	// Close releases producer resources.
	Close(ctx context.Context) error
}

// Consumer delivers messages from a set of destinations.
// Implementations must be safe for concurrent use.
type Consumer interface {
	// Messages yields delivered messages. The channel is closed only after Stop or
	// Release completes. Drivers must not close it on transient errors.
	Messages() <-chan InboundMessage

	// Errors returns asynchronous errors reported for this consumer, such as
	// connection loss, rebalance notifications, or transport decode failures.
	// The core logs and counts these; fatal errors stop only this subscription.
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

	// Stop performs final settlement and then closes Messages. It refuses while
	// any delivered message is still unsettled. Stop must flush
	// committed positions before returning; if ctx expires first, it returns
	// ErrDrainTimeout. A Stop refused because messages are still unsettled
	// wraps ErrResourcesOutstanding. After any Stop error the core calls
	// Release, which must then give back the unsettled messages. Stop after
	// Release returns nil.
	Stop(ctx context.Context) error

	// Release abandons outstanding deliveries without settling them and then
	// closes the consumer. The broker redelivers un-settled deliveries to the
	// same consumer group. Release makes no durability promise about committed
	// positions.
	//
	// Release on a consumer with no outstanding deliveries behaves like Stop.
	// Release is idempotent and returns nil when the consumer is already stopped
	// or released. A driver that cannot return unsettled deliveries returns
	// [ErrUnsupported].
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
	// Purge empties a destination, keeps it, and returns the number of messages
	// removed.
	Purge(ctx context.Context, destination string) (int64, error)

	// Prune is the only deletion operation in the port and reports the result
	// for each requested name.
	//
	// A nil error means the batch was processed, not that every requested
	// destination was deleted. A refused destination is reported in its own
	// PruneResult with Deleted false and a non-empty Reason. Callers must
	// inspect every result.
	//
	// A destination is prunable only when it is empty, its driver-managed
	// auxiliary destinations are empty, and no consumer is attached. Drivers
	// must recheck these guards before each delete and delete auxiliaries first.
	// The recheck and the delete are not atomic on every broker: a message
	// published or a consumer attached between them can be deleted with the
	// destination, so Prune only destinations traffic has already left.
	Prune(ctx context.Context, names []string) ([]PruneResult, error)
}

// Config contains connection settings shared by all drivers.
type Config struct {
	Endpoints             []string      // Endpoints lists the broker endpoints used when opening a connection.
	ClientID              string        // ClientID is the stable client identity presented to the broker.
	InstanceID            string        // InstanceID is the stable identity of this consumer group instance.
	RebalanceDrainTimeout time.Duration // RebalanceDrainTimeout bounds in-flight settlement during a rebalance drain.
	ConnectTimeout        time.Duration // ConnectTimeout bounds retries performed by Open.
	TLS                   *TLSConfig    // TLS configures transport security; nil disables TLS.
	SASL                  *SASLConfig   // SASL configures authentication; nil disables SASL.
	// Logger receives driver diagnostics that do not fail the operation that
	// encountered them. A nil Logger uses [slog.Default].
	Logger *slog.Logger
	// DriverOptions contains driver-specific settings that the core does not
	// interpret.
	DriverOptions map[string]string
}

// TLSConfig contains the portable subset of TLS settings exposed by the port.
type TLSConfig struct {
	Enabled            bool   // Enabled reports whether TLS is enabled.
	CAFile             string // CAFile names the trusted CA certificate file.
	CertFile           string // CertFile names the client certificate file.
	KeyFile            string // KeyFile names the client private key file.
	ServerName         string // ServerName overrides the host name used to verify the broker certificate; empty derives it from the endpoint.
	InsecureSkipVerify bool   // InsecureSkipVerify disables broker certificate verification and is intended only for test fixtures.
}

// SASLConfig selects a SASL mechanism and carries credentials.
// Empty Mechanism means no SASL. Credentials must not be logged.
type SASLConfig struct {
	Mechanism string // Mechanism selects the SASL mechanism, such as "plain" or "scram-sha-256".
	Username  string // Username is the credential name supplied to the SASL mechanism.
	Password  string // Password is the credential secret supplied to the SASL mechanism.
}

// ProducerConfig configures a Producer.
type ProducerConfig struct {
	// Effective is the capability set selected by the core.
	Effective Capabilities
}

// ConsumerConfig configures a Consumer over one subscription's destinations.
type ConsumerConfig struct {
	Group        string   // Group is the consumer group or queue-set identity.
	Destinations []string // Destinations lists this subscription's main and retry destinations.

	// Prefetch bounds total SDK-admitted unsettled deliveries across all
	// destinations when positive. The core always supplies a positive budget:
	// automatic sizing uses the sum of lane capacities, and explicit budgets
	// are capped by that sum. Broker/client transport read-ahead is separate.
	// In ordered mode, the effective prefetch is also the worker queue depth.
	Prefetch int
	// PerDestination gives an additional admission ceiling for each physical
	// destination. Its entries may sum above Prefetch; both bounds apply.
	PerDestination map[string]int

	// Delays maps physical destination names to their declared delays. A
	// destination absent from the map has no delay. The core derives these
	// values from the same topology it passes to EnsureTopology, not from state
	// left on the connection by an earlier call. Drivers that defer delivery on
	// the consumer side use these values; drivers that defer on publish may
	// ignore them.
	Delays map[string]time.Duration

	Exclusive bool          // Exclusive requests single-active-consumer semantics.
	StartAt   StartPosition // StartAt selects the start position for a new group.

	// Effective is the capability set selected by the core. The zero value is
	// the unset sentinel: the driver resolves a capability it also learns from
	// the connection there instead of from this field. The Kafka consumer
	// resolves every capability that way and the RabbitMQ consumer resolves its
	// delivery count that way; a capability a driver reads only from this field
	// is taken as it stands, so a zero value asks for the driver's unset
	// behaviour rather than for what the connection can do. The core sets it
	// on every call, but a strict-portability profile can itself be all zero,
	// and a driver cannot tell that apart from unset.
	Effective Capabilities
}

// DestinationPrefetch returns the in-flight capacity of Destinations[index]:
// its PerDestination entry when that is positive, otherwise an even share of
// Prefetch with the remainder going to the first destinations. index must be
// in range. It is never less than 1, so when Prefetch is smaller than the
// number of destinations the windows add up to more than Prefetch. These
// destination ceilings do not replace the positive aggregate Prefetch cap.
func (c ConsumerConfig) DestinationPrefetch(index int) int {
	if index >= 0 && index < len(c.Destinations) {
		if value := c.PerDestination[c.Destinations[index]]; value > 0 {
			return value
		}
	}
	if c.Prefetch > 0 && len(c.Destinations) > 0 {
		base := c.Prefetch / len(c.Destinations)
		if index < c.Prefetch%len(c.Destinations) {
			base++
		}
		if base > 0 {
			return base
		}
	}
	return 1
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
