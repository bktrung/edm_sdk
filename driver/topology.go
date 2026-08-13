package driver

import "time"

// ExchangeSpec describes a routing point.
type ExchangeSpec struct {
	Name    string // broker routing-point name
	Kind    string // "direct" | "topic" | "fanout"
	Durable bool   // whether the routing point survives broker restart
}

// DestKind classifies a core-owned destination.
type DestKind int

const (
	// DestMain is the ordinary delivery destination.
	DestMain DestKind = iota
	// DestRetry is a deferred retry destination.
	DestRetry
	// DestDLQ is a dead-letter destination.
	DestDLQ
	// DestBackstopDLQ is the broker backstop dead-letter destination.
	DestBackstopDLQ
)

// DestinationSpec describes one destination the core needs.
type DestinationSpec struct {
	Name          string        // resolved by the core's naming function
	Kind          DestKind      // Main | Retry | DLQ | BackstopDLQ
	Durable       bool          // whether the destination survives restart
	Partitions    int           // Kafka; ignored elsewhere
	Delay         time.Duration // non-zero marks a deferred destination
	DeadLetter    *Route        // explicit broker-side backstop route
	DeliveryLimit int           // broker backstop counter; <= 0 means none
}

// Route identifies a broker-side dead-letter route.
type Route struct {
	Exchange string // empty on brokers without a routing layer
	Key      string // broker routing key; ignored when no routing layer exists
}

// BindingSpec joins a publish entry point to a destination that receives a copy.
type BindingSpec struct {
	Source      string // exchange name, from TopologySpec.Exchanges
	Destination string // destination name, from TopologySpec.Destinations
}

// TopologyPolicy selects how a driver handles the topology in a spec.
type TopologyPolicy int

const (
	// TopologyDeclare creates missing topology and is the default.
	TopologyDeclare TopologyPolicy = iota
	// TopologyVerify checks that topology exists and creates nothing.
	TopologyVerify
	// TopologyNone assumes topology exists and makes no broker round trip.
	TopologyNone
)

// TopologySpec is the declarative description of broker topology.
type TopologySpec struct {
	Exchanges    []ExchangeSpec
	Destinations []DestinationSpec
	Bindings     []BindingSpec
	// Policy selects whether the driver creates, verifies, or skips topology.
	Policy TopologyPolicy
	// Scope lists prefixes the driver may scan for core-owned orphans. An empty
	// Scope disables scanning and sets OrphanScanError.
	Scope []string
	// Effective is the capability set selected by the core. Drivers must
	// branch on Effective, never on their own connection capability view.
	Effective Capabilities
}

// TopologyState reports the current state of requested destinations.
type TopologyState struct {
	// Depth maps requested destination names to current message counts,
	// including driver-internal auxiliaries. A missing destination is absent
	// from the map rather than present with zero, so topology diff can
	// distinguish missing from empty.
	Depth map[string]int64
}

// TopologyDiff reports topology changes and orphaned destinations.
type TopologyDiff struct {
	CreatedExchanges    []string
	CreatedDestinations []string
	CreatedBindings     []string
	Existing            []string
	// Orphaned lists core-owned destinations that exist but are absent from the
	// spec. Include auxiliary message counts in their parent; report an
	// auxiliary by name when its parent is absent.
	Orphaned []OrphanedDestination
	// OrphanScanError explains why orphan scanning did not run. When non-empty,
	// Orphaned is incomplete.
	OrphanScanError string
}

// OrphanedDestination is a broker destination absent from the current spec.
type OrphanedDestination struct {
	Name string
	// Messages includes anything held in driver-internal auxiliaries.
	Messages int64
}

// PruneResult reports whether one destination was deleted.
type PruneResult struct {
	Name    string
	Deleted bool
	// Reason explains why a destination was not deleted.
	Reason string
}
