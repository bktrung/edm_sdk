package driver

import "time"

// ExchangeSpec describes a routing point.
type ExchangeSpec struct {
	Name    string // Name is the broker routing-point name.
	Kind    string // Kind is the broker routing-point kind, such as "direct", "topic", or "fanout".
	Durable bool   // Durable reports whether the routing point survives a broker restart.
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
	Name          string        // Name is the resolved broker destination name.
	Kind          DestKind      // Kind identifies the destination's role.
	Durable       bool          // Durable reports whether the destination survives a broker restart.
	Partitions    int           // Partitions sets the partition count on brokers that support partitions; other drivers ignore it.
	Delay         time.Duration // Delay is the destination's declared delivery delay; zero means no destination delay.
	FixedDelay    bool          // FixedDelay means each message is due exactly Delay after publish.
	DeadLetter    *Route        // DeadLetter is the explicit broker-side backstop route, if any.
	DeliveryLimit int           // DeliveryLimit is the broker backstop count; values at or below zero mean no limit.
}

// Route identifies a broker-side dead-letter route.
type Route struct {
	Exchange string // Exchange is the routing point, or empty when the broker has no routing layer.
	Key      string // Key is the broker routing key; drivers without a routing layer ignore it.
}

// BindingSpec joins a publish entry point to a destination that receives a copy.
type BindingSpec struct {
	Source      string // Source names an exchange listed in TopologySpec.Exchanges.
	Destination string // Destination names a destination listed in TopologySpec.Destinations.
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
	// Exchanges lists the routing points required by the topology.
	Exchanges []ExchangeSpec
	// Destinations lists the destinations required by the topology.
	Destinations []DestinationSpec
	// Bindings lists the routes from publish entry points to receiving destinations.
	Bindings []BindingSpec
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
	// CreatedExchanges lists routing points created by this call.
	CreatedExchanges []string
	// CreatedDestinations lists destinations created by this call.
	CreatedDestinations []string
	// CreatedBindings lists bindings created by this call.
	CreatedBindings []BindingSpec
	// ExistingExchanges lists requested routing points that already existed.
	ExistingExchanges []string
	// ExistingDestinations lists requested destinations that already existed.
	ExistingDestinations []string
	// ExistingBindings lists requested bindings that already existed.
	ExistingBindings []BindingSpec
	// Orphaned lists core-owned destinations that exist but are absent from the
	// spec. Include auxiliary message counts in their parent; report an
	// auxiliary by name when its parent is absent.
	Orphaned []OrphanedDestination
	// OrphanScanError explains why orphan scanning did not run. When non-empty,
	// Orphaned is incomplete.
	OrphanScanError string
	// Drifted lists destinations whose broker-side arguments do not match the
	// spec under TopologyVerify. If a driver cannot inspect the relevant values,
	// it must return an error rather than report a clean diff.
	Drifted []ArgumentDrift
}

// ArgumentDrift reports one destination argument whose broker-side value no
// longer matches what the spec asks for. The comparison is one-directional:
// only arguments the spec sets are checked, so broker-added arguments the
// spec never mentioned are never reported as drift.
type ArgumentDrift struct {
	// Name is the destination whose argument differs.
	Name string
	// Argument is the differing broker argument key, such as "x-delivery-limit".
	Argument string
	// Want is the spec value formatted for display.
	Want string
	// Got is the broker value formatted for display, or "<absent>" when the key is missing.
	Got string
}

// OrphanedDestination is a broker destination absent from the current spec.
type OrphanedDestination struct {
	// Name is the broker destination absent from the current spec.
	Name string
	// Messages includes anything held in driver-internal auxiliaries.
	Messages int64
}

// PruneResult reports whether one destination was deleted.
type PruneResult struct {
	// Name is the destination requested for pruning.
	Name string
	// Deleted reports whether the destination was deleted.
	Deleted bool
	// Reason explains why a destination was not deleted.
	Reason string
}
