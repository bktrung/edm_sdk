package driver

// Capabilities describes what a driver can do natively.
//
// Capabilities describe optimisations, never semantics. The core must produce
// identical observable results when each capability is forced false.
type Capabilities struct {
	PerMessageAck        bool         // native per-message settlement
	OrderedByKey         bool         // ordering guaranteed for equal keys
	NativePriority       PriorityMode // None | Relative | Strict
	NativePriorityLevels int          // native priority depth
	NativeDelay          bool         // broker-level delayed delivery support
	NativeDeliveryCount  bool         // broker reports redelivery count
	NativeDLQ            bool         // broker routes to a DLQ on limit
	ConsumerScaling      Scaling      // PartitionBound | Free
	MaxMessageBytes      int          // physical broker limit
	MaxHeaderBytes       int          // physical broker limit
	LagQueryable         bool         // broker exposes backlog
}

// PriorityMode describes broker-side priority support.
type PriorityMode int

const (
	// PriorityNone means the broker has no priority concept.
	PriorityNone PriorityMode = iota
	// PriorityRelative means the broker provides relative priority.
	PriorityRelative
	// PriorityStrict means the broker provides strict priority ordering.
	PriorityStrict
)

// String returns the wire name of the priority mode.
func (p PriorityMode) String() string {
	switch p {
	case PriorityRelative:
		return "relative"
	case PriorityStrict:
		return "strict"
	default:
		return "none"
	}
}

// Scaling describes how consumer count relates to broker parallelism.
type Scaling int

const (
	// ScalingPartitionBound limits consumers by broker partitions.
	ScalingPartitionBound Scaling = iota
	// ScalingFree allows consumers to scale independently of partitions.
	ScalingFree
)

// String returns the wire name of the scaling mode.
func (s Scaling) String() string {
	if s == ScalingFree {
		return "free"
	}
	return "partition-bound"
}

// Strict returns c with native capabilities disabled and physical constraints
// kept.
//
// The strict profile withdraws capabilities: things one broker offers that
// another may not, where the core can fall back to a portable path. It does not
// withdraw constraints, which are facts about the broker that hold whichever
// profile the core selects. MaxMessageBytes and MaxHeaderBytes are constraints,
// and so is ConsumerScaling: a broker whose consumers are bounded by partition
// count stays bounded, and no core may declare otherwise.
//
// ConsumerScaling is preserved rather than zeroed for a second reason. Its zero
// value is ScalingPartitionBound, so zeroing it would not withdraw the
// declaration but replace ScalingFree with a different parallelism model -
// handing a driver that has no partitions a partition-bound declaration with no
// partition count, which read literally says no consumer may ever receive.
func (c Capabilities) Strict() Capabilities {
	return Capabilities{
		ConsumerScaling: c.ConsumerScaling,
		MaxMessageBytes: c.MaxMessageBytes,
		MaxHeaderBytes:  c.MaxHeaderBytes,
	}
}
