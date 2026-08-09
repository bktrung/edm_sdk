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
	Transactions         bool         // broker transaction support
	ConsumerScaling      Scaling      // PartitionBound | Free
	ServerSideFilter     bool         // broker-side message filtering
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

// Strict returns c with native capabilities disabled and physical limits kept.
func (c Capabilities) Strict() Capabilities {
	return Capabilities{
		MaxMessageBytes: c.MaxMessageBytes,
		MaxHeaderBytes:  c.MaxHeaderBytes,
	}
}
