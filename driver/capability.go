package driver

import "time"

// DelayAccuracy bounds how late a deferred delivery can be.
//
// Lateness is at most max(Floor, Relative * requested delay), for a requested
// delay of at most MaxDelay. Above MaxDelay the driver declares no bound.
//
// The bound is relative because a driver that bucketed delays into fixed steps
// cannot state one worst case that is both true and useful: a bound wide enough
// for the longest delay the driver accepts would tell an application deferring
// one second that the delay path is unusable for that case, which is not what
// it is.
//
// The bound covers the driver's own release of a deferred message. Time a
// consumer spends not reading is backpressure, and no driver declares a bound
// on it.
//
// The zero value declares no accuracy, and is rendered as undeclared rather
// than as a zero duration, because zero reads as a promise to deliver at the
// due time exactly. A driver declaring a bound must set MaxDelay: it is the
// largest requested delay the bound holds for, and the largest duration the
// port can carry when no delay a caller can request is above the bound.
//
// DelayAccuracy is a comparable value type, so Capabilities stays comparable
// with ==.
type DelayAccuracy struct {
	Floor    time.Duration
	Relative float64
	MaxDelay time.Duration
}

// Capabilities describes what a driver can do natively.
//
// Capabilities describe optimisations, never semantics. The core must produce
// identical observable results when each capability is forced false.
type Capabilities struct {
	PerMessageAck        bool          // native per-message settlement
	OrderedByKey         bool          // ordering guaranteed for equal keys
	Fanout               FanoutMode    // when one message becomes one copy per subscription
	NativePriority       PriorityMode  // None | Relative | Strict
	NativePriorityLevels int           // native priority depth
	NativeDelay          bool          // broker-level delayed delivery support
	DelayAccuracy        DelayAccuracy // zero value declares no bound on lateness
	NativeDeliveryCount  bool          // broker reports redelivery count
	NativeDLQ            bool          // broker routes to a DLQ on limit
	ConsumerScaling      Scaling       // PartitionBound | Free
	MaxMessageBytes      int           // physical broker limit
	MaxHeaderBytes       int           // physical broker limit
	LagQueryable         bool          // broker exposes backlog
}

// FanoutMode describes when a published message becomes one copy per subscription.
type FanoutMode int

const (
	// FanoutAtConsume means subscriptions share a destination and consume independently.
	FanoutAtConsume FanoutMode = iota
	// FanoutAtPublish means the broker copies messages to per-subscription destinations.
	FanoutAtPublish
)

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
// count stays bounded, and no core may declare otherwise. OrderedByKey is
// preserved because ordering is a semantic the core must keep in either
// profile; strict selects the portable implementation, not an unordered one.
//
// ConsumerScaling is preserved rather than zeroed for a second reason. Its zero
// value is ScalingPartitionBound, so zeroing it would not withdraw the
// declaration but replace ScalingFree with a different parallelism model -
// handing a driver that has no partitions a partition-bound declaration with no
// partition count, which read literally says no consumer may ever receive.
func (c Capabilities) Strict() Capabilities {
	// DelayAccuracy is withdrawn with the capability it describes: the number
	// is a property of the driver's own delay path, and this profile declares
	// nothing rather than a bound it does not stand behind. The accuracy of
	// the core's emulated path is a separate question, and is not invented
	// here.
	return Capabilities{
		Fanout:          c.Fanout,
		ConsumerScaling: c.ConsumerScaling,
		OrderedByKey:    c.OrderedByKey,
		MaxMessageBytes: c.MaxMessageBytes,
		MaxHeaderBytes:  c.MaxHeaderBytes,
	}
}
