package driver

import "time"

// DelayAccuracy describes how late a driver's deferred-message release may be.
//
// For a requested delay no greater than [MaxDelay], lateness is bounded by
// max([Floor], [Relative] times the requested delay). A driver declaring a
// bound must set MaxDelay; requests above it have no declared bound. The zero
// value declares no accuracy bound. This bound covers the driver's release of
// a message, not time spent waiting for a consumer to read. DelayAccuracy is
// comparable, so [Capabilities] remains comparable with ==.
type DelayAccuracy struct {
	Floor    time.Duration // Floor is the minimum value used for the lateness upper bound.
	Relative float64       // Relative is the lateness bound as a fraction of the requested delay.
	MaxDelay time.Duration // MaxDelay is the largest requested delay covered by the bound.
}

// Capabilities describes a driver's native features and physical limits.
//
// Capabilities describe optimizations, not semantics. The core must produce
// identical observable results when any capability is disabled.
type Capabilities struct {
	PerMessageAck        bool          // PerMessageAck reports whether individual deliveries can be settled.
	OrderedByKey         bool          // OrderedByKey reports whether messages with the same key retain publish order.
	Fanout               FanoutMode    // Fanout describes when a published message is copied for each subscription.
	NativePriority       PriorityMode  // NativePriority describes broker-side message priority support.
	NativePriorityLevels int           // NativePriorityLevels is the number of broker-side priority levels.
	NativeDelay          bool          // NativeDelay reports whether the broker supports delayed delivery.
	DelayAccuracy        DelayAccuracy // DelayAccuracy bounds lateness of native deferred-message release; zero means no declared bound.
	NativeDeliveryCount  bool          // NativeDeliveryCount reports whether the broker supplies redelivery counts.
	NativeDLQ            bool          // NativeDLQ reports whether the broker routes messages to a dead-letter destination at its delivery limit.
	ConsumerScaling      Scaling       // ConsumerScaling describes how consumer count relates to broker parallelism.
	MaxMessageBytes      int           // MaxMessageBytes is the physical broker message-size limit.
	MaxHeaderBytes       int           // MaxHeaderBytes is the physical broker header-size limit.
	LagQueryable         bool          // LagQueryable reports whether consumers can query broker backlog.
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

// String returns the wire name of the priority mode. Unknown values return
// "none".
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

// String returns the wire name of the scaling mode. Unknown values return
// "partition-bound".
func (s Scaling) String() string {
	if s == ScalingFree {
		return "free"
	}
	return "partition-bound"
}

// Strict returns c with native optimizations disabled while retaining fanout,
// key ordering, consumer scaling, and physical message and header limits.
//
// The returned value preserves Fanout, OrderedByKey, ConsumerScaling,
// MaxMessageBytes, and MaxHeaderBytes from c. Every other field is zero.
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
