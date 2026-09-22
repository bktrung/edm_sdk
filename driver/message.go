package driver

import (
	"context"
	"time"
)

// OutboundMessage is a message the driver sends to a broker.
type OutboundMessage struct {
	// Destination is the broker destination already selected by the core.
	// Drivers must use it as provided and must not build or parse its name.
	Destination string
	// EntryPoint is true when Destination is a publish entry point that the
	// broker fans out to subscriber destinations. False means Destination is
	// one concrete destination. The core sets this from the selected
	// capability; drivers must not infer it from the name or their own cache.
	EntryPoint bool
	Key        []byte
	Headers    []Header
	Body       []byte

	// Priority is an optional broker-priority hint. The core does not depend on
	// broker priority for correctness.
	Priority uint8

	// DelayUntil is the time when a deferred message becomes eligible for
	// delivery. Zero means deliver immediately.
	//
	// A driver may instead deliver a deferred message sent to a destination that
	// declares a delay at the message's publish instant plus that delay, whatever
	// this field says, and never earlier than that instant. Such a driver delivers
	// the messages it deferred within one partition of a destination in the order it
	// published them, which for a destination with one partition is the destination's
	// own order. The core writes this field as the instant it builds the retry copy
	// plus the retry tier's delay, unless the application asked for a custom retry
	// delay.
	DelayUntil time.Time
}

// InboundMessage is a message received from a broker.
type InboundMessage struct {
	Destination string
	Key         []byte
	Headers     []Header
	Body        []byte

	// DeliveryCount is the number of previous broker redeliveries, or -1 when
	// the broker does not provide this information.
	DeliveryCount int
	ReceivedAt    time.Time // time received by the driver
	// EnqueuedAt is the time the broker or producer enqueued the event. Its zero
	// value means the enqueue time is unknown.
	EnqueuedAt time.Time
	// EnqueuedAtSource identifies the source of EnqueuedAt. Its zero value means
	// the source is unknown. source=producer is the event's creation time; on a retry or DLQ copy it includes all earlier attempts.
	EnqueuedAtSource EnqueueSource
	Ref              BrokerRef // broker reference for this delivery
	Settle           Settler   // finishes this delivery exactly once
}

// Header is one key/value metadata item sent with a broker message.
type Header struct {
	Key   string
	Value []byte
}

// BrokerRef stores the broker-specific identity of a delivery. The core does
// not interpret its fields; each driver uses the identity format supported by
// its broker: partition and offset, delivery tag, or raw string. The zero value
// means that the driver did not provide a broker reference; a valid delivery
// must populate at least one field even when its numeric identity is zero.
type BrokerRef struct {
	Partition int32
	Offset    int64
	Tag       uint64
	Raw       string
}

// Settler provides operations to finish one broker delivery. A delivery must
// not be settled more than once, and implementations must support concurrent
// use.
type Settler interface {
	// Ack marks the message as successfully handled.
	Ack(ctx context.Context) error

	// Nack asks the broker to redeliver the message or to discard/dead-letter it
	// according to broker policy.
	Nack(ctx context.Context, opt NackOptions) error
}

// NackOptions controls what the broker should do after Nack.
type NackOptions struct {
	// Requeue asks the broker to deliver the message again. False means let the
	// broker discard or dead-letter it according to policy.
	Requeue bool
	// CountAsFailure asks the driver to count this as a failed delivery. This is
	// best effort because some brokers count every rejected delivery.
	CountAsFailure bool
}
