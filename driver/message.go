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
	// Key is the message routing or partition key.
	Key []byte
	// Headers are metadata sent with the message.
	Headers []Header
	// Body contains the message payload bytes.
	Body []byte

	// Priority is an optional broker-priority hint. The core does not depend on
	// broker priority for correctness.
	Priority uint8

	// DelayUntil is when a deferred message becomes eligible for delivery. Zero
	// means deliver immediately.
	//
	// A driver may instead use the destination's declared delay. In that model,
	// delivery is eligible at publish time plus the destination delay, regardless
	// of DelayUntil, and must not occur earlier. Deferred messages within one
	// partition are delivered in publish order; for a single-partition destination
	// this is destination order.
	//
	// The core sets DelayUntil to the time it builds a retry copy plus that
	// retry tier's delay, unless the application requests a custom retry delay.
	DelayUntil time.Time
}

// InboundMessage is a message received from a broker.
type InboundMessage struct {
	// Destination is the broker destination from which the message was received.
	Destination string
	// Key is the message routing or partition key.
	Key []byte
	// Headers are metadata received with the message.
	Headers []Header
	// Body contains the message payload bytes.
	Body []byte
	// DeliveryCount is the number of previous broker redeliveries, or -1 when
	// the broker does not provide this information.
	DeliveryCount int
	// ReceivedAt is the time the driver received the message.
	ReceivedAt time.Time
	// EnqueuedAt is the time the broker or producer enqueued the event. Its zero
	// value means the enqueue time is unknown.
	EnqueuedAt time.Time
	// EnqueuedAtSource identifies the source of EnqueuedAt. Its zero value means
	// the source is unknown. source=producer is the event's creation time; on a retry or DLQ copy it includes all earlier attempts.
	EnqueuedAtSource EnqueueSource
	// Ref is the broker-specific identity of this delivery.
	Ref BrokerRef
	// Settle provides operations to finish this delivery.
	Settle Settler
}

// Header is one key/value metadata item sent with a broker message.
type Header struct {
	// Key is the metadata item name.
	Key string
	// Value is the metadata item value.
	Value []byte
}

// BrokerRef stores the broker-specific identity of a delivery. The zero value
// means no broker reference was provided. A valid delivery must populate at
// least one field even when its numeric identity is zero.
type BrokerRef struct {
	// Partition is the broker partition, when the broker uses partitions.
	Partition int32
	// Offset is the broker offset, when the broker uses offsets.
	Offset int64
	// Tag is the broker delivery tag, when the broker uses delivery tags.
	Tag uint64
	// Raw is the broker's string identity, when it has no numeric identity.
	Raw string
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
