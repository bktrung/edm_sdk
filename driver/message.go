package driver

import (
	"context"
	"time"
)

// OutboundMessage is a message ready for broker transport.
type OutboundMessage struct {
	// Destination is physical and already resolved by the core. Drivers must
	// not construct or parse destination names.
	Destination string
	Key         []byte
	Headers     []Header
	Body        []byte

	// Priority is a hint. The core never relies on broker-side priority for
	// correctness.
	Priority uint8

	// DelayUntil is the due time for a deferred destination. Zero means
	// deliver immediately.
	DelayUntil time.Time
}

// InboundMessage is a message delivered from a broker.
type InboundMessage struct {
	Destination string
	Key         []byte
	Headers     []Header
	Body        []byte

	// DeliveryCount is the broker redelivery counter, or -1 when unavailable.
	DeliveryCount int       // -1 when the broker does not track it
	ReceivedAt    time.Time // time received by the driver
	Ref           BrokerRef // opaque broker identity for forensics
	Settle        Settler   // settles this message exactly once
}

// Header is one wire header.
type Header struct {
	Key   string
	Value []byte
}

// BrokerRef identifies a broker message opaquely to the core.
// Each driver uses one identity form: Partition and Offset, Tag, or Raw.
type BrokerRef struct {
	Partition int32
	Offset    int64
	Tag       uint64
	Raw       string
}

// Settler settles one delivered message exactly once.
// Implementations must be safe for concurrent use.
type Settler interface {
	// Ack marks the message as successfully handled.
	Ack(ctx context.Context) error

	// Nack returns the message for broker redelivery or broker-policy
	// discard/dead-lettering.
	Nack(ctx context.Context, opt NackOptions) error
}

// NackOptions controls broker redelivery.
type NackOptions struct {
	// Requeue asks the broker to redeliver. False means discard or
	// dead-letter according to broker policy.
	Requeue bool
	// CountAsFailure is best effort because drivers may count every return as a
	// delivery failure.
	CountAsFailure bool
}
