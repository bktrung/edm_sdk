package driver

import (
	"context"
	"time"
)

// EnqueueSource identifies the system that supplied an enqueue timestamp. Its
// zero value, EnqueueSourceUnknown, means the source is unknown.
type EnqueueSource int

const (
	// EnqueueSourceUnknown means the enqueue timestamp is unknown.
	EnqueueSourceUnknown EnqueueSource = iota
	// EnqueueSourceProducer means the timestamp was supplied by the producer.
	EnqueueSourceProducer
	// EnqueueSourceBroker means the timestamp was supplied by the broker.
	EnqueueSourceBroker
)

// String returns the wire name of the enqueue timestamp source. Unknown and
// out-of-range values return "unknown".
func (s EnqueueSource) String() string {
	switch s {
	case EnqueueSourceProducer:
		return "producer"
	case EnqueueSourceBroker:
		return "broker"
	default:
		return "unknown"
	}
}

// BacklogSample reports a destination's backlog and oldest enqueue timestamp.
// Its zero value reports no backlog and an unknown head timestamp and source.
type BacklogSample struct {
	// Lag is the number of messages counted in the backlog.
	Lag int64
	// HeadEnqueuedAt is the oldest counted message's enqueue time. Its zero
	// value means the destination is empty or the broker cannot report the time.
	HeadEnqueuedAt time.Time
	// HeadSource identifies the source of HeadEnqueuedAt. Its zero value is
	// EnqueueSourceUnknown and must be used when HeadEnqueuedAt is zero.
	HeadSource EnqueueSource
}

// BacklogReader is an optional Consumer extension that reports lag and the
// head enqueue time per destination. A nil BacklogReader provides no sample.
type BacklogReader interface {
	Backlog(ctx context.Context) (map[string]BacklogSample, error)
}
