package f1

import (
	"fmt"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/codec"
)

// Event is a read-only view of one message delivered to a Handler. Getters on a
// nil Event return zero values; Decode returns an error when the event or its
// codec is unavailable.
type Event struct {
	envelope Envelope
	raw      []byte
	codec    codec.Codec
	headers  map[string]string
}

// ID returns the stable event identifier from the envelope.
func (e *Event) ID() string {
	if e == nil {
		return ""
	}
	return e.envelope.ID
}

// Type returns the event type used for handler routing.
func (e *Event) Type() string {
	if e == nil {
		return ""
	}
	return e.envelope.Type
}

// Subject returns the optional business subject.
func (e *Event) Subject() string {
	if e == nil {
		return ""
	}
	return e.envelope.Subject
}

// Source returns the producer source URI.
func (e *Event) Source() string {
	if e == nil {
		return ""
	}
	return e.envelope.Source
}

// Time returns the event creation time.
func (e *Event) Time() time.Time {
	if e == nil {
		return time.Time{}
	}
	return e.envelope.Time
}

// Priority returns the event's scheduling lane.
func (e *Event) Priority() Priority {
	if e == nil {
		return PriorityMedium
	}
	return e.envelope.Priority
}

// Attempt returns the one-based handler attempt.
func (e *Event) Attempt() int {
	if e == nil {
		return 0
	}
	return e.envelope.Attempt
}

// MaxAttempts returns the effective attempt cap carried by the event.
func (e *Event) MaxAttempts() int {
	if e == nil {
		return 0
	}
	return e.envelope.MaxAttempts
}

// CorrelationID returns the workflow correlation identifier.
func (e *Event) CorrelationID() string {
	if e == nil {
		return ""
	}
	return e.envelope.CorrelationID
}

// CausationID returns the identifier of the event that caused this event.
func (e *Event) CausationID() string {
	if e == nil {
		return ""
	}
	return e.envelope.CausationID
}

// Header returns one canonical or extension header.
func (e *Event) Header(key string) (string, bool) {
	if e == nil {
		return "", false
	}
	value, ok := e.headers[key]
	return value, ok
}

// Raw returns a copy of the undecoded payload.
func (e *Event) Raw() []byte {
	if e == nil {
		return nil
	}
	return append([]byte(nil), e.raw...)
}

// Decode unmarshals the payload using the codec selected for this event.
func (e *Event) Decode(value any) error {
	if e == nil || e.codec == nil {
		return fmt.Errorf("f1: event codec is unavailable")
	}
	return e.codec.Decode(e.raw, value)
}

// IdempotencyKey returns the producer key, falling back to the event ID.
func (e *Event) IdempotencyKey() string {
	if e == nil {
		return ""
	}
	if e.envelope.IdempotencyKey != "" {
		return e.envelope.IdempotencyKey
	}
	return e.envelope.ID
}
