package f1

// DeathReason identifies why a message was dead-lettered.
type DeathReason string

const (
	// ReasonUnspecified is the zero value for live or legacy DLQ messages.
	ReasonUnspecified DeathReason = ""

	// ReasonMaxAttempts means the retry limit was exhausted.
	ReasonMaxAttempts DeathReason = "max_attempts"
	// ReasonTerminal means the handler returned a terminal error.
	ReasonTerminal DeathReason = "terminal"
	// ReasonPanic means the handler panicked.
	ReasonPanic DeathReason = "panic"
	// ReasonDecode means the payload could not be decoded.
	ReasonDecode DeathReason = "decode"
	// ReasonExpired means the event expired before handling.
	ReasonExpired DeathReason = "expired"
	// ReasonPoison means the message repeatedly crashed its worker process.
	ReasonPoison DeathReason = "poison"
	// ReasonUnmatched means no handler matched the event type.
	ReasonUnmatched DeathReason = "unmatched"
	// ReasonDedupeUnavailable means the deduplication store exceeded its deferral bound.
	ReasonDedupeUnavailable DeathReason = "dedupe_unavailable"
)

// String returns the wire value of r.
func (r DeathReason) String() string { return string(r) }

// Valid reports whether r is a known dead-letter reason.
func (r DeathReason) Valid() bool {
	switch r {
	case ReasonMaxAttempts, ReasonTerminal, ReasonPanic, ReasonDecode,
		ReasonExpired, ReasonPoison, ReasonUnmatched, ReasonDedupeUnavailable:
		return true
	default:
		return false
	}
}
