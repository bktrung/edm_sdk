package f1

// DeathReason identifies why a message was dead-lettered.
type DeathReason string

const (
	// ReasonUnspecified is the empty value for live messages and DLQ messages
	// without an SDK-recorded death reason.
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
	// ReasonPoison means the envelope attempt counter exceeded the delivery's
	// effective maxAttempts by more than 10, so the worker dead-letters the delivery as a
	// retry-counter runaway.
	ReasonPoison DeathReason = "poison"
	// ReasonUnmatched means no handler matched the event type.
	ReasonUnmatched DeathReason = "unmatched"
)

// String returns the wire value of r.
func (r DeathReason) String() string { return string(r) }
