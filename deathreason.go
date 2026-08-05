package f1

// DeathReason is why the core dead-lettered a message: the f1deathreason
// attribute of doc 03 §2.3, whose value set doc 07 §5.1 is normative for.
// It is set by the core and never by a handler.
//
// Unlike Priority the underlying type IS the wire form. There is no safe
// default to put at the zero value - a message that reached the DLQ died for
// exactly one of the eight reasons below - so the zero value is explicitly
// invalid. A string type makes that zero the empty string, which is what
// omitempty already omits on a live message's envelope, so one type covers
// "not dead" and "invalid" without a second representation to keep in sync.
type DeathReason string

const (
	// ReasonUnspecified is the zero value: not dead-lettered, or a DLQ
	// message read back from a producer this build predates. Never stamped.
	ReasonUnspecified DeathReason = ""

	// ReasonMaxAttempts means the retry ladder was exhausted.
	ReasonMaxAttempts DeathReason = "max_attempts"
	// ReasonTerminal means the handler returned f1.Terminal.
	ReasonTerminal DeathReason = "terminal"
	// ReasonPanic means the handler panicked; recovered and treated as a defect.
	ReasonPanic DeathReason = "panic"
	// ReasonDecode means the payload failed to decode.
	ReasonDecode DeathReason = "decode"
	// ReasonExpired means the event was handled after f1expiry.
	ReasonExpired DeathReason = "expired"
	// ReasonPoison means the message repeatedly crashed the worker process itself.
	ReasonPoison DeathReason = "poison"
	// ReasonUnmatched means no Handlers entry matched the event type and
	// UnmatchedPolicy was DeadLetter.
	ReasonUnmatched DeathReason = "unmatched"
	// ReasonDedupeUnavailable means the dedupe store was unavailable past
	// the deferral bound.
	ReasonDedupeUnavailable DeathReason = "dedupe_unavailable"
)

// String returns the wire form, which is the value itself.
func (r DeathReason) String() string { return string(r) }

// Valid reports whether r is one of the eight reasons doc 07 §5.1 defines.
//
// Reading a DLQ message does NOT go through this: DecodeHeaders preserves an
// unrecognised f1deathreason as-is (TestDLQ_UnknownReasonSurvivesReplay),
// where an unrecognised Priority is a decode error (ParsePriority). The
// asymmetry is deliberate: an unknown lane cannot be scheduled, so accepting
// it would misroute the message; an unknown death reason is diagnostic
// metadata on a message that is already dead, and doc 07 §5.1 requires a DLQ
// message to be replayable with no external state. Failing the decode would
// make a newer producer's DLQ unreadable by an older replayer, destroying
// the evidence the DLQ exists to hold.
func (r DeathReason) Valid() bool {
	switch r {
	case ReasonMaxAttempts, ReasonTerminal, ReasonPanic, ReasonDecode,
		ReasonExpired, ReasonPoison, ReasonUnmatched, ReasonDedupeUnavailable:
		return true
	default:
		return false
	}
}
