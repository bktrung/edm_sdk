package f1

import "encoding/json"

// Priority selects one of the three lanes doc 08 schedules fairly - it is
// never a broker priority field (doc 08 §1: neither broker can do this for
// us). The zero value is PriorityNormal, the safe default (doc 15 §5).
//
// The numeric value is an IDENTITY, not a rank. It exists only so that the
// zero value can be PriorityNormal; PriorityHigh does not compare less than
// PriorityLow and nothing may order lanes by comparing two Priority values
// or by indexing an array with one. Lane order is Subscription.Priorities,
// which is explicit and highest-first, and doc 08's budgets are keyed by
// name. Ordering by the int would silently transpose high and normal.
type Priority int

// The three lanes, in no particular numeric rank (see the identity note above).
const (
	PriorityNormal Priority = iota
	PriorityHigh
	PriorityLow
)

// String returns the wire form. Doc 03 §2 is normative for it: f1priority is
// a string attribute valued high|normal|low, the AMQP routing key composes
// it as <priority>.<partition key> (doc 03 §3.2), and lane queues bind on
// <priority>.#. An int on the wire would serialise as 0|1|2 and produce the
// routing key "1.customer-42", which matches no binding.
//
// A Priority that is none of the three constants returns "invalid", NOT a
// lane name (F-P45). Mapping it onto "normal" - which is what a switch whose
// default case returns the zero value's name does - is the silent lane
// downgrade ParsePriority already refuses on the decode side, performed on
// the encode side instead: "invalid" binds to no lane, so such a message is
// visibly stuck rather than quietly in the wrong one, and EncodeHeaders'
// Valid() check (below) stops it reaching the wire at all.
func (p Priority) String() string {
	switch p {
	case PriorityNormal:
		return "normal"
	case PriorityHigh:
		return "high"
	case PriorityLow:
		return "low"
	default:
		return "invalid"
	}
}

// Valid reports whether p is one of the three declared lanes. EncodeHeaders
// rejects !Valid() with ErrInvalidPriority rather than letting String's
// "invalid" reach the wire - the same stamping-side assertion DeathReason.Valid
// is, and the two are deliberately the same shape (F-P45).
func (p Priority) Valid() bool {
	switch p {
	case PriorityNormal, PriorityHigh, PriorityLow:
		return true
	default:
		return false
	}
}

// ParsePriority reads the wire form. Absent or empty decodes to
// PriorityNormal (doc 03 §2's default); an unrecognised value is an error -
// a lane this build cannot schedule must not be silently downgraded to
// normal. The caller classifies it (doc 05 §4's Terminal, once M1-07 exists):
// this package has no error taxonomy yet to do that itself.
func ParsePriority(s string) (Priority, error) {
	switch s {
	case "", "normal":
		return PriorityNormal, nil
	case "high":
		return PriorityHigh, nil
	case "low":
		return PriorityLow, nil
	default:
		return 0, &UnrecognisedValueError{Attribute: "f1priority", Value: s}
	}
}

// MarshalJSON and UnmarshalJSON carry the string form, so the Go type and
// the frozen wire format can differ without either document being wrong.
func (p Priority) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.String())
}

// UnmarshalJSON is the counterpart to MarshalJSON above.
func (p *Priority) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := ParsePriority(s)
	if err != nil {
		return err
	}
	*p = v
	return nil
}
