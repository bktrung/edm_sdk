package f1

// Priority identifies a delivery lane. Its zero value is PriorityNormal;
// numeric values are identifiers, not an ordering.
type Priority int

const (
	// PriorityNormal is the default delivery lane.
	PriorityNormal Priority = iota
	// PriorityHigh is the high-priority delivery lane.
	PriorityHigh
	// PriorityLow is the low-priority delivery lane.
	PriorityLow
)

// String returns the wire name of p, or "invalid" for an unknown value.
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

// Valid reports whether p is a declared delivery lane.
func (p Priority) Valid() bool {
	switch p {
	case PriorityNormal, PriorityHigh, PriorityLow:
		return true
	default:
		return false
	}
}

// ParsePriority converts a wire name to a Priority. Empty input selects
// PriorityNormal; unknown names return an error.
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
