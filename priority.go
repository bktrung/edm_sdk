package f1

import "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/wire"

// Priority identifies a delivery lane. Its zero value is PriorityMedium;
// numeric values are identifiers, not an ordering.
type Priority int

const (
	// PriorityMedium is the default delivery lane.
	PriorityMedium Priority = iota
	// PriorityHigh is the high-priority delivery lane.
	PriorityHigh
	// PriorityLow is the low-priority delivery lane.
	PriorityLow
)

// String returns the wire name of p, or "invalid" for an unknown value.
func (p Priority) String() string {
	switch p {
	case PriorityMedium:
		return "medium"
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
	case PriorityMedium, PriorityHigh, PriorityLow:
		return true
	default:
		return false
	}
}

// ParsePriority converts a wire name to a Priority. Empty input selects
// PriorityMedium; unknown names return an error.
func ParsePriority(s string) (Priority, error) {
	switch s {
	case "":
		return PriorityMedium, nil
	case "medium":
		return PriorityMedium, nil
	case "high":
		return PriorityHigh, nil
	case "low":
		return PriorityLow, nil
	default:
		return 0, &UnrecognisedValueError{Attribute: wire.Priority, Value: s}
	}
}
