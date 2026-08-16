package lifecycle

import "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/dispatch"

// Disposition is the terminal accounting category for one accepted message.
type Disposition = dispatch.Disposition

const (
	// Handled means the original message was acknowledged as successful.
	Handled = dispatch.DispositionHandled
	// Requeued means the original message was returned for broker redelivery.
	Requeued = dispatch.DispositionRequeued
	// Retried means a successor retry copy was durably published.
	Retried = dispatch.DispositionRetried
	// DeadLettered means a successor dead-letter copy was durably published.
	DeadLettered = dispatch.DispositionDeadLettered
)

// Counts is a point-in-time snapshot of terminal message dispositions.
type Counts = dispatch.DispositionCounts

// Accounting reads terminal disposition counts from a dispatch registry.
type Accounting struct {
	registry *dispatch.Registry
}

// NewAccounting returns a read-only accounting view over registry.
func NewAccounting(registry *dispatch.Registry) *Accounting {
	return &Accounting{registry: registry}
}

// Snapshot returns the current terminal disposition counts.
func (a *Accounting) Snapshot() Counts {
	if a == nil || a.registry == nil {
		return Counts{}
	}
	return a.registry.Counts().Dispositions
}
