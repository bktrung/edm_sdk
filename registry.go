package f1

import (
	"context"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/dispatch"
)

type settlementOperation = dispatch.SettlementOperation

const (
	settlementOperationNone = dispatch.SettlementOperationNone
	settlementOperationAck  = dispatch.SettlementOperationAck
	settlementOperationNack = dispatch.SettlementOperationNack
)

type settlementOutcome = dispatch.SettlementOutcome

const (
	settlementOutcomeSettled   = dispatch.SettlementOutcomeSettled
	settlementOutcomeRequeued  = dispatch.SettlementOutcomeRequeued
	settlementOutcomeUnknown   = dispatch.SettlementOutcomeUnknown
	settlementOutcomeAbandoned = dispatch.SettlementOutcomeAbandoned
)

type settlementCounts struct {
	received  uint64
	settled   uint64
	requeued  uint64
	unknown   uint64
	abandoned uint64
}

// inflightRegistry preserves the root worker's settlement call shape while
// delegating storage and accounting to the internal dispatch registry.
type inflightRegistry struct {
	registry *dispatch.Registry
}

func newInflightRegistry() *inflightRegistry {
	return &inflightRegistry{registry: dispatch.NewRegistry()}
}

// Add registers a delivery. The message is accepted for compatibility, but
// the registry only needs the identity key and does not retain the message.
func (r *inflightRegistry) Add(_ driver.InboundMessage) uint64 {
	if r == nil {
		return 0
	}
	return r.registry.Add()
}

// SetDisposition records the message outcome to use if the delivery settles successfully.
func (r *inflightRegistry) SetDisposition(id uint64, disposition dispatch.Disposition) {
	if r != nil {
		r.registry.SetDisposition(id, disposition)
	}
}

// Remove records the default successful settlement for a delivery.
func (r *inflightRegistry) Remove(id uint64) {
	if r != nil {
		r.registry.Remove(id)
	}
}

// RemoveAs removes a delivery and records its settlement outcome.
func (r *inflightRegistry) RemoveAs(id uint64, outcome settlementOutcome) {
	if r != nil {
		r.registry.RemoveAs(id, outcome)
	}
}

func (r *inflightRegistry) Counts() settlementCounts {
	if r == nil {
		return settlementCounts{}
	}
	counts := r.registry.Counts()
	return settlementCounts{
		received:  counts.Received,
		settled:   counts.Settlements.Settled,
		requeued:  counts.Settlements.Requeued,
		unknown:   counts.Settlements.Unknown,
		abandoned: counts.Settlements.Abandoned,
	}
}

// DispositionCounts returns a snapshot of terminal message dispositions.
func (r *inflightRegistry) DispositionCounts() dispatch.DispositionCounts {
	if r == nil {
		return dispatch.DispositionCounts{}
	}
	return r.registry.Counts().Dispositions
}

// Len returns the number of deliveries without a recorded settlement result.
func (r *inflightRegistry) Len() int {
	if r == nil {
		return 0
	}
	return r.registry.Len()
}

// WaitZero waits until every registered delivery has a settlement result.
func (r *inflightRegistry) WaitZero(ctx context.Context) error {
	if r == nil {
		return nil
	}
	return r.registry.WaitZero(ctx)
}
