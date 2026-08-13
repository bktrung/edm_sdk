package dispatch

import (
	"context"
	"sync"
)

// SettlementOperation identifies the kind of settlement call made for a delivery.
type SettlementOperation uint8

const (
	// SettlementOperationNone means no settlement call has been made.
	SettlementOperationNone SettlementOperation = iota
	// SettlementOperationAck means the delivery was asked to be acknowledged.
	SettlementOperationAck
	// SettlementOperationNack means the delivery was asked to be negatively acknowledged.
	SettlementOperationNack
)

// SettlementOutcome identifies the terminal result recorded for a delivery.
type SettlementOutcome uint8

const (
	// SettlementOutcomeSettled means the delivery was acknowledged successfully.
	SettlementOutcomeSettled SettlementOutcome = iota
	// SettlementOutcomeRequeued means the delivery was negatively acknowledged for redelivery.
	SettlementOutcomeRequeued
	// SettlementOutcomeUnknown means the settlement call did not return success.
	SettlementOutcomeUnknown
	// SettlementOutcomeAbandoned means the handler did not complete before shutdown.
	SettlementOutcomeAbandoned
)

// SettlementCounts is a snapshot of received deliveries by settlement outcome.
type SettlementCounts struct {
	Received  uint64
	Settled   uint64
	Requeued  uint64
	Unknown   uint64
	Abandoned uint64
}

// Registry tracks accepted work until its settlement result is recorded.
type Registry struct {
	mu      sync.Mutex
	items   map[uint64]struct{}
	next    uint64
	zero    chan struct{}
	changed chan struct{}
	count   SettlementCounts
}

// NewRegistry returns an empty settlement-aware registry.
func NewRegistry() *Registry {
	zero := make(chan struct{})
	close(zero)
	return &Registry{items: make(map[uint64]struct{}), zero: zero, changed: make(chan struct{})}
}

// Add registers work and returns its identity.
func (r *Registry) Add() uint64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.next++
	if len(r.items) == 0 {
		r.zero = make(chan struct{})
	}
	r.count.Received++
	r.items[r.next] = struct{}{}
	return r.next
}

// Remove records the default successful settlement for a delivery.
func (r *Registry) Remove(id uint64) {
	r.RemoveAs(id, SettlementOutcomeSettled)
}

// RemoveAs removes a delivery and records its settlement outcome.
func (r *Registry) RemoveAs(id uint64, outcome SettlementOutcome) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.items[id]; !ok {
		return
	}
	delete(r.items, id)
	close(r.changed)
	r.changed = make(chan struct{})
	switch outcome {
	case SettlementOutcomeSettled:
		r.count.Settled++
	case SettlementOutcomeRequeued:
		r.count.Requeued++
	case SettlementOutcomeUnknown:
		r.count.Unknown++
	case SettlementOutcomeAbandoned:
		r.count.Abandoned++
	default:
		r.count.Unknown++
	}
	if len(r.items) == 0 {
		close(r.zero)
	}
}

// Counts returns a snapshot of all recorded settlement outcomes.
func (r *Registry) Counts() SettlementCounts {
	if r == nil {
		return SettlementCounts{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

// Len returns the number of deliveries without a recorded settlement result.
func (r *Registry) Len() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.items)
}

// WaitZero waits until every registered delivery has a settlement result.
//
//nolint:contextcheck // the caller context intentionally controls this wait.
func (r *Registry) WaitZero(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	zero := r.zero
	r.mu.Unlock()
	select {
	case <-zero:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitFor waits until every identity in ids has a settlement result.
//
//nolint:contextcheck // the caller context intentionally controls this wait.
func (r *Registry) WaitFor(ctx context.Context, ids []uint64) error {
	if r == nil || len(ids) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		r.mu.Lock()
		remaining := false
		for _, id := range ids {
			if _, ok := r.items[id]; ok {
				remaining = true
				break
			}
		}
		changed := r.changed
		r.mu.Unlock()
		if !remaining {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
