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

// Disposition identifies the terminal message outcome recorded by the dispatch registry.
type Disposition uint8

const (
	// DispositionHandled means the original message was acknowledged successfully.
	DispositionHandled Disposition = iota
	// DispositionRequeued means the original message is left for broker redelivery.
	DispositionRequeued
	// DispositionRetried means a successor retry or deferral copy was published.
	DispositionRetried
	// DispositionDeadLettered means a successor dead-letter copy was published.
	DispositionDeadLettered
)

// DispositionCounts is a snapshot of terminal message dispositions.
type DispositionCounts struct {
	Handled      uint64
	Requeued     uint64
	Retried      uint64
	DeadLettered uint64
}

// Total returns the number of recorded message dispositions.
func (c DispositionCounts) Total() uint64 {
	return c.Handled + c.Requeued + c.Retried + c.DeadLettered
}

// SettlementCounts is a snapshot of settlement call outcomes.
type SettlementCounts struct {
	Settled   uint64
	Requeued  uint64
	Unknown   uint64
	Abandoned uint64
}

// Total returns the number of recorded settlement outcomes.
func (c SettlementCounts) Total() uint64 {
	return c.Settled + c.Requeued + c.Unknown + c.Abandoned
}

// Counts is a snapshot of received deliveries and both accounting axes.
type Counts struct {
	Received     uint64
	Settlements  SettlementCounts
	Dispositions DispositionCounts
}

// Registry tracks accepted work until its settlement result is recorded.
type Registry struct {
	mu      sync.Mutex
	items   map[uint64]Disposition
	next    uint64
	zero    chan struct{}
	changed chan struct{}
	count   Counts
}

// NewRegistry returns an empty settlement-aware registry.
func NewRegistry() *Registry {
	zero := make(chan struct{})
	close(zero)
	return &Registry{items: make(map[uint64]Disposition), zero: zero, changed: make(chan struct{})}
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
	r.items[r.next] = DispositionHandled
	return r.next
}

// SetDisposition records the message outcome to use if the delivery settles successfully.
func (r *Registry) SetDisposition(id uint64, disposition Disposition) {
	if r == nil {
		return
	}
	if disposition < DispositionHandled || disposition > DispositionDeadLettered {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.items[id]; !ok {
		return
	}
	r.items[id] = disposition
}

// Remove records the default successful settlement for a delivery.
func (r *Registry) Remove(id uint64) {
	r.RemoveAs(id, SettlementOutcomeSettled)
}

// RemoveAs removes a delivery and records both accounting outcomes.
func (r *Registry) RemoveAs(id uint64, outcome SettlementOutcome) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	disposition, ok := r.items[id]
	if !ok {
		return
	}
	delete(r.items, id)
	close(r.changed)
	r.changed = make(chan struct{})
	switch outcome {
	case SettlementOutcomeSettled:
		switch disposition {
		case DispositionHandled:
			r.count.Dispositions.Handled++
		case DispositionRequeued:
			r.count.Dispositions.Requeued++
		case DispositionRetried:
			r.count.Dispositions.Retried++
		case DispositionDeadLettered:
			r.count.Dispositions.DeadLettered++
		}
		r.count.Settlements.Settled++
	case SettlementOutcomeRequeued:
		r.count.Dispositions.Requeued++
		r.count.Settlements.Requeued++
	case SettlementOutcomeUnknown:
		r.count.Dispositions.Requeued++
		r.count.Settlements.Unknown++
	case SettlementOutcomeAbandoned:
		r.count.Dispositions.Requeued++
		r.count.Settlements.Abandoned++
	default:
		r.count.Dispositions.Requeued++
		r.count.Settlements.Unknown++
	}
	if len(r.items) == 0 {
		close(r.zero)
	}
}

// Counts returns a snapshot of received deliveries and both accounting axes.
func (r *Registry) Counts() Counts {
	if r == nil {
		return Counts{}
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
