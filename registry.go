package f1

import (
	"context"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/dispatch"
)

// inflightRegistry tracks the runner's accepted deliveries until they
// settle, delegating storage to the internal dispatch registry. Drain waits
// on it; nothing else reads from it.
type inflightRegistry struct {
	registry *dispatch.Registry
}

func newInflightRegistry() *inflightRegistry {
	return &inflightRegistry{registry: dispatch.NewRegistry()}
}

// Add registers a delivery and returns its identity. The registry tracks the
// identity alone; the message belongs to the settlement path.
func (r *inflightRegistry) Add() uint64 {
	if r == nil {
		return 0
	}
	return r.registry.Add()
}

// Remove drops the delivery once its settlement path finished, whether it
// settled by ack, by requeue, or at the end of the bounded cleanup budget.
func (r *inflightRegistry) Remove(id uint64) {
	if r != nil {
		r.registry.Remove(id)
	}
}

// Len returns the number of deliveries still in flight.
func (r *inflightRegistry) Len() int {
	if r == nil {
		return 0
	}
	return r.registry.Len()
}

// WaitZero waits until every accepted delivery has left the registry.
func (r *inflightRegistry) WaitZero(ctx context.Context) error {
	if r == nil {
		return nil
	}
	return r.registry.WaitZero(ctx)
}
