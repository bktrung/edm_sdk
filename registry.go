package f1

import (
	"context"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
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

// Add registers a delivery. The message is accepted for compatibility, but
// the registry only needs the identity key and does not retain the message.
func (r *inflightRegistry) Add(_ driver.InboundMessage) uint64 {
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
