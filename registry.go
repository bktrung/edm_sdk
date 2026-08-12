package f1

import (
	"context"
	"sync"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// inflightRegistry tracks broker deliveries from the moment the fetcher accepts
// them until the worker has completed settlement. The mutex protects both the
// map and the zero-transition channel; callers never hold it while doing
// handler, publish, or driver work.
// Invariant: items contains every delivery accepted by the fetcher until its
// settlement call returns; the zero channel changes only under mu.
type inflightRegistry struct {
	mu    sync.Mutex
	items map[uint64]driver.InboundMessage
	next  uint64
	zero  chan struct{}
}

func newInflightRegistry() *inflightRegistry {
	return &inflightRegistry{items: make(map[uint64]driver.InboundMessage), zero: closedSignal()}
}

func closedSignal() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// Add registers a delivery before it is placed on the worker queue.
func (r *inflightRegistry) Add(message driver.InboundMessage) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.next++
	if len(r.items) == 0 {
		r.zero = make(chan struct{})
	}
	id := r.next
	r.items[id] = message
	return id
}

// Remove deregisters a delivery after its settlement call has returned.
func (r *inflightRegistry) Remove(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.items[id]; !ok {
		return
	}
	delete(r.items, id)
	if len(r.items) == 0 {
		close(r.zero)
	}
}

// Len returns the number of deliveries that have not completed settlement.
func (r *inflightRegistry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.items)
}

// WaitZero waits until every registered delivery has completed settlement.
func (r *inflightRegistry) WaitZero(ctx context.Context) error {
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
