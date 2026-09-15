package dispatch

import (
	"context"
	"sync"
)

// Registry tracks accepted work until the worker removes it.
// The zero value is not usable; build one with NewRegistry. Every method is
// safe for concurrent use, and a nil registry is empty and already drained.
type Registry struct {
	mu    sync.Mutex
	items map[uint64]struct{}
	next  uint64
	// zero is closed whenever items becomes empty. It is replaced whenever
	// Add grows the set back from empty, so a waiter that captured a closed
	// channel has already observed a genuinely empty set at capture time.
	zero chan struct{}
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	zero := make(chan struct{})
	close(zero)
	return &Registry{items: make(map[uint64]struct{}), zero: zero}
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
	r.items[r.next] = struct{}{}
	return r.next
}

// Remove drops a delivery from the registry. Removing an unknown or already
// removed identity does nothing, so a duplicate removal never closes the
// channel twice.
func (r *Registry) Remove(id uint64) {
	if r == nil {
		return
	}
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

// Len returns the number of deliveries still registered.
func (r *Registry) Len() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.items)
}

// WaitZero waits until the registry is empty.
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
