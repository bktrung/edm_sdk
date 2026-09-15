package lifecycle

import (
	"fmt"
	"sync"
)

// State identifies one lifecycle state of a running consumer.
type State uint8

const (
	// Starting means startup checks and worker construction are in progress.
	Starting State = iota
	// Ready means the consumer accepts work and may report ready.
	Ready
	// Reconnecting means the broker connection is being rebuilt. The process
	// remains live, but the consumer is not ready for traffic.
	Reconnecting
	// Draining means new fetches are stopped and accepted work is finishing.
	Draining
	// Closed means all owned resources have been released.
	Closed
	// Aborted means shutdown exceeded a deadline or a fatal shutdown error occurred.
	Aborted
	// Failed means this runner encountered a terminal consumer error.
	Failed
)

// String returns the stable state name.
func (s State) String() string {
	switch s {
	case Starting:
		return "starting"
	case Ready:
		return "ready"
	case Reconnecting:
		return "reconnecting"
	case Draining:
		return "draining"
	case Closed:
		return "closed"
	case Aborted:
		return "aborted"
	case Failed:
		return "failed"
	default:
		return "unknown"
	}
}

// Machine stores lifecycle state and validates transitions.
type Machine struct {
	mu    sync.RWMutex
	state State
}

// New returns a machine in Starting.
func New() *Machine { return &Machine{state: Starting} }

// State returns the current state.
func (m *Machine) State() State {
	if m == nil {
		return Aborted
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

// Transition moves to next when the lifecycle table permits it.
func (m *Machine) Transition(next State) error {
	if m == nil {
		return fmt.Errorf("lifecycle: nil machine")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !allowed(m.state, next) {
		return fmt.Errorf("lifecycle: invalid transition %s -> %s", m.state, next)
	}
	m.state = next
	return nil
}

func allowed(from, to State) bool {
	if from == to {
		return true
	}
	if to == Failed && from != Closed && from != Failed && from != Aborted {
		return true
	}
	if to == Aborted && from != Closed && from != Aborted {
		return true
	}
	switch from {
	case Starting:
		return to == Ready
	case Ready:
		return to == Reconnecting || to == Draining
	case Reconnecting:
		return to == Ready || to == Draining
	case Draining:
		return to == Closed
	default:
		return false
	}
}
