package f1

import (
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
)

// This file builds the client lifecycle state a fixture means, so a test that
// needs a client part way through shutdown reaches it the way the product does
// rather than by writing a flag the product no longer has.
//
// lifecyclePath is the sequence of transitions a Close takes to reach state,
// and it is the product's own sequence: New moves the machine to Ready, so a
// client in Draining is one whose Close has been entered, a client in Aborted
// is one whose Close gave up part way, and a client in Closed is one whose
// Close released its resources. A fixture that asked for a state this list
// cannot reach has a fixture defect and panics.
func lifecyclePath(state lifecycle.State) []lifecycle.State {
	switch state {
	case lifecycle.Ready:
		return []lifecycle.State{lifecycle.Ready}
	case lifecycle.Draining:
		return []lifecycle.State{lifecycle.Ready, lifecycle.Draining}
	case lifecycle.Aborted:
		return []lifecycle.State{lifecycle.Ready, lifecycle.Draining, lifecycle.Aborted}
	case lifecycle.Closed:
		return []lifecycle.State{lifecycle.Ready, lifecycle.Draining, lifecycle.Closed}
	default:
		panic("lifecyclePath: the client has no path to " + state.String())
	}
}

// lifecycleIn returns a client lifecycle machine in state, reached through the
// transitions lifecyclePath names. The machine is not locked against anything:
// a fixture installs it before the client is shared with another goroutine.
func lifecycleIn(state lifecycle.State) *lifecycle.Machine {
	machine := lifecycle.New()
	for _, next := range lifecyclePath(state) {
		if err := machine.Transition(next); err != nil {
			panic("lifecycleIn: " + err.Error())
		}
	}
	return machine
}

// setClientLifecycle moves a client that is already running to state, under its
// lock. It is for the tests that drive a client into a state whose transition
// they cannot wait for, such as a Close whose bound expires.
func setClientLifecycle(c *Client, state lifecycle.State) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lifecycle = lifecycleIn(state)
}
