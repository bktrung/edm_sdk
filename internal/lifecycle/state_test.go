package lifecycle

import "testing"

func TestMachineTransitionsStartingToClosed(t *testing.T) {
	machine := New()
	if got := machine.State(); got != Starting {
		t.Fatalf("new machine state = %s, want starting", got)
	}
	for _, state := range []State{Ready, Draining, Closed} {
		if err := machine.Transition(state); err != nil {
			t.Fatalf("Transition(%s): %v", state, err)
		}
		if got := machine.State(); got != state {
			t.Fatalf("State() after Transition(%s) = %s", state, got)
		}
	}
}

func TestMachineRejectsIllegalTransitions(t *testing.T) {
	machine := New()
	if err := machine.Transition(Draining); err == nil {
		t.Fatal("Starting -> Draining must be rejected")
	}
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	if err := machine.Transition(Ready); err != nil {
		t.Fatal("same-state transition should be idempotent")
	}
	if err := machine.Transition(Starting); err == nil {
		t.Fatal("Ready -> Starting must be rejected")
	}
}

// TestMachineDrainingOnlyClosesOrAborts pins the post-drain fan-out: once a
// runner is Draining the only forward move is Closed, and Closed is the one
// live state Aborted is no longer reachable from.
func TestMachineDrainingOnlyClosesOrAborts(t *testing.T) {
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	if err := machine.Transition(Draining); err != nil {
		t.Fatal(err)
	}
	if err := machine.Transition(Ready); err == nil {
		t.Fatal("Draining -> Ready must be rejected")
	}
	if err := machine.Transition(Reconnecting); err == nil {
		t.Fatal("Draining -> Reconnecting must be rejected")
	}
	if err := machine.Transition(Closed); err != nil {
		t.Fatal(err)
	}
	if err := machine.Transition(Aborted); err == nil {
		t.Fatal("Closed -> Aborted must be rejected")
	}
}

func TestMachineReconnectingTransitions(t *testing.T) {
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	if err := machine.Transition(Reconnecting); err != nil {
		t.Fatal(err)
	}
	if got := machine.State(); got != Reconnecting {
		t.Fatalf("state after reconnect = %s, want reconnecting", got)
	}
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	if got := machine.State(); got != Ready {
		t.Fatalf("state after reconnect finished = %s, want ready", got)
	}
}

func TestMachineAbortIsTerminal(t *testing.T) {
	machine := New()
	if err := machine.Transition(Aborted); err != nil {
		t.Fatal(err)
	}
	if got := machine.State(); got != Aborted {
		t.Fatalf("state = %s, want aborted", got)
	}
	if err := machine.Transition(Ready); err == nil {
		t.Fatal("Aborted -> Ready must be rejected")
	}
}

// TestMachineAbortedMayDrainAgain pins the one move out of Aborted: a shutdown
// that failed part way is retried, and the retry runs the drain again. Every
// other move stays refused, and Closed stays the state that does not move at
// all.
func TestMachineAbortedMayDrainAgain(t *testing.T) {
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	if err := machine.Transition(Draining); err != nil {
		t.Fatal(err)
	}
	if err := machine.Transition(Aborted); err != nil {
		t.Fatal(err)
	}
	if err := machine.Transition(Draining); err != nil {
		t.Fatalf("Aborted -> Draining must be allowed for a retried shutdown: %v", err)
	}
	if got := machine.State(); got != Draining {
		t.Fatalf("state after a retried drain = %s, want draining", got)
	}
	if err := machine.Transition(Closed); err != nil {
		t.Fatal(err)
	}
	if err := machine.Transition(Draining); err == nil {
		t.Fatal("Closed -> Draining must be rejected")
	}
}

func TestMachineFailedIsTerminal(t *testing.T) {
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	if err := machine.Transition(Failed); err != nil {
		t.Fatal(err)
	}
	if got := machine.State(); got != Failed || got.String() != "failed" {
		t.Fatalf("state = %s, want failed", got)
	}
	if err := machine.Transition(Ready); err == nil {
		t.Fatal("Failed -> Ready must be rejected")
	}
	if err := machine.Transition(Reconnecting); err == nil {
		t.Fatal("Failed -> Reconnecting must be rejected")
	}
}

func TestMachineAbortCannotBecomeFailed(t *testing.T) {
	machine := New()
	if err := machine.Transition(Aborted); err != nil {
		t.Fatal(err)
	}
	if err := machine.Transition(Failed); err == nil {
		t.Fatal("Aborted -> Failed must be rejected")
	}
}

func TestStateStringsAndNilMachine(t *testing.T) {
	if got := State(99).String(); got != "unknown" {
		t.Fatalf("unknown state string = %q", got)
	}
	var machine *Machine
	if machine.State() != Aborted {
		t.Fatal("nil machine must report aborted")
	}
	if err := machine.Transition(Ready); err == nil {
		t.Fatal("nil transition must fail")
	}
}

func TestAllStateStrings(t *testing.T) {
	for state, want := range map[State]string{
		Starting: "starting", Ready: "ready", Reconnecting: "reconnecting", Draining: "draining",
		Closed: "closed", Aborted: "aborted", Failed: "failed",
	} {
		if got := state.String(); got != want {
			t.Errorf("State(%d).String() = %q, want %q", state, got, want)
		}
	}
}
