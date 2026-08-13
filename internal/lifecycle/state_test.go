package lifecycle

import "testing"

func TestMachineTransitionsAndProbes(t *testing.T) {
	machine := New()
	if !machine.Live() || machine.Ready() || machine.State() != Starting {
		t.Fatal("new machine must be live, not ready, and starting")
	}
	for _, state := range []State{Ready, Draining, Settling, Flushing, Closed} {
		if err := machine.Transition(state); err != nil {
			t.Fatalf("Transition(%s): %v", state, err)
		}
		if state == Ready && !machine.Ready() {
			t.Fatal("ready probe rejected Ready")
		}
	}
	if machine.Live() != true || machine.Ready() {
		t.Fatal("closed machine probes are incorrect")
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

func TestMachineAbortIsTerminal(t *testing.T) {
	machine := New()
	if err := machine.Transition(Aborted); err != nil {
		t.Fatal(err)
	}
	if machine.Live() || machine.Ready() {
		t.Fatal("aborted machine must not be live or ready")
	}
	if err := machine.Transition(Ready); err == nil {
		t.Fatal("Aborted -> Ready must be rejected")
	}
}

func TestStateStringsAndNilMachine(t *testing.T) {
	if got := State(99).String(); got != "unknown" {
		t.Fatalf("unknown state string = %q", got)
	}
	var machine *Machine
	if machine.State() != Aborted || machine.Ready() || machine.Live() {
		t.Fatal("nil machine probes must be closed and unhealthy")
	}
	if err := machine.Transition(Ready); err == nil {
		t.Fatal("nil transition must fail")
	}
}

func TestAllStateStrings(t *testing.T) {
	for state, want := range map[State]string{
		Starting: "starting", Ready: "ready", Draining: "draining", Settling: "settling",
		Flushing: "flushing", Closed: "closed", Aborted: "aborted",
	} {
		if got := state.String(); got != want {
			t.Errorf("State(%d).String() = %q, want %q", state, got, want)
		}
	}
}
