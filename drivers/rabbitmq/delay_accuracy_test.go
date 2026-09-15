package rabbitmq

import (
	"testing"
	"time"
)

// TestDelayAccuracyMatchesTheParkLadder holds the accuracy the driver declares
// to the parking ladder it is derived from. The declaration is read from
// Capabilities rather than recomputed here, so a hand edit to the declaration
// is caught as well as a drift in the derivation: the two are checked against
// each other nowhere else, which leaves a false number shipping green.
//
// The ladder is not frozen by this. The derivation recomputes the declared
// bound from the rungs, so an edited ladder moves the declaration along with it
// and every assertion here still holds.
func TestDelayAccuracyMatchesTheParkLadder(t *testing.T) {
	accuracy := Driver{}.Capabilities().DelayAccuracy

	first := parkRungs[0]
	last := parkRungs[len(parkRungs)-1]
	if accuracy.Floor != first || accuracy.MaxDelay != last {
		t.Fatalf("declared accuracy = %+v, want floor %s from the first rung and max delay %s from the last", accuracy, first, last)
	}

	// Every rung sits within one relative step of the rung below it. That is
	// the declaration's own promise: a delay parked in a rung above the one it
	// rounded out of is late by at most that step, which the relative covers
	// for every delay in the band and not only for the rungs shipped here.
	for i := range len(parkRungs) - 1 {
		previous, rung := parkRungs[i], parkRungs[i+1]
		if float64(rung) > float64(previous)*(1+accuracy.Relative) {
			t.Errorf("rung %s is more than %v times the rung below it %s, so the declared relative %v is too small", rung, 1+accuracy.Relative, previous, accuracy.Relative)
		}
	}

	// The edges of the declared band, where the ladder rounds a delay up to a
	// rung and the lateness is the whole gap: a delay of one nanosecond and a
	// delay at the first rung, both parked in that first rung, then a delay one
	// nanosecond above every rung below the top one.
	delays := make([]time.Duration, 0, len(parkRungs)+1)
	delays = append(delays, 1, first)
	for _, rung := range parkRungs[:len(parkRungs)-1] {
		delays = append(delays, rung+1)
	}
	for _, delay := range delays {
		parked := parkRung(delay)
		if parked == 0 {
			t.Errorf("parkRung(%s) = 0, want a rung at or below the declared maximum %s", delay, last)
			continue
		}
		bound := max(accuracy.Floor, time.Duration(float64(delay)*accuracy.Relative))
		if lateness := parked - delay; lateness > bound {
			t.Errorf("parkRung(%s) = %s, late by %s, want at most %s", delay, parked, lateness, bound)
		}
	}

	// Above the top rung the driver declares no bound, and the routing agrees:
	// the delay belongs on the per-message expiration path, which has no rung.
	if parked := parkRung(last + 1); parked != 0 {
		t.Errorf("parkRung(%s) = %s, want 0: the declared bound ends at %s", last+1, parked, last)
	}
}
