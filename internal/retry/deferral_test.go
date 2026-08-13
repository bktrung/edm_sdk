package retry

import (
	"testing"
	"time"
)

func TestDeferralTierHoldsAtLastTier(t *testing.T) {
	for hop, want := range map[int]int{0: 1, 1: 1, 2: 2, 3: 3, 99: 3} {
		if got := DeferralTier(hop, 3); got != want {
			t.Errorf("DeferralTier(%d) = %d, want %d", hop, got, want)
		}
	}
	if got := DeferralTier(1, 0); got != 0 {
		t.Fatalf("zero-tier deferral = %d, want 0", got)
	}
}

func TestCounterRunawayUsesSanityMargin(t *testing.T) {
	if CounterRunaway(4, 24, 4, 24) {
		t.Fatal("normal caps must not be runaway")
	}
	if !CounterRunaway(35, 24, 4, 24) || !CounterRunaway(4, 35, 4, 24) {
		t.Fatal("counter beyond cap plus margin must be runaway")
	}
	if SanityMargin() != 10 {
		t.Fatalf("SanityMargin() = %d, want 10", SanityMargin())
	}
}

func TestDeferralCoverUsesHeldLastTier(t *testing.T) {
	tiers := []time.Duration{time.Second, 5 * time.Second, 25 * time.Second}
	if got, want := LadderCover(tiers), 31*time.Second; got != want {
		t.Fatalf("LadderCover() = %s, want %s", got, want)
	}
	if got, want := DeferralCover(tiers, 24), 556*time.Second; got != want {
		t.Fatalf("DeferralCover() = %s, want %s", got, want)
	}
}

func TestCoverHelpersHandleEmptyAndNonPositiveInputs(t *testing.T) {
	if got := LadderCover([]time.Duration{-time.Second, 0, time.Second}); got != time.Second {
		t.Fatalf("LadderCover() = %s, want 1s", got)
	}
	if got := DeferralCover(nil, 3); got != 0 {
		t.Fatalf("empty DeferralCover() = %s, want 0", got)
	}
	if got := DeferralCover([]time.Duration{time.Second}, 0); got != 0 {
		t.Fatalf("zero-hop DeferralCover() = %s, want 0", got)
	}
}
