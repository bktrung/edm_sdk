package retry

import "time"

const sanityMargin = 10

// DeferralTier resolves a one-based deferral hop to a retry tier, holding at
// the last tier when the outage lasts longer than the configured ladder.
func DeferralTier(hop, tiers int) int {
	if tiers <= 0 {
		return 0
	}
	if hop < 1 {
		hop = 1
	}
	if hop > tiers {
		return tiers
	}
	return hop
}

// CounterRunaway reports whether either counter has exceeded its configured
// limit by the sanity margin. This distinguishes a normal cap from a counter
// that stopped advancing and would otherwise loop forever.
func CounterRunaway(attempt, deferrals, maxAttempts, maxDeferrals int) bool {
	return (maxAttempts > 0 && attempt > maxAttempts+sanityMargin) ||
		(maxDeferrals > 0 && deferrals > maxDeferrals+sanityMargin)
}

// SanityMargin returns the deliberate gap between a normal cap and the
// runaway-counter poison threshold.
func SanityMargin() int { return sanityMargin }

// LadderCover returns the total nominal delay represented by a ladder.
func LadderCover(tiers []time.Duration) time.Duration {
	var total time.Duration
	for _, tier := range tiers {
		if tier > 0 {
			total += tier
		}
	}
	return total
}

// DeferralCover returns the nominal delay available across maxDeferrals hops.
// The last ladder tier is held for all later hops.
func DeferralCover(tiers []time.Duration, maxDeferrals int) time.Duration {
	if len(tiers) == 0 || maxDeferrals <= 0 {
		return 0
	}
	var total time.Duration
	for hop := 1; hop <= maxDeferrals; hop++ {
		total += tiers[DeferralTier(hop, len(tiers))-1]
	}
	return total
}
