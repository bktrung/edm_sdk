package retry

import "time"

const sanityMargin = 10

// CounterRunaway reports whether the attempt counter exceeded its configured
// limit by the sanity margin.
func CounterRunaway(attempt, maxAttempts int) bool {
	return maxAttempts > 0 && attempt > maxAttempts+sanityMargin
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
