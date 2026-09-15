package retry

const sanityMargin = 10

// CounterRunaway reports whether the attempt counter exceeded its configured
// limit by the sanity margin.
func CounterRunaway(attempt, maxAttempts int) bool {
	return maxAttempts > 0 && attempt > maxAttempts+sanityMargin
}
