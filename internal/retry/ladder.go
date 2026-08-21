package retry

import (
	"math"
	"time"
)

// Config is the retry-ladder data needed by the decision package. It mirrors
// the public retry configuration without importing the root package.
type Config struct {
	MaxAttempts     int
	InitialInterval time.Duration
	Multiplier      float64
	MaxInterval     time.Duration
	Jitter          float64
	Tiers           []time.Duration
}

// DelayFor returns the nominal delay for a one-based retry number.
func (c Config) DelayFor(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if len(c.Tiers) > 0 {
		index := attempt - 1
		if index >= len(c.Tiers) {
			index = len(c.Tiers) - 1
		}
		return c.Tiers[index]
	}
	initial := c.InitialInterval
	if initial <= 0 {
		initial = time.Second
	}
	multiplier := c.Multiplier
	if multiplier == 0 {
		multiplier = 5
	}
	delay := initial
	for i := 1; i < attempt; i++ {
		if c.MaxInterval > 0 && delay >= c.MaxInterval {
			return c.MaxInterval
		}
		next := float64(delay) * multiplier
		if next <= 0 || next >= float64(time.Duration(1<<63-1)) {
			return c.MaxInterval
		}
		delay = time.Duration(next)
	}
	if c.MaxInterval > 0 && delay > c.MaxInterval {
		return c.MaxInterval
	}
	return delay
}

// FullJitter returns a delay uniformly sampled from zero through nominal.
// sample must be in [0, 1]; values outside that range are clamped.
func FullJitter(nominal time.Duration, sample float64) time.Duration {
	if nominal <= 0 {
		return 0
	}
	if math.IsNaN(sample) || sample < 0 {
		sample = 0
	}
	if sample > 1 {
		sample = 1
	}
	return time.Duration(float64(nominal) * sample)
}

// TierCount returns the number of retry destinations represented by c.
func (c Config) TierCount() int {
	if len(c.Tiers) > 0 {
		return len(c.Tiers)
	}
	if c.MaxAttempts <= 1 {
		return 0
	}
	return c.MaxAttempts - 1
}

// ResolveTier selects the ordinary retry tier for a one-based attempt.
func ResolveTier(c Config, attempt int) int {
	count := c.TierCount()
	if count == 0 {
		return 0
	}
	if attempt < 1 {
		attempt = 1
	}
	if attempt > count {
		return count
	}
	return attempt
}

// ResolveRetryAfter routes an explicit delay to its nearest nominal tier and
// clamps it to that tier's jitter band. The bool reports whether clamping was
// required. A zero-tier ladder returns zero values without panicking.
func ResolveRetryAfter(c Config, requested time.Duration) (tier int, delay time.Duration, clamped bool) {
	count := c.TierCount()
	if count == 0 {
		return 0, 0, false
	}
	if requested < 0 {
		requested = 0
	}
	tier = 1
	bestDistance := absDuration(requested - c.DelayFor(1))
	for candidate := 2; candidate <= count; candidate++ {
		distance := absDuration(requested - c.DelayFor(candidate))
		if distance < bestDistance {
			tier = candidate
			bestDistance = distance
		}
	}
	nominal := c.DelayFor(tier)
	low, high := jitterBand(nominal, c.Jitter)
	delay = requested
	if delay < low {
		delay = low
		clamped = true
	}
	if delay > high {
		delay = high
		clamped = true
	}
	return tier, delay, clamped
}

func jitterBand(nominal time.Duration, jitter float64) (time.Duration, time.Duration) {
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}
	low := time.Duration(float64(nominal) * (1 - jitter))
	high := time.Duration(float64(nominal) * (1 + jitter))
	return low, high
}

func absDuration(value time.Duration) time.Duration {
	if value < 0 {
		return -value
	}
	return value
}
