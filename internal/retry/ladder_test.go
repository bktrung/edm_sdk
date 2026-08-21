package retry

import (
	"testing"
	"time"
)

func TestResolveTierHoldsAtLastTier(t *testing.T) {
	cfg := Config{Tiers: []time.Duration{time.Second, 5 * time.Second, 25 * time.Second}}
	for attempt, want := range map[int]int{0: 1, 1: 1, 2: 2, 3: 3, 99: 3} {
		if got := ResolveTier(cfg, attempt); got != want {
			t.Errorf("ResolveTier(%d) = %d, want %d", attempt, got, want)
		}
	}
}

func TestResolveRetryAfterRoutesAndClamps(t *testing.T) {
	cfg := Config{Tiers: []time.Duration{time.Second, 2 * time.Second}, Jitter: .2}
	tier, delay, clamped := ResolveRetryAfter(cfg, 5*time.Minute)
	if tier != 2 || delay != 2400*time.Millisecond || !clamped {
		t.Fatalf("ResolveRetryAfter() = tier %d delay %s clamped %v", tier, delay, clamped)
	}
	tier, delay, clamped = ResolveRetryAfter(cfg, 1900*time.Millisecond)
	if tier != 2 || delay != 1900*time.Millisecond || clamped {
		t.Fatalf("in-band ResolveRetryAfter() = tier %d delay %s clamped %v", tier, delay, clamped)
	}
}

func TestResolveRetryAfterHandlesEmptyAndSingleTier(t *testing.T) {
	if tier, delay, clamped := ResolveRetryAfter(Config{}, time.Second); tier != 0 || delay != 0 || clamped {
		t.Fatalf("empty ladder = %d, %s, %v", tier, delay, clamped)
	}
	cfg := Config{Tiers: []time.Duration{2 * time.Second}, Jitter: .2}
	if tier, delay, clamped := ResolveRetryAfter(cfg, 5*time.Second); tier != 1 || delay != 2400*time.Millisecond || !clamped {
		t.Fatalf("single ladder = %d, %s, %v", tier, delay, clamped)
	}
}

func TestDelayForUsesExponentialDefaultsAndMaximum(t *testing.T) {
	if got := (Config{}).DelayFor(0); got != time.Second {
		t.Fatalf("zero config first delay = %s, want 1s", got)
	}
	if got := (Config{InitialInterval: 2 * time.Second, Multiplier: 3, MaxInterval: 5 * time.Second}).DelayFor(3); got != 5*time.Second {
		t.Fatalf("maximum-clamped delay = %s, want 5s", got)
	}
	if got := (Config{Tiers: []time.Duration{time.Second, 2 * time.Second}}).DelayFor(99); got != 2*time.Second {
		t.Fatalf("held tier delay = %s, want 2s", got)
	}
}

func TestTierCountAndResolveTierBoundaries(t *testing.T) {
	for _, test := range []struct {
		cfg     Config
		attempt int
		want    int
	}{
		{cfg: Config{MaxAttempts: 1}, attempt: 1, want: 0},
		{cfg: Config{MaxAttempts: 4}, attempt: 0, want: 1},
		{cfg: Config{MaxAttempts: 4}, attempt: 99, want: 3},
		{cfg: Config{Tiers: []time.Duration{time.Second, 2 * time.Second}}, attempt: 2, want: 2},
	} {
		if got := ResolveTier(test.cfg, test.attempt); got != test.want {
			t.Errorf("ResolveTier(%+v, %d) = %d, want %d", test.cfg, test.attempt, got, test.want)
		}
	}
	if got := (Config{MaxAttempts: 1}).TierCount(); got != 0 {
		t.Fatalf("TierCount(maxAttempts=1) = %d, want 0", got)
	}
}

func TestDelayForDefensiveDefaults(t *testing.T) {
	for _, test := range []struct {
		name    string
		cfg     Config
		attempt int
		want    time.Duration
	}{
		{
			name:    "zero InitialInterval and zero Multiplier both default, second attempt",
			cfg:     Config{},
			attempt: 2,
			want:    5 * time.Second,
		},
		{
			name:    "zero InitialInterval and zero Multiplier both default, third attempt",
			cfg:     Config{},
			attempt: 3,
			want:    25 * time.Second,
		},
		{
			name:    "negative InitialInterval defaults to 1s",
			cfg:     Config{InitialInterval: -3 * time.Second},
			attempt: 1,
			want:    time.Second,
		},
		{
			name:    "negative InitialInterval with an explicit Multiplier",
			cfg:     Config{InitialInterval: -3 * time.Second, Multiplier: 2},
			attempt: 3,
			want:    4 * time.Second,
		},
		{
			name:    "zero Multiplier defaults to 5 with an explicit InitialInterval",
			cfg:     Config{InitialInterval: 2 * time.Second, Multiplier: 0},
			attempt: 2,
			want:    10 * time.Second,
		},
		{
			name:    "zero InitialInterval interacts with MaxInterval clamp",
			cfg:     Config{InitialInterval: 0, Multiplier: 10, MaxInterval: 20 * time.Second},
			attempt: 3,
			want:    20 * time.Second,
		},
		{
			name:    "negative InitialInterval stays under MaxInterval when unclamped",
			cfg:     Config{InitialInterval: -time.Second, Multiplier: 10, MaxInterval: 20 * time.Second},
			attempt: 2,
			want:    10 * time.Second,
		},
		{
			name:    "explicit Tiers overrides zero InitialInterval and zero Multiplier",
			cfg:     Config{InitialInterval: 0, Multiplier: 0, Tiers: []time.Duration{3 * time.Second, 9 * time.Second}},
			attempt: 1,
			want:    3 * time.Second,
		},
		{
			name:    "explicit Tiers overrides negative InitialInterval and holds at the last tier",
			cfg:     Config{InitialInterval: -5 * time.Second, Multiplier: 0, Tiers: []time.Duration{3 * time.Second, 9 * time.Second}},
			attempt: 5,
			want:    9 * time.Second,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.cfg.DelayFor(test.attempt); got != test.want {
				t.Fatalf("DelayFor(%d) with %+v = %s, want %s", test.attempt, test.cfg, got, test.want)
			}
		})
	}
}

func TestResolveRetryAfterClampsNegativeAndJitterBounds(t *testing.T) {
	if tier, delay, clamped := ResolveRetryAfter(Config{Tiers: []time.Duration{time.Second}, Jitter: -1}, -time.Second); tier != 1 || delay != time.Second || !clamped {
		t.Fatalf("negative request = %d, %s, %v", tier, delay, clamped)
	}
	if tier, delay, clamped := ResolveRetryAfter(Config{Tiers: []time.Duration{time.Second}, Jitter: 2}, 3*time.Second); tier != 1 || delay != 2*time.Second || !clamped {
		t.Fatalf("wide jitter request = %d, %s, %v", tier, delay, clamped)
	}
}

func TestDelayForCapsBeforeDurationOverflow(t *testing.T) {
	cfg := Config{InitialInterval: time.Second, Multiplier: 2, MaxInterval: 30 * time.Second}
	if got := cfg.DelayFor(1000); got != 30*time.Second {
		t.Fatalf("DelayFor(1000) = %s, want 30s", got)
	}
}

func TestFullJitterClampsSample(t *testing.T) {
	for _, test := range []struct {
		sample float64
		want   time.Duration
	}{
		{sample: -1, want: 0},
		{sample: 0.5, want: 500 * time.Millisecond},
		{sample: 2, want: time.Second},
	} {
		if got := FullJitter(time.Second, test.sample); got != test.want {
			t.Errorf("FullJitter(1s, %v) = %s, want %s", test.sample, got, test.want)
		}
	}
}
