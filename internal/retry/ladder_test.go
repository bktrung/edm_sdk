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

func TestDelayForCapsBeforeDurationOverflow(t *testing.T) {
	cfg := Config{InitialInterval: time.Second, Multiplier: 2, MaxInterval: 30 * time.Second}
	if got := cfg.DelayFor(1000); got != 30*time.Second {
		t.Fatalf("DelayFor(1000) = %s, want 30s", got)
	}
	cfg = Config{InitialInterval: time.Duration(1 << 62), Multiplier: 4, MaxInterval: time.Duration(1<<63 - 1)}
	if got := cfg.DelayFor(2); got != cfg.MaxInterval {
		t.Fatalf("overflow-clamped delay = %s, want %s", got, cfg.MaxInterval)
	}
}

func TestDelayForDecaysBelowMaximum(t *testing.T) {
	for _, test := range []struct {
		name    string
		initial time.Duration
		want    []time.Duration
	}{
		{name: "at cap", initial: 30 * time.Second, want: []time.Duration{30 * time.Second, 15 * time.Second, 7500 * time.Millisecond}},
		{name: "above cap", initial: 40 * time.Second, want: []time.Duration{30 * time.Second, 20 * time.Second, 10 * time.Second}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := Config{InitialInterval: test.initial, Multiplier: 0.5, MaxInterval: 30 * time.Second}
			for i, want := range test.want {
				if got := cfg.DelayFor(i + 1); got != want {
					t.Errorf("DelayFor(%d) = %s, want %s", i+1, got, want)
				}
			}
		})
	}
}

func TestDelayForDecayReachesZeroWithoutRebounding(t *testing.T) {
	for _, cap := range []time.Duration{0, 30 * time.Second} {
		cfg := Config{InitialInterval: time.Second, Multiplier: 0.5, MaxInterval: cap}
		previous := cfg.DelayFor(1)
		for attempt := 2; attempt <= 1000; attempt++ {
			got := cfg.DelayFor(attempt)
			if got < 0 || got > previous {
				t.Fatalf("cap %s: DelayFor(%d) = %s, previous %s", cap, attempt, got, previous)
			}
			previous = got
		}
		if previous != 0 {
			t.Fatalf("cap %s: final delay = %s, want zero", cap, previous)
		}
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
