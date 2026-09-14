package f1

import (
	"context"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// reportedLimits connects a client to a fake driver declaring caps and returns
// the feature report that connection publishes.
func reportedLimits(t *testing.T, caps driver.Capabilities, options ...Option) Limits {
	t.Helper()
	fakeDriver := &testDriver{conn: &testConn{caps: caps, info: driver.BrokerInfo{Kind: "test", Version: "1"}}}
	client, err := New(context.Background(), testClientConfig(t), append([]Option{WithDriver(fakeDriver)}, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(context.Background()); err != nil {
			t.Errorf("close client: %v", err)
		}
	})
	return client.Limits()
}

func TestDelayAccuracyDetail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		accuracy driver.DelayAccuracy
		want     []string
		deny     []string
	}{
		{
			name:     "undeclared",
			accuracy: driver.DelayAccuracy{},
			want:     []string{"not declared"},
			deny:     []string{"0s"},
		},
		{
			name:     "floor and relative bound",
			accuracy: driver.DelayAccuracy{Floor: 500 * time.Millisecond, Relative: 1, MaxDelay: 64 * time.Second},
			want:     []string{"500ms", "the requested delay", "1m4s"},
		},
		{
			name:     "floor alone",
			accuracy: driver.DelayAccuracy{Floor: 2 * time.Second, MaxDelay: time.Minute},
			want:     []string{"2s", "1m"},
			deny:     []string{"requested delay"},
		},
		{
			name:     "relative alone",
			accuracy: driver.DelayAccuracy{Relative: 0.5, MaxDelay: 64 * time.Second},
			want:     []string{"0.5 times the requested delay", "1m4s"},
		},
		{
			name:     "at the due time for every delay",
			accuracy: driver.DelayAccuracy{MaxDelay: maxDelayDuration},
			want:     []string{"delivered at its due time", "any requested delay"},
			deny:     []string{"late by"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := delayAccuracyDetail(test.accuracy)
			for _, want := range test.want {
				if !strings.Contains(got, want) {
					t.Errorf("detail %q does not mention %q", got, want)
				}
			}
			for _, deny := range test.deny {
				if strings.Contains(got, deny) {
					t.Errorf("detail %q says %q", got, deny)
				}
			}
		})
	}
}

func TestLimitsReportsDriverDelayAccuracy(t *testing.T) {
	t.Parallel()

	caps := driver.Capabilities{
		DelayAccuracy: driver.DelayAccuracy{Floor: 500 * time.Millisecond, Relative: 1, MaxDelay: 64 * time.Second},
	}
	got := featureStatus(reportedLimits(t, caps), "native_delay")
	if got.Mode != FeatureEmulated {
		t.Errorf("native_delay mode = %v, want emulated", got.Mode)
	}
	// The report has to carry the relative term and the ceiling, not one worst
	// case: a caller deferring 700ms and a caller deferring a minute read
	// different bounds out of the same declaration.
	for _, want := range []string{"500ms", "the requested delay", "1m4s"} {
		if !strings.Contains(got.Detail, want) {
			t.Errorf("native_delay detail %q does not mention %q", got.Detail, want)
		}
	}
}

func TestLimitsRendersUndeclaredDelayAccuracy(t *testing.T) {
	t.Parallel()

	// A driver that never sets the field declares nothing. Rendering the zero
	// value as a duration would read as a promise to deliver at the due time
	// exactly, which is the strongest claim the shape can make.
	got := featureStatus(reportedLimits(t, driver.Capabilities{}), "native_delay")
	if got.Detail == "" {
		t.Fatal("native_delay reported no detail for an undeclared accuracy")
	}
	if strings.Contains(got.Detail, "0s") {
		t.Fatalf("native_delay detail = %q, want an undeclared accuracy rather than a duration", got.Detail)
	}
	if !strings.Contains(got.Detail, "not declared") {
		t.Fatalf("native_delay detail = %q, want it to say the accuracy is undeclared", got.Detail)
	}
}

func TestStrictPortabilityWithdrawsDelayAccuracy(t *testing.T) {
	t.Parallel()

	// Under strict the core emulates the delay, so the driver's bound is not
	// the one in force and reporting it would have an application plan against
	// a number the core is not delivering.
	caps := driver.Capabilities{
		NativeDelay:     true,
		ConsumerScaling: driver.ScalingFree,
		DelayAccuracy:   driver.DelayAccuracy{Floor: 500 * time.Millisecond, Relative: 1, MaxDelay: 64 * time.Second},
	}
	got := featureStatus(reportedLimits(t, caps, WithStrictPortability()), "native_delay")
	if !strings.Contains(got.Detail, "not declared") {
		t.Fatalf("strict native_delay detail = %q, want the driver accuracy withdrawn", got.Detail)
	}
}

func TestCapabilitiesEqualityCountsDelayAccuracy(t *testing.T) {
	t.Parallel()

	// The driver consumers read Effective == Capabilities{} as "the caller
	// supplied no profile". A declared accuracy is part of a profile, so it
	// takes part in that comparison instead of being invisible to it.
	declared := driver.Capabilities{
		DelayAccuracy: driver.DelayAccuracy{Floor: 500 * time.Millisecond, Relative: 1, MaxDelay: 64 * time.Second},
	}
	if declared == (driver.Capabilities{}) {
		t.Fatal("a capabilities value carrying a declared delay accuracy compares equal to the zero value")
	}
}
