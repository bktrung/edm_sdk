package kafka

import (
	"testing"
	"time"
)

func TestExceedsKafkaDrainBoundPinsBoundary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		groupTimeout time.Duration
		bound        time.Duration
	}{
		{
			name:         "duration divisible by five nanoseconds",
			groupTimeout: 17 * time.Second,
			bound:        10*time.Second + 200*time.Millisecond,
		},
		{
			// 17s+2ns has a 2ns remainder; (2ns*3)/5 contributes 1ns.
			name:         "duration with remainder",
			groupTimeout: 17*time.Second + 2*time.Nanosecond,
			bound:        10*time.Second + 200*time.Millisecond + time.Nanosecond,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if ExceedsKafkaDrainBound(tc.bound, tc.groupTimeout) {
				t.Fatalf("ExceedsKafkaDrainBound(%s, %s) = true at the bound", tc.bound, tc.groupTimeout)
			}
			if !ExceedsKafkaDrainBound(tc.bound+time.Nanosecond, tc.groupTimeout) {
				t.Fatalf("ExceedsKafkaDrainBound(%s, %s) = false above the bound", tc.bound+time.Nanosecond, tc.groupTimeout)
			}
		})
	}
}
