// Package kafka contains Kafka-specific rules shared by the core and adapter.
package kafka

import "time"

// ExceedsKafkaDrainBound reports whether drainTimeout is strictly greater than
// 60% of groupTimeout. Zero and negative durations are evaluated by the same
// integer arithmetic rather than rejected. The function has no shared state and
// is safe for concurrent use.
func ExceedsKafkaDrainBound(drainTimeout, groupTimeout time.Duration) bool {
	// Divide before multiplying so an extreme duration cannot overflow int64.
	bound := groupTimeout/5*3 + (groupTimeout%5)*3/5
	return drainTimeout > bound
}
