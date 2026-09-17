package kafka

import (
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// TestAssignmentWarningSkipsRetryTiers pins that a destination fed by the retry
// ladder is not warned about however few of its partitions this member holds,
// while an ordinary destination of the same member in the same assignment still
// is. A retry tier's slots describe a delay budget rather than a throughput
// ceiling, so its partition count is not the lever the warning names, and
// warning about it would point an operator at the wrong knob.
func TestAssignmentWarningSkipsRetryTiers(t *testing.T) {
	retry, topic := "orders.retry.1", "orders"
	consumer := &consumer{
		cfg: driver.ConsumerConfig{Delays: map[string]time.Duration{retry: time.Second}},
		budgets: map[string]int{
			retry: 4,
			topic: 4,
		},
		owned: map[partitionKey]bool{
			{destination: retry, partition: 0}: true,
			{destination: topic, partition: 0}: true,
		},
	}

	consumer.mu.Lock()
	warnings := consumer.assignmentWarningsLocked(map[string][]int32{retry: {0}, topic: {0}})
	consumer.mu.Unlock()

	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want one for the destination that is not a retry tier", warnings)
	}
	if warnings[0].destination != topic {
		t.Fatalf("warnings[0].destination = %q, want %q: the retry tier has a delay and must not warn", warnings[0].destination, topic)
	}
	if warnings[0].held != 1 || warnings[0].budget != 4 {
		t.Fatalf("warning = %+v, want held 1 of budget 4", warnings[0])
	}
}

// TestAssignmentWarningInTwoRounds pins what the warning does when one
// destination's assignment for a member arrives in two callbacks of the same
// rebalance, which is the shape a cooperative rebalance takes when the first
// round carries only the partitions no other member held.
//
// The decision is that the warning is decided on the callback that names the
// destination, on the member's accumulated owned count, and is not retracted:
// nothing a callback carries says whether another round follows it, and waiting
// for a round that may never come would not warn at all on the single-round
// rebalance that is the common case. The cost is that a member whose first
// callback is short and whose second fills its budget keeps a warning it does
// not deserve; this test is where that behaviour is recorded rather than
// discovered.
func TestAssignmentWarningInTwoRounds(t *testing.T) {
	topic := "orders"
	consumer := &consumer{
		cfg:     driver.ConsumerConfig{},
		budgets: map[string]int{topic: 3},
		owned:   map[partitionKey]bool{{destination: topic, partition: 0}: true},
	}

	consumer.mu.Lock()
	warnings := consumer.assignmentWarningsLocked(map[string][]int32{topic: {0}})
	consumer.mu.Unlock()
	if len(warnings) != 1 {
		t.Fatalf("first round warnings = %v, want one: the member holds one of the three partitions its budget calls for", warnings)
	}

	// The second round of the same rebalance hands over the two partitions the
	// other member was holding, so the member ends the rebalance with its
	// budget. The warning above is not taken back, and the second callback adds
	// none of its own.
	consumer.mu.Lock()
	consumer.owned[partitionKey{destination: topic, partition: 1}] = true
	consumer.owned[partitionKey{destination: topic, partition: 2}] = true
	later := consumer.assignmentWarningsLocked(map[string][]int32{topic: {1, 2}})
	consumer.mu.Unlock()
	if len(later) != 0 {
		t.Fatalf("second round warnings = %v, want none: the destination is already recorded as warned", later)
	}
	if held := consumer.ownedPartitionsLocked(topic); held != 3 {
		t.Fatalf("the member holds %d partitions after both rounds, want its budget of 3", held)
	}
}
