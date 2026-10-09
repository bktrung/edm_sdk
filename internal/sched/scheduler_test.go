package sched

import (
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestSchedulerShareMatchesWeights(t *testing.T) {
	scheduler, err := New([]LaneSpec{{ID: "high", Weight: 8, Capacity: 10001}, {ID: "medium", Weight: 4, Capacity: 10001}, {ID: "low", Weight: 1, Capacity: 10001}}, clock.NewFake(time.Unix(0, 0)), false)
	if err != nil {
		t.Fatal(err)
	}
	for range 10000 {
		for _, id := range []string{"high", "medium", "low"} {
			if err := scheduler.Enqueue(id, Item{Value: id}); err != nil {
				t.Fatal(err)
			}
		}
	}
	counts := map[string]int{}
	for range 130 {
		item, _, ok := scheduler.Next()
		if !ok {
			t.Fatal("scheduler unexpectedly empty")
		}
		counts[item.Value.(string)]++
	}
	if counts["high"] != 80 || counts["medium"] != 40 || counts["low"] != 10 {
		t.Fatalf("weighted prefix = %v, want high=80 medium=40 low=10", counts)
	}
}

func TestSchedulerNoStarvationUnderSaturation(t *testing.T) {
	scheduler, err := New([]LaneSpec{{ID: "high", Weight: 8, Capacity: 1000}, {ID: "low", Weight: 1, Capacity: 1000}}, clock.NewFake(time.Unix(0, 0)), false)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		_ = scheduler.Enqueue("high", Item{Value: "high"})
		_ = scheduler.Enqueue("low", Item{Value: i})
	}
	seenLow := 0
	for {
		item, _, ok := scheduler.Next()
		if !ok {
			break
		}
		if _, ok := item.Value.(int); ok {
			seenLow++
		}
	}
	if seenLow != 100 {
		t.Fatalf("low messages dispatched = %d, want 100", seenLow)
	}
}

func TestSchedulerDeficitResetOnEmpty(t *testing.T) {
	scheduler, err := New([]LaneSpec{{ID: "high", Weight: 8, Capacity: 100}, {ID: "low", Weight: 1, Capacity: 100}}, clock.NewFake(time.Unix(0, 0)), false)
	if err != nil {
		t.Fatal(err)
	}
	for range 8 {
		if err := scheduler.Enqueue("high", Item{Value: "high"}); err != nil {
			t.Fatal(err)
		}
		if _, _, ok := scheduler.Next(); !ok {
			t.Fatal("high item not dispatched")
		}
	}
	if err := scheduler.Enqueue("low", Item{Value: "low"}); err != nil {
		t.Fatal(err)
	}
	item, _, ok := scheduler.Next()
	if !ok || item.Value != "low" {
		t.Fatalf("first returning lane item = %#v, %v", item.Value, ok)
	}
}

func TestDeadlinePromotionOverrunOrdering(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	scheduler, err := New([]LaneSpec{{ID: "first", Weight: 8, Budget: time.Second, Capacity: 10}, {ID: "second", Weight: 1, Budget: time.Second, Capacity: 10}}, fake, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Enqueue("first", Item{Value: "first", EnqueuedAt: start}); err != nil {
		t.Fatal(err)
	}
	fake.Advance(2 * time.Second)
	if err := scheduler.Enqueue("second", Item{Value: "second", EnqueuedAt: start.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	item, _, ok := scheduler.Next()
	if !ok || item.Value != "first" {
		t.Fatalf("promoted item = %#v, %v", item.Value, ok)
	}
}

func TestDeadlinePromotionTieUsesConfiguredSlotOrder(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	scheduler, err := New([]LaneSpec{
		{ID: "first", Weight: 1, Budget: time.Second, Capacity: 1},
		{ID: "second", Weight: 1, Budget: time.Second, Capacity: 1},
	}, fake, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"first", "second"} {
		if err := scheduler.Enqueue(id, Item{Value: id, EnqueuedAt: start}); err != nil {
			t.Fatal(err)
		}
	}
	fake.Advance(2 * time.Second)
	item, _, ok := scheduler.Next()
	if !ok || item.Value != "first" {
		t.Fatalf("equal slot overrun selected %#v, %v; want first", item.Value, ok)
	}
}

func TestDeadlinePromotionTieUsesConfiguredLaneOrder(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	scheduler, err := New([]LaneSpec{
		{ID: "first", Group: "retry", Weight: 1, Budget: time.Second, Capacity: 1},
		{ID: "second", Group: "retry", Weight: 1, Budget: time.Second, Capacity: 1},
	}, fake, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"first", "second"} {
		if err := scheduler.Enqueue(id, Item{Value: id, EnqueuedAt: start}); err != nil {
			t.Fatal(err)
		}
	}
	fake.Advance(2 * time.Second)
	item, _, ok := scheduler.Next()
	if !ok || item.Value != "first" {
		t.Fatalf("equal lane overrun selected %#v, %v; want first", item.Value, ok)
	}
}

func TestDeadlinePromotionFallsBackToWeightedSelectionWhenNothingIsOverdue(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	scheduler, err := New([]LaneSpec{
		{ID: "high", Weight: 2, Budget: time.Hour, Capacity: 3},
		{ID: "low", Weight: 1, Budget: time.Hour, Capacity: 3},
	}, fake, true)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := scheduler.Enqueue("high", Item{Value: "high", EnqueuedAt: start}); err != nil {
			t.Fatal(err)
		}
		if err := scheduler.Enqueue("low", Item{Value: "low", EnqueuedAt: start}); err != nil {
			t.Fatal(err)
		}
	}
	// Smooth weighted round robin interleaves 2:1 as high, low, high; pick four distinguishes it from 1:1.
	for i, want := range []string{"high", "low", "high", "high"} {
		item, _, ok := scheduler.Next()
		if !ok || item.Value != want {
			t.Fatalf("fallback item %d = %#v, %v; want %s", i, item.Value, ok, want)
		}
	}
}

func TestDeadlinePromotionUsesLaneBudgetAndEnqueueTime(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	scheduler, err := New([]LaneSpec{
		{ID: "old", Group: "retry", Weight: 1, Budget: 2 * time.Second, Capacity: 2},
		{ID: "young", Group: "retry", Weight: 1, Budget: 5 * time.Second, Capacity: 2},
		{ID: "main", Weight: 1, Budget: 2 * time.Second, Capacity: 2},
	}, fake, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Enqueue("old", Item{Value: "old", EnqueuedAt: start}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Enqueue("young", Item{Value: "young", EnqueuedAt: start.Add(4 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Enqueue("main", Item{Value: "main", EnqueuedAt: start}); err != nil {
		t.Fatal(err)
	}
	fake.Advance(4 * time.Second)
	item, _, ok := scheduler.Next()
	if !ok || item.Value != "old" {
		t.Fatalf("most overdue item = %#v, %v; want old", item.Value, ok)
	}
}

func TestSchedulerValidationDepthPendingAndCapacity(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	if _, err := New(nil, fake, false); err == nil {
		t.Fatal("empty specs must fail")
	}
	if _, err := New([]LaneSpec{{ID: "", Weight: 1}}, fake, false); err == nil {
		t.Fatal("empty lane id must fail")
	}
	if _, err := New([]LaneSpec{{ID: "x", Weight: 0}}, fake, false); err == nil {
		t.Fatal("zero lane weight must fail")
	}
	if _, err := New([]LaneSpec{{ID: "x", Weight: 1}, {ID: "x", Weight: 1}}, fake, false); err == nil {
		t.Fatal("duplicate lane id must fail")
	}
	scheduler, err := New([]LaneSpec{{ID: "x", Weight: 1, Capacity: 1}}, fake, false)
	if err != nil {
		t.Fatal(err)
	}
	if scheduler.Depth("unknown") != 0 || scheduler.Pending() != 0 {
		t.Fatal("empty scheduler depth is incorrect")
	}
	if err := scheduler.Enqueue("unknown", Item{}); err == nil {
		t.Fatal("unknown lane must fail")
	}
	if err := scheduler.Enqueue("x", Item{Value: "first"}); err != nil {
		t.Fatal(err)
	}
	if scheduler.Depth("x") != 1 || scheduler.Pending() != 1 {
		t.Fatal("queued depth is incorrect")
	}
	if err := scheduler.Enqueue("x", Item{}); err == nil {
		t.Fatal("full lane must fail")
	}
	if _, _, ok := scheduler.Next(); !ok || scheduler.Pending() != 0 {
		t.Fatal("queued item was not drained")
	}
}

func TestSchedulerRequiresClockAndHandlesEmptyNext(t *testing.T) {
	if _, err := New([]LaneSpec{{ID: "x", Weight: 1}}, nil, false); err == nil {
		t.Fatal("nil clock must fail")
	}
	var scheduler Scheduler
	if _, _, ok := scheduler.Next(); ok || scheduler.Pending() != 0 {
		t.Fatal("zero scheduler must be empty")
	}
}

func TestGroupedLanesShareOneWeightedSlot(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	scheduler, err := New([]LaneSpec{
		{ID: "retry-1", Group: "retry", Weight: 2, Capacity: 10},
		{ID: "retry-2", Group: "retry", Weight: 2, Capacity: 10},
	}, fake, false)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := scheduler.Enqueue("retry-1", Item{Value: "one"}); err != nil {
			t.Fatal(err)
		}
		if err := scheduler.Enqueue("retry-2", Item{Value: "two"}); err != nil {
			t.Fatal(err)
		}
	}
	for i, want := range []string{"one", "two", "one", "two"} {
		item, _, ok := scheduler.Next()
		if !ok || item.Value != want {
			t.Fatalf("grouped item %d = %#v, %v; want %s", i, item.Value, ok, want)
		}
	}
}

func TestGroupedLanesRequireMatchingWeights(t *testing.T) {
	if _, err := New([]LaneSpec{{ID: "one", Group: "retry", Weight: 1}, {ID: "two", Group: "retry", Weight: 2}}, clock.NewFake(time.Unix(0, 0)), false); err == nil {
		t.Fatal("grouped lanes with different weights must fail")
	}
}

func TestSlotHelpersHandleEmptyAndSparseGroups(t *testing.T) {
	var empty slot
	if !empty.empty() || empty.pop().Value != nil {
		t.Fatal("empty slot helpers returned work")
	}
	first, err := newLane(LaneSpec{ID: "first", Weight: 1, Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := newLane(LaneSpec{ID: "second", Weight: 1, Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.enqueue(Item{Value: "second"}); err != nil {
		t.Fatal(err)
	}
	group := slot{lanes: []*lane{first, second}}
	if group.empty() || group.pop().Value != "second" {
		t.Fatal("sparse slot helpers selected the wrong item")
	}
	var scheduler *Scheduler
	if scheduler.Pending() != 0 {
		t.Fatal("nil scheduler pending count must be zero")
	}
}

func TestDeadlinePromotionKeepsLowPriorityMovingUnderSustainedHighLoad(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	scheduler, err := New([]LaneSpec{
		{ID: "high", Weight: 8, Capacity: 32},
		{ID: "low", Weight: 1, Budget: 10 * time.Millisecond, Capacity: 2},
	}, fake, true)
	if err != nil {
		t.Fatal(err)
	}
	for range 16 {
		if err := scheduler.Enqueue("high", Item{Value: "high", EnqueuedAt: start}); err != nil {
			t.Fatal(err)
		}
	}
	if err := scheduler.Enqueue("low", Item{Value: "low", EnqueuedAt: start}); err != nil {
		t.Fatal(err)
	}
	fake.Advance(10 * time.Millisecond)

	item, _, ok := scheduler.Next()
	if !ok || item.Value != "low" {
		t.Fatalf("deadline-promoted low-priority item = %#v, %v; want low", item.Value, ok)
	}
}

func TestNextReportsPromotionLaneAndPrePopDepth(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	scheduler, err := New([]LaneSpec{
		{ID: "overdue", Weight: 1, Budget: time.Second, Capacity: 10},
		{ID: "fresh", Weight: 1, Budget: time.Hour, Capacity: 10},
	}, fake, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Enqueue("overdue", Item{Value: "first", EnqueuedAt: start}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Enqueue("overdue", Item{Value: "second", EnqueuedAt: start}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Enqueue("fresh", Item{Value: "fresh", EnqueuedAt: start}); err != nil {
		t.Fatal(err)
	}
	fake.Advance(2 * time.Second)
	item, prom, ok := scheduler.Next()
	if !ok || item.Value != "first" {
		t.Fatalf("promoted item = %#v, %v; want first", item.Value, ok)
	}
	if prom.LaneID != "overdue" {
		t.Fatalf("promotion lane = %q, want overdue", prom.LaneID)
	}
	if prom.Depth != 2 {
		t.Fatalf("promotion depth = %d, want 2", prom.Depth)
	}
}

func TestNextReturnsZeroPromotionForRoundRobinPick(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	scheduler, err := New([]LaneSpec{
		{ID: "high", Weight: 2, Budget: time.Hour, Capacity: 3},
		{ID: "low", Weight: 1, Budget: time.Hour, Capacity: 3},
	}, fake, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Enqueue("high", Item{Value: "high", EnqueuedAt: start}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Enqueue("low", Item{Value: "low", EnqueuedAt: start}); err != nil {
		t.Fatal(err)
	}
	_, prom, ok := scheduler.Next()
	if !ok {
		t.Fatal("scheduler unexpectedly empty")
	}
	if prom != (Promotion{}) {
		t.Fatalf("round-robin promotion = %+v, want zero", prom)
	}
}

func TestNextReturnsZeroPromotionWhenDisabled(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	scheduler, err := New([]LaneSpec{
		{ID: "first", Weight: 1, Budget: time.Second, Capacity: 2},
		{ID: "second", Weight: 1, Budget: time.Second, Capacity: 2},
	}, fake, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Enqueue("first", Item{Value: "first", EnqueuedAt: start}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Enqueue("second", Item{Value: "second", EnqueuedAt: start}); err != nil {
		t.Fatal(err)
	}
	fake.Advance(2 * time.Second)
	_, prom, ok := scheduler.Next()
	if !ok {
		t.Fatal("scheduler unexpectedly empty")
	}
	if prom != (Promotion{}) {
		t.Fatalf("disabled promotion = %+v, want zero", prom)
	}
}
