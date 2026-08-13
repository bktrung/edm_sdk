package sched

import (
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestDWRRShareMatchesWeights(t *testing.T) {
	scheduler, err := New([]LaneSpec{{ID: "high", Weight: 8, Capacity: 10001}, {ID: "normal", Weight: 4, Capacity: 10001}, {ID: "low", Weight: 1, Capacity: 10001}}, clock.NewFake(time.Unix(0, 0)), false)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		for _, id := range []string{"high", "normal", "low"} {
			if err := scheduler.Enqueue(id, Item{Value: id}); err != nil {
				t.Fatal(err)
			}
		}
	}
	counts := map[string]int{}
	for i := 0; i < 130; i++ {
		item, ok := scheduler.Next()
		if !ok {
			t.Fatal("scheduler unexpectedly empty")
		}
		counts[item.Value.(string)]++
	}
	if counts["high"] != 80 || counts["normal"] != 40 || counts["low"] != 10 {
		t.Fatalf("weighted prefix = %v, want high=80 normal=40 low=10", counts)
	}
}

func TestDWRRNoStarvationUnderSaturation(t *testing.T) {
	scheduler, err := New([]LaneSpec{{ID: "high", Weight: 8, Capacity: 1000}, {ID: "low", Weight: 1, Capacity: 1000}}, clock.NewFake(time.Unix(0, 0)), false)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		_ = scheduler.Enqueue("high", Item{Value: "high"})
		_ = scheduler.Enqueue("low", Item{Value: i})
	}
	seenLow := 0
	for {
		item, ok := scheduler.Next()
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

func TestDWRRDeficitResetOnEmpty(t *testing.T) {
	scheduler, err := New([]LaneSpec{{ID: "high", Weight: 8, Capacity: 100}, {ID: "low", Weight: 1, Capacity: 100}}, clock.NewFake(time.Unix(0, 0)), false)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if err := scheduler.Enqueue("high", Item{Value: "high"}); err != nil {
			t.Fatal(err)
		}
		if _, ok := scheduler.Next(); !ok {
			t.Fatal("high item not dispatched")
		}
	}
	if err := scheduler.Enqueue("low", Item{Value: "low"}); err != nil {
		t.Fatal(err)
	}
	item, ok := scheduler.Next()
	if !ok || item.Value != "low" {
		t.Fatalf("first returning lane item = %#v, %v", item.Value, ok)
	}
}

func TestAgingOverrunOrdering(t *testing.T) {
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
	item, ok := scheduler.Next()
	if !ok || item.Value != "first" {
		t.Fatalf("promoted item = %#v, %v", item.Value, ok)
	}
}

func TestAgingUsesLaneBudgetAndEnqueueTime(t *testing.T) {
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
	item, ok := scheduler.Next()
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
	if _, ok := scheduler.Next(); !ok || scheduler.Pending() != 0 {
		t.Fatal("queued item was not drained")
	}
}

func TestSchedulerRequiresClockAndHandlesEmptyNext(t *testing.T) {
	if _, err := New([]LaneSpec{{ID: "x", Weight: 1}}, nil, false); err == nil {
		t.Fatal("nil clock must fail")
	}
	var scheduler Scheduler
	if _, ok := scheduler.Next(); ok || scheduler.Pending() != 0 {
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
	for i := 0; i < 3; i++ {
		if err := scheduler.Enqueue("retry-1", Item{Value: "one"}); err != nil {
			t.Fatal(err)
		}
		if err := scheduler.Enqueue("retry-2", Item{Value: "two"}); err != nil {
			t.Fatal(err)
		}
	}
	for i, want := range []string{"one", "two", "one", "two"} {
		item, ok := scheduler.Next()
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
	if !empty.empty() || empty.head().Value != nil || empty.pop().Value != nil {
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
	if group.empty() || group.head().Value != "second" || group.pop().Value != "second" {
		t.Fatal("sparse slot helpers selected the wrong item")
	}
	var scheduler *Scheduler
	if scheduler.Pending() != 0 {
		t.Fatal("nil scheduler pending count must be zero")
	}
}

func TestAgingKeepsLowPriorityMovingUnderSustainedHighLoad(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	scheduler, err := New([]LaneSpec{
		{ID: "high", Weight: 8, Capacity: 32},
		{ID: "low", Weight: 1, Budget: 10 * time.Millisecond, Capacity: 2},
	}, fake, true)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		if err := scheduler.Enqueue("high", Item{Value: "high", EnqueuedAt: start}); err != nil {
			t.Fatal(err)
		}
	}
	if err := scheduler.Enqueue("low", Item{Value: "low", EnqueuedAt: start}); err != nil {
		t.Fatal(err)
	}
	fake.Advance(10 * time.Millisecond)

	item, ok := scheduler.Next()
	if !ok || item.Value != "low" {
		t.Fatalf("aged low-priority item = %#v, %v; want low", item.Value, ok)
	}
}
