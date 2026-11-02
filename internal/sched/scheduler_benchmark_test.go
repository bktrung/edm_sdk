package sched

import (
	"fmt"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func BenchmarkSchedulerNext(b *testing.B) {
	for _, test := range []struct {
		name           string
		promoteOverdue bool
		overdue        bool
	}{
		{name: "deadline-promotion-disabled"},
		{name: "deadline-promotion-enabled-no-overdue", promoteOverdue: true},
		{name: "deadline-promotion-enabled-all-overdue", promoteOverdue: true, overdue: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			scheduler, remaining := newBenchmarkScheduler(test.promoteOverdue, test.overdue)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if remaining == 0 {
					b.StopTimer()
					scheduler, remaining = newBenchmarkScheduler(test.promoteOverdue, test.overdue)
					b.StartTimer()
				}
				if _, _, ok := scheduler.Next(); !ok {
					b.Fatal("scheduler unexpectedly empty")
				}
				remaining--
			}
		})
	}
}

func newBenchmarkScheduler(promoteOverdue, overdue bool) (*Scheduler, int) {
	const (
		slotCount    = 60
		lanesPerSlot = 2
		itemsPerLane = 4
	)
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	specs := make([]LaneSpec, 0, slotCount*lanesPerSlot)
	for slot := range slotCount {
		for lane := range lanesPerSlot {
			specs = append(specs, LaneSpec{
				ID:       fmt.Sprintf("slot-%d-lane-%d", slot, lane),
				Group:    fmt.Sprintf("slot-%d", slot),
				Weight:   1,
				Budget:   time.Second,
				Capacity: itemsPerLane,
			})
		}
	}
	scheduler, err := New(specs, fake, promoteOverdue)
	if err != nil {
		panic(err)
	}
	for slot := range slotCount {
		for lane := range lanesPerSlot {
			id := fmt.Sprintf("slot-%d-lane-%d", slot, lane)
			for range itemsPerLane {
				if err := scheduler.Enqueue(id, Item{EnqueuedAt: start}); err != nil {
					panic(err)
				}
			}
		}
	}
	if overdue {
		fake.Advance(2 * time.Second)
	}
	return scheduler, slotCount * lanesPerSlot * itemsPerLane
}

// BenchmarkLaneFillDrain measures one lane filled to capacity and drained
// through the public API per op. A whole fill and drain is one op so a
// queue that reallocates as it refills costs at least one allocation per op
// instead of an amortized fraction that would round to zero.
func BenchmarkLaneFillDrain(b *testing.B) {
	const capacity = 8
	start := time.Unix(0, 0)
	scheduler, err := New([]LaneSpec{{ID: "lane", Weight: 1, Capacity: capacity}}, clock.NewFake(start), false)
	if err != nil {
		b.Fatal(err)
	}
	item := Item{EnqueuedAt: start}
	// One enqueue and pop offsets the head so every later fill crosses the end
	// of the buffer.
	if err := scheduler.Enqueue("lane", item); err != nil {
		b.Fatal(err)
	}
	if _, _, ok := scheduler.Next(); !ok {
		b.Fatal("scheduler unexpectedly empty")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for range capacity {
			if err := scheduler.Enqueue("lane", item); err != nil {
				b.Fatal(err)
			}
		}
		for range capacity {
			if _, _, ok := scheduler.Next(); !ok {
				b.Fatal("scheduler unexpectedly empty")
			}
		}
	}
}
