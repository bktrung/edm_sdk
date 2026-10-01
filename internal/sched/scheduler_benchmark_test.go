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
