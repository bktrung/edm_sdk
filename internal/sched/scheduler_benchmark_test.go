package sched

import (
	"fmt"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func BenchmarkSchedulerNext(b *testing.B) {
	for _, test := range []struct {
		name    string
		aging   bool
		overdue bool
	}{
		{name: "aging-disabled"},
		{name: "aging-enabled-no-overdue", aging: true},
		{name: "aging-enabled-all-overdue", aging: true, overdue: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			scheduler, remaining := newBenchmarkScheduler(test.aging, test.overdue)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if remaining == 0 {
					b.StopTimer()
					scheduler, remaining = newBenchmarkScheduler(test.aging, test.overdue)
					b.StartTimer()
				}
				if _, ok := scheduler.Next(); !ok {
					b.Fatal("scheduler unexpectedly empty")
				}
				remaining--
			}
		})
	}
}

func newBenchmarkScheduler(aging, overdue bool) (*Scheduler, int) {
	const (
		slotCount    = 60
		lanesPerSlot = 2
		itemsPerLane = 4
	)
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	specs := make([]LaneSpec, 0, slotCount*lanesPerSlot)
	for slot := 0; slot < slotCount; slot++ {
		for lane := 0; lane < lanesPerSlot; lane++ {
			specs = append(specs, LaneSpec{
				ID:       fmt.Sprintf("slot-%d-lane-%d", slot, lane),
				Group:    fmt.Sprintf("slot-%d", slot),
				Weight:   1,
				Budget:   time.Second,
				Capacity: itemsPerLane,
			})
		}
	}
	scheduler, err := New(specs, fake, aging)
	if err != nil {
		panic(err)
	}
	for slot := 0; slot < slotCount; slot++ {
		for lane := 0; lane < lanesPerSlot; lane++ {
			id := fmt.Sprintf("slot-%d-lane-%d", slot, lane)
			for item := 0; item < itemsPerLane; item++ {
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
