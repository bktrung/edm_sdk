package sched

import (
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestSmoothWeightedShareAndGap(t *testing.T) {
	scheduler, err := New([]LaneSpec{
		{ID: "high", Weight: 4, Capacity: 70},
		{ID: "medium", Weight: 2, Capacity: 70},
		{ID: "low", Weight: 1, Capacity: 70},
	}, clock.NewFake(time.Unix(0, 0)), false)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"high", "medium", "low"} {
		for range 70 {
			if err := scheduler.Enqueue(id, Item{Value: id}); err != nil {
				t.Fatal(err)
			}
		}
	}

	counts := map[string]int{}
	positions := map[string][]int{}
	sequence := make([]string, 0, 70)
	for pick := range 70 {
		item, _, ok := scheduler.Next()
		if !ok {
			t.Fatal("scheduler unexpectedly empty")
		}
		id := item.Value.(string)
		counts[id]++
		positions[id] = append(positions[id], pick)
		sequence = append(sequence, id)
	}
	for pick, want := range []string{"high", "medium", "high", "low", "high", "medium", "high"} {
		if sequence[pick] != want {
			t.Fatalf("smooth prefix item %d = %s; want %s", pick, sequence[pick], want)
		}
	}
	if counts["high"] != 40 || counts["medium"] != 20 || counts["low"] != 10 {
		t.Fatalf("weighted prefix = %v, want high=40 medium=20 low=10", counts)
	}
	for _, test := range []struct {
		id     string
		maxGap int
	}{
		{id: "high", maxGap: 2},
		{id: "medium", maxGap: 4},
		{id: "low", maxGap: 7},
	} {
		for index := 1; index < len(positions[test.id]); index++ {
			gap := positions[test.id][index] - positions[test.id][index-1]
			if gap > test.maxGap {
				t.Fatalf("%s gap = %d at pick %d, want <= %d", test.id, gap, positions[test.id][index], test.maxGap)
			}
		}
	}
}

func TestSmoothWeightedFourTopicContentionWindowSharesAndUtilization(t *testing.T) {
	const (
		runs   = 5
		topics = 4
		picks  = 130
	)
	priorities := []struct {
		name   string
		weight int
	}{
		{name: "high", weight: 8},
		{name: "medium", weight: 4},
		{name: "low", weight: 1},
	}
	want := map[string]int{"high": 80, "medium": 40, "low": 10}

	for run := range runs {
		start := time.Unix(0, 0)
		specs := make([]LaneSpec, 0, topics*len(priorities))
		laneIDs := make([]string, 0, topics*len(priorities))
		for topic := range topics {
			for _, priority := range priorities {
				id := priority.name + "-" + string(rune('0'+topic))
				specs = append(specs, LaneSpec{
					ID:       id,
					Group:    id,
					Weight:   priority.weight,
					Budget:   time.Hour,
					Capacity: picks,
				})
				laneIDs = append(laneIDs, id)
			}
		}
		scheduler, err := New(specs, clock.NewFake(start), true)
		if err != nil {
			t.Fatal(err)
		}

		for topic := range topics {
			for _, priority := range priorities {
				id := priority.name + "-" + string(rune('0'+topic))
				for range picks {
					if err := scheduler.Enqueue(id, Item{Value: priority.name, EnqueuedAt: start}); err != nil {
						t.Fatal(err)
					}
				}
			}
		}

		windowPicks := 0
		counts := make(map[string]int, len(want))
		for pick := range picks {
			allLanesContend := true
			for _, id := range laneIDs {
				if scheduler.Depth(id) == 0 {
					allLanesContend = false
					break
				}
			}
			if allLanesContend {
				windowPicks++
			}
			item, _, ok := scheduler.Next()
			if !ok {
				t.Fatalf("run %d pick %d: scheduler unexpectedly empty", run, pick)
			}
			counts[item.Value.(string)]++
		}
		utilization := float64(windowPicks) / float64(picks)
		if utilization != 1 {
			t.Fatalf("run %d contention utilization = %.4f, want 1", run, utilization)
		}
		for priority, expected := range want {
			if counts[priority] != expected {
				t.Fatalf("run %d %s contention share = %d/%d, want %d/%d", run, priority, counts[priority], picks, expected, picks)
			}
		}
	}
}

func TestSmoothWeightedDeficitResetsAfterEmptyLane(t *testing.T) {
	scheduler, err := New([]LaneSpec{
		{ID: "high", Weight: 4, Capacity: 10},
		{ID: "low", Weight: 1, Capacity: 10},
	}, clock.NewFake(time.Unix(0, 0)), false)
	if err != nil {
		t.Fatal(err)
	}
	for range 10 {
		if err := scheduler.Enqueue("high", Item{Value: "high"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := scheduler.Enqueue("low", Item{Value: "low"}); err != nil {
		t.Fatal(err)
	}
	for pick, want := range []string{"high", "high", "low"} {
		item, _, ok := scheduler.Next()
		if !ok || item.Value != want {
			t.Fatalf("initial item %d = %#v, %v; want %s", pick, item.Value, ok, want)
		}
	}
	item, _, ok := scheduler.Next()
	if !ok || item.Value != "high" {
		t.Fatalf("empty-lane reset item = %#v, %v; want high", item.Value, ok)
	}
	if err := scheduler.Enqueue("low", Item{Value: "low"}); err != nil {
		t.Fatal(err)
	}
	for pick, want := range []string{"high", "high", "high", "low"} {
		item, _, ok := scheduler.Next()
		if !ok || item.Value != want {
			t.Fatalf("refilled item %d = %#v, %v; want %s", pick, item.Value, ok, want)
		}
	}
}

func TestSmoothWeightedDeficitResetSeparatesZeroFromOne(t *testing.T) {
	scheduler, err := New([]LaneSpec{
		{ID: "high", Weight: 5, Capacity: 10},
		{ID: "low", Weight: 1, Capacity: 10},
	}, clock.NewFake(time.Unix(0, 0)), false)
	if err != nil {
		t.Fatal(err)
	}
	for range 8 {
		if err := scheduler.Enqueue("high", Item{Value: "high"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := scheduler.Enqueue("low", Item{Value: "low"}); err != nil {
		t.Fatal(err)
	}
	for pick, want := range []string{"high", "high", "high", "low"} {
		item, _, ok := scheduler.Next()
		if !ok || item.Value != want {
			t.Fatalf("initial item %d = %#v, %v; want %s", pick, item.Value, ok, want)
		}
	}
	item, _, ok := scheduler.Next()
	if !ok || item.Value != "high" {
		t.Fatalf("empty-lane reset item = %#v, %v; want high", item.Value, ok)
	}
	if err := scheduler.Enqueue("low", Item{Value: "low"}); err != nil {
		t.Fatal(err)
	}
	for pick, want := range []string{"high", "high", "high", "high", "low"} {
		item, _, ok := scheduler.Next()
		if !ok || item.Value != want {
			t.Fatalf("refilled item %d = %#v, %v; want %s", pick, item.Value, ok, want)
		}
	}
}

func TestSmoothWeightedDeficitResetsAfterDeadlinePromotion(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	scheduler, err := New([]LaneSpec{
		{ID: "high", Weight: 5, Budget: time.Second, Capacity: 10},
		{ID: "low", Weight: 1, Capacity: 10},
	}, fake, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Enqueue("high", Item{Value: "high", EnqueuedAt: start}); err != nil {
		t.Fatal(err)
	}
	fake.Advance(2 * time.Second)
	item, promotion, ok := scheduler.Next()
	if !ok || item.Value != "high" || promotion.LaneID != "high" {
		t.Fatalf("promoted item = %#v, promotion = %#v, %v; want high promotion", item.Value, promotion, ok)
	}
	now := fake.Now()
	for _, id := range []string{"high", "low"} {
		for range 10 {
			if err := scheduler.Enqueue(id, Item{Value: id, EnqueuedAt: now}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for pick, want := range []string{"high", "high", "high", "low", "high", "high", "high"} {
		item, promotion, ok := scheduler.Next()
		if !ok || item.Value != want || promotion != (Promotion{}) {
			t.Fatalf("weighted item %d = %#v, promotion = %#v, %v; want %s", pick, item.Value, promotion, ok, want)
		}
	}
}

func TestSmoothWeightedDeadlinePromotionRemainsFirst(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	scheduler, err := New([]LaneSpec{
		{ID: "high", Weight: 4, Budget: time.Hour, Capacity: 10},
		{ID: "low", Weight: 1, Budget: time.Hour, Capacity: 10},
		{ID: "urgent", Weight: 1, Budget: time.Second, Capacity: 1},
	}, fake, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"high", "low"} {
		for range 10 {
			if err := scheduler.Enqueue(id, Item{Value: id, EnqueuedAt: start}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for pick, want := range []string{"high", "high", "low"} {
		item, _, ok := scheduler.Next()
		if !ok || item.Value != want {
			t.Fatalf("fallback item %d = %#v, %v; want %s", pick, item.Value, ok, want)
		}
	}
	fake.Advance(2 * time.Second)
	if err := scheduler.Enqueue("urgent", Item{Value: "urgent", EnqueuedAt: start}); err != nil {
		t.Fatal(err)
	}
	item, _, ok := scheduler.Next()
	if !ok || item.Value != "urgent" {
		t.Fatalf("promoted item = %#v, %v; want urgent", item.Value, ok)
	}
}

func TestSmoothWeightedTieUsesLowestSlot(t *testing.T) {
	scheduler, err := New([]LaneSpec{
		{ID: "first", Weight: 1, Capacity: 3},
		{ID: "second", Weight: 1, Capacity: 3},
	}, clock.NewFake(time.Unix(0, 0)), false)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"first", "second"} {
		for range 3 {
			if err := scheduler.Enqueue(id, Item{Value: id}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for pick, want := range []string{"first", "second", "first"} {
		item, _, ok := scheduler.Next()
		if !ok || item.Value != want {
			t.Fatalf("tie item %d = %#v, %v; want %s", pick, item.Value, ok, want)
		}
	}
}
