package obs

import (
	"sync"
	"testing"
)

func TestSampledCounterAccumulatesAndResets(t *testing.T) {
	var counter SampledCounter
	counter.Add(2)
	counter.Add(3)
	if got := counter.Sample(); got != 5 {
		t.Fatalf("Sample() = %d, want 5", got)
	}
	if got := counter.Sample(); got != 0 {
		t.Fatalf("second Sample() = %d, want 0", got)
	}
}

func TestSampledGaugeKeepsHighWaterAndResets(t *testing.T) {
	var gauge SampledGauge
	gauge.Observe(4)
	gauge.Observe(2)
	gauge.Observe(9)
	if got := gauge.Sample(); got != 9 {
		t.Fatalf("Sample() = %d, want 9", got)
	}
	if got := gauge.Sample(); got != 0 {
		t.Fatalf("second Sample() = %d, want 0", got)
	}
}

func TestSampledValuesAreConcurrent(t *testing.T) {
	const workers = 32
	const observations = 128
	var counter SampledCounter
	var gauge SampledGauge
	var group sync.WaitGroup
	group.Add(workers)
	for worker := 1; worker <= workers; worker++ {
		go func(value uint64) {
			defer group.Done()
			for i := 0; i < observations; i++ {
				counter.Add(1)
				gauge.Observe(value)
			}
		}(uint64(worker))
	}
	group.Wait()
	if got, want := counter.Sample(), uint64(workers*observations); got != want {
		t.Fatalf("counter Sample() = %d, want %d", got, want)
	}
	if got := gauge.Sample(); got != workers {
		t.Fatalf("gauge Sample() = %d, want %d", got, workers)
	}
}

func TestSampledHotPathOperationsDoNotAllocate(t *testing.T) {
	var counter SampledCounter
	if allocs := testing.AllocsPerRun(1000, func() { counter.Add(1) }); allocs != 0 {
		t.Fatalf("counter Add allocations = %v, want 0", allocs)
	}
	var gauge SampledGauge
	if allocs := testing.AllocsPerRun(1000, func() {
		gauge.Observe(1)
		gauge.Sample()
	}); allocs != 0 {
		t.Fatalf("gauge Observe allocations = %v, want 0", allocs)
	}
}

func TestSampledGaugeSetSeparatesNamesAndLanes(t *testing.T) {
	set := NewSampledGaugeSet([]string{"payments", "orders", "orders"}, 3)
	set.Observe("orders", 1, 4)
	set.Observe("orders", 2, 2)
	set.Observe("payments", 0, 7)

	if got := set.Sample("orders", 1); got != 4 {
		t.Fatalf("orders lane 1 = %d, want 4", got)
	}
	if got := set.Sample("orders", 2); got != 2 {
		t.Fatalf("orders lane 2 = %d, want 2", got)
	}
	if got := set.Sample("payments", 0); got != 7 {
		t.Fatalf("payments lane 0 = %d, want 7", got)
	}
}

func TestSampledGaugeSetObservationDoesNotAllocate(t *testing.T) {
	set := NewSampledGaugeSet([]string{"orders"}, 3)
	if allocs := testing.AllocsPerRun(1000, func() {
		set.Observe("orders", 1, 4)
	}); allocs != 0 {
		t.Fatalf("gauge set observation allocations = %v, want 0", allocs)
	}
}

func TestSampledCounterLoadDoesNotReset(t *testing.T) {
	var counter SampledCounter
	counter.Add(3)
	if got := counter.Load(); got != 3 {
		t.Fatalf("Load() = %d, want 3", got)
	}
	counter.Add(2)
	if got := counter.Load(); got != 5 {
		t.Fatalf("Load() after Add = %d, want 5", got)
	}
	if got := counter.Sample(); got != 5 {
		t.Fatalf("Sample() = %d, want 5", got)
	}
}
