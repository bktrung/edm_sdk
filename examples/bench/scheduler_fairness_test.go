package bench_test

import (
	"context"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/bench"
)

const (
	fairnessMessages    = 3000
	fairnessConcurrency = 1
	fairnessHandlerWork = time.Millisecond
	fairnessTimeout     = 30 * time.Second
)

// BenchmarkSchedulerFairness measures settle latency by priority on the
// in-memory driver using the shipped high, medium, and low lane default. The
// medium lane is the one under test because smooth weighting is meant to keep
// it from waiting behind a burst of high-priority work. One handler slot makes
// scheduler ordering, rather than parallel handler capacity, the measured source
// of medium-lane wait.
func BenchmarkSchedulerFairness(b *testing.B) {
	priorities := []f1.Priority{f1.PriorityHigh, f1.PriorityMedium, f1.PriorityLow}
	for range b.N {
		h, err := bench.New(inmem.New(), bench.Config{
			Namespace:   "scheduler-fairness",
			Messages:    fairnessMessages,
			Publishers:  4,
			Concurrency: fairnessConcurrency,
			HandlerWork: fairnessHandlerWork,
			Priorities:  priorities,
			Timeout:     fairnessTimeout,
		})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		result, err := h.Consume(context.Background())
		b.StopTimer()
		if closeErr := h.Close(context.Background()); err == nil {
			err = closeErr
		}
		if err != nil {
			b.Fatal(err)
		}
		for _, priority := range priorities {
			b.Logf("priority=%s p50=%s p95=%s p99=%s", priority,
				result.SettlePercentileForPriority(priority, priorities, 50),
				result.SettlePercentileForPriority(priority, priorities, 95),
				result.SettlePercentileForPriority(priority, priorities, 99))
		}
		b.Logf("aggregate p50=%s p95=%s p99=%s",
			result.SettlePercentile(50),
			result.SettlePercentile(95),
			result.SettlePercentile(99))
	}
}
