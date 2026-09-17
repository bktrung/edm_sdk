//go:build integration

package bench_test

import (
	"context"
	"fmt"
	"syscall"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/rabbitmq"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/bench"
)

var (
	// sweepConcurrencies are the handler concurrencies a sweep measures, and
	// sweepPrefetches are the in-flight budgets it measures each of them at.
	// The two are the settings a caller sizes a subscription with, and the grid
	// is the shape of the answer: a budget only buys throughput up to what the
	// handlers can settle, and handlers only help up to what the budget lets
	// the consumer hold ahead of them.
	sweepConcurrencies = []int{1, 4, 16, 32, 64}
	sweepPrefetches    = []int{1, 16, 64, 100, 300}
)

// refusedPrefetchFloor is the smallest in-flight budget this shape can ask for:
// the core refuses one below its lane count, and one topic on one priority over
// the shipped four-attempt retry ladder is four lanes (topics x priorities x
// (1 + retryTiers)). The grid's prefetch-1 column sits below that floor on
// purpose, so its refusal is logged as a result, while a refusal at or above
// the floor is one the sweep did not ask for and fails the benchmark. If the
// shipped ladder ever changes, this floor is wrong in the safe direction: a
// cell that used to be a logged refusal fails loudly instead of passing quiet.
const refusedPrefetchFloor = 4

// sweepPublishers is how many publishers fill one sweep cell's corpus. It is
// the count the client's own single-message publish path reaches its ceiling
// at: measured at 1, 4, 8, 16, 24, 32, 48 and 64 publishers the path offered
// 227, 521, 1272, 2558, 2517, 2533, 2557 and 1696 messages a second, so no
// publisher count above 16 offers more and 64 offers less. A sweep cell's
// measured rate is therefore capped by this load while the corpus is still
// being published, and BenchmarkRabbitMQPublishLoad reports the cap beside the
// cells.
const sweepPublishers = 16

// sweepPublisherCounts are the publisher counts the load control measures: the
// one the sweep uses, one more that offers the same, and one that offers less.
// The plateau is the point of the control - it is what says a cell at the
// sweep's rate is at the load path's ceiling rather than short of publishers.
var sweepPublisherCounts = []int{16, 32, 64}

// BenchmarkRabbitMQConsumeSweep measures RabbitMQ consume-and-settle throughput
// over handler concurrency and the subscription's in-flight budget, with the
// shape benchmarkConsume uses: one priority, a handler that returns at once,
// and a corpus drained by one lane.
//
// A cell publishes its corpus first and opens its window on the backlog that
// leaves, holding every delivery back from the core until it does, so the
// window contains the drain and not the load. The alternative shape - the one
// benchmarkConsume has, where the window opens before the corpus is published -
// cannot measure a consumer that settles faster than the client publishes: its
// window ends no earlier than the last publish call, and the harness's own
// single-message publish path offers about 2.5k messages a second whatever the
// publisher count (BenchmarkRabbitMQPublishLoad reports exactly what it
// offers). Every cell here is therefore the consumer's own window, though not
// its own CPU: that figure covers the load as well, and is read beside the
// publish-only control.
//
// Every cell reports messages a second, the p50 and p99 settle latency (the
// time from the pump offering the delivery to the core to the ack call the
// runtime made for it, which is the interval a rate cannot show), the largest
// live heap the process held, and the client's own CPU seconds per 1000
// messages. The CPU figure is read before and after the measurement, so it
// covers the client's whole measurement: the corpus load, the drain and the
// teardown. BenchmarkRabbitMQPublishLoad reports the same figure for the load
// alone at this publisher count, so a cell's drain is read as the difference.
//
// What a cell configures is a request, not what the driver is given. The core
// refuses an in-flight budget below the subscription's lane count, which for
// this shape's four lanes makes every prefetch-1 cell fail its subscription,
// and it caps one above the total its lanes can hold, logging the configured
// and effective values at startup. Both are read beside the cell: a refused
// cell logs the refusal the core gave and reports no rate, which is why the
// sweep is expected to print five refused cells, and the cap is what a plateau
// across the larger budgets means.
func BenchmarkRabbitMQConsumeSweep(b *testing.B) {
	namespace := benchNamespace(b)
	drv := rabbitmq.Driver{}
	endpoint := rabbitMQEndpoint()
	for _, concurrency := range sweepConcurrencies {
		for _, prefetch := range sweepPrefetches {
			b.Run(fmt.Sprintf("concurrency-%d/prefetch-%d", concurrency, prefetch), func(b *testing.B) {
				runConsumeCell(b, drv, endpoint, namespace, concurrency, prefetch)
			})
		}
	}
}

// BenchmarkRabbitMQPublishLoad measures what the sweep's publisher count
// offers with no consumer attached, which bounds a sweep cell's own load rate
// from above and is therefore the rate above which a cell's number describes
// the consumer. It reports the same CPU seconds per 1000 messages a sweep cell
// does, so the publisher's share of a cell's CPU can be read beside the cell
// rather than guessed at. A cell publishes against a live consumer, so its own
// load rate is at best this bound and its own load CPU at least this figure.
func BenchmarkRabbitMQPublishLoad(b *testing.B) {
	for _, publishers := range sweepPublisherCounts {
		b.Run(fmt.Sprintf("publishers-%d", publishers), func(b *testing.B) {
			h := newBench(b, rabbitmq.Driver{}, bench.Config{
				Namespace:  benchNamespace(b),
				Endpoint:   rabbitMQEndpoint(),
				Messages:   b.N,
				Publishers: publishers,
				Timeout:    measurementTimeout,
			})
			defer closeBench(b, h)
			ctx, cancel := context.WithTimeout(context.Background(), measurementTimeout)
			defer cancel()
			cpuBefore := cpuSeconds()
			result, err := h.Publish(ctx)
			cpuAfter := cpuSeconds()
			if err != nil {
				b.Fatalf("rabbitmq publish: %v", err)
			}
			b.ReportMetric(result.Rate(), "msgs/s")
			b.ReportMetric((cpuAfter-cpuBefore)*1000/float64(result.Messages), "cpu-s-per-1k")
			b.ReportMetric(float64(result.Duplicates), "dups")
		})
	}
}

// runConsumeCell measures one sweep cell: the consume-and-settle throughput of
// one subscription of that shape.
func runConsumeCell(b *testing.B, drv driver.Driver, endpoint, namespace string, concurrency, prefetch int) {
	b.Helper()
	h := newBench(b, drv, bench.Config{
		Namespace:   namespace,
		Endpoint:    endpoint,
		Messages:    b.N,
		Publishers:  sweepPublishers,
		Concurrency: concurrency,
		Prefetch:    prefetch,
		Backlog:     true,
		Timeout:     measurementTimeout,
	})
	defer closeBench(b, h)
	ctx, cancel := context.WithTimeout(context.Background(), measurementTimeout)
	defer cancel()
	cpuBefore := cpuSeconds()
	result, err := h.Consume(ctx)
	cpuAfter := cpuSeconds()
	if err != nil {
		// A cell the core refuses is a result, not a reason to stop the sweep,
		// and this grid asks for one the core refuses by construction: a budget
		// below the subscription's lane count. That refusal is logged with the
		// floor it missed and the cell reports no rate, so the cell is visible
		// in the output without making every run of the sweep end in failure.
		// Anything else is a refusal the sweep did not ask for, and fails.
		if prefetch < refusedPrefetchFloor {
			b.Logf("%s consume at concurrency %d prefetch %d refused by the core: %v", drv.Name(), concurrency, prefetch, err)
			return
		}
		b.Errorf("%s consume at concurrency %d prefetch %d: %v", drv.Name(), concurrency, prefetch, err)
		return
	}
	b.ReportMetric(result.Rate(), "msgs/s")
	b.ReportMetric(float64(result.SettlePercentile(50))/float64(time.Millisecond), "p50-settle-ms")
	b.ReportMetric(float64(result.SettlePercentile(99))/float64(time.Millisecond), "p99-settle-ms")
	b.ReportMetric(float64(result.HeapMax), "heap-max-B")
	b.ReportMetric((cpuAfter-cpuBefore)*1000/float64(result.Messages), "cpu-s-per-1k")
	b.ReportMetric(float64(result.Duplicates), "dups")
}

// cpuSeconds reports the CPU time this process has used, user and system
// together, which is what getrusage calls RUSAGE_SELF. Two readings around a
// measurement are what turn it into the time that measurement cost.
//
// A reading that fails is reported as no time at all rather than as an error:
// the figure is read beside a rate, and a process that will not answer for its
// own CPU time is not a reason to lose the measurement.
func cpuSeconds() float64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	user := time.Duration(usage.Utime.Sec)*time.Second + time.Duration(usage.Utime.Usec)*time.Microsecond
	system := time.Duration(usage.Stime.Sec)*time.Second + time.Duration(usage.Stime.Usec)*time.Microsecond
	return (user + system).Seconds()
}
