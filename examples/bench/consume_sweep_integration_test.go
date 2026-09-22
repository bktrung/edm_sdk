//go:build integration

package bench_test

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/rabbitmq"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/bench"
)

// sweepConcurrencies are the handler concurrencies a sweep measures. The set is
// fine at the low end on purpose: the rate a subscription reaches with several
// handlers is only interesting beside the rate one handler reaches, and whether
// the second handler doubles it or buys nothing is the question the cells below
// four answer.
var sweepConcurrencies = []int{1, 2, 3, 4, 8, 16}

// sweepQueueTypes are the broker queue kinds a sweep measures, named the way the
// driver's queue-type option names them. The two kinds keep a message
// differently - a quorum queue replicates it through a raft group and a classic
// queue does not - so a difference between their cells at the same concurrency
// is the broker's own work, and a rate that is the same on both is a limit
// neither kind supplies.
var sweepQueueTypes = []string{"quorum", "classic"}

// sweepRepeats is how many times a sweep measures each cell. A cell's number is
// a band rather than a point - a smaller share of these cells has come back at
// half their family's rate - so the median of three, read beside the range, is
// what a cell reports.
const sweepRepeats = 3

// sweepPublishers is how many goroutines load one cell's corpus.
const sweepPublishers = 16

// sweepPublishBatch is how many corpus messages one loader call carries, which
// is what makes a corpus of the size these cells need affordable to fill: the
// single-message path offers about two and a half thousand messages a second
// whatever the publisher count, and one call carrying a driver window's worth of
// them is confirmed as one. BenchmarkRabbitMQPublishLoad measures both paths at
// this publisher count.
const sweepPublishBatch = 64

// sweepProbeMessages is the corpus the sizing pass drains to learn a shape's
// rate before the cell that reports one is measured.
const sweepProbeMessages = 4000

// sweepWindow is the drain window a cell aims for. A rate read over a window
// this long is a rate over thousands of cycles of what it measures, and the
// window each cell actually got is reported beside its rate rather than assumed
// from this one. The target sits above the floor below because the probe that
// sizes a cell and the cell itself are not the same measurement: a shape whose
// cell runs faster than its probe would otherwise land under the floor.
const sweepWindow = 10 * time.Second

// sweepCorpusFloor and sweepCorpusCeiling bound the corpus a cell asks for. The
// floor keeps a slow shape's window from being a handful of messages, and the
// ceiling bounds what one cell may load: a corpus is published before the window
// opens, so it costs the loader's rate in front of every cell that follows.
const (
	sweepCorpusFloor   = 2000
	sweepCorpusCeiling = 300000
)

// sweepAttempts is how many times one cell may be measured before its number is
// taken as it stands. The retry answers one mistake: a corpus sized from a probe
// that ran faster than the cell leaves a window under the floor, and the next
// attempt corrects it with a corpus sized from the cell's own rate. A cell
// measured above the load limit is not retried, because that limit is about
// whose workload the number describes, not about the cell's own sizing.
const sweepAttempts = 2

// sweepWindowFloor is the window below which a cell's number is a reading of too
// short an interval to trust. A cell under it is logged as void rather than read
// as a number about its shape.
const sweepWindowFloor = 5 * time.Second

// loadLimit is the 1-minute load average above which a cell is void: this box
// runs several rows and their brokers at once, and a cell measured under
// another row's workload is evidence about that workload. The figure includes
// the cell's own load, and every cell reports the one it was measured at, so a
// sweep whose cells all exceeded it is read as a band rather than discarded.
const loadLimit = 5.0

// publishLoadMessages is the corpus the load control publishes on one loader
// path: the size the sweep's cells drain on the chunked path, and a tenth of it
// on the single-message path, which fills at its own rate whatever the corpus
// is, so the smaller corpus costs the same order of time and still bounds the
// path it measures.
func publishLoadMessages(batch int) int {
	if batch > 0 {
		return sweepCorpusCeiling
	}
	return sweepCorpusCeiling / 10
}

// publishLoadBatches are the loader batch sizes the control measures: the
// sweep's own chunk size and the single-message path the publish measurements
// report.
var publishLoadBatches = []int{0, sweepPublishBatch}

// intakeSplitConcurrencies are the handler concurrencies the intake-split probe
// compares its two shapes at, and intakeSplitQueueTypes are the queue kinds it
// compares them on.
var (
	intakeSplitConcurrencies = []int{4, 16}
	intakeSplitQueueTypes    = []string{"quorum", "classic"}
)

// intakeSplitPriorities is the priority set the two-destination shape consumes.
// One subscription over two priorities is two main destinations, and the driver
// opens one AMQP channel per destination, which is what makes this shape read a
// second channel's intake at the same handler concurrency as the one-destination
// cell beside it.
var intakeSplitPriorities = []f1.Priority{f1.PriorityHigh, f1.PriorityMedium}

// BenchmarkRabbitMQConsumeSweep measures RabbitMQ consume-and-settle throughput
// over handler concurrency and queue kind, with the shape benchmarkConsume uses:
// one priority, a handler that returns at once, and a corpus drained by the
// subscription's handlers. The in-flight budget is the one a caller who
// configures nothing gets, which is the shipped default for this shape's lanes.
//
// A cell publishes its corpus first and opens its window on the backlog that
// leaves, holding every delivery back from the core until it does, so the
// window contains the drain and not the load. The alternative shape - the one
// benchmarkConsume has, where the window opens before the corpus is published -
// cannot measure a consumer that settles faster than the client publishes: its
// window ends no earlier than the last publish call. Every cell here is
// therefore the consumer's own window, though not its own CPU: that figure
// covers the load as well, and is read beside the publish control.
//
// A cell's corpus is sized, not fixed. The two queue kinds are an order of
// magnitude apart on this harness - a classic queue hands a slot its next
// delivery in a fraction of a millisecond and a quorum queue takes several - so
// one fixed corpus is either too short to time the fast cells or an expensive
// one to load for the slow ones. A shape's corpus is therefore measured once,
// by draining a probe corpus at that shape and asking what would take
// sweepWindow at the rate it reached, and the three cells that follow measure
// the same corpus as each other. The probe cell and its rate are logged, and
// every cell reports the corpus it drained and the window it got, so a cell
// whose window came out short of the floor is visible rather than averaged away.
//
// Every cell reports messages a second, that rate divided by the cell's handler
// concurrency (the rate one handler slot carries), the window it was measured
// over, the p50 and p99 settle latency (the time from the pump offering the
// delivery to the core to the ack call the runtime made for it, which is the
// interval a rate cannot show), the largest live heap the process held, the
// client's own CPU seconds per 1000 messages, and the machine's 1-minute load
// average. The CPU figure is read before and after the measurement, so it covers
// the client's whole measurement: the corpus load, the drain and the teardown.
// BenchmarkRabbitMQPublishLoad reports the same figure for the load alone at
// this publisher count, so a cell's drain is read as the difference.
//
// The queue kind is what the harness asks the driver for, through the driver's
// own queue-type option; the cells are run in alternation, one repeat at a time,
// so a change in the box's load over the run falls on both kinds rather than on
// one of them.
func BenchmarkRabbitMQConsumeSweep(b *testing.B) {
	var shapes []sweepCell
	for _, concurrency := range sweepConcurrencies {
		for _, queueType := range sweepQueueTypes {
			shapes = append(shapes, sweepCell{concurrency: concurrency, queueType: queueType})
		}
	}
	runSweep(b, rabbitmq.Driver{}, rabbitMQEndpoint(), benchNamespace(b), shapes)
}

// BenchmarkRabbitMQBrokerPrefetchSweep measures the opt-in broker credit on
// quorum queues at the two concurrency values the option targets. The unset
// cell keeps the shipped fairness factor; positive broker-prefetch cells use a
// factor of one so the c=16, broker-prefetch=16 cell meets the core-window
// precondition. The final cell adds handler work to show that broker credit
// does not replace handler capacity.
func BenchmarkRabbitMQBrokerPrefetchSweep(b *testing.B) {
	var shapes []sweepCell
	for _, concurrency := range []int{4, 16} {
		for _, brokerPrefetch := range []int{0, 16, 32, 64, 128} {
			fairness := f1.FairnessConfig{}
			if brokerPrefetch > 0 {
				fairness.PrefetchFactor = 1
			}
			shapes = append(shapes, sweepCell{
				concurrency:         concurrency,
				queueType:           "quorum",
				brokerPrefetch:      brokerPrefetch,
				brokerPrefetchSweep: true,
				fairness:            fairness,
			})
		}
	}
	shapes = append(shapes, sweepCell{
		concurrency:         4,
		queueType:           "quorum",
		brokerPrefetch:      128,
		brokerPrefetchSweep: true,
		handlerWork:         5 * time.Millisecond,
		fairness:            f1.FairnessConfig{PrefetchFactor: 1},
	})
	runSweep(b, rabbitmq.Driver{}, rabbitMQEndpoint(), benchNamespace(b), shapes)
}

// BenchmarkRabbitMQIntakeSplit compares one subscription over one main
// destination with one subscription over two, at the same handler concurrency.
//
// The core derives one destination per topic and priority, the driver opens one
// AMQP channel per destination, and one goroutine reads each channel, so the
// two shapes differ in how many consumers on the connection the broker is
// handing deliveries to. The corpus is split evenly between the destinations,
// and both must settle before the window closes, so a shape whose second
// destination was starved reports a longer window rather than a faster cell.
//
// The reading is the ratio between the two shapes at one concurrency. A second
// channel that doubles the rate says one channel's intake is the wall, which is
// a client-side limit; a second channel that moves the rate by much less, on a
// connection whose broker is the same, says the broker's handover per consumer
// is the wall, because a consumer is what the two shapes added. The second
// destination is not the only difference between the shapes: the core's
// in-flight cap is the sum of the lanes' capacities, so two priorities also give
// the subscription a larger total budget, and a rate the second channel raises
// is the two together.
func BenchmarkRabbitMQIntakeSplit(b *testing.B) {
	var shapes []sweepCell
	for _, concurrency := range intakeSplitConcurrencies {
		for _, queueType := range intakeSplitQueueTypes {
			for _, priorities := range [][]f1.Priority{nil, intakeSplitPriorities} {
				shapes = append(shapes, sweepCell{concurrency: concurrency, queueType: queueType, priorities: priorities})
			}
		}
	}
	runSweep(b, rabbitmq.Driver{}, rabbitMQEndpoint(), benchNamespace(b), shapes)
}

// BenchmarkRabbitMQPublishLoad measures what the sweep's loader offers with no
// consumer attached, which bounds a cell's own load rate from above and is
// therefore the rate above which a cell's number describes the consumer. It
// measures both loader paths: the single-message one every publish measurement
// reports, and the chunked one a sweep cell fills its corpus with. It reports
// the same CPU seconds per 1000 messages a sweep cell does, so the loader's
// share of a cell's CPU can be read beside the cell rather than guessed at.
func BenchmarkRabbitMQPublishLoad(b *testing.B) {
	for _, batch := range publishLoadBatches {
		for _, publishers := range sweepPublisherCounts {
			b.Run(fmt.Sprintf("batch-%d/publishers-%d", batch, publishers), func(b *testing.B) {
				h := newBench(b, rabbitmq.Driver{}, bench.Config{
					Namespace:    benchNamespace(b),
					Endpoint:     rabbitMQEndpoint(),
					Messages:     publishLoadMessages(batch),
					Publishers:   publishers,
					PublishBatch: batch,
					Timeout:      measurementTimeout,
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
}

// sweepPublisherCounts are the publisher counts the load control measures: the
// one the sweep uses, one more that offers the same on the single-message path,
// and one that offers less there. The plateau is the point of the control - it is
// what says a cell at the sweep's rate is at the load path's ceiling rather than
// short of publishers.
var sweepPublisherCounts = []int{16, 32, 64}

// sweepCell is one consume cell's shape: the handler concurrency, the queue kind
// its destinations are created as, and the priority set its subscription
// consumes, which is how many main destinations it opens. The corpus is set when
// the shape is measured rather than being part of it, so the repeats of one
// shape share a corpus and two shapes with the same name are the same cell.
type sweepCell struct {
	concurrency         int
	queueType           string
	priorities          []f1.Priority
	brokerPrefetch      int
	brokerPrefetchSweep bool
	handlerWork         time.Duration
	fairness            f1.FairnessConfig
	corpus              int
}

// name identifies a cell in a log line and in a sub-benchmark name.
func (c sweepCell) name() string {
	destinations := "destinations-1"
	if len(c.priorities) > 1 {
		destinations = fmt.Sprintf("destinations-%d", len(c.priorities))
	}
	if !c.brokerPrefetchSweep && c.brokerPrefetch == 0 && c.handlerWork == 0 && c.fairness.PrefetchFactor == 0 {
		return fmt.Sprintf("queue-%s/concurrency-%d/%s", c.queueType, c.concurrency, destinations)
	}
	brokerPrefetch := "unset"
	if c.brokerPrefetch > 0 {
		brokerPrefetch = strconv.Itoa(c.brokerPrefetch)
	}
	return fmt.Sprintf(
		"queue-%s/concurrency-%d/%s/broker-prefetch-%s/handler-%s",
		c.queueType, c.concurrency, destinations, brokerPrefetch, c.handlerWork,
	)
}

// runSweep sizes every shape once and then measures each of them repeat times,
// with the repeat loop outermost so the shapes alternate within one pass over
// the box's load rather than being measured in shape order.
//
// A shape whose window came out under the floor is measured again, with a corpus
// resized from the rate it just reached. The metrics reported for a cell are the
// last attempt's; every attempt is in the log beside them.
func runSweep(b *testing.B, drv driver.Driver, endpoint, namespace string, shapes []sweepCell) {
	corpora := make(map[string]int, len(shapes))
	for repeat := 1; repeat <= sweepRepeats; repeat++ {
		for _, shape := range shapes {
			b.Run(fmt.Sprintf("%s/repeat-%d", shape.name(), repeat), func(b *testing.B) {
				cell := shape
				for attempt := 1; attempt <= sweepAttempts; attempt++ {
					corpus, sized := corpora[cell.name()]
					if !sized {
						corpus = sizeCorpus(b, drv, endpoint, namespace, cell)
					}
					cell.corpus = corpus
					measurement, windowOK := runConsumeCell(b, drv, endpoint, namespace, cell, attempt)
					if windowOK {
						return
					}
					corpora[cell.name()] = corpusFor(measurement.result.Rate())
				}
			})
		}
	}
}

// corpusFor is the corpus that would take sweepWindow at a measured rate,
// between the floor and the ceiling.
func corpusFor(rate float64) int {
	return min(max(int(math.Ceil(rate*sweepWindow.Seconds())), sweepCorpusFloor), sweepCorpusCeiling)
}

// sizeCorpus drains a probe corpus at one shape and returns the corpus that
// would take sweepWindow at the rate the probe reached, between the floor and
// the ceiling.
//
// The probe is a cell of the same shape and is not reported as one: it is
// visible in the log with its rate, so a shape whose rate the sweep later
// contradicts can be read against what it was sized from.
func sizeCorpus(b *testing.B, drv driver.Driver, endpoint, namespace string, cell sweepCell) int {
	b.Helper()
	cell.corpus = sweepProbeMessages
	measurement, err := measureCell(b, drv, endpoint, namespace, cell)
	if err != nil {
		b.Fatalf("sizing %s: %v", cell.name(), err)
	}
	rate := measurement.result.Rate()
	corpus := corpusFor(rate)
	b.Logf("sizing %s: %d messages drained in %s at %.1f msgs/s, corpus %d",
		cell.name(), measurement.result.Messages, measurement.result.Elapsed, rate, corpus)
	return corpus
}

// runConsumeCell measures one sized consume cell, reports its metrics, and says
// whether the cell's window met the floor, which is the one reading the caller
// corrects by measuring the cell again.
func runConsumeCell(b *testing.B, drv driver.Driver, endpoint, namespace string, cell sweepCell, attempt int) (cellMeasurement, bool) {
	b.Helper()
	measurement, err := measureCell(b, drv, endpoint, namespace, cell)
	if err != nil {
		b.Fatalf("%s: %v", cell.name(), err)
	}
	result := measurement.result
	rate := result.Rate()
	b.ReportMetric(rate, "msgs/s")
	b.ReportMetric(rate/float64(cell.concurrency), "per-slot-msgs/s")
	b.ReportMetric(float64(result.Messages), "corpus")
	b.ReportMetric(result.Elapsed.Seconds(), "window-s")
	b.ReportMetric(float64(result.SettlePercentile(50))/float64(time.Millisecond), "p50-settle-ms")
	b.ReportMetric(float64(result.SettlePercentile(99))/float64(time.Millisecond), "p99-settle-ms")
	b.ReportMetric(float64(result.HeapMax), "heap-max-B")
	b.ReportMetric(measurement.cpu, "cpu-s-per-1k")
	b.ReportMetric(float64(result.Duplicates), "dups")
	b.ReportMetric(measurement.load, "load1")
	b.Logf("attempt %d %s: %d messages drained in %s at %.1f msgs/s, load1 %.2f",
		attempt, cell.name(), result.Messages, result.Elapsed, rate, measurement.load)
	windowOK := true
	if result.Elapsed < sweepWindowFloor {
		b.Logf("%s VOID: its %s drain is under the %s this sweep reads a rate over",
			cell.name(), result.Elapsed, sweepWindowFloor)
		windowOK = false
	}
	if measurement.load > loadLimit {
		b.Logf("%s VOID: measured at 1-minute load %.2f, above %.1f", cell.name(), measurement.load, loadLimit)
	}
	return measurement, windowOK
}

// cellMeasurement is what one cell's measurement reported: the harness's result,
// the CPU the whole call cost per 1000 messages, and the machine's load as the
// window closed.
type cellMeasurement struct {
	result bench.Result
	cpu    float64
	load   float64
}

// measureCell runs one consume cell.
func measureCell(b *testing.B, drv driver.Driver, endpoint, namespace string, cell sweepCell) (cellMeasurement, error) {
	b.Helper()
	h := newBench(b, drv, bench.Config{
		Namespace:      namespace,
		Endpoint:       endpoint,
		Messages:       cell.corpus,
		Publishers:     sweepPublishers,
		PublishBatch:   sweepPublishBatch,
		Concurrency:    cell.concurrency,
		HandlerWork:    cell.handlerWork,
		BrokerPrefetch: cell.brokerPrefetch,
		Fairness:       cell.fairness,
		Priorities:     cell.priorities,
		QueueType:      cell.queueType,
		Backlog:        true,
		Timeout:        measurementTimeout,
	})
	defer closeBench(b, h)
	ctx, cancel := context.WithTimeout(context.Background(), measurementTimeout)
	defer cancel()
	cpuBefore := cpuSeconds()
	result, err := h.Consume(ctx)
	cpuAfter := cpuSeconds()
	if err != nil {
		return cellMeasurement{}, fmt.Errorf("%s consume on a %s queue at concurrency %d: %w",
			drv.Name(), cell.queueType, cell.concurrency, err)
	}
	return cellMeasurement{
		result: result,
		cpu:    (cpuAfter - cpuBefore) * 1000 / float64(result.Messages),
		load:   loadAverage(),
	}, nil
}

// loadAverage reports the machine's 1-minute load average, which is the figure a
// cell's rate is read against: this box is shared with other rows and their
// brokers, and a cell measured under another workload describes that workload. A
// host that will not report one reads as zero, which is a load the cell cannot
// be judged by rather than one it passed; the invocation's own uptime is printed
// beside the cells for that case.
func loadAverage() float64 {
	raw, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(raw))
	if len(fields) == 0 {
		return 0
	}
	load, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0
	}
	return load
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
