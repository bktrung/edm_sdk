//go:build integration

package bench_test

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/kafka"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/rabbitmq"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/bench"
)

const (
	// defaultKafkaEndpoint and defaultRabbitMQEndpoint are where the fixtures
	// listen unless the environment says otherwise, which is also what the
	// driver-acceptance harness falls back to.
	defaultKafkaEndpoint    = "localhost:19092"
	defaultRabbitMQEndpoint = "amqp://guest:guest@localhost:5672/" //nolint:gosec // test fixture endpoint

	// kafkaDriverName is the driver whose cells carry a partition count.
	kafkaDriverName = "kafka"

	// kafkaFixturePartitions is the partition count a Kafka run asks for when
	// F1_KAFKA_PARTITIONS names none, which is the count this repository's
	// fixture creates when nothing asks for one: docker/docker-compose.yml sets
	// KAFKA_NUM_PARTITIONS to 3. A run states its own count in that variable,
	// and the count travels to the broker as the driver option a deployment
	// sets for its own topics, so the number a cell reports is the one its
	// destinations were created with.
	//
	// The count is reported beside every Kafka number because it is part of the
	// shape measured: it is how many partitions the corpus is spread over, and
	// it is the ceiling a subscription limited to one handler per partition
	// runs into. The consume cells open their window before the corpus is
	// published, so each Kafka consume number here is read against the publish
	// benchmark's rate at consumeLoadPublishers, not as a ceiling the partition
	// count supplies. Measured at 3f6cbfa that rate was 20127 msgs/s, no Kafka
	// consume cell came within 10 percent of it, and the work-5ms shipped
	// cell's 2561 msgs/s needs about thirteen handlers in flight, which three
	// partitions cannot supply one each.
	kafkaFixturePartitions = 3

	// slowHandlerWork is the handler hold time the work-5ms consume cells
	// measure with. A handler that holds a delivery is what makes a consume rate
	// describe what the handler concurrency buys: with a handler that returns at
	// once the handler slots are not what the cell runs out of, so no slot count
	// can be read from it. The wait is orders of magnitude inside the harness's
	// handler timeout, so it is never what ends a delivery.
	slowHandlerWork = 5 * time.Millisecond

	// longHandlerWork is the hold the work-200ms cell measures with. It is long
	// enough that one handler is idle between deliveries whatever the broker
	// does, so the rate that cell reports is the hold and nothing else, and one
	// handler cannot keep the subscription's own work moving. That is what makes
	// the lag and heap the cell samples figures about how much the subscription
	// let pile up behind a single slow handler: at a 5ms hold the arrivals keep
	// up with the handler, and the same bound is hidden behind them.
	longHandlerWork = 200 * time.Millisecond

	// kafkaRetryTier is the retry delay the Kafka retry benchmark configures.
	// The driver checks a deferred record's due time against a band whose lower
	// edge is the record's own timestamp plus half the tier
	// (drivers/kafka/deferral.go, evaluateDeferral), and the core computes that
	// due time before the record carrying it is stamped, so a stamp lagging by
	// more than half the tier is reported as a deferred-delivery fault. A 1ms
	// tier raised one such fault in a three-run benchmark. At 10ms the stamp
	// would have to lag by 5ms.
	kafkaRetryTier = 10 * time.Millisecond

	// rabbitRetryTier is the shortest retry delay the RabbitMQ driver honours.
	// A deferred destination parks in a ladder of queue TTLs whose first rung
	// is 500ms, and the driver declares that rung as the floor of its delay
	// accuracy, so anything shorter is delivered a full rung late regardless.
	rabbitRetryTier = 500 * time.Millisecond

	// loadPublishers is how many publishers fill the corpus for the retry
	// measurement. The consume measurement has its own count, because its rate
	// is read against its load.
	loadPublishers = 4

	// consumeLoadPublishers is how many publishers fill the corpus for the
	// consume measurement. A consume measurement opens its window before the
	// corpus is published, so a consume rate can never beat the rate this load
	// reaches, and 16 is the highest publisher count the publish benchmark
	// measures, which is what a consume cell's ceiling is read against.
	consumeLoadPublishers = 16

	// measurementTimeout bounds one measurement, including client construction,
	// warm-up and drain. The retry measurement on RabbitMQ is the slow one: the
	// corpus is settled one tier delay after it is published.
	measurementTimeout = 5 * time.Minute
	// cleanupTimeout bounds the deletion of one run's destinations.
	cleanupTimeout = 2 * time.Minute
)

// BenchmarkKafkaPublish measures Kafka publish throughput.
func BenchmarkKafkaPublish(b *testing.B) {
	benchmarkPublish(b, kafka.Driver{}, kafkaEndpoint())
}

// BenchmarkRabbitMQPublish measures RabbitMQ publish throughput.
func BenchmarkRabbitMQPublish(b *testing.B) {
	benchmarkPublish(b, rabbitmq.Driver{}, rabbitMQEndpoint())
}

// BenchmarkKafkaConsume measures Kafka consume-and-settle throughput on one
// lane. With a handler that returns at once it runs at one handler at a time
// and at the shipped handler concurrency; with a handler that holds every
// delivery for 5ms it runs at one handler at a time, at three, at the shipped
// handler concurrency, and on an ordered subscription at the shipped handler
// concurrency; with a handler that holds every delivery for 200ms it runs at
// one handler at a time.
func BenchmarkKafkaConsume(b *testing.B) {
	benchmarkConsume(b, kafka.Driver{}, kafkaEndpoint())
}

// BenchmarkRabbitMQConsume measures RabbitMQ consume-and-settle throughput on
// one lane. With a handler that returns at once it runs at one handler at a
// time and at the shipped handler concurrency; with a handler that holds every
// delivery for 5ms it runs at one handler at a time, at three, at the shipped
// handler concurrency, and on an ordered subscription at the shipped handler
// concurrency.
func BenchmarkRabbitMQConsume(b *testing.B) {
	benchmarkConsume(b, rabbitmq.Driver{}, rabbitMQEndpoint())
}

// BenchmarkKafkaLatency measures Kafka end-to-end publish-to-ack latency.
func BenchmarkKafkaLatency(b *testing.B) {
	benchmarkLatency(b, kafka.Driver{}, kafkaEndpoint())
}

// BenchmarkRabbitMQLatency measures RabbitMQ end-to-end publish-to-ack latency.
func BenchmarkRabbitMQLatency(b *testing.B) {
	benchmarkLatency(b, rabbitmq.Driver{}, rabbitMQEndpoint())
}

// BenchmarkKafkaRetry measures Kafka retry-path throughput with the shortest
// tier its deferred delivery holds to.
func BenchmarkKafkaRetry(b *testing.B) {
	benchmarkRetry(b, kafka.Driver{}, kafkaEndpoint(), kafkaRetryTier)
}

// BenchmarkRabbitMQRetry measures RabbitMQ retry-path throughput with the
// shortest tier its parking ladder honours.
func BenchmarkRabbitMQRetry(b *testing.B) {
	benchmarkRetry(b, rabbitmq.Driver{}, rabbitMQEndpoint(), rabbitRetryTier)
}

// benchmarkPublish runs one publish measurement per concurrent publisher
// count. Concurrent publishers on one client is the shape the answer to a
// shared producer lock turns on: a client that serialises single-message
// publishes shows the same rate at 1, 4 and 16.
func benchmarkPublish(b *testing.B, drv driver.Driver, endpoint string) {
	namespace := benchNamespace(b)
	for _, publishers := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("publishers-%d", publishers), func(b *testing.B) {
			h := newBench(b, drv, bench.Config{
				Namespace:  namespace,
				Endpoint:   endpoint,
				Messages:   b.N,
				Publishers: publishers,
				Partitions: kafkaPartitions(),
				Timeout:    measurementTimeout,
			})
			defer closeBench(b, h)
			ctx, cancel := context.WithTimeout(context.Background(), measurementTimeout)
			defer cancel()
			result, err := h.Publish(ctx)
			if err != nil {
				b.Fatalf("%s publish: %v", drv.Name(), err)
			}
			reportPartitions(b, drv)
			b.ReportMetric(result.Rate(), "msgs/s")
			b.ReportMetric(float64(result.Duplicates), "dups")
			b.ReportMetric(float64(result.GoroutineDelta), "goroutines-delta")
		})
	}
}

// benchmarkConsume runs the consume-and-settle measurement six times per
// driver, and a seventh time on Kafka, which is the driver whose destinations
// have a partition count.
//
// Every consume measurement opens its window before the corpus is published, so
// a rate it reports contains its own load: the corpus can only arrive as fast as
// consumeLoadPublishers publish it, and a cell whose rate is level with the
// publish benchmark at that same publisher count is describing the load rather
// than the consumer. Each cell below is read against that publish rate, which at
// 3f6cbfa was 20127 msgs/s on Kafka and 2815 on RabbitMQ.
//
// concurrency-1 is one handler at a time across all of the subscription's
// partitions: the rate one message-at-a-time handling reaches, whatever the
// partition count is.
//
// concurrency-shipped is the concurrency a caller who sets none gets, which is
// the rate a subscription of this shape actually reaches. The two are not
// interchangeable: a subscription over several partitions shares its handler
// slots between them, so the second number is not the first one multiplied by
// anything. Measured at 3f6cbfa with a handler that returns at once, Kafka
// reached 3625 msgs/s at one handler and 7178 at the shipped concurrency, both
// far under its load, so those two cells describe the consumer; RabbitMQ
// reached 742.5 and 2828, and the second of those sits at the load rate, so
// that one cell is bound by the load and says nothing about the consumer.
//
// The three work-5ms cells hold each delivery in the handler for 5ms, which is
// what shows what the handler concurrency buys once a handler takes time.
// concurrency-3 is three handler slots, which was the fixture's partition count
// when the cell was recorded; the other two are the same single-slot and shipped
// shapes.
// Measured at 3f6cbfa against ceilings of 200, 600 and 3200 msgs/s for one,
// three and sixteen slots, Kafka reached 177.6, 529.7 and 2561, which is 88.8,
// 88.3 and 80.0 percent of them, and RabbitMQ reached 173.1, 395.8 and 2021,
// 86.6, 66.0 and 63.2 percent. No work-5ms cell is within 10 percent of its
// driver's load rate, so each of the six measures the consumer; RabbitMQ's
// shipped cell gets 2021 msgs/s out of the sixteen slots, about ten of them
// busy on average.
//
// The ordered cell is the same 5ms hold at the shipped handler concurrency on a
// subscription that handles deliveries sharing a key one at a time, with the
// publishers spreading the corpus over a fixed key set. It is the only cell that
// measures the ordered path, and its rate is read against the unordered
// work-5ms shipped cell beside it: the difference between the two is what the
// ordering costs at this hold, whatever the partition count it runs at.
//
// The work-200ms cell holds each delivery for 200ms at one handler, which is
// long enough that no broker or harness cost can show through the hold: its rate
// is the hold and nothing else. The figures to read on it are therefore the lag
// and the heap it samples, which are what the subscription let pile up behind
// one slow handler. It is measured on Kafka alone.
//
// The shipped case passes zero rather than the value the client ships with, so
// the case keeps measuring the shipped value if that value changes.
func benchmarkConsume(b *testing.B, drv driver.Driver, endpoint string) {
	namespace := benchNamespace(b)
	for _, c := range []struct {
		name        string
		concurrency int
		handlerWork time.Duration
		ordered     bool
		kafkaOnly   bool
	}{
		{name: "concurrency-1", concurrency: 1},
		{name: "concurrency-shipped"},
		{name: "work-5ms/concurrency-1", concurrency: 1, handlerWork: slowHandlerWork},
		{name: "work-5ms/concurrency-3", concurrency: kafkaFixturePartitions, handlerWork: slowHandlerWork},
		{name: "work-5ms/concurrency-shipped", handlerWork: slowHandlerWork},
		{name: "work-5ms/ordered/concurrency-shipped", handlerWork: slowHandlerWork, ordered: true},
		{name: "work-200ms/concurrency-1", concurrency: 1, handlerWork: longHandlerWork, kafkaOnly: true},
	} {
		if c.kafkaOnly && drv.Name() != kafkaDriverName {
			continue
		}
		b.Run(c.name, func(b *testing.B) {
			h := newBench(b, drv, bench.Config{
				Namespace:   namespace,
				Endpoint:    endpoint,
				Messages:    b.N,
				Publishers:  consumeLoadPublishers,
				Concurrency: c.concurrency,
				HandlerWork: c.handlerWork,
				Ordered:     c.ordered,
				Partitions:  kafkaPartitions(),
				Timeout:     measurementTimeout,
			})
			defer closeBench(b, h)
			ctx, cancel := context.WithTimeout(context.Background(), measurementTimeout)
			defer cancel()
			result, err := h.Consume(ctx)
			if err != nil {
				b.Fatalf("%s consume: %v", drv.Name(), err)
			}
			reportPartitions(b, drv)
			b.ReportMetric(result.Rate(), "msgs/s")
			b.ReportMetric(float64(result.Duplicates), "dups")
			b.ReportMetric(float64(result.LagMax), "lag-max")
			b.ReportMetric(float64(result.HeapMax), "heap-max")
			b.ReportMetric(float64(result.GoroutineDelta), "goroutines-delta")
		})
	}
}

// benchmarkLatency runs the latency measurement and reports the percentiles in
// milliseconds, the unit the two numbers a caller budgets against are read in.
func benchmarkLatency(b *testing.B, drv driver.Driver, endpoint string) {
	h := newBench(b, drv, bench.Config{
		Namespace:   benchNamespace(b),
		Endpoint:    endpoint,
		Messages:    b.N,
		Publishers:  1,
		Concurrency: 1,
		Partitions:  kafkaPartitions(),
		Timeout:     measurementTimeout,
	})
	defer closeBench(b, h)
	ctx, cancel := context.WithTimeout(context.Background(), measurementTimeout)
	defer cancel()
	result, err := h.Latency(ctx)
	if err != nil {
		b.Fatalf("%s latency: %v", drv.Name(), err)
	}
	reportPartitions(b, drv)
	b.ReportMetric(float64(result.Percentile(50))/float64(time.Millisecond), "p50-ms")
	b.ReportMetric(float64(result.Percentile(99))/float64(time.Millisecond), "p99-ms")
	b.ReportMetric(float64(result.Duplicates), "dups")
	b.ReportMetric(float64(result.GoroutineDelta), "goroutines-delta")
}

// benchmarkRetry runs the retry-path measurement. The tier delay is reported
// beside the rate and is never subtracted from it: the elapsed time contains
// one tier per message, and a rate quoted without its tier would compare two
// different ladders as if they were one.
func benchmarkRetry(b *testing.B, drv driver.Driver, endpoint string, tier time.Duration) {
	h := newBench(b, drv, bench.Config{
		Namespace:   benchNamespace(b),
		Endpoint:    endpoint,
		Messages:    b.N,
		Publishers:  loadPublishers,
		Concurrency: 1,
		RetryTier:   tier,
		Partitions:  kafkaPartitions(),
		Timeout:     measurementTimeout,
	})
	defer closeBench(b, h)
	ctx, cancel := context.WithTimeout(context.Background(), measurementTimeout)
	defer cancel()
	result, err := h.Retry(ctx)
	if err != nil {
		b.Fatalf("%s retry: %v", drv.Name(), err)
	}
	reportPartitions(b, drv)
	b.ReportMetric(result.Rate(), "msgs/s")
	b.ReportMetric(float64(result.RetryTier)/float64(time.Millisecond), "tier-ms")
	b.ReportMetric(float64(result.Duplicates), "dups")
	b.ReportMetric(float64(result.GoroutineDelta), "goroutines-delta")
}

// reportPartitions records the partition count a Kafka run created its
// destinations with. Other drivers deliver from a shared destination and have no
// partition count to report.
//
// The count reported is the one the run asked for, through the driver option
// that both gives a destination naming no count that many partitions and refuses
// one that has fewer. It is not read back from the topic: the port has no
// partition-count read, and an example may not reach the broker through a broker
// client of its own, which is what reading it back would take. What makes the
// two the same number is that each run creates its destinations fresh and the
// driver's own floor check fails the run when a destination has fewer partitions
// than it was given, so a run that reported a count it did not get would have
// failed rather than reported it.
func reportPartitions(b *testing.B, drv driver.Driver) {
	if drv.Name() != kafkaDriverName {
		return
	}
	b.ReportMetric(float64(kafkaPartitions()), "partitions")
}

// kafkaPartitions is the partition count a Kafka run creates its destinations
// with, read once from F1_KAFKA_PARTITIONS. A value that is missing or unusable
// keeps the fixture's count, so a run that states nothing measures the shape
// the recorded baseline was taken at.
func kafkaPartitions() int {
	value, err := strconv.Atoi(os.Getenv("F1_KAFKA_PARTITIONS"))
	if err != nil || value < 1 {
		return kafkaFixturePartitions
	}
	return value
}

var benchBrokerProbes struct {
	sync.Mutex
	results map[string]string
}

func requireBenchBroker(b *testing.B, drv driver.Driver, endpoint string) {
	b.Helper()
	key := drv.Name() + "\x00" + endpoint
	benchBrokerProbes.Lock()
	if benchBrokerProbes.results == nil {
		benchBrokerProbes.results = make(map[string]string)
	}
	reason, checked := benchBrokerProbes.results[key]
	if !checked {
		reason = benchBrokerFailure(drv, endpoint)
		benchBrokerProbes.results[key] = reason
	}
	benchBrokerProbes.Unlock()
	if reason != "" {
		b.Skip(reason)
	}
}

func benchBrokerFailure(drv driver.Driver, endpoint string) string {
	brokerName, envName, address := "Kafka", "F1_KAFKA_ENDPOINT", endpoint
	if drv.Name() == "rabbitmq" {
		brokerName, envName, address = "RabbitMQ", "F1_RABBITMQ_ENDPOINT", benchRabbitMQAddress(endpoint)
	}
	if address == "" {
		return fmt.Sprintf("%s broker is unavailable; set %s to a reachable broker", brokerName, envName)
	}
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		return fmt.Sprintf("%s broker is unavailable (%v); set %s to a reachable broker", brokerName, err, envName)
	}
	_ = conn.Close()
	return ""
}

func benchRabbitMQAddress(endpoint string) string {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Hostname() == "" {
		return ""
	}
	port := parsed.Port()
	if port == "" {
		port = "5672"
		if parsed.Scheme == "amqps" {
			port = "5671"
		}
	}
	return net.JoinHostPort(parsed.Hostname(), port)
}

func kafkaEndpoint() string {
	return benchEnv("F1_KAFKA_ENDPOINT", defaultKafkaEndpoint)
}

func rabbitMQEndpoint() string {
	return benchEnv("F1_RABBITMQ_ENDPOINT", defaultRabbitMQEndpoint)
}

func benchEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func newBench(b *testing.B, drv driver.Driver, cfg bench.Config) *bench.Harness {
	b.Helper()
	requireBenchBroker(b, drv, cfg.Endpoint)
	h, err := bench.New(drv, cfg)
	if err != nil {
		b.Fatalf("bench.New for %s at %s: %v", drv.Name(), cfg.Endpoint, err)
	}
	return h
}

func closeBench(b *testing.B, h *bench.Harness) {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	if err := h.Close(ctx); err != nil {
		b.Errorf("delete the run's destinations: %v", err)
	}
}
