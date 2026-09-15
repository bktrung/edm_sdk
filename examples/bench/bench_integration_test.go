//go:build integration

package bench_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
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

	// kafkaFixturePartitions is the partition count a Kafka topic gets in this
	// repository's fixture. The core requests no explicit count -
	// DestinationSpec.Partitions is zero - so the broker's own
	// KAFKA_NUM_PARTITIONS applies, and docker/docker-compose.yml sets it to 3.
	// The count is reported beside every Kafka number because it is part of the
	// shape measured, not because it bounds the rate: on these three partitions
	// the subscription reached 430.1 msgs/s at the shipped handler concurrency
	// and 76.47 msgs/s with one handler at a time, which is more than three
	// times the single-handler rate and therefore past any bound the count
	// could impose. A run against another broker states its own count in
	// F1_KAFKA_PARTITIONS.
	kafkaFixturePartitions = 3

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

	// loadPublishers is how many publishers fill the corpus for the consume,
	// latency and retry measurements. One publisher would put its own confirm
	// latency in front of every message and the rate would describe the
	// publisher rather than the consumer.
	loadPublishers = 4

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
// lane, at one handler at a time and at the shipped handler concurrency.
func BenchmarkKafkaConsume(b *testing.B) {
	benchmarkConsume(b, kafka.Driver{}, kafkaEndpoint())
}

// BenchmarkRabbitMQConsume measures RabbitMQ consume-and-settle throughput on
// one lane, at one handler at a time and at the shipped handler concurrency.
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
		})
	}
}

// benchmarkConsume runs the consume-and-settle measurement twice per driver.
//
// concurrency-1 is one handler at a time across all of the subscription's
// partitions: the rate one message-at-a-time handling reaches, whatever the
// partition count is.
//
// concurrency-shipped is the concurrency a caller who sets none gets, which is
// the rate a subscription of this shape actually reaches. The two are not
// interchangeable: a subscription over several partitions shares its handler
// slots between them, so the second number is not the first one multiplied by
// anything. On three Kafka partitions, measured, the first was 76.47 msgs/s and
// the second 430.1 msgs/s, above three times the first, so what bounds this
// shape is the handler concurrency and not the partition count.
//
// The shipped case passes zero rather than the value the client ships with, so
// the case keeps measuring the shipped value if that value changes.
func benchmarkConsume(b *testing.B, drv driver.Driver, endpoint string) {
	namespace := benchNamespace(b)
	for _, c := range []struct {
		name        string
		concurrency int
	}{
		{name: "concurrency-1", concurrency: 1},
		{name: "concurrency-shipped"},
	} {
		b.Run(c.name, func(b *testing.B) {
			h := newBench(b, drv, bench.Config{
				Namespace:   namespace,
				Endpoint:    endpoint,
				Messages:    b.N,
				Publishers:  loadPublishers,
				Concurrency: c.concurrency,
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
}

// reportPartitions records the Kafka partition count the run's topics were
// created with. Other drivers deliver from a shared destination and have no
// partition count to report.
func reportPartitions(b *testing.B, drv driver.Driver) {
	if drv.Name() != "kafka" {
		return
	}
	b.ReportMetric(float64(kafkaPartitions()), "partitions")
}

func kafkaPartitions() int {
	value, err := strconv.Atoi(os.Getenv("F1_KAFKA_PARTITIONS"))
	if err != nil || value < 1 {
		return kafkaFixturePartitions
	}
	return value
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
