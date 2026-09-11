//go:build integration

package acceptance_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/codec"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/kafka"
)

func TestAcceptanceAgainstKafka(t *testing.T) {
	const published = 10
	tempDir := t.TempDir()
	suffix := filepath.Base(filepath.Dir(tempDir))
	topic := "acceptance.kafka." + suffix
	subscription := "acceptance-kafka-" + suffix

	ctx := t.Context()
	endpoint := kafkaAcceptanceEndpoint()
	publisherConfig := kafkaAcceptanceConfig(endpoint, "acceptance-kafka-publisher")
	consumerConfig := kafkaAcceptanceConfig(endpoint, "acceptance-kafka-consumer")
	destinations := []string{
		fmt.Sprintf("f1.%s.%s.medium", publisherConfig.Env, topic),
		fmt.Sprintf("f1.%s.%s.dlq.%s", consumerConfig.Env, topic, subscription),
		fmt.Sprintf("f1.%s.unknown.dlq.%s", consumerConfig.Env, subscription),
	}
	cleanupConn, err := (kafka.Driver{}).Open(ctx, driver.Config{
		Endpoints:      []string{endpoint},
		ClientID:       "f1-acceptance-kafka-cleanup",
		ConnectTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("Kafka cleanup connection at %s: %v", endpoint, err)
	}
	cleanupKafkaAcceptance(t, cleanupConn, destinations)

	publisher, err := f1.New(ctx, publisherConfig,
		f1.WithDriver(kafka.Driver{}),
		f1.WithCodec(codec.JSON{}),
		f1.WithTopology(f1.TopologyDeclare),
		f1.WithPublishTopics(topic),
		f1.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatalf("Kafka publisher New() error: %v", err)
	}
	publisherClosed := false
	t.Cleanup(func() {
		if !publisherClosed {
			if err := publisher.Close(context.Background()); err != nil {
				t.Errorf("Kafka publisher cleanup Close(): %v", err)
			}
		}
	})

	consumer, err := f1.New(ctx, consumerConfig,
		f1.WithDriver(kafka.Driver{}),
		f1.WithCodec(codec.JSON{}),
		f1.WithTopology(f1.TopologyDeclare),
		f1.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatalf("Kafka consumer New() error: %v", err)
	}
	consumerClosed := false
	t.Cleanup(func() {
		if !consumerClosed {
			if err := consumer.Close(context.Background()); err != nil {
				t.Errorf("Kafka consumer cleanup Close(): %v", err)
			}
		}
	})

	received := make(chan int, published)
	runner, err := consumer.Subscribe(ctx, f1.Subscription{
		Name:            subscription,
		Topics:          []string{topic},
		Concurrency:     1,
		Prefetch:        2,
		Priorities:      []f1.Priority{f1.PriorityMedium},
		Retry:           f1.RetryConfig{MaxAttempts: 1},
		HandlerTimeout:  time.Second,
		UnmatchedPolicy: f1.Ignore,
		Handlers: map[string]f1.Handler{
			"acceptance.message.v1": f1.HandlerFunc(func(handlerCtx context.Context, event *f1.Event) error {
				var payload acceptanceMessage
				if err := event.Decode(&payload); err != nil {
					return f1.Terminal(err)
				}
				select {
				case received <- payload.Sequence:
					return nil
				case <-handlerCtx.Done():
					return handlerCtx.Err()
				}
			}),
		},
	})
	if err != nil {
		t.Fatalf("Kafka consumer Subscribe() error: %v", err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()

	for sequence := 1; sequence <= published; sequence++ {
		if _, err := publisher.Publisher().Publish(ctx, "acceptance.message.v1", acceptanceMessage{
			Sequence: sequence,
			Outcome:  "handled",
			Text:     fmt.Sprintf("acceptance-%03d", sequence),
		}, f1.WithTopic(topic)); err != nil {
			t.Fatalf("Kafka Publish(sequence=%d) error: %v", sequence, err)
		}
	}

	got := make(map[int]int, published)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer waitCancel()
	for len(got) < published {
		select {
		case sequence := <-received:
			got[sequence]++
		case runErr := <-runDone:
			t.Fatalf("Kafka runner stopped before all messages settled: %v", runErr)
		case <-waitCtx.Done():
			t.Fatalf("Kafka acceptance timed out: received=%v: %v", got, waitCtx.Err())
		}
		if total := receivedCount(got); total == published {
			break
		}
	}
	for sequence := 1; sequence <= published; sequence++ {
		if got[sequence] != 1 {
			t.Fatalf("Kafka sequence %d received %d times, want once; all=%v", sequence, got[sequence], got)
		}
	}

	limits := consumer.Limits()
	if limits.Driver != "kafka" {
		t.Fatalf("Kafka consumer limits driver = %q, want kafka", limits.Driver)
	}
	if err := consumer.Close(context.Background()); err != nil {
		t.Fatalf("Kafka consumer Close() error: %v", err)
	}
	consumerClosed = true
	runnerWaitCtx, runnerWaitCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer runnerWaitCancel()
	select {
	case runErr := <-runDone:
		if runErr != nil && !errors.Is(runErr, context.Canceled) {
			t.Fatalf("Kafka runner.Run() error after Close: %v", runErr)
		}
	case <-runnerWaitCtx.Done():
		t.Fatal("Kafka runner.Run() did not stop after Close")
	}
	if err := publisher.Close(context.Background()); err != nil {
		t.Fatalf("Kafka publisher Close() error: %v", err)
	}
	publisherClosed = true
}

func kafkaAcceptanceConfig(endpoint, service string) f1.Config {
	cfg := acceptanceFlowConfig()
	cfg.Env = "acceptance"
	cfg.Service = service
	cfg.Broker.Driver = "kafka"
	cfg.Broker.Endpoints = []string{endpoint}
	cfg.Broker.ConnectTimeout = 10 * time.Second
	cfg.Broker.DefaultPrefetch = 2
	cfg.Lifecycle.RebalanceDrainTimeout = 5 * time.Second
	return cfg
}

func kafkaAcceptanceEndpoint() string {
	if endpoint := os.Getenv("F1_ACCEPTANCE_ENDPOINT"); endpoint != "" {
		return endpoint
	}
	return "localhost:19092"
}

func receivedCount(sequences map[int]int) int {
	total := 0
	for _, count := range sequences {
		total += count
	}
	return total
}

func cleanupKafkaAcceptance(t *testing.T, conn driver.Conn, destinations []string) {
	t.Helper()
	maintenance, ok := conn.Admin().(driver.Maintenance)
	if !ok {
		_ = conn.Close(context.Background())
		t.Fatal("Kafka Admin does not implement driver.Maintenance")
	}
	cleanup := func(ctx context.Context) error {
		for _, destination := range destinations {
			if _, err := maintenance.Purge(ctx, destination); err != nil {
				if errors.Is(err, driver.ErrDestinationMissing) {
					continue
				}
				return fmt.Errorf("Purge(%q): %w", destination, err)
			}
			results, err := maintenance.Prune(ctx, []string{destination})
			if err != nil {
				return fmt.Errorf("Prune(%q): %w", destination, err)
			}
			for _, result := range results {
				if result.Name == destination && !result.Deleted {
					return fmt.Errorf("Prune(%q) kept destination: %s", destination, result.Reason)
				}
			}
		}
		return nil
	}
	preCtx, preCancel := context.WithTimeout(context.Background(), 10*time.Second)
	preErr := cleanup(preCtx)
	preCancel()
	if preErr != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = conn.Close(closeCtx)
		closeCancel()
		t.Fatalf("Kafka pre-test cleanup: %v", preErr)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := cleanup(ctx); err != nil {
			t.Errorf("Kafka cleanup: %v", err)
		}
		if err := conn.Close(ctx); err != nil {
			t.Errorf("Kafka cleanup connection Close(): %v", err)
		}
	})
}
