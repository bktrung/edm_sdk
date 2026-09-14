//go:build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	//nolint:depguard // this test must exercise the public f1 API against Kafka.
	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestPublicRunnerConsumesKafkaMessagesAfterAssignmentNotification(t *testing.T) {
	requireBroker(t)
	adminCtx, _, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "public-runner")
	physicalTopic := fmt.Sprintf("f1.test.%s.medium", topic)
	group := kafkaTestTopic(t, "public-runner-group")
	deadLetterTopic := fmt.Sprintf("f1.test.%s.dlq.%s", topic, group)
	unknownDeadLetterTopic := fmt.Sprintf("f1.test.unknown.dlq.%s", group)
	cleanupKafkaTopics(t, admin, physicalTopic, deadLetterTopic, unknownDeadLetterTopic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, adminCtx, physicalTopic, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	client, err := f1.New(ctx, kafkaPublicTestConfig(),
		f1.WithDriver(Driver{}),
		f1.WithTopology(f1.TopologyDeclare),
		f1.WithPublishTopics(topic),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = client.Close(context.Background())
		}
	})

	const (
		eventType = "kafka.rebalance.notification.v1"
		published = 5
	)
	var (
		arrivals   atomic.Int64
		receivedMu sync.Mutex
		received   = make(map[string]int, published)
		done       = make(chan struct{})
		doneOnce   sync.Once
	)
	runner, err := client.Subscribe(ctx, f1.Subscription{
		Name:           group,
		Topics:         []string{topic},
		Concurrency:    1,
		Prefetch:       1,
		Priorities:     []f1.Priority{f1.PriorityMedium},
		Retry:          f1.RetryConfig{MaxAttempts: 1},
		HandlerTimeout: 5 * time.Second,
		Handlers: map[string]f1.Handler{
			eventType: f1.HandlerFunc(func(_ context.Context, event *f1.Event) error {
				var message rebalanceNotificationMessage
				if err := event.Decode(&message); err != nil {
					return f1.Terminal(err)
				}
				receivedMu.Lock()
				received[event.ID()]++
				receivedMu.Unlock()
				if arrivals.Add(1) == published {
					doneOnce.Do(func() { close(done) })
				}
				return nil
			}),
		},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()

	expected := make(map[string]struct{}, published)
	for sequence := range published {
		message := rebalanceNotificationMessage{ID: fmt.Sprintf("message-%d", sequence)}
		publishedID, err := client.Publisher().Publish(ctx, eventType, message, f1.WithTopic(topic), f1.WithIdempotencyKey(message.ID))
		if err != nil {
			t.Fatalf("Publish(%q): %v", message.ID, err)
		}
		expected[publishedID] = struct{}{}
	}

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("received %d of %d messages: %v", arrivals.Load(), published, ctx.Err())
	}

	receivedMu.Lock()
	got := make(map[string]int, len(received))
	for id, count := range received {
		got[id] = count
	}
	receivedMu.Unlock()
	if gotCount := arrivals.Load(); gotCount != published {
		t.Fatalf("handler arrivals = %d, want %d", gotCount, published)
	}
	if len(got) != len(expected) {
		t.Fatalf("received identities = %#v, want %#v", got, expected)
	}
	for id := range expected {
		if got[id] != 1 {
			t.Fatalf("received identity %q count = %d, want 1; all=%#v", id, got[id], got)
		}
	}
	t.Logf("Kafka public Runner observed message count=%d identities=%v", arrivals.Load(), got)

	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed = true
	runWaitCtx, runWaitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	select {
	case runErr := <-runDone:
		runWaitCancel()
		if runErr != nil && !errors.Is(runErr, context.Canceled) {
			t.Fatalf("Runner.Run: %v", runErr)
		}
	case <-runWaitCtx.Done():
		runWaitCancel()
		t.Fatal("Runner.Run did not stop after Close")
	}
}

func TestPublicRunnerReconnectsAfterTransientConsumerError(t *testing.T) {
	requireBroker(t)
	adminCtx, _, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "public-reconnect")
	physicalTopic := fmt.Sprintf("f1.test.%s.medium", topic)
	group := kafkaTestTopic(t, "public-reconnect-group")
	cleanupKafkaTopics(t, admin, physicalTopic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, adminCtx, physicalTopic, 1)

	driverUnderTest := &recordingKafkaDriver{created: make(chan *consumer, 8)}
	client, err := f1.New(context.Background(), kafkaPublicTestConfig(),
		f1.WithDriver(driverUnderTest),
		f1.WithTopology(f1.TopologyNone),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	closed := false
	runCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		if !closed {
			_ = client.Close(context.Background())
		}
	})

	runner, err := client.Subscribe(runCtx, f1.Subscription{
		Name:           group,
		Topics:         []string{topic},
		Concurrency:    1,
		Prefetch:       1,
		Priorities:     []f1.Priority{f1.PriorityMedium},
		Retry:          f1.RetryConfig{MaxAttempts: 1},
		HandlerTimeout: 5 * time.Second,
		Handlers: map[string]f1.Handler{
			"kafka.rebalance.notification.v1": f1.HandlerFunc(func(context.Context, *f1.Event) error { return nil }),
		},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(runCtx) }()

	first := receiveRecordedKafkaConsumer(t, driverUnderTest.created)
	waitForKafkaConsumerState(t, first, "initial assignment", func() bool {
		first.mu.Lock()
		defer first.mu.Unlock()
		return len(first.activeGenerations) > 0
	})
	stillRunningCtx, stillRunningCancel := context.WithTimeout(context.Background(), time.Second)
	select {
	case runErr := <-runDone:
		stillRunningCancel()
		t.Fatalf("Runner.Run stopped on assignment notification: %v", runErr)
	case <-stillRunningCtx.Done():
	}
	stillRunningCancel()

	first.sendError(classify("consumer", driver.KindTransient, errors.New("injected transient consumer failure")))
	second := receiveRecordedKafkaConsumer(t, driverUnderTest.created)
	if first == second {
		t.Fatal("transient consumer error reused the original consumer")
	}
	waitForKafkaConsumerState(t, second, "reconnected assignment", func() bool {
		second.mu.Lock()
		defer second.mu.Unlock()
		return len(second.activeGenerations) > 0
	})

	cancel()
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed = true
	runWaitCtx, runWaitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	select {
	case runErr := <-runDone:
		runWaitCancel()
		if runErr != nil && !errors.Is(runErr, context.Canceled) {
			t.Fatalf("Runner.Run after cancellation: %v", runErr)
		}
	case <-runWaitCtx.Done():
		runWaitCancel()
		t.Fatal("Runner.Run did not stop after cancellation")
	}
}

type rebalanceNotificationMessage struct {
	ID string `json:"id"`
}

func kafkaPublicTestConfig() f1.Config {
	return f1.Config{
		Env:     "test",
		Service: "kafka-rebalance-notification",
		Broker: f1.BrokerConfig{
			Driver:          "kafka",
			Endpoints:       []string{kafkaEndpoint},
			ConnectTimeout:  10 * time.Second,
			DefaultPrefetch: 1,
		},
		Topology: f1.TopologyConfig{
			Priorities: []f1.Priority{f1.PriorityMedium},
		},
		Codec: f1.CodecConfig{
			Default:        "json",
			ContentMode:    "binary",
			MaxHeaderBytes: f1.CoreMaxHeaderBytes,
			MaxBodyBytes:   1 << 20,
		},
		Lifecycle: f1.LifecycleConfig{
			DrainTimeout:          10 * time.Second,
			HandlerGrace:          time.Second,
			FlushTimeout:          time.Second,
			CloseTimeout:          time.Second,
			RebalanceDrainTimeout: time.Second,
		},
	}
}

type recordingKafkaDriver struct {
	created chan *consumer
}

func (*recordingKafkaDriver) Name() string { return "kafka" }

func (recordingKafkaDriver) Capabilities() driver.Capabilities { return (Driver{}).Capabilities() }

func (d *recordingKafkaDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	opened, err := (Driver{}).Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &recordingKafkaConn{Conn: opened, created: d.created}, nil
}

type recordingKafkaConn struct {
	driver.Conn
	created chan *consumer
}

func (c *recordingKafkaConn) Consumer(ctx context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	value, err := c.Conn.Consumer(ctx, cfg)
	if err == nil {
		if consumerValue, ok := value.(*consumer); ok {
			select {
			case c.created <- consumerValue:
			default:
			}
		}
	}
	return value, err
}

func receiveRecordedKafkaConsumer(t *testing.T, consumers <-chan *consumer) *consumer {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	select {
	case consumerValue := <-consumers:
		return consumerValue
	case <-ctx.Done():
		t.Fatalf("Kafka consumer was not created: %v", ctx.Err())
		return nil
	}
}
