package inmem

import (
	"context"
	"errors"
	"testing"
	"time"

	//nolint:depguard // integration tests exercise the SDK through the in-memory driver
	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// successorFamilyDriver exposes the raw conn so the test can observe the
// consumer attach on the driver side while the core drives everything else.
type successorFamilyDriver struct {
	Driver
	conn *conn
}

func (d *successorFamilyDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	opened, err := d.Driver.Open(ctx, cfg)
	if err == nil {
		d.conn, _ = opened.(*conn)
	}
	return opened, err
}

// TestDeadLetterSuccessorReachesTheDeclaredConsumerFamily proves the
// undeclared-family refusal against this broker's own rules: a
// publish to a destination the declared topology does not contain fails
// rather than being created lazily. A message published with a foreign event
// type onto the orders.created topic must dead-letter into the orders.created
// family; deriving the successor topic from the event type would target the
// undeclared payments.charged family and the broker would refuse every
// republish attempt, releasing the consumer instead of settling the delivery.
func TestDeadLetterSuccessorReachesTheDeclaredConsumerFamily(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	broker := &successorFamilyDriver{}
	client, err := f1.New(ctx, successorFamilyConfig(), f1.WithDriver(broker),
		f1.WithPublishTopics("orders.created"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	deadLettered := make(chan f1.DeadLettered, 4)
	runner, err := client.Subscribe(ctx, f1.Subscription{
		Name:       "orders",
		Topics:     []string{"orders.created"},
		Priorities: []f1.Priority{f1.PriorityNormal},
		Retry:      f1.RetryConfig{MaxAttempts: 1},
		OnDeadLetter: func(_ context.Context, deadLetter f1.DeadLettered) {
			deadLettered <- deadLetter
		},
		Handlers: map[string]f1.Handler{
			"payments.charged.v2": f1.HandlerFunc(func(context.Context, *f1.Event) error {
				return errors.New("terminal handler failure")
			}),
		},
		HandlerTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()

	// The subscription group consumes from latest, so anything published
	// before the consumer attaches is skipped by design. Wait for the attach
	// here, where the consumer registry is directly observable.
	realClock := clock.NewReal()
	attachCtx, attachCancel := context.WithTimeout(ctx, 10*time.Second)
	defer attachCancel()
	for {
		broker.conn.mu.Lock()
		var attached bool
		if dest := broker.conn.destinations["f1.test.orders.created.normal"]; dest != nil {
			attached = len(dest.consumers) > 0
		}
		broker.conn.mu.Unlock()
		if attached {
			break
		}
		if err := realClock.Sleep(attachCtx, 5*time.Millisecond); err != nil {
			t.Fatal("the runner never attached its consumer")
		}
	}

	if _, err := client.Publisher().Publish(ctx, "payments.charged.v2", map[string]string{"id": "1"}, f1.WithTopic("orders.created")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	watchdog := realClock.Timer(10 * time.Second)
	defer watchdog.Stop()
	select {
	case deadLetter := <-deadLettered:
		// The consuming family's DLQ for env=test, logical topic
		// orders.created, subscription orders.
		const want = "f1.test.orders.created.dlq.orders"
		if deadLetter.Destination != want {
			t.Fatalf("dead-letter destination = %q, want %q", deadLetter.Destination, want)
		}
	case err := <-runDone:
		t.Fatalf("runner stopped before the dead-letter hand-off: %v", err)
	case <-watchdog.C:
		t.Fatal("no DeadLettered notification arrived; the successor likely targeted an undeclared family and was refused")
	}
}

func successorFamilyConfig() f1.Config {
	return f1.Config{
		Env:     "test",
		Service: "orders",
		Broker: f1.BrokerConfig{
			Driver:          "inmem",
			ConnectTimeout:  5 * time.Second,
			DefaultPrefetch: 8,
		},
		Topology: f1.TopologyConfig{
			AutoCreate: true,
			Priorities: []f1.Priority{f1.PriorityNormal},
		},
		Codec: f1.CodecConfig{
			Default:        "json",
			ContentMode:    "binary",
			MaxHeaderBytes: f1.CoreMaxHeaderBytes,
			MaxBodyBytes:   1 << 20,
		},
		Lifecycle: f1.LifecycleConfig{
			DrainTimeout:          5 * time.Second,
			HandlerGrace:          time.Second,
			FlushTimeout:          time.Second,
			CloseTimeout:          time.Second,
			RebalanceDrainTimeout: time.Second,
		},
	}
}
