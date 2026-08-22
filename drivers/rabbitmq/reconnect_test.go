package rabbitmq

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	//nolint:depguard // integration test exercises the SDK through the RabbitMQ driver
	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver/conformance"
)

var rabbitReconnectSequence atomic.Uint64

type captureRabbitDriver struct {
	mu     sync.Mutex
	opened []driver.Conn
	base   Driver
}

func (d *captureRabbitDriver) Name() string { return d.base.Name() }

func (d *captureRabbitDriver) Capabilities() driver.Capabilities { return d.base.Capabilities() }

func (d *captureRabbitDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	connection, err := d.base.Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	d.opened = append(d.opened, connection)
	d.mu.Unlock()
	return connection, nil
}

func (d *captureRabbitDriver) connections() []driver.Conn {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]driver.Conn(nil), d.opened...)
}

func waitRabbit(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(15 * time.Second) //nolint:forbidigo // live broker wait is intentionally wall-clock based
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond) //nolint:forbidigo // live broker wait is intentionally wall-clock based
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("RabbitMQ reconnect condition timed out")
		case <-ticker.C:
		}
	}
}

func TestRabbitMQCoreReconnectsWithSyntheticTransientFault(t *testing.T) {
	requireBroker(t)
	sequence := rabbitReconnectSequence.Add(1)
	topic := fmt.Sprintf("reconnect.live.%d.%d", os.Getpid(), sequence)
	cfg := f1.Config{
		Env:        "test",
		Service:    "reconnect",
		InstanceID: fmt.Sprintf("%d", sequence),
		Broker: f1.BrokerConfig{
			Driver:               "rabbitmq",
			Endpoints:            []string{defaultEndpoint},
			ConnectTimeout:       5 * time.Second,
			MaxReconnectAttempts: 3,
			DefaultPrefetch:      1,
		},
		Topology: f1.TopologyConfig{AutoCreate: true},
		Lifecycle: f1.LifecycleConfig{
			DrainTimeout: 5 * time.Second,
			HandlerGrace: 500 * time.Millisecond,
			FlushTimeout: 5 * time.Second,
			CloseTimeout: 5 * time.Second,
		},
	}
	driverCapture := &captureRabbitDriver{base: Driver{}}
	client, err := f1.New(context.Background(), cfg,
		f1.WithDriver(driverCapture),
		f1.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()

	var handled atomic.Int32
	runner, err := client.Subscribe(context.Background(), f1.Subscription{
		Name:           "reconnect-live",
		Topics:         []string{topic},
		Prefetch:       12,
		HandlerTimeout: 2 * time.Second,
		Handlers: map[string]f1.Handler{
			topic: f1.HandlerFunc(func(context.Context, *f1.Event) error {
				handled.Add(1)
				return nil
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(context.Background()) }()
	waitRabbit(t, func() bool {
		connections := driverCapture.connections()
		if len(connections) != 1 {
			return false
		}
		connection, ok := connections[0].(*conn)
		if !ok {
			return false
		}
		connection.mu.RLock()
		active := len(connection.active)
		connection.mu.RUnlock()
		return active == 1
	})

	connections := driverCapture.connections()
	injector, err := rabbitFaultInjector(connections[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := injector(context.Background(), conformance.FaultConnectionDrop); err != nil {
		t.Fatal(err)
	}
	waitRabbit(t, func() bool { return len(driverCapture.connections()) == 2 })

	if _, err := client.Publisher().Publish(context.Background(), topic, map[string]string{"source": "reconnect"}); err != nil {
		t.Fatal(err)
	}
	waitRabbit(t, func() bool { return handled.Load() == 1 })
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("runner Run() = %v", err)
	}
}

func TestRabbitMQCoreRebuildsAfterLaneChannelClosure(t *testing.T) {
	requireBroker(t)
	sequence := rabbitReconnectSequence.Add(1)
	topicA := fmt.Sprintf("lane-close.live.%d.%d.a", os.Getpid(), sequence)
	topicB := fmt.Sprintf("lane-close.live.%d.%d.b", os.Getpid(), sequence)
	cfg := f1.Config{
		Env:        "test",
		Service:    "lane-close",
		InstanceID: fmt.Sprintf("%d", sequence),
		Broker: f1.BrokerConfig{
			Driver:               "rabbitmq",
			Endpoints:            []string{defaultEndpoint},
			ConnectTimeout:       5 * time.Second,
			MaxReconnectAttempts: 3,
			DefaultPrefetch:      4,
		},
		Topology: f1.TopologyConfig{
			AutoCreate: true,
			Priorities: []f1.Priority{f1.PriorityNormal},
		},
		Lifecycle: f1.LifecycleConfig{
			DrainTimeout: 5 * time.Second,
			HandlerGrace: 500 * time.Millisecond,
			FlushTimeout: 5 * time.Second,
			CloseTimeout: 5 * time.Second,
		},
	}
	driverCapture := &captureRabbitDriver{base: Driver{}}
	client, err := f1.New(context.Background(), cfg,
		f1.WithDriver(driverCapture),
		f1.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()

	var handledB atomic.Int32
	runner, err := client.Subscribe(context.Background(), f1.Subscription{
		Name:           "lane-close-live",
		Topics:         []string{topicA, topicB},
		Priorities:     []f1.Priority{f1.PriorityNormal},
		Retry:          f1.RetryConfig{MaxAttempts: 1},
		Prefetch:       2,
		HandlerTimeout: 2 * time.Second,
		Handlers: map[string]f1.Handler{
			topicA: f1.HandlerFunc(func(context.Context, *f1.Event) error { return nil }),
			topicB: f1.HandlerFunc(func(context.Context, *f1.Event) error {
				handledB.Add(1)
				return nil
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(context.Background()) }()
	waitRabbit(t, func() bool {
		connections := driverCapture.connections()
		if len(connections) != 1 {
			return false
		}
		connection, ok := connections[0].(*conn)
		if !ok {
			return false
		}
		connection.mu.RLock()
		defer connection.mu.RUnlock()
		for item := range connection.active {
			if len(item.lanes) >= 2 {
				return true
			}
		}
		return false
	})

	if _, err := client.Publisher().Publish(context.Background(), topicB, map[string]string{"phase": "before"}); err != nil {
		t.Fatal(err)
	}
	waitRabbit(t, func() bool { return handledB.Load() == 1 })
	injector, err := rabbitFaultInjector(driverCapture.connections()[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := injector(context.Background(), conformance.FaultLaneChannelClose); err != nil {
		t.Fatal(err)
	}
	waitRabbit(t, func() bool { return len(driverCapture.connections()) == 2 })
	if _, err := client.Publisher().Publish(context.Background(), topicB, map[string]string{"phase": "after"}); err != nil {
		t.Fatal(err)
	}
	waitRabbit(t, func() bool { return handledB.Load() == 2 })
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("runner Run() = %v", err)
	}
}
