package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
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
	mu        sync.Mutex
	opened    []driver.Conn
	base      Driver
	attempts  atomic.Int32
	failOpens atomic.Int64
}

func (d *captureRabbitDriver) Name() string { return d.base.Name() }

func (d *captureRabbitDriver) Capabilities() driver.Capabilities { return d.base.Capabilities() }

func (d *captureRabbitDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	d.attempts.Add(1)
	for {
		remaining := d.failOpens.Load()
		if remaining <= 0 {
			break
		}
		if d.failOpens.CompareAndSwap(remaining, remaining-1) {
			return nil, classify("open", driver.KindTransient, errors.New("test injected open failure"))
		}
	}
	connection, err := d.base.Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	d.opened = append(d.opened, connection)
	d.mu.Unlock()
	return connection, nil
}

func (d *captureRabbitDriver) failNextOpens(count int) {
	d.failOpens.Store(int64(count))
}

func (d *captureRabbitDriver) openAttempts() int {
	return int(d.attempts.Load())
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

func activeRabbitConsumer(connection *conn) *consumer {
	connection.mu.RLock()
	defer connection.mu.RUnlock()
	for item := range connection.active {
		return item
	}
	return nil
}

func waitRabbitConsumerReplacement(t *testing.T, capture *captureRabbitDriver, before *consumer) {
	t.Helper()
	waitRabbit(t, func() bool {
		for _, opened := range capture.connections() {
			connection, ok := opened.(*conn)
			if !ok {
				continue
			}
			connection.mu.RLock()
			for item := range connection.active {
				if item != before {
					connection.mu.RUnlock()
					return true
				}
			}
			connection.mu.RUnlock()
		}
		return false
	})
}

func closeRabbitConnection(ctx context.Context, connection *conn, clientID string) error {
	localPort := -1
	if local := connection.amqp.LocalAddr(); local != nil {
		_, port, splitErr := net.SplitHostPort(local.String())
		if splitErr == nil {
			localPort, _ = strconv.Atoi(port)
		}
	}
	deadline := time.NewTimer(5 * time.Second) //nolint:forbidigo // broker metadata may lag the live AMQP socket
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond) //nolint:forbidigo // broker metadata may lag the live AMQP socket
	defer ticker.Stop()
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, connection.management.baseURL+"/api/connections", nil)
		if err != nil {
			return err
		}
		request.SetBasicAuth(connection.management.username, connection.management.password)
		response, err := connection.management.client.Do(request)
		if err != nil {
			return err
		}
		var connections []map[string]any
		decodeErr := json.NewDecoder(response.Body).Decode(&connections)
		_ = response.Body.Close()
		if decodeErr != nil {
			return decodeErr
		}
		if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
			return fmt.Errorf("management API GET connections: %s", response.Status)
		}
		for _, item := range connections {
			name, _ := item["name"].(string)
			provided, _ := item["user_provided_name"].(string)
			if provided == "" {
				if properties, ok := item["client_properties"].(map[string]any); ok {
					provided, _ = properties["connection_name"].(string)
				}
			}
			peerPort, _ := item["peer_port"].(float64)
			if provided != clientID && int(peerPort) != localPort {
				continue
			}
			request, err := http.NewRequestWithContext(ctx, http.MethodDelete, connection.management.baseURL+"/api/connections/"+url.PathEscape(name), nil)
			if err != nil {
				return err
			}
			request.SetBasicAuth(connection.management.username, connection.management.password)
			response, err := connection.management.client.Do(request)
			if err != nil {
				return err
			}
			_ = response.Body.Close()
			if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
				return fmt.Errorf("management API DELETE connection %q: %s", name, response.Status)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("RabbitMQ connection %q did not appear in management API", clientID)
		case <-ticker.C:
		}
	}
}

func cleanupRabbitTestQueues(t *testing.T, capture *captureRabbitDriver, prefixes ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	seen := make(map[string]struct{})
	for _, opened := range capture.connections() {
		connection, ok := opened.(*conn)
		if !ok || connection.management == nil {
			continue
		}
		queues, err := connection.management.listQueues(ctx)
		if err != nil {
			t.Errorf("list live-test queues: %v", err)
			continue
		}
		for _, queue := range queues {
			matchesPrefix := false
			for _, prefix := range prefixes {
				if strings.HasPrefix(queue.Name, prefix) {
					matchesPrefix = true
					break
				}
			}
			if !matchesPrefix {
				continue
			}
			if _, alreadySeen := seen[queue.Name]; alreadySeen {
				continue
			}
			seen[queue.Name] = struct{}{}
			if _, err := connection.management.deleteQueue(ctx, queue.Name); err != nil {
				t.Errorf("delete live-test queue %q: %v", queue.Name, err)
			}
		}
	}
}

func TestRabbitMQCoreRepairsAfterSyntheticTransientFault(t *testing.T) {
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
	t.Cleanup(func() {
		_ = client.Close(context.Background())
		cleanupRabbitTestQueues(t, driverCapture, "f1.test."+topic, "f1.test.unknown.dlq.reconnect-live")
	})

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
	connection, ok := connections[0].(*conn)
	if !ok {
		t.Fatal("captured connection has unexpected type")
	}
	before := activeRabbitConsumer(connection)
	if before == nil {
		t.Fatal("no active consumer before synthetic fault")
	}
	injector, err := rabbitFaultInjector(connection)
	if err != nil {
		t.Fatal(err)
	}
	if err := injector(context.Background(), conformance.FaultConnectionDrop); err != nil {
		t.Fatal(err)
	}
	waitRabbitConsumerReplacement(t, driverCapture, before)
	if got := len(driverCapture.connections()); got != 1 {
		t.Fatalf("driver Open count = %d, want 1 during lane repair", got)
	}

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

func TestRabbitMQCoreReconnectsAfterRealConnectionDeath(t *testing.T) {
	requireBroker(t)
	sequence := rabbitReconnectSequence.Add(1)
	topic := fmt.Sprintf("connection-death.live.%d.%d", os.Getpid(), sequence)
	cfg := f1.Config{
		Env:        "test",
		Service:    "connection-death",
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
	cleanupPrefix := fmt.Sprintf("f1.test.%s", topic)
	t.Cleanup(func() {
		_ = client.Close(context.Background())
		cleanupRabbitTestQueues(t, driverCapture, cleanupPrefix, "f1.test.unknown.dlq.connection-death")
	})

	var handled atomic.Int32
	runner, err := client.Subscribe(context.Background(), f1.Subscription{
		Name:           "connection-death-live",
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
		return ok && activeRabbitConsumer(connection) != nil
	})
	connection, ok := driverCapture.connections()[0].(*conn)
	if !ok {
		t.Fatal("captured connection has unexpected type")
	}
	if _, err := client.Publisher().Publish(context.Background(), topic, map[string]string{"phase": "before"}); err != nil {
		t.Fatal(err)
	}
	waitRabbit(t, func() bool { return handled.Load() == 1 })
	if err := closeRabbitConnection(context.Background(), connection, cfg.InstanceID); err != nil {
		t.Fatal(err)
	}
	waitRabbit(t, func() bool {
		connections := driverCapture.connections()
		if len(connections) != 2 {
			return false
		}
		replacement, ok := connections[1].(*conn)
		return ok && activeRabbitConsumer(replacement) != nil
	})
	if _, err := client.Publisher().Publish(context.Background(), topic, map[string]string{"phase": "after"}); err != nil {
		t.Fatal(err)
	}
	waitRabbit(t, func() bool { return handled.Load() == 2 })
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("runner Run() = %v", err)
	}
}

func TestRabbitMQCoreReconnectBudgetAfterRealConnectionDeath(t *testing.T) {
	requireBroker(t)
	sequence := rabbitReconnectSequence.Add(1)
	topic := fmt.Sprintf("connection-budget.live.%d.%d", os.Getpid(), sequence)
	cfg := f1.Config{
		Env:        "test",
		Service:    "connection-budget",
		InstanceID: fmt.Sprintf("%d", sequence),
		Broker: f1.BrokerConfig{
			Driver:               "rabbitmq",
			Endpoints:            []string{defaultEndpoint},
			ConnectTimeout:       5 * time.Second,
			MaxReconnectAttempts: 2,
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
	cleanupPrefix := fmt.Sprintf("f1.test.%s", topic)
	t.Cleanup(func() {
		_ = client.Close(context.Background())
		cleanupRabbitTestQueues(t, driverCapture, cleanupPrefix, "f1.test.unknown.dlq.connection-budget")
	})

	var handled atomic.Int32
	runner, err := client.Subscribe(context.Background(), f1.Subscription{
		Name:           "connection-budget-live",
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
		return ok && activeRabbitConsumer(connection) != nil
	})
	connection, ok := driverCapture.connections()[0].(*conn)
	if !ok {
		t.Fatal("captured connection has unexpected type")
	}
	if _, err := client.Publisher().Publish(context.Background(), topic, map[string]string{"phase": "before"}); err != nil {
		t.Fatal(err)
	}
	waitRabbit(t, func() bool { return handled.Load() == 1 })
	driverCapture.failNextOpens(cfg.Broker.MaxReconnectAttempts)
	if err := closeRabbitConnection(context.Background(), connection, cfg.InstanceID); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(15 * time.Second) //nolint:forbidigo // live broker reconnect budget is intentionally wall-clock based
	defer timer.Stop()
	var runErr error
	select {
	case runErr = <-runDone:
	case <-timer.C:
		t.Fatal("runner did not exit after reconnect budget exhaustion")
	}
	if runErr == nil {
		t.Fatal("runner Run() = nil after reconnect budget exhaustion")
	}
	if kind, classified := driver.Classify(runErr); !classified || kind != driver.KindFatal {
		t.Fatalf("runner Run() = %v, want classified fatal", runErr)
	}
	if got := driverCapture.openAttempts(); got != 3 {
		t.Fatalf("driver Open attempts = %d, want initial open plus two reconnect attempts", got)
	}
	if got := len(driverCapture.connections()); got != 1 {
		t.Fatalf("successful driver connections = %d, want 1 after exhausted reconnects", got)
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
			Priorities: []f1.Priority{f1.PriorityMedium},
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
	cleanupPrefix := fmt.Sprintf("f1.test.lane-close.live.%d.%d.", os.Getpid(), sequence)
	t.Cleanup(func() {
		_ = client.Close(context.Background())
		cleanupRabbitTestQueues(t, driverCapture, cleanupPrefix, "f1.test.unknown.dlq.lane-close-live")
	})

	var handledB atomic.Int32
	runner, err := client.Subscribe(context.Background(), f1.Subscription{
		Name:           "lane-close-live",
		Topics:         []string{topicA, topicB},
		Priorities:     []f1.Priority{f1.PriorityMedium},
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

	connections := driverCapture.connections()
	connection, ok := connections[0].(*conn)
	if !ok {
		t.Fatal("captured connection has unexpected type")
	}
	before := activeRabbitConsumer(connection)
	if before == nil {
		t.Fatal("no active consumer before lane closure")
	}
	if _, err := client.Publisher().Publish(context.Background(), topicB, map[string]string{"phase": "before"}); err != nil {
		t.Fatal(err)
	}
	waitRabbit(t, func() bool { return handledB.Load() == 1 })
	injector, err := rabbitFaultInjector(connection)
	if err != nil {
		t.Fatal(err)
	}
	if err := injector(context.Background(), conformance.FaultLaneChannelClose); err != nil {
		t.Fatal(err)
	}
	waitRabbitConsumerReplacement(t, driverCapture, before)
	if got := len(driverCapture.connections()); got != 1 {
		t.Fatalf("driver Open count = %d, want 1 during lane repair", got)
	}
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
