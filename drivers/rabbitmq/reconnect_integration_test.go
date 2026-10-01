//go:build integration

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

// rabbitManagementPort derives the fixture's management port from the AMQP
// endpoint the suite is pointed at, the way the driver does when no
// rabbitmq.managementPort option is set. A test that hides the AMQP port
// behind a proxy has to pass this value back to the driver.
func rabbitManagementPort(t *testing.T) int {
	t.Helper()
	parsed, err := url.Parse(defaultEndpoint)
	if err != nil {
		t.Fatalf("parse %s: %v", rabbitMQEndpointEnv, err)
	}
	port, err := strconv.Atoi(parsed.Port())
	if err != nil {
		t.Fatalf("%s must carry an explicit port, got %q", rabbitMQEndpointEnv, parsed.Port())
	}
	return port + 10000
}

// rabbitResetProxy forwards AMQP connections to the fixture and keeps every
// socket pair it opens, so a test can break the transport under the driver
// without asking the broker to close anything.
type rabbitResetProxy struct {
	listener net.Listener
	target   string
	mu       sync.Mutex
	pairs    []*rabbitResetPair
}

type rabbitResetPair struct {
	client   net.Conn
	upstream net.Conn
}

func newRabbitResetProxy(t *testing.T, target string) *rabbitResetProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for the AMQP proxy: %v", err)
	}
	proxy := &rabbitResetProxy{listener: listener, target: target}
	t.Cleanup(proxy.stop)
	go proxy.serve()
	return proxy
}

// endpoint rewrites the fixture endpoint onto the proxy and keeps its
// credentials, so the driver still reads a loopback plaintext endpoint.
func (p *rabbitResetProxy) endpoint(t *testing.T) string {
	t.Helper()
	parsed, err := url.Parse(defaultEndpoint)
	if err != nil {
		t.Fatalf("parse %s: %v", rabbitMQEndpointEnv, err)
	}
	parsed.Host = p.listener.Addr().String()
	return parsed.String()
}

func (p *rabbitResetProxy) serve() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		upstream, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.mu.Lock()
		p.pairs = append(p.pairs, &rabbitResetPair{client: client, upstream: upstream})
		p.mu.Unlock()
		go func() { _, _ = io.Copy(upstream, client) }()
		go func() { _, _ = io.Copy(client, upstream) }()
	}
}

// reset drops every socket pair opened so far with an RST instead of a FIN, so
// the driver reads a failed socket rather than the broker's close frame. The
// listener stays open, which is what lets the driver reconnect through it.
func (p *rabbitResetProxy) reset() {
	p.mu.Lock()
	pairs := append([]*rabbitResetPair(nil), p.pairs...)
	p.mu.Unlock()
	for _, pair := range pairs {
		resetRabbitSocket(pair.client)
		resetRabbitSocket(pair.upstream)
	}
}

// resetRabbitSocket sets SO_LINGER to zero before closing, which makes the
// kernel send an RST and discard whatever was buffered. A plain Close would
// send a FIN, which the driver reads as a clean shutdown and the broker as a
// client disconnect.
func resetRabbitSocket(socket net.Conn) {
	if tcp, ok := socket.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}
	_ = socket.Close()
}

func (p *rabbitResetProxy) stop() {
	_ = p.listener.Close()
	p.mu.Lock()
	pairs := append([]*rabbitResetPair(nil), p.pairs...)
	p.mu.Unlock()
	for _, pair := range pairs {
		_ = pair.client.Close()
		_ = pair.upstream.Close()
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

// TestRabbitMQCoreReconnectsAfterSocketReset resets the TCP socket under a live
// subscription, which is how a consumer meets a lost transport: the driver
// fails to read and reports a client-side close, and the core has to reconnect.
// The tests above close the connection through the broker's management API,
// which arrives as a server-sent frame and takes the other path.
func TestRabbitMQCoreReconnectsAfterSocketReset(t *testing.T) {
	requireBroker(t)
	sequence := rabbitReconnectSequence.Add(1)
	topic := fmt.Sprintf("socket-reset.live.%d.%d", os.Getpid(), sequence)
	proxy := newRabbitResetProxy(t, brokerAddress(defaultEndpoint))
	cfg := f1.Config{
		Env:        "test",
		Service:    "socket-reset",
		InstanceID: fmt.Sprintf("%d", sequence),
		Broker: f1.BrokerConfig{
			Driver:               "rabbitmq",
			Endpoints:            []string{proxy.endpoint(t)},
			ConnectTimeout:       5 * time.Second,
			MaxReconnectAttempts: 3,
			DefaultPrefetch:      1,
			// The proxy port hides the fixture's AMQP port, and the driver
			// derives the management port from the endpoint it was handed.
			DriverOptions: map[string]string{
				"rabbitmq.managementPort": strconv.Itoa(rabbitManagementPort(t)),
			},
		},
		Topology: f1.TopologyConfig{AutoCreate: true},
		Lifecycle: f1.LifecycleConfig{
			DrainTimeout: 5 * time.Second,
			HandlerGrace: 500 * time.Millisecond,
			CloseTimeout: 5 * time.Second,
		},
	}
	driverCapture := &captureRabbitDriver{base: Driver{}}
	consumerErrors := make(chan error, 8)
	client, err := f1.New(context.Background(), cfg,
		f1.WithDriver(driverCapture),
		f1.WithErrorHandler(func(_ context.Context, _ *f1.Event, err error) {
			select {
			case consumerErrors <- err:
			default:
			}
		}),
		f1.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatal(err)
	}
	cleanupPrefix := fmt.Sprintf("f1.test.%s", topic)
	t.Cleanup(func() {
		_ = client.Close(context.Background())
		cleanupRabbitTestQueues(t, driverCapture, cleanupPrefix, "f1.test.unknown.dlq.socket-reset-live")
	})

	var handled atomic.Int32
	runner, err := client.Subscribe(context.Background(), f1.Subscription{
		Name:           "socket-reset-live",
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
	if _, err := client.Publisher().Publish(context.Background(), topic, map[string]string{"phase": "before"}); err != nil {
		t.Fatal(err)
	}
	waitRabbit(t, func() bool { return handled.Load() == 1 })

	proxy.reset()

	// The reset must reach the core as a transient consumer error. A fatal one
	// cancels the runner, so the replacement wait below would fail as an
	// unattributed timeout; this names the cause instead.
	var resetErr error
	timer := time.NewTimer(15 * time.Second) //nolint:forbidigo // live broker reconnect wait is intentionally wall-clock based
	defer timer.Stop()
	select {
	case resetErr = <-consumerErrors:
	case <-timer.C:
		t.Fatal("the core reported no consumer error after the socket reset")
	}
	if kind, classified := driver.Classify(resetErr); !classified || kind != driver.KindTransient {
		t.Fatalf("consumer error after the socket reset = %v, kind %v, classified %t; want a classified transient error", resetErr, kind, classified)
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
		t.Fatalf("client.Close() = %v, want nil", err)
	}
	runTimer := time.NewTimer(15 * time.Second) //nolint:forbidigo // live broker shutdown is intentionally wall-clock based
	defer runTimer.Stop()
	select {
	case runErr := <-runDone:
		if runErr != nil {
			t.Fatalf("runner Run() = %v, want nil", runErr)
		}
	case <-runTimer.C:
		t.Fatal("runner Run() did not return after Close")
	}
	if got := len(driverCapture.connections()); got != 2 {
		t.Fatalf("driver connections after Close = %d, want 2: a reconnect must not start once Close begins", got)
	}
	// A reconnect the shutdown cancelled before its dial finished raises the
	// attempt count without ever appending to the connection list, so the
	// count is what catches one the list cannot show.
	if got := driverCapture.openAttempts(); got != 2 {
		t.Fatalf("driver Open attempts after Close = %d, want 2: a reconnect must not start once Close begins", got)
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
