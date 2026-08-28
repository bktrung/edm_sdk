package f1

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/retry"
)

type reconnectTestDriver struct {
	mu            sync.Mutex
	opens         int
	failOpens     int
	failConsumers int
	connections   []*reconnectTestConn
	created       chan *reconnectTestConsumer
}

func (d *reconnectTestDriver) Name() string { return "reconnect-test" }

func (d *reconnectTestDriver) Capabilities() driver.Capabilities {
	return driver.Capabilities{PerMessageAck: true, NativeDeliveryCount: true}
}

func (d *reconnectTestDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.opens++
	if d.opens > 1 && d.failOpens > 0 {
		d.failOpens--
		return nil, &driver.Error{Driver: d.Name(), Op: "open", K: driver.KindTransient, Err: errors.New("open failed")}
	}
	conn := &reconnectTestConn{driver: d, admin: &reconnectTestAdmin{}}
	d.connections = append(d.connections, conn)
	return conn, nil
}

func (d *reconnectTestDriver) OpenCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.opens
}

func (d *reconnectTestDriver) setFailOpens(n int) {
	d.mu.Lock()
	d.failOpens = n
	d.mu.Unlock()
}

func (d *reconnectTestDriver) setFailConsumers(n int) {
	d.mu.Lock()
	d.failConsumers = n
	d.mu.Unlock()
}

type reconnectTestConn struct {
	driver   *reconnectTestDriver
	admin    *reconnectTestAdmin
	closed   atomic.Bool
	producer atomic.Int32
}

func (c *reconnectTestConn) Capabilities() driver.Capabilities { return c.driver.Capabilities() }
func (*reconnectTestConn) BrokerInfo() driver.BrokerInfo {
	return driver.BrokerInfo{Kind: "reconnect-test", Version: "1"}
}

func (c *reconnectTestConn) Producer(context.Context, driver.ProducerConfig) (driver.Producer, error) {
	if c.closed.Load() {
		return nil, &driver.Error{Driver: c.driver.Name(), Op: "producer", K: driver.KindTransient, Err: errors.New("connection closed")}
	}
	c.producer.Add(1)
	return &reconnectTestProducer{conn: c}, nil
}

func (c *reconnectTestConn) Consumer(_ context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	c.driver.mu.Lock()
	if c.driver.failConsumers > 0 {
		c.driver.failConsumers--
		c.driver.mu.Unlock()
		return nil, &driver.Error{Driver: c.driver.Name(), Op: "consumer", K: driver.KindTransient, Err: errors.New("consumer failed")}
	}
	c.driver.mu.Unlock()
	if c.closed.Load() {
		return nil, &driver.Error{Driver: c.driver.Name(), Op: "consumer", K: driver.KindTransient, Err: errors.New("connection closed")}
	}
	consumer := &reconnectTestConsumer{
		group:    cfg.Group,
		messages: make(chan driver.InboundMessage, 8),
		errors:   make(chan error, 8),
	}
	select {
	case c.driver.created <- consumer:
	default:
	}
	return consumer, nil
}
func (c *reconnectTestConn) Admin() driver.Admin      { return c.admin }
func (*reconnectTestConn) Ping(context.Context) error { return nil }
func (c *reconnectTestConn) Close(context.Context) error {
	c.closed.Store(true)
	return nil
}

type reconnectTestAdmin struct {
	ensures atomic.Int32
}

func (a *reconnectTestAdmin) EnsureTopology(context.Context, driver.TopologySpec) (driver.TopologyDiff, error) {
	a.ensures.Add(1)
	return driver.TopologyDiff{}, nil
}

func (*reconnectTestAdmin) DescribeTopology(context.Context, []string) (driver.TopologyState, error) {
	return driver.TopologyState{}, driver.ErrUnsupported
}

func (*reconnectTestAdmin) Purge(context.Context, string) (int64, error) {
	return 0, driver.ErrUnsupported
}

func (*reconnectTestAdmin) Prune(context.Context, []string) ([]driver.PruneResult, error) {
	return nil, driver.ErrUnsupported
}

type reconnectTestProducer struct {
	conn *reconnectTestConn
}

func (*reconnectTestProducer) Publish(context.Context, ...driver.OutboundMessage) error { return nil }
func (*reconnectTestProducer) Flush(context.Context) error                              { return nil }
func (p *reconnectTestProducer) Close(context.Context) error {
	p.conn.producer.Add(-1)
	return nil
}

type reconnectTestConsumer struct {
	group        string
	messages     chan driver.InboundMessage
	errors       chan error
	drainStarted chan struct{}
	drainRelease <-chan struct{}
	drainOnce    sync.Once
	once         sync.Once
	mu           sync.RWMutex
	closed       atomic.Bool
}

func (c *reconnectTestConsumer) Messages() <-chan driver.InboundMessage { return c.messages }
func (c *reconnectTestConsumer) Errors() <-chan error                   { return c.errors }
func (*reconnectTestConsumer) Pause(...string) error                    { return nil }
func (*reconnectTestConsumer) Resume(...string) error                   { return nil }
func (c *reconnectTestConsumer) Drain(context.Context) error {
	if c.drainStarted == nil {
		return nil
	}
	c.drainOnce.Do(func() { close(c.drainStarted) })
	<-c.drainRelease
	c.close()
	return nil
}

func (c *reconnectTestConsumer) Stop(context.Context) error {
	c.close()
	return nil
}

func (c *reconnectTestConsumer) Release(context.Context) error {
	if c.drainRelease != nil {
		return nil
	}
	c.close()
	return nil
}

func (c *reconnectTestConsumer) close() {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed.Store(true)
		close(c.messages)
		close(c.errors)
		c.mu.Unlock()
	})
}

func (*reconnectTestConsumer) Lag(context.Context) (map[string]int64, error) {
	return nil, driver.ErrUnsupported
}

func (c *reconnectTestConsumer) sendError(err error) {
	c.errors <- err
}

func (c *reconnectTestConsumer) send(message driver.InboundMessage) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return false
	}
	c.messages <- message
	return true
}

type reconnectTestSettler struct{}

func (*reconnectTestSettler) Ack(ctx context.Context) error {
	return ctx.Err()
}

func (*reconnectTestSettler) Nack(ctx context.Context, _ driver.NackOptions) error {
	return ctx.Err()
}

type recordingClock struct {
	*clock.Fake
	mu           sync.Mutex
	sleeps       []time.Duration
	sleepStarted chan int
}

func (c *recordingClock) Sleep(ctx context.Context, duration time.Duration) error {
	before := c.NumWaiters()
	c.mu.Lock()
	c.sleeps = append(c.sleeps, duration)
	started := c.sleepStarted
	c.mu.Unlock()
	if started != nil {
		select {
		case started <- before:
		default:
		}
	}
	return c.Fake.Sleep(ctx, duration)
}

func (c *recordingClock) sleepCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.sleeps)
}

func (c *recordingClock) sleepAt(index int) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sleeps[index]
}

func newReconnectTestClient(t *testing.T, d *reconnectTestDriver, c clock.Clock, maxAttempts int, extra ...Option) *Client {
	t.Helper()
	cfg := testClientConfig(t)
	cfg.Broker.MaxReconnectAttempts = maxAttempts
	cfg.Lifecycle.DrainTimeout = 500 * time.Millisecond
	cfg.Lifecycle.HandlerGrace = 100 * time.Millisecond
	cfg.Lifecycle.FlushTimeout = 100 * time.Millisecond
	cfg.Lifecycle.CloseTimeout = 100 * time.Millisecond
	options := []Option{
		WithDriver(d),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	}
	options = append(options, extra...)
	if c != nil {
		options = append(options, WithClock(c))
	}
	client, err := New(context.Background(), cfg, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client
}

func TestRequestAndWaitReconnectPrefersRequestError(t *testing.T) {
	driver := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 1)}
	client := newReconnectTestClient(t, driver, nil, 0)
	requestErr := errors.New("request reconnect failed")
	client.mu.Lock()
	client.reconnectErr = requestErr
	client.mu.Unlock()

	runner := &Runner{client: client}
	if err := runner.requestAndWaitReconnect(context.Background(), errors.New("cause")); !errors.Is(err, requestErr) {
		t.Fatalf("requestAndWaitReconnect() = %v, want request error %v", err, requestErr)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.reconnect != nil {
		t.Fatal("request error unexpectedly created a reconnect attempt")
	}
}

func TestRequestAndWaitReconnectWaitsAfterSuccessfulRequest(t *testing.T) {
	driver := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 1)}
	client := newReconnectTestClient(t, driver, nil, 0)
	attempt := &reconnectAttempt{done: make(chan struct{})}
	client.mu.Lock()
	client.reconnecting = true
	client.reconnect = attempt
	client.mu.Unlock()

	wantErr := errors.New("reconnect wait failed")
	result := make(chan error, 1)
	go func() {
		result <- (&Runner{client: client}).requestAndWaitReconnect(context.Background(), errors.New("cause"))
	}()
	select {
	case err := <-result:
		t.Fatalf("requestAndWaitReconnect() returned before reconnect completion: %v", err)
	case <-clock.NewReal().Timer(20 * time.Millisecond).C:
	}
	client.finishReconnect(attempt, wantErr)
	select {
	case err := <-result:
		if !errors.Is(err, wantErr) {
			t.Fatalf("requestAndWaitReconnect() = %v, want wait error %v", err, wantErr)
		}
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("requestAndWaitReconnect() did not return after reconnect completion")
	}
}

func waitReconnectCondition(t *testing.T, condition func() bool) {
	t.Helper()
	realClock := clock.NewReal()
	deadline := realClock.Timer(2 * time.Second)
	defer deadline.Stop()
	ticker := realClock.Ticker(time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("reconnect condition timed out")
		case <-ticker.C:
		}
	}
}

func TestRunnerPostRunDrainUsesGenerationContext(t *testing.T) {
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 1)}
	client := newReconnectTestClient(t, d, nil, 0)
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Prefetch:       12,
		HandlerTimeout: 10 * time.Millisecond,
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error { return nil }),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	type runContextKey struct{}
	key := runContextKey{}
	runCtx := context.WithValue(context.Background(), key, "run-value")
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(runCtx) }()
	consumer := <-d.created
	consumer.once.Do(func() {
		close(consumer.messages)
		close(consumer.errors)
	})
	timeout := clock.NewReal().Timer(2 * time.Second)
	defer timeout.Stop()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("runner Run() = %v", err)
		}
	case <-timeout.C:
		t.Fatal("runner Run() did not return")
	}

	runner.mu.Lock()
	settleCtx := runner.settleCtx
	runner.mu.Unlock()
	if settleCtx == nil {
		t.Fatal("post-run drain did not create a settlement context")
	}
	if got := settleCtx.Value(key); got != "run-value" {
		t.Fatalf("settlement context value = %v, want run-value", got)
	}
	if _, ok := settleCtx.Deadline(); !ok {
		t.Fatal("post-run drain settlement context has no deadline")
	}
}

func reconnectMessage(t *testing.T, id string, settler driver.Settler) driver.InboundMessage {
	t.Helper()
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          id,
		Source:      "/test/orders",
		Type:        "orders.created",
		Priority:    PriorityNormal,
	}
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	return driver.InboundMessage{
		Destination: "f1.test.orders.created.normal",
		Headers:     headerSlice(headers),
		Settle:      settler,
	}
}

func validReconnectMessage(t *testing.T, id string) driver.InboundMessage {
	return reconnectMessage(t, id, &reconnectTestSettler{})
}

func TestFatalConsumerErrorStopsOnlyItsRunner(t *testing.T) {
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	recorder := newErrorHandlerRecorder()
	client := newReconnectTestClient(t, d, nil, 0, WithErrorHandler(recorder.handle))

	fatalRunner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "fatal",
		Topics:         []string{"orders.created"},
		Prefetch:       12,
		HandlerTimeout: 10 * time.Millisecond,
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error {
				t.Fatal("fatal subscription handler received a message")
				return nil
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var healthyHandled atomic.Int32
	healthyRunner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "healthy",
		Topics:         []string{"orders.created"},
		Prefetch:       12,
		HandlerTimeout: 10 * time.Millisecond,
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error {
				healthyHandled.Add(1)
				return nil
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	fatalCtx, cancelFatal := context.WithCancel(context.Background())
	healthyCtx, cancelHealthy := context.WithCancel(context.Background())
	t.Cleanup(cancelFatal)
	t.Cleanup(cancelHealthy)
	fatalDone := make(chan error, 1)
	healthyDone := make(chan error, 1)
	go func() { fatalDone <- fatalRunner.Run(fatalCtx) }()
	go func() { healthyDone <- healthyRunner.Run(healthyCtx) }()

	consumers := make(map[string]*reconnectTestConsumer, 2)
	timeout := clock.NewReal().Timer(2 * time.Second)
	defer timeout.Stop()
	for len(consumers) < 2 {
		select {
		case consumer := <-d.created:
			consumers[consumer.group] = consumer
		case <-timeout.C:
			t.Fatal("timed out waiting for both subscription consumers")
		}
	}
	waitReconnectCondition(t, func() bool {
		return fatalRunner.lifecycle.Ready() && healthyRunner.lifecycle.Ready()
	})

	cause := &driver.Error{Driver: d.Name(), Op: "consumer", K: driver.KindFatal, Err: errors.New("permission denied")}
	consumers["fatal"].sendError(cause)
	waitReconnectCondition(t, func() bool { return fatalRunner.lifecycle.State() == lifecycle.Failed })

	select {
	case runErr := <-fatalDone:
		if !errors.Is(runErr, cause) {
			t.Fatalf("fatal runner Run() = %v, want %v", runErr, cause)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("fatal runner did not stop")
	}
	if consumers["fatal"].send(validReconnectMessage(t, "must-not-consume")) {
		t.Fatal("fatal consumer accepted a message after failure")
	}
	if d.OpenCount() != 1 || client.isReconnecting() {
		t.Fatalf("fatal runner triggered reconnect: opens=%d reconnecting=%t", d.OpenCount(), client.isReconnecting())
	}
	if !healthyRunner.lifecycle.Ready() || !healthyRunner.lifecycle.Live() {
		t.Fatalf("healthy runner state = %s, ready=%t live=%t", healthyRunner.lifecycle.State(), healthyRunner.lifecycle.Ready(), healthyRunner.lifecycle.Live())
	}
	if !consumers["healthy"].send(validReconnectMessage(t, "healthy-after-fatal")) {
		t.Fatal("healthy consumer rejected a message after sibling failure")
	}
	waitReconnectCondition(t, func() bool { return healthyHandled.Load() == 1 })
	recorder.waitForCall(t, time.Second)
	if got := recorder.count(); got != 1 {
		t.Fatalf("fatal error handler calls = %d, want 1", got)
	}
	if err := client.Health(context.Background()); err == nil || !strings.Contains(err.Error(), "subscription fatal failed") {
		t.Fatalf("Health() = %v, want failed fatal subscription", err)
	}
	select {
	case err := <-healthyDone:
		t.Fatalf("healthy runner stopped after sibling fatal error: %v", err)
	default:
	}
}

func TestFatalConsumerErrorSkipsConcurrentReconnect(t *testing.T) {
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 1)}
	client := newReconnectTestClient(t, d, nil, 0)
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "fatal",
		Topics:         []string{"orders.created"},
		Prefetch:       12,
		HandlerTimeout: 10 * time.Millisecond,
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error { return nil }),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(context.Background()) }()
	consumer := <-d.created
	waitReconnectCondition(t, func() bool {
		runner.mu.Lock()
		machine := runner.lifecycle
		runner.mu.Unlock()
		return machine != nil && machine.Ready()
	})
	attempt := &reconnectAttempt{done: make(chan struct{})}
	client.mu.Lock()
	client.reconnecting = true
	client.reconnect = attempt
	client.mu.Unlock()
	cause := &driver.Error{Driver: d.Name(), Op: "consumer", K: driver.KindFatal, Err: errors.New("permission denied")}
	consumer.sendError(cause)
	select {
	case runErr := <-runDone:
		if !errors.Is(runErr, cause) {
			t.Fatalf("fatal runner Run() = %v, want %v", runErr, cause)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("fatal runner joined a concurrent reconnect")
	}
	client.mu.Lock()
	client.reconnecting = false
	client.reconnect = nil
	client.mu.Unlock()
	if d.OpenCount() != 1 {
		t.Fatalf("fatal runner opened a replacement consumer: open count = %d", d.OpenCount())
	}
	if err := runner.abandonForReconnect(context.Background()); err != nil {
		t.Fatalf("abandonForReconnect(Failed) = %v, want nil", err)
	}
	runner.mu.Lock()
	reconnectCause := runner.reconnectCause
	runner.mu.Unlock()
	if reconnectCause != nil {
		t.Fatalf("failed runner acquired reconnect cause %v", reconnectCause)
	}
}

func TestDrainGivesInFlightHandlerItsGraceBudget(t *testing.T) {
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 1)}
	client := newReconnectTestClient(t, d, nil, 0)
	handlerStarted := make(chan struct{})
	handlerCanceled := make(chan struct{})
	releaseHandler := make(chan struct{})
	var startedOnce sync.Once
	var canceledOnce sync.Once
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Prefetch:       12,
		HandlerTimeout: 100 * time.Millisecond,
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(ctx context.Context, _ *Event) error {
				startedOnce.Do(func() { close(handlerStarted) })
				select {
				case <-releaseHandler:
					return nil
				case <-ctx.Done():
					canceledOnce.Do(func() { close(handlerCanceled) })
					return ctx.Err()
				}
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(context.Background()) }()
	consumer := <-d.created
	waitReconnectCondition(t, func() bool { return runner.lifecycle.Ready() })
	if !consumer.send(validReconnectMessage(t, "drain-grace-in-flight")) {
		t.Fatal("consumer rejected the in-flight message")
	}
	select {
	case <-handlerStarted:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("handler did not start")
	}

	drainDone := make(chan error, 1)
	go func() { drainDone <- runner.Drain(context.Background()) }()
	select {
	case err := <-drainDone:
		t.Fatalf("Drain() returned before handler grace was used: %v", err)
	case <-handlerCanceled:
		t.Fatal("handler was canceled immediately at drain start")
	case <-clock.NewReal().Timer(50 * time.Millisecond).C:
	}
	close(releaseHandler)
	if err := <-drainDone; err != nil {
		t.Fatalf("Drain() = %v, want nil after in-flight handler completed", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("runner Run() = %v, want nil after graceful drain", err)
	}
}

type graceContextSettler struct {
	acked  bool
	nacked bool
}

func (s *graceContextSettler) Ack(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.acked = true
	return nil
}

func (s *graceContextSettler) Nack(ctx context.Context, _ driver.NackOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.nacked = true
	return nil
}

func TestDrainCancelsInFlightHandlerIntoRetryLane(t *testing.T) {
	producer := &dispatchProducer{}
	consumer := newDispatchConsumer()
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&dispatchDriver{conn: &dispatchConn{producer: producer, consumer: consumer, admin: &dispatchAdmin{}}}),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatal(err)
	}
	client.config.Lifecycle.DrainTimeout = 50 * time.Millisecond
	client.config.Lifecycle.HandlerGrace = 10 * time.Millisecond
	handlerStarted := make(chan struct{})
	handlerCanceled := make(chan struct{})
	var startedOnce sync.Once
	var canceledOnce sync.Once
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Prefetch:       12,
		Priorities:     []Priority{PriorityHigh},
		Retry:          RetryConfig{MaxAttempts: 2, Tiers: []time.Duration{time.Second}},
		HandlerTimeout: 45 * time.Millisecond,
		Handlers: map[string]Handler{
			"orders.created.v1": HandlerFunc(func(ctx context.Context, _ *Event) error {
				startedOnce.Do(func() { close(handlerStarted) })
				<-ctx.Done()
				canceledOnce.Do(func() { close(handlerCanceled) })
				return ctx.Err()
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(context.Background()) }()
	waitReconnectCondition(t, func() bool {
		runner.mu.Lock()
		machine := runner.lifecycle
		runner.mu.Unlock()
		return machine != nil && machine.Ready()
	})
	settler := &graceContextSettler{}
	consumer.messages <- retryBridgeMessage(t, Envelope{
		SpecVersion: "1.0",
		ID:          "drain-grace-retry",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
	}, settler)
	select {
	case <-handlerStarted:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("handler did not start")
	}
	if err := runner.Drain(context.Background()); err != nil {
		t.Fatalf("Drain() = %v, want nil", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("runner Run() = %v, want nil", err)
	}
	select {
	case <-handlerCanceled:
	default:
		t.Fatal("handler did not receive cancellation at the grace deadline")
	}
	if !settler.acked || settler.nacked {
		t.Fatalf("drain-grace settlement = acked %t nacked %t, want ack only", settler.acked, settler.nacked)
	}
	if len(producer.messages) != 1 {
		t.Fatalf("retry successors = %d, want 1", len(producer.messages))
	}
	wantDestination := retryDestinationFor(client.source, "orders.created", PriorityHigh, 1, runner.subscription.Name)
	if got := producer.messages[0].Destination; got != wantDestination {
		t.Fatalf("retry destination = %q, want %q", got, wantDestination)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerRepairsConsumerAndResumesDelivery(t *testing.T) {
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, nil, 0)
	var handled atomic.Int32
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Prefetch:       12,
		HandlerTimeout: 10 * time.Millisecond,
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error {
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
	first := <-d.created
	waitReconnectCondition(t, func() bool { return runner.lifecycle.Ready() })
	first.sendError(&driver.Error{Driver: d.Name(), Op: "consumer", K: driver.KindTransient, Err: errors.New("transient")})
	second := <-d.created
	if first == second {
		t.Fatal("repair reused the old consumer")
	}
	waitReconnectCondition(t, func() bool { return runner.lifecycle.Ready() })
	if client.isReconnecting() {
		t.Fatal("lane repair entered client reconnecting state")
	}
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health during lane repair = %v, want nil", err)
	}
	publishErr := publishMessages(client, context.Background(), false, driver.OutboundMessage{Destination: "test"})
	if publishErr != nil {
		t.Fatalf("publish during lane repair = %v", publishErr)
	}
	if !runner.lifecycle.Ready() || !runner.lifecycle.Live() {
		t.Fatalf("runner probes during lane repair = ready %t live %t", runner.lifecycle.Ready(), runner.lifecycle.Live())
	}
	if d.OpenCount() != 1 {
		t.Fatalf("driver Open count = %d, want 1", d.OpenCount())
	}
	second.send(validReconnectMessage(t, "after-reconnect"))
	waitReconnectCondition(t, func() bool { return handled.Load() == 1 })
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("runner Run() = %v", err)
	}
	client.mu.Lock()
	shutdownStarted := client.shutdownStarted
	client.mu.Unlock()
	if !shutdownStarted {
		t.Fatal("Close did not set shutdownStarted")
	}
}

func TestSupervisorReconnectKeepsAbandonedSiblingRunning(t *testing.T) {
	fake := clock.NewFake(time.Unix(350, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan int, 8)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	var handlerCalls atomic.Int32
	client := newReconnectTestClient(t, d, recorded, 0, WithErrorHandler(func(context.Context, *Event, error) {
		handlerCalls.Add(1)
	}))
	client.config.Lifecycle.DrainTimeout = 20 * time.Millisecond
	client.reconnectRandom = func() float64 { return 0 }

	var causingHandled, victimHandled atomic.Int32
	causingRunner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "causing",
		Topics:         []string{"orders.created"},
		Prefetch:       12,
		HandlerTimeout: 10 * time.Millisecond,
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error {
				causingHandled.Add(1)
				return nil
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	victimRunner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "victim",
		Topics:         []string{"orders.created"},
		Prefetch:       12,
		HandlerTimeout: 10 * time.Millisecond,
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error {
				victimHandled.Add(1)
				return nil
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	causingCtx, cancelCausing := context.WithCancel(context.Background())
	victimCtx, cancelVictim := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancelCausing()
		cancelVictim()
	})
	causingDone := make(chan error, 1)
	victimDone := make(chan error, 1)
	go func() { causingDone <- causingRunner.Run(causingCtx) }()
	go func() { victimDone <- victimRunner.Run(victimCtx) }()

	initial := make(map[string]*reconnectTestConsumer, 2)
	initialTimer := clock.NewReal().Timer(2 * time.Second)
	defer initialTimer.Stop()
	for len(initial) < 2 {
		select {
		case consumer := <-d.created:
			initial[consumer.group] = consumer
		case <-initialTimer.C:
			t.Fatal("timed out waiting for both initial consumers")
		}
	}
	waitReconnectCondition(t, func() bool {
		return causingRunner.lifecycle.Ready() && victimRunner.lifecycle.Ready()
	})
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health before supervisor reconnect = %v, want nil", err)
	}

	causing := initial["causing"]
	causing.drainStarted = make(chan struct{})
	causingRelease := make(chan struct{})
	causing.drainRelease = causingRelease
	victim := initial["victim"]
	victim.drainStarted = make(chan struct{})
	victimRelease := make(chan struct{})
	victim.drainRelease = victimRelease
	var releaseCausing, releaseVictim sync.Once
	release := func() {
		releaseCausing.Do(func() { close(causingRelease) })
		releaseVictim.Do(func() { close(victimRelease) })
	}
	t.Cleanup(release)
	cause := errors.New("causing runner requested reconnect")
	causeDone := make(chan error, 1)
	go func() { causeDone <- causingRunner.requestAndWaitReconnect(context.Background(), cause) }()

	waitReconnectCondition(t, func() bool {
		causingStarted := false
		victimStarted := false
		select {
		case <-causing.drainStarted:
			causingStarted = true
		default:
		}
		select {
		case <-victim.drainStarted:
			victimStarted = true
		default:
		}
		return causingStarted && victimStarted
	})
	waitReconnectCondition(t, func() bool { return recorded.sleepCount() == 1 })
	releaseCausing.Do(func() { close(causingRelease) })
	select {
	case before := <-recorded.sleepStarted:
		fake.BlockUntil(before + 1)
		fake.Advance(0)
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("reconnect sleep did not start")
	}
	select {
	case err := <-causeDone:
		if err != nil {
			t.Fatalf("causing runner reconnect = %v, want nil", err)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("supervisor reconnect did not finish")
	}
	releaseVictim.Do(func() { close(victimRelease) })

	sent := make(map[*reconnectTestConsumer]bool)
	replacementTimer := clock.NewReal().Timer(2 * time.Second)
	defer replacementTimer.Stop()
	for causingHandled.Load() < 1 || victimHandled.Load() < 1 || client.isReconnecting() {
		select {
		case err := <-victimDone:
			t.Fatalf("victim Run returned after supervisor reconnect: %v", err)
		case consumer := <-d.created:
			if sent[consumer] {
				continue
			}
			sent[consumer] = true
			switch consumer.group {
			case "causing":
				consumer.send(validReconnectMessage(t, "causing-after-reconnect"))
			case "victim":
				consumer.send(validReconnectMessage(t, "victim-after-reconnect"))
			}
		case before := <-recorded.sleepStarted:
			fake.BlockUntil(before + 1)
			fake.Advance(0)
		case <-replacementTimer.C:
			if causingHandled.Load() >= 1 && victimHandled.Load() >= 1 && !client.isReconnecting() {
				break
			}
			t.Fatalf("timed out waiting for deliveries: causing=%d victim=%d reconnecting=%t", causingHandled.Load(), victimHandled.Load(), client.isReconnecting())
		}
	}
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health after supervisor reconnect = %v, want nil", err)
	}
	select {
	case err := <-victimDone:
		t.Fatalf("victim Run returned after delivering its rebuilt generation: %v", err)
	default:
	}
	select {
	case err := <-causingDone:
		t.Fatalf("causing Run returned after delivering its rebuilt generation: %v", err)
	default:
	}
	if got := handlerCalls.Load(); got != 0 {
		t.Fatalf("error handler calls = %d, want 0", got)
	}
}

// TestAbandonForReconnectPreservesRunnerOwnCause proves that a runner which
// already recorded its own driver error before a client-wide reconnect began
// keeps that error through supervisor abandonment, instead of having it
// replaced by the generic reconnect placeholder every abandoned runner would
// otherwise receive.
func TestAbandonForReconnectPreservesRunnerOwnCause(t *testing.T) {
	fake := clock.NewFake(time.Unix(350, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan int, 8)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.config.Lifecycle.DrainTimeout = 20 * time.Millisecond
	client.reconnectRandom = func() float64 { return 0 }

	causingRunner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "causing",
		Topics:         []string{"orders.created"},
		Prefetch:       12,
		HandlerTimeout: 10 * time.Millisecond,
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error { return nil }),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	victimRunner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "victim",
		Topics:         []string{"orders.created"},
		Prefetch:       12,
		HandlerTimeout: 10 * time.Millisecond,
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error { return nil }),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	causingCtx, cancelCausing := context.WithCancel(context.Background())
	victimCtx, cancelVictim := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancelCausing()
		cancelVictim()
	})
	causingDone := make(chan error, 1)
	victimDone := make(chan error, 1)
	go func() { causingDone <- causingRunner.Run(causingCtx) }()
	go func() { victimDone <- victimRunner.Run(victimCtx) }()

	initial := make(map[string]*reconnectTestConsumer, 2)
	initialTimer := clock.NewReal().Timer(2 * time.Second)
	defer initialTimer.Stop()
	for len(initial) < 2 {
		select {
		case consumer := <-d.created:
			initial[consumer.group] = consumer
		case <-initialTimer.C:
			t.Fatal("timed out waiting for both initial consumers")
		}
	}
	waitReconnectCondition(t, func() bool {
		return causingRunner.lifecycle.Ready() && victimRunner.lifecycle.Ready()
	})

	// The victim runner records its own driver error, the same way
	// consumeRunnerErrors does for a transient consumer error, before any
	// client-wide reconnect has been requested.
	ownCause := errors.New("victim runner's own transient consumer error")
	victimRunner.mu.Lock()
	victimRunner.reconnectCause = ownCause
	victimRunner.mu.Unlock()

	causing := initial["causing"]
	causing.drainStarted = make(chan struct{})
	causingRelease := make(chan struct{})
	causing.drainRelease = causingRelease
	victim := initial["victim"]
	victim.drainStarted = make(chan struct{})
	victimRelease := make(chan struct{})
	victim.drainRelease = victimRelease
	var releaseCausing, releaseVictim sync.Once
	release := func() {
		releaseCausing.Do(func() { close(causingRelease) })
		releaseVictim.Do(func() { close(victimRelease) })
	}
	t.Cleanup(release)
	cause := errors.New("causing runner requested reconnect")
	causeDone := make(chan error, 1)
	go func() { causeDone <- causingRunner.requestAndWaitReconnect(context.Background(), cause) }()

	waitReconnectCondition(t, func() bool {
		causingStarted := false
		victimStarted := false
		select {
		case <-causing.drainStarted:
			causingStarted = true
		default:
		}
		select {
		case <-victim.drainStarted:
			victimStarted = true
		default:
		}
		return causingStarted && victimStarted
	})

	// Both runners' contexts are cancelled by abandonForReconnect before
	// their post-cancel drain (which is what just closed drainStarted)
	// begins, so abandonForReconnect has already run to completion for the
	// victim runner by this point. Its own cause must still be on record.
	victimRunner.mu.Lock()
	gotCause := victimRunner.reconnectCause
	victimRunner.mu.Unlock()
	if !errors.Is(gotCause, ownCause) {
		t.Fatalf("victim reconnectCause after abandonment = %v, want %v", gotCause, ownCause)
	}

	releaseCausing.Do(func() { close(causingRelease) })
	select {
	case before := <-recorded.sleepStarted:
		fake.BlockUntil(before + 1)
		fake.Advance(0)
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("reconnect sleep did not start")
	}
	select {
	case err := <-causeDone:
		if err != nil {
			t.Fatalf("causing runner reconnect = %v, want nil", err)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("supervisor reconnect did not finish")
	}
	releaseVictim.Do(func() { close(victimRelease) })

	// Releasing the victim's drain hands it a fresh generation whose own
	// bottom-of-loop reconnect request (client.reconnecting has already gone
	// false by now) starts a second reconnect cycle on the same fake clock,
	// exactly as in TestSupervisorReconnectKeepsAbandonedSiblingRunning.
	settleTimer := clock.NewReal().Timer(2 * time.Second)
	defer settleTimer.Stop()
	for !causingRunner.lifecycle.Ready() || !victimRunner.lifecycle.Ready() || client.isReconnecting() {
		select {
		case err := <-victimDone:
			t.Fatalf("victim Run returned after supervisor reconnect: %v", err)
		case err := <-causingDone:
			t.Fatalf("causing Run returned after supervisor reconnect: %v", err)
		case <-d.created:
		case before := <-recorded.sleepStarted:
			fake.BlockUntil(before + 1)
			fake.Advance(0)
		case <-settleTimer.C:
			t.Fatalf("timed out settling after abandonment: reconnecting=%t", client.isReconnecting())
		}
	}
	select {
	case err := <-victimDone:
		t.Fatalf("victim Run returned after rebuilding its generation: %v", err)
	default:
	}
	select {
	case err := <-causingDone:
		t.Fatalf("causing Run returned after rebuilding its generation: %v", err)
	default:
	}
}

func TestPublishFailsFastDuringReconnect(t *testing.T) {
	fake := clock.NewFake(time.Unix(150, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 1 }
	if _, err := client.requestReconnect(errors.New("transient")); err != nil {
		t.Fatal(err)
	}
	waitReconnectCondition(t, func() bool { return recorded.sleepCount() == 1 })

	publishDone := make(chan error, 1)
	go func() {
		_, err := client.Publisher().Publish(context.Background(), "orders.created", map[string]string{"value": "during-reconnect"})
		publishDone <- err
	}()
	timer := clock.NewReal().Timer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case err := <-publishDone:
		if kind, classified := driver.Classify(err); !classified || kind != driver.KindTransient {
			t.Fatalf("Publish() = %v, want classified transient", err)
		}
		if !errors.Is(err, errClientReconnecting) {
			t.Fatalf("Publish() = %v, want errClientReconnecting", err)
		}
	case <-timer.C:
		t.Fatal("Publish() blocked while reconnect was in progress")
	}
	if !client.isReconnecting() {
		t.Fatal("reconnect completed before the fail-fast publish returned")
	}
	if got := d.connections[0].producer.Load(); got != 0 {
		t.Fatalf("old connection producer count = %d, want 0", got)
	}
}

func TestPublishRefusesStaleConnection(t *testing.T) {
	codecFixture := blockingCodec{
		encodeStarted:     make(chan struct{}),
		encodeStartedOnce: make(chan struct{}),
		encodeRelease:     make(chan struct{}),
	}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, nil, 0, WithCodec(codecFixture))
	oldConn := d.connections[0]
	publishDone := make(chan error, 1)
	go func() {
		_, err := client.Publisher().Publish(context.Background(), "orders.created", map[string]string{"value": "stale"})
		publishDone <- err
	}()
	codecTimer := clock.NewReal().Timer(time.Second)
	defer codecTimer.Stop()
	select {
	case <-codecFixture.encodeStarted:
	case <-codecTimer.C:
		t.Fatal("publish did not reach the codec")
	}

	replacement := &reconnectTestConn{driver: d, admin: &reconnectTestAdmin{}}
	client.mu.Lock()
	client.conn = replacement
	client.mu.Unlock()
	if err := oldConn.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(codecFixture.encodeRelease)

	timer := clock.NewReal().Timer(time.Second)
	defer timer.Stop()
	select {
	case err := <-publishDone:
		if kind, classified := driver.Classify(err); !classified || kind != driver.KindTransient {
			t.Fatalf("Publish() = %v, want classified transient", err)
		}
		if !errors.Is(err, errClientReconnecting) {
			t.Fatalf("Publish() = %v, want errClientReconnecting", err)
		}
	case <-timer.C:
		t.Fatal("stale publish did not return")
	}
	if got := oldConn.producer.Load(); got != 0 {
		t.Fatalf("retired connection producer count = %d, want 0", got)
	}
	if got := replacement.producer.Load(); got != 0 {
		t.Fatalf("replacement connection producer count = %d, want 0", got)
	}
}

func advanceReconnect(t *testing.T, c *recordingClock, duration time.Duration, expectedSleeps int) {
	t.Helper()
	waitReconnectCondition(t, func() bool { return c.sleepCount() >= expectedSleeps })
	c.BlockUntil(1)
	c.Advance(duration)
}

func TestReconnectBackoffBudgetAndJitter(t *testing.T) {
	start := time.Unix(100, 0)
	fake := clock.NewFake(start)
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 3)
	samples := []float64{0.25, 0.5, 0.75}
	var sample atomic.Int32
	client.reconnectRandom = func() float64 {
		return samples[sample.Add(1)-1]
	}
	d.setFailOpens(3)
	attempt, err := client.requestReconnect(errors.New("transient"))
	if err != nil {
		t.Fatal(err)
	}
	for i, nominal := range []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second} {
		advanceReconnect(t, recorded, time.Duration(float64(nominal)*samples[i]), i+1)
		if got := recorded.sleepAt(i); got < 0 || got > nominal {
			t.Fatalf("backoff[%d] = %s, want range [0,%s]", i, got, nominal)
		}
	}
	if err := client.waitReconnect(context.Background(), attempt); err == nil {
		t.Fatal("finite reconnect budget returned nil")
	} else if kind, classified := driver.Classify(err); !classified || kind != driver.KindFatal {
		t.Fatalf("finite reconnect error = %v, want classified fatal", err)
	}
}

func TestPublishOnlyClientSurfacesReconnectExhaustion(t *testing.T) {
	fake := clock.NewFake(time.Unix(225, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 1)
	client.reconnectRandom = func() float64 { return 1 }
	d.setFailOpens(1)
	attempt, err := client.requestReconnect(errors.New("transient"))
	if err != nil {
		t.Fatal(err)
	}
	advanceReconnect(t, recorded, 500*time.Millisecond, 1)
	reconnectErr := client.waitReconnect(context.Background(), attempt)
	if kind, classified := driver.Classify(reconnectErr); !classified || kind != driver.KindFatal {
		t.Fatalf("reconnect error = %v, want classified fatal", reconnectErr)
	}

	for i := range 2 {
		if healthErr := client.Health(context.Background()); !errors.Is(healthErr, reconnectErr) {
			t.Fatalf("Health attempt %d = %v, want stored exhaustion %v", i+1, healthErr, reconnectErr)
		}
	}
	if _, publishErr := client.Publisher().Publish(context.Background(), "orders.created", map[string]string{"value": "after-exhaustion"}); !errors.Is(publishErr, reconnectErr) {
		t.Fatalf("Publish() = %v, want stored exhaustion %v", publishErr, reconnectErr)
	}
	client.mu.Lock()
	shutdownStarted := client.shutdownStarted
	client.mu.Unlock()
	if shutdownStarted {
		t.Fatal("reconnect exhaustion set shutdownStarted")
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close() after reconnect exhaustion = %v", err)
	}
}

func TestReconnectUnlimitedBudget(t *testing.T) {
	fake := clock.NewFake(time.Unix(200, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 1 }
	d.setFailOpens(2)
	attempt, err := client.requestReconnect(errors.New("transient"))
	if err != nil {
		t.Fatal(err)
	}
	for i, duration := range []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second} {
		advanceReconnect(t, recorded, duration, i+1)
	}
	if err := client.waitReconnect(context.Background(), attempt); err != nil {
		t.Fatalf("unlimited reconnect = %v", err)
	}
	if got := d.OpenCount(); got != 4 {
		t.Fatalf("Open count = %d, want initial plus two failures and success", got)
	}
}

func TestFiniteReconnectBudgetDrainsRunnerAndReturnsFatal(t *testing.T) {
	fake := clock.NewFake(time.Unix(250, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 1)
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Prefetch:       12,
		HandlerTimeout: 10 * time.Millisecond,
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error { return nil }),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(context.Background()) }()
	first := <-d.created
	waitReconnectCondition(t, func() bool { return runner.lifecycle.Ready() })
	d.setFailConsumers(1)
	d.setFailOpens(1)
	first.sendError(&driver.Error{Driver: d.Name(), Op: "consumer", K: driver.KindTransient, Err: errors.New("transient")})
	advanceReconnect(t, recorded, 500*time.Millisecond, 1)
	select {
	case err := <-runDone:
		if kind, classified := driver.Classify(err); !classified || kind != driver.KindFatal {
			t.Fatalf("runner error = %v, want classified fatal", err)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("runner did not exit after reconnect budget exhaustion")
	}
	if got := runner.lifecycle.State(); got != lifecycle.Closed {
		t.Fatalf("runner lifecycle after exhaustion = %s, want closed", got)
	}
}

func TestCloseDuringReconnectKeepsShutdownSeparate(t *testing.T) {
	fake := clock.NewFake(time.Unix(300, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 1 }
	attempt, err := client.requestReconnect(errors.New("transient"))
	if err != nil {
		t.Fatal(err)
	}
	waitReconnectCondition(t, func() bool { return client.isReconnecting() && recorded.sleepCount() == 1 })
	client.mu.Lock()
	shutdownStarted := client.shutdownStarted
	client.mu.Unlock()
	if shutdownStarted {
		t.Fatal("reconnect set shutdownStarted")
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.waitReconnect(context.Background(), attempt); !errors.Is(err, context.Canceled) {
		t.Fatalf("reconnect after Close = %v, want context canceled", err)
	}
	select {
	case <-client.supervisorDone:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("reconnect supervisor did not stop after Close")
	}
}

func TestReconnectPolicyUsesFullJitter(t *testing.T) {
	for attempt, want := range map[int]time.Duration{1: 500 * time.Millisecond, 2: time.Second, 3: 2 * time.Second, 100: 30 * time.Second} {
		if got := reconnectPolicy.DelayFor(attempt); got != want {
			t.Fatalf("reconnect delay %d = %s, want %s", attempt, got, want)
		}
	}
	for _, test := range []struct {
		sample  float64
		nominal time.Duration
		want    time.Duration
	}{
		{sample: 0, nominal: 500 * time.Millisecond, want: 0},
		{sample: 0.5, nominal: time.Second, want: 500 * time.Millisecond},
		{sample: 1, nominal: 30 * time.Second, want: 30 * time.Second},
	} {
		if got := retry.FullJitter(test.nominal, test.sample); got != test.want {
			t.Fatalf("FullJitter(%s,%v) = %s, want %s", test.nominal, test.sample, got, test.want)
		}
	}
}
