package f1

import (
	"context"
	"errors"
	"io"
	"log/slog"
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

func (c *reconnectTestConn) Consumer(context.Context, driver.ConsumerConfig) (driver.Consumer, error) {
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
	messages chan driver.InboundMessage
	errors   chan error
	once     sync.Once
}

func (c *reconnectTestConsumer) Messages() <-chan driver.InboundMessage { return c.messages }
func (c *reconnectTestConsumer) Errors() <-chan error                   { return c.errors }
func (*reconnectTestConsumer) Pause(...string) error                    { return nil }
func (*reconnectTestConsumer) Resume(...string) error                   { return nil }
func (*reconnectTestConsumer) Drain(context.Context) error              { return nil }
func (c *reconnectTestConsumer) Stop(context.Context) error {
	c.once.Do(func() {
		close(c.messages)
		close(c.errors)
	})
	return nil
}

func (c *reconnectTestConsumer) Release(context.Context) error {
	c.once.Do(func() {
		close(c.messages)
		close(c.errors)
	})
	return nil
}

func (*reconnectTestConsumer) Lag(context.Context) (map[string]int64, error) {
	return nil, driver.ErrUnsupported
}

func (c *reconnectTestConsumer) sendError(err error) {
	c.errors <- err
}

func (c *reconnectTestConsumer) send(message driver.InboundMessage) {
	c.messages <- message
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
	mu     sync.Mutex
	sleeps []time.Duration
}

func (c *recordingClock) Sleep(ctx context.Context, duration time.Duration) error {
	c.mu.Lock()
	c.sleeps = append(c.sleeps, duration)
	c.mu.Unlock()
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

	for i := 0; i < 2; i++ {
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
