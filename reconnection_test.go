package f1

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/sched"
)

type reconnectTestDriver struct {
	mu               sync.Mutex
	opens            int
	failOpens        int
	openErr          error
	failConsumers    int
	consumerErr      error
	publishErr       error
	consumerOpenHook func(context.Context, string) error
	connections      []*reconnectTestConn
	created          chan *reconnectTestConsumer
	prefetches       []int
	consumerConfigs  []driver.ConsumerConfig
	// refuseTopology, when set, is the answer every connection after the
	// first gives to an EnsureTopology call.
	refuseTopology func(driver.TopologySpec) error
	// firstCloseGate, when set, holds the first connection's Close until it is
	// closed, the way a driver whose teardown hangs on a dead broker does.
	firstCloseGate    chan struct{}
	firstCloseEntered atomic.Bool
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
		if d.openErr != nil {
			return nil, d.openErr
		}
		return nil, &driver.Error{Driver: d.Name(), Op: "open", K: driver.KindTransient, Err: errors.New("open failed")}
	}
	admin := &reconnectTestAdmin{}
	if d.opens > 1 {
		admin.refuse = d.refuseTopology
	}
	conn := &reconnectTestConn{driver: d, admin: admin}
	d.connections = append(d.connections, conn)
	return conn, nil
}

func (d *reconnectTestDriver) OpenCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.opens
}

func (d *reconnectTestDriver) prefetchesSnapshot() []int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]int(nil), d.prefetches...)
}

func (d *reconnectTestDriver) setFailOpens(n int) {
	d.mu.Lock()
	d.failOpens = n
	d.openErr = nil
	d.mu.Unlock()
}

func (d *reconnectTestDriver) setFailOpensWithError(n int, err error) {
	d.mu.Lock()
	d.failOpens = n
	d.openErr = err
	d.mu.Unlock()
}

func (d *reconnectTestDriver) setPublishError(err error) {
	d.mu.Lock()
	d.publishErr = err
	d.mu.Unlock()
}

func (d *reconnectTestDriver) publishError() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.publishErr
}

func (d *reconnectTestDriver) setFailConsumers(n int) {
	d.setFailConsumersWithError(n, nil)
}

func (d *reconnectTestDriver) setFailConsumersWithError(n int, err error) {
	d.mu.Lock()
	d.failConsumers = n
	d.consumerErr = err
	d.mu.Unlock()
}

type reconnectTestConn struct {
	driver   *reconnectTestDriver
	admin    *reconnectTestAdmin
	closed   atomic.Bool
	producer atomic.Int32
	// openConsumers is the number of consumers this connection still carries,
	// counted by the same close that releases each one. openAtClose is what
	// Close found, so a test can tell whether a connection was retired with a
	// consumer still on it, which the real drivers refuse to do.
	openConsumers atomic.Int32
	openAtClose   atomic.Int32
	// refuseCloseWithConsumers makes Close fail while openConsumers is positive,
	// the way a broker connection whose teardown is refused while it still
	// carries a consumer behaves: the consumer must be gone before the
	// connection can be retired. It is off unless a test asks for it, because
	// the rest of this file is about what the core does with the count, not
	// about what the driver refuses.
	refuseCloseWithConsumers atomic.Bool
	producerCloseCalls       atomic.Int32
	closeCalls               atomic.Int32
	producersAtClose         atomic.Int32
	previousOpenAtClose      atomic.Int32
	closeMu                  sync.Mutex
	producerGate             <-chan struct{}
	producerEntered          chan struct{}
	producerFailures         int
	producerErr              error
	closeFailures            int
	closeErr                 error
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

func (c *reconnectTestConn) Consumer(ctx context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	c.driver.mu.Lock()
	c.driver.prefetches = append(c.driver.prefetches, cfg.Prefetch)
	c.driver.consumerConfigs = append(c.driver.consumerConfigs, cfg)
	if c.driver.failConsumers > 0 {
		c.driver.failConsumers--
		err := c.driver.consumerErr
		c.driver.mu.Unlock()
		if err == nil {
			err = errors.New("consumer failed")
		}
		return nil, &driver.Error{Driver: c.driver.Name(), Op: "consumer", K: driver.KindTransient, Err: err}
	}
	openHook := c.driver.consumerOpenHook
	c.driver.mu.Unlock()
	if openHook != nil {
		if err := openHook(ctx, cfg.Group); err != nil {
			return nil, err
		}
	}
	if c.closed.Load() {
		return nil, &driver.Error{Driver: c.driver.Name(), Op: "consumer", K: driver.KindTransient, Err: errors.New("connection closed")}
	}
	consumer := &reconnectTestConsumer{
		conn:     c,
		group:    cfg.Group,
		messages: make(chan driver.InboundMessage, 8),
		errors:   make(chan error, 8),
	}
	// Counted before the consumer is handed to anyone, so a test that receives
	// it from created never sees the connection without its consumer.
	c.openConsumers.Add(1)
	select {
	case c.driver.created <- consumer:
	default:
	}
	return consumer, nil
}
func (c *reconnectTestConn) Admin() driver.Admin      { return c.admin }
func (*reconnectTestConn) Ping(context.Context) error { return nil }
func (c *reconnectTestConn) Close(context.Context) error {
	c.closeCalls.Add(1)
	c.driver.mu.Lock()
	gate := c.driver.firstCloseGate
	first := len(c.driver.connections) > 0 && c.driver.connections[0] == c
	for _, previous := range c.driver.connections {
		if previous == c {
			break
		}
		if !previous.closed.Load() {
			c.previousOpenAtClose.Add(1)
		}
	}
	c.driver.mu.Unlock()
	if gate != nil && first {
		c.driver.firstCloseEntered.Store(true)
		<-gate
	}
	c.producersAtClose.Store(c.producer.Load())
	c.closeMu.Lock()
	if c.closeFailures > 0 {
		c.closeFailures--
		err := c.closeErr
		c.closeMu.Unlock()
		return err
	}
	c.closeMu.Unlock()
	c.openAtClose.Store(c.openConsumers.Load())
	if c.refuseCloseWithConsumers.Load() && c.openConsumers.Load() > 0 {
		return &driver.Error{
			Driver: c.driver.Name(),
			Op:     "close",
			K:      driver.KindTransient,
			Err:    errors.New("connection still carries a consumer"),
		}
	}
	c.closed.Store(true)
	return nil
}

type reconnectTestAdmin struct {
	ensures atomic.Int32
	mu      sync.Mutex
	specs   []driver.TopologySpec
	refuse  func(driver.TopologySpec) error
}

func (a *reconnectTestAdmin) EnsureTopology(_ context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	a.ensures.Add(1)
	if a.refuse != nil {
		if err := a.refuse(spec); err != nil {
			return driver.TopologyDiff{}, err
		}
	}
	a.mu.Lock()
	a.specs = append(a.specs, spec)
	a.mu.Unlock()
	return driver.TopologyDiff{}, nil
}

// ensuredDestinations reports every destination name an EnsureTopology call on
// this admin carried.
func (a *reconnectTestAdmin) ensuredDestinations() map[string]struct{} {
	a.mu.Lock()
	defer a.mu.Unlock()
	names := make(map[string]struct{})
	for _, spec := range a.specs {
		for _, destination := range spec.Destinations {
			names[destination.Name] = struct{}{}
		}
	}
	return names
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
	once sync.Once
}

func (p *reconnectTestProducer) Publish(context.Context, ...driver.OutboundMessage) error {
	return p.conn.driver.publishError()
}

func (p *reconnectTestProducer) Close(context.Context) error {
	c := p.conn
	c.producerCloseCalls.Add(1)
	c.closeMu.Lock()
	gate, entered := c.producerGate, c.producerEntered
	c.producerEntered = nil
	failing := c.producerFailures > 0
	if failing {
		c.producerFailures--
	}
	err := c.producerErr
	c.closeMu.Unlock()
	if entered != nil {
		close(entered)
	}
	if gate != nil {
		<-gate
	}
	if failing {
		return err
	}
	p.once.Do(func() { c.producer.Add(-1) })
	return nil
}

type reconnectTestConsumer struct {
	// conn is the connection this consumer was opened on, which a test reads to
	// tell which connection a runner's consumer landed on.
	conn         *reconnectTestConn
	group        string
	messages     chan driver.InboundMessage
	errors       chan error
	drainStarted chan struct{}
	drainRelease <-chan struct{}
	drainOnce    sync.Once
	once         sync.Once
	mu           sync.RWMutex
	closed       atomic.Bool
	// releaseFailures is armed by a test through setFailRelease. While it is
	// positive, Release returns releaseErr and leaves the consumer open, the
	// way a driver whose teardown call fails on a broken connection behaves.
	releaseFailures int
	releaseErr      error
	// stopHold and stopEntered are armed by a test through holdStop. While
	// stopHold is non-nil the next Stop closes stopEntered and waits for
	// stopHold to close before it closes the consumer, so a test can keep a
	// runner inside its post-error teardown.
	stopHold    chan struct{}
	stopEntered chan struct{}
	// releaseHold and releaseEntered are the same seam for Release, armed by a
	// test through holdRelease. They keep an abandoned runner's release in
	// flight, which is the window a swap must wait for before it retires the
	// connection the consumer is still registered on.
	releaseHold    chan struct{}
	releaseEntered chan struct{}
	releaseCalls   atomic.Int32
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

// holdStop arms this consumer's next Stop to signal entered and then block
// until the returned release runs, before it closes the consumer. It lets a
// test hold a runner inside its post-error teardown while a second runner of
// the same subscription name starts. release is idempotent. A consumer whose
// Stop was never armed ignores it, and an armed hold applies to one Stop only.
func (c *reconnectTestConsumer) holdStop() (entered <-chan struct{}, release func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	held := make(chan struct{})
	signal := make(chan struct{})
	c.stopHold = held
	c.stopEntered = signal
	var once sync.Once
	return signal, func() { once.Do(func() { close(held) }) }
}

// holdRelease arms this consumer's next Release to signal entered and then
// block until the returned release runs, before it releases the consumer. It
// lets a test hold an abandoned runner's release open while it observes what
// the supervisor does in the meantime. release is idempotent, and an armed hold
// applies to one Release only. A consumer whose Release was never armed ignores
// it.
func (c *reconnectTestConsumer) holdRelease() (entered <-chan struct{}, release func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	held := make(chan struct{})
	signal := make(chan struct{})
	c.releaseHold = held
	c.releaseEntered = signal
	var once sync.Once
	return signal, func() { once.Do(func() { close(held) }) }
}

func (c *reconnectTestConsumer) Stop(context.Context) error {
	c.mu.Lock()
	held := c.stopHold
	entered := c.stopEntered
	c.stopHold = nil
	c.stopEntered = nil
	c.mu.Unlock()
	if held != nil {
		close(entered)
		<-held
	}
	c.close()
	return nil
}

func (c *reconnectTestConsumer) Release(context.Context) error {
	c.releaseCalls.Add(1)
	if c.drainRelease != nil {
		return nil
	}
	c.mu.Lock()
	held := c.releaseHold
	entered := c.releaseEntered
	c.releaseHold = nil
	c.releaseEntered = nil
	failing := c.releaseFailures > 0
	var releaseErr error
	if failing {
		c.releaseFailures--
		releaseErr = c.releaseErr
	}
	c.mu.Unlock()
	if held != nil {
		close(entered)
		<-held
	}
	if failing {
		return releaseErr
	}
	c.close()
	return nil
}

func (c *reconnectTestConsumer) setFailRelease(n int, err error) {
	c.mu.Lock()
	c.releaseFailures = n
	c.releaseErr = err
	c.mu.Unlock()
}

// close releases the consumer from its connection. Both Release and Stop end
// here, and a consumer that Release leaves open never reaches it, so the
// connection's count follows the consumers that are really gone.
func (c *reconnectTestConsumer) close() {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed.Store(true)
		close(c.messages)
		close(c.errors)
		c.mu.Unlock()
		if c.conn != nil {
			c.conn.openConsumers.Add(-1)
		}
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
	sleepStarted chan chan struct{}
	timerStarted chan time.Duration
}

func (c *recordingClock) Timer(duration time.Duration) clock.Timer {
	timer := c.Fake.Timer(duration)
	if c.timerStarted != nil {
		c.timerStarted <- duration
	}
	return timer
}

func (c *recordingClock) Sleep(ctx context.Context, duration time.Duration) error {
	c.mu.Lock()
	c.sleeps = append(c.sleeps, duration)
	started := c.sleepStarted
	c.mu.Unlock()

	var registered func()
	if started != nil {
		token := make(chan struct{})
		registered = func() {
			close(token)
			started <- token
		}
	}
	return c.SleepWithRegistration(ctx, duration, registered)
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

func TestRecordingClockSleepSignalsItsOwnRegistration(t *testing.T) {
	t.Parallel()

	fake := clock.NewFake(time.Unix(0, 0))
	unrelated := fake.Timer(time.Hour)
	before := fake.NumWaiters()
	if before != 1 {
		t.Fatalf("waiters before tracked sleep = %d, want 1", before)
	}
	if !unrelated.Stop() {
		t.Fatal("unrelated timer did not stop")
	}

	recorded := &recordingClock{
		Fake:         fake,
		sleepStarted: make(chan chan struct{}, 1),
	}
	ctx, cancel := context.WithCancel(context.Background())
	sleepDone := make(chan error, 1)
	go func() {
		sleepDone <- recorded.Sleep(ctx, time.Hour)
	}()

	registrationDone := make(chan struct{})
	go func() {
		token := <-recorded.sleepStarted
		<-token
		close(registrationDone)
	}()
	registrationTimer := clock.NewReal().Timer(time.Second)
	select {
	case <-registrationDone:
		registrationTimer.Stop()
	case <-registrationTimer.C:
		rescue := fake.Timer(time.Hour)
		unblockTimer := clock.NewReal().Timer(time.Second)
		select {
		case <-registrationDone:
		case <-unblockTimer.C:
			t.Fatal("registration wait did not unblock")
		}
		unblockTimer.Stop()
		rescue.Stop()
		t.Fatal("sleep registration signal timed out")
	}

	oldWaitDone := make(chan struct{})
	go func() {
		fake.BlockUntil(before + 1)
		close(oldWaitDone)
	}()
	oldWaitTimer := clock.NewReal().Timer(20 * time.Millisecond)
	select {
	case <-oldWaitDone:
		t.Fatal("shared waiter count reached the stale baseline")
	case <-oldWaitTimer.C:
	}

	rescue := fake.Timer(time.Hour)
	unblockTimer := clock.NewReal().Timer(time.Second)
	select {
	case <-oldWaitDone:
	case <-unblockTimer.C:
		t.Fatal("stale-baseline waiter did not unblock")
	}
	unblockTimer.Stop()
	if !rescue.Stop() {
		t.Fatal("rescue timer did not stop")
	}

	cancel()
	if err := <-sleepDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("tracked sleep error = %v, want context canceled", err)
	}
}

func newReconnectTestClient(t *testing.T, d *reconnectTestDriver, c clock.Clock, maxAttempts int, extra ...Option) *Client {
	return newReconnectTestClientWithLogger(t, d, c, maxAttempts, slog.New(slog.NewTextHandler(io.Discard, nil)), extra...)
}

func newReconnectTestClientWithLogger(t *testing.T, d *reconnectTestDriver, c clock.Clock, maxAttempts int, logger *slog.Logger, extra ...Option) *Client {
	t.Helper()
	cfg := testClientConfig(t)
	cfg.Broker.MaxReconnectAttempts = maxAttempts
	cfg.Lifecycle.DrainTimeout = 500 * time.Millisecond
	cfg.Lifecycle.HandlerGrace = 100 * time.Millisecond
	cfg.Lifecycle.CloseTimeout = 100 * time.Millisecond
	options := []Option{WithDriver(d)}
	if logger != nil {
		options = append(options, WithLogger(logger))
	}
	options = append(options, extra...)
	if c != nil {
		options = append(options, withClock(c))
	}
	client, err := New(context.Background(), cfg, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client
}

func newReconnectSupervisorTestClient() (*Client, context.CancelFunc) {
	supervisorCtx, supervisorCancel := context.WithCancel(context.Background())
	return &Client{
		// A client that came through New is Ready with a live connection, and
		// these two facts are what admission reads, so a hand-built client
		// states them rather than relying on a zero value.
		lifecycle:         lifecycleIn(lifecycle.Ready),
		reconnectRequests: make(chan reconnectRequest, 1),
		supervisorCtx:     supervisorCtx,
		supervisorCancel:  supervisorCancel,
		supervisorDone:    make(chan struct{}),
	}, supervisorCancel
}

func TestRequestReconnectIsRefusedWhileShuttingDown(t *testing.T) {
	client, runner, fake, _ := newBudgetTestClient(t)
	client.config.Lifecycle.ConsumerDrainTimeout = 5 * time.Second
	client.mu.Lock()
	client.runners[runner] = struct{}{}
	client.mu.Unlock()

	closed := make(chan error, 1)
	go func() { closed <- client.Close(context.Background()) }()
	waitForFakeTimer(t, fake)
	fake.Advance(5 * time.Second)
	watchdog := clock.NewReal().Timer(2 * time.Second)
	defer watchdog.Stop()
	select {
	case err := <-closed:
		if err == nil || !strings.Contains(err.Error(), "consumer drain") {
			t.Fatalf("Close() error = %v, want the consumer drain phase deadline", err)
		}
	case <-watchdog.C:
		t.Fatal("Close did not return after the consumer drain budget expired")
	}
	client.mu.Lock()
	life := client.lifecycleLocked()
	client.mu.Unlock()
	if life != lifecycle.Aborted {
		t.Fatalf("lifecycle after a failed Close = %s, want shutdown begun and the client open", life)
	}

	err := client.requestReconnect(errors.New("during shutdown"), 0)
	if err == nil || !strings.Contains(err.Error(), "client is closing") {
		t.Fatalf("request while shutting down = %v, want client-closing error", err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.conn == connReconnecting {
		t.Fatal("client remains reconnecting after rejected shutdown request")
	}
}

// TestRequestReconnectCancellationStillFinishesItsAttempt pins the request that
// arrives behind a supervisor that is already leaving: it is not served, and
// the caller is told so rather than left waiting for an attempt that will never
// run.
func TestRequestReconnectCancellationStillFinishesItsAttempt(t *testing.T) {
	supervisorCtx, supervisorCancel := context.WithCancel(context.Background())
	defer supervisorCancel()
	client := &Client{
		lifecycle:         lifecycleIn(lifecycle.Ready),
		supervisorCtx:     supervisorCtx,
		reconnectRequests: make(chan reconnectRequest, 1),
	}
	supervisorCancel()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := client.awaitRebuild(waitCtx, errors.New("canceled request"), 0, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request = %v, want context canceled", err)
	}
}

func TestReconnectStartLogsToTheClientLogger(t *testing.T) {
	for _, test := range []struct {
		name       string
		configured bool
	}{
		{name: "configured logger", configured: true},
		{name: "process default without a configured logger"},
	} {
		t.Run(test.name, func(t *testing.T) {
			defaultOutput := captureProcessDefault(t)
			var configuredOutput logSink
			var logger *slog.Logger
			if test.configured {
				logger = slog.New(slog.NewTextHandler(&configuredOutput, nil))
			}
			fake := clock.NewFake(time.Unix(450, 0))
			recorded := &recordingClock{Fake: fake}
			d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 1)}
			client := newReconnectTestClientWithLogger(t, d, recorded, 0, logger)
			cause := errors.New("reconnect cause for " + test.name)
			if err := client.requestReconnect(cause, 0); err != nil {
				t.Fatal(err)
			}
			advanceReconnect(t, recorded, 500*time.Millisecond, 1)
			if err := client.awaitRebuild(context.Background(), nil, 0, nil); err != nil {
				t.Fatalf("reconnect = %v, want nil", err)
			}
			if !test.configured {
				assertReconnectStartedLog(t, defaultOutput.String(), cause)
				return
			}
			assertReconnectStartedLog(t, configuredOutput.String(), cause)
			if got := defaultOutput.String(); got != "" {
				t.Fatalf("process default output = %q, want empty", got)
			}
		})
	}
}

func TestRunnerReconnectReportsGenerationCause(t *testing.T) {
	defaultOutput := captureProcessDefault(t)
	var configuredOutput logSink
	fake := clock.NewFake(time.Unix(450, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	cause := errors.New("runner generation cause")
	client := newReconnectTestClientWithLogger(
		t,
		d,
		recorded,
		0,
		slog.New(slog.NewTextHandler(&configuredOutput, nil)),
	)
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
	runCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(runCtx) }()
	first := <-d.created
	waitReconnectCondition(t, func() bool { return runner.lifecycle.State() == lifecycle.Ready })
	d.setFailConsumersWithError(1, cause)
	first.sendError(&driver.Error{
		Driver: d.Name(),
		Op:     "consumer",
		K:      driver.KindTransient,
		Err:    errors.New("runner failure signal"),
	})
	advanceReconnect(t, recorded, 500*time.Millisecond, 1)
	waitReconnectCondition(t, func() bool {
		return d.OpenCount() >= 2 && runner.lifecycle.State() == lifecycle.Ready && !client.isReconnecting()
	})
	assertReconnectStartedLog(t, configuredOutput.String(), cause)
	if got := defaultOutput.String(); got != "" {
		t.Fatalf("process default output = %q, want empty", got)
	}
	cancel()
	select {
	case <-runDone:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("runner did not stop after context cancellation")
	}
}

func TestReconnectStartLoggedOnceForSharedAttempt(t *testing.T) {
	var configuredOutput logSink
	fake := clock.NewFake(time.Unix(450, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 1)}
	client := newReconnectTestClientWithLogger(
		t,
		d,
		recorded,
		0,
		slog.New(slog.NewTextHandler(&configuredOutput, nil)),
	)
	cause := errors.New("shared reconnect cause")
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- client.awaitRebuild(context.Background(), cause, 0, nil)
	}()
	waitReconnectCondition(t, func() bool { return recorded.sleepCount() == 1 })
	results := make(chan error, 8)
	for range 8 {
		// Each caller joins the attempt that is already running: the request is
		// made here, while it is in flight, and the wait is what the test
		// watches return.
		if err := client.requestReconnect(cause, 0); err != nil {
			t.Fatal(err)
		}
		go func() { results <- client.awaitRebuild(context.Background(), nil, 0, nil) }()
	}
	advanceReconnect(t, recorded, 500*time.Millisecond, 1)
	select {
	case err := <-firstResult:
		if err != nil {
			t.Fatalf("first awaitRebuild() = %v, want nil", err)
		}
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("first reconnect waiter did not return")
	}
	for range 8 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("shared reconnect waiter = %v, want nil", err)
			}
		case <-clock.NewReal().Timer(time.Second).C:
			t.Fatal("shared reconnect waiter did not return")
		}
	}
	assertReconnectStartedLog(t, configuredOutput.String(), cause)
}

func assertReconnectStartedLog(t *testing.T, output string, cause error) {
	t.Helper()
	if got := strings.Count(output, "f1 reconnect started"); got != 1 {
		t.Fatalf("reconnect start log lines = %d, want 1; output = %q", got, output)
	}
	if !strings.Contains(output, cause.Error()) {
		t.Fatalf("reconnect start output = %q, want cause %q", output, cause)
	}
}

func TestAwaitRebuildPrefersTheRequestError(t *testing.T) {
	driver := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 1)}
	client := newReconnectTestClient(t, driver, nil, 0)
	requestErr := errors.New("request reconnect failed")
	client.mu.Lock()
	client.reconnectErr = requestErr
	client.mu.Unlock()

	if err := client.awaitRebuild(context.Background(), errors.New("cause"), 0, nil); !errors.Is(err, requestErr) {
		t.Fatalf("awaitRebuild() = %v, want request error %v", err, requestErr)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.conn == connReconnecting {
		t.Fatal("request error unexpectedly started a reconnect")
	}
}

func TestRunReturnsFailedPreRunReconnectOwner(t *testing.T) {
	fake := clock.NewFake(time.Unix(350, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 8)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 1)
	client.reconnectRandom = func() float64 { return 0 }
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

	d.setFailOpens(1)
	err = client.requestReconnect(errors.New("pre-run reconnect"), 0)
	if err != nil {
		t.Fatal(err)
	}
	waitReconnectCondition(t, func() bool { return recorded.sleepCount() == 1 })
	select {
	case token := <-recorded.sleepStarted:
		<-token
		fake.Advance(0)
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("pre-run reconnect sleep did not start")
	}
	wantErr := client.awaitRebuild(context.Background(), nil, 0, nil)
	if wantErr == nil {
		t.Fatal("pre-run reconnect returned nil, want fatal error")
	}

	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(context.Background()) }()
	select {
	case consumer := <-d.created:
		t.Fatalf("Run opened consumer %p before returning failed pre-run reconnect", consumer)
	case runErr := <-runDone:
		if runErr != wantErr { //nolint:errorlint // exact attempt error identity is the startup contract
			t.Fatalf("Run() = %v, want exact reconnect error %v", runErr, wantErr)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("Run did not return the failed pre-run reconnect")
	}
	// Health preserves the reconnect cause through errors.Is alongside subscription diagnostics.
	if healthErr := client.Health(context.Background()); !errors.Is(healthErr, wantErr) {
		t.Fatalf("Health() = %v, want reconnect cause %v", healthErr, wantErr)
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

func secondConsumerOpenGate(group string, started chan struct{}, release <-chan struct{}, returnContextError bool, canceled *atomic.Bool) func(context.Context, string) error {
	var opens atomic.Int32
	return func(ctx context.Context, openedGroup string) error {
		if openedGroup != group || opens.Add(1) != 2 {
			return nil
		}
		close(started)
		<-release
		if !returnContextError {
			return nil
		}
		cause := ctx.Err()
		if cause == nil {
			return errors.New("consumer open gate was not canceled")
		}
		if canceled != nil {
			canceled.Store(true)
		}
		return &driver.Error{Driver: "reconnect-test", Op: "consumer", K: driver.KindTransient, Err: cause}
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
	if got := runnerSettlementContext(runner, context.Background()); got == nil {
		t.Fatal("runnerSettlementContext() returned nil")
	}
}

func reconnectMessage(t *testing.T, id string, settler driver.Settler) driver.InboundMessage {
	t.Helper()
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          id,
		Source:      "/test/orders",
		Type:        "orders.created",
		Priority:    PriorityMedium,
	}
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	return driver.InboundMessage{
		Destination: "f1.test.orders.created.medium",
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
		return fatalRunner.lifecycle.State() == lifecycle.Ready && healthyRunner.lifecycle.State() == lifecycle.Ready
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
	if healthyRunner.lifecycle.State() != lifecycle.Ready {
		t.Fatalf("healthy runner state = %s, want ready", healthyRunner.lifecycle.State())
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

func TestAHungRetiredConnectionCloseEndsTheReconnectAndCloseWaitsForIt(t *testing.T) {
	client, d, recorded := newRetirementTestClient(t)
	d.firstCloseGate = make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(d.firstCloseGate) }) }
	t.Cleanup(release)
	old := retirementConnection(d, 0)
	beginRetirementReconnect(t, client, recorded)
	waitReconnectCondition(t, d.firstCloseEntered.Load)
	expireRetirementWait(t, client, recorded)
	if err := timedRetirementClose(t, client, recorded); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close while old connection hangs = %v, want deadline", err)
	}
	if retirementConnection(d, 1).closeCalls.Load() != 0 {
		t.Fatal("current connection closed while retired connection hangs")
	}
	release()
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if old.closeCalls.Load() != 1 || !old.closed.Load() {
		t.Fatalf("old connection calls = %d, closed = %v", old.closeCalls.Load(), old.closed.Load())
	}
	if retirementConnection(d, 1).previousOpenAtClose.Load() != 0 {
		t.Fatal("current connection close entered before retirement completed")
	}
}

func newRetirementTestClient(t *testing.T) (*Client, *reconnectTestDriver, *recordingClock) {
	t.Helper()
	recorded := &recordingClock{
		Fake:         clock.NewFake(time.Unix(940, 0)),
		sleepStarted: make(chan chan struct{}, 8),
		timerStarted: make(chan time.Duration, 64),
	}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 0 }
	publishRetirementMessage(t, client)
	return client, d, recorded
}

func publishRetirementMessage(t *testing.T, client *Client) {
	t.Helper()
	if _, err := client.Publisher().Publish(context.Background(), "orders.created", map[string]string{"value": "retirement"}); err != nil {
		t.Fatal(err)
	}
}

func retirementConnection(d *reconnectTestDriver, index int) *reconnectTestConn {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.connections[index]
}

func holdRetirementProducer(t *testing.T, conn *reconnectTestConn) (<-chan struct{}, func()) {
	t.Helper()
	gate, entered := make(chan struct{}), make(chan struct{})
	conn.closeMu.Lock()
	conn.producerGate, conn.producerEntered = gate, entered
	conn.closeMu.Unlock()
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	return entered, release
}

func awaitRetirementSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	waitReconnectCondition(t, func() bool {
		select {
		case <-signal:
			return true
		default:
			return false
		}
	})
}

func nextRetirementTimer(t *testing.T, recorded *recordingClock) time.Duration {
	t.Helper()
	waitReconnectCondition(t, func() bool { return len(recorded.timerStarted) > 0 })
	return <-recorded.timerStarted
}

func beginRetirementReconnect(t *testing.T, client *Client, recorded *recordingClock) {
	t.Helper()
	client.mu.Lock()
	epoch := client.current.epoch
	client.mu.Unlock()
	if err := client.requestReconnect(errors.New("retirement transport failure"), epoch); err != nil {
		t.Fatal(err)
	}
	waitReconnectCondition(t, func() bool { return len(recorded.sleepStarted) > 0 })
	awaitRetirementSignal(t, <-recorded.sleepStarted)
	// Publish quiescence has finished before the backoff registers. Discard
	// its stopped timer so the next signal identifies this retirement's wait.
	for len(recorded.timerStarted) > 0 {
		<-recorded.timerStarted
	}
	recorded.Advance(0)
}

func expireRetirementWait(t *testing.T, client *Client, recorded *recordingClock) {
	t.Helper()
	if duration := nextRetirementTimer(t, recorded); duration != 100*time.Millisecond {
		t.Fatalf("retirement timer = %v, want close bound", duration)
	}
	// The held driver entry identifies the unfinished stage. A baseline
	// retirement can have left a stopped producer timer in the event queue
	// before registering its held connection timer.
	waitReconnectCondition(t, func() bool { return recorded.NumWaiters() == 1 })
	recorded.Advance(100 * time.Millisecond)
	waitReconnectCondition(t, func() bool { return !client.isReconnecting() })
}

func timedRetirementClose(t *testing.T, client *Client, recorded *recordingClock) error {
	t.Helper()
	for len(recorded.timerStarted) > 0 {
		<-recorded.timerStarted
	}
	closed := make(chan error, 1)
	go func() { closed <- client.Close(context.Background()) }()
	// The first timer belongs to the supervisor barrier. Its completion
	// precedes the second registration, the pending retirement join.
	_ = nextRetirementTimer(t, recorded)
	waitReconnectCondition(t, func() bool {
		select {
		case <-client.supervisorDone:
			return true
		default:
			return false
		}
	})
	waitReconnectCondition(t, func() bool { return len(recorded.timerStarted) > 0 || len(closed) > 0 })
	if len(closed) == 0 {
		if duration := nextRetirementTimer(t, recorded); duration != 100*time.Millisecond {
			t.Fatalf("Close timer = %v, want close bound", duration)
		}
		recorded.Advance(100 * time.Millisecond)
	}
	waitReconnectCondition(t, func() bool { return len(closed) > 0 })
	return <-closed
}

func keptRetirementConsumer(t *testing.T, client *Client, conn *reconnectTestConn, epoch uint64) *reconnectTestConsumer {
	t.Helper()
	consumer, err := conn.Consumer(context.Background(), driver.ConsumerConfig{Group: "kept"})
	if err != nil {
		t.Fatal(err)
	}
	client.keepUnreleasedConsumer(consumer, epoch)
	t.Cleanup(func() { _ = consumer.Release(context.Background()) })
	return consumer.(*reconnectTestConsumer)
}

func TestRetiredProducerTimeoutKeepsConnectionCloseOrdered(t *testing.T) {
	client, d, recorded := newRetirementTestClient(t)
	old := retirementConnection(d, 0)
	consumer := keptRetirementConsumer(t, client, old, 1)
	entered, release := holdRetirementProducer(t, old)
	beginRetirementReconnect(t, client, recorded)
	awaitRetirementSignal(t, entered)
	expireRetirementWait(t, client, recorded)
	if d.OpenCount() != 2 || old.producerCloseCalls.Load() != 1 {
		t.Fatal("reconnect did not install one replacement with one pending producer close")
	}
	if consumer.releaseCalls.Load() != 0 || old.closeCalls.Load() != 0 {
		t.Fatalf("teardown overtook producer: releases = %d, connection closes = %d", consumer.releaseCalls.Load(), old.closeCalls.Load())
	}
	release()
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if consumer.releaseCalls.Load() != 1 || old.closeCalls.Load() != 1 || old.producersAtClose.Load() != 0 || old.openAtClose.Load() != 0 {
		t.Fatal("retirement did not release consumer and close connection once after producer success")
	}
}

func TestClientCloseRejoinsPendingRetirement(t *testing.T) {
	baseline := runtime.NumGoroutine()
	client, d, recorded := newRetirementTestClient(t)
	old := retirementConnection(d, 0)
	entered, release := holdRetirementProducer(t, old)
	beginRetirementReconnect(t, client, recorded)
	awaitRetirementSignal(t, entered)
	expireRetirementWait(t, client, recorded)
	publishRetirementMessage(t, client)
	current := retirementConnection(d, 1)
	if err := timedRetirementClose(t, client, recorded); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close while producer hangs = %v, want deadline", err)
	}
	if old.producerCloseCalls.Load() != 1 || old.closeCalls.Load() != 0 || current.producerCloseCalls.Load() != 0 || current.closeCalls.Load() != 0 {
		t.Fatal("Close duplicated pending work or tore down current resources")
	}
	release()
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if old.producerCloseCalls.Load() != 1 || old.closeCalls.Load() != 1 || current.closeCalls.Load() != 1 {
		t.Fatal("retry did not rejoin the single retirement attempt and close both epochs")
	}
	if current.previousOpenAtClose.Load() != 0 {
		t.Fatal("current connection close entered before retirement completed")
	}
	assertRetirementGoroutinesReturned(t, baseline)
}

func assertRetirementGoroutinesReturned(t *testing.T, baseline int) {
	t.Helper()
	deadline := clock.NewReal().Timer(2 * time.Second)
	defer deadline.Stop()
	poll := clock.NewReal().Ticker(5 * time.Millisecond)
	defer poll.Stop()
	for {
		current := runtime.NumGoroutine()
		if current <= baseline {
			return
		}
		select {
		case <-deadline.C:
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			var stacks []string
			for stack := range strings.SplitSeq(string(buf[:n]), "\n\n") {
				lines := strings.Split(stack, "\n")
				for i := 1; i+1 < len(lines); i++ {
					if strings.HasPrefix(lines[i], "created by ") {
						break
					}
					source := strings.TrimSpace(lines[i+1])
					if strings.Contains(lines[i], "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk") && strings.Contains(source, ".go:") && !strings.Contains(source, "_test.go:") {
						stacks = append(stacks, stack)
						break
					}
				}
			}
			t.Fatalf("goroutines after Close = %d, baseline = %d\nmodule-owned goroutine stacks:\n%s", current, baseline, strings.Join(stacks, "\n\n"))
		case <-poll.C:
		}
	}
}

func TestClientCloseRetriesFailedRetirement(t *testing.T) {
	for _, stage := range []string{"producer", "consumer release", "connection"} {
		t.Run(stage, func(t *testing.T) {
			client, d, recorded := newRetirementTestClient(t)
			old := retirementConnection(d, 0)
			consumer := keptRetirementConsumer(t, client, old, 1)
			failure := errors.New(stage + " retirement failed")
			switch stage {
			case "producer":
				old.producerFailures, old.producerErr = 3, failure
			case "consumer release":
				consumer.setFailRelease(3, failure)
			case "connection":
				old.closeFailures, old.closeErr = 3, failure
			}
			t.Cleanup(func() {
				old.closeMu.Lock()
				old.producerFailures, old.closeFailures = 0, 0
				old.closeMu.Unlock()
				consumer.setFailRelease(0, nil)
			})
			beginRetirementReconnect(t, client, recorded)
			waitReconnectCondition(t, func() bool { return !client.isReconnecting() })
			publishRetirementMessage(t, client)
			current := retirementConnection(d, 1)
			assertCalls := func(attempt int32) {
				t.Helper()
				producer, releases, connection := int32(1), int32(1), int32(0)
				switch stage {
				case "producer":
					producer, releases = attempt, 0
				case "consumer release":
					releases = attempt
				case "connection":
					connection = attempt
				}
				if old.producerCloseCalls.Load() != producer || consumer.releaseCalls.Load() != releases || old.closeCalls.Load() != connection {
					t.Fatalf("attempt %d: producer/release/connection = %d/%d/%d, want %d/%d/%d", attempt, old.producerCloseCalls.Load(), consumer.releaseCalls.Load(), old.closeCalls.Load(), producer, releases, connection)
				}
				if current.producerCloseCalls.Load() != 0 || current.closeCalls.Load() != 0 {
					t.Fatal("failed retirement closed current resources")
				}
			}
			assertCalls(1)
			recorded.Advance(time.Hour)
			assertCalls(1)
			for attempt := int32(2); attempt <= 3; attempt++ {
				if err := client.Close(context.Background()); !errors.Is(err, failure) {
					t.Fatalf("Close attempt %d = %v, want retirement failure", attempt, err)
				}
				assertCalls(attempt)
				recorded.Advance(time.Hour)
				assertCalls(attempt)
			}
			if err := client.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			wantProducer, wantRelease, wantConnection := int32(1), int32(1), int32(1)
			switch stage {
			case "producer":
				wantProducer = 4
			case "consumer release":
				wantRelease = 4
			case "connection":
				wantConnection = 4
			}
			if old.producerCloseCalls.Load() != wantProducer || consumer.releaseCalls.Load() != wantRelease || old.closeCalls.Load() != wantConnection || !old.closed.Load() || current.closeCalls.Load() != 1 {
				t.Fatal("successful retry did not skip completed stages and finish both epochs")
			}
			if old.producersAtClose.Load() != 0 || old.openAtClose.Load() != 0 || d.OpenCount() != 2 {
				t.Fatal("retirement violated dependency order or retried reconnect")
			}
		})
	}
}

func TestRetirementsKeepEveryEpochOwned(t *testing.T) {
	client, d, recorded := newRetirementTestClient(t)
	first := retirementConnection(d, 0)
	firstEntered, releaseFirst := holdRetirementProducer(t, first)
	beginRetirementReconnect(t, client, recorded)
	awaitRetirementSignal(t, firstEntered)
	expireRetirementWait(t, client, recorded)
	publishRetirementMessage(t, client)
	second := retirementConnection(d, 1)
	secondEntered, releaseSecond := holdRetirementProducer(t, second)
	beginRetirementReconnect(t, client, recorded)
	awaitRetirementSignal(t, secondEntered)
	expireRetirementWait(t, client, recorded)
	if first.closeCalls.Load() != 0 || second.closeCalls.Load() != 0 {
		t.Fatal("an old connection closed before its producer")
	}
	releaseSecond()
	waitReconnectCondition(t, second.closed.Load)
	current := retirementConnection(d, 2)
	if err := timedRetirementClose(t, client, recorded); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close with epoch 1 pending = %v, want deadline", err)
	}
	if first.closeCalls.Load() != 0 || current.closeCalls.Load() != 0 || second.closeCalls.Load() != 1 {
		t.Fatal("out-of-order completion lost or duplicated a retirement")
	}
	releaseFirst()
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, conn := range []*reconnectTestConn{first, second} {
		if conn.producerCloseCalls.Load() != 1 || conn.closeCalls.Load() != 1 || conn.producersAtClose.Load() != 0 || !conn.closed.Load() {
			t.Fatal("stacked retirement was not closed once after its producer")
		}
	}
	if current.closeCalls.Load() != 1 || current.previousOpenAtClose.Load() != 0 {
		t.Fatal("current epoch was not closed after both retirements")
	}
}

func TestCloseJoinsRetirementBetweenProducerAndConnection(t *testing.T) {
	client, d, recorded := newRetirementTestClient(t)
	d.firstCloseGate = make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(d.firstCloseGate) }) }
	t.Cleanup(release)
	old := retirementConnection(d, 0)
	beginRetirementReconnect(t, client, recorded)
	waitReconnectCondition(t, d.firstCloseEntered.Load)
	if old.producerCloseCalls.Load() != 1 || old.producer.Load() != 0 {
		t.Fatal("connection close entered before producer completed")
	}
	expireRetirementWait(t, client, recorded)
	closed := make(chan error, 1)
	go func() { closed <- client.Close(context.Background()) }()
	_ = nextRetirementTimer(t, recorded)
	waitReconnectCondition(t, func() bool {
		select {
		case <-client.supervisorDone:
			return true
		default:
			return false
		}
	})
	waitReconnectCondition(t, func() bool { return len(recorded.timerStarted) > 0 || len(closed) > 0 })
	if len(closed) > 0 {
		t.Fatalf("Close returned before retired connection completed: %v", <-closed)
	}
	_ = nextRetirementTimer(t, recorded)
	if old.closeCalls.Load() != 1 || retirementConnection(d, 1).closeCalls.Load() != 0 {
		t.Fatal("Close duplicated old connection close or closed current connection early")
	}
	release()
	waitReconnectCondition(t, func() bool { return len(closed) > 0 })
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if old.producerCloseCalls.Load() != 1 || old.closeCalls.Load() != 1 || !old.closed.Load() {
		t.Fatal("Close did not join the single sequential retirement")
	}
	if retirementConnection(d, 1).previousOpenAtClose.Load() != 0 {
		t.Fatal("current connection close entered before retirement completed")
	}
}

func TestSevereConsumerErrorKeepsTheErrorReaderAlive(t *testing.T) {
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 1)}
	recorder := newErrorHandlerRecorder()
	client := newReconnectTestClient(t, d, nil, 0, WithErrorHandler(recorder.handle))
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "severe",
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
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	var consumer *reconnectTestConsumer
	select {
	case consumer = <-d.created:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("timed out waiting for the subscription consumer")
	}
	waitReconnectCondition(t, func() bool { return runner.lifecycle.State() == lifecycle.Ready })

	consumer.sendError(&driver.Error{Driver: d.Name(), Op: "consumer", K: driver.KindNotFound, Err: errors.New("queue missing")})
	recorder.waitForCall(t, time.Second)
	if state := runner.lifecycle.State(); state != lifecycle.Ready {
		t.Fatalf("state after a not-found consumer error = %s, want ready", state)
	}

	// A fatal error after the severe one must still reach the core, so the
	// reader has to keep draining Errors() once it has reported the first.
	fatal := &driver.Error{Driver: d.Name(), Op: "consumer", K: driver.KindFatal, Err: errors.New("permission denied")}
	consumer.sendError(fatal)
	select {
	case runErr := <-done:
		// Run returns the first failure it saw, which is the not-found error.
		if runErr == nil {
			t.Fatal("Run() = nil, want the runner's failure")
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("a fatal consumer error after a not-found one did not stop the runner")
	}
	if state := runner.lifecycle.State(); state != lifecycle.Failed {
		t.Fatalf("state = %s, want failed", state)
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
	waitReconnectCondition(t, func() bool { return runner.lifecycle.State() == lifecycle.Ready })
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
		return machine != nil && machine.State() == lifecycle.Ready
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
	wantDestination := "f1.test.orders.created.orders.high.retry.1"
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
	waitReconnectCondition(t, func() bool { return runner.lifecycle.State() == lifecycle.Ready })
	first.sendError(&driver.Error{Driver: d.Name(), Op: "consumer", K: driver.KindTransient, Err: errors.New("transient")})
	second := <-d.created
	if first == second {
		t.Fatal("repair reused the old consumer")
	}
	waitReconnectCondition(t, func() bool { return runner.lifecycle.State() == lifecycle.Ready })
	if client.isReconnecting() {
		t.Fatal("lane repair entered client reconnecting state")
	}
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health during lane repair = %v, want nil", err)
	}
	publishErr := publishMessages(client, context.Background(), driver.OutboundMessage{Destination: "test"})
	if publishErr != nil {
		t.Fatalf("publish during lane repair = %v", publishErr)
	}
	if runner.lifecycle.State() != lifecycle.Ready {
		t.Fatalf("runner state during lane repair = %s, want ready", runner.lifecycle.State())
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
	life := client.lifecycleLocked()
	client.mu.Unlock()
	if life != lifecycle.Closed {
		t.Fatalf("lifecycle after Close = %s, want closed", life)
	}
}

func TestRunnerPreservesOwnerThroughConsumerOpenCancellation(t *testing.T) {
	fake := clock.NewFake(time.Unix(350, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 8)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 16)}
	client := newReconnectTestClient(t, d, recorded, 0)
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

	victimOpenStarted := make(chan struct{})
	victimOpenRelease := make(chan struct{})
	var openCanceled atomic.Bool
	var releaseOpenOnce sync.Once
	d.mu.Lock()
	d.consumerOpenHook = secondConsumerOpenGate("victim", victimOpenStarted, victimOpenRelease, true, &openCanceled)
	d.mu.Unlock()
	t.Cleanup(func() { releaseOpenOnce.Do(func() { close(victimOpenRelease) }) })

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
		return causingRunner.lifecycle.State() == lifecycle.Ready && victimRunner.lifecycle.State() == lifecycle.Ready
	})

	initial["victim"].sendError(&driver.Error{
		Driver: d.Name(),
		Op:     "consumer",
		K:      driver.KindTransient,
		Err:    errors.New("victim consumer disconnected"),
	})
	select {
	case <-victimOpenStarted:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("victim replacement consumer did not block in open")
	}

	cause := errors.New("causing runner requested reconnect")
	reconnectDone := make(chan error, 1)
	go func() { reconnectDone <- client.awaitRebuild(context.Background(), cause, 0, nil) }()
	waitReconnectCondition(t, func() bool { return recorded.sleepCount() == 1 })
	select {
	case token := <-recorded.sleepStarted:
		<-token
		fake.Advance(0)
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("supervisor reconnect sleep did not start")
	}
	select {
	case err := <-reconnectDone:
		if err != nil {
			t.Fatalf("supervisor reconnect = %v, want nil", err)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("supervisor reconnect did not finish")
	}

	sent := make(map[*reconnectTestConsumer]bool)
	serviceCtx, stopServing := context.WithCancel(context.Background())
	servingDone := make(chan struct{})
	var serviceMu sync.Mutex
	go func() {
		defer close(servingDone)
		for {
			select {
			case <-serviceCtx.Done():
				return
			case consumer := <-d.created:
				serviceMu.Lock()
				if serviceCtx.Err() != nil {
					serviceMu.Unlock()
					return
				}
				if sent[consumer] {
					serviceMu.Unlock()
					continue
				}
				sent[consumer] = true
				switch consumer.group {
				case "causing":
					consumer.send(validReconnectMessage(t, "causing-after-open-cancellation"))
				case "victim":
					consumer.send(validReconnectMessage(t, "victim-after-open-cancellation"))
				}
				serviceMu.Unlock()
			case token := <-recorded.sleepStarted:
				serviceMu.Lock()
				if serviceCtx.Err() != nil {
					serviceMu.Unlock()
					return
				}
				<-token
				fake.Advance(0)
				serviceMu.Unlock()
			}
		}
	}()
	stopService := func() {
		serviceMu.Lock()
		stopServing()
		serviceMu.Unlock()
		<-servingDone
	}
	defer stopService()
	releaseOpenOnce.Do(func() { close(victimOpenRelease) })
	waitReconnectCondition(t, func() bool {
		serviceMu.Lock()
		defer serviceMu.Unlock()
		return causingHandled.Load() >= 1 &&
			victimHandled.Load() >= 1 &&
			!client.isReconnecting()
	})
	if !openCanceled.Load() {
		t.Fatal("replacement consumer open was not canceled")
	}
	if got := recorded.sleepCount(); got != 1 {
		t.Fatalf("reconnect sleep count = %d, want 1", got)
	}
	if got := d.OpenCount(); got != 2 {
		t.Fatalf("driver Open count = %d, want 2", got)
	}
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health after consumer-open cancellation = %v, want nil", err)
	}
	select {
	case err := <-victimDone:
		t.Fatalf("victim Run returned after consumer-open cancellation: %v", err)
	default:
	}
	select {
	case err := <-causingDone:
		t.Fatalf("causing Run returned after consumer-open cancellation: %v", err)
	default:
	}
	stopService()
}

// TestRunnerStartedDuringReconnectOpensOnTheReplacementConnection proves that a
// runner which starts while a reconnect is in progress opens its consumer on
// the connection the reconnect leaves live, instead of on the connection that
// reconnect is about to retire.
func TestRunnerStartedDuringReconnectOpensOnTheReplacementConnection(t *testing.T) {
	fake := clock.NewFake(time.Unix(400, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 4)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 4)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 0 }

	if err := client.requestReconnect(errors.New("supervisor reconnect"), 0); err != nil {
		t.Fatalf("requestReconnect = %v, want nil", err)
	}
	select {
	case token := <-recorded.sleepStarted:
		<-token
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("supervisor reconnect sleep did not start")
	}
	// The supervisor is parked in its backoff. It has abandoned the runners it
	// knew about and has not opened the replacement, so c.current.conn is still
	// the connection it is going to retire.

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
	runCtx, cancelRun := context.WithCancel(context.Background())
	t.Cleanup(cancelRun)
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(runCtx) }()

	// The supervisor is parked in its backoff with the clock stopped, so the
	// runner has nothing to do but wait: a consumer it opened here would be one
	// on the connection the swap is about to retire, and the assertions below
	// are what catch it.
	waitReconnectCondition(t, func() bool {
		runner.mu.Lock()
		defer runner.mu.Unlock()
		return runner.started
	})
	fake.Advance(0)
	waitReconnectCondition(t, func() bool { return !client.isReconnecting() })

	d.mu.Lock()
	connections := append([]*reconnectTestConn(nil), d.connections...)
	d.mu.Unlock()
	if len(connections) != 2 {
		t.Fatalf("driver connections after the reconnect = %d, want 2", len(connections))
	}
	oldConn, replacement := connections[0], connections[1]

	var consumer *reconnectTestConsumer
	select {
	case consumer = <-d.created:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("runner opened no consumer")
	}
	if consumer.conn != replacement {
		t.Fatalf("runner consumer connection = %p, want the replacement connection %p", consumer.conn, replacement)
	}
	if got := oldConn.openAtClose.Load(); got != 0 {
		t.Fatalf("consumers open when the retired connection closed = %d, want 0", got)
	}
	select {
	case err := <-runDone:
		t.Fatalf("runner Run returned during the reconnect: %v", err)
	default:
	}
}

// TestConsumerOpenedAsAReconnectBeginsIsReleased proves that a consumer whose
// open straddles the start of a reconnect is released rather than adopted: the
// reconnect's abandon ran before that consumer existed, so nothing else would
// ever close it, and the connection it was opened on would be retired with a
// consumer still on it.
func TestConsumerOpenedAsAReconnectBeginsIsReleased(t *testing.T) {
	fake := clock.NewFake(time.Unix(400, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 4)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 4)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 0 }

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
	runCtx, cancelRun := context.WithCancel(context.Background())
	t.Cleanup(cancelRun)
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(runCtx) }()

	failing := <-d.created
	d.mu.Lock()
	retiring := d.connections[0]
	d.mu.Unlock()
	waitReconnectCondition(t, func() bool { return runner.lifecycle.State() == lifecycle.Ready })

	// The next open is the one that straddles the reconnect: it asks for a
	// reconnect and waits until the supervisor has abandoned the runners (the
	// backoff it parks in is after the abandon), so its consumer is created
	// after that abandon and lands on the connection now being retired.
	var gateOnce sync.Once
	var gateFailed atomic.Bool
	d.mu.Lock()
	d.consumerOpenHook = func(context.Context, string) error {
		gateOnce.Do(func() {
			if err := client.requestReconnect(errors.New("consumer open requested reconnect"), 0); err != nil {
				gateFailed.Store(true)
				return
			}
			select {
			case token := <-recorded.sleepStarted:
				<-token
			case <-clock.NewReal().Timer(time.Second).C:
				gateFailed.Store(true)
			}
		})
		return nil
	}
	d.mu.Unlock()

	failing.sendError(&driver.Error{Driver: d.Name(), Op: "consumer", K: driver.KindTransient, Err: errors.New("consumer disconnected")})
	var replacement *reconnectTestConsumer
	select {
	case replacement = <-d.created:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("runner opened no replacement consumer")
	}
	if replacement.conn != retiring {
		t.Fatalf("replacement consumer connection = %p, want the retiring connection %p", replacement.conn, retiring)
	}

	// The runner either releases the consumer it opened here or adopts it as
	// its generation's consumer. Waiting for that decision makes the count the
	// retiring connection records the runner's decision, not the order two
	// goroutines reached the connection in.
	waitReconnectCondition(t, func() bool {
		if retiring.openConsumers.Load() == 0 {
			return true
		}
		runner.mu.Lock()
		defer runner.mu.Unlock()
		return runner.consumer == replacement
	})

	fake.Advance(0)
	waitReconnectCondition(t, func() bool { return !client.isReconnecting() })

	if got := retiring.openAtClose.Load(); got != 0 {
		t.Fatalf("consumers open when the retired connection closed = %d, want 0", got)
	}
	d.mu.Lock()
	connections := append([]*reconnectTestConn(nil), d.connections...)
	d.mu.Unlock()
	if len(connections) != 2 {
		t.Fatalf("driver connections after the reconnect = %d, want 2", len(connections))
	}
	liveOn := connections[1]
	waitReconnectCondition(t, func() bool {
		runner.mu.Lock()
		defer runner.mu.Unlock()
		consumer, ok := runner.consumer.(*reconnectTestConsumer)
		return ok && consumer.conn == liveOn
	})
	if gateFailed.Load() {
		t.Fatal("consumer open gate did not reach the running abandon")
	}
	select {
	case err := <-runDone:
		t.Fatalf("runner Run returned during the reconnect: %v", err)
	default:
	}
}

// TestConsumerOpenedAfterTheConnectionWasReplacedIsReleased proves that a
// consumer whose open straddles a connection swap is released rather than
// adopted: the runner captured its connection before the swap, so the consumer
// it opened belongs to the incarnation the client has left and nothing would
// release it once the connection is retired. It also pins the shape of the one
// thing that discard reports: a single debug entry naming the subscription and
// both incarnations, and no Health entry, because a discard the runtime repairs
// on its next iteration is not a health transition.
//
// The swap is installed inside the consumer open on purpose, because that is
// where the runner's view of the connection is already fixed: it read the
// connection and its incarnation as one value before asking the driver, and the
// admission runs after the driver answers. Installing it there is what makes
// the interleaving deterministic instead of a race the test would have to win.
func TestConsumerOpenedAfterTheConnectionWasReplacedIsReleased(t *testing.T) {
	fake := clock.NewFake(time.Unix(500, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 4)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	var delivered atomic.Int32
	var output logSink
	// The discard is reported at debug level, so the handler has to admit debug
	// entries for the sink to see it.
	logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := newReconnectTestClientWithLogger(t, d, recorded, 0, logger)
	client.reconnectRandom = func() float64 { return 0 }

	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Prefetch:       12,
		HandlerTimeout: 10 * time.Millisecond,
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error {
				delivered.Add(1)
				return nil
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	t.Cleanup(cancelRun)
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(runCtx) }()

	initial := <-d.created
	d.mu.Lock()
	retiring := d.connections[0]
	d.mu.Unlock()
	waitReconnectCondition(t, func() bool { return runner.lifecycle.State() == lifecycle.Ready })

	var gateOnce sync.Once
	var gateFailed atomic.Bool
	d.mu.Lock()
	d.consumerOpenHook = func(ctx context.Context, _ string) error {
		gateOnce.Do(func() {
			replacement, openErr := d.Open(ctx, driver.Config{})
			if openErr != nil {
				gateFailed.Store(true)
				return
			}
			client.mu.Lock()
			client.current = currentConnection{conn: replacement, epoch: client.current.epoch + 1}
			client.mu.Unlock()
		})
		return nil
	}
	d.mu.Unlock()

	// A transient consumer error ends the first generation, so the loop opens
	// the next consumer through the gate above, on the connection the runner
	// is about to lose.
	initial.sendError(&driver.Error{Driver: d.Name(), Op: "consumer", K: driver.KindTransient, Err: errors.New("consumer disconnected")})

	var stale *reconnectTestConsumer
	select {
	case stale = <-d.created:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("runner opened no consumer across the swap")
	}
	if stale.conn != retiring {
		t.Fatalf("consumer opened across the swap = %p, want the retiring connection %p", stale.conn, retiring)
	}

	var live *reconnectTestConsumer
	select {
	case live = <-d.created:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("runner did not open a consumer again after the swap")
	}
	if live == stale {
		t.Fatal("runner reused the consumer opened on the replaced connection")
	}
	if live.conn == retiring {
		t.Fatalf("consumer after the swap = %p, want the replacement connection", live.conn)
	}
	if !stale.closed.Load() {
		t.Fatal("the consumer opened on the replaced connection was not released")
	}

	// The generation the runner opened after the swap must be the one that
	// serves deliveries, so the replacement consumer is exercised rather than
	// only observed.
	if !live.send(validReconnectMessage(t, "after-the-swap")) {
		t.Fatal("the replacement consumer was already closed")
	}
	waitReconnectCondition(t, func() bool { return delivered.Load() == 1 })
	if gateFailed.Load() {
		t.Fatal("the consumer open gate did not install the replacement connection")
	}

	// The discard reports itself once, naming the incarnations it compared: the
	// connection New opened is incarnation 1, the swap installed 2, and those
	// are the numbers the discard saw. It is not a Health entry, so Health still
	// answers for a client whose runner discarded a consumer and went on.
	var discards []string
	for line := range strings.SplitSeq(output.String(), "\n") {
		if strings.Contains(line, "f1 consumer discarded after the connection was replaced") {
			discards = append(discards, line)
		}
	}
	if len(discards) != 1 {
		t.Fatalf("consumer discard entries = %d, want 1; output = %q", len(discards), output.String())
	}
	for _, want := range []string{"level=DEBUG", "subscription=orders", "opened_epoch=1", "current_epoch=2"} {
		if !strings.Contains(discards[0], want) {
			t.Fatalf("consumer discard entry = %q, want %s", discards[0], want)
		}
	}
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health() after a discarded consumer = %v, want nil", err)
	}

	select {
	case err := <-runDone:
		t.Fatalf("runner Run returned after delivering on the replacement consumer: %v", err)
	default:
	}
}

func TestPublishFailsFastDuringReconnect(t *testing.T) {
	fake := clock.NewFake(time.Unix(150, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 1 }
	if err := client.requestReconnect(errors.New("transient"), 0); err != nil {
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
	// The connection a swap installs and the incarnation it names are one
	// value, which is what makes the epoch the answer to "is this still the
	// connection I started on". A publish that spans the swap is refused by the
	// move.
	client.current = currentConnection{conn: replacement, epoch: client.current.epoch + 1}
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
	err := client.requestReconnect(errors.New("transient"), 0)
	if err != nil {
		t.Fatal(err)
	}
	for i, nominal := range []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second} {
		want := time.Duration(float64(nominal) * samples[i])
		advanceReconnect(t, recorded, want, i+1)
		if got := recorded.sleepAt(i); got != want {
			t.Fatalf("backoff[%d] = %s, want %s", i, got, want)
		}
	}
	if err := client.awaitRebuild(context.Background(), nil, 0, nil); err == nil {
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
	err := client.requestReconnect(errors.New("transient"), 0)
	if err != nil {
		t.Fatal(err)
	}
	advanceReconnect(t, recorded, 500*time.Millisecond, 1)
	reconnectErr := client.awaitRebuild(context.Background(), nil, 0, nil)
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
	life := client.lifecycleLocked()
	client.mu.Unlock()
	if life != lifecycle.Ready {
		t.Fatalf("lifecycle after reconnect exhaustion = %s, want ready: exhaustion is not shutdown", life)
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
	err := client.requestReconnect(errors.New("transient"), 0)
	if err != nil {
		t.Fatal(err)
	}
	for i, duration := range []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second} {
		advanceReconnect(t, recorded, duration, i+1)
	}
	if err := client.awaitRebuild(context.Background(), nil, 0, nil); err != nil {
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
	waitReconnectCondition(t, func() bool { return runner.lifecycle.State() == lifecycle.Ready })
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
	err := client.requestReconnect(errors.New("transient"), 0)
	if err != nil {
		t.Fatal(err)
	}
	waitReconnectCondition(t, func() bool { return client.isReconnecting() && recorded.sleepCount() == 1 })
	client.mu.Lock()
	life := client.lifecycleLocked()
	client.mu.Unlock()
	if life != lifecycle.Ready {
		t.Fatalf("lifecycle during a reconnect = %s, want ready: a rebuild is the connection axis, not the lifecycle", life)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.awaitRebuild(context.Background(), nil, 0, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("reconnect after Close = %v, want context canceled", err)
	}
	select {
	case <-client.supervisorDone:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("reconnect supervisor did not stop after Close")
	}
}

// TestHealthReportsReconnectingWhileAnAttemptRebuildsTheConnection pins what a
// liveness probe sees during a rebuild: the connection axis says an attempt is
// in flight, so Health answers with the reconnecting error rather than
// reporting ready against a connection that is not the client's to use. The
// supervisor is parked in its backoff, so the attempt stays in flight for the
// whole assertion.
func TestHealthReportsReconnectingWhileAnAttemptRebuildsTheConnection(t *testing.T) {
	fake := clock.NewFake(time.Unix(310, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 1 }
	if err := client.requestReconnect(errors.New("transient"), 0); err != nil {
		t.Fatal(err)
	}
	waitReconnectCondition(t, func() bool { return client.isReconnecting() && recorded.sleepCount() == 1 })

	healthErr := client.Health(context.Background())
	if healthErr == nil || !strings.Contains(healthErr.Error(), "client is reconnecting") {
		t.Fatalf("Health() during a rebuild = %v, want the reconnecting error", healthErr)
	}
}

// TestReconnectPolicyCapsTheNominalDelay pins the ceiling of the reconnect
// backoff: however many attempts have failed, the nominal delay a jittered
// sleep is drawn from stays at 30 seconds. The early rungs are pinned through
// the client by TestReconnectBackoffBudgetAndJitter.
func TestReconnectPolicyCapsTheNominalDelay(t *testing.T) {
	if got := reconnectPolicy.DelayFor(100); got != 30*time.Second {
		t.Fatalf("reconnect delay at attempt 100 = %s, want 30s", got)
	}
}

// --- Which stopped subscriptions reach Client.Health ---

func TestSuccessorPublishFailureRecordsTheStoppedSubscription(t *testing.T) {
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, nil, 0)
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Prefetch:       12,
		Retry:          RetryConfig{MaxAttempts: 1},
		HandlerTimeout: 10 * time.Millisecond,
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error {
				return errors.New("handler failed")
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	publishErr := errors.New("dead-letter publish failed")
	d.setPublishError(publishErr)
	runCtx, cancelRun := context.WithCancel(context.Background())
	t.Cleanup(cancelRun)
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(runCtx) }()
	consumer := <-d.created
	waitReconnectCondition(t, func() bool { return runner.lifecycle.State() == lifecycle.Ready })
	if !consumer.send(validReconnectMessage(t, "successor-publish-failure")) {
		t.Fatal("consumer rejected the message")
	}
	var runErr error
	select {
	case runErr = <-runDone:
	case <-clock.NewReal().Timer(3 * time.Second).C:
		t.Fatal("runner did not stop after its successor publish failed")
	}
	if !errors.Is(runErr, publishErr) {
		t.Fatalf("Run() = %v, want the successor publish failure %v", runErr, publishErr)
	}
	healthErr := client.Health(context.Background())
	if !errors.Is(healthErr, publishErr) {
		t.Fatalf("Health() = %v, want the successor publish failure %v", healthErr, publishErr)
	}
	if !strings.Contains(healthErr.Error(), "subscription orders failed") {
		t.Fatalf("Health() = %v, want the orders subscription named", healthErr)
	}
}

func TestFatalReconnectFailureRecordsTheStoppedSubscription(t *testing.T) {
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, nil, 0)
	client.reconnectRandom = func() float64 { return 0 }
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
	runCtx, cancelRun := context.WithCancel(context.Background())
	t.Cleanup(cancelRun)
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(runCtx) }()
	<-d.created
	waitReconnectCondition(t, func() bool { return runner.lifecycle.State() == lifecycle.Ready })

	openErr := &driver.Error{Driver: d.Name(), Op: "open", K: driver.KindFatal, Err: errors.New("credentials rejected")}
	d.setFailOpensWithError(1, openErr)
	if err := client.requestReconnect(errors.New("connection lost"), 0); err != nil {
		t.Fatal(err)
	}
	var runErr error
	select {
	case runErr = <-runDone:
	case <-clock.NewReal().Timer(3 * time.Second).C:
		t.Fatal("runner did not stop after a fatal reconnect failure")
	}
	if !errors.Is(runErr, openErr) {
		t.Fatalf("Run() = %v, want the fatal reconnect failure %v", runErr, openErr)
	}
	healthErr := client.Health(context.Background())
	if !errors.Is(healthErr, openErr) {
		t.Fatalf("Health() = %v, want the fatal reconnect failure %v", healthErr, openErr)
	}
	if !strings.Contains(healthErr.Error(), "subscription orders failed") {
		t.Fatalf("Health() = %v, want the orders subscription named", healthErr)
	}
}

func TestReconnectReleaseFailureStillAbandonsEverySubscription(t *testing.T) {
	var sink logSink
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClientWithLogger(t, d, nil, 0, slog.New(slog.NewTextHandler(&sink, nil)))
	client.reconnectRandom = func() float64 { return 0 }
	subscribe := func(name string, handled *atomic.Int32) *Runner {
		t.Helper()
		runner, err := client.Subscribe(context.Background(), Subscription{
			Name:           name,
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
		return runner
	}
	var alphaHandled, betaHandled atomic.Int32
	alpha := subscribe("alpha", &alphaHandled)
	beta := subscribe("beta", &betaHandled)
	alphaCtx, cancelAlpha := context.WithCancel(context.Background())
	betaCtx, cancelBeta := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancelAlpha()
		cancelBeta()
	})
	alphaDone := make(chan error, 1)
	betaDone := make(chan error, 1)
	go func() { alphaDone <- alpha.Run(alphaCtx) }()
	go func() { betaDone <- beta.Run(betaCtx) }()

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
		return alpha.lifecycle.State() == lifecycle.Ready && beta.lifecycle.State() == lifecycle.Ready
	})

	releaseErr := errors.New("release failed")
	initial["alpha"].setFailRelease(1, releaseErr)
	if err := client.requestReconnect(errors.New("connection lost"), 0); err != nil {
		t.Fatal(err)
	}

	rebuilt := make(map[string]*reconnectTestConsumer, 2)
	rebuiltTimer := clock.NewReal().Timer(2 * time.Second)
	defer rebuiltTimer.Stop()
	for len(rebuilt) < 2 {
		select {
		case consumer := <-d.created:
			rebuilt[consumer.group] = consumer
		case <-rebuiltTimer.C:
			t.Fatal("timed out waiting for both rebuilt consumers")
		}
	}
	waitReconnectCondition(t, func() bool {
		return alpha.lifecycle.State() == lifecycle.Ready && beta.lifecycle.State() == lifecycle.Ready
	})
	if !rebuilt["alpha"].send(validReconnectMessage(t, "alpha-after-abandon")) {
		t.Fatal("rebuilt alpha consumer rejected a message")
	}
	if !rebuilt["beta"].send(validReconnectMessage(t, "beta-after-abandon")) {
		t.Fatal("rebuilt beta consumer rejected a message")
	}
	waitReconnectCondition(t, func() bool {
		return alphaHandled.Load() == 1 && betaHandled.Load() == 1
	})
	select {
	case err := <-alphaDone:
		t.Fatalf("alpha Run returned during the reconnect: %v", err)
	default:
	}
	select {
	case err := <-betaDone:
		t.Fatalf("beta Run returned during the reconnect: %v", err)
	default:
	}
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health after the reconnect = %v, want nil", err)
	}
	output := sink.String()
	if !strings.Contains(output, "level=WARN") || !strings.Contains(output, "subscription=alpha") || !strings.Contains(output, releaseErr.Error()) {
		t.Fatalf("reconnect log = %q, want a warning naming alpha and %v", output, releaseErr)
	}
}

func TestRecordedSubscriptionFailureClearsOnlyOnAFreshStart(t *testing.T) {
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, nil, 0)
	client.reconnectRandom = func() float64 { return 0 }
	subscribe := func() *Runner {
		t.Helper()
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
		return runner
	}
	start := func(runner *Runner) (context.CancelFunc, <-chan error) {
		t.Helper()
		runCtx, cancelRun := context.WithCancel(context.Background())
		t.Cleanup(cancelRun)
		runDone := make(chan error, 1)
		go func() { runDone <- runner.Run(runCtx) }()
		return cancelRun, runDone
	}

	_, firstDone := start(subscribe())
	firstConsumer := <-d.created
	firstFatal := &driver.Error{Driver: d.Name(), Op: "consumer", K: driver.KindFatal, Err: errors.New("permission denied")}
	firstConsumer.sendError(firstFatal)
	select {
	case err := <-firstDone:
		if !errors.Is(err, firstFatal) {
			t.Fatalf("first Run() = %v, want the fatal consumer error %v", err, firstFatal)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("first runner did not stop after a fatal consumer error")
	}
	healthErr := client.Health(context.Background())
	if !errors.Is(healthErr, firstFatal) || !strings.Contains(healthErr.Error(), "subscription orders failed") {
		t.Fatalf("Health after a fatal consumer error = %v, want the orders failure", healthErr)
	}

	second := subscribe()
	_, _ = start(second)
	<-d.created
	waitReconnectCondition(t, func() bool { return second.lifecycle.State() == lifecycle.Ready })
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health after the name reached ready again = %v, want nil", err)
	}

	third := subscribe()
	_, thirdDone := start(third)
	thirdConsumer := <-d.created
	waitReconnectCondition(t, func() bool { return third.lifecycle.State() == lifecycle.Ready })
	thirdFatal := &driver.Error{Driver: d.Name(), Op: "consumer", K: driver.KindFatal, Err: errors.New("quota exceeded")}
	thirdConsumer.sendError(thirdFatal)
	select {
	case err := <-thirdDone:
		if !errors.Is(err, thirdFatal) {
			t.Fatalf("third Run() = %v, want the fatal consumer error %v", err, thirdFatal)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("third runner did not stop after a fatal consumer error")
	}
	if err := client.requestReconnect(errors.New("connection lost"), 0); err != nil {
		t.Fatal(err)
	}
	waitReconnectCondition(t, func() bool {
		return d.OpenCount() == 2 && second.lifecycle.State() == lifecycle.Ready && !client.isReconnecting()
	})
	healthErr = client.Health(context.Background())
	if !errors.Is(healthErr, thirdFatal) {
		t.Fatalf("Health after a live runner reconnected = %v, want the recorded %v", healthErr, thirdFatal)
	}
}

func TestRecordedSubscriptionFailureIsOneEntryPerName(t *testing.T) {
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, nil, 0)
	first := subscribeOrders(t, client)
	second := subscribeOrders(t, client)
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancelFirst()
		cancelSecond()
	})
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() { firstDone <- first.Run(firstCtx) }()
	go func() { secondDone <- second.Run(secondCtx) }()

	consumers := make([]*reconnectTestConsumer, 0, 2)
	consumersTimer := clock.NewReal().Timer(2 * time.Second)
	defer consumersTimer.Stop()
	for len(consumers) < 2 {
		select {
		case consumer := <-d.created:
			consumers = append(consumers, consumer)
		case <-consumersTimer.C:
			t.Fatal("timed out waiting for both consumers of the shared subscription name")
		}
	}
	waitReconnectCondition(t, func() bool {
		return first.lifecycle.State() == lifecycle.Ready && second.lifecycle.State() == lifecycle.Ready
	})

	// Two runners carry one subscription name and each stops on its own fatal
	// consumer error, so the name is recorded twice unless a record replaces
	// the entry the name already has.
	firstFatal := &driver.Error{Driver: d.Name(), Op: "consumer", K: driver.KindFatal, Err: errors.New("permission denied")}
	secondFatal := &driver.Error{Driver: d.Name(), Op: "consumer", K: driver.KindFatal, Err: errors.New("quota exceeded")}
	consumers[0].sendError(firstFatal)
	consumers[1].sendError(secondFatal)
	for name, done := range map[string]<-chan error{"first": firstDone, "second": secondDone} {
		select {
		case err := <-done:
			if err == nil {
				t.Fatalf("%s Run() = nil, want a fatal consumer error", name)
			}
		case <-clock.NewReal().Timer(2 * time.Second).C:
			t.Fatalf("%s runner did not stop after a fatal consumer error", name)
		}
	}
	healthErr := client.Health(context.Background())
	if !errors.Is(healthErr, firstFatal) && !errors.Is(healthErr, secondFatal) {
		t.Fatalf("Health() = %v, want one of the two fatal consumer errors", healthErr)
	}
	if got := strings.Count(healthErr.Error(), "f1: subscription orders failed"); got != 1 {
		t.Fatalf("Health() reports the orders failure %d times, want 1: %v", got, healthErr)
	}
}

func TestCancelledDrainedAndRefusedRunsDoNotRecordFailures(t *testing.T) {
	t.Run("ordinary failure is recorded", func(t *testing.T) {
		d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
		client := newReconnectTestClient(t, d, nil, 0)
		runner, err := client.Subscribe(context.Background(), Subscription{
			Name:           "orders",
			Topics:         []string{"orders.created"},
			HandlerTimeout: 10 * time.Millisecond,
			Handlers:       map[string]Handler{"orders.created": HandlerFunc(func(context.Context, *Event) error { return nil })},
		})
		if err != nil {
			t.Fatal(err)
		}
		fatal := errors.New("ordinary runner failure")
		runner.recordRunExit(context.Background(), fatal)
		if healthErr := client.Health(context.Background()); !errors.Is(healthErr, fatal) {
			t.Fatalf("Health() = %v, want ordinary runner failure", healthErr)
		}
	})
	t.Run("caller cancel", func(t *testing.T) {
		openEntered := make(chan struct{})
		d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
		d.consumerOpenHook = func(ctx context.Context, _ string) error {
			close(openEntered)
			<-ctx.Done()
			return context.Canceled
		}
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
		runCtx, cancelRun := context.WithCancel(context.Background())
		runDone := make(chan error, 1)
		go func() { runDone <- runner.Run(runCtx) }()
		<-openEntered
		cancelRun()
		select {
		case runErr := <-runDone:
			if runErr == nil {
				t.Fatal("Run() = nil, want the cancelled consumer open error")
			}
		case <-clock.NewReal().Timer(2 * time.Second).C:
			t.Fatal("runner did not stop after the caller cancelled")
		}
		if err := client.Health(context.Background()); err != nil {
			t.Fatalf("Health after the caller cancelled = %v, want nil", err)
		}
		assertNoRecordedFailure(t, client, "orders")
	})

	t.Run("client close", func(t *testing.T) {
		openEntered := make(chan struct{})
		d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
		d.consumerOpenHook = func(ctx context.Context, _ string) error {
			close(openEntered)
			<-ctx.Done()
			return context.Canceled
		}
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
		runDone := make(chan error, 1)
		go func() { runDone <- runner.Run(context.Background()) }()
		<-openEntered
		if err := client.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		select {
		case <-runDone:
		case <-clock.NewReal().Timer(2 * time.Second).C:
			t.Fatal("runner did not stop after Close")
		}
		if err := client.Health(context.Background()); err == nil || !strings.Contains(err.Error(), "closed") {
			t.Fatalf("Health after Close = %v, want the closed-client error", err)
		}
		assertNoRecordedFailure(t, client, "orders")
	})

	t.Run("second run refused", func(t *testing.T) {
		d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
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
		runCtx, cancelRun := context.WithCancel(context.Background())
		t.Cleanup(cancelRun)
		runDone := make(chan error, 1)
		go func() { runDone <- runner.Run(runCtx) }()
		<-d.created
		waitReconnectCondition(t, func() bool { return runner.lifecycle.State() == lifecycle.Ready })
		if err := runner.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "already running") {
			t.Fatalf("second Run() = %v, want the already-running refusal", err)
		}
		if err := client.Health(context.Background()); err != nil {
			t.Fatalf("Health after a refused second Run = %v, want nil", err)
		}
		assertNoRecordedFailure(t, client, "orders")
		select {
		case err := <-runDone:
			t.Fatalf("first Run returned after a refused second Run: %v", err)
		default:
		}
	})
}

// assertNoRecordedFailure fails when the client holds a stopped-subscription
// record for name. Health cannot observe the record once the client is closed,
// so the test reads the record itself.
func assertNoRecordedFailure(t *testing.T, client *Client, name string) {
	t.Helper()
	client.mu.Lock()
	defer client.mu.Unlock()
	for _, failed := range client.failedSubscriptions {
		if failed.name == name {
			t.Fatalf("subscription %s is recorded as failed: %v", name, failed.err)
		}
	}
}

// subscribeOrders subscribes a runner named orders with a no-op handler, so a
// test can hold two runners under one subscription name.
func subscribeOrders(t *testing.T, client *Client) *Runner {
	t.Helper()
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
	return runner
}

// TestLateExitDoesNotRestoreAFailureANewerRunnerCleared holds a failed
// runner's consumer teardown open while a second runner of the same
// subscription name starts. The failed runner's exit must not mark that live
// name failed again, and must not overwrite the failure the newer runner
// records.
func TestLateExitDoesNotRestoreAFailureANewerRunnerCleared(t *testing.T) {
	t.Run("cleared by a newer ready runner", func(t *testing.T) {
		d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
		client := newReconnectTestClient(t, d, nil, 0)
		oldRunner := subscribeOrders(t, client)
		oldCtx, cancelOld := context.WithCancel(context.Background())
		t.Cleanup(cancelOld)
		oldDone := make(chan error, 1)
		go func() { oldDone <- oldRunner.Run(oldCtx) }()
		oldConsumer := <-d.created

		entered, release := oldConsumer.holdStop()
		t.Cleanup(release)
		waitReconnectCondition(t, func() bool { return oldRunner.lifecycle.State() == lifecycle.Ready })

		fatal := &driver.Error{Driver: d.Name(), Op: "consumer", K: driver.KindFatal, Err: errors.New("permission denied")}
		oldConsumer.sendError(fatal)
		waitReconnectCondition(t, func() bool {
			return errors.Is(client.Health(context.Background()), fatal)
		})
		select {
		case <-entered:
		case <-clock.NewReal().Timer(2 * time.Second).C:
			t.Fatal("the failed runner did not reach its consumer teardown")
		}
		select {
		case err := <-oldDone:
			t.Fatalf("the failed runner returned while its teardown was held: %v", err)
		default:
		}

		newRunner := subscribeOrders(t, client)
		newCtx, cancelNew := context.WithCancel(context.Background())
		t.Cleanup(cancelNew)
		newDone := make(chan error, 1)
		go func() { newDone <- newRunner.Run(newCtx) }()
		<-d.created
		waitReconnectCondition(t, func() bool { return newRunner.lifecycle.State() == lifecycle.Ready })
		select {
		case err := <-oldDone:
			t.Fatalf("the failed runner returned before its teardown was released: %v", err)
		default:
		}
		if err := client.Health(context.Background()); err != nil {
			t.Fatalf("Health after a newer runner of the same name reached ready = %v, want nil", err)
		}

		release()
		select {
		case err := <-oldDone:
			if !errors.Is(err, fatal) {
				t.Fatalf("the superseded runner Run() = %v, want %v", err, fatal)
			}
		case <-clock.NewReal().Timer(2 * time.Second).C:
			t.Fatal("the superseded runner did not return after its teardown was released")
		}
		if err := client.Health(context.Background()); err != nil {
			t.Fatalf("Health after the superseded runner exited = %v, want nil", err)
		}
	})

	t.Run("replaced by a newer runner's failure", func(t *testing.T) {
		d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
		client := newReconnectTestClient(t, d, nil, 0)
		oldRunner := subscribeOrders(t, client)
		oldCtx, cancelOld := context.WithCancel(context.Background())
		t.Cleanup(cancelOld)
		oldDone := make(chan error, 1)
		go func() { oldDone <- oldRunner.Run(oldCtx) }()
		oldConsumer := <-d.created

		entered, release := oldConsumer.holdStop()
		t.Cleanup(release)
		waitReconnectCondition(t, func() bool { return oldRunner.lifecycle.State() == lifecycle.Ready })

		fatal := &driver.Error{Driver: d.Name(), Op: "consumer", K: driver.KindFatal, Err: errors.New("permission denied")}
		oldConsumer.sendError(fatal)
		waitReconnectCondition(t, func() bool {
			return errors.Is(client.Health(context.Background()), fatal)
		})
		select {
		case <-entered:
		case <-clock.NewReal().Timer(2 * time.Second).C:
			t.Fatal("the failed runner did not reach its consumer teardown")
		}
		select {
		case err := <-oldDone:
			t.Fatalf("the failed runner returned while its teardown was held: %v", err)
		default:
		}

		newRunner := subscribeOrders(t, client)
		newCtx, cancelNew := context.WithCancel(context.Background())
		t.Cleanup(cancelNew)
		newDone := make(chan error, 1)
		go func() { newDone <- newRunner.Run(newCtx) }()
		newConsumer := <-d.created
		waitReconnectCondition(t, func() bool { return newRunner.lifecycle.State() == lifecycle.Ready })
		select {
		case err := <-oldDone:
			t.Fatalf("the failed runner returned before its teardown was released: %v", err)
		default:
		}
		if err := client.Health(context.Background()); err != nil {
			t.Fatalf("Health after a newer runner of the same name reached ready = %v, want nil", err)
		}

		newFatal := &driver.Error{Driver: d.Name(), Op: "consumer", K: driver.KindFatal, Err: errors.New("quota exceeded")}
		newConsumer.sendError(newFatal)
		select {
		case err := <-newDone:
			if !errors.Is(err, newFatal) {
				t.Fatalf("the newer runner Run() = %v, want %v", err, newFatal)
			}
		case <-clock.NewReal().Timer(2 * time.Second).C:
			t.Fatal("the newer runner did not stop after a fatal consumer error")
		}
		if healthErr := client.Health(context.Background()); !errors.Is(healthErr, newFatal) {
			t.Fatalf("Health after the newer runner failed = %v, want %v", healthErr, newFatal)
		}

		release()
		select {
		case err := <-oldDone:
			if !errors.Is(err, fatal) {
				t.Fatalf("the superseded runner Run() = %v, want %v", err, fatal)
			}
		case <-clock.NewReal().Timer(2 * time.Second).C:
			t.Fatal("the superseded runner did not return after its teardown was released")
		}
		healthErr := client.Health(context.Background())
		if !errors.Is(healthErr, newFatal) {
			t.Fatalf("Health after the superseded runner exited = %v, want the newer failure %v", healthErr, newFatal)
		}
		if errors.Is(healthErr, fatal) {
			t.Fatalf("Health = %v, want no trace of the superseded runner's %v", healthErr, fatal)
		}
	})
}

// --- The reconnect protocol's behaviour ---
//
// The tests below pin the protocol from the outside: a subscription, the fake
// driver's consumer lifecycle, and the client's health. None of them reads an
// ownership pointer or installs a decision hook, so the same six tests describe
// the behaviour before and after the connection epoch replaces those pointers.

// namedRunner subscribes a runner under name whose handler counts deliveries,
// so a test can hold two runners under one client and see which of them is
// serving.
func namedRunner(t *testing.T, client *Client, name string, handled *atomic.Int32) *Runner {
	t.Helper()
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           name,
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
	return runner
}

// runnerState reads a runner's lifecycle state under the runner's own lock.
func runnerState(runner *Runner) lifecycle.State {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if runner.lifecycle == nil {
		return lifecycle.Starting
	}
	return runner.lifecycle.State()
}

// consumerTransient builds the transient consumer error a test feeds a runner
// to end its generation.
func consumerTransient(d *reconnectTestDriver, message string) *driver.Error {
	return &driver.Error{Driver: d.Name(), Op: "consumer", K: driver.KindTransient, Err: errors.New(message)}
}

// startRunners runs every runner on its own context and collects the consumer
// each one opens first. Every context is cancelled by the test's cleanup.
func startRunners(t *testing.T, d *reconnectTestDriver, runners map[string]*Runner) (map[string]*reconnectTestConsumer, map[string]chan error) {
	t.Helper()
	done := make(map[string]chan error, len(runners))
	for name, runner := range runners {
		runCtx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		done[name] = make(chan error, 1)
		go func(runner *Runner, runCtx context.Context, done chan error) { done <- runner.Run(runCtx) }(runner, runCtx, done[name])
	}
	consumers := make(map[string]*reconnectTestConsumer, len(runners))
	timer := clock.NewReal().Timer(2 * time.Second)
	defer timer.Stop()
	for len(consumers) < len(runners) {
		select {
		case consumer := <-d.created:
			consumers[consumer.group] = consumer
		case <-timer.C:
			t.Fatalf("timed out waiting for %d initial consumers, got %d", len(runners), len(consumers))
		}
	}
	return consumers, done
}

// TestReconnectSurvivesAFailedReplacement pins a rebuild whose first
// replacement connection fails: the runner waits for the second attempt, ends
// ready on the replacement connection with nothing recorded against its
// subscription, and the rebuild reports the runner's own cause rather than the
// failed connection's error.
func TestReconnectSurvivesAFailedReplacement(t *testing.T) {
	fake := clock.NewFake(time.Unix(810, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 4)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	var output logSink
	client := newReconnectTestClientWithLogger(t, d, recorded, 0, slog.New(slog.NewTextHandler(&output, nil)))
	client.reconnectRandom = func() float64 { return 0 }

	var handled atomic.Int32
	runner := namedRunner(t, client, "orders", &handled)
	runCtx, cancelRun := context.WithCancel(context.Background())
	t.Cleanup(cancelRun)
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(runCtx) }()
	first := <-d.created
	waitReconnectCondition(t, func() bool { return runnerState(runner) == lifecycle.Ready })

	// The runner's own cause is the consumer its repair cannot open. The
	// rebuild that follows is the only one the supervisor starts, and the first
	// replacement connection fails too, so the second one is where the runner
	// has to end up.
	d.setFailConsumersWithError(1, errors.New("consumer unavailable"))
	d.setFailOpens(1)
	first.sendError(consumerTransient(d, "consumer disconnected"))

	advanceReconnect(t, recorded, 0, 1)
	advanceReconnect(t, recorded, 0, 2)
	select {
	case err := <-runDone:
		t.Fatalf("Run() = %v, want the runner to survive the failed replacement", err)
	default:
	}
	var replacement *reconnectTestConsumer
	select {
	case replacement = <-d.created:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("the runner did not open a consumer on the replacement connection")
	}
	waitReconnectCondition(t, func() bool {
		return runnerState(runner) == lifecycle.Ready && !client.isReconnecting()
	})
	if got := d.OpenCount(); got != 3 {
		t.Fatalf("driver Open count = %d, want the initial connection and two replacement attempts; sleeps = %d", got, recorded.sleepCount())
	}
	if got := strings.Count(output.String(), "f1 reconnect started"); got != 1 {
		t.Fatalf("reconnect start log lines = %d, want 1; output = %q", got, output.String())
	}
	if !strings.Contains(output.String(), "consumer unavailable") {
		t.Fatalf("reconnect start output = %q, want the runner's own cause", output.String())
	}
	if !replacement.send(validReconnectMessage(t, "after-a-failed-replacement")) {
		t.Fatal("the replacement consumer was already closed")
	}
	waitReconnectCondition(t, func() bool { return handled.Load() == 1 })
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health() after a failed replacement = %v, want nil", err)
	}
}

// TestDrainEndsAReconnectWaitBeforeTheConsumerOpens pins the drain side of a
// reconnect wait: a runner that is waiting for a rebuild its client has already
// started returns from Run when it is drained, and never opens a consumer on
// the connection that rebuild is replacing.
func TestDrainEndsAReconnectWaitBeforeTheConsumerOpens(t *testing.T) {
	fake := clock.NewFake(time.Unix(820, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 4)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 0 }

	var causingHandled, waitingHandled atomic.Int32
	causing := namedRunner(t, client, "causing", &causingHandled)
	waiting := namedRunner(t, client, "waiting", &waitingHandled)
	causingCtx, cancelCausing := context.WithCancel(context.Background())
	t.Cleanup(cancelCausing)
	causingDone := make(chan error, 1)
	go func() { causingDone <- causing.Run(causingCtx) }()
	causingConsumer := <-d.created
	waitReconnectCondition(t, func() bool { return runnerState(causing) == lifecycle.Ready })

	// The runner that asked cannot open its repaired consumer, so its own
	// transient error becomes a rebuild. The supervisor abandons every runner it
	// holds before it parks in its backoff, so the runner that has not started
	// yet is abandoned before Run is called on it and has to wait for the
	// rebuild before it opens anything.
	d.setFailConsumers(1)
	causingConsumer.sendError(consumerTransient(d, "causing consumer disconnected"))
	waitReconnectCondition(t, func() bool {
		return client.isReconnecting() && recorded.sleepCount() == 1
	})

	runDone := make(chan error, 1)
	go func() { runDone <- waiting.Run(context.Background()) }()
	waitReconnectCondition(t, func() bool {
		waiting.mu.Lock()
		defer waiting.mu.Unlock()
		return waiting.started
	})
	drainDone := make(chan error, 1)
	go func() { drainDone <- waiting.Drain(context.Background()) }()
	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("Drain() = %v, want the wait to end without an error", err)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("Drain did not end the reconnect wait")
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() = %v, want nil after Drain", err)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("Run did not return after Drain")
	}
	select {
	case consumer := <-d.created:
		t.Fatalf("the drained runner opened consumer %p for %s", consumer, consumer.group)
	default:
	}
	cancelCausing()
	select {
	case <-causingDone:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("the causing runner did not stop after its context was cancelled")
	}
}

// TestReconnectRebuildsEveryAbandonedRunner pins the abandon step itself: the
// runner that asked and the sibling nobody asked about both end ready on the
// replacement connection, and the connection the rebuild replaced is retired
// with no consumer left on it.
func TestReconnectRebuildsEveryAbandonedRunner(t *testing.T) {
	fake := clock.NewFake(time.Unix(830, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 4)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 0 }

	// The sibling's replacement open is held, so the test can tell when it is
	// about to serve again. The gate counts opens through the hook, and the
	// first one for this group is the initial consumer.
	victimOpenStarted := make(chan struct{})
	victimOpenRelease := make(chan struct{})
	var releaseVictimOpen sync.Once
	releaseOpen := func() { releaseVictimOpen.Do(func() { close(victimOpenRelease) }) }
	t.Cleanup(releaseOpen)
	d.mu.Lock()
	d.consumerOpenHook = secondConsumerOpenGate("victim", victimOpenStarted, victimOpenRelease, false, nil)
	d.mu.Unlock()

	var causingHandled, victimHandled atomic.Int32
	causing := namedRunner(t, client, "causing", &causingHandled)
	victim := namedRunner(t, client, "victim", &victimHandled)
	consumers, done := startRunners(t, d, map[string]*Runner{"causing": causing, "victim": victim})
	waitReconnectCondition(t, func() bool {
		return runnerState(causing) == lifecycle.Ready && runnerState(victim) == lifecycle.Ready
	})
	d.mu.Lock()
	retiring := d.connections[0]
	d.mu.Unlock()

	d.setFailConsumers(1)
	consumers["causing"].sendError(consumerTransient(d, "causing consumer disconnected"))
	waitReconnectCondition(t, func() bool { return recorded.sleepCount() == 1 })
	advanceReconnect(t, recorded, 0, 1)
	select {
	case <-victimOpenStarted:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("the abandoned sibling did not start its replacement consumer")
	}
	if got := d.OpenCount(); got != 2 {
		t.Fatalf("driver Open count = %d, want the initial connection and one replacement", got)
	}
	if got := retiring.openAtClose.Load(); got != 0 {
		t.Fatalf("consumers open when the replaced connection closed = %d, want 0", got)
	}
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health() after the rebuild = %v, want nil", err)
	}
	releaseOpen()

	live := make(map[string]*reconnectTestConsumer, 2)
	liveTimer := clock.NewReal().Timer(2 * time.Second)
	defer liveTimer.Stop()
	for len(live) < 2 {
		select {
		case consumer := <-d.created:
			live[consumer.group] = consumer
		case <-liveTimer.C:
			t.Fatalf("timed out waiting for both replacement consumers, got %d", len(live))
		}
	}
	d.mu.Lock()
	replacement := d.connections[1]
	d.mu.Unlock()
	for name, consumer := range live {
		if consumer == consumers[name] {
			t.Fatalf("the runner for %s reused its abandoned consumer", name)
		}
		if consumer.conn != replacement {
			t.Fatalf("consumer for %s opened on %p, want the replacement connection %p", name, consumer.conn, replacement)
		}
	}
	waitReconnectCondition(t, func() bool {
		return runnerState(causing) == lifecycle.Ready && runnerState(victim) == lifecycle.Ready && !client.isReconnecting()
	})
	if !live["victim"].send(validReconnectMessage(t, "victim-after-the-rebuild")) {
		t.Fatal("the replacement consumer was already closed")
	}
	waitReconnectCondition(t, func() bool { return victimHandled.Load() == 1 })
	for name, channel := range done {
		select {
		case err := <-channel:
			t.Fatalf("Run() for %s returned after the rebuild: %v", name, err)
		default:
		}
	}
}

// TestSupervisorExitStopsEveryWaitingRunner pins the wake the supervisor owes
// its waiters on the way out: when it stops mid-attempt, every runner waiting
// for that attempt returns instead of waiting for a rebuild that is no longer
// running.
func TestSupervisorExitStopsEveryWaitingRunner(t *testing.T) {
	fake := clock.NewFake(time.Unix(840, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 4)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 0 }

	var causingHandled, victimHandled atomic.Int32
	causing := namedRunner(t, client, "causing", &causingHandled)
	victim := namedRunner(t, client, "victim", &victimHandled)
	consumers, done := startRunners(t, d, map[string]*Runner{"causing": causing, "victim": victim})
	waitReconnectCondition(t, func() bool {
		return runnerState(causing) == lifecycle.Ready && runnerState(victim) == lifecycle.Ready
	})

	d.setFailConsumers(1)
	consumers["causing"].sendError(consumerTransient(d, "causing consumer disconnected"))
	waitReconnectCondition(t, func() bool {
		return client.isReconnecting() && recorded.sleepCount() == 1
	})

	client.supervisorCancel()
	for name, channel := range done {
		select {
		case <-channel:
		case <-clock.NewReal().Timer(2 * time.Second).C:
			t.Fatalf("runner %s did not return after the supervisor exited", name)
		}
	}
	select {
	case <-client.supervisorDone:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("the reconnect supervisor did not exit")
	}
	if client.isReconnecting() {
		t.Fatal("the client still reports a reconnect after the supervisor exited")
	}
}

// TestFiniteReconnectBudgetStopsTheWaitingRunner pins the wake a failed attempt
// owes the runner waiting for it when the epoch does not move: the budget runs
// out, the runner stops with the retained exhaustion error instead of waiting
// for a connection that is not coming, and both the health surface and the
// recorded failure name it.
func TestFiniteReconnectBudgetStopsTheWaitingRunner(t *testing.T) {
	fake := clock.NewFake(time.Unix(850, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 4)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 1)
	client.reconnectRandom = func() float64 { return 0 }

	var handled atomic.Int32
	runner := namedRunner(t, client, "orders", &handled)
	runCtx, cancelRun := context.WithCancel(context.Background())
	t.Cleanup(cancelRun)
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(runCtx) }()
	first := <-d.created
	waitReconnectCondition(t, func() bool { return runnerState(runner) == lifecycle.Ready })

	// The runner's repair cannot open its consumer and the replacement
	// connection cannot be opened either, so the one attempt the budget allows
	// ends while the runner is waiting for it.
	d.setFailConsumers(1)
	d.setFailOpens(1)
	first.sendError(consumerTransient(d, "consumer disconnected"))
	advanceReconnect(t, recorded, 0, 1)

	var runErr error
	select {
	case runErr = <-runDone:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("the runner did not stop after the reconnect budget ran out")
	}
	if kind, classified := driver.Classify(runErr); !classified || kind != driver.KindFatal {
		t.Fatalf("Run() = %v, want the classified fatal exhaustion error", runErr)
	}
	client.mu.Lock()
	exhaustion := client.reconnectErr
	var recordedFailure error
	for _, failed := range client.failedSubscriptions {
		if failed.name == "orders" {
			recordedFailure = failed.err
		}
	}
	client.mu.Unlock()
	if exhaustion == nil || !errors.Is(runErr, exhaustion) {
		t.Fatalf("Run() = %v, want the retained exhaustion error %v", runErr, exhaustion)
	}
	if !errors.Is(recordedFailure, exhaustion) {
		t.Fatalf("recorded failure for orders = %v, want the exhaustion error %v", recordedFailure, exhaustion)
	}
	if healthErr := client.Health(context.Background()); !errors.Is(healthErr, exhaustion) {
		t.Fatalf("Health() = %v, want the exhaustion error %v", healthErr, exhaustion)
	}
	if got := runnerState(runner); got == lifecycle.Ready {
		t.Fatalf("runner lifecycle after exhaustion = %s, want it to have stopped", got)
	}
}

// TestRetiredConnectionClosesOnlyAfterTheAbandonedRunnerReleases pins the order
// the swap owes a connection that refuses to close while a consumer is still
// registered on it: the abandoned runner's release finishes first, so the
// connection reports closed instead of being left open. A swap that runs ahead
// of that release is caught before it happens, by the backoff the supervisor
// starts only after the release returns.
func TestRetiredConnectionClosesOnlyAfterTheAbandonedRunnerReleases(t *testing.T) {
	fake := clock.NewFake(time.Unix(860, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 4)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 0 }

	var causingHandled, victimHandled atomic.Int32
	causing := namedRunner(t, client, "causing", &causingHandled)
	victim := namedRunner(t, client, "victim", &victimHandled)
	consumers, _ := startRunners(t, d, map[string]*Runner{"causing": causing, "victim": victim})
	for i, prefetch := range d.prefetchesSnapshot() {
		if prefetch < 1 {
			t.Fatalf("consumer %d prefetch = %d, want positive", i, prefetch)
		}
	}
	waitReconnectCondition(t, func() bool {
		return runnerState(causing) == lifecycle.Ready && runnerState(victim) == lifecycle.Ready
	})
	d.mu.Lock()
	retiring := d.connections[0]
	d.mu.Unlock()
	retiring.refuseCloseWithConsumers.Store(true)

	entered, release := consumers["victim"].holdRelease()
	t.Cleanup(release)
	d.setFailConsumers(1)
	consumers["causing"].sendError(consumerTransient(d, "causing consumer disconnected"))
	select {
	case <-entered:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("the supervisor did not reach the abandoned runner's release")
	}
	if got := recorded.sleepCount(); got != 0 {
		t.Fatalf("reconnect backoff sleeps while an abandoned release is in flight = %d, want 0", got)
	}
	if got := retiring.openConsumers.Load(); got != 1 {
		t.Fatalf("consumers registered on the replaced connection = %d, want the held one", got)
	}
	release()

	advanceReconnect(t, recorded, 0, 1)
	waitReconnectCondition(t, func() bool { return !client.isReconnecting() })
	if !retiring.closed.Load() {
		t.Fatal("the replaced connection did not report closed after the reconnect")
	}
	if got := retiring.openAtClose.Load(); got != 0 {
		t.Fatalf("consumers open when the replaced connection closed = %d, want 0", got)
	}
	if got := retiring.openConsumers.Load(); got != 0 {
		t.Fatalf("consumers left on the replaced connection = %d, want 0", got)
	}
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health() after the reconnect = %v, want nil", err)
	}
}

// TestReconnectRetriesAFailedReleaseBeforeClosingTheReplacedConnection proves
// that a consumer the abandon could not release is released again before the
// swap closes the connection carrying it. A driver refuses to close a
// connection that still has a consumer on it, so without the retry the
// replaced connection is never retired.
func TestReconnectRetriesAFailedReleaseBeforeClosingTheReplacedConnection(t *testing.T) {
	fake := clock.NewFake(time.Unix(870, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 4)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 0 }

	var causingHandled, victimHandled atomic.Int32
	causing := namedRunner(t, client, "causing", &causingHandled)
	victim := namedRunner(t, client, "victim", &victimHandled)
	consumers, _ := startRunners(t, d, map[string]*Runner{"causing": causing, "victim": victim})
	waitReconnectCondition(t, func() bool {
		return runnerState(causing) == lifecycle.Ready && runnerState(victim) == lifecycle.Ready
	})
	d.mu.Lock()
	retiring := d.connections[0]
	d.mu.Unlock()
	retiring.refuseCloseWithConsumers.Store(true)
	consumers["victim"].setFailRelease(1, errors.New("release failed"))

	d.setFailConsumers(1)
	consumers["causing"].sendError(consumerTransient(d, "causing consumer disconnected"))
	advanceReconnect(t, recorded, 0, 1)
	waitReconnectCondition(t, func() bool { return !client.isReconnecting() })

	if !retiring.closed.Load() {
		t.Fatal("the replaced connection was not closed after the failed release was retried")
	}
	if got := retiring.openAtClose.Load(); got != 0 {
		t.Fatalf("consumers open when the replaced connection closed = %d, want 0", got)
	}
	if got := retiring.openConsumers.Load(); got != 0 {
		t.Fatalf("consumers left on the replaced connection = %d, want 0", got)
	}
}

// TestCallerCanceledPublishDoesNotRequestReconnect proves that a publish its
// own caller canceled says nothing about the connection: a shutdown that
// cancels its in-flight publishes must not start a reconnect. A publish that
// ran out of time still does, because a broker that stopped confirming is what
// runs a publish past its deadline.
func TestCallerCanceledPublishDoesNotRequestReconnect(t *testing.T) {
	fake := clock.NewFake(time.Unix(500, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 4)}
	client := newReconnectTestClient(t, d, recorded, 0)
	transient := &driver.Error{Driver: d.Name(), Op: "publish", K: driver.KindTransient, Err: context.Canceled}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	requestReconnectOnTransient(canceled, client, transient, 1)
	if client.isReconnecting() {
		t.Fatal("a publish its caller canceled requested a reconnect")
	}

	expired, cancelExpired := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancelExpired()
	requestReconnectOnTransient(expired, client, transient, 1)
	if !client.isReconnecting() {
		t.Fatal("a publish that ran past its deadline did not request a reconnect")
	}
}

// TestCallerCanceledPublishReportsATransportFailure proves that a publish its
// caller gave up on can still be the call that noticed a broken connection:
// only an error reporting the caller's own cancellation is about the caller,
// so only that one suppresses the reconnect.
func TestCallerCanceledPublishReportsATransportFailure(t *testing.T) {
	fake := clock.NewFake(time.Unix(505, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 4)}
	client := newReconnectTestClient(t, d, recorded, 0)
	transport := &driver.Error{Driver: d.Name(), Op: "publish", K: driver.KindTransient, Err: errors.New("connection reset by peer")}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	requestReconnectOnTransient(canceled, client, transport, 1)
	if !client.isReconnecting() {
		t.Fatal("a publish that failed on the connection did not request a reconnect because its caller canceled")
	}
}

// TestReconnectReplaysSubscriptionTopologyOnTheReplacementConnection proves
// that the replacement connection learns every subscription's delayed
// destinations before it is installed. A handler still running when the swap
// lands publishes its retry successor on the new connection before its runner
// reopens, and a driver routes a delayed destination only from what an
// EnsureTopology call on that connection told it.
func TestReconnectReplaysSubscriptionTopologyOnTheReplacementConnection(t *testing.T) {
	fake := clock.NewFake(time.Unix(510, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 4)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 4)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 0 }

	// The runner is never run, so nothing but the reconnect can ensure its
	// topology on the replacement connection.
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Prefetch:       12,
		HandlerTimeout: 10 * time.Millisecond,
		Retry:          RetryConfig{MaxAttempts: 2, InitialInterval: 250 * time.Millisecond},
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error { return nil }),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	want := subscriptionTopologySpecs(client.effective, client.source, runner.subscription)
	client.mu.Unlock()
	var delayed []string
	for _, destination := range want.Destinations {
		if destination.Delay > 0 {
			delayed = append(delayed, destination.Name)
		}
	}
	if len(delayed) == 0 {
		t.Fatal("the subscription declares no delayed destination to route retries through")
	}

	if err := client.requestReconnect(errors.New("transient"), 0); err != nil {
		t.Fatalf("requestReconnect = %v, want nil", err)
	}
	select {
	case token := <-recorded.sleepStarted:
		<-token
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("reconnect sleep did not start")
	}
	fake.Advance(0)
	waitReconnectCondition(t, func() bool { return !client.isReconnecting() })

	d.mu.Lock()
	connections := append([]*reconnectTestConn(nil), d.connections...)
	d.mu.Unlock()
	if len(connections) != 2 {
		t.Fatalf("driver connections after the reconnect = %d, want 2", len(connections))
	}
	ensured := connections[1].admin.ensuredDestinations()
	for _, name := range delayed {
		if _, ok := ensured[name]; !ok {
			t.Fatalf("replacement connection was installed without the delayed destination %q", name)
		}
	}
}

// TestReconnectReplaysSubscriptionTopologiesConcurrently proves that one
// reconnect replays several subscriptions' topologies at once, bounded by
// topologyReplayConcurrency, instead of waiting for each destination family
// before it starts the next. The gate holds every replay, so a serial replay
// never reaches the second entry at all.
func TestReconnectReplaysSubscriptionTopologiesConcurrently(t *testing.T) {
	const subscriptions = 6
	fake := clock.NewFake(time.Unix(530, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 4)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 4)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 0 }

	gate := make(chan struct{})
	entered := make(chan struct{}, subscriptions)
	var concurrent, peak atomic.Int32
	d.refuseTopology = func(driver.TopologySpec) error {
		current := concurrent.Add(1)
		for {
			observed := peak.Load()
			if current <= observed || peak.CompareAndSwap(observed, current) {
				break
			}
		}
		entered <- struct{}{}
		<-gate
		concurrent.Add(-1)
		return nil
	}

	names := []string{"orders-a", "orders-b", "orders-c", "orders-d", "orders-e", "orders-f"}
	for _, name := range names {
		if _, err := client.Subscribe(context.Background(), Subscription{
			Name:           name,
			Topics:         []string{"orders.created"},
			HandlerTimeout: 10 * time.Millisecond,
			Handlers: map[string]Handler{
				"orders.created": HandlerFunc(func(context.Context, *Event) error { return nil }),
			},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.requestReconnect(errors.New("connection lost"), 0); err != nil {
		t.Fatal(err)
	}
	advanceReconnect(t, recorded, 0, 1)
	for range topologyReplayConcurrency {
		select {
		case <-entered:
		case <-clock.NewReal().Timer(2 * time.Second).C:
			t.Fatalf("the replay did not run %d subscription topologies at once", topologyReplayConcurrency)
		}
	}
	close(gate)
	waitReconnectCondition(t, func() bool { return !client.isReconnecting() })

	if got := peak.Load(); got != topologyReplayConcurrency {
		t.Fatalf("topology replay concurrency = %d, want %d", got, topologyReplayConcurrency)
	}
	d.mu.Lock()
	connections := append([]*reconnectTestConn(nil), d.connections...)
	d.mu.Unlock()
	if len(connections) != 2 {
		t.Fatalf("driver connections after the reconnect = %d, want 2", len(connections))
	}
	connections[1].admin.mu.Lock()
	replays := len(connections[1].admin.specs)
	connections[1].admin.mu.Unlock()
	if replays != subscriptions {
		t.Fatalf("replayed topologies on the replacement connection = %d, want %d", replays, subscriptions)
	}
}

// TestReconnectInstallsTheConnectionWhenOneSubscriptionTopologyIsRefused proves
// that a broker refusing one subscription's topology on the replacement
// connection does not keep the client on the old one: the connection is
// installed, the other subscription's topology is on it, and the refused
// subscription is left for its own runner to report when it reopens.
func TestReconnectInstallsTheConnectionWhenOneSubscriptionTopologyIsRefused(t *testing.T) {
	fake := clock.NewFake(time.Unix(520, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 4)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 4)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 0 }

	subscribe := func(name, topic string) *Runner {
		t.Helper()
		runner, err := client.Subscribe(context.Background(), Subscription{
			Name:           name,
			Topics:         []string{topic},
			HandlerTimeout: 10 * time.Millisecond,
			Retry:          RetryConfig{MaxAttempts: 2, InitialInterval: 250 * time.Millisecond},
			Handlers: map[string]Handler{
				topic: HandlerFunc(func(context.Context, *Event) error { return nil }),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return runner
	}
	orders := subscribe("orders", "orders.created")
	subscribe("billing", "billing.charged")

	refused := &driver.Error{Driver: d.Name(), Op: "ensure_topology", K: driver.KindFatal, Err: errors.New("precondition failed")}
	d.mu.Lock()
	d.refuseTopology = func(spec driver.TopologySpec) error {
		for _, destination := range spec.Destinations {
			if strings.Contains(destination.Name, "billing") {
				return refused
			}
		}
		return nil
	}
	d.mu.Unlock()

	if err := client.requestReconnect(errors.New("transient"), 0); err != nil {
		t.Fatalf("requestReconnect = %v, want nil", err)
	}
	select {
	case token := <-recorded.sleepStarted:
		<-token
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("reconnect sleep did not start")
	}
	fake.Advance(0)
	waitReconnectCondition(t, func() bool { return !client.isReconnecting() })

	d.mu.Lock()
	connections := append([]*reconnectTestConn(nil), d.connections...)
	d.mu.Unlock()
	if len(connections) != 2 {
		t.Fatalf("driver connections after the reconnect = %d, want 2", len(connections))
	}
	client.mu.Lock()
	installed := client.current.conn == driver.Conn(connections[1])
	epoch := client.current.epoch
	want := subscriptionTopologySpecs(client.effective, client.source, orders.subscription)
	client.mu.Unlock()
	if !installed || epoch != 2 {
		t.Fatalf("installed replacement = %v at epoch %d, want the replacement at epoch 2", installed, epoch)
	}
	if connections[1].closed.Load() {
		t.Fatal("the replacement connection was closed after one subscription's topology was refused")
	}
	ensured := connections[1].admin.ensuredDestinations()
	for _, destination := range want.Destinations {
		if _, ok := ensured[destination.Name]; !ok {
			t.Fatalf("replacement connection is missing the orders destination %q", destination.Name)
		}
	}
}

// TestConsumerOpenCapsOnlyTheDestinationsItDeclares pins that the open sizes
// each destination's cap from the capabilities it read with the connection. A
// capability change that lands during the open's topology call must not key
// the caps on destinations the consumer was never given.
func TestConsumerOpenCapsOnlyTheDestinationsItDeclares(t *testing.T) {
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 4)}
	client := newReconnectTestClient(t, d, clock.NewReal(), 0)
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		HandlerTimeout: 10 * time.Millisecond,
		Retry:          RetryConfig{MaxAttempts: 2, InitialInterval: 250 * time.Millisecond},
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error { return nil }),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	admin := d.connections[0].admin
	d.mu.Unlock()
	admin.refuse = func(driver.TopologySpec) error {
		client.mu.Lock()
		client.effective.Fanout = driver.FanoutAtPublish
		client.mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()
	openTimer := clock.NewReal().Timer(time.Second)
	defer openTimer.Stop()
	select {
	case <-d.created:
	case <-openTimer.C:
		t.Fatal("consumer did not open")
	}
	cancel()
	<-runDone

	d.mu.Lock()
	cfg := d.consumerConfigs[0]
	d.mu.Unlock()
	declared := make(map[string]bool, len(cfg.Destinations))
	for _, destination := range cfg.Destinations {
		declared[destination] = true
	}
	for destination := range cfg.PerDestination {
		if !declared[destination] {
			t.Fatalf("cap keyed on %q, which is not among the declared destinations %v", destination, cfg.Destinations)
		}
	}
}

// TestSchedulerLanesUseTheDestinationsTheConsumerDeclares pins that the
// scheduler and the deadline-promotion table are built from the plan the
// consumer open established. A capability change that lands during the open's
// topology call must not hand the pipeline a lane for a destination that
// consumer was never given.
func TestSchedulerLanesUseTheDestinationsTheConsumerDeclares(t *testing.T) {
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 4)}
	client := newReconnectTestClient(t, d, clock.NewReal(), 0)
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Priorities:     []Priority{PriorityHigh, PriorityLow},
		HandlerTimeout: 10 * time.Millisecond,
		Retry:          RetryConfig{MaxAttempts: 2, InitialInterval: 250 * time.Millisecond},
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error { return nil }),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	admin := d.connections[0].admin
	d.mu.Unlock()
	admin.refuse = func(driver.TopologySpec) error {
		client.mu.Lock()
		client.effective.Fanout = driver.FanoutAtPublish
		client.mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()
	openTimer := clock.NewReal().Timer(time.Second)
	defer openTimer.Stop()
	select {
	case <-d.created:
	case <-openTimer.C:
		t.Fatal("consumer did not open")
	}

	d.mu.Lock()
	cfg := d.consumerConfigs[0]
	d.mu.Unlock()
	declared := make(map[string]bool, len(cfg.Destinations))
	for _, destination := range cfg.Destinations {
		declared[destination] = true
	}
	plan := runnerLanePlan(runner)
	if len(plan) == 0 {
		t.Fatal("the runner has no lane plan after the open")
	}
	scheduler, err := newRunnerScheduler(runner)
	if err != nil {
		t.Fatal(err)
	}
	for _, lane := range plan {
		if !declared[lane.destination] {
			t.Fatalf("lane %s feeds destination %q, which is not among the declared destinations %v", lane.id, lane.destination, cfg.Destinations)
		}
		if err := scheduler.Enqueue(lane.id, sched.Item{}); err != nil {
			t.Fatalf("scheduler does not carry lane %s: %v", lane.id, err)
		}
	}
	for id, lane := range newPromotionLimiter(runner).lanes {
		if !declared[lane.destination] {
			t.Fatalf("promotion lane %s feeds destination %q, which is not among the declared destinations %v", id, lane.destination, cfg.Destinations)
		}
	}
	cancel()
	<-runDone
}

// TestPublishStartedDuringReconnectIsRefusedAtEntry pins that a publish that
// begins while a reconnect waits for in-flight publishes is refused before it
// is counted. Counted, it would hold the wait open, and callers retrying in a
// loop could keep the reconnect from ever replacing the connection.
func TestPublishStartedDuringReconnectIsRefusedAtEntry(t *testing.T) {
	codecFixture := blockingCodec{
		encodeStarted:     make(chan struct{}),
		encodeStartedOnce: make(chan struct{}),
		encodeRelease:     make(chan struct{}),
	}
	fake := clock.NewFake(time.Unix(160, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 0, WithCodec(codecFixture))
	client.reconnectRandom = func() float64 { return 1 }

	firstDone := make(chan error, 1)
	go func() {
		_, err := client.Publisher().Publish(context.Background(), "orders.created", map[string]string{"value": "first"})
		firstDone <- err
	}()
	encodeTimer := clock.NewReal().Timer(time.Second)
	defer encodeTimer.Stop()
	select {
	case <-codecFixture.encodeStarted:
	case <-encodeTimer.C:
		t.Fatal("first publish did not reach the codec")
	}
	if err := client.requestReconnect(errors.New("transient"), 0); err != nil {
		t.Fatal(err)
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := client.Publisher().Publish(context.Background(), "orders.created", map[string]string{"value": "second"})
		secondDone <- err
	}()
	timer := clock.NewReal().Timer(time.Second)
	defer timer.Stop()
	select {
	case err := <-secondDone:
		if !errors.Is(err, errClientReconnecting) {
			t.Fatalf("Publish() during reconnect = %v, want errClientReconnecting", err)
		}
	case <-timer.C:
		close(codecFixture.encodeRelease)
		t.Fatal("a publish started during the reconnect was admitted and held the in-flight wait")
	}

	close(codecFixture.encodeRelease)
	if err := <-firstDone; !errors.Is(err, errClientReconnecting) {
		t.Fatalf("first Publish() = %v, want errClientReconnecting", err)
	}
	waitReconnectCondition(t, func() bool { return recorded.sleepCount() == 1 })
}
