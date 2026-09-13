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
	mu               sync.Mutex
	opens            int
	failOpens        int
	failConsumers    int
	consumerErr      error
	consumerOpenHook func(context.Context, string) error
	connections      []*reconnectTestConn
	created          chan *reconnectTestConsumer
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
	sleepStarted chan chan struct{}
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
	cfg.Lifecycle.FlushTimeout = 100 * time.Millisecond
	cfg.Lifecycle.CloseTimeout = 100 * time.Millisecond
	options := []Option{WithDriver(d)}
	if logger != nil {
		options = append(options, WithLogger(logger))
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

func newReconnectSupervisorTestClient() (*Client, context.CancelFunc) {
	supervisorCtx, supervisorCancel := context.WithCancel(context.Background())
	return &Client{
		reconnectRequests: make(chan reconnectRequest, 1),
		supervisorCtx:     supervisorCtx,
		supervisorCancel:  supervisorCancel,
		supervisorDone:    make(chan struct{}),
	}, supervisorCancel
}

func registerReconnectAttempt(client *Client) *reconnectAttempt {
	attempt := &reconnectAttempt{done: make(chan struct{})}
	client.mu.Lock()
	client.reconnecting = true
	client.reconnect = attempt
	client.mu.Unlock()
	return attempt
}

func TestSupervisorExitReleasesAnInFlightAttempt(t *testing.T) {
	client, cancel := newReconnectSupervisorTestClient()
	attempt := registerReconnectAttempt(client)
	cancel()
	go client.reconnectSupervisor()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := client.waitReconnect(waitCtx, attempt); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitReconnect() after supervisor exit = %v, want context canceled", err)
	}
	select {
	case <-client.supervisorDone:
	case <-waitCtx.Done():
		t.Fatalf("reconnect supervisor did not exit: %v", waitCtx.Err())
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.reconnect != nil {
		t.Fatal("reconnect attempt remains registered after supervisor exit")
	}
	if client.reconnecting {
		t.Fatal("client remains reconnecting after supervisor exit")
	}
}

func TestDrainCompletesWhenTheSupervisorExitsMidReconnect(t *testing.T) {
	client, cancel := newReconnectSupervisorTestClient()
	attempt := registerReconnectAttempt(client)
	runner := &Runner{
		client:    client,
		started:   true,
		done:      make(chan struct{}),
		lifecycle: lifecycle.New(),
	}
	waitCtx, waitCancel := context.WithCancel(context.Background())
	defer waitCancel()
	go func() {
		_ = client.waitReconnect(waitCtx, attempt)
		close(runner.done)
	}()

	cancel()
	go client.reconnectSupervisor()
	drainCtx, drainCancel := context.WithTimeout(context.Background(), time.Second)
	defer drainCancel()
	if err := runner.Drain(drainCtx); err != nil {
		t.Fatalf("Drain() after supervisor exit = %v, want nil", err)
	}
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
	shutdownStarted, closedState := client.shutdownStarted, client.closed
	client.mu.Unlock()
	if !shutdownStarted || closedState {
		t.Fatalf("shutdown state = shutdownStarted:%t closed:%t, want started and open", shutdownStarted, closedState)
	}

	attempt, err := client.requestReconnect(errors.New("during shutdown"))
	if attempt != nil {
		t.Fatal("request while shutting down returned a reconnect attempt")
	}
	if err == nil || !strings.Contains(err.Error(), "client is closing") {
		t.Fatalf("request while shutting down = %v, want client-closing error", err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.reconnect != nil {
		t.Fatal("request while shutting down registered a reconnect attempt")
	}
	if client.reconnecting {
		t.Fatal("client remains reconnecting after rejected shutdown request")
	}
}

func TestRequestReconnectCancellationStillFinishesItsAttempt(t *testing.T) {
	supervisorCtx, supervisorCancel := context.WithCancel(context.Background())
	defer supervisorCancel()
	client := &Client{supervisorCtx: supervisorCtx}
	requestResult := make(chan struct {
		attempt *reconnectAttempt
		err     error
	}, 1)
	go func() {
		attempt, err := client.requestReconnect(errors.New("canceled request"))
		requestResult <- struct {
			attempt *reconnectAttempt
			err     error
		}{attempt: attempt, err: err}
	}()

	var attempt *reconnectAttempt
	realClock := clock.NewReal()
	poll := realClock.Ticker(time.Millisecond)
	defer poll.Stop()
	deadline := realClock.Timer(time.Second)
	defer deadline.Stop()
	for attempt == nil {
		client.mu.Lock()
		attempt = client.reconnect
		client.mu.Unlock()
		if attempt != nil {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("requestReconnect() did not register an attempt")
		case <-poll.C:
		}
	}

	supervisorCancel()
	resultTimer := realClock.Timer(time.Second)
	defer resultTimer.Stop()
	select {
	case result := <-requestResult:
		if result.attempt != nil {
			t.Fatalf("canceled request returned attempt %p, want nil", result.attempt)
		}
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("canceled request = %v, want context canceled", result.err)
		}
	case <-resultTimer.C:
		t.Fatal("canceled request did not return")
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := client.waitReconnect(waitCtx, attempt); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request attempt = %v, want context canceled", err)
	}
}

func TestReconnectStartUsesConfiguredLogger(t *testing.T) {
	defaultOutput := captureProcessDefault(t)
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
	cause := errors.New("configured reconnect cause")
	attempt, err := client.requestReconnect(cause)
	if err != nil {
		t.Fatal(err)
	}
	advanceReconnect(t, recorded, 500*time.Millisecond, 1)
	if err := client.waitReconnect(context.Background(), attempt); err != nil {
		t.Fatalf("reconnect = %v, want nil", err)
	}
	assertReconnectStartedLog(t, configuredOutput.String(), cause)
	if got := defaultOutput.String(); got != "" {
		t.Fatalf("process default output = %q, want empty", got)
	}
}

func TestReconnectStartUsesProcessDefaultWithoutConfiguredLogger(t *testing.T) {
	defaultOutput := captureProcessDefault(t)
	fake := clock.NewFake(time.Unix(450, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 1)}
	client := newReconnectTestClientWithLogger(t, d, recorded, 0, nil)
	cause := errors.New("default reconnect cause")
	attempt, err := client.requestReconnect(cause)
	if err != nil {
		t.Fatal(err)
	}
	advanceReconnect(t, recorded, 500*time.Millisecond, 1)
	if err := client.waitReconnect(context.Background(), attempt); err != nil {
		t.Fatalf("reconnect = %v, want nil", err)
	}
	assertReconnectStartedLog(t, defaultOutput.String(), cause)
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
	waitReconnectCondition(t, func() bool { return runner.lifecycle.Ready() })
	d.setFailConsumersWithError(1, cause)
	first.sendError(&driver.Error{
		Driver: d.Name(),
		Op:     "consumer",
		K:      driver.KindTransient,
		Err:    errors.New("runner failure signal"),
	})
	advanceReconnect(t, recorded, 500*time.Millisecond, 1)
	waitReconnectCondition(t, func() bool {
		return d.OpenCount() >= 2 && runner.lifecycle.Ready() && !client.isReconnecting()
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
		firstResult <- (&Runner{client: client}).requestAndWaitReconnect(context.Background(), cause)
	}()
	waitReconnectCondition(t, func() bool { return recorded.sleepCount() == 1 })
	results := make(chan error, 8)
	for range 8 {
		attempt, err := client.requestReconnect(cause)
		if err != nil {
			t.Fatal(err)
		}
		go func(attempt *reconnectAttempt) {
			results <- client.waitReconnect(context.Background(), attempt)
		}(attempt)
	}
	advanceReconnect(t, recorded, 500*time.Millisecond, 1)
	select {
	case err := <-firstResult:
		if err != nil {
			t.Fatalf("first requestAndWaitReconnect() = %v, want nil", err)
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

func TestRunnerReturnsOwnedReconnectFailure(t *testing.T) {
	driver := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 1)}
	client := newReconnectTestClient(t, driver, nil, 0)
	attempt := &reconnectAttempt{done: make(chan struct{})}
	wantErr := errors.New("owned reconnect failed")
	client.mu.Lock()
	client.reconnecting = true
	client.reconnect = attempt
	client.mu.Unlock()
	client.finishReconnect(attempt, wantErr)

	runner := &Runner{
		client:                client,
		reconnectCause:        errClientReconnecting,
		reconnectCauseAttempt: attempt,
	}
	if err := runner.waitOwnedReconnect(context.Background(), attempt); !errors.Is(err, wantErr) {
		t.Fatalf("waitOwnedReconnect() = %v, want %v", err, wantErr)
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if runner.reconnectCause != errClientReconnecting || runner.reconnectCauseAttempt != attempt { //nolint:errorlint // exact sentinel is the ownership marker
		t.Fatalf("owned reconnect state = cause %v attempt %p, want cause %v attempt %p", runner.reconnectCause, runner.reconnectCauseAttempt, errClientReconnecting, attempt)
	}
}

func TestGenerationStartPreservesPendingReconnectAttempt(t *testing.T) {
	driver := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 1)}
	client := newReconnectTestClient(t, driver, nil, 0)
	runner := &Runner{client: client}
	first := &reconnectAttempt{done: make(chan struct{})}
	close(first.done)
	runner.mu.Lock()
	runner.reconnectCause = errClientReconnecting
	runner.reconnectCauseAttempt = first
	runner.mu.Unlock()
	if err := runner.waitOwnedReconnect(context.Background(), first); err != nil {
		t.Fatalf("waitOwnedReconnect() = %v, want nil", err)
	}

	second := &reconnectAttempt{done: make(chan struct{})}
	if err := runner.abandonForReconnect(context.Background(), second); err != nil {
		t.Fatalf("abandonForReconnect() = %v, want nil", err)
	}
	beginRunnerGeneration(runner, context.Background(), func() {})

	runner.mu.Lock()
	defer runner.mu.Unlock()
	if runner.reconnectCause != errClientReconnecting || runner.reconnectCauseAttempt != second { //nolint:errorlint // exact sentinel is the ownership marker
		t.Fatalf("pending reconnect state = cause %v attempt %p, want cause %v attempt %p", runner.reconnectCause, runner.reconnectCauseAttempt, errClientReconnecting, second)
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
	attempt, err := client.requestReconnect(errors.New("pre-run reconnect"))
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
	wantErr := client.waitReconnect(context.Background(), attempt)
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
	if healthErr := client.Health(context.Background()); healthErr != wantErr { //nolint:errorlint // exact attempt error identity is the health contract
		t.Fatalf("Health() = %v, want exact reconnect error %v", healthErr, wantErr)
	}
}

func TestRunConsumesSuccessfulPreRunReconnectOwner(t *testing.T) {
	fake := clock.NewFake(time.Unix(350, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 8)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 0 }
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

	attempt, err := client.requestReconnect(errors.New("pre-run reconnect"))
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
	if err := client.waitReconnect(context.Background(), attempt); err != nil {
		t.Fatalf("pre-run reconnect = %v, want nil", err)
	}

	decisionEntered := make(chan struct{})
	releaseDecision := make(chan struct{})
	var releaseDecisionOnce sync.Once
	releaseDecisionGate := func() {
		releaseDecisionOnce.Do(func() { close(releaseDecision) })
	}
	var decisionOnce sync.Once
	runner.reconnectDecisionHook = func() {
		decisionOnce.Do(func() {
			close(decisionEntered)
			<-releaseDecision
		})
	}
	t.Cleanup(releaseDecisionGate)
	runCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(runCtx) }()
	first := <-d.created
	waitReconnectCondition(t, func() bool { return runner.lifecycle.Ready() })
	runner.mu.Lock()
	preRunCause := runner.reconnectCause
	preRunAttempt := runner.reconnectCauseAttempt
	runner.mu.Unlock()
	if preRunCause != nil || preRunAttempt != nil {
		t.Fatalf("pre-run reconnect state = cause %v attempt %p, want nil state", preRunCause, preRunAttempt)
	}
	transientCause := &driver.Error{
		Driver: d.Name(),
		Op:     "consumer",
		K:      driver.KindTransient,
		Err:    errors.New("consumer disconnected"),
	}
	first.sendError(transientCause)
	select {
	case <-decisionEntered:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("runner did not reach transient-error decision")
	}
	runner.mu.Lock()
	gotCause := runner.reconnectCause
	gotAttempt := runner.reconnectCauseAttempt
	runner.mu.Unlock()
	if !errors.Is(gotCause, transientCause) {
		t.Fatalf("transient reconnect cause = %v, want %v", gotCause, transientCause)
	}
	if gotAttempt != nil {
		t.Fatalf("transient reconnect cause acquired attempt %p", gotAttempt)
	}
	releaseDecisionGate()
	second := <-d.created
	waitReconnectCondition(t, func() bool {
		return runner.lifecycle.Ready() && !client.isReconnecting()
	})
	if !second.send(validReconnectMessage(t, "after-pre-run-owner")) {
		t.Fatal("replacement consumer rejected a message")
	}
	waitReconnectCondition(t, func() bool { return handled.Load() == 1 })
	if got := recorded.sleepCount(); got != 1 {
		t.Fatalf("reconnect sleep count = %d, want 1", got)
	}
	if got := d.OpenCount(); got != 2 {
		t.Fatalf("driver Open count = %d, want 2", got)
	}
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health after pre-run owner cleanup = %v, want nil", err)
	}
	cancel()
	select {
	case <-runDone:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("runner did not stop after cancellation")
	}
}

func TestRunWaitsForNewerPreRunReconnectOwner(t *testing.T) {
	fake := clock.NewFake(time.Unix(350, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 8)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 0 }
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

	attemptB, err := client.requestReconnect(errors.New("completed pre-run reconnect"))
	if err != nil {
		t.Fatal(err)
	}
	waitReconnectCondition(t, func() bool { return recorded.sleepCount() == 1 })
	select {
	case token := <-recorded.sleepStarted:
		<-token
		fake.Advance(0)
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("first pre-run reconnect sleep did not start")
	}
	if err := client.waitReconnect(context.Background(), attemptB); err != nil {
		t.Fatalf("first pre-run reconnect = %v, want nil", err)
	}

	attemptA := &reconnectAttempt{done: make(chan struct{})}
	runner.mu.Lock()
	ownerB := runner.reconnectCauseAttempt
	runner.mu.Unlock()
	if ownerB != attemptB {
		t.Fatalf("completed pre-run owner = %p, want %p", ownerB, attemptB)
	}
	client.mu.Lock()
	client.reconnecting = true
	client.reconnect = attemptA
	client.mu.Unlock()

	waitEntered := make(chan struct{})
	observedOwner := make(chan *reconnectAttempt, 2)
	newerAttempt := &reconnectAttempt{done: make(chan struct{})}
	var enteredOnce sync.Once
	runner.reconnectDecisionHook = func() {
		runner.mu.Lock()
		owner := runner.reconnectCauseAttempt
		runner.mu.Unlock()
		select {
		case observedOwner <- owner:
		default:
		}
		enteredOnce.Do(func() {
			client.finishReconnect(attemptA, nil)
			client.mu.Lock()
			client.reconnecting = true
			client.reconnect = newerAttempt
			client.mu.Unlock()
			close(waitEntered)
		})
	}
	runCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(runCtx) }()
	select {
	case consumer := <-d.created:
		t.Fatalf("Run opened consumer %p before newer pre-run reconnect completed", consumer)
	case <-waitEntered:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("Run did not wait for the newer pre-run reconnect")
	}
	select {
	case owner := <-observedOwner:
		if owner != attemptA {
			t.Fatalf("startup adopted owner %p, want current owner %p", owner, attemptA)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("startup wait did not observe the current owner")
	}
	select {
	case owner := <-observedOwner:
		if owner != newerAttempt {
			t.Fatalf("startup recheck kept owner %p, want newer owner %p", owner, newerAttempt)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("startup wait did not observe the newer owner")
	}
	select {
	case consumer := <-d.created:
		t.Fatalf("Run opened consumer %p while newer pre-run reconnect was pending", consumer)
	default:
	}
	client.finishReconnect(newerAttempt, nil)
	if err := client.waitReconnect(context.Background(), newerAttempt); err != nil {
		t.Fatalf("newer pre-run reconnect = %v, want nil", err)
	}
	first := <-d.created
	waitReconnectCondition(t, func() bool { return runner.lifecycle.Ready() })
	if !first.send(validReconnectMessage(t, "after-newer-pre-run-owner")) {
		t.Fatal("consumer rejected the message after newer pre-run reconnect")
	}
	waitReconnectCondition(t, func() bool { return handled.Load() == 1 })
	if got := recorded.sleepCount(); got != 1 {
		t.Fatalf("reconnect sleep count = %d, want 1", got)
	}
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health after newer pre-run reconnect = %v, want nil", err)
	}
	select {
	case runErr := <-runDone:
		t.Fatalf("Run returned after newer pre-run reconnect: %v", runErr)
	default:
	}
	cancel()
	select {
	case <-runDone:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("runner did not stop after cancellation")
	}
}

func TestRunDrainStopsPreRunReconnectWait(t *testing.T) {
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
	attempt := &reconnectAttempt{done: make(chan struct{})}
	client.mu.Lock()
	client.reconnecting = true
	client.reconnect = attempt
	client.mu.Unlock()
	runner.mu.Lock()
	runner.reconnectCause = errClientReconnecting
	runner.reconnectCauseAttempt = attempt
	runner.mu.Unlock()

	waitEntered := make(chan struct{})
	var enteredOnce sync.Once
	runner.reconnectDecisionHook = func() {
		enteredOnce.Do(func() { close(waitEntered) })
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(context.Background()) }()
	select {
	case <-waitEntered:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("Run did not enter the pre-run reconnect wait")
	}

	drainDone := make(chan error, 1)
	go func() { drainDone <- runner.Drain(context.Background()) }()
	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("Drain() = %v, want nil", err)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("Drain did not finish the pre-run reconnect wait")
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() = %v after Drain, want nil", err)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("Run did not stop after Drain")
	}
	select {
	case consumer := <-d.created:
		t.Fatalf("Run opened consumer %p after Drain", consumer)
	default:
	}
}

func TestRunPreservesGenuinePreRunCause(t *testing.T) {
	fake := clock.NewFake(time.Unix(350, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 8)}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 0 }
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

	attempt, err := client.requestReconnect(errors.New("completed pre-run reconnect"))
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
	if err := client.waitReconnect(context.Background(), attempt); err != nil {
		t.Fatalf("pre-run reconnect = %v, want nil", err)
	}

	genuineCause := &driver.Error{
		Driver: d.Name(),
		Op:     "consumer",
		K:      driver.KindTransient,
		Err:    errors.New("genuine pre-run cause"),
	}
	runner.mu.Lock()
	runner.reconnectCause = genuineCause
	runner.mu.Unlock()
	observedCause := make(chan error, 1)
	runner.reconnectDecisionHook = func() {
		runner.mu.Lock()
		cause := runner.reconnectCause
		runner.mu.Unlock()
		select {
		case observedCause <- cause:
		default:
		}
	}

	runCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(runCtx) }()
	first := <-d.created
	waitReconnectCondition(t, func() bool { return runner.lifecycle.Ready() })
	first.sendError(&driver.Error{
		Driver: d.Name(),
		Op:     "consumer",
		K:      driver.KindTransient,
		Err:    errors.New("consumer disconnected"),
	})
	select {
	case gotCause := <-observedCause:
		if gotCause != genuineCause { //nolint:errorlint // exact genuine cause identity is the preservation contract
			t.Fatalf("decision cause = %v, want genuine pre-run cause %v", gotCause, genuineCause)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("runner did not expose the genuine pre-run cause")
	}
	second := <-d.created
	waitReconnectCondition(t, func() bool { return runner.lifecycle.Ready() })
	if !second.send(validReconnectMessage(t, "after-genuine-pre-run-cause")) {
		t.Fatal("replacement consumer rejected the message")
	}
	waitReconnectCondition(t, func() bool { return handled.Load() == 1 })
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health after genuine pre-run cause = %v, want nil", err)
	}
	select {
	case runErr := <-runDone:
		t.Fatalf("Run returned after genuine pre-run cause: %v", runErr)
	default:
	}
	cancel()
	select {
	case <-runDone:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("runner did not stop after cancellation")
	}
}

func TestRunAdoptsNewerOwnerAfterFailedPreRunReconnect(t *testing.T) {
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

	attemptA := &reconnectAttempt{done: make(chan struct{})}
	staleErr := errors.New("superseded pre-run reconnect failure")
	client.mu.Lock()
	client.reconnecting = true
	client.reconnect = attemptA
	client.mu.Unlock()
	runner.mu.Lock()
	runner.reconnectCause = errClientReconnecting
	runner.reconnectCauseAttempt = attemptA
	runner.mu.Unlock()

	attemptB := &reconnectAttempt{done: make(chan struct{})}
	waitEntered := make(chan struct{})
	observedOwner := make(chan *reconnectAttempt, 2)
	var transferOnce sync.Once
	runner.reconnectDecisionHook = func() {
		runner.mu.Lock()
		owner := runner.reconnectCauseAttempt
		runner.mu.Unlock()
		select {
		case observedOwner <- owner:
		default:
		}
		transferOnce.Do(func() {
			// The owned attempt closes with an error while the runner still owns
			// it, and ownership moves to a newer attempt before the waiter reads
			// that result.
			client.finishReconnect(attemptA, staleErr)
			client.mu.Lock()
			client.reconnecting = true
			client.reconnect = attemptB
			client.mu.Unlock()
			runner.mu.Lock()
			runner.reconnectCauseAttempt = attemptB
			runner.mu.Unlock()
			close(waitEntered)
		})
	}
	runCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(runCtx) }()
	select {
	case consumer := <-d.created:
		t.Fatalf("Run opened consumer %p while a newer owner was pending", consumer)
	case runErr := <-runDone:
		t.Fatalf("Run() = %v, want the newer owner awaited instead of the superseded failure", runErr)
	case <-waitEntered:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("Run did not wait for the pre-run reconnect")
	}
	for _, want := range []*reconnectAttempt{attemptA, attemptB} {
		select {
		case owner := <-observedOwner:
			if owner != want {
				t.Fatalf("startup owner = %p, want %p", owner, want)
			}
		case runErr := <-runDone:
			t.Fatalf("Run() = %v, want owner %p awaited", runErr, want)
		case <-clock.NewReal().Timer(2 * time.Second).C:
			t.Fatalf("startup wait did not observe owner %p", want)
		}
	}
	select {
	case consumer := <-d.created:
		t.Fatalf("Run opened consumer %p while the adopted owner was pending", consumer)
	default:
	}
	client.finishReconnect(attemptB, nil)
	first := <-d.created
	waitReconnectCondition(t, func() bool { return runner.lifecycle.Ready() })
	if !first.send(validReconnectMessage(t, "after-adopted-pre-run-owner")) {
		t.Fatal("consumer rejected the message after adopting the newer owner")
	}
	waitReconnectCondition(t, func() bool { return handled.Load() == 1 })
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health after adopting the newer owner = %v, want nil", err)
	}
	select {
	case runErr := <-runDone:
		t.Fatalf("Run returned after adopting the newer owner: %v", runErr)
	default:
	}
	cancel()
	select {
	case <-runDone:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("runner did not stop after cancellation")
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
	if err := runner.abandonForReconnect(context.Background(), nil); err != nil {
		t.Fatalf("abandonForReconnect(Failed) = %v, want nil", err)
	}
	runner.mu.Lock()
	reconnectCause := runner.reconnectCause
	reconnectCauseAttempt := runner.reconnectCauseAttempt
	runner.mu.Unlock()
	if reconnectCause != nil {
		t.Fatalf("failed runner acquired reconnect cause %v", reconnectCause)
	}
	if reconnectCauseAttempt != nil {
		t.Fatalf("failed runner acquired reconnect attempt %p", reconnectCauseAttempt)
	}
}

func TestAbandonPreservesGenuineTransientCause(t *testing.T) {
	testDriver := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 1)}
	client := newReconnectTestClient(t, testDriver, nil, 0)
	cause := &driver.Error{
		Driver: testDriver.Name(),
		Op:     "consumer",
		K:      driver.KindTransient,
		Err:    errors.New("consumer disconnected"),
	}
	attempt := &reconnectAttempt{done: make(chan struct{})}
	runner := &Runner{client: client, reconnectCause: cause}

	if err := runner.abandonForReconnect(context.Background(), attempt); err != nil {
		t.Fatalf("abandonForReconnect() = %v, want nil", err)
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if runner.reconnectCause != cause { //nolint:errorlint // exact identity protects the genuine cause
		t.Fatalf("reconnect cause = %v, want %v", runner.reconnectCause, cause)
	}
	if runner.reconnectCauseAttempt != nil {
		t.Fatalf("genuine transient cause acquired reconnect attempt %p", runner.reconnectCauseAttempt)
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
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 8)}
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
	pauseEntered := make(chan struct{})
	releasePause := make(chan struct{})
	var pauseOnce, releasePauseOnce sync.Once
	releasePausedRunner := func() {
		releasePauseOnce.Do(func() { close(releasePause) })
	}
	t.Cleanup(releasePausedRunner)
	victimRunner.reconnectDecisionHook = func() {
		pauseOnce.Do(func() {
			close(pauseEntered)
			<-releasePause
		})
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
	releaseVictim.Do(func() { close(victimRelease) })
	select {
	case <-pauseEntered:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("victim runner did not reach reconnect decision")
	}
	select {
	case token := <-recorded.sleepStarted:
		<-token
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
	causingMessage := validReconnectMessage(t, "causing-after-reconnect")
	victimMessage := validReconnectMessage(t, "victim-after-reconnect")
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
					consumer.send(causingMessage)
				case "victim":
					consumer.send(victimMessage)
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
	waitReconnectCondition(t, func() bool {
		return causingHandled.Load() >= 1 && !client.isReconnecting()
	})
	releasePausedRunner()
	waitReconnectCondition(t, func() bool {
		serviceMu.Lock()
		defer serviceMu.Unlock()
		return causingHandled.Load() >= 1 &&
			victimHandled.Load() >= 1 &&
			!client.isReconnecting()
	})
	if got := recorded.sleepCount(); got != 1 {
		t.Fatalf("reconnect sleep count = %d, want 1", got)
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
	stopService()
}

func TestSupervisorReconnectPreservesNewestAbandonedOwner(t *testing.T) {
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
	d.mu.Lock()
	d.consumerOpenHook = secondConsumerOpenGate("victim", victimOpenStarted, victimOpenRelease, false, nil)
	d.mu.Unlock()
	pauseEntered := make(chan struct{})
	releasePause := make(chan struct{})
	var releasePauseOnce, releaseOpenOnce sync.Once
	releasePauseGate := func() {
		releasePauseOnce.Do(func() { close(releasePause) })
	}
	var decisionCount atomic.Int32
	waitingForNewestOwner := make(chan struct{})
	victimRunner.reconnectDecisionHook = func() {
		switch decisionCount.Add(1) {
		case 1:
			close(pauseEntered)
			<-releasePause
		case 3:
			close(waitingForNewestOwner)
		}
	}
	t.Cleanup(func() {
		releasePauseGate()
		releaseOpenOnce.Do(func() { close(victimOpenRelease) })
	})

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

	causeB := errors.New("first supervisor reconnect")
	bDone := make(chan error, 1)
	go func() { bDone <- causingRunner.requestAndWaitReconnect(context.Background(), causeB) }()
	waitReconnectCondition(t, func() bool { return recorded.sleepCount() == 1 })
	select {
	case <-pauseEntered:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("victim runner did not reach the completed-owner pause")
	}
	select {
	case token := <-recorded.sleepStarted:
		<-token
		fake.Advance(0)
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("first reconnect sleep did not start")
	}
	select {
	case err := <-bDone:
		if err != nil {
			t.Fatalf("first reconnect = %v, want nil", err)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("first reconnect did not finish")
	}

	causeA := errors.New("newer supervisor reconnect")
	aDone := make(chan error, 1)
	go func() { aDone <- causingRunner.requestAndWaitReconnect(context.Background(), causeA) }()
	waitReconnectCondition(t, func() bool { return recorded.sleepCount() == 2 })
	releasePauseGate()
	select {
	case <-waitingForNewestOwner:
	case <-victimOpenStarted:
		t.Fatal("victim started replacement before the newer owner completed")
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("victim did not wait for the newer owner")
	}
	select {
	case token := <-recorded.sleepStarted:
		<-token
		fake.Advance(0)
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("second reconnect sleep did not start")
	}
	select {
	case err := <-aDone:
		if err != nil {
			t.Fatalf("second reconnect = %v, want nil", err)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("second reconnect did not finish")
	}
	select {
	case <-victimOpenStarted:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("victim replacement consumer did not start after the newer owner completed")
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
					consumer.send(validReconnectMessage(t, "causing-after-newer-owner"))
				case "victim":
					consumer.send(validReconnectMessage(t, "victim-after-newer-owner"))
				}
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
	if got := recorded.sleepCount(); got != 2 {
		t.Fatalf("reconnect sleep count = %d, want 2", got)
	}
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health after newer owner handoff = %v, want nil", err)
	}
	select {
	case err := <-victimDone:
		t.Fatalf("victim Run returned after newer owner handoff: %v", err)
	default:
	}
	select {
	case err := <-causingDone:
		t.Fatalf("causing Run returned after newer owner handoff: %v", err)
	default:
	}
	stopService()
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
		return causingRunner.lifecycle.Ready() && victimRunner.lifecycle.Ready()
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
	go func() { reconnectDone <- causingRunner.requestAndWaitReconnect(context.Background(), cause) }()
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

// TestAbandonForReconnectPreservesRunnerOwnCause proves that a runner which
// already recorded its own driver error before a client-wide reconnect began
// keeps that error through supervisor abandonment, instead of having it
// replaced by the generic reconnect placeholder every abandoned runner would
// otherwise receive.
func TestAbandonForReconnectPreservesRunnerOwnCause(t *testing.T) {
	fake := clock.NewFake(time.Unix(350, 0))
	recorded := &recordingClock{Fake: fake, sleepStarted: make(chan chan struct{}, 8)}
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
	case token := <-recorded.sleepStarted:
		<-token
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
	serviceCtx, stopServing := context.WithCancel(context.Background())
	servingDone := make(chan struct{})
	var serviceMu sync.Mutex
	go func() {
		defer close(servingDone)
		for {
			select {
			case <-serviceCtx.Done():
				return
			case <-d.created:
				serviceMu.Lock()
				if serviceCtx.Err() != nil {
					serviceMu.Unlock()
					return
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
	waitReconnectCondition(t, func() bool {
		serviceMu.Lock()
		defer serviceMu.Unlock()
		settled := causingRunner.lifecycle.Ready() && victimRunner.lifecycle.Ready() && !client.isReconnecting()
		if settled {
			stopServing()
		}
		return settled
	})
	stopService()
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
