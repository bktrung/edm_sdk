package f1

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
)

// ownerProbeConsumer records how a running runner let it go, and can hold the
// release open so a test knows the runner is inside its teardown rather than
// guessing from timing.
type ownerProbeConsumer struct {
	mu          sync.Mutex
	stops       int
	releases    int
	stopStarted chan struct{}
	stopRelease chan struct{}
	stopOnce    sync.Once
	messages    chan driver.InboundMessage
	errors      chan error
}

func newOwnerProbeConsumer() *ownerProbeConsumer {
	return &ownerProbeConsumer{
		messages: make(chan driver.InboundMessage, 8),
		errors:   make(chan error, 8),
	}
}

func (c *ownerProbeConsumer) Messages() <-chan driver.InboundMessage { return c.messages }
func (c *ownerProbeConsumer) Errors() <-chan error                   { return c.errors }
func (*ownerProbeConsumer) Pause(...string) error                    { return nil }
func (*ownerProbeConsumer) Resume(...string) error                   { return nil }
func (*ownerProbeConsumer) Drain(context.Context) error              { return nil }
func (*ownerProbeConsumer) Lag(context.Context) (map[string]int64, error) {
	return nil, driver.ErrUnsupported
}

func (c *ownerProbeConsumer) Stop(context.Context) error {
	c.mu.Lock()
	c.stops++
	started := c.stopStarted
	release := c.stopRelease
	c.mu.Unlock()
	if started != nil {
		c.stopOnce.Do(func() { close(started) })
	}
	if release != nil {
		<-release
	}
	return nil
}

func (c *ownerProbeConsumer) Release(context.Context) error {
	c.mu.Lock()
	c.releases++
	c.mu.Unlock()
	return nil
}

func (c *ownerProbeConsumer) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stops, c.releases
}

// ownerPanicMessagesConsumer panics where the fetcher asks for its channel,
// which is inside one of the runner's own source goroutines. It waits for the
// test to release it first, so the runner is already serving when the source
// dies rather than still opening.
type ownerPanicMessagesConsumer struct {
	*ownerProbeConsumer
	release <-chan struct{}
}

func (c *ownerPanicMessagesConsumer) Messages() <-chan driver.InboundMessage {
	<-c.release
	panic("owner test: messages channel is not available")
}

// ownerProbeSettler records the context error of the last settlement call the
// runner made. That is the whole question once a drain deadline has passed: a
// settlement refused with the drain's deadline is a bounded budget doing its
// job, and one refused with a cancellation is the runner's own teardown
// reaching a call that was still in flight.
type ownerProbeSettler struct {
	mu      sync.Mutex
	ctxErr  error
	settled bool
}

func (s *ownerProbeSettler) Ack(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ctxErr = ctx.Err()
	if s.ctxErr != nil {
		return s.ctxErr
	}
	s.settled = true
	return nil
}

func (s *ownerProbeSettler) Nack(ctx context.Context, _ driver.NackOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ctxErr = ctx.Err()
	if s.ctxErr != nil {
		return s.ctxErr
	}
	s.settled = true
	return nil
}

func (s *ownerProbeSettler) record() (error, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ctxErr, s.settled
}

type ownerProbeConn struct {
	consumer driver.Consumer
	producer driver.Producer
}

func (*ownerProbeConn) Capabilities() driver.Capabilities {
	return driver.Capabilities{PerMessageAck: true}
}

func (*ownerProbeConn) BrokerInfo() driver.BrokerInfo { return driver.BrokerInfo{Kind: "owner-probe"} }

func (c *ownerProbeConn) Producer(context.Context, driver.ProducerConfig) (driver.Producer, error) {
	return c.producer, nil
}

func (c *ownerProbeConn) Consumer(context.Context, driver.ConsumerConfig) (driver.Consumer, error) {
	return c.consumer, nil
}
func (*ownerProbeConn) Admin() driver.Admin        { return &dispatchAdmin{} }
func (*ownerProbeConn) Ping(context.Context) error { return nil }
func (*ownerProbeConn) Close(context.Context) error {
	return nil
}

type ownerProbeDriver struct{ conn *ownerProbeConn }

func (*ownerProbeDriver) Name() string { return "owner-probe" }
func (*ownerProbeDriver) Capabilities() driver.Capabilities {
	return driver.Capabilities{PerMessageAck: true}
}

func (d *ownerProbeDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return d.conn, nil
}

func ownerProbeSubscription() Subscription {
	return Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Concurrency:    1,
		Prefetch:       12,
		HandlerTimeout: 50 * time.Millisecond,
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error { return nil }),
		},
	}
}

// ownerRunner is a running runner and the handles a test needs to end it.
type ownerRunner struct {
	runner  *Runner
	client  *Client
	fake    *clock.Fake
	runDone <-chan error
	cancel  context.CancelFunc
	stopped *sync.WaitGroup
}

// startOwnerRunner builds a runner that is actually running. The owner loop
// only exists after Run, and what these tests pin is what it does with the
// reports its own goroutines make, so a hand-built Runner would not exercise
// them. runDone carries Run's result and is buffered, so a test that does not
// read it cannot stall the runner's goroutine.
func startOwnerRunner(t *testing.T, consumer driver.Consumer) *ownerRunner {
	t.Helper()
	fake := clock.NewFake(time.Unix(0, 0))
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&ownerProbeDriver{conn: &ownerProbeConn{consumer: consumer, producer: &dispatchProducer{}}}),
		withClock(fake))
	if err != nil {
		t.Fatal(err)
	}
	runner, err := client.Subscribe(context.Background(), ownerProbeSubscription())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	var stopped sync.WaitGroup
	stopped.Add(1)
	go func() {
		defer stopped.Done()
		result := runner.Run(ctx)
		select {
		case runDone <- result:
		default:
		}
	}()
	t.Cleanup(func() {
		cancel()
		finished := make(chan struct{})
		go func() { stopped.Wait(); close(finished) }()
		timer := clock.NewReal().Timer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-finished:
		case <-timer.C:
			t.Errorf("runner %q did not stop", runner.subscription.Name)
		}
	})
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	waitOwnerReady(t, runner)
	return &ownerRunner{runner: runner, client: client, fake: fake, runDone: runDone, cancel: cancel, stopped: &stopped}
}

func waitOwnerReady(t *testing.T, runner *Runner) {
	t.Helper()
	deadline := clock.NewReal().Timer(2 * time.Second)
	defer deadline.Stop()
	ticker := clock.NewReal().Ticker(time.Millisecond)
	defer ticker.Stop()
	for {
		runner.mu.Lock()
		state := runner.lifecycle.State()
		runner.mu.Unlock()
		if state == lifecycle.Ready {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("runner never became ready, state %s", state)
		case <-ticker.C:
		}
	}
}

func ownerProbeMessage(t *testing.T, settler driver.Settler) driver.InboundMessage {
	t.Helper()
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "owner-probe-1",
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
		Body:        []byte(`{"id":"owner-probe-1"}`),
		Settle:      settler,
	}
}

func ownerStopCount(t *testing.T, consumer *ownerProbeConsumer) int {
	t.Helper()
	stops, _ := consumer.counts()
	return stops
}

// TestThreeRequestsToEndLeaveOneDrain pins that a runner's teardown happens
// once however many callers ask for it. Close's request, the supervisor's
// abandon and the caller cancelling the run context arrive together, and once
// the drain is under way a further request arrives while it is running. Every
// one of them has to join the same drain: a second one would stop a consumer
// that was already released, which is the runner taking work back from the
// broker after handing it away.
func TestThreeRequestsToEndLeaveOneDrain(t *testing.T) {
	consumer := newOwnerProbeConsumer()
	consumer.stopStarted = make(chan struct{})
	consumer.stopRelease = make(chan struct{})
	run := startOwnerRunner(t, consumer)

	closeDone := make(chan error, 1)
	var requests sync.WaitGroup
	requests.Add(3)
	go func() { defer requests.Done(); closeDone <- run.client.Close(context.Background()) }()
	go func() { defer requests.Done(); run.client.abandonRunners(context.Background()) }()
	go func() { defer requests.Done(); run.cancel() }()

	// The drain is holding its release open, so the runner is inside its
	// teardown and a further request lands while it runs.
	timer := clock.NewReal().Timer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-consumer.stopStarted:
	case <-timer.C:
		t.Fatal("the runner never reached its drain")
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- run.runner.Drain(context.Background()) }()

	// Give a second drain every chance to reach the consumer before reading
	// the count. One that started would enter the release within microseconds.
	settle := clock.NewReal().Timer(200 * time.Millisecond)
	defer settle.Stop()
	ticker := clock.NewReal().Ticker(2 * time.Millisecond)
	defer ticker.Stop()
watch:
	for {
		if stops := ownerStopCount(t, consumer); stops > 1 {
			t.Fatalf("the consumer was stopped %d times, want one drain however many callers asked for it", stops)
		}
		select {
		case <-settle.C:
			break watch
		case <-ticker.C:
		}
	}
	if stops := ownerStopCount(t, consumer); stops != 1 {
		t.Fatalf("the consumer was stopped %d times, want exactly one", stops)
	}

	close(consumer.stopRelease)
	requests.Wait()
	for name, done := range map[string]<-chan error{"close": closeDone, "drain": secondDone, "run": run.runDone} {
		select {
		case <-done:
		case <-clock.NewReal().Timer(2 * time.Second).C:
			t.Fatalf("%s never returned while the runner drained", name)
		}
	}
	run.runner.mu.Lock()
	state := run.runner.lifecycle.State()
	run.runner.mu.Unlock()
	if state != lifecycle.Closed {
		t.Fatalf("runner state after every caller returned = %s, want closed", state)
	}
}

// TestPanickingSourceEndsTheGeneration pins that a source goroutine that dies
// abnormally still reports. A source that panics takes its own goroutine down
// on the way out, and the report the runner waits for has to leave from the
// deferred function that runs on that path: a report left on the success path
// would never arrive, and the runner would wait for a generation end that can
// no longer happen.
func TestPanickingSourceEndsTheGeneration(t *testing.T) {
	release := make(chan struct{})
	consumer := &ownerPanicMessagesConsumer{ownerProbeConsumer: newOwnerProbeConsumer(), release: release}
	run := startOwnerRunner(t, consumer)
	close(release)

	timer := clock.NewReal().Timer(2 * time.Second)
	defer timer.Stop()
	select {
	case err := <-run.runDone:
		if err == nil {
			t.Fatal("Run returned nil after a source panicked, want the panic reported as an error")
		}
	case <-timer.C:
		t.Fatal("Run never returned after a source panicked: the generation end the runner waits for was never reported")
	}
	run.runner.mu.Lock()
	state := run.runner.lifecycle.State()
	run.runner.mu.Unlock()
	if state != lifecycle.Closed && state != lifecycle.Failed {
		t.Fatalf("runner state after a source panicked = %s, want a terminal state", state)
	}
}

// TestLateSettlementAfterTheDrainDeadlineIsRefused pins the shape of the
// settlement context a drain installs: a deadline, never a cancellation. A
// delivery whose cleanup reaches the driver after that deadline is refused
// with the deadline, while the runner is still draining and the context its
// own caller holds is already cancelled. Settling on that cancelled context
// instead would fail as a cancellation the runner caused, which the driver
// reports as a transient failure and the runner then retries against itself.
func TestLateSettlementAfterTheDrainDeadlineIsRefused(t *testing.T) {
	consumer := newOwnerProbeConsumer()
	consumer.stopStarted = make(chan struct{})
	consumer.stopRelease = make(chan struct{})
	run := startOwnerRunner(t, consumer)
	t.Cleanup(func() { close(consumer.stopRelease) })
	run.client.config.Lifecycle.DrainTimeout = 20 * time.Millisecond

	// The drain's own wait runs on the fake clock and is never advanced here,
	// so the runner stays in its drain while the settlement window, which is
	// real time, expires underneath it.
	run.runner.inflight.Add()
	go func() { _ = run.runner.Drain(context.Background()) }()

	cancelled, cancelCancelled := context.WithCancel(context.Background())
	cancelCancelled()
	deadline := clock.NewReal().Timer(2 * time.Second)
	defer deadline.Stop()
	for !errors.Is(runnerSettlementContext(run.runner, cancelled).Err(), context.DeadlineExceeded) {
		select {
		case <-deadline.C:
			t.Fatal("the drain installed no settlement deadline")
		default:
		}
		if err := clock.NewReal().Sleep(context.Background(), time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}

	// The release is held open, so the runner is still inside its drain under
	// the expired window when the settlement below runs: a drain that finished
	// would have released the window at teardown, and the assertion would be
	// about a teardown instead of about the deadline.
	select {
	case <-consumer.stopStarted:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("the drain never reached its release")
	}

	settler := &ownerProbeSettler{}
	message := ownerProbeMessage(t, settler)
	processDelivery(run.runner, cancelled, delivery{id: run.runner.inflight.Add(), message: message})

	ctxErr, settled := settler.record()
	if !errors.Is(ctxErr, context.DeadlineExceeded) {
		t.Fatalf("settlement context error = %v, want the drain deadline; a cancellation here is the runner's own teardown reaching a settlement still in flight", ctxErr)
	}
	if settled {
		t.Fatal("a settlement after the drain deadline was accepted")
	}
}
