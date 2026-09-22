package f1

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	//nolint:depguard // backlog panic test drives deliveries through the in-memory driver.
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/testhook"
)

// backlogRecordingObserver keeps only point events. Start and Finish satisfy
// the Observer interface and do nothing: the poll emits points only.
type backlogRecordingObserver struct {
	mu     sync.Mutex
	points []PointEvent
}

func (o *backlogRecordingObserver) Start(ctx context.Context, _ StartEvent) (context.Context, Token) {
	return ctx, Token{}
}

func (o *backlogRecordingObserver) Finish(Token, FinishEvent) {}

func (o *backlogRecordingObserver) Record(event PointEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.points = append(o.points, event)
}

func (o *backlogRecordingObserver) backlogPoints() []PointEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]PointEvent, 0, len(o.points))
	for _, p := range o.points {
		if p.Kind == ObserverBacklogSampled {
			out = append(out, p)
		}
	}
	return out
}

func (o *backlogRecordingObserver) pointCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.points)
}

// backlogLagConsumer is a probe consumer in the shape of the drain and owner
// probes: blocked message and error streams, immediate Drain, counted Stop
// and Release, and a Lag with a call counter and an injectable error.
type backlogLagConsumer struct {
	mu       sync.Mutex
	lagCalls int
	lagMap   map[string]int64
	lagErr   error
	messages chan driver.InboundMessage
	errs     chan error
	stops    int
	releases int
	stopOnce sync.Once
}

func newBacklogLagConsumer(lagMap map[string]int64) *backlogLagConsumer {
	return &backlogLagConsumer{
		lagMap:   lagMap,
		messages: make(chan driver.InboundMessage, 8),
		errs:     make(chan error, 8),
	}
}

func (c *backlogLagConsumer) Messages() <-chan driver.InboundMessage { return c.messages }
func (c *backlogLagConsumer) Errors() <-chan error                   { return c.errs }
func (*backlogLagConsumer) Pause(...string) error                    { return nil }
func (*backlogLagConsumer) Resume(...string) error                   { return nil }
func (*backlogLagConsumer) Drain(context.Context) error              { return nil }

func (c *backlogLagConsumer) Stop(context.Context) error {
	c.mu.Lock()
	c.stops++
	c.mu.Unlock()
	c.stopOnce.Do(func() {
		close(c.messages)
		close(c.errs)
	})
	return nil
}

func (c *backlogLagConsumer) Release(context.Context) error {
	c.mu.Lock()
	c.releases++
	c.mu.Unlock()
	c.stopOnce.Do(func() {
		close(c.messages)
		close(c.errs)
	})
	return nil
}

func (c *backlogLagConsumer) Lag(context.Context) (map[string]int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lagCalls++
	if c.lagErr != nil {
		return nil, c.lagErr
	}
	out := make(map[string]int64, len(c.lagMap))
	for k, v := range c.lagMap {
		out[k] = v
	}
	return out, nil
}

func (c *backlogLagConsumer) setLagMap(m map[string]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lagMap = m
}

func (c *backlogLagConsumer) setLagErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lagErr = err
}

func (c *backlogLagConsumer) calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lagCalls
}

type backlogReaderConsumer struct {
	*backlogLagConsumer
	backlogCalls int
	backlogMap   map[string]driver.BacklogSample
	backlogErr   error
}

func newBacklogReaderConsumer(samples map[string]driver.BacklogSample) *backlogReaderConsumer {
	return &backlogReaderConsumer{
		backlogLagConsumer: newBacklogLagConsumer(nil),
		backlogMap:         samples,
	}
}

func (c *backlogReaderConsumer) Backlog(context.Context) (map[string]driver.BacklogSample, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.backlogCalls++
	if c.backlogErr != nil {
		return nil, c.backlogErr
	}
	out := make(map[string]driver.BacklogSample, len(c.backlogMap))
	for k, v := range c.backlogMap {
		out[k] = v
	}
	return out, nil
}

func (c *backlogReaderConsumer) backlogCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.backlogCalls
}

func (c *backlogReaderConsumer) setBacklogMap(m map[string]driver.BacklogSample) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.backlogMap = m
}

func (c *backlogReaderConsumer) setBacklogErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.backlogErr = err
}

type inmemBacklogObserverClient struct {
	client   *Client
	observer *backlogRecordingObserver
}

func newInmemBacklogObserverClient(t *testing.T, fake *clock.Fake, interval time.Duration) inmemBacklogObserverClient {
	t.Helper()
	observer := &backlogRecordingObserver{}
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(testhook.Driver(fake)),
		WithObserver(observer),
		WithPublishTopics("orders.created"),
		WithBacklogPollInterval(interval),
		withClock(fake),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return inmemBacklogObserverClient{client: client, observer: observer}
}

type backlogConn struct {
	consumer driver.Consumer
}

func (*backlogConn) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (*backlogConn) BrokerInfo() driver.BrokerInfo     { return driver.BrokerInfo{Kind: "backlog-probe"} }

func (*backlogConn) Producer(context.Context, driver.ProducerConfig) (driver.Producer, error) {
	return nil, errors.New("backlog probe has no producer")
}

func (c *backlogConn) Consumer(context.Context, driver.ConsumerConfig) (driver.Consumer, error) {
	return c.consumer, nil
}
func (*backlogConn) Admin() driver.Admin         { return &dispatchAdmin{} }
func (*backlogConn) Ping(context.Context) error  { return nil }
func (*backlogConn) Close(context.Context) error { return nil }

type backlogDriver struct {
	conn *backlogConn
}

func (*backlogDriver) Name() string                      { return "inmem" }
func (*backlogDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (d *backlogDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return d.conn, nil
}

func backlogTestSubscription() Subscription {
	return Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Concurrency:    1,
		Prefetch:       4,
		Priorities:     []Priority{PriorityMedium},
		Retry:          RetryConfig{MaxAttempts: 1},
		HandlerTimeout: 50 * time.Millisecond,
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error { return nil }),
		},
	}
}

func newBacklogClient(t *testing.T, fake *clock.Fake, observer Observer, interval time.Duration, consumer driver.Consumer) *Client {
	t.Helper()
	opts := []Option{
		WithDriver(&backlogDriver{conn: &backlogConn{consumer: consumer}}),
		withClock(fake),
		WithBacklogPollInterval(interval),
	}
	if observer != nil {
		opts = append(opts, WithObserver(observer))
	}
	client, err := New(context.Background(), testClientConfig(t), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client
}

func startBacklogRunner(t *testing.T, client *Client) (*Runner, context.CancelFunc, chan error) {
	t.Helper()
	return startBacklogRunnerSub(t, client, backlogTestSubscription())
}

func startBacklogRunnerSub(t *testing.T, client *Client, sub Subscription) (*Runner, context.CancelFunc, chan error) {
	t.Helper()
	runner, err := client.Subscribe(context.Background(), sub)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runDone:
		case <-clock.NewReal().Timer(5 * time.Second).C:
		}
	})
	waitOwnerReady(t, runner)
	return runner, cancel, runDone
}

func waitBacklogCondition(t *testing.T, msg string, cond func() bool) {
	t.Helper()
	deadline := clock.NewReal().Timer(5 * time.Second)
	defer deadline.Stop()
	ticker := clock.NewReal().Ticker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if cond() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal(msg)
			return
		case <-ticker.C:
		}
	}
}

func TestBacklogPollOneTick(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	rec := &backlogRecordingObserver{}
	consumer := newBacklogLagConsumer(nil)
	client := newBacklogClient(t, fake, rec, time.Second, consumer)
	type want struct {
		topic    string
		priority Priority
		backlog  int64
	}
	wants := make(map[string]want)
	lagMap := make(map[string]int64)
	topics := []string{"orders.created", "orders.updated"}
	priorities := []Priority{PriorityHigh, PriorityMedium}
	backlogs := []int64{7, 8, 9, 11}
	i := 0
	for _, tp := range topics {
		for _, pr := range priorities {
			dest := consumeDestination(client.effective, client.source, tp, pr, "orders")
			lagMap[dest] = backlogs[i]
			wants[dest] = want{topic: tp, priority: pr, backlog: backlogs[i]}
			i++
		}
	}
	retryDest := retryDestinationFor(client.source, "orders.created", PriorityMedium, 1, "orders")
	lagMap[retryDest] = 99
	lagMap["unknown-dest"] = 5
	consumer.setLagMap(lagMap)
	runner, _, _ := startBacklogRunnerSub(t, client, backlogTwoByTwoSubscription())
	_ = runner
	waitForFakeTimer(t, fake)
	before := fake.Now()
	fake.Advance(time.Second)
	waitBacklogCondition(t, "backlog_sampled did not arrive after one tick", func() bool {
		return len(rec.backlogPoints()) >= 4
	})
	points := rec.backlogPoints()
	if len(points) != 4 {
		t.Fatalf("backlog_sampled count = %d, want 4", len(points))
	}
	seen := make(map[string]bool, len(wants))
	for _, p := range points {
		matched := false
		for dest, w := range wants {
			if seen[dest] {
				continue
			}
			if p.Topic == w.topic && p.Priority == w.priority {
				if p.Subscription != "orders" {
					t.Fatalf("Subscription = %q, want orders", p.Subscription)
				}
				if p.Backlog != w.backlog {
					t.Fatalf("Backlog for %s/%v = %d, want %d", w.topic, w.priority, p.Backlog, w.backlog)
				}
				if p.HeadAgeKnown {
					t.Fatalf("HeadAgeKnown = true, want false")
				}
				if p.HeadAge != 0 {
					t.Fatalf("HeadAge = %v, want 0", p.HeadAge)
				}
				if !p.At.Equal(before.Add(time.Second)) {
					t.Fatalf("At = %v, want %v", p.At, before.Add(time.Second))
				}
				seen[dest] = true
				matched = true
				break
			}
		}
		if !matched {
			t.Fatalf("unexpected backlog_sampled: %+v", p)
		}
	}
}

func TestBacklogPollInmemHeadAge(t *testing.T) {
	start := time.Unix(1000, 0)
	fake := clock.NewFake(start)
	bundle := newInmemBacklogObserverClient(t, fake, 5*time.Second)
	runner, _, _ := startBacklogRunner(t, bundle.client)
	runner.mu.Lock()
	consumer := runner.consumer
	runner.mu.Unlock()
	if consumer == nil {
		t.Fatal("runner consumer is nil")
	}
	if err := consumer.Pause(); err != nil {
		t.Fatal(err)
	}
	if _, err := bundle.client.Publisher().Publish(context.Background(), "orders.created", map[string]string{"id": "first"}); err != nil {
		t.Fatal(err)
	}
	fake.Advance(time.Second)
	if _, err := bundle.client.Publisher().Publish(context.Background(), "orders.created", map[string]string{"id": "second"}); err != nil {
		t.Fatal(err)
	}
	fake.Advance(4 * time.Second)
	waitBacklogCondition(t, "in-memory backlog sample did not arrive", func() bool {
		return len(bundle.observer.backlogPoints()) >= 1
	})
	points := bundle.observer.backlogPoints()
	if len(points) != 1 {
		t.Fatalf("backlog_sampled count = %d, want 1", len(points))
	}
	point := points[0]
	if point.Backlog != 2 {
		t.Fatalf("Backlog = %d, want 2", point.Backlog)
	}
	if !point.HeadAgeKnown {
		t.Fatal("HeadAgeKnown = false, want true")
	}
	if point.HeadAge != 5*time.Second {
		t.Fatalf("HeadAge = %v, want 5s", point.HeadAge)
	}
	if point.EnqueuedAtSource != EnqueuedAtBroker {
		t.Fatalf("EnqueuedAtSource = %q, want broker", point.EnqueuedAtSource)
	}
}

func TestBacklogPollEmptyDestinationUnknown(t *testing.T) {
	fake := clock.NewFake(time.Unix(1100, 0))
	bundle := newInmemBacklogObserverClient(t, fake, time.Second)
	_, _, _ = startBacklogRunner(t, bundle.client)
	waitForFakeTimer(t, fake)
	fake.Advance(time.Second)
	waitBacklogCondition(t, "empty destination backlog sample did not arrive", func() bool {
		return len(bundle.observer.backlogPoints()) >= 1
	})
	point := bundle.observer.backlogPoints()[0]
	if point.Backlog != 0 {
		t.Fatalf("Backlog = %d, want 0", point.Backlog)
	}
	if point.HeadAgeKnown {
		t.Fatal("HeadAgeKnown = true, want false")
	}
	if point.HeadAge != 0 {
		t.Fatalf("HeadAge = %v, want 0", point.HeadAge)
	}
	if point.EnqueuedAtSource != EnqueuedAtUnknown {
		t.Fatalf("EnqueuedAtSource = %q, want unknown", point.EnqueuedAtSource)
	}
}

func TestBacklogPollUsesBacklogReader(t *testing.T) {
	fake := clock.NewFake(time.Unix(1200, 0))
	rec := &backlogRecordingObserver{}
	consumer := newBacklogReaderConsumer(nil)
	client := newBacklogClient(t, fake, rec, time.Second, consumer)
	destination := consumeDestination(client.effective, client.source, "orders.created", PriorityMedium, "orders")
	consumer.setBacklogMap(map[string]driver.BacklogSample{
		destination: {Lag: 9, HeadEnqueuedAt: fake.Now().Add(-2 * time.Second), HeadSource: driver.EnqueueSourceProducer},
	})
	_, _, _ = startBacklogRunner(t, client)
	waitForFakeTimer(t, fake)
	fake.Advance(time.Second)
	waitBacklogCondition(t, "Backlog reader sample did not arrive", func() bool {
		return consumer.backlogCallCount() >= 1 && len(rec.backlogPoints()) >= 1
	})
	if got := consumer.backlogCallCount(); got != 1 {
		t.Fatalf("Backlog calls = %d, want 1", got)
	}
	if got := consumer.calls(); got != 0 {
		t.Fatalf("Lag calls = %d, want 0", got)
	}
	point := rec.backlogPoints()[0]
	if point.Backlog != 9 {
		t.Fatalf("Backlog = %d, want 9", point.Backlog)
	}
	if !point.HeadAgeKnown || point.HeadAge != 3*time.Second {
		t.Fatalf("head age = %v known=%v, want 3s known=true", point.HeadAge, point.HeadAgeKnown)
	}
	if point.EnqueuedAtSource != EnqueuedAtProducer {
		t.Fatalf("EnqueuedAtSource = %q, want producer", point.EnqueuedAtSource)
	}
}

func TestBacklogPollFutureHeadClampsAge(t *testing.T) {
	fake := clock.NewFake(time.Unix(1300, 0))
	rec := &backlogRecordingObserver{}
	consumer := newBacklogReaderConsumer(nil)
	client := newBacklogClient(t, fake, rec, time.Second, consumer)
	destination := consumeDestination(client.effective, client.source, "orders.created", PriorityMedium, "orders")
	consumer.setBacklogMap(map[string]driver.BacklogSample{
		destination: {Lag: 1, HeadEnqueuedAt: fake.Now().Add(2 * time.Second), HeadSource: driver.EnqueueSourceBroker},
	})
	_, _, _ = startBacklogRunner(t, client)
	waitForFakeTimer(t, fake)
	fake.Advance(time.Second)
	waitBacklogCondition(t, "future head sample did not arrive", func() bool {
		return len(rec.backlogPoints()) >= 1
	})
	point := rec.backlogPoints()[0]
	if !point.HeadAgeKnown {
		t.Fatal("HeadAgeKnown = false, want true")
	}
	if point.HeadAge != 0 {
		t.Fatalf("HeadAge = %v, want 0", point.HeadAge)
	}
	if point.EnqueuedAtSource != EnqueuedAtBroker {
		t.Fatalf("EnqueuedAtSource = %q, want broker", point.EnqueuedAtSource)
	}
}

func backlogTwoByTwoSubscription() Subscription {
	return Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created", "orders.updated"},
		Concurrency:    1,
		Prefetch:       12,
		Priorities:     []Priority{PriorityHigh, PriorityMedium},
		Retry:          RetryConfig{MaxAttempts: 1},
		HandlerTimeout: 50 * time.Millisecond,
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error { return nil }),
			"orders.updated": HandlerFunc(func(context.Context, *Event) error { return nil }),
		},
	}
}

func TestBacklogPollNoObserver(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	rec := &backlogRecordingObserver{}
	controlConsumer := newBacklogLagConsumer(nil)
	controlClient := newBacklogClient(t, fake, rec, time.Second, controlConsumer)
	mainDest := consumeDestination(controlClient.effective, controlClient.source, "orders.created", PriorityMedium, "orders")
	controlConsumer.setLagMap(map[string]int64{mainDest: 3})
	controlRunner, controlCancel, _ := startBacklogRunner(t, controlClient)
	_ = controlRunner
	waitForFakeTimer(t, fake)
	fake.Advance(time.Second)
	waitBacklogCondition(t, "control poll did not run with an observer", func() bool {
		return controlConsumer.calls() >= 1
	})
	controlCancel()

	fake2 := clock.NewFake(time.Unix(0, 0))
	consumer := newBacklogLagConsumer(map[string]int64{mainDest: 3})
	_ = newBacklogClient(t, fake2, nil, time.Second, consumer)
	client2, err := New(context.Background(), testClientConfig(t),
		WithDriver(&backlogDriver{conn: &backlogConn{consumer: consumer}}),
		withClock(fake2),
		WithBacklogPollInterval(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client2.Close(context.Background()) })
	runner2, err := client2.Subscribe(context.Background(), backlogTestSubscription())
	if err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	runDone2 := make(chan error, 1)
	go func() { runDone2 <- runner2.Run(ctx2) }()
	t.Cleanup(func() {
		cancel2()
		select {
		case <-runDone2:
		case <-clock.NewReal().Timer(5 * time.Second).C:
		}
	})
	waitOwnerReady(t, runner2)
	for range 3 {
		fake2.Advance(time.Second)
	}
	settle := clock.NewReal().Timer(200 * time.Millisecond)
	defer settle.Stop()
	select {
	case <-settle.C:
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("settle timer did not fire")
	}
	if got := consumer.calls(); got != 0 {
		t.Fatalf("Lag calls without observer = %d, want 0", got)
	}
}

func TestBacklogPollDisabledInterval(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	rec := &backlogRecordingObserver{}
	controlConsumer := newBacklogLagConsumer(nil)
	controlClient := newBacklogClient(t, fake, rec, time.Second, controlConsumer)
	mainDest := consumeDestination(controlClient.effective, controlClient.source, "orders.created", PriorityMedium, "orders")
	controlConsumer.setLagMap(map[string]int64{mainDest: 3})
	controlRunner, controlCancel, _ := startBacklogRunner(t, controlClient)
	_ = controlRunner
	waitForFakeTimer(t, fake)
	fake.Advance(time.Second)
	waitBacklogCondition(t, "control poll did not run with a positive interval", func() bool {
		return controlConsumer.calls() >= 1
	})
	controlCancel()

	fake2 := clock.NewFake(time.Unix(0, 0))
	rec2 := &backlogRecordingObserver{}
	consumer := newBacklogLagConsumer(map[string]int64{mainDest: 3})
	client2 := newBacklogClient(t, fake2, rec2, -1, consumer)
	runner2, cancel2, _ := startBacklogRunner(t, client2)
	_ = runner2
	defer cancel2()
	for range 3 {
		fake2.Advance(time.Second)
	}
	settle := clock.NewReal().Timer(200 * time.Millisecond)
	defer settle.Stop()
	select {
	case <-settle.C:
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("settle timer did not fire")
	}
	if got := consumer.calls(); got != 0 {
		t.Fatalf("Lag calls with disabled interval = %d, want 0", got)
	}
	if got := len(rec2.backlogPoints()); got != 0 {
		t.Fatalf("backlog_sampled with disabled interval = %d, want 0", got)
	}
}

func TestBacklogPollLagErrorSkipsTick(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	rec := &backlogRecordingObserver{}
	consumer := newBacklogLagConsumer(nil)
	client := newBacklogClient(t, fake, rec, time.Second, consumer)
	mainDest := consumeDestination(client.effective, client.source, "orders.created", PriorityMedium, "orders")
	consumer.setLagMap(map[string]int64{mainDest: 11})
	consumer.setLagErr(errors.New("lag boom"))
	runner, _, _ := startBacklogRunner(t, client)
	_ = runner
	waitForFakeTimer(t, fake)
	fake.Advance(time.Second)
	settle := clock.NewReal().Timer(200 * time.Millisecond)
	select {
	case <-settle.C:
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("settle timer did not fire")
	}
	settle.Stop()
	if got := len(rec.backlogPoints()); got != 0 {
		t.Fatalf("backlog_sampled after Lag error = %d, want 0", got)
	}
	if got := consumer.calls(); got < 1 {
		t.Fatalf("Lag calls after error tick = %d, want at least 1", got)
	}
	consumer.setLagErr(nil)
	fake.Advance(time.Second)
	waitBacklogCondition(t, "backlog_sampled did not arrive after Lag recovered", func() bool {
		return len(rec.backlogPoints()) >= 1
	})
	points := rec.backlogPoints()
	if len(points) != 1 {
		t.Fatalf("backlog_sampled count after recovery = %d, want 1", len(points))
	}
	if points[0].Backlog != 11 {
		t.Fatalf("Backlog after recovery = %d, want 11", points[0].Backlog)
	}
}

func TestBacklogPollStopsWithDrain(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	rec := &backlogRecordingObserver{}
	consumer := newBacklogLagConsumer(nil)
	client := newBacklogClient(t, fake, rec, time.Second, consumer)
	mainDest := consumeDestination(client.effective, client.source, "orders.created", PriorityMedium, "orders")
	consumer.setLagMap(map[string]int64{mainDest: 4})
	baseline := runtime.NumGoroutine()
	runner, cancel, runDone := startBacklogRunner(t, client)
	defer cancel()
	waitForFakeTimer(t, fake)
	fake.Advance(time.Second)
	waitBacklogCondition(t, "poll did not run before drain", func() bool {
		return consumer.calls() >= 1 && len(rec.backlogPoints()) >= 1
	})
	callsBeforeDrain := consumer.calls()
	if err := runner.Drain(context.Background()); err != nil {
		t.Fatalf("Drain() = %v, want nil", err)
	}
	select {
	case <-runDone:
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("Run did not return after Drain")
	}
	// Refill so the shared cleanup's receive does not wait the full timeout.
	select {
	case runDone <- nil:
	default:
	}
	callsAfterDrain := consumer.calls()
	for range 3 {
		fake.Advance(time.Second)
	}
	settle := clock.NewReal().Timer(200 * time.Millisecond)
	defer settle.Stop()
	select {
	case <-settle.C:
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("settle timer did not fire")
	}
	if got := consumer.calls(); got != callsAfterDrain {
		t.Fatalf("Lag calls after drain advanced = %d, want %d", got, callsAfterDrain)
	}
	if callsBeforeDrain < 1 {
		t.Fatalf("Lag calls before drain = %d, want at least 1", callsBeforeDrain)
	}
	deadline := clock.NewReal().Timer(2 * time.Second)
	defer deadline.Stop()
	ticker := clock.NewReal().Ticker(5 * time.Millisecond)
	defer ticker.Stop()
	for runtime.NumGoroutine() > baseline {
		select {
		case <-deadline.C:
			t.Fatalf("goroutines after drain = %d, baseline = %d, want no leak", runtime.NumGoroutine(), baseline)
			return
		case <-ticker.C:
		}
	}
}

// joinGateConsumer blocks in Lag once the tick context ends, until the test
// opens the gate. It reuses the probe consumer for everything else.
type joinGateConsumer struct {
	*backlogLagConsumer
	gate     chan struct{}
	gateOnce sync.Once
}

func newJoinGateConsumer(lagMap map[string]int64) *joinGateConsumer {
	return &joinGateConsumer{
		backlogLagConsumer: newBacklogLagConsumer(lagMap),
		gate:               make(chan struct{}),
	}
}

func (c *joinGateConsumer) Lag(ctx context.Context) (map[string]int64, error) {
	c.mu.Lock()
	c.lagCalls++
	c.mu.Unlock()
	<-ctx.Done()
	<-c.gate
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int64, len(c.lagMap))
	for k, v := range c.lagMap {
		out[k] = v
	}
	return out, nil
}

func (c *joinGateConsumer) openGate() {
	c.gateOnce.Do(func() { close(c.gate) })
}

func TestBacklogPollJoinWaitsForLag(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	rec := &backlogRecordingObserver{}
	consumer := newJoinGateConsumer(nil)
	client := newBacklogClient(t, fake, rec, time.Second, consumer)
	mainDest := consumeDestination(client.effective, client.source, "orders.created", PriorityMedium, "orders")
	consumer.setLagMap(map[string]int64{mainDest: 4})
	t.Cleanup(func() { consumer.openGate() })
	runner, cancel, runDone := startBacklogRunner(t, client)
	defer cancel()
	waitForFakeTimer(t, fake)
	fake.Advance(time.Second)
	waitBacklogCondition(t, "poll did not reach Lag", func() bool {
		return consumer.calls() >= 1
	})
	drainDone := make(chan error, 1)
	go func() { drainDone <- runner.Drain(context.Background()) }()
	select {
	case err := <-runDone:
		t.Fatalf("Run returned while the gate was shut: %v", err)
	case err := <-drainDone:
		t.Fatalf("Drain returned while the gate was shut: %v", err)
	case <-clock.NewReal().Timer(100 * time.Millisecond).C:
	}
	consumer.openGate()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() = %v, want nil", err)
		}
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("Run did not return after the gate opened")
	}
	select {
	case runDone <- nil:
	default:
	}
	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("Drain() = %v, want nil", err)
		}
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("Drain did not return after the gate opened")
	}
}

// panicLagDriver wraps the in-memory driver so deliveries flow while Lag
// panics. It exists so the panic test proves the generation survives.
type panicLagDriver struct {
	inner driver.Driver
}

func (d *panicLagDriver) Name() string                      { return d.inner.Name() }
func (d *panicLagDriver) Capabilities() driver.Capabilities { return d.inner.Capabilities() }
func (d *panicLagDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	conn, err := d.inner.Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &panicLagConn{Conn: conn}, nil
}

type panicLagConn struct {
	driver.Conn
}

func (c *panicLagConn) Consumer(ctx context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	consumer, err := c.Conn.Consumer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &panicLagConsumer{Consumer: consumer}, nil
}

type panicLagConsumer struct {
	driver.Consumer
	mu    sync.Mutex
	calls int
}

func (c *panicLagConsumer) Lag(context.Context) (map[string]int64, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	panic("backlog poll test panic")
}

func (c *panicLagConsumer) lagCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func TestBacklogPollLagPanicKeepsGoing(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	rec := &backlogRecordingObserver{}
	var logs logSink
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	panicDriver := &panicLagDriver{inner: inmem.New()}
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(panicDriver),
		WithObserver(rec),
		WithLogger(logger),
		withClock(fake),
		WithBacklogPollInterval(time.Second),
		WithPublishTopics("orders.created"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	handled := make(chan struct{})
	var handledOnce sync.Once
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Concurrency:    1,
		Prefetch:       4,
		Priorities:     []Priority{PriorityMedium},
		Retry:          RetryConfig{MaxAttempts: 1},
		HandlerTimeout: 50 * time.Millisecond,
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error {
				handledOnce.Do(func() { close(handled) })
				return nil
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runDone:
		case <-clock.NewReal().Timer(5 * time.Second).C:
		}
	})
	waitOwnerReady(t, runner)
	waitForFakeTimer(t, fake)
	fake.Advance(time.Second)
	waitBacklogCondition(t, "poll panic was not logged", func() bool {
		return strings.Count(logs.String(), "backlog poll panicked") >= 1
	})
	fake.Advance(time.Second)
	fake.Advance(time.Second)
	settle := clock.NewReal().Timer(200 * time.Millisecond)
	defer settle.Stop()
	select {
	case <-settle.C:
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("settle timer did not fire")
	}
	if got := strings.Count(logs.String(), "backlog poll panicked"); got != 1 {
		t.Fatalf("panic log lines = %d, want 1", got)
	}
	publishConsumeOne(t, client, context.Background(), map[string]string{"id": "after-panic"})
	select {
	case <-handled:
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("handler did not run after the poll panic")
	}
	if err := runner.Drain(context.Background()); err != nil {
		t.Fatalf("Drain() = %v, want nil", err)
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() = %v, want nil", err)
		}
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("Run did not return after Drain")
	}
	select {
	case runDone <- nil:
	default:
	}
}
