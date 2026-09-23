package f1

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"

	"golang.org/x/sync/errgroup"
)

// TestPendingDeliveryKeepsItsChannelEnqueueStamp proves a delivery's
// deadline-promotion budget starts when the delivery leaves the fetch channel.
// Four deliveries fill the lane while the one handler holds the pool, so the
// fifth waits in the pipeline's pending slot, and every one of them leaves the
// channel while the clock is still. The wait a promoted lane reports must
// therefore measure the whole time from the channel, not only the part after
// the full lane could take the delivery.
//
// Two things make the trace deterministic. The worker is held by a fake-clock
// sleep that only this test advances, so no handler can return while the lane
// fills. And the pending delivery names a family no lane feeds, so its enqueue
// attempt logs the fallback warning: that line is written after the delivery's
// enqueue stamp, which is when the test knows the clock can move without moving
// the stamp with it.
func TestPendingDeliveryKeepsItsChannelEnqueueStamp(t *testing.T) {
	const (
		holdDuration   = 30 * time.Minute
		handlerAdvance = 20 * time.Second // Wider than the promotion rate-limit window, so every promotion is recorded.
		deliveries     = 5                // One dispatched, three queued, one pending.
	)
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	rec := &consumeRecordingObserver{}
	var output logSink
	client, runner := newRetryBridgeRunner(t, &dispatchProducer{}, "orders.created",
		WithObserver(rec), withClock(fake), WithLogger(slog.New(slog.NewTextHandler(&output, nil))))
	defer func() { _ = client.Close(context.Background()) }()
	runner.subscription.Concurrency = 1
	runner.subscription.Prefetch = 1
	runner.subscription.HandlerTimeout = time.Hour
	runner.subscription.Retry = RetryConfig{MaxAttempts: 1}
	runner.subscription.Fairness = FairnessConfig{
		Weights:        map[Priority]int{PriorityHigh: 1},
		Budgets:        map[Priority]time.Duration{PriorityHigh: time.Second},
		PrefetchFactor: 1,
	}
	runner.inflight = newInflightRegistry()
	held := make(chan struct{})
	var firstCall atomic.Bool
	handler := HandlerFunc(func(ctx context.Context, _ *Event) error {
		if firstCall.CompareAndSwap(false, true) {
			if err := fake.SleepWithRegistration(ctx, holdDuration, func() { close(held) }); err != nil {
				return err
			}
		}
		fake.Advance(handlerAdvance)
		return nil
	})
	runner.subscription.Handlers = map[string]Handler{"orders.created": handler, "payments.charged": handler}
	messages := make([]driver.InboundMessage, 0, deliveries)
	for i := range deliveries {
		eventType := "orders.created"
		if i == deliveries-1 {
			// The pending delivery: no configured family names this event
			// type's lane, so its enqueue attempt reaches the fallback lane,
			// which the three queued deliveries have already filled.
			eventType = "payments.charged"
		}
		messages = append(messages, retryBridgeMessage(t, Envelope{
			SpecVersion: "1.0",
			ID:          fmt.Sprintf("evt-stamp-%d", i),
			Source:      "/test/orders",
			Type:        eventType,
			Priority:    PriorityHigh,
			Attempt:     1,
		}, &dispatchSettler{}))
	}
	dispatch := make(chan delivery)
	go func() {
		for _, message := range messages {
			dispatch <- delivery{id: runner.inflight.Add(), message: message}
		}
		close(dispatch)
	}()
	pipelineDone := make(chan error, 1)
	go func() {
		pipelineDone <- runDispatchPipeline(runner, context.Background(), dispatch, runner.subscription.Prefetch)
	}()
	select {
	case <-held:
	case err := <-pipelineDone:
		t.Fatalf("pipeline returned before the handler held the worker: %v", err)
	case <-oneSecondTimer(t).C:
		t.Fatal("handler did not hold the worker")
	}
	deadline := clock.NewReal().Timer(time.Second)
	defer deadline.Stop()
	poll := clock.NewReal().Ticker(time.Millisecond)
	defer poll.Stop()
	for !strings.Contains(output.String(), "routing to fallback lane") {
		select {
		case err := <-pipelineDone:
			t.Fatalf("pipeline returned before the pending delivery reached its lane: %v", err)
		case <-deadline.C:
			t.Fatal("the pending delivery never reached a full lane")
		case <-poll.C:
		}
	}
	fake.Advance(holdDuration)
	select {
	case err := <-pipelineDone:
		if err != nil {
			t.Fatalf("runDispatchPipeline() error = %v, want nil", err)
		}
	case <-oneSecondTimer(t).C:
		t.Fatal("pipeline did not drain after the worker was released")
	}
	promotions := promotionPoints(rec)
	if len(promotions) == 0 {
		t.Fatal("no deadline promotion was recorded")
	}
	for i, promotion := range promotions {
		if want := promotion.At.Sub(start); promotion.LaneWait != want {
			t.Fatalf("promotion %d lane wait = %v, want %v, the whole wait since its delivery left the channel", i, promotion.LaneWait, want)
		}
	}
}

func TestOversizeBodyDeadLettersBeforeHandlerRuns(t *testing.T) {
	producer := &dispatchProducer{}
	conn := &dispatchConn{producer: producer}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()
	client.config.Codec.MaxBodyBytes = 3
	handled := false
	runner := &Runner{client: client, subscription: Subscription{
		Name:           "orders",
		HandlerTimeout: time.Second,
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error {
				handled = true
				return nil
			}),
		},
	}}
	envelope := Envelope{SpecVersion: "1.0", ID: "evt-1", Source: "/test/orders", Type: "orders.created", Attempt: 1}
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	settler := &dispatchSettler{}
	message := driver.InboundMessage{Destination: "f1.test.orders.created.medium", Headers: headerSlice(headers), Body: []byte("oversize"), Settle: settler}
	abandoned := false
	if !dispatchMessage(runner, context.Background(), message, &Envelope{}, &abandoned, &deliveryState{}) {
		t.Fatal("oversize message was not settled")
	}
	if handled {
		t.Fatal("handler ran for an oversize body")
	}
	if !settler.acked {
		t.Fatal("oversize message was not acked after DLQ publish")
	}
	if len(producer.messages) != 1 || headerValue(producer.messages[0].Headers, "f1deathreason") != ReasonDecode.String() {
		t.Fatalf("DLQ messages = %#v, want one decode message", producer.messages)
	}
}

func TestDispatchSettlesMessagesWithoutDeliveryCount(t *testing.T) {
	producer := &dispatchProducer{}
	conn := &dispatchConn{producer: producer}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()
	runner := &Runner{client: client, subscription: Subscription{
		Name:       "orders",
		Topics:     []string{"orders.created", "payments.created"},
		Priorities: []Priority{PriorityHigh, PriorityMedium},
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error { return nil }),
		},
	}}
	settler := &dispatchSettler{}
	envelope := Envelope{SpecVersion: "1.0", ID: "evt-sampled", Source: "/test/orders", Type: "orders.created", Priority: PriorityHigh, Attempt: 5}
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	message := driver.InboundMessage{
		Destination:   "f1.test.orders.created.medium",
		Headers:       headerSlice(headers),
		Body:          []byte(`{}`),
		DeliveryCount: 1,
		Settle:        settler,
	}
	abandoned := false
	if !dispatchMessage(runner, context.Background(), message, &Envelope{}, &abandoned, &deliveryState{}) {
		t.Fatal("message was not settled")
	}
	if !settler.acked {
		t.Fatal("message was not acked")
	}
	noCount := message
	noCount.DeliveryCount = -1
	noCountSettler := &dispatchSettler{}
	noCount.Settle = noCountSettler
	if !dispatchMessage(runner, context.Background(), noCount, &Envelope{}, &abandoned, &deliveryState{}) {
		t.Fatal("message without delivery count was not settled")
	}
	if !noCountSettler.acked {
		t.Fatal("message without delivery count was not acked")
	}
}

func TestTerminalNotificationsCannotBlockSettlement(t *testing.T) {
	tests := []struct {
		name   string
		settle func(*testing.T, func(context.Context), chan<- bool)
	}{
		{
			name: "dead-letter",
			settle: func(t *testing.T, callback func(context.Context), result chan<- bool) {
				producer := &dispatchProducer{}
				client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: &dispatchConn{producer: producer}}))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = client.Close(context.Background()) })
				runner := &Runner{client: client, asyncGroup: new(errgroup.Group), subscription: Subscription{
					Name:         "orders",
					OnDeadLetter: func(ctx context.Context, _ DeadLettered) { callback(ctx) },
				}}
				envelope := Envelope{SpecVersion: "1.0", ID: "evt-blocking", Source: "/test/orders", Type: "orders.created", Attempt: 1}
				go func() {
					result <- deadLetterAndSettle(runner, context.Background(), driver.InboundMessage{Destination: "orders", Settle: &dispatchSettler{}}, envelope, ReasonTerminal, errors.New("bad request"), &deliveryState{})
				}()
			},
		},
		{
			name: "discarded",
			settle: func(t *testing.T, callback func(context.Context), result chan<- bool) {
				client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: &dispatchConn{producer: &dispatchProducer{}}}))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = client.Close(context.Background()) })
				runner := &Runner{client: client, asyncGroup: new(errgroup.Group), subscription: Subscription{
					Name:        "orders",
					OnDiscarded: func(ctx context.Context, _ Discarded) { callback(ctx) },
				}}
				envelope := Envelope{SpecVersion: "1.0", ID: "evt-discarded", Source: "/test/orders", Type: "orders.unmatched", Attempt: 1}
				headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
				if err != nil {
					t.Fatal(err)
				}
				message := driver.InboundMessage{Destination: "orders", Headers: headerSlice(headers), Body: []byte(`{}`), Settle: &dispatchSettler{}}
				go func() {
					result <- dispatchMessage(runner, context.Background(), message, &Envelope{}, new(bool), &deliveryState{})
				}()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			finished := make(chan struct{})
			var releaseOnce sync.Once
			releaseCallback := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(releaseCallback)
			callback := func(context.Context) {
				close(entered)
				<-release
				close(finished)
			}
			result := make(chan bool, 1)
			test.settle(t, callback, result)

			startedGuard := clock.NewReal().Timer(time.Second)
			select {
			case <-entered:
				startedGuard.Stop()
			case <-startedGuard.C:
				t.Fatal("terminal notification did not start")
			}
			settledGuard := clock.NewReal().Timer(time.Second)
			select {
			case settled := <-result:
				settledGuard.Stop()
				if !settled {
					t.Fatal("terminal settlement failed")
				}
			case <-settledGuard.C:
				t.Fatal("blocking terminal notification prevented settlement")
			}
			select {
			case <-finished:
				t.Fatal("terminal notification returned before its release")
			default:
			}

			releaseCallback()
			finishedGuard := clock.NewReal().Timer(time.Second)
			defer finishedGuard.Stop()
			select {
			case <-finished:
			case <-finishedGuard.C:
				t.Fatal("terminal notification did not finish after release")
			}
		})
	}
}

func TestNonCooperativeHandlerIsReportedAsStuck(t *testing.T) {
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: &dispatchConn{producer: &dispatchProducer{}}}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()
	release := make(chan struct{})
	runner := &Runner{client: client, subscription: Subscription{Name: "orders", HandlerTimeout: 10 * time.Millisecond}}
	result := invokeHandler(runner, context.Background(), HandlerFunc(func(context.Context, *Event) error {
		<-release
		return nil
	}), &Event{}, "", 0)
	close(release)
	if !result.stuck {
		t.Fatalf("invokeHandler() = %#v, want stuck result", result)
	}
}

func TestInvokeHandlerReturnsStuckForCancelledParent(t *testing.T) {
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: &dispatchConn{producer: &dispatchProducer{}}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	handled := false
	runner := &Runner{
		client:       client,
		group:        new(errgroup.Group),
		subscription: Subscription{Name: "orders", HandlerTimeout: time.Second},
	}
	parent, cancel := context.WithCancel(context.Background())
	cancel()

	result := invokeHandler(runner, parent, HandlerFunc(func(ctx context.Context, _ *Event) error {
		handled = true
		return ctx.Err()
	}), &Event{}, "", 0)
	if err := runner.group.Wait(); err != nil {
		t.Fatalf("handler group wait = %v", err)
	}
	if !result.stuck {
		t.Fatalf("invokeHandler() = %#v, want stuck result for a cancelled parent", result)
	}
	if handled {
		t.Fatal("invokeHandler() launched a handler for a cancelled parent")
	}
}

func TestInvokeHandlerReportsSequentialStuckThresholds(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	var output logSink
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&dispatchDriver{conn: &dispatchConn{producer: &dispatchProducer{}}}),
		withClock(fake),
		WithLogger(slog.New(slog.NewTextHandler(&output, nil))),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	const timeout = 10 * time.Millisecond
	release := make(chan struct{})
	exited := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		releaseHandler()
		select {
		case <-exited:
		case <-clock.NewReal().Timer(time.Second).C:
			t.Error("stuck handler did not exit after release")
		}
	})

	runner := &Runner{
		client: client,
		subscription: Subscription{
			Name:           "orders",
			HandlerTimeout: timeout,
		},
	}
	result := make(chan handlerResult, 1)
	go func() {
		result <- invokeHandler(runner, context.Background(), HandlerFunc(func(context.Context, *Event) error {
			defer close(exited)
			<-release
			return nil
		}), &Event{}, "", 0)
	}()

	fake.BlockUntil(1)
	select {
	case got := <-result:
		t.Fatalf("invokeHandler() returned before first stuck phase: %#v", got)
	default:
	}

	fake.Advance(2 * timeout)
	fake.BlockUntil(1)
	if got := strings.Count(output.String(), "f1 stuck worker"); got != 1 {
		t.Fatalf("stuck warning count after first phase = %d, want 1; output=%q", got, output.String())
	}
	select {
	case got := <-result:
		t.Fatalf("invokeHandler() returned after first stuck phase: %#v", got)
	default:
	}

	fake.Advance(2 * timeout)
	select {
	case got := <-result:
		if !got.stuck {
			t.Fatalf("invokeHandler() result = %#v, want stuck", got)
		}
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("invokeHandler() did not return after cumulative 4x threshold")
	}
	if got := strings.Count(output.String(), "f1 worker exceeded stuck threshold"); got != 1 {
		t.Fatalf("stuck error count after second phase = %d, want 1; output=%q", got, output.String())
	}
	releaseHandler()
	<-exited
}

func TestProducerAttemptCapDoesNotCreateZeroRetryTier(t *testing.T) {
	producer := &dispatchProducer{}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: &dispatchConn{producer: producer}}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()
	runner := &Runner{client: client, subscription: Subscription{
		Name:  "orders",
		Retry: RetryConfig{MaxAttempts: 1},
	}}
	settler := &dispatchSettler{}
	envelope := Envelope{SpecVersion: "1.0", ID: "evt-cap", Source: "/test/orders", Type: "orders.created", Attempt: 1, MaxAttempts: 3}
	if !retryAndSettle(runner, context.Background(), driver.InboundMessage{Destination: "orders", Settle: settler}, envelope, errors.New("temporary"), &deliveryState{}) {
		t.Fatal("producer-capped event was not settled")
	}
	if len(producer.messages) != 1 || !strings.Contains(producer.messages[0].Destination, ".dlq.") || strings.Contains(producer.messages[0].Destination, ".retry.0") {
		t.Fatalf("successor destination = %q, want DLQ and no retry.0", producer.messages[0].Destination)
	}
}

func TestRunnerRunRejectedWhileClientClosing(t *testing.T) {
	consumer := newDispatchConsumer()
	conn := &dispatchConn{producer: &dispatchProducer{}, consumer: consumer, closeStarted: make(chan struct{}), closeRelease: make(chan struct{})}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	runner := &Runner{client: client, subscription: Subscription{
		Name: "orders", Topics: []string{"orders.created"}, Concurrency: 1, Prefetch: 1,
		Priorities: []Priority{PriorityMedium}, Retry: RetryConfig{MaxAttempts: 1}, HandlerTimeout: time.Second,
	}}
	client.mu.Lock()
	client.runners[runner] = struct{}{}
	client.mu.Unlock()
	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(context.Background()) }()
	select {
	case <-conn.closeStarted:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("Close did not reach the connection")
	}
	close(conn.closeRelease)
	if err := runner.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "closing") {
		t.Fatalf("Run() error = %v, want client-closing rejection", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
}

func TestRunnerDispatchesHandlerAndDrains(t *testing.T) {
	consumer := newDispatchConsumer()
	conn := &dispatchConn{producer: &dispatchProducer{}, consumer: consumer, admin: &dispatchAdmin{}}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()
	settled := make(chan struct{})
	settler := &dispatchSettler{onSettle: func() { close(settled) }}
	envelope := Envelope{SpecVersion: "1.0", ID: "evt-3", Source: "/test/orders", Type: "orders.created", Attempt: 1}
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	consumer.messages <- driver.InboundMessage{Destination: "f1.test.orders.created.medium", Headers: headerSlice(headers), Body: []byte(`{}`), Settle: settler}
	runner := &Runner{client: client, subscription: Subscription{
		Name: "orders", Topics: []string{"orders.created"}, Concurrency: 1, Prefetch: 1,
		Priorities: []Priority{PriorityMedium}, Retry: RetryConfig{MaxAttempts: 1}, HandlerTimeout: time.Second,
		Handlers: map[string]Handler{"orders.created": HandlerFunc(func(context.Context, *Event) error { return nil })},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	select {
	case <-settled:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("handler did not settle message")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("Run() did not drain and stop")
	}
}

func headerValue(headers []driver.Header, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

type dispatchDriver struct{ conn *dispatchConn }

func (*dispatchDriver) Name() string                      { return "inmem" }
func (*dispatchDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (d *dispatchDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return d.conn, nil
}

type dispatchConn struct {
	mu               sync.Mutex
	producer         *dispatchProducer
	producerOverride driver.Producer
	consumer         *dispatchConsumer
	generationCtx    context.Context
	admin            driver.Admin
	closed           bool
	closeCalls       int
	closeStarted     chan struct{}
	closeRelease     chan struct{}
}

func (*dispatchConn) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (*dispatchConn) BrokerInfo() driver.BrokerInfo     { return driver.BrokerInfo{Kind: "test"} }
func (c *dispatchConn) Producer(context.Context, driver.ProducerConfig) (driver.Producer, error) {
	if c.producerOverride != nil {
		return c.producerOverride, nil
	}
	return c.producer, nil
}

func (c *dispatchConn) Consumer(ctx context.Context, _ driver.ConsumerConfig) (driver.Consumer, error) {
	if c.consumer == nil {
		return nil, errors.New("consumer not used by direct dispatch test")
	}
	c.mu.Lock()
	c.generationCtx = ctx
	c.mu.Unlock()
	return c.consumer, nil
}
func (c *dispatchConn) Admin() driver.Admin      { return c.admin }
func (*dispatchConn) Ping(context.Context) error { return nil }
func (c *dispatchConn) Close(context.Context) error {
	c.mu.Lock()
	c.closed = true
	c.closeCalls++
	started := c.closeStarted
	release := c.closeRelease
	c.mu.Unlock()
	if started != nil {
		close(started)
	}
	if release != nil {
		<-release
	}
	return nil
}

func (c *dispatchConn) closeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeCalls
}

// dispatchAdmin is a no-op driver.Admin: EnsureTopology succeeds trivially,
// matching what every real driver in this repo provides, so dispatch tests
// that are not exercising topology behavior are not tripped by the consumer
// path now requiring a non-nil Admin under a non-TopologyNone policy.
type dispatchAdmin struct{}

func (*dispatchAdmin) EnsureTopology(context.Context, driver.TopologySpec) (driver.TopologyDiff, error) {
	return driver.TopologyDiff{}, nil
}

func (*dispatchAdmin) DescribeTopology(context.Context, []string) (driver.TopologyState, error) {
	return driver.TopologyState{}, driver.ErrUnsupported
}

func (*dispatchAdmin) Purge(context.Context, string) (int64, error) { return 0, driver.ErrUnsupported }

func (*dispatchAdmin) Prune(context.Context, []string) ([]driver.PruneResult, error) {
	return nil, driver.ErrUnsupported
}

type dispatchProducer struct {
	mu         sync.Mutex
	messages   []driver.OutboundMessage
	closed     bool
	closeCalls int
}

func (p *dispatchProducer) Publish(_ context.Context, messages ...driver.OutboundMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, message := range messages {
		message.Headers = append([]driver.Header(nil), message.Headers...)
		p.messages = append(p.messages, message)
	}
	return nil
}

func (p *dispatchProducer) Close(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.closeCalls++
	return nil
}

func (p *dispatchProducer) closeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closeCalls
}

type dispatchSettler struct {
	mu       sync.Mutex
	acked    bool
	nacked   bool
	onSettle func()
}

func (s *dispatchSettler) Ack(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acked = true
	if s.onSettle != nil {
		s.onSettle()
	}
	return nil
}

func (s *dispatchSettler) Nack(context.Context, driver.NackOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nacked = true
	if s.onSettle != nil {
		s.onSettle()
	}
	return nil
}

type dispatchConsumer struct {
	mu               sync.Mutex
	messages         chan driver.InboundMessage
	errs             chan error
	drained          bool
	stopped          bool
	released         bool
	open             int
	outstanding      []driver.InboundMessage
	releasedMessages chan driver.InboundMessage
}

func newDispatchConsumer() *dispatchConsumer {
	return &dispatchConsumer{messages: make(chan driver.InboundMessage, 1), errs: make(chan error, 1)}
}

func (c *dispatchConsumer) Messages() <-chan driver.InboundMessage { return c.messages }
func (c *dispatchConsumer) Errors() <-chan error                   { return c.errs }
func (*dispatchConsumer) Pause(...string) error                    { return nil }
func (*dispatchConsumer) Resume(...string) error                   { return nil }
func (c *dispatchConsumer) Drain(context.Context) error {
	c.mu.Lock()
	c.drained = true
	c.mu.Unlock()
	return nil
}

func (c *dispatchConsumer) Stop(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.open != 0 {
		return errors.New("outstanding dispatch message")
	}
	if !c.stopped {
		c.stopped = true
		close(c.messages)
		close(c.errs)
	}
	return nil
}

func (c *dispatchConsumer) Release(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.stopped {
		c.released = true
		if c.releasedMessages != nil {
			for _, message := range c.outstanding {
				c.releasedMessages <- message
			}
		}
		c.outstanding = nil
		c.open = 0
		c.stopped = true
		close(c.messages)
		close(c.errs)
	}
	return nil
}

func TestDispatchConsumerReleaseReturnsOutstandingDeliveries(t *testing.T) {
	consumer := newDispatchConsumer()
	consumer.open = 1

	if err := consumer.Release(context.Background()); err != nil {
		t.Fatalf("Release() = %v, want nil with an outstanding delivery", err)
	}
	if !consumer.released {
		t.Fatal("Release() did not record released=true")
	}
	if !consumer.stopped {
		t.Fatal("Release() did not close the consumer")
	}
	if consumer.open != 0 {
		t.Fatalf("Release() left %d outstanding deliveries, want 0", consumer.open)
	}
}

func (*dispatchConsumer) Lag(context.Context) (map[string]int64, error) {
	return nil, driver.ErrUnsupported
}

// TestHandlerDeadlineIsRetriedAndPanicIsDeadLettered pins the two handler
// results that reach settlement without a classification of their own. A
// handler error that is not terminal, dropped, or delay-carrying takes the
// retry path, so a handler deadline publishes a retry copy and settles the
// original without dead-lettering it. A handler panic never reaches
// classification: it is dead-lettered first, and it must stay that way.
func TestHandlerDeadlineIsRetriedAndPanicIsDeadLettered(t *testing.T) {
	// Each case builds its own runner and producer, so a case run alone sees
	// only its own publishes.
	newRunner := func(t *testing.T) (*dispatchProducer, *Runner) {
		t.Helper()
		producer := &dispatchProducer{}
		client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: &dispatchConn{producer: producer}}))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close(context.Background()) })
		return producer, &Runner{
			client: client,
			subscription: Subscription{
				Name:           "orders",
				Topics:         []string{"orders.created"},
				Priorities:     []Priority{PriorityHigh},
				Retry:          RetryConfig{MaxAttempts: 2, InitialInterval: time.Second},
				HandlerTimeout: time.Second,
			},
			inflight: newInflightRegistry(),
		}
	}
	envelope := func(id string) Envelope {
		return Envelope{SpecVersion: "1.0", ID: id, Source: "/test/orders", Type: "orders.created.v1", Priority: PriorityHigh, Attempt: 1}
	}

	t.Run("deadline is retried", func(t *testing.T) {
		producer, runner := newRunner(t)
		deadlineSettler := &dispatchSettler{}
		deadlineMessage := retryBridgeMessage(t, envelope("handler-deadline"), deadlineSettler)
		runner.subscription.Handlers = map[string]Handler{
			"orders.created.v1": HandlerFunc(func(context.Context, *Event) error {
				return context.DeadlineExceeded
			}),
		}
		deadlineID := runner.inflight.Add()
		processDelivery(runner, context.Background(), delivery{id: deadlineID, message: deadlineMessage})

		if got := len(producer.messages); got != 1 {
			t.Fatalf("publishes after a handler deadline = %d, want one retry copy and no dead letter", got)
		}
		if destination := producer.messages[0].Destination; !strings.Contains(destination, "."+retryDestinationSegment+".") || strings.Contains(destination, ".dlq.") {
			t.Fatalf("deadline successor destination = %q, want the retry destination and no dead letter", destination)
		}
		if !deadlineSettler.acked || deadlineSettler.nacked {
			t.Fatalf("deadline settlement = acked %t nacked %t, want the original acked once its retry copy was published", deadlineSettler.acked, deadlineSettler.nacked)
		}
	})

	t.Run("panic is dead-lettered", func(t *testing.T) {
		producer, runner := newRunner(t)
		panicSettler := &dispatchSettler{}
		panicMessage := retryBridgeMessage(t, envelope("handler-panic"), panicSettler)
		runner.subscription.Handlers = map[string]Handler{
			"orders.created.v1": HandlerFunc(func(context.Context, *Event) error {
				panic("handler exploded")
			}),
		}
		panicID := runner.inflight.Add()
		processDelivery(runner, context.Background(), delivery{id: panicID, message: panicMessage})

		if got := len(producer.messages); got != 1 {
			t.Fatalf("publishes after a handler panic = %d, want one dead letter and no retry copy", got)
		}
		deadLettered := producer.messages[0]
		if !strings.Contains(deadLettered.Destination, ".dlq.") {
			t.Fatalf("panic successor destination = %q, want the dead-letter destination", deadLettered.Destination)
		}
		if reason := headerValue(deadLettered.Headers, "f1deathreason"); reason != ReasonPanic.String() {
			t.Fatalf("panic death reason = %q, want %q", reason, ReasonPanic.String())
		}
		if !panicSettler.acked {
			t.Fatal("panic delivery was not acked after its dead letter was published")
		}
	})
}

func TestMalformedHeadersDeadLetterAsDecodeBeforeHandlerRuns(t *testing.T) {
	producer := &dispatchProducer{}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: &dispatchConn{producer: producer}}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()
	handled := false
	runner := &Runner{client: client, subscription: Subscription{
		Name:           "orders",
		HandlerTimeout: time.Second,
		Handlers: map[string]Handler{"orders.created": HandlerFunc(func(context.Context, *Event) error {
			handled = true
			return nil
		})},
	}}
	settler := &dispatchSettler{}
	message := driver.InboundMessage{
		Destination: "f1.test.orders.created.medium",
		Headers: headerSlice(map[string]string{
			"specversion": "1.0",
			"source":      "/test/orders",
			"type":        "orders.created",
		}),
		Body:   []byte(`{"id":"evt-1"}`),
		Settle: settler,
	}
	if !dispatchMessage(runner, context.Background(), message, &Envelope{}, new(bool), &deliveryState{}) {
		t.Fatal("malformed message was not settled")
	}
	if handled {
		t.Fatal("handler ran for a malformed envelope")
	}
	if !settler.acked {
		t.Fatal("malformed message was not acked after DLQ publish")
	}
	if len(producer.messages) != 1 || headerValue(producer.messages[0].Headers, "f1deathreason") != ReasonDecode.String() {
		t.Fatalf("DLQ messages = %#v, want one decode message", producer.messages)
	}
}

// invokeHandler runs handler as one delivery of destination with deliveryCount.
func invokeHandler(r *Runner, parent context.Context, handler Handler, event *Event, destination string, deliveryCount int) handlerResult {
	return invokeHandlerMessage(r, parent, handler, event, driver.InboundMessage{
		Destination:   destination,
		DeliveryCount: deliveryCount,
	})
}
