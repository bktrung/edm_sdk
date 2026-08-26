package f1

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"

	"golang.org/x/sync/errgroup"
)

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
	message := driver.InboundMessage{Destination: "f1.test.orders.created.normal", Headers: headerSlice(headers), Body: []byte("oversize"), Settle: settler}
	abandoned := false
	if !dispatchMessage(runner, context.Background(), message, &Envelope{}, &abandoned) {
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
		Priorities: []Priority{PriorityHigh, PriorityNormal},
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
		Destination:   "f1.test.orders.created.normal",
		Headers:       headerSlice(headers),
		Body:          []byte(`{}`),
		DeliveryCount: 1,
		Settle:        settler,
	}
	abandoned := false
	if !dispatchMessage(runner, context.Background(), message, &Envelope{}, &abandoned) {
		t.Fatal("message was not settled")
	}
	if !settler.acked {
		t.Fatal("message was not acked")
	}
	noCount := message
	noCount.DeliveryCount = -1
	noCountSettler := &dispatchSettler{}
	noCount.Settle = noCountSettler
	if !dispatchMessage(runner, context.Background(), noCount, &Envelope{}, &abandoned) {
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
					result <- deadLetterAndSettle(runner, context.Background(), driver.InboundMessage{Destination: "orders", Settle: &dispatchSettler{}}, envelope, ReasonTerminal, errors.New("bad request"))
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
				go func() { result <- dispatchMessage(runner, context.Background(), message, &Envelope{}, new(bool)) }()
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
	}), &Event{})
	close(release)
	if !result.stuck {
		t.Fatalf("invokeHandler() = %#v, want stuck result", result)
	}
}

func TestInvokeHandlerReportsSequentialStuckThresholds(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	var output bytes.Buffer
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&dispatchDriver{conn: &dispatchConn{producer: &dispatchProducer{}}}),
		WithClock(fake),
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
		}), &Event{})
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
	if !retryAndSettle(runner, context.Background(), driver.InboundMessage{Destination: "orders", Settle: settler}, envelope, errors.New("temporary")) {
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
		Priorities: []Priority{PriorityNormal}, Retry: RetryConfig{MaxAttempts: 1}, HandlerTimeout: time.Second,
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
	consumer.messages <- driver.InboundMessage{Destination: "f1.test.orders.created.normal", Headers: headerSlice(headers), Body: []byte(`{}`), Settle: settler}
	runner := &Runner{client: client, subscription: Subscription{
		Name: "orders", Topics: []string{"orders.created"}, Concurrency: 1, Prefetch: 1,
		Priorities: []Priority{PriorityNormal}, Retry: RetryConfig{MaxAttempts: 1}, HandlerTimeout: time.Second,
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
	mu           sync.Mutex
	producer     *dispatchProducer
	consumer     *dispatchConsumer
	admin        driver.Admin
	closed       bool
	closeCalls   int
	closeStarted chan struct{}
	closeRelease chan struct{}
}

func (*dispatchConn) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (*dispatchConn) BrokerInfo() driver.BrokerInfo     { return driver.BrokerInfo{Kind: "test"} }
func (c *dispatchConn) Producer(context.Context, driver.ProducerConfig) (driver.Producer, error) {
	return c.producer, nil
}

func (c *dispatchConn) Consumer(context.Context, driver.ConsumerConfig) (driver.Consumer, error) {
	if c.consumer == nil {
		return nil, errors.New("consumer not used by direct dispatch test")
	}
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
func (*dispatchProducer) Flush(context.Context) error { return nil }
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

func waitDispatchSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	timer := clock.NewReal().Timer(time.Second)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatal(message)
	}
}

func TestRunnerAccountingAxesSumAfterMixedOutcomes(t *testing.T) {
	producer := &dispatchProducer{}
	conn := &dispatchConn{producer: producer}
	cfg := testClientConfig(t)
	client, err := New(context.Background(), cfg, WithDriver(&dispatchDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()
	runner := &Runner{
		client: client,
		subscription: Subscription{
			Name: "orders", Retry: RetryConfig{MaxAttempts: 2, InitialInterval: time.Second},
			HandlerTimeout: time.Second,
		},
		inflight: newInflightRegistry(),
	}
	runner.accounting = lifecycle.NewAccounting(runner.inflight.registry)
	message := func(id string, settler driver.Settler) driver.InboundMessage {
		envelope := Envelope{SpecVersion: "1.0", ID: id, Source: "/test/orders", Type: "orders.created", Priority: PriorityNormal, Attempt: 1}
		headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
		if err != nil {
			t.Fatal(err)
		}
		return driver.InboundMessage{Destination: "f1.test.orders.created.normal", Headers: headerSlice(headers), Body: []byte(`{}`), DeliveryCount: 1, Settle: settler}
	}
	process := func(id string, handler Handler, ctx context.Context) {
		runner.subscription.Handlers = map[string]Handler{"orders.created": handler}
		item := message(id, &dispatchSettler{})
		itemID := runner.inflight.Add(item)
		processDelivery(runner, ctx, delivery{id: itemID, message: item})
	}
	process("retry", HandlerFunc(func(context.Context, *Event) error { return errors.New("temporary") }), context.Background())
	process("dead-letter", HandlerFunc(func(context.Context, *Event) error { return Terminal(errors.New("invalid")) }), context.Background())
	requeueCtx, cancel := context.WithCancel(context.Background())
	cancel()
	process("requeue", HandlerFunc(func(ctx context.Context, _ *Event) error {
		<-ctx.Done()
		return ctx.Err()
	}), requeueCtx)
	settlement := runner.inflight.Counts()
	disposition := runner.inflight.DispositionCounts()
	if settlement.received != 3 || settlement.settled+settlement.requeued+settlement.unknown+settlement.abandoned != 3 {
		t.Fatalf("settlement counts = %#v, want three received and terminal outcomes", settlement)
	}
	if disposition.Total() != 3 || disposition.Retried != 1 || disposition.DeadLettered != 1 || disposition.Requeued != 1 {
		t.Fatalf("disposition counts = %#v, want one retry, dead-letter, and requeue", disposition)
	}
	if got := runner.accounting.Snapshot(); got != disposition {
		t.Fatalf("read-only accounting = %#v, registry dispositions = %#v", got, disposition)
	}
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
		Destination: "f1.test.orders.created.normal",
		Headers: headerSlice(map[string]string{
			"specversion": "1.0",
			"source":      "/test/orders",
			"type":        "orders.created",
		}),
		Body:   []byte(`{"id":"evt-1"}`),
		Settle: settler,
	}
	if !dispatchMessage(runner, context.Background(), message, &Envelope{}, new(bool)) {
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
