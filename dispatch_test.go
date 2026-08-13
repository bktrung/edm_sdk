package f1

import (
	"context"
	"errors"
	"hash/fnv"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
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

func TestDispatchSamplesAttemptDivergenceBeforeSettlement(t *testing.T) {
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
	}, metrics: newDeliveryMetrics([]string{"orders.created", "payments.created"})}
	settler := &dispatchSettler{onSettle: func() {
		if got := runner.metrics.sampleAttemptDivergence("orders.created", PriorityHigh); got != 4 {
			t.Errorf("sampled divergence at settlement = %d, want 4", got)
		}
	}}
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
	if got := runner.metrics.sampleAttemptDivergence("orders.created", PriorityHigh); got != 0 {
		t.Fatalf("sampled divergence after reset = %d, want 0", got)
	}
	if got := runner.metrics.sampleAttemptDivergence("payments.created", PriorityHigh); got != 0 {
		t.Fatalf("unobserved lane divergence = %d, want 0", got)
	}
	noCount := message
	noCount.DeliveryCount = -1
	noCount.Settle = &dispatchSettler{}
	if !dispatchMessage(runner, context.Background(), noCount, &Envelope{}, &abandoned) {
		t.Fatal("message without delivery count was not settled")
	}
	if got := runner.metrics.sampleAttemptDivergence("orders.created", PriorityHigh); got != 0 {
		t.Fatalf("unavailable delivery count divergence = %d, want 0", got)
	}
}

func TestDeliveryMetricsKeepTopicAndPriorityLanesSeparate(t *testing.T) {
	metrics := newDeliveryMetrics([]string{"payments.created", "orders.created"})
	metrics.observeAttemptDivergence("orders.created", PriorityHigh, 4)
	metrics.observeAttemptDivergence("orders.created", PriorityNormal, 2)
	metrics.observeAttemptDivergence("payments.created", PriorityNormal, 7)

	if got := metrics.sampleAttemptDivergence("orders.created", PriorityHigh); got != 4 {
		t.Fatalf("orders high divergence = %d, want 4", got)
	}
	if got := metrics.sampleAttemptDivergence("orders.created", PriorityNormal); got != 2 {
		t.Fatalf("orders normal divergence = %d, want 2", got)
	}
	if got := metrics.sampleAttemptDivergence("payments.created", PriorityNormal); got != 7 {
		t.Fatalf("payments normal divergence = %d, want 7", got)
	}
}

func TestDeliveryMetricObservationDoesNotAllocate(t *testing.T) {
	metrics := newDeliveryMetrics([]string{"orders.created"})
	if allocs := testing.AllocsPerRun(1000, func() {
		metrics.observeAttemptDivergence("orders.created", PriorityHigh, 4)
	}); allocs != 0 {
		t.Fatalf("delivery metric observation allocations = %v, want 0", allocs)
	}
}

func TestSettleLastNotificationRunsBeforeAck(t *testing.T) {
	producer := &dispatchProducer{}
	conn := &dispatchConn{producer: producer}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()
	settler := &dispatchSettler{}
	observed := make(chan bool, 1)
	runner := &Runner{client: client, subscription: Subscription{
		Name:           "orders",
		HandlerTimeout: time.Second,
		OnDeadLetter: func(ctx context.Context, _ DeadLettered) {
			_, hasDeadline := ctx.Deadline()
			observed <- !settler.acked && ctx.Err() == nil && hasDeadline
		},
	}}
	envelope := Envelope{SpecVersion: "1.0", ID: "evt-2", Source: "/test/orders", Type: "orders.created", Attempt: 1}
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	message := driver.InboundMessage{Destination: "f1.test.orders.created.normal", Headers: headerSlice(headers), Body: []byte(`{}`), Settle: settler}
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	if !deadLetterAndSettle(runner, parent, message, envelope, ReasonTerminal, errors.New("bad request")) {
		t.Fatal("terminal message was not settled")
	}
	if got := <-observed; !got {
		t.Fatal("notification ran after settlement")
	}
	if !settler.acked {
		t.Fatal("terminal message was not acked")
	}
}

func TestBlockingTerminalNotificationStillSettles(t *testing.T) {
	producer := &dispatchProducer{}
	conn := &dispatchConn{producer: producer}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()
	settler := &dispatchSettler{}
	entered := make(chan struct{})
	release := make(chan struct{})
	runner := &Runner{client: client, subscription: Subscription{
		Name: "orders",
		OnDeadLetter: func(context.Context, DeadLettered) {
			close(entered)
			<-release
		},
	}}
	envelope := Envelope{SpecVersion: "1.0", ID: "evt-blocking", Source: "/test/orders", Type: "orders.created", Attempt: 1}
	result := make(chan bool, 1)
	go func() {
		result <- deadLetterAndSettle(runner, context.Background(), driver.InboundMessage{Destination: "orders", Settle: settler}, envelope, ReasonTerminal, errors.New("bad request"))
	}()
	select {
	case <-entered:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("terminal notification did not start")
	}
	select {
	case settled := <-result:
		if !settled || !settler.acked {
			t.Fatalf("deadLetterAndSettle() = %v, acked = %v; want settlement despite callback", settled, settler.acked)
		}
	case <-clock.NewReal().Timer(1500 * time.Millisecond).C:
		t.Fatal("blocking terminal notification prevented settlement")
	}
	close(release)
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
	if !retryAndSettle(runner, context.Background(), driver.InboundMessage{Destination: "orders", Settle: settler}, envelope, errors.New("temporary"), 0, true) {
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
	conn := &dispatchConn{producer: &dispatchProducer{}, consumer: consumer}
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
	closed       bool
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
func (*dispatchConn) Admin() driver.Admin        { return nil }
func (*dispatchConn) Ping(context.Context) error { return nil }
func (c *dispatchConn) Close(context.Context) error {
	c.mu.Lock()
	c.closed = true
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

type dispatchProducer struct {
	mu       sync.Mutex
	messages []driver.OutboundMessage
	closed   bool
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
func (*dispatchProducer) Flush(context.Context) error   { return nil }
func (p *dispatchProducer) Close(context.Context) error { p.closed = true; return nil }

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
	mu       sync.Mutex
	messages chan driver.InboundMessage
	errs     chan error
	drained  bool
	stopped  bool
	open     int
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

func (*dispatchConsumer) Lag(context.Context) (map[string]int64, error) {
	return nil, driver.ErrUnsupported
}

func TestRunnerEmulatesOrderedByKeyWithConcurrentWorkers(t *testing.T) {
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	otherStarted := make(chan struct{})
	release := make(chan struct{})
	var orderMu sync.Mutex
	var sameIDs []string
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	consumer := newDispatchConsumer()
	conn := &dispatchConn{producer: &dispatchProducer{}, consumer: consumer}
	cfg := testClientConfig(t)
	cfg.Lifecycle.DrainTimeout = 10 * time.Millisecond
	cfg.Lifecycle.HandlerGrace = 5 * time.Millisecond
	fake := clock.NewFake(time.Unix(0, 0))
	client, err := New(context.Background(), cfg, WithDriver(&dispatchDriver{conn: conn}), WithClock(fake))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Mode:           OrderedByKey,
		Concurrency:    2,
		Prefetch:       2,
		Priorities:     []Priority{PriorityNormal},
		Retry:          RetryConfig{MaxAttempts: 1},
		MaxDeferrals:   1,
		HandlerTimeout: time.Millisecond,
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(_ context.Context, event *Event) error {
				switch event.ID() {
				case "same-1":
					close(firstStarted)
					<-release
				case "same-2":
					close(secondStarted)
				case "other":
					close(otherStarted)
				}
				if event.ID() == "same-1" || event.ID() == "same-2" {
					orderMu.Lock()
					sameIDs = append(sameIDs, event.ID())
					orderMu.Unlock()
				}
				return nil
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if client.effective.OrderedByKey {
		t.Fatal("test driver unexpectedly provides native ordering")
	}
	sameKey, otherKey := distinctDispatchKeys(2)
	defer releaseHandler()
	settled := make(chan struct{}, 3)
	message := func(id string, key []byte) driver.InboundMessage {
		envelope := Envelope{SpecVersion: "1.0", ID: id, Source: "/test/orders", Type: "orders.created", Attempt: 1}
		headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
		if err != nil {
			t.Fatal(err)
		}
		return driver.InboundMessage{Destination: "f1.test.orders.created.normal", Key: key, Headers: headerSlice(headers), Body: []byte(`{}`), DeliveryCount: 1, Settle: &dispatchSettler{onSettle: func() { settled <- struct{}{} }}}
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(context.Background()) }()
	consumer.messages <- message("same-1", sameKey)
	consumer.messages <- message("same-2", sameKey)
	consumer.messages <- message("other", otherKey)
	waitDispatchSignal(t, firstStarted, "same-key handler did not start")
	waitDispatchSignal(t, otherStarted, "different-key handler did not run concurrently")
	select {
	case <-secondStarted:
		t.Fatal("equal keys ran concurrently")
	default:
	}
	releaseHandler()
	waitDispatchSignal(t, secondStarted, "same-key successor did not run")
	for i := 0; i < 3; i++ {
		waitDispatchSignal(t, settled, "message did not settle")
	}
	drainDone := make(chan error, 1)
	go func() { drainDone <- runner.Drain(context.Background()) }()
	var drainErr error
	for i := 0; i < 100; i++ {
		fake.Advance(time.Millisecond)
		runtime.Gosched()
		select {
		case drainErr = <-drainDone:
			i = 100
		default:
		}
	}
	if drainErr != nil {
		t.Fatal(drainErr)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	orderMu.Lock()
	defer orderMu.Unlock()
	if len(sameIDs) != 2 || sameIDs[0] != "same-1" || sameIDs[1] != "same-2" {
		t.Fatalf("same-key order = %v, want [same-1 same-2]", sameIDs)
	}
}

func distinctDispatchKeys(concurrency uint32) ([]byte, []byte) {
	first := []byte{0}
	firstHash := fnv.New32a()
	_, _ = firstHash.Write(first)
	for value := 1; value < 256; value++ {
		candidate := []byte{byte(value)}
		hash := fnv.New32a()
		_, _ = hash.Write(candidate)
		if hash.Sum32()%concurrency != firstHash.Sum32()%concurrency {
			return first, candidate
		}
	}
	return first, []byte{1}
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
			Name: "orders", Retry: RetryConfig{MaxAttempts: 2, InitialInterval: time.Second}, MaxDeferrals: 1,
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
