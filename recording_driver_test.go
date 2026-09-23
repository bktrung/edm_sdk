package f1_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"

	//nolint:depguard // this wrapper delegates to the in-memory driver, the only adapter that needs no broker, and importing it here is what keeps these tests outside package f1.
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem"
)

// recordingWaitTimeout bounds every wait these tests do on the wrapper. The
// wrapper closes the channel each wait is on, so a timeout means the core did
// something the ordering rules out, not that the test was slow.
const recordingWaitTimeout = 10 * time.Second

// recordingRetryInterval is the first retry tier both order tests use. It only
// has to be positive and short: the test waits for the retry copy on a channel,
// never on the delay.
const recordingRetryInterval = time.Millisecond

// recordingEventKind names one step of the driver call order the wrapper
// observed. The tests assert on these kinds rather than on a rendered log, so
// the wording of a failure message can change without moving an assertion.
type recordingEventKind string

const (
	eventPublished        recordingEventKind = "publish returned"
	eventSettled          recordingEventKind = "settle returned"
	eventNacked           recordingEventKind = "nack returned"
	eventProducerCreated  recordingEventKind = "producer created"
	eventConsumerCreated  recordingEventKind = "consumer created"
	eventConsumerDrained  recordingEventKind = "consumer drain returned"
	eventConsumerStopped  recordingEventKind = "consumer stop returned"
	eventConsumerReleased recordingEventKind = "consumer release returned"
	eventProducerClosed   recordingEventKind = "producer close returned"
	eventConnClosed       recordingEventKind = "conn close returned"
	eventHandlerFailed    recordingEventKind = "handler failed"
	eventHandlerHandled   recordingEventKind = "handler handled"
)

// recordingEvent is one entry of the log: a monotonic sequence number and what
// happened. The sequence, not a wall-clock time, is what makes two entries
// taken in the same millisecond comparable.
//
// destination and key carry the identity of the message a publish or a settle
// call was about, for a call that carried exactly one message. Assertions match
// on those two fields rather than on an entry's position, because position is
// not identity: a driver may hand a message to its consumer before its own
// Publish call returns, so the publish record of the original can land after
// the record of its successor copy. A call carrying several messages records
// the rendered detail only.
type recordingEvent struct {
	seq         int
	kind        recordingEventKind
	detail      string
	destination string
	key         string
}

// recordingLog is the shared log every wrapped call appends to. Appending under
// the mutex the assertions read under is the whole ordering guarantee: two
// entries are ordered exactly when they were appended in that order.
type recordingLog struct {
	mu      sync.Mutex
	next    int
	counts  map[recordingEventKind]int
	events  []recordingEvent
	waiters map[waitKey][]chan struct{}
}

// waitKey identifies the index-th event of one kind, which is what a test waits
// for when the first event of a kind is not the one it needs: the first settle
// belongs to the original delivery and the second to its retry copy.
type waitKey struct {
	kind  recordingEventKind
	index int
}

func newRecordingLog() *recordingLog {
	return &recordingLog{counts: make(map[recordingEventKind]int), waiters: make(map[waitKey][]chan struct{})}
}

// record appends one event whose only identity is its rendered detail.
func (l *recordingLog) record(kind recordingEventKind, detail string) {
	l.append(recordingEvent{kind: kind, detail: detail})
}

// recordMessage appends one publish or settle event about a single message, and
// keeps the destination and the message key as fields the assertions match on.
//
// The key is what says two calls were about the same message: a successor copy
// keeps the key of the delivery it was made from, which is also why the copy's
// destination is the field that tells a copy apart from the delivery itself.
func (l *recordingLog) recordMessage(kind recordingEventKind, destination, key string, err error) {
	l.append(recordingEvent{
		kind:        kind,
		detail:      resultDetail(messageDetail(destination, key), err),
		destination: destination,
		key:         key,
	})
}

func (l *recordingLog) append(event recordingEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.next++
	event.seq = l.next
	l.counts[event.kind]++
	l.events = append(l.events, event)
	key := waitKey{kind: event.kind, index: l.counts[event.kind]}
	for _, waiter := range l.waiters[key] {
		close(waiter)
	}
	delete(l.waiters, key)
}

// observed returns a channel closed when the first event of kind is recorded,
// already closed when one is already there. Waiting on it is how a test in
// these files avoids a sleep.
func (l *recordingLog) observed(kind recordingEventKind) <-chan struct{} {
	return l.observedNth(kind, 1)
}

// observedNth is observed for the index-th event of kind, counting from one.
func (l *recordingLog) observedNth(kind recordingEventKind, index int) <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	waiter := make(chan struct{})
	if l.counts[kind] >= index {
		close(waiter)
		return waiter
	}
	key := waitKey{kind: kind, index: index}
	l.waiters[key] = append(l.waiters[key], waiter)
	return waiter
}

// snapshot copies the log so an assertion reads one consistent order.
func (l *recordingLog) snapshot() []recordingEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.events)
}

// entries returns every entry of kind in log order.
func (l *recordingLog) entries(kind recordingEventKind) []recordingEvent {
	var matching []recordingEvent
	for _, event := range l.snapshot() {
		if event.kind == kind {
			matching = append(matching, event)
		}
	}
	return matching
}

// describe renders the whole log for a failure message, so a failing ordering
// assertion shows the order it saw instead of only the pair it wanted.
func (l *recordingLog) describe() string {
	var builder strings.Builder
	for _, event := range l.snapshot() {
		fmt.Fprintf(&builder, "\n\t%d: %s", event.seq, event.kind)
		if event.detail != "" {
			fmt.Fprintf(&builder, " (%s)", event.detail)
		}
	}
	return builder.String()
}

// publishHold is one wrapped driver call the wrapper keeps open until its test
// releases it. It is named for the publish case it was written for; the
// consumer and settle calls the wrapper holds use the same shape, so a reader
// should read "publish" here as the call being held.
//
// The hold sits between the driver call's return and the wrapper's own, so the
// call the core made has not returned: the core still counts it in flight, and
// Close must still wait it out. The driver call itself is already made, so a
// drain that cancels the caller's context mid-hold cannot turn the held call
// into a failed one and change what the test is measuring.
type publishHold struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *publishHold) letGo() {
	h.once.Do(func() { close(h.release) })
}

// await blocks until the test releases the hold, and reports that the call
// reached it by closing started.
func (h *publishHold) await() {
	close(h.started)
	<-h.release
}

// callGate arranges for one call the wrapper observes next to be held open
// until the test releases it. Holding is by the event kind the call records,
// which is what the call sites already name: a test arms the kind it is about
// and the wrapper consults the gate where it records that kind.
//
// A hold is taken by the first call of its kind, wherever in the log that call
// falls, so a test that arms the release hold before it publishes still holds
// the release the core makes later rather than waiting on a clock.
type callGate struct {
	mu   sync.Mutex
	next map[recordingEventKind]*publishHold
}

func newCallGate() *callGate {
	return &callGate{next: make(map[recordingEventKind]*publishHold)}
}

// holdNext arms the gate to keep the next call recording one of kinds open
// until the returned hold is released.
func (g *callGate) holdNext(kinds ...recordingEventKind) *publishHold {
	g.mu.Lock()
	defer g.mu.Unlock()
	slot := &publishHold{started: make(chan struct{}), release: make(chan struct{})}
	for _, kind := range kinds {
		g.next[kind] = slot
	}
	return slot
}

// begin takes the hold armed for one call recording kind, if any.
func (g *callGate) begin(kind recordingEventKind) *publishHold {
	g.mu.Lock()
	defer g.mu.Unlock()
	hold := g.next[kind]
	delete(g.next, kind)
	return hold
}

// publishGate decides which publish calls the wrapper holds, and records how
// many publishes were still in flight when the producer was closed.
//
// Holding is by one-based call index, counted by this gate itself. A test can
// therefore hold the successor publish and the application publish in flight
// around Close without depending on a clock or on which goroutine arrives
// first, and the same counter answers the question the ordering rule turns on:
// was anything still publishing when producer.Close ran.
type publishGate struct {
	mu              sync.Mutex
	calls           int
	inFlight        int
	holds           map[int]*publishHold
	closeCalls      int
	inFlightAtClose int
}

func newPublishGate() *publishGate {
	return &publishGate{holds: make(map[int]*publishHold)}
}

// hold arms the gate to keep the call-th publish open until the returned hold
// is released. The hold's started channel closes when that publish reaches it.
func (g *publishGate) hold(call int) *publishHold {
	g.mu.Lock()
	defer g.mu.Unlock()
	slot := &publishHold{started: make(chan struct{}), release: make(chan struct{})}
	g.holds[call] = slot
	return slot
}

// begin counts one publish call and returns the hold armed for it, if any.
func (g *publishGate) begin() *publishHold {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls++
	g.inFlight++
	hold := g.holds[g.calls]
	delete(g.holds, g.calls)
	return hold
}

// finish records one publish call as returned. It must be called exactly once
// per begin.
func (g *publishGate) finish() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.inFlight--
}

// producerCloseSnapshot reports, for the first producer close, how many
// publishes were in flight when it ran, and how many times Close was called.
//
// The first close seen through the wrapper is taken to be the close being
// measured. That holds while a client owns one producer; a test whose client can
// close a discarded producer would have to select by producer identity instead.
func (g *publishGate) producerCloseSnapshot() (inFlight, calls int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closeCalls++
	if g.closeCalls == 1 {
		g.inFlightAtClose = g.inFlight
	}
	return g.inFlightAtClose, g.closeCalls
}

// recordingDriver wraps the in-memory driver so a test outside the package can
// see the order of the driver calls the core makes: which publish returned
// before which settle, and where producer and connection teardown sit relative
// to both. It implements the exported port and changes no behaviour.
type recordingDriver struct {
	inner           inmem.Driver
	log             *recordingLog
	gate            *publishGate
	calls           *callGate
	injected        chan error
	consumerCreated chan struct{}
}

var _ driver.Driver = (*recordingDriver)(nil)

func (d *recordingDriver) Name() string { return d.inner.Name() }

func (d *recordingDriver) Capabilities() driver.Capabilities { return d.inner.Capabilities() }

func (d *recordingDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	conn, err := d.inner.Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &recordingConn{inner: conn, log: d.log, gate: d.gate, calls: d.calls, injected: d.injected, consumerCreated: d.consumerCreated}, nil
}

// recordingConn wraps one connection. consumerCreated closes once the runner
// has opened its consumer, which is also the point the runner has declared the
// subscription's topology, so a publish after it cannot fail for a missing
// destination.
type recordingConn struct {
	inner           driver.Conn
	log             *recordingLog
	gate            *publishGate
	calls           *callGate
	injected        chan error
	consumerCreated chan struct{}
	created         sync.Once
}

var _ driver.Conn = (*recordingConn)(nil)

func (c *recordingConn) Capabilities() driver.Capabilities { return c.inner.Capabilities() }

func (c *recordingConn) BrokerInfo() driver.BrokerInfo { return c.inner.BrokerInfo() }

func (c *recordingConn) Producer(ctx context.Context, cfg driver.ProducerConfig) (driver.Producer, error) {
	producer, err := c.inner.Producer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	c.log.record(eventProducerCreated, "")
	return &recordingProducer{inner: producer, log: c.log, gate: c.gate}, nil
}

func (c *recordingConn) Consumer(ctx context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	hold := c.calls.begin(eventConsumerCreated)
	consumer, err := c.inner.Consumer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if hold != nil {
		hold.await()
	}
	c.log.record(eventConsumerCreated, "")
	c.created.Do(func() { close(c.consumerCreated) })
	return &recordingConsumer{
		inner:    consumer,
		log:      c.log,
		calls:    c.calls,
		errs:     relayConsumerErrors(consumer.Errors(), c.injected),
		messages: recordingMessages(consumer, c.log, c.calls),
	}, nil
}

func (c *recordingConn) Admin() driver.Admin { return c.inner.Admin() }

func (c *recordingConn) Ping(ctx context.Context) error { return c.inner.Ping(ctx) }

func (c *recordingConn) Close(ctx context.Context) error {
	err := c.inner.Close(ctx)
	c.log.record(eventConnClosed, resultDetail("", err))
	return err
}

type recordingProducer struct {
	inner driver.Producer
	log   *recordingLog
	gate  *publishGate
}

var _ driver.Producer = (*recordingProducer)(nil)

func (p *recordingProducer) Publish(ctx context.Context, messages ...driver.OutboundMessage) error {
	hold := p.gate.begin()
	err := p.inner.Publish(ctx, messages...)
	if hold != nil {
		hold.await()
	}
	if len(messages) == 1 {
		p.log.recordMessage(eventPublished, messages[0].Destination, string(messages[0].Key), err)
	} else {
		p.log.record(eventPublished, resultDetail(publishDestinations(messages), err))
	}
	p.gate.finish()
	return err
}

func (p *recordingProducer) Close(ctx context.Context) error {
	p.gate.producerCloseSnapshot()
	err := p.inner.Close(ctx)
	p.log.record(eventProducerClosed, resultDetail("", err))
	return err
}

type recordingConsumer struct {
	inner    driver.Consumer
	log      *recordingLog
	calls    *callGate
	errs     <-chan error
	messages <-chan driver.InboundMessage
}

var _ driver.Consumer = (*recordingConsumer)(nil)

func (c *recordingConsumer) Messages() <-chan driver.InboundMessage { return c.messages }

func (c *recordingConsumer) Errors() <-chan error { return c.errs }

func (c *recordingConsumer) Pause(destinations ...string) error {
	return c.inner.Pause(destinations...)
}

func (c *recordingConsumer) Resume(destinations ...string) error {
	return c.inner.Resume(destinations...)
}

func (c *recordingConsumer) Drain(ctx context.Context) error {
	err := c.inner.Drain(ctx)
	c.log.record(eventConsumerDrained, resultDetail("", err))
	return err
}

func (c *recordingConsumer) Stop(ctx context.Context) error {
	err := c.inner.Stop(ctx)
	c.awaitHold(eventConsumerStopped)
	c.log.record(eventConsumerStopped, resultDetail("", err))
	return err
}

func (c *recordingConsumer) Release(ctx context.Context) error {
	err := c.inner.Release(ctx)
	c.awaitHold(eventConsumerReleased)
	c.log.record(eventConsumerReleased, resultDetail("", err))
	return err
}

// awaitHold keeps one teardown call open after the driver has already made it,
// so a test can assert that the core is still inside that call rather than
// reading the order after the fact.
func (c *recordingConsumer) awaitHold(kind recordingEventKind) {
	if hold := c.calls.begin(kind); hold != nil {
		hold.await()
	}
}

func (c *recordingConsumer) Lag(ctx context.Context) (map[string]int64, error) {
	return c.inner.Lag(ctx)
}

// recordingSettler observes the settle the core performs on one delivery. The
// core settles the message rather than the consumer, so this is the only place
// outside the package where an ack can be seen at all, and the only place the
// order of that ack against a confirmed successor publish can be asserted.
type recordingSettler struct {
	inner       driver.Settler
	log         *recordingLog
	calls       *callGate
	destination string
	key         string
}

var _ driver.Settler = (*recordingSettler)(nil)

func (s *recordingSettler) Ack(ctx context.Context) error {
	err := s.inner.Ack(ctx)
	s.awaitHold(eventSettled)
	s.log.recordMessage(eventSettled, s.destination, s.key, err)
	return err
}

func (s *recordingSettler) Nack(ctx context.Context, options driver.NackOptions) error {
	err := s.inner.Nack(ctx, options)
	s.awaitHold(eventNacked)
	s.log.recordMessage(eventNacked, s.destination, s.key, err)
	return err
}

// awaitHold keeps one settle call open after the driver has already settled the
// delivery, so a test can assert that the runner's shutdown still waits for the
// settle to return rather than assuming it is done because the delivery left the
// registry.
func (s *recordingSettler) awaitHold(kind recordingEventKind) {
	if hold := s.calls.begin(kind); hold != nil {
		hold.await()
	}
}

// recordingMessages relays the driver's deliveries through a channel of the
// wrapper's own so every inbound message carries a recording settler. The relay
// ends when the driver closes its channel, which the port only does after Stop
// or Release completes.
//
// The extra hop is not free during a shutdown. A delivery the driver has
// already handed over can sit in this relay while the core's drain read loop
// sees an empty channel and finishes, and it is then never settled: the driver
// counts it outstanding and consumer.Stop refuses with "cannot stop with 1
// outstanding messages", which fails Close. A test using this wrapper must
// therefore keep deliveries off the consumer in the window where the client is
// closing; close_order_test.go publishes its second event to a topic the
// subscription does not consume for exactly that reason.
func recordingMessages(inner driver.Consumer, log *recordingLog, calls *callGate) <-chan driver.InboundMessage {
	messages := make(chan driver.InboundMessage)
	go func() {
		defer close(messages)
		for message := range inner.Messages() {
			message.Settle = &recordingSettler{
				inner:       message.Settle,
				log:         log,
				calls:       calls,
				destination: message.Destination,
				key:         string(message.Key),
			}
			messages <- message
		}
	}()
	return messages
}

// relayConsumerErrors merges the driver's consumer errors with the ones a test
// injects, so a test can drive the runner's error paths over the exported port.
// The relay ends when the driver closes its channel, which the port does only
// after Stop or Release completes, and a queued injection is dropped at that
// point because the consumer it was meant for is gone.
func relayConsumerErrors(inner <-chan error, injected <-chan error) <-chan error {
	errs := make(chan error)
	go func() {
		defer close(errs)
		for {
			select {
			case err, ok := <-inner:
				if !ok {
					return
				}
				errs <- err
			case err, ok := <-injected:
				if !ok {
					return
				}
				errs <- err
			}
		}
	}()
	return errs
}

// messageDetail renders the destination and partition key of one message, so a
// reader of the log can tell the original, a retry copy, a dead-letter copy and
// a duplicate apart. The same rendering is what a publish record and the settle
// record of that message carry, which is why the two are comparable.
func messageDetail(destination, key string) string {
	return destination + " key=" + key
}

// publishDestinations renders the destination and partition key of each message
// in one Publish call. A call carrying one message is recorded with
// recordMessage instead; this is the rendering for the batch case, where
// destination and key are per message and the call has no single identity.
func publishDestinations(messages []driver.OutboundMessage) string {
	details := make([]string, len(messages))
	for i, message := range messages {
		details[i] = messageDetail(message.Destination, string(message.Key))
	}
	return strings.Join(details, ",")
}

// resultDetail appends a call's error to its detail, so the rendered log shows a
// failed settle or publish where the ordering assertion is read.
func resultDetail(detail string, err error) string {
	if err == nil {
		return detail
	}
	if detail == "" {
		return err.Error()
	}
	return detail + ": " + err.Error()
}

// recordingFixture is one client driven through the wrapper, with the log and
// the publish gate its test asserts on.
type recordingFixture struct {
	client          *f1.Client
	log             *recordingLog
	gate            *publishGate
	calls           *callGate
	injected        chan error
	consumerCreated <-chan struct{}
}

func newRecordingFixture(t *testing.T, opts ...f1.Option) *recordingFixture {
	t.Helper()
	log := newRecordingLog()
	gate := newPublishGate()
	calls := newCallGate()
	injected := make(chan error, 4)
	consumerCreated := make(chan struct{})
	wrapper := &recordingDriver{inner: inmem.New(), log: log, gate: gate, calls: calls, injected: injected, consumerCreated: consumerCreated}
	options := append([]f1.Option{
		f1.WithDriver(wrapper),
		f1.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	}, opts...)
	client, err := f1.New(context.Background(), recordingClientConfig(), options...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return &recordingFixture{
		client:          client,
		log:             log,
		gate:            gate,
		calls:           calls,
		injected:        injected,
		consumerCreated: consumerCreated,
	}
}

// injectError hands err to the runner as a consumer error, the same channel a
// driver uses to report a broken consumer. It is how a test reaches the
// runner's error paths over the exported port, which no public API exposes.
func (f *recordingFixture) injectError(err error) {
	f.injected <- err
}

// recordingClientConfig is the smallest resolvable configuration that carries a
// subscription through the in-memory driver. DrainTimeout is explicit because
// the drain budget also bounds the settlement window a held publish runs in.
func recordingClientConfig() f1.Config {
	return f1.Config{
		Env:        "test",
		Service:    "orders",
		InstanceID: "recording-order-tests",
		Broker:     f1.BrokerConfig{Driver: "inmem", ConnectTimeout: 5 * time.Second, DefaultPrefetch: 8},
		Topology:   f1.TopologyConfig{AutoCreate: true, Priorities: []f1.Priority{f1.PriorityMedium}},
		Codec:      f1.CodecConfig{Default: "json", MaxHeaderBytes: f1.CoreMaxHeaderBytes, MaxBodyBytes: 1 << 20},
		Lifecycle:  f1.LifecycleConfig{DrainTimeout: 10 * time.Second, HandlerGrace: time.Second, CloseTimeout: 5 * time.Second},
	}
}

// recordingSubscription declares the one subscription both order tests consume
// from: the medium lane of orders.created, with the retry ladder the caller
// asks for. HandlerTimeout exceeds nothing here; the drain budget is longer.
func recordingSubscription(retry f1.RetryConfig, handler f1.Handler) f1.Subscription {
	return f1.Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Concurrency:    1,
		Prefetch:       4,
		Priorities:     []f1.Priority{f1.PriorityMedium},
		Retry:          retry,
		HandlerTimeout: 2 * time.Second,
		Handlers:       map[string]f1.Handler{"orders.created.v1": handler},
	}
}

// startRecordingRunner subscribes, runs the runner for the life of the client,
// and returns after the runner has opened its consumer. Waiting for that
// consumer is what makes the publishes in these tests deterministic: the runner
// declares the subscription topology just before it opens one, so a publish
// issued afterwards cannot miss a destination.
func startRecordingRunner(t *testing.T, fixture *recordingFixture, subscription f1.Subscription) <-chan error {
	t.Helper()
	runner, err := fixture.client.Subscribe(context.Background(), subscription)
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(context.Background()) }()
	waitForChannel(t, fixture.log, fixture.consumerCreated, "the runner never opened its consumer")
	return runDone
}

// waitForChannel blocks until signal closes and fails the test when the wait
// outlives recordingWaitTimeout. Every signal in these tests is a channel the
// wrapper closes, so no wait here sleeps.
func waitForChannel(t *testing.T, log *recordingLog, signal <-chan struct{}, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), recordingWaitTimeout)
	defer cancel()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatalf("timed out after %s waiting for %s; log:%s", recordingWaitTimeout, what, log.describe())
	}
}

// waitForChannelWithout waits for signal, and fails the moment the log records
// forbidden instead. Use it where the ordering rules one event out of the way of
// another: watching for the forbidden event reports the violation where it
// happens, instead of leaving the test to time out waiting for the event it
// wanted.
//
// The check is best-effort when forbidden is already in the log: both cases of
// the select are then ready and Go picks at random. That costs the precision of
// the failure message, not the verdict, because the ordering chain the test
// asserts afterwards still fails.
func waitForChannelWithout(t *testing.T, log *recordingLog, signal <-chan struct{}, forbidden recordingEventKind, what string) {
	t.Helper()
	forbiddenObserved := log.observed(forbidden)
	ctx, cancel := context.WithTimeout(context.Background(), recordingWaitTimeout)
	defer cancel()
	select {
	case <-forbiddenObserved:
		t.Fatalf("%s; log:%s", what, log.describe())
	case <-signal:
	case <-ctx.Done():
		t.Fatalf("timed out after %s waiting for %s; log:%s", recordingWaitTimeout, what, log.describe())
	}
}

// requireNoEventWithin fails when the log records kind inside window. Use it
// only for an event the ordering forbids until something else has happened, and
// only where the order the test reads after the fact would be racy: an event
// whose absence is asserted while the thing that would legitimately cause it is
// still blocked needs a window, because there is no channel to wait on. The
// window bounds how long a violation takes to surface; it is not a
// synchronization delay, since a violation cannot be invented by waiting.
func requireNoEventWithin(t *testing.T, log *recordingLog, kind recordingEventKind, window time.Duration, what string) {
	t.Helper()
	observed := log.observed(kind)
	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	select {
	case <-observed:
		t.Fatalf("%s; log:%s", what, log.describe())
	case <-ctx.Done():
	}
}

// waitForResult blocks until done produces a value and fails the test when the
// wait outlives recordingWaitTimeout.
func waitForResult(t *testing.T, log *recordingLog, done <-chan error, what string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), recordingWaitTimeout)
	defer cancel()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		t.Fatalf("timed out after %s waiting for %s; log:%s", recordingWaitTimeout, what, log.describe())
		return nil
	}
}

// requirePending fails when done has already produced a result. Call it only
// where the ordering makes that answer deterministic.
func requirePending(t *testing.T, log *recordingLog, done <-chan error, what string) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("%s returned while a publish was still held: err = %v; log:%s", what, err, log.describe())
	default:
	}
}

// requireEntry returns the index-th entry of kind in log order.
func requireEntry(t *testing.T, log *recordingLog, kind recordingEventKind, index int, what string) recordingEvent {
	t.Helper()
	entries := log.entries(kind)
	if len(entries) <= index {
		t.Fatalf("%s: want at least %d %q event(s), got %d; log:%s", what, index+1, kind, len(entries), log.describe())
	}
	return entries[index]
}

// requireSuccessorPublish returns the single publish entry that carried the key
// of delivery from a destination other than the one it was delivered from,
// which is the publish of that message's successor copy.
//
// The message key is what makes the copy identifiable: a copy keeps the key of
// the delivery it was made from, and its destination is what tells it apart
// from the original, whose own publish record carries the destination and key
// of the delivery. A position in the log cannot do this job, because the
// original's publish record and the delivery race: the driver may hand the
// message to the consumer before its own Publish call returns, so the original's
// record can land after its successor's.
func requireSuccessorPublish(t *testing.T, log *recordingLog, delivery recordingEvent, what string) recordingEvent {
	t.Helper()
	if delivery.key == "" {
		t.Fatalf("%s: the delivery has no message key recorded, so its copy cannot be identified; log:%s", what, log.describe())
	}
	var found []recordingEvent
	for _, event := range log.entries(eventPublished) {
		if event.key == delivery.key && event.destination != delivery.destination {
			found = append(found, event)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%s: want exactly one publish of message %q from a destination other than %q, got %d; log:%s",
			what, delivery.key, delivery.destination, len(found), log.describe())
	}
	return found[0]
}

// requirePublishOfKey returns the single publish entry that carried key. Use it
// where a test knows the key it published under and the entry it wants is the
// only publish of that message.
func requirePublishOfKey(t *testing.T, log *recordingLog, key, what string) recordingEvent {
	t.Helper()
	var found []recordingEvent
	for _, event := range log.entries(eventPublished) {
		if event.key == key {
			found = append(found, event)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%s: want exactly one publish of message %q, got %d; log:%s", what, key, len(found), log.describe())
	}
	return found[0]
}

// requireSameMessage fails unless settled and published name the same message:
// the same key, sent to the same destination.
func requireSameMessage(t *testing.T, log *recordingLog, settled, published recordingEvent, what string) {
	t.Helper()
	if settled.key == published.key && settled.destination == published.destination {
		return
	}
	t.Fatalf("%s: settled as %s (key %q), want the message published as %s (key %q); log:%s",
		what, settled.detail, settled.key, published.detail, published.key, log.describe())
}

// requireOrdered fails unless each entry follows the one before it in the log.
// The comparison is inclusive so passing the same entry twice, which is always
// a mistake in the assertion rather than in the core, fails with the same
// message instead of passing.
func requireOrdered(t *testing.T, log *recordingLog, what string, events ...recordingEvent) {
	t.Helper()
	for i := 1; i < len(events); i++ {
		if events[i].seq <= events[i-1].seq {
			t.Fatalf("%s: %s (seq %d) must follow %s (seq %d); log:%s",
				what, events[i].kind, events[i].seq, events[i-1].kind, events[i-1].seq, log.describe())
		}
	}
}
