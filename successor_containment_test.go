package f1

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// A retry or dead-letter copy that can never be published is contained instead
// of stopping the subscription: the original is acknowledged and the drop is
// reported. These tests drive a running subscription through the public API
// over the in-memory test driver, with the broker's answer to a successor
// publish scripted per destination.

const (
	// containmentSubscription, containmentTopic, and containmentSource are what
	// the harness config produces. The successor destinations the tests refuse
	// come from the core's own helpers applied to them, and the harness proves
	// the source it actually got is the one the helpers were given.
	containmentSubscription = "orders"
	containmentTopic        = "orders.created"
	containmentSource       = "/test/orders"

	// containmentRetryID is handled with an ordinary failure, so it takes the
	// retry path; containmentTerminalID is handled with Terminal, so it takes
	// the dead-letter path without a retry copy.
	containmentRetryID    = "containment-retry"
	containmentTerminalID = "containment-terminal"

	// containmentTimeout bounds every wait. A successor publish retries at most
	// maxSuccessorPublishAttempts times with a short backoff, so work still
	// outstanding after this is stuck rather than slow.
	containmentTimeout = 3 * time.Second
)

func containmentRetryDestination() string {
	return retryDestinationFor(containmentSource, containmentTopic, PriorityHigh, 1, containmentSubscription)
}

func containmentDeadLetterDestination() string {
	return deadLetterDestinationFor(containmentSource, containmentTopic, containmentSubscription)
}

// containmentDeliveryDestination is where the harness delivers the original
// from, which is the destination a drop report names.
func containmentDeliveryDestination() string {
	return publishEntryPoint(containmentSource, containmentTopic, PriorityHigh)
}

func containmentEnvelope(id string) Envelope {
	return Envelope{
		SpecVersion: "1.0",
		ID:          id,
		Source:      containmentSource,
		Type:        containmentTopic + ".v1",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
}

// destinationRefusal is one destination's scripted answer to a publish.
type destinationRefusal struct {
	destination string
	kind        driver.Kind
	cause       error
}

// successorBroker plays the broker's part for one harness: it refuses the
// destinations the test names, and reports every accepted publish on a signal
// channel so a test blocks on the publish instead of polling for it.
type successorBroker struct {
	mu       sync.Mutex
	refusals map[string]destinationRefusal
	refused  []string
	accepted chan struct{}
}

func newSuccessorBroker(refusals []destinationRefusal) *successorBroker {
	broker := &successorBroker{
		refusals: make(map[string]destinationRefusal, len(refusals)),
		accepted: make(chan struct{}, 8),
	}
	for _, refusal := range refusals {
		broker.refusals[refusal.destination] = refusal
	}
	return broker
}

func (b *successorBroker) refusalFor(destination string) (destinationRefusal, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	refusal, refused := b.refusals[destination]
	if refused {
		b.refused = append(b.refused, destination)
	}
	return refusal, refused
}

// refusedDestinations returns the refused destinations, each once, sorted.
func (b *successorBroker) refusedDestinations() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Compact(slices.Sorted(slices.Values(b.refused)))
}

func (b *successorBroker) acceptedPublish() {
	select {
	case b.accepted <- struct{}{}:
	default:
	}
}

// successorRefusalDriver is the in-memory test driver with the broker's
// refusals in front of its producer. It wraps the shared test connection the
// way publishFanoutDriver does, so the consume path, the admin surface, and the
// recording producer are the ones the other successor tests use.
type successorRefusalDriver struct {
	conn   *dispatchConn
	broker *successorBroker
}

func (*successorRefusalDriver) Name() string                      { return "inmem" }
func (*successorRefusalDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }

func (d *successorRefusalDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return &successorRefusalConn{dispatchConn: d.conn, broker: d.broker}, nil
}

type successorRefusalConn struct {
	*dispatchConn
	broker *successorBroker
}

func (c *successorRefusalConn) Producer(ctx context.Context, cfg driver.ProducerConfig) (driver.Producer, error) {
	inner, err := c.dispatchConn.Producer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &successorRefusalProducer{inner: inner, broker: c.broker}, nil
}

// successorRefusalProducer publishes to the in-memory producer unless the
// broker refuses the destination, in which case it reports the classification
// the test chose for that destination and publishes nothing.
type successorRefusalProducer struct {
	inner  driver.Producer
	broker *successorBroker
}

func (p *successorRefusalProducer) Publish(ctx context.Context, messages ...driver.OutboundMessage) error {
	failed := make(map[int]error)
	for index, message := range messages {
		refusal, refused := p.broker.refusalFor(message.Destination)
		if refused {
			failed[index] = &driver.Error{Driver: "inmem", Op: "publish", K: refusal.kind, Err: refusal.cause}
			continue
		}
		if err := p.inner.Publish(ctx, message); err != nil {
			failed[index] = err
			continue
		}
		p.broker.acceptedPublish()
	}
	if len(failed) == 0 {
		return nil
	}
	return &driver.PublishError{Failed: failed}
}

func (p *successorRefusalProducer) Close(ctx context.Context) error {
	return p.inner.Close(ctx)
}

// containmentSettler is the settler of a delivered message: it records which
// settlement operation the worker chose, and reports the acknowledgement on a
// channel so a test blocks on it instead of racing the worker to read the flag.
type containmentSettler struct {
	mu           sync.Mutex
	acked        bool
	nacked       bool
	acknowledged chan struct{}
}

func newContainmentSettler() *containmentSettler {
	return &containmentSettler{acknowledged: make(chan struct{}, 1)}
}

func (s *containmentSettler) Ack(context.Context) error {
	s.mu.Lock()
	s.acked = true
	s.mu.Unlock()
	select {
	case s.acknowledged <- struct{}{}:
	default:
	}
	return nil
}

func (s *containmentSettler) Nack(context.Context, driver.NackOptions) error {
	s.mu.Lock()
	s.nacked = true
	s.mu.Unlock()
	return nil
}

func (s *containmentSettler) settled() (acked, nacked bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acked, s.nacked
}

// requireAcknowledged blocks until the original delivery was acknowledged and
// then requires that it was acked and never nacked.
func (s *containmentSettler) requireAcknowledged(t *testing.T) {
	t.Helper()
	timer := clock.NewReal().Timer(containmentTimeout)
	defer timer.Stop()
	select {
	case <-s.acknowledged:
	case <-timer.C:
		t.Fatal("the original delivery was not acknowledged in time")
	}
	if acked, nacked := s.settled(); !acked || nacked {
		t.Fatalf("original settlement = acked %t nacked %t, want ack only", acked, nacked)
	}
}

// containmentOptions is what a containment test changes about the harness
// before the subscription starts.
type containmentOptions struct {
	// refusals are the destinations the broker refuses, and how it refuses them.
	refusals []destinationRefusal
	// headerMaxBytes overrides the codec header cap when positive. That is how
	// the encode case pushes the dead-letter copy past the cap.
	headerMaxBytes int
}

// successorContainment is one running subscription over the in-memory test
// driver plus the observations the containment tests assert on.
type successorContainment struct {
	client   *Client
	runner   *Runner
	consumer *dispatchConsumer
	producer *dispatchProducer
	broker   *successorBroker
	recorder *errorHandlerRecorder
	handled  chan string
	runDone  chan error
	cancel   context.CancelFunc
}

func newSuccessorContainment(t *testing.T, options containmentOptions) *successorContainment {
	t.Helper()
	config := testClientConfig(t)
	if options.headerMaxBytes > 0 {
		config.Codec.MaxHeaderBytes = options.headerMaxBytes
	}
	broker := newSuccessorBroker(options.refusals)
	producer := &dispatchProducer{}
	consumer := newDispatchConsumer()
	conn := &dispatchConn{producer: producer, consumer: consumer, admin: &dispatchAdmin{}}
	recorder := newErrorHandlerRecorder()
	containment := &successorContainment{
		consumer: consumer,
		producer: producer,
		broker:   broker,
		recorder: recorder,
		handled:  make(chan string, 8),
		runDone:  make(chan error, 1),
	}
	client, err := New(context.Background(), config,
		WithDriver(&successorRefusalDriver{conn: conn, broker: broker}),
		WithErrorHandler(recorder.handle),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	if client.source != containmentSource {
		t.Fatalf("client source = %q, want %q: the refused destinations are derived from it", client.source, containmentSource)
	}
	containment.client = client

	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           containmentSubscription,
		Topics:         []string{containmentTopic},
		Priorities:     []Priority{PriorityHigh},
		Retry:          RetryConfig{MaxAttempts: 3, Tiers: []time.Duration{time.Second}},
		HandlerTimeout: time.Second,
		Handlers: map[string]Handler{
			containmentTopic + ".v1": HandlerFunc(containment.handle),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	containment.runner = runner
	return containment
}

// handle fails the two scripted event IDs and reports every other event, which
// is how a test tells that the subscription went on consuming.
func (c *successorContainment) handle(_ context.Context, event *Event) error {
	switch event.ID() {
	case containmentTerminalID:
		return Terminal(errors.New("handler will not retry"))
	case containmentRetryID:
		return errors.New("handler failed")
	default:
		c.handled <- event.ID()
		return nil
	}
}

func (c *successorContainment) start(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	go func() { c.runDone <- c.runner.Run(ctx) }()
}

// deliver hands one inbound delivery to the running subscription, the way the
// broker would.
func (c *successorContainment) deliver(t *testing.T, envelope Envelope, settler driver.Settler) {
	t.Helper()
	timer := clock.NewReal().Timer(containmentTimeout)
	defer timer.Stop()
	select {
	case c.consumer.messages <- retryBridgeMessage(t, envelope, settler):
	case <-timer.C:
		t.Fatal("the running subscription did not take the delivery")
	}
}

func (c *successorContainment) acceptedPublishes() []driver.OutboundMessage {
	c.producer.mu.Lock()
	defer c.producer.mu.Unlock()
	return append([]driver.OutboundMessage(nil), c.producer.messages...)
}

// waitForPublish blocks until the broker accepted a publish and returns it.
func (c *successorContainment) waitForPublish(t *testing.T) driver.OutboundMessage {
	t.Helper()
	timer := clock.NewReal().Timer(containmentTimeout)
	defer timer.Stop()
	select {
	case <-c.broker.accepted:
	case <-timer.C:
		t.Fatal("the broker accepted no publish in time")
	}
	accepted := c.acceptedPublishes()
	if len(accepted) == 0 {
		t.Fatal("the broker signalled an accepted publish but recorded none")
	}
	return accepted[len(accepted)-1]
}

func (c *successorContainment) released() bool {
	c.consumer.mu.Lock()
	defer c.consumer.mu.Unlock()
	return c.consumer.released
}

// waitForReport blocks until the error handler was called and returns the event
// and error it was given.
func (c *successorContainment) waitForReport(t *testing.T) (*Event, error) {
	t.Helper()
	c.recorder.waitForCall(t, containmentTimeout)
	c.recorder.mu.Lock()
	defer c.recorder.mu.Unlock()
	return c.recorder.calls[0].event, c.recorder.calls[0].err
}

// waitForHandled blocks until the subscription handled an event and returns it.
func (c *successorContainment) waitForHandled(t *testing.T) string {
	t.Helper()
	timer := clock.NewReal().Timer(containmentTimeout)
	defer timer.Stop()
	select {
	case id := <-c.handled:
		return id
	case <-timer.C:
		t.Fatal("the subscription handled no event in time")
		return ""
	}
}

// stop cancels the run and requires it to end without an error, which is what
// "the subscription kept running" means once the test is done with it.
func (c *successorContainment) stop(t *testing.T) {
	t.Helper()
	c.cancel()
	if err := c.waitRun(t); err != nil {
		t.Fatalf("Run() after the test cancelled = %v, want nil", err)
	}
}

func (c *successorContainment) waitRun(t *testing.T) error {
	t.Helper()
	timer := clock.NewReal().Timer(containmentTimeout)
	defer timer.Stop()
	select {
	case err := <-c.runDone:
		return err
	case <-timer.C:
		t.Fatal("the subscription did not stop")
		return nil
	}
}

func (c *successorContainment) health(t *testing.T) error {
	t.Helper()
	return c.client.Health(context.Background())
}

// requireDropHeadline proves a report about a dropped delivery leads with the
// message-drop headline rather than the missing-route one. The exact prefix is
// asserted because the two headlines share the words "message dropped", so a
// substring check on those words cannot tell them apart.
func requireDropHeadline(t *testing.T, err error) {
	t.Helper()
	if !strings.HasPrefix(err.Error(), "f1: message dropped: event_id=") {
		t.Fatalf("drop report = %q, want the message-drop headline", err)
	}
}

// TestOversizedRetryCopyIsDeadLetteredAndTheSubscriptionContinues proves the
// first half of the ruling: when the broker refuses the retry copy as too
// large, the message goes to the dead-letter path exactly as an unencodable
// retry copy does, so the original is acknowledged and the subscription keeps
// consuming.
func TestOversizedRetryCopyIsDeadLetteredAndTheSubscriptionContinues(t *testing.T) {
	t.Parallel()
	containment := newSuccessorContainment(t, containmentOptions{
		refusals: []destinationRefusal{{
			destination: containmentRetryDestination(),
			kind:        driver.KindTooLarge,
			cause:       errors.New("message body exceeds the broker size limit"),
		}},
	})
	containment.start(t)

	settler := newContainmentSettler()
	containment.deliver(t, containmentEnvelope(containmentRetryID), settler)

	published := containment.waitForPublish(t)
	if want := containmentDeadLetterDestination(); published.Destination != want {
		t.Fatalf("published destination = %q, want %q: the refused retry copy must take the dead-letter path", published.Destination, want)
	}
	if got, want := headerValue(published.Headers, "f1deathreason"), ReasonTerminal.String(); got != want {
		t.Fatalf("dead-letter death reason = %q, want %q", got, want)
	}
	if got, want := containment.broker.refusedDestinations(), []string{containmentRetryDestination()}; !slices.Equal(got, want) {
		t.Fatalf("refused destinations = %v, want %v", got, want)
	}
	settler.requireAcknowledged(t)
	if got := containment.recorder.count(); got != 0 {
		t.Fatalf("error handler calls = %d, want 0: the message reached the dead-letter destination", got)
	}

	containment.deliver(t, containmentEnvelope("after-oversized-retry"), newContainmentSettler())
	if got := containment.waitForHandled(t); got != "after-oversized-retry" {
		t.Fatalf("handled event = %q, want the event published after the drop", got)
	}
	if err := containment.health(t); err != nil {
		t.Fatalf("Health() = %v, want nil: the subscription kept consuming", err)
	}
	containment.stop(t)
}

// TestOversizedDeadLetterCopyIsDroppedAndReported proves the second half: a
// dead-letter copy the broker refuses as too large is acknowledged and
// reported, and the subscription keeps consuming.
func TestOversizedDeadLetterCopyIsDroppedAndReported(t *testing.T) {
	t.Parallel()
	refusalCause := errors.New("message body exceeds the broker size limit")
	containment := newSuccessorContainment(t, containmentOptions{
		refusals: []destinationRefusal{{
			destination: containmentDeadLetterDestination(),
			kind:        driver.KindTooLarge,
			cause:       refusalCause,
		}},
	})
	containment.start(t)

	settler := newContainmentSettler()
	containment.deliver(t, containmentEnvelope(containmentTerminalID), settler)

	event, callErr := containment.waitForReport(t)
	if event == nil || event.ID() != containmentTerminalID {
		t.Fatalf("drop report event = %v, want the dropped event %q", event, containmentTerminalID)
	}
	requireDropHeadline(t, callErr)
	for _, want := range []string{
		containmentTerminalID,
		containmentDeliveryDestination(),
		"death reason " + ReasonTerminal.String(),
		refusalCause.Error(),
	} {
		if !strings.Contains(callErr.Error(), want) {
			t.Fatalf("drop report = %q, missing %q", callErr, want)
		}
	}
	if !errors.Is(callErr, refusalCause) {
		t.Fatalf("drop report = %v, want it to wrap the broker refusal %v", callErr, refusalCause)
	}
	if got, want := containment.recorder.count(), 1; got != want {
		t.Fatalf("error handler calls = %d, want %d: the drop is reported once", got, want)
	}
	if got, want := containment.broker.refusedDestinations(), []string{containmentDeadLetterDestination()}; !slices.Equal(got, want) {
		t.Fatalf("refused destinations = %v, want %v", got, want)
	}
	settler.requireAcknowledged(t)
	if len(containment.acceptedPublishes()) != 0 {
		t.Fatal("a refused dead-letter copy reached the broker")
	}

	containment.deliver(t, containmentEnvelope("after-oversized-dead-letter"), newContainmentSettler())
	if got := containment.waitForHandled(t); got != "after-oversized-dead-letter" {
		t.Fatalf("handled event = %q, want the event published after the drop", got)
	}
	if err := containment.health(t); err != nil {
		t.Fatalf("Health() = %v, want nil: the subscription kept consuming", err)
	}
	containment.stop(t)
}

// TestUnencodableDeadLetterCopyIsDroppedAndReported proves the same drop for a
// dead-letter copy the process cannot encode: the header cap is smaller than
// the envelope's own never-shed partition key, so the copy is unbuildable on
// every redelivery too.
func TestUnencodableDeadLetterCopyIsDroppedAndReported(t *testing.T) {
	t.Parallel()
	containment := newSuccessorContainment(t, containmentOptions{headerMaxBytes: 512})
	containment.start(t)

	envelope := containmentEnvelope(containmentTerminalID)
	envelope.PartitionKey = strings.Repeat("k", 2000)
	settler := newContainmentSettler()
	containment.deliver(t, envelope, settler)

	event, callErr := containment.waitForReport(t)
	if event == nil || event.ID() != containmentTerminalID {
		t.Fatalf("drop report event = %v, want the dropped event %q", event, containmentTerminalID)
	}
	requireDropHeadline(t, callErr)
	for _, want := range []string{
		containmentTerminalID,
		containmentDeliveryDestination(),
		"cannot be encoded",
		"death reason " + ReasonTerminal.String(),
	} {
		if !strings.Contains(callErr.Error(), want) {
			t.Fatalf("drop report = %q, missing %q", callErr, want)
		}
	}
	if !errors.Is(callErr, ErrEnvelopeTooLarge) {
		t.Fatalf("drop report = %v, want it to wrap %v", callErr, ErrEnvelopeTooLarge)
	}
	settler.requireAcknowledged(t)
	if len(containment.acceptedPublishes()) != 0 {
		t.Fatal("an unencodable dead-letter copy reached the broker")
	}

	containment.deliver(t, containmentEnvelope("after-unencodable-dead-letter"), newContainmentSettler())
	if got := containment.waitForHandled(t); got != "after-unencodable-dead-letter" {
		t.Fatalf("handled event = %q, want the event published after the drop", got)
	}
	if err := containment.health(t); err != nil {
		t.Fatalf("Health() = %v, want nil: the subscription kept consuming", err)
	}
	containment.stop(t)
}

// TestRetryAndDeadLetterCopiesBothTooLargeDropOnce proves the chain: a retry
// copy the broker refuses leads to a dead-letter copy it also refuses, and the
// message is dropped once rather than reported twice.
func TestRetryAndDeadLetterCopiesBothTooLargeDropOnce(t *testing.T) {
	t.Parallel()
	refusalCause := errors.New("message headers exceed the broker size limit")
	refusals := []destinationRefusal{
		{destination: containmentRetryDestination(), kind: driver.KindTooLarge, cause: refusalCause},
		{destination: containmentDeadLetterDestination(), kind: driver.KindTooLarge, cause: refusalCause},
	}
	containment := newSuccessorContainment(t, containmentOptions{refusals: refusals})
	containment.start(t)

	settler := newContainmentSettler()
	containment.deliver(t, containmentEnvelope(containmentRetryID), settler)

	event, callErr := containment.waitForReport(t)
	if event == nil || event.ID() != containmentRetryID {
		t.Fatalf("drop report event = %v, want the dropped event %q", event, containmentRetryID)
	}
	if !errors.Is(callErr, refusalCause) {
		t.Fatalf("drop report = %v, want it to wrap the broker refusal %v", callErr, refusalCause)
	}
	requireDropHeadline(t, callErr)
	for _, want := range []string{containmentDeliveryDestination(), "death reason terminal"} {
		if !strings.Contains(callErr.Error(), want) {
			t.Fatalf("drop report = %q, missing %q", callErr, want)
		}
	}
	if got, want := containment.recorder.count(), 1; got != want {
		t.Fatalf("error handler calls = %d, want %d: the chain is reported once", got, want)
	}
	wantRefused := []string{containmentDeadLetterDestination(), containmentRetryDestination()}
	if got := containment.broker.refusedDestinations(); !slices.Equal(got, wantRefused) {
		t.Fatalf("refused destinations = %v, want %v", got, wantRefused)
	}
	settler.requireAcknowledged(t)
	if len(containment.acceptedPublishes()) != 0 {
		t.Fatal("a refused copy reached the broker")
	}

	containment.deliver(t, containmentEnvelope("after-the-chain"), newContainmentSettler())
	if got := containment.waitForHandled(t); got != "after-the-chain" {
		t.Fatalf("handled event = %q, want the event published after the drop", got)
	}
	if err := containment.health(t); err != nil {
		t.Fatalf("Health() = %v, want nil: the subscription kept consuming", err)
	}
	containment.stop(t)
}

// TestTransientSuccessorRefusalStillStopsTheSubscription pins the case the
// ruling leaves alone: a refusal that can heal is a destination problem, so the
// subscription stops and Health reports it rather than the message being
// dropped. Both successor publishes are covered, because the predicate must
// capture only a copy the broker said can never be accepted: one that also
// matched a transient refusal would stop every handoff.
func TestTransientSuccessorRefusalStillStopsTheSubscription(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name        string
		destination string
		eventID     string
	}{
		{
			name:        "dead-letter copy",
			destination: containmentDeadLetterDestination(),
			eventID:     containmentTerminalID,
		},
		{
			name:        "retry copy",
			destination: containmentRetryDestination(),
			eventID:     containmentRetryID,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			refusalCause := errors.New("broker refused the publish")
			containment := newSuccessorContainment(t, containmentOptions{
				refusals: []destinationRefusal{{
					destination: testCase.destination,
					kind:        driver.KindTransient,
					cause:       refusalCause,
				}},
			})
			containment.start(t)

			settler := newContainmentSettler()
			containment.deliver(t, containmentEnvelope(testCase.eventID), settler)

			runErr := containment.waitRun(t)
			if runErr == nil {
				t.Fatal("Run() = nil, want the successor handoff failure: the subscription must stop")
			}
			if !errors.Is(runErr, refusalCause) {
				t.Fatalf("Run() = %v, want the broker refusal %v", runErr, refusalCause)
			}
			healthErr := containment.health(t)
			if !errors.Is(healthErr, refusalCause) {
				t.Fatalf("Health() = %v, want the broker refusal %v", healthErr, refusalCause)
			}
			if !strings.Contains(healthErr.Error(), "subscription "+containmentSubscription+" failed") {
				t.Fatalf("Health() = %v, want the %s subscription named", healthErr, containmentSubscription)
			}
			if acked, nacked := settler.settled(); acked || nacked {
				t.Fatalf("original settlement = acked %t nacked %t, want it left unsettled for redelivery", acked, nacked)
			}
			if !containment.released() {
				t.Fatal("the consumer was not released: nothing would redeliver the still-unsettled original")
			}
			if got, want := containment.broker.refusedDestinations(), []string{testCase.destination}; !slices.Equal(got, want) {
				t.Fatalf("refused destinations = %v, want %v", got, want)
			}
			_, callErr := containment.waitForReport(t)
			if _, ok := errors.AsType[*driver.Error](callErr); !ok {
				t.Fatalf("error handler error = %q, want the classified handoff failure", callErr)
			}
			if strings.HasPrefix(callErr.Error(), "f1: message dropped: event_id=") {
				t.Fatalf("error handler error = %q, want a handoff failure rather than a drop", callErr)
			}
			if got := containment.recorder.count(); got != 1 {
				t.Fatalf("error handler calls = %d, want 1", got)
			}
		})
	}
}
