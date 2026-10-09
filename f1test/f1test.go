// Package f1test provides deterministic handler-test helpers backed by an
// in-memory client, a manually advanced clock, and an observer recorder.
package f1test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/testhook"
)

// Client wraps an f1.Client backed by the in-memory driver and a fake clock. It
// embeds f1.Client and adds deterministic delivery, capture, and fake-time
// helpers for handler tests. Create clients with [NewClient]; the zero value is
// not initialized.
type Client struct {
	*f1.Client

	clock    *clock.Fake
	captures *captureStore
	state    *captureState
}

// Captured is one message accepted by the in-memory driver's producer.
// EventType and Headers are copied from the message's envelope headers, and
// Payload is a copy of the encoded body.
type Captured struct {
	EventType string
	Payload   []byte
	Headers   map[string]string
}

// NewClient returns a Client backed by the in-memory driver with a fake clock
// and registers cleanup to close it when t completes. It fails the test if
// initialization fails or closing the client fails during cleanup.
func NewClient(t *testing.T, opts ...f1.Option) *Client {
	t.Helper()

	client, err := newClient(opts...)
	if err != nil {
		t.Fatalf("f1test: create client: %v", err)
	}
	t.Cleanup(func() {
		if err := closeClient(client); err != nil {
			t.Errorf("f1test: close client: %v", err)
		}
	})
	return client
}

func closeClient(client *Client) error {
	return client.Close(context.Background())
}

func newClient(opts ...f1.Option) (*Client, error) {
	fake := clock.NewFake(time.Unix(0, 0))
	captures := &captureStore{signal: make(chan struct{})}
	state := &captureState{
		destinations: make(map[string]chan struct{}),
		ready:        make(map[string]bool),
		kinds:        make(map[string]driver.DestKind),
		waiting:      make(chan string, 16),
		released:     make(chan struct{}, 16),
	}
	options := append([]f1.Option(nil), opts...)
	// These options are last so every helper always observes the same fake
	// clock that the in-memory driver's deferred-delivery queue uses.
	options = append(options,
		f1.WithDriver(captureDriver{inner: testhook.Driver(fake).(inmem.Driver), captures: captures, state: state}),
		testhook.ClientOption(fake).(f1.Option),
	)
	core, err := f1.New(context.Background(), testConfig(), options...)
	if err != nil {
		return nil, err
	}
	return &Client{Client: core, clock: fake, captures: captures, state: state}, nil
}

func testConfig() f1.Config {
	return f1.Config{
		Env:        "test",
		Service:    "f1test",
		InstanceID: "f1test",
		Broker: f1.BrokerConfig{
			Driver:          "inmem",
			ConnectTimeout:  30 * time.Second,
			DefaultPrefetch: 64,
		},
		Topology: f1.TopologyConfig{
			AutoCreate:    true,
			VerifyOnStart: true,
			Priorities:    []f1.Priority{f1.PriorityHigh, f1.PriorityMedium, f1.PriorityLow},
		},
		Codec: f1.CodecConfig{
			Default:        "json",
			MaxHeaderBytes: f1.CoreMaxHeaderBytes,
			MaxBodyBytes:   1 << 20,
		},
		Lifecycle: f1.LifecycleConfig{
			DrainTimeout:          time.Minute,
			HandlerGrace:          5 * time.Second,
			CloseTimeout:          10 * time.Second,
			RebalanceDrainTimeout: 25 * time.Second,
		},
		Subscriptions: map[string]f1.SubscriptionConfig{},
	}
}

// Deliver waits up to one second for the event's destination to become ready,
// then publishes the event through the in-memory broker. A running subscription
// is required for its handler to receive the event; Deliver does not wait for
// the handler to process it. Deliver fails the test if the destination is not
// ready before the deadline or publishing fails.
func (c *Client) Deliver(t *testing.T, eventType string, payload any, opts ...f1.PublishOption) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := deliver(c, ctx, eventType, payload, opts...); err != nil {
		t.Fatalf("f1test: deliver %q: %v", eventType, err)
	}
}

func deliver(c *Client, ctx context.Context, eventType string, payload any, opts ...f1.PublishOption) error {
	if c == nil || c.Client == nil {
		return errors.New("f1test: nil client")
	}
	if _, err := c.Publisher().Publish(ctx, eventType, payload, opts...); err != nil {
		return err
	}
	return nil
}

// Published returns and clears messages accepted by the in-memory driver that
// were not sent to a dead-letter destination. A nil receiver returns nil.
func (c *Client) Published() []Captured {
	if c == nil || c.captures == nil {
		return nil
	}
	return c.captures.take(false)
}

// DLQ returns and clears messages accepted by the in-memory driver whose
// destination is a dead-letter destination. A nil receiver returns nil.
func (c *Client) DLQ() []Captured {
	if c == nil || c.captures == nil {
		return nil
	}
	return c.captures.take(true)
}

// Advance adds d to the fake clock and releases deferred messages due at the
// resulting time before returning. A nil receiver is a no-op.
func (c *Client) Advance(d time.Duration) {
	if c == nil || c.clock == nil {
		return
	}
	c.clock.Advance(d)
	if c.state == nil {
		return
	}
	// A reconnect reopens the driver and replaces releaseDue, so it is read
	// under the lock the Open hook writes it under.
	c.state.mu.Lock()
	releaseDue := c.state.releaseDue
	c.state.mu.Unlock()
	if releaseDue != nil {
		releaseDue()
	}
}

type captureState struct {
	mu           sync.Mutex
	destinations map[string]chan struct{}
	ready        map[string]bool
	kinds        map[string]driver.DestKind
	waiting      chan string
	releaseDue   func()
	released     chan struct{}
}

func (s *captureState) destinationGate(name string) (chan struct{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	gate := s.destinations[name]
	ready := s.ready[name]
	if gate == nil {
		gate = make(chan struct{})
		s.destinations[name] = gate
		if ready {
			close(gate)
		}
	}
	return gate, ready
}

// markReady records the spec's destinations as ready to receive and remembers
// the kind the core declared for each one, which is what tells a dead-letter
// destination from a published one: a topic is free to spell "dlq" in its name
// without becoming a dead-letter destination.
func (s *captureState) markReady(specs []driver.DestinationSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, spec := range specs {
		s.kinds[spec.Name] = spec.Kind
		gate := s.destinations[spec.Name]
		if gate == nil {
			gate = make(chan struct{})
			s.destinations[spec.Name] = gate
		}
		if s.ready[spec.Name] {
			continue
		}
		s.ready[spec.Name] = true
		close(gate)
	}
}

// deadLetterDestination reports whether the core declared name as a
// dead-letter destination, one that holds messages no other destination
// accepted. A destination the core never declared is not one: without a
// declared kind the capture has nothing but the name to go on, and a name is
// not a kind.
func (s *captureState) deadLetterDestination(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.kinds[name] {
	case driver.DestDLQ, driver.DestBackstopDLQ:
		return true
	default:
		return false
	}
}

func (s *captureState) waitDestination(ctx context.Context, name string) error {
	gate, ready := s.destinationGate(name)
	if !ready {
		select {
		case s.waiting <- name:
		default:
		}
	}
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("f1test: destination %q was not ready: %w", name, ctx.Err())
	}
}

type captureStore struct {
	mu        sync.Mutex
	published []Captured
	dlq       []Captured
	// signal is closed by add while published is non-empty. take can be called
	// for a DLQ drain while a waiter has already selected on this open channel;
	// it must not replace that channel unless add closed it, or that waiter
	// would wait forever for a close on a channel no future add can close.
	signal chan struct{}
}

// add records the messages a producer accepted, separating those the core
// declared as dead-letter destinations from the published ones. state supplies
// the declared kind of each destination; a message to a destination with no
// declared kind is published.
func (s *captureStore) add(state *captureState, messages []driver.OutboundMessage, err error) {
	if s == nil {
		return
	}
	failed := map[int]struct{}{}
	if partial, ok := errors.AsType[*driver.PublishError](err); ok {
		for index := range partial.Failed {
			failed[index] = struct{}{}
		}
	} else if err != nil {
		return
	}

	s.mu.Lock()
	published := false
	for index, message := range messages {
		if _, ok := failed[index]; ok {
			continue
		}
		captured := captureMessage(message)
		if state != nil && state.deadLetterDestination(message.Destination) {
			s.dlq = append(s.dlq, captured)
			continue
		}
		s.published = append(s.published, captured)
		published = true
	}
	if published && s.signal != nil {
		select {
		case <-s.signal:
		default:
			close(s.signal)
		}
	}
	s.mu.Unlock()
}

func (s *captureStore) waitPublished(ctx context.Context) error {
	for {
		s.mu.Lock()
		published := len(s.published) > 0
		signal := s.signal
		s.mu.Unlock()
		select {
		case <-signal:
		case <-ctx.Done():
			return ctx.Err()
		}
		if published {
			return nil
		}
	}
}

func (s *captureStore) take(dlq bool) []Captured {
	s.mu.Lock()
	defer s.mu.Unlock()
	var source *[]Captured
	if dlq {
		source = &s.dlq
	} else {
		source = &s.published
	}
	result := append([]Captured(nil), (*source)...)
	*source = nil
	if len(s.published) == 0 {
		select {
		case <-s.signal:
			s.signal = make(chan struct{})
		default:
		}
	}
	return result
}

func captureMessage(message driver.OutboundMessage) Captured {
	headers := make(map[string]string, len(message.Headers))
	eventType := ""
	for _, header := range message.Headers {
		value := string(header.Value)
		headers[header.Key] = value
		if header.Key == "type" {
			eventType = value
		}
	}
	return Captured{EventType: eventType, Payload: append([]byte(nil), message.Body...), Headers: headers}
}

type captureDriver struct {
	inner    inmem.Driver
	captures *captureStore
	state    *captureState
}

func (d captureDriver) Name() string                      { return d.inner.Name() }
func (d captureDriver) Capabilities() driver.Capabilities { return d.inner.Capabilities() }
func (d captureDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	conn, err := d.inner.Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if advancer, ok := conn.(interface{ ReleaseDue() }); ok && d.state != nil {
		d.state.mu.Lock()
		d.state.releaseDue = func() {
			advancer.ReleaseDue()
			select {
			case d.state.released <- struct{}{}:
			default:
			}
		}
		d.state.mu.Unlock()
	}
	return &captureConn{Conn: conn, captures: d.captures, state: d.state}, nil
}

type captureConn struct {
	driver.Conn
	captures *captureStore
	state    *captureState
}

func (c *captureConn) Admin() driver.Admin {
	admin := c.Conn.Admin()
	if admin == nil {
		return nil
	}
	captured := &captureAdmin{Admin: admin, state: c.state}
	maintenance, ok := admin.(driver.Maintenance)
	if !ok {
		return captured
	}
	return &captureMaintenanceAdmin{captureAdmin: captured, maintenance: maintenance}
}

type captureAdmin struct {
	driver.Admin
	state *captureState
}

type captureMaintenanceAdmin struct {
	*captureAdmin
	maintenance driver.Maintenance
}

func (a *captureMaintenanceAdmin) Purge(ctx context.Context, destination string) (int64, error) {
	return a.maintenance.Purge(ctx, destination)
}

func (a *captureMaintenanceAdmin) Prune(ctx context.Context, names []string) ([]driver.PruneResult, error) {
	return a.maintenance.Prune(ctx, names)
}

func (a *captureAdmin) EnsureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	diff, err := a.Admin.EnsureTopology(ctx, spec)
	if err == nil && a.state != nil {
		a.state.markReady(spec.Destinations)
	}
	return diff, err
}

func (c *captureConn) Producer(ctx context.Context, cfg driver.ProducerConfig) (driver.Producer, error) {
	producer, err := c.Conn.Producer(ctx, cfg)
	if err != nil || producer == nil {
		return producer, err
	}
	return &captureProducer{Producer: producer, captures: c.captures, state: c.state}, nil
}

type captureProducer struct {
	driver.Producer
	captures *captureStore
	state    *captureState
}

// destinationWait bounds how long a publish waits for its destination to be
// declared. A test often publishes right after starting Run, before the
// runner's topology exists, so the publish waits for it; a destination that
// nothing declares is not coming, and the publish then reaches the driver and
// returns its error instead of waiting for the caller's context to end.
const destinationWait = time.Second

func (p *captureProducer) Publish(ctx context.Context, messages ...driver.OutboundMessage) error {
	if p.state != nil {
		waitCtx, cancel := context.WithTimeout(ctx, destinationWait)
		for _, message := range messages {
			if err := p.state.waitDestination(waitCtx, message.Destination); err != nil {
				if ctx.Err() != nil {
					cancel()
					return err
				}
				break
			}
		}
		cancel()
	}
	err := p.Producer.Publish(ctx, messages...)
	p.captures.add(p.state, messages, err)
	return err
}
