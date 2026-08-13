// Package f1test provides deterministic handler tests backed by the in-memory
// driver and a manually advanced clock.
package f1test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// Client wraps an f1.Client backed by the in-memory driver and a fake clock.
// The embedded client exposes the normal Subscribe and Runner APIs; the
// methods declared here provide deterministic publishing and observation
// helpers for handler tests.
type Client struct {
	*f1.Client

	clock     *clock.Fake
	timer20ms <-chan struct{}
	captures  *captureStore
	state     *captureState
}

// Captured is one message accepted by the in-memory driver's producer.
// EventType and Headers are copied from the message's envelope headers, and
// Payload is a copy of the encoded body.
type Captured struct {
	EventType string
	Payload   []byte
	Headers   map[string]string
}

// NewClient returns a Client backed by the in-memory driver with a fake clock.
// The client is closed automatically through t.Cleanup.
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
	timer20ms := make(chan struct{})
	observedClock := &observedClock{Fake: fake, delay: 20 * time.Millisecond, armed: timer20ms}
	captures := &captureStore{publishedSignal: make(chan struct{}, 1)}
	state := &captureState{
		destinations: make(map[string]chan struct{}),
		ready:        make(map[string]bool),
		waiting:      make(chan string, 16),
		released:     make(chan struct{}, 16),
	}
	options := append([]f1.Option(nil), opts...)
	// These options are last so every helper always observes the same fake
	// clock that the in-memory driver's deferred-delivery queue uses.
	options = append(options,
		f1.WithDriver(captureDriver{inner: inmem.New(observedClock), captures: captures, state: state}),
		f1.WithClock(observedClock),
	)
	core, err := f1.New(context.Background(), testConfig(), options...)
	if err != nil {
		return nil, err
	}
	return &Client{Client: core, clock: fake, timer20ms: timer20ms, captures: captures, state: state}, nil
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
			AutoCreate:        true,
			VerifyOnStart:     true,
			PartitionsDefault: 12,
			RetentionDefault:  7 * 24 * time.Hour,
			DLQRetention:      30 * 24 * time.Hour,
			Priorities:        []f1.Priority{f1.PriorityHigh, f1.PriorityNormal, f1.PriorityLow},
		},
		Codec: f1.CodecConfig{
			Default:        "json",
			ContentMode:    "binary",
			MaxHeaderBytes: f1.CoreMaxHeaderBytes,
			MaxBodyBytes:   1 << 20,
		},
		Lifecycle: f1.LifecycleConfig{
			DrainTimeout:          time.Minute,
			HandlerGrace:          5 * time.Second,
			FlushTimeout:          20 * time.Second,
			CloseTimeout:          10 * time.Second,
			RebalanceDrainTimeout: 25 * time.Second,
		},
		Subscriptions: map[string]f1.SubscriptionConfig{},
	}
}

// Deliver waits for the event's destination to be created, then publishes an
// event through the in-memory broker. A subscription returned by
// Client.Subscribe must be running for its handler to receive it. The call
// fails the test when the destination is not ready or the message is not
// accepted by the driver.
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
// were not sent to a dead-letter destination.
func (c *Client) Published() []Captured {
	if c == nil || c.captures == nil {
		return nil
	}
	return c.captures.take(false)
}

// DLQ returns and clears messages accepted by the in-memory driver whose
// destination is a dead-letter destination.
func (c *Client) DLQ() []Captured {
	if c == nil || c.captures == nil {
		return nil
	}
	return c.captures.take(true)
}

// Advance moves the fake clock and releases due deferred messages from the
// in-memory driver before returning. The same concrete clock is supplied to
// the core and driver, so both sides observe the new instant.
func (c *Client) Advance(d time.Duration) {
	if c == nil || c.clock == nil {
		return
	}
	c.clock.Advance(d)
	if c.state != nil && c.state.releaseDue != nil {
		c.state.releaseDue()
	}
}

type captureState struct {
	mu           sync.Mutex
	destinations map[string]chan struct{}
	ready        map[string]bool
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

func (s *captureState) markReady(specs []driver.DestinationSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, spec := range specs {
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
	mu              sync.Mutex
	published       []Captured
	dlq             []Captured
	publishedSignal chan struct{}
}

func (s *captureStore) add(messages []driver.OutboundMessage, err error) {
	if s == nil {
		return
	}
	failed := map[int]struct{}{}
	var partial *driver.PublishError
	if errors.As(err, &partial) {
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
		if strings.Contains(message.Destination, ".dlq.") {
			s.dlq = append(s.dlq, captured)
		} else {
			s.published = append(s.published, captured)
			published = true
		}
	}
	s.mu.Unlock()
	if published && s.publishedSignal != nil {
		select {
		case s.publishedSignal <- struct{}{}:
		default:
		}
	}
}

func (s *captureStore) waitPublished(ctx context.Context) error {
	for {
		s.mu.Lock()
		published := len(s.published) > 0
		s.mu.Unlock()
		if published {
			return nil
		}
		select {
		case <-s.publishedSignal:
		case <-ctx.Done():
			return ctx.Err()
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
		d.state.releaseDue = func() {
			advancer.ReleaseDue()
			select {
			case d.state.released <- struct{}{}:
			default:
			}
		}
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
	return &captureAdmin{Admin: admin, state: c.state}
}

type captureAdmin struct {
	driver.Admin
	state *captureState
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

func (p *captureProducer) Publish(ctx context.Context, messages ...driver.OutboundMessage) error {
	if p.state != nil {
		for _, message := range messages {
			if err := p.state.waitDestination(ctx, message.Destination); err != nil {
				return err
			}
		}
	}
	err := p.Producer.Publish(ctx, messages...)
	p.captures.add(messages, err)
	return err
}

type observedClock struct {
	*clock.Fake
	delay time.Duration
	armed chan struct{}
	once  sync.Once
}

func (c *observedClock) Timer(d time.Duration) clock.Timer {
	if d == c.delay {
		c.once.Do(func() { close(c.armed) })
	}
	return c.Fake.Timer(d)
}
