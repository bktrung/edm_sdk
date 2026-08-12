// Package f1test provides deterministic handler tests backed by the in-memory
// driver and a manually advanced clock.
package f1test

import (
	"context"
	"errors"
	"runtime"
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

	clock    *clock.Fake
	captures *captureStore
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
	captures := &captureStore{}
	options := append([]f1.Option(nil), opts...)
	// These options are last so every helper always observes the same fake
	// clock that the in-memory driver's deferred-delivery queue uses.
	options = append(options,
		f1.WithDriver(captureDriver{inner: inmem.New(fake), captures: captures}),
		f1.WithClock(fake),
	)
	core, err := f1.New(context.Background(), testConfig(), options...)
	if err != nil {
		return nil, err
	}
	return &Client{Client: core, clock: fake, captures: captures}, nil
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

// Deliver publishes an event through the in-memory broker. A subscription
// returned by Client.Subscribe must be running for its handler to receive it.
// The call fails the test when the message is not accepted by the driver.
func (c *Client) Deliver(t *testing.T, eventType string, payload any, opts ...f1.PublishOption) {
	t.Helper()
	if c == nil || c.Client == nil {
		t.Fatal("f1test: nil client")
	}
	for attempt := 0; attempt < 1000; attempt++ {
		if _, err := c.Publisher().Publish(context.Background(), eventType, payload, opts...); err == nil {
			return
		} else if !errors.Is(err, driver.ErrDestinationMissing) {
			t.Fatalf("f1test: deliver %q: %v", eventType, err)
		}
		runtime.Gosched()
	}
	t.Fatalf("f1test: deliver %q: subscription topology was not ready", eventType)
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

// Advance moves the fake clock. The same concrete clock is supplied to both
// the core and in-memory driver, so advancing it also fires deferred retry
// deliveries owned by the driver.
func (c *Client) Advance(d time.Duration) {
	if c == nil || c.clock == nil {
		return
	}
	c.clock.Advance(d)
	// A producer can signal the driver's pump immediately before this method
	// runs. Give that pump a chance to register its due timer, then drain timers
	// that became due at the new instant without introducing real-time sleeps.
	for i := 0; i < 32; i++ {
		runtime.Gosched()
		c.clock.Advance(0)
	}
}

type captureStore struct {
	mu        sync.Mutex
	published []Captured
	dlq       []Captured
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
	defer s.mu.Unlock()
	for index, message := range messages {
		if _, ok := failed[index]; ok {
			continue
		}
		captured := captureMessage(message)
		if strings.Contains(message.Destination, ".dlq.") {
			s.dlq = append(s.dlq, captured)
		} else {
			s.published = append(s.published, captured)
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
}

func (d captureDriver) Name() string                      { return d.inner.Name() }
func (d captureDriver) Capabilities() driver.Capabilities { return d.inner.Capabilities() }
func (d captureDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	conn, err := d.inner.Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &captureConn{Conn: conn, captures: d.captures}, nil
}

type captureConn struct {
	driver.Conn
	captures *captureStore
}

func (c *captureConn) Producer(ctx context.Context, cfg driver.ProducerConfig) (driver.Producer, error) {
	producer, err := c.Conn.Producer(ctx, cfg)
	if err != nil || producer == nil {
		return producer, err
	}
	return &captureProducer{Producer: producer, captures: c.captures}, nil
}

type captureProducer struct {
	driver.Producer
	captures *captureStore
}

func (p *captureProducer) Publish(ctx context.Context, messages ...driver.OutboundMessage) error {
	err := p.Producer.Publish(ctx, messages...)
	p.captures.add(messages, err)
	return err
}
