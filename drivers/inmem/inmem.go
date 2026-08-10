package inmem

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// Driver is a stateless in-memory driver factory. Clock selects the time
// source used by connections; a nil clock uses the real clock.
type Driver struct{ Clock clock.Clock }

const (
	// These limits exercise the driver's limit-reporting and enforcement paths;
	// they are deliberately independent of the in-memory store's capacity.
	maxMessageBytes = 1 << 20
	maxHeaderBytes  = 64 << 10
)

var _ driver.Driver = Driver{}

// New returns a driver using c for all time-dependent behavior.
func New(c clock.Clock) Driver { return Driver{Clock: c} }

// Name returns the stable in-memory driver key.
func (Driver) Name() string { return "inmem" }

// Capabilities reports the behavior implemented by the in-memory driver. Native DLQ,
// priority, transactions, and server-side filtering are intentionally false.
func (Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		PerMessageAck:       true,
		OrderedByKey:        true,
		NativeDelay:         true,
		NativeDeliveryCount: true,
		ConsumerScaling:     driver.ScalingFree,
		LagQueryable:        true,
		MaxMessageBytes:     maxMessageBytes,
		MaxHeaderBytes:      maxHeaderBytes,
	}
}

// Open creates an isolated in-memory connection.
func (d Driver) Open(ctx context.Context, _ driver.Config) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("open", driver.KindTransient, err)
	}
	c := d.Clock
	if c == nil {
		c = clock.NewReal()
	}
	conn := &conn{
		clock:        c,
		caps:         d.Capabilities(),
		info:         driver.BrokerInfo{Kind: "inmem", Version: "1"},
		destinations: make(map[string]*destination),
		consumers:    make(map[*consumer]struct{}),
		wake:         make(chan struct{}, 1),
		done:         make(chan struct{}),
		pumpDone:     make(chan struct{}),
	}
	go conn.pump()
	return conn, nil
}

type conn struct {
	mu sync.Mutex

	clock        clock.Clock
	caps         driver.Capabilities
	info         driver.BrokerInfo
	destinations map[string]*destination
	consumers    map[*consumer]struct{}
	producers    int
	closed       bool
	nextRef      atomic.Uint64
	wake         chan struct{}
	done         chan struct{}
	pumpDone     chan struct{}
	closeOnce    sync.Once
}

var _ driver.Conn = (*conn)(nil)

type destination struct {
	spec      driver.DestinationSpec
	messages  []*queuedMessage
	consumers map[*consumer]struct{}
	affinity  map[string]*consumer
	order     []*consumer
	next      int
}

type queuedMessage struct {
	message       driver.OutboundMessage
	deliveryCount int
	due           time.Time
}

func (c *conn) Capabilities() driver.Capabilities { return c.caps }
func (c *conn) BrokerInfo() driver.BrokerInfo     { return c.info }
func (c *conn) Admin() driver.Admin               { return &admin{conn: c} }

func (c *conn) Producer(ctx context.Context, cfg driver.ProducerConfig) (driver.Producer, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("producer", driver.KindTransient, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, classify("producer", driver.KindFatal, driver.ErrResourcesOutstanding)
	}
	c.producers++
	return &producer{conn: c, cfg: cfg}, nil
}

func (c *conn) Consumer(ctx context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("consumer", driver.KindTransient, err)
	}
	if len(cfg.Destinations) == 0 {
		return nil, classify("consumer", driver.KindFatal, errors.New("no destinations"))
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, classify("consumer", driver.KindFatal, driver.ErrResourcesOutstanding)
	}
	for _, name := range cfg.Destinations {
		if _, ok := c.destinations[name]; !ok {
			return nil, classify("consumer", driver.KindNotFound, driver.ErrDestinationMissing)
		}
	}
	for _, name := range cfg.Destinations {
		for existing := range c.destinations[name].consumers {
			if cfg.Exclusive || existing.cfg.Exclusive {
				return nil, classify("consumer", driver.KindFatal, errors.New("exclusive consumer already attached"))
			}
		}
	}
	capacity := cfg.Prefetch
	if capacity < 1 {
		capacity = 1
	}
	cs := &consumer{
		conn:         c,
		cfg:          cfg,
		destinations: append([]string(nil), cfg.Destinations...),
		messages:     make(chan driver.InboundMessage, capacity),
		errs:         make(chan error, 1),
		paused:       make(map[string]bool),
		unsettled:    make(map[string]int),
	}
	for _, name := range cfg.Destinations {
		c.destinations[name].consumers[cs] = struct{}{}
		c.destinations[name].order = append(c.destinations[name].order, cs)
	}
	c.consumers[cs] = struct{}{}
	c.dispatchLocked()
	c.signalWake()
	return cs, nil
}

func (c *conn) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("ping", driver.KindTransient, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return classify("ping", driver.KindTransient, errors.New("connection closed"))
	}
	return nil
}

func (c *conn) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("close", driver.KindTransient, err)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	if c.producers != 0 || len(c.consumers) != 0 {
		c.mu.Unlock()
		return classify("close", driver.KindFatal, driver.ErrResourcesOutstanding)
	}
	c.closed = true
	c.closeOnce.Do(func() { close(c.done) })
	c.mu.Unlock()
	<-c.pumpDone
	return nil
}

func (c *conn) signalWake() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *conn) pump() {
	defer close(c.pumpDone)
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return
		}
		c.dispatchLocked()
		next, ok := c.nextDueLocked()
		c.mu.Unlock()
		if !ok {
			select {
			case <-c.wake:
			case <-c.done:
				return
			}
			continue
		}
		d := next.Sub(c.clock.Now())
		if d < 0 {
			d = 0
		}
		t := c.clock.Timer(d)
		select {
		case <-t.C:
		case <-c.wake:
			t.Stop()
		case <-c.done:
			t.Stop()
			return
		}
	}
}

func (c *conn) nextDueLocked() (time.Time, bool) {
	var next time.Time
	for _, dest := range c.destinations {
		if len(dest.messages) == 0 || dest.messages[0].due.IsZero() {
			continue
		}
		if next.IsZero() || dest.messages[0].due.Before(next) {
			next = dest.messages[0].due
		}
	}
	return next, !next.IsZero()
}

func (c *conn) dispatchLocked() {
	now := c.clock.Now()
destinationLoop:
	for _, name := range c.destinationNamesLocked() {
		dest := c.destinations[name]
		for len(dest.messages) > 0 {
			msg := dest.messages[0]
			if !msg.due.IsZero() && now.Before(msg.due) {
				break
			}
			cs := dest.pickConsumer(msg.message.Key)
			if cs == nil {
				break
			}
			inbound := c.inboundLocked(cs, dest.spec.Name, msg)
			select {
			case cs.messages <- inbound:
				cs.outstanding++
				cs.unsettled[dest.spec.Name]++
				dest.messages = dest.messages[1:]
			default:
				continue destinationLoop
			}
		}
	}
}

func (c *conn) destinationNamesLocked() []string {
	names := make([]string, 0, len(c.destinations))
	for name := range c.destinations {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (d *destination) pickConsumer(key []byte) *consumer {
	eligible := make([]*consumer, 0, len(d.order))
	for _, cs := range d.order {
		if !cs.stopped && !cs.draining && !cs.paused[d.spec.Name] {
			eligible = append(eligible, cs)
		}
	}
	if len(eligible) == 0 {
		return nil
	}
	if len(key) != 0 {
		if cs := d.affinity[string(key)]; cs != nil {
			if !cs.stopped && !cs.draining && !cs.paused[d.spec.Name] {
				return cs
			}
			return nil
		}
	}
	cs := eligible[d.next%len(eligible)]
	d.next++
	if len(key) != 0 {
		d.affinity[string(key)] = cs
	}
	return cs
}

func (c *conn) inboundLocked(cs *consumer, destination string, msg *queuedMessage) driver.InboundMessage {
	c.nextRef.Add(1)
	return driver.InboundMessage{
		Destination:   destination,
		Key:           cloneBytes(msg.message.Key),
		Headers:       cloneHeaders(msg.message.Headers),
		Body:          cloneBytes(msg.message.Body),
		DeliveryCount: deliveryCount(cs.cfg.Effective, msg.deliveryCount),
		ReceivedAt:    c.clock.Now(),
		Ref:           driver.BrokerRef{Tag: c.nextRef.Load(), Raw: destination},
		Settle:        &settler{conn: c, consumer: cs, message: msg},
	}
}

func deliveryCount(caps driver.Capabilities, count int) int {
	if !caps.NativeDeliveryCount {
		return -1
	}
	return count
}

func cloneBytes(v []byte) []byte { return append([]byte(nil), v...) }

func cloneHeaders(v []driver.Header) []driver.Header {
	out := make([]driver.Header, len(v))
	for i, h := range v {
		out[i] = driver.Header{Key: h.Key, Value: cloneBytes(h.Value)}
	}
	return out
}

func classify(op string, kind driver.Kind, err error) error {
	if err == nil {
		return nil
	}
	return &driver.Error{Driver: "inmem", Op: op, K: kind, Err: err}
}
