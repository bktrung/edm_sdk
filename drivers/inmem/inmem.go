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
// source used by connections; a nil clock uses the real clock. The minimal
// capability mode is conformance-only and intentionally unexported.
type Driver struct {
	Clock   clock.Clock
	minimal bool
}

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
func (d Driver) Capabilities() driver.Capabilities {
	caps := driver.Capabilities{
		PerMessageAck:       true,
		OrderedByKey:        true,
		NativeDelay:         true,
		NativeDeliveryCount: true,
		ConsumerScaling:     driver.ScalingFree,
		LagQueryable:        true,
		MaxMessageBytes:     maxMessageBytes,
		MaxHeaderBytes:      maxHeaderBytes,
	}
	if d.minimal {
		// This is the one deliberate non-native declaration: the capability
		// suite needs to execute both sides of preserved declarations. The
		// driver still implements the partition-bound behavior (one consumer
		// is exercised), while the zero limits and denied ordering path are
		// enforced from Effective exactly like the normal declaration.
		caps.OrderedByKey = false
		caps.ConsumerScaling = driver.ScalingPartitionBound
		caps.MaxMessageBytes = 0
		caps.MaxHeaderBytes = 0
	}
	return caps
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
		history:      make(map[string][]*queuedMessage),
		consumers:    make(map[*consumer]struct{}),
		groups:       make(map[string]*groupState),
		wake:         make(chan struct{}, 1),
		done:         make(chan struct{}),
		pumpDone:     make(chan struct{}),
	}
	go conn.pump()
	return conn, nil
}

type conn struct {
	mu sync.Mutex

	clock            clock.Clock
	caps             driver.Capabilities
	info             driver.BrokerInfo
	destinations     map[string]*destination
	history          map[string][]*queuedMessage
	consumers        map[*consumer]struct{}
	groups           map[string]*groupState
	producers        int
	closed           bool
	failPublish      bool
	failPublishFatal bool
	failNextAck      bool
	failNextNack     bool
	ackFailures      uint64
	nackFailures     uint64
	nextRef          atomic.Uint64
	nextMessage      atomic.Uint64
	wake             chan struct{}
	done             chan struct{}
	pumpDone         chan struct{}
	closeOnce        sync.Once
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

type groupState struct {
	positions map[string]uint64
}

type queuedMessage struct {
	message         driver.OutboundMessage
	deliveryCount   int
	sequence        uint64
	due             time.Time
	deliveredGroups map[string]bool
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
	return c.newConsumer(ctx, cfg, 0)
}

func (c *conn) newConsumer(ctx context.Context, cfg driver.ConsumerConfig, ackDeadline time.Duration) (driver.Consumer, error) {
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
	var group *groupState
	createdGroup := false
	startAfter := make(map[string]uint64, len(cfg.Destinations))
	if cfg.Group != "" {
		group = c.groups[cfg.Group]
		if group == nil {
			createdGroup = true
			group = &groupState{positions: make(map[string]uint64, len(cfg.Destinations))}
			c.groups[cfg.Group] = group
			if cfg.StartAt == driver.StartLatest {
				for _, name := range cfg.Destinations {
					group.positions[name] = c.nextMessage.Load()
				}
			}
		}
		for _, name := range cfg.Destinations {
			startAfter[name] = group.positions[name]
		}
		if createdGroup && cfg.StartAt != driver.StartLatest {
			c.replayHistoryLocked(cfg.Group, cfg.Destinations, startAfter)
		}
	} else if cfg.StartAt == driver.StartLatest {
		for _, name := range cfg.Destinations {
			startAfter[name] = c.nextMessage.Load()
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
		unsettledKey: make(map[deliveryKey]int),
		group:        group,
		startAfter:   startAfter,
		ackDeadline:  ackDeadline,
		inflight:     make(map[*settler]struct{}),
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

func (c *conn) replayHistoryLocked(group string, destinations []string, startAfter map[string]uint64) {
	for _, name := range destinations {
		dest := c.destinations[name]
		current := make(map[uint64]struct{}, len(dest.messages))
		for _, message := range dest.messages {
			current[message.sequence] = struct{}{}
		}
		for _, original := range c.history[name] {
			if original.sequence <= startAfter[name] {
				continue
			}
			if _, ok := current[original.sequence]; ok {
				continue
			}
			deliveredGroups := make(map[string]bool, len(c.groups))
			for existing := range c.groups {
				deliveredGroups[existing] = existing != group
			}
			dest.messages = append(dest.messages, &queuedMessage{
				message:         cloneOutbound(original.message),
				deliveryCount:   original.deliveryCount,
				sequence:        original.sequence,
				due:             original.due,
				deliveredGroups: deliveredGroups,
			})
		}
	}
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

// ReleaseDue dispatches deferred messages whose due time has arrived.
// It lets deterministic test helpers coordinate fake time with the driver's
// deferred-delivery storage without adding test methods to the driver port.
func (c *conn) ReleaseDue() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.dispatchLocked()
	c.signalWake()
}

func (c *conn) pump() {
	defer close(c.pumpDone)
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return
		}
		c.expireDeadlinesLocked()
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
	for consumer := range c.consumers {
		if consumer.ackDeadline <= 0 {
			continue
		}
		for delivery := range consumer.inflight {
			due := delivery.deliveredAt.Add(consumer.ackDeadline)
			if next.IsZero() || due.Before(next) {
				next = due
			}
		}
	}
	return next, !next.IsZero()
}

func (c *conn) expireDeadlinesLocked() {
	now := c.clock.Now()
	for consumer := range c.consumers {
		if consumer.ackDeadline <= 0 {
			continue
		}
		for delivery := range consumer.inflight {
			if now.Before(delivery.deliveredAt.Add(consumer.ackDeadline)) {
				continue
			}
			c.requeueDeliveryLocked(delivery, now)
		}
	}
}

func (c *conn) requeueDeliveryLocked(delivery *settler, now time.Time) {
	delivery.mu.Lock()
	if delivery.settled {
		delivery.mu.Unlock()
		return
	}
	delivery.settled = true
	delivery.mu.Unlock()
	consumer := delivery.consumer
	delete(consumer.inflight, delivery)
	if consumer.outstanding > 0 {
		consumer.outstanding--
	}
	name := delivery.message.message.Destination
	if consumer.unsettled[name] > 0 {
		consumer.unsettled[name]--
	}
	key := string(delivery.message.message.Key)
	if key != "" {
		keyID := deliveryKey{destination: name, key: key}
		if consumer.unsettledKey[keyID] > 1 {
			consumer.unsettledKey[keyID]--
		} else {
			delete(consumer.unsettledKey, keyID)
			if dest, ok := c.destinations[name]; ok && dest.affinity[key] == consumer {
				delete(dest.affinity, key)
			}
		}
	}
	delivery.message.deliveryCount++
	delivery.message.due = now
	if delivery.message.deliveredGroups != nil {
		delete(delivery.message.deliveredGroups, consumer.cfg.Group)
	}
	if dest, ok := c.destinations[name]; ok {
		dest.messages = append([]*queuedMessage{delivery.message}, dest.messages...)
	}
}

func (c *conn) dropInFlightLocked(op string) {
	now := c.clock.Now()
	for consumer := range c.consumers {
		select {
		case consumer.errs <- classify(op, driver.KindTransient, errors.New("injected connection fault")):
		default:
		}
		for delivery := range consumer.inflight {
			c.requeueDeliveryLocked(delivery, now)
		}
	}
	c.dispatchLocked()
	c.signalWake()
}

func (c *conn) dispatchLocked() {
	now := c.clock.Now()
destinationLoop:
	for _, name := range c.destinationNamesLocked() {
		dest := c.destinations[name]
		for len(dest.messages) > 0 {
			delivered := false
			for i, msg := range dest.messages {
				if !msg.due.IsZero() && now.Before(msg.due) {
					break
				}
				cs := dest.pickConsumer(msg)
				if cs == nil {
					continue
				}
				inbound, delivery := c.inboundLocked(cs, dest.spec.Name, msg)
				select {
				case cs.messages <- inbound:
					cs.outstanding++
					cs.unsettled[dest.spec.Name]++
					cs.inflight[delivery] = struct{}{}
					if msg.deliveredGroups == nil {
						msg.deliveredGroups = make(map[string]bool)
					}
					msg.deliveredGroups[cs.cfg.Group] = true
					key := string(msg.message.Key)
					if key != "" {
						dest.affinity[key] = cs
						cs.unsettledKey[deliveryKey{destination: dest.spec.Name, key: key}]++
					}
					if dest.messageComplete(msg) {
						dest.messages = append(dest.messages[:i], dest.messages[i+1:]...)
					}
					delivered = true
				default:
					continue destinationLoop
				}
				break
			}
			if !delivered {
				break
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

func (d *destination) pickConsumer(message *queuedMessage) *consumer {
	eligible := make([]*consumer, 0, len(d.order))
	for _, cs := range d.order {
		if message.deliveredGroups != nil && message.deliveredGroups[cs.cfg.Group] {
			continue
		}
		if !cs.stopped && !cs.draining && !cs.paused[d.spec.Name] &&
			cs.canReceive(d.spec.Name) && cs.visible(message, d.spec.Name) {
			eligible = append(eligible, cs)
		}
	}
	if len(eligible) == 0 {
		return nil
	}
	if len(message.message.Key) != 0 {
		if cs := d.affinity[string(message.message.Key)]; cs != nil {
			if !cs.stopped && !cs.draining && !cs.paused[d.spec.Name] &&
				cs.canReceive(d.spec.Name) && cs.visible(message, d.spec.Name) {
				return cs
			}
			return nil
		}
	}
	cs := eligible[d.next%len(eligible)]
	d.next++
	return cs
}

func (d *destination) messageComplete(message *queuedMessage) bool {
	groups := make(map[string]struct{})
	for _, cs := range d.order {
		if cs.stopped || cs.draining {
			continue
		}
		groups[cs.cfg.Group] = struct{}{}
		if message.deliveredGroups == nil || !message.deliveredGroups[cs.cfg.Group] {
			return false
		}
	}
	return len(groups) > 0
}

func (c *consumer) canReceive(destination string) bool {
	if c.cfg.Prefetch > 0 && c.outstanding >= c.cfg.Prefetch {
		return false
	}
	limit, ok := c.cfg.PerDestination[destination]
	if !ok {
		limit = c.cfg.Prefetch
	}
	if limit < 1 {
		limit = 1
	}
	return c.unsettled[destination] < limit
}

func (c *consumer) visible(message *queuedMessage, destination string) bool {
	return message.sequence > c.startAfter[destination]
}

func (c *conn) inboundLocked(cs *consumer, destination string, msg *queuedMessage) (driver.InboundMessage, *settler) {
	c.nextRef.Add(1)
	delivery := &settler{conn: c, consumer: cs, message: msg, deliveredAt: c.clock.Now()}
	return driver.InboundMessage{
		Destination:   destination,
		Key:           cloneBytes(msg.message.Key),
		Headers:       cloneHeaders(msg.message.Headers),
		Body:          cloneBytes(msg.message.Body),
		DeliveryCount: deliveryCount(cs.cfg.Effective, msg.deliveryCount),
		ReceivedAt:    c.clock.Now(),
		Ref:           driver.BrokerRef{Tag: c.nextRef.Load(), Raw: destination},
		Settle:        delivery,
	}, delivery
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
