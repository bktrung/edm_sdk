package inmem

import (
	"context"
	"errors"
	"math"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/testhook"
)

// Driver is an in-memory driver factory. New returns isolated connections;
// NewShared returns connections over one broker. The minimal capability mode is
// conformance-only and intentionally unexported.
type Driver struct {
	clock   clock.Clock
	minimal bool
	shared  *sharedBroker
}

const (
	// These limits exercise the driver's limit-reporting and enforcement paths;
	// they are deliberately independent of the in-memory store's capacity.
	maxMessageBytes = 1 << 20
	maxHeaderBytes  = 64 << 10

	// maxDestinationHistory bounds how many published messages a destination
	// retains to replay for a consumer group that attaches from earliest
	// after every prior consumer has already left. Every already-attached
	// consumer group is served from the live per-destination queue, not from
	// history, so history exists only for that late-attach replay case; left
	// unbounded, a destination with a long-lived attached consumer and
	// continuous publishing grows it for the life of the connection. Once
	// capped, a group attaching from earliest sees the newest
	// maxDestinationHistory messages, oldest evicted first - the same
	// trade-off a real broker with bounded retention makes.
	maxDestinationHistory = 10000
)

var _ driver.Driver = Driver{}

// New returns a driver whose connections read time from the real clock source.
func New() Driver { return Driver{} }

// NewShared returns a driver whose repeated Open calls share one broker and read
// time from the real clock source.
func NewShared() Driver {
	return Driver{shared: &sharedBroker{}}
}

// init hands the module's own tests a way to build on a fake clock. The clock
// type is internal, so New cannot take one from an outside caller, and the
// deferred-delivery queue has to share the test's clock or a test that advances
// time measures nothing.
func init() {
	testhook.RegisterDriver(func(c clock.Clock) driver.Driver { return Driver{clock: c} })
}

// Name returns the stable in-memory driver key.
func (Driver) Name() string { return "inmem" }

// Capabilities reports the behavior implemented by the in-memory driver. Native DLQ,
// priority, transactions, and server-side filtering are intentionally false.
//
// DelayAccuracy is zero lateness with no delay excluded: the pump holds every
// message it has accepted and releases one in the pass that the connection's
// clock reaches its due time, so nothing between the due time and the release
// can put the message late. MaxDelay at the largest duration is how the
// declaration says no requested delay is above the bound.
func (d Driver) Capabilities() driver.Capabilities {
	caps := driver.Capabilities{
		PerMessageAck:       true,
		OrderedByKey:        true,
		NativeDelay:         true,
		DelayAccuracy:       driver.DelayAccuracy{MaxDelay: time.Duration(math.MaxInt64)},
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

type sharedBroker struct {
	mu   sync.Mutex
	conn *conn
	refs int
}

// Open creates an in-memory connection, isolated unless d came from NewShared.
func (d Driver) Open(ctx context.Context, _ driver.Config) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("open", driver.KindTransient, err)
	}
	if d.shared != nil {
		d.shared.mu.Lock()
		defer d.shared.mu.Unlock()
		if d.shared.conn == nil {
			isolated := d
			isolated.shared = nil
			opened, err := isolated.Open(ctx, driver.Config{})
			if err != nil {
				return nil, err
			}
			d.shared.conn = opened.(*conn)
		}
		d.shared.refs++
		return &sharedConn{Conn: d.shared.conn, broker: d.shared}, nil
	}

	c := d.clock
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

type sharedConn struct {
	driver.Conn
	broker *sharedBroker
	mu     sync.Mutex
	closed bool
}

func (c *sharedConn) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.broker.mu.Lock()
	defer c.broker.mu.Unlock()
	if c.broker.refs > 1 {
		c.broker.refs--
		c.closed = true
		return nil
	}
	if err := c.Conn.Close(ctx); err != nil {
		return err
	}
	c.broker.refs = 0
	c.broker.conn = nil
	c.closed = true
	return nil
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
	closing          bool
	failPublish      bool
	failPublishFatal bool
	closeFault       bool
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
	affinity  map[affinityKey]*consumer
	order     []*consumer
	// next holds one round-robin cursor per group, created with the
	// destination.
	next map[string]int
}

// affinityKey identifies one group's key affinity on one destination. Holding
// affinity per group is what lets two groups on a destination each keep their
// own consumer for a key: a single holder would duplicate the message into the
// group that already has it and withhold it from the other.
type affinityKey struct {
	group string
	key   string
}

type groupState struct {
	positions map[string]uint64
	// members holds every destination this group has attached to, for the
	// life of the connection. Membership outlives the consumers that created
	// it, so a message the group has not been given is not retired by the
	// groups that are still attached.
	members map[string]bool
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
func (c *conn) Admin() driver.Admin               { return &admin{operations: &adminOperations{conn: c}} }

func (c *conn) Producer(ctx context.Context, cfg driver.ProducerConfig) (driver.Producer, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("producer", driver.KindTransient, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.closing {
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
	if c.closed || c.closing {
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
			group = &groupState{
				positions: make(map[string]uint64, len(cfg.Destinations)),
				members:   make(map[string]bool, len(cfg.Destinations)),
			}
			c.groups[cfg.Group] = group
			if cfg.StartAt == driver.StartLatest {
				for _, name := range cfg.Destinations {
					group.positions[name] = c.nextMessage.Load()
				}
			}
		}
		for _, name := range cfg.Destinations {
			startAfter[name] = group.positions[name]
			group.members[name] = true
		}
		if createdGroup && cfg.StartAt != driver.StartLatest {
			c.replayHistoryLocked(cfg.Group, cfg.Destinations, startAfter)
		}
	} else if cfg.StartAt == driver.StartLatest {
		for _, name := range cfg.Destinations {
			startAfter[name] = c.nextMessage.Load()
		}
	}
	capacity := max(cfg.Prefetch, 1)
	cs := &consumer{
		conn:         c,
		cfg:          cfg,
		destinations: append([]string(nil), cfg.Destinations...),
		messages:     make(chan driver.InboundMessage, capacity),
		errs:         make(chan error, 1),
		paused:       make(map[string]bool),
		unsettled:    make(map[string]int),
		unsettledKey: make(map[deliveryKey]int),
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
			for existing, state := range c.groups {
				// A group that never attached to this destination is not a
				// member of it, so the replayed copy must not wait on a group
				// that will never be given the message.
				if !state.members[name] {
					continue
				}
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

// recordHistoryLocked appends entry to destination's replay history,
// evicting the oldest entries first once maxDestinationHistory is exceeded.
// The caller must hold c.mu.
func (c *conn) recordHistoryLocked(destination string, entry *queuedMessage) {
	entries := append(c.history[destination], entry)
	if overflow := len(entries) - maxDestinationHistory; overflow > 0 {
		kept := make([]*queuedMessage, len(entries)-overflow)
		copy(kept, entries[overflow:])
		entries = kept
	}
	c.history[destination] = entries
}

func (c *conn) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("ping", driver.KindTransient, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.closing {
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
	// Reject the precondition before marking teardown started so admission
	// remains open when resources are still outstanding.
	if !c.closing && (c.producers != 0 || len(c.consumers) != 0) {
		c.mu.Unlock()
		return classify("close", driver.KindFatal, driver.ErrResourcesOutstanding)
	}
	c.closing = true
	if c.closeFault {
		c.closeFault = false
		if _, hasDeadline := ctx.Deadline(); !hasDeadline {
			c.mu.Unlock()
			return classify("close", driver.KindTransient, errors.New("injected close failure"))
		}
		c.mu.Unlock()
		<-ctx.Done()
		return classify("close", driver.KindTransient, ctx.Err())
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
		d := max(next.Sub(c.clock.Now()), 0)
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
		for _, message := range dest.messages {
			if message.due.IsZero() {
				continue
			}
			if next.IsZero() || message.due.Before(next) {
				next = message.due
			}
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
			affinity := affinityKey{group: consumer.cfg.Group, key: key}
			if dest, ok := c.destinations[name]; ok && dest.affinity[affinity] == consumer {
				delete(dest.affinity, affinity)
			}
		}
	}
	delivery.message.deliveryCount++
	delivery.message.due = now
	if delivery.message.deliveredGroups != nil {
		delete(delivery.message.deliveredGroups, consumer.cfg.Group)
	}
	if dest, ok := c.destinations[name]; ok {
		dest.requeueLocked(delivery.message)
	}
}

// requeueLocked returns message to the head of the destination's queue unless
// it is already there. A message stays queued while any member group has not
// been given it, so a requeue from a group that already holds it must not add a
// second entry: the duplicate would outlive the delivery that created it,
// because every group would already hold the message and no consumer would
// ever be eligible for the extra copy.
func (d *destination) requeueLocked(message *queuedMessage) {
	if slices.Contains(d.messages, message) {
		return
	}
	d.messages = append([]*queuedMessage{message}, d.messages...)
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
					continue
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
						dest.affinity[affinityKey{group: cs.cfg.Group, key: key}] = cs
						cs.unsettledKey[deliveryKey{destination: dest.spec.Name, key: key}]++
					}
					if c.messageComplete(dest, msg) {
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

// pickConsumer selects the consumer that takes message next, or nil when no
// group can take it now. Groups are considered one at a time, in the order
// their consumers attached, and each group is considered once per selection,
// at the first consumer that still represents it. A group that already holds
// the message has nothing to take. A group with an affinity consumer for the
// key uses it, and is skipped for now when that consumer cannot take the
// message, so one group's holder never withholds the message from the groups
// behind it. A group with no affinity for the key round-robins among its own
// eligible consumers, so one group's keys never select another group's
// consumer.
func (d *destination) pickConsumer(message *queuedMessage) *consumer {
	key := string(message.message.Key)
	for index, cs := range d.order {
		group := cs.cfg.Group
		if message.deliveredGroups[group] {
			continue
		}
		// The first attached consumer of a group has no earlier consumer that
		// could have represented it, and this runs once per queued message per
		// dispatch pass, so that case skips the scan.
		if index > 0 && representedEarlier(d.order[:index], group) {
			continue
		}
		if key != "" {
			if holder := d.affinity[affinityKey{group: group, key: key}]; holder != nil {
				if holder.eligibleFor(message, d.spec.Name) {
					return holder
				}
				continue
			}
		}
		count := 0
		for _, candidate := range d.order {
			if candidate.cfg.Group == group && candidate.eligibleFor(message, d.spec.Name) {
				count++
			}
		}
		if count == 0 {
			continue
		}
		next := d.next[group] % count
		d.next[group] = next + 1
		for _, candidate := range d.order {
			if candidate.cfg.Group != group || !candidate.eligibleFor(message, d.spec.Name) {
				continue
			}
			if next == 0 {
				return candidate
			}
			next--
		}
	}
	return nil
}

// representedEarlier reports whether a consumer attached before the one being
// considered already stands for group, so each group is considered once.
func representedEarlier(earlier []*consumer, group string) bool {
	for _, cs := range earlier {
		if cs.cfg.Group == group {
			return true
		}
	}
	return false
}

// messageComplete reports whether every group with a claim on the destination
// has been given message. A named group keeps its claim after its consumers
// leave, so a message that group has not been given keeps waiting for it. A
// group created after the message was published has no claim on it: its floor
// on the destination is at or above the message's sequence, and visible can
// never offer it the message, so waiting for it would strand the message for
// the life of the connection. The anonymous group has no claim of its own and
// counts only while an ungrouped consumer is attached, which is what keeps
// ungrouped delivery stateless. A destination with no group at all retires
// nothing.
func (c *conn) messageComplete(dest *destination, message *queuedMessage) bool {
	groups := 0
	for name, group := range c.groups {
		if !group.members[dest.spec.Name] || message.sequence <= group.positions[dest.spec.Name] {
			continue
		}
		groups++
		if !message.deliveredGroups[name] {
			return false
		}
	}
	for _, cs := range dest.order {
		if cs.stopped || cs.draining || cs.cfg.Group != "" {
			continue
		}
		groups++
		if !message.deliveredGroups[""] {
			return false
		}
	}
	return groups > 0
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

// eligibleFor reports whether c can be given message from destination now:
// still attached, not paused there, inside its prefetch budget, and past the
// floor its group was created at. One selection asks this question at several
// points, so the decision is stated once.
func (c *consumer) eligibleFor(message *queuedMessage, destination string) bool {
	return !c.stopped && !c.draining && !c.paused[destination] &&
		c.canReceive(destination) && c.visible(message, destination)
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
