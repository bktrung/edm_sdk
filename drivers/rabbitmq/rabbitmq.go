package rabbitmq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

var errMissingEndpoints = errors.New("rabbitmq: broker endpoints must not be empty")

var errUnsupportedHostlessEndpoint = errors.New("rabbitmq: endpoint without a host is unsupported; use an explicit host, for example amqp://localhost/orders")

var _ driver.Driver = Driver{}

// Driver is a stateless RabbitMQ driver factory.
type Driver struct{}

// Name returns the stable RabbitMQ driver key.
func (Driver) Name() string { return "rabbitmq" }

type queueKind string

const (
	queueKindQuorum  queueKind = "quorum"
	queueKindClassic queueKind = "classic"
)

const (
	brokerPrefetchOption = "rabbitmq.brokerPrefetch"
	maxBrokerPrefetch    = 1<<16 - 1
)

const trustBrokerTimestampOption = "rabbitmq.trustBrokerTimestamp"

func resolveTrustBrokerTimestamp(options map[string]string) (bool, error) {
	configured := strings.TrimSpace(options[trustBrokerTimestampOption])
	if configured == "" {
		return false, nil
	}
	trusted, err := strconv.ParseBool(configured)
	if err != nil {
		return false, fmt.Errorf("rabbitmq: invalid %s %q: want a boolean", trustBrokerTimestampOption, options[trustBrokerTimestampOption])
	}
	return trusted, nil
}

func configuredQueueKind(options map[string]string) (queueKind, error) {
	switch strings.ToLower(strings.TrimSpace(options["rabbitmq.queueType"])) {
	case "", string(queueKindQuorum):
		return queueKindQuorum, nil
	case string(queueKindClassic):
		return queueKindClassic, nil
	default:
		return "", fmt.Errorf("rabbitmq: unsupported queueType %q; supported values: quorum, classic, or empty", options["rabbitmq.queueType"])
	}
}

func resolveBrokerPrefetch(options map[string]string) (int, error) {
	configured, present := options[brokerPrefetchOption]
	if !present {
		return 0, nil
	}
	prefetch, err := strconv.Atoi(configured)
	if err != nil || prefetch < 1 || prefetch > maxBrokerPrefetch {
		return 0, fmt.Errorf(
			"rabbitmq: invalid %s %q; want an integer from 1 to %d",
			brokerPrefetchOption, configured, maxBrokerPrefetch,
		)
	}
	return prefetch, nil
}

func capabilitiesForQueueKind(kind queueKind) driver.Capabilities {
	caps := Driver{}.Capabilities()
	if kind == queueKindClassic {
		caps.NativeDeliveryCount = false
		caps.NativeDLQ = false
	}
	return caps
}

// Capabilities reports the default RabbitMQ capabilities: per-message
// acknowledgements, key ordering, fanout at publish time, unrestricted
// consumer scaling, and lag queries. It reports no native priority: the
// driver declares no x-max-priority queue and sets no AMQP message priority,
// because the core carries priority in its own per-priority destinations. It
// does not advertise native delay support: a message on a delayed destination
// waits in a parking queue until its expiration passes. A connection using
// classic queues does not report native delivery counts or native dead-letter
// queues.
func (Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		PerMessageAck:       true,
		OrderedByKey:        true,
		NativeDelay:         false,
		NativeDeliveryCount: true,
		NativeDLQ:           true,
		ConsumerScaling:     driver.ScalingFree,
		Fanout:              driver.FanoutAtPublish,
		LagQueryable:        true,
	}
}

// Open establishes a RabbitMQ connection and retries failed endpoint dials
// until the caller's context or ConnectTimeout expires.
func (Driver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("open", driver.KindTransient, err)
	}

	if err := validateSASL(cfg.SASL); err != nil {
		return nil, classify("open", driver.KindFatal, err)
	}
	queueKind, err := configuredQueueKind(cfg.DriverOptions)
	if err != nil {
		return nil, classify("open", driver.KindFatal, err)
	}
	trustBrokerTimestamp, err := resolveTrustBrokerTimestamp(cfg.DriverOptions)
	if err != nil {
		return nil, classify("open", driver.KindFatal, err)
	}
	brokerPrefetch, err := resolveBrokerPrefetch(cfg.DriverOptions)
	if err != nil {
		return nil, classify("open", driver.KindFatal, err)
	}
	// The AMQP settings, TLS material included, are the same for every
	// endpoint and every attempt, so a file that fails to load is a
	// configuration error: retrying the dial cannot fix it.
	if len(cfg.Endpoints) == 0 {
		return nil, classify("open", driver.KindFatal, errMissingEndpoints)
	}
	amqpConfig, err := makeAMQPConfig(cfg)
	if err != nil {
		return nil, classify("open", driver.KindFatal, err)
	}
	openCtx := ctx
	if cfg.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		openCtx, cancel = context.WithTimeout(ctx, cfg.ConnectTimeout)
		defer cancel()
	}

	endpoints := cfg.Endpoints
	var lastErr error
	for {
		for _, endpoint := range endpoints {
			if err := openCtx.Err(); err != nil {
				return nil, classify("open", driver.KindTransient, err)
			}
			if err := validateEndpoint(endpoint); err != nil {
				return nil, classify("open", driver.KindFatal, err)
			}
			conn, err := dial(openCtx, endpoint, amqpConfig, cfg.ConnectTimeout)
			if err == nil {
				managedConn, connErr := newConn(conn, capabilitiesForQueueKind(queueKind), endpoint, cfg, queueKind, trustBrokerTimestamp, brokerPrefetch)
				if connErr != nil {
					_ = conn.Close()
					return nil, classify("open", driver.KindFatal, connErr)
				}
				return managedConn, nil
			}
			lastErr = err
		}

		if err := openCtx.Err(); err != nil {
			if lastErr != nil {
				return nil, classify("open", driver.KindTransient, errors.Join(err, lastErr))
			}
			return nil, classify("open", driver.KindTransient, err)
		}
		if err := waitRetry(openCtx); err != nil {
			return nil, classify("open", driver.KindTransient, err)
		}
	}
}

type conn struct {
	mu                   sync.RWMutex
	topologyMu           sync.Mutex
	amqp                 *amqp.Connection
	caps                 driver.Capabilities
	info                 driver.BrokerInfo
	queueKind            queueKind
	trustBrokerTimestamp bool
	logger               *slog.Logger
	// brokerPrefetch is resolved once at Open and applies to every consumer
	// created from this connection. Zero preserves the core window.
	brokerPrefetch int
	// consumerTimeout is the x-consumer-timeout this connection declares on
	// quorum destination queues. It is resolved at Open because a queue
	// argument is fixed at declare time, and the topology and admin paths have
	// no other way back to DriverOptions.
	consumerTimeout time.Duration
	management      *managementClient
	closed          bool
	closing         bool
	closeAttempt    bool
	active          map[*consumer]struct{}
	producers       map[*producer]struct{}
	publishFault    atomic.Int32 // 0 = unset; otherwise driver.Kind + 1
	closeFault      atomic.Bool
	deferred        map[string]time.Duration
	// durabilityUpgrades holds the destinations whose non-durable declaration
	// this connection has already reported as upgraded to durable, so the
	// report is one per destination rather than one per ensure topology pass.
	durabilityUpgrades map[string]struct{}
	// Publishing-block state, deliberately outside mu: Ping reads it on every
	// health probe and the publish path reads it before every write, so neither
	// may queue behind topology or consumer-admission work that wants a write
	// lock. blocked is nil while the broker is not blocking this connection.
	blocked atomic.Pointer[blockedState]
	// blockMu serializes block-state transitions and the waiter channel each
	// one closes. blocked and that channel are replaced together under it, so a
	// waiter that has a state in hand always has the channel that state's end
	// closes.
	blockMu    sync.Mutex
	blockWatch sync.WaitGroup
	// detachMu serializes channel-close registration with the wait in
	// Conn.Close. detaching becomes true before that wait, so no channel close
	// can Add to a zero-counter WaitGroup. None of the goroutines detachWatch
	// tracks takes detachMu, so waitDetached can hold it across the Wait.
	detachMu  sync.Mutex
	detaching bool
	// detachWatch tracks the goroutines that outlive the caller that began
	// them: channel closes, publish channel watchers, and the releases a
	// cancelled publish leaves for its channel's watcher. amqp091's
	// Channel.Close waits for close-ok, which a connection the broker has
	// stopped reading never sends, so a close that a caller cannot wait for
	// runs on a goroutine of its own; a watcher lives as long as its channel,
	// and a release until the confirmations it waits for resolve, which a
	// closed channel guarantees. Close waits on that set once the connection
	// is actually closed, for the same reason it waits on blockWatch. The Add
	// for a watcher or a release can race that Wait only from a publish still
	// running when the connection closed, and it is benign: the goroutine ends
	// as soon as the connection's shutdown reaches its channel.
	detachWatch sync.WaitGroup
}

// blockedState is the broker's latest connection.blocked notification. It is
// immutable once stored and replaced as a whole, so a reader sees the reason
// and the waiter channel that belong to each other.
type blockedState struct {
	// reason is the broker's own explanation for the block, such as the
	// resource it ran out of. It is carried into error text and never branched
	// on: which resource the broker gives up on is the broker's business, and a
	// reason this driver did not anticipate must not change what a caller sees.
	// A broker that sends no reason leaves the error naming the block alone.
	reason string
	// cleared is closed when the state stops applying, because the broker sent
	// connection.unblocked or because the connection closed under the block. A
	// waiter parks on it rather than polling; a connection that closes releases
	// its waiters through the same channel, so nobody waits for a notification
	// that can no longer arrive.
	cleared chan struct{}
}

// message renders the block for an error: what the connection cannot do, then
// the broker's reason for it when the broker gave one.
func (s *blockedState) message() string {
	if s.reason == "" {
		return "rabbitmq: connection blocked by broker"
	}
	return "rabbitmq: connection blocked by broker: " + s.reason
}

// blockedEventBuffer is the depth of the connection.blocked receiver. amqp091
// delivers these from its frame reader, which waits on a full receiver before
// abandoning the send, so the channel is buffered and drained by a goroutine of
// its own: an undrained receiver stalls the whole connection, heartbeats
// included. The notification carries a two-state flag, so a receiver deeper
// than the transitions the drain can fall behind by is what keeps a
// notification the broker sends from being lost, which for the unblock is what
// would leave a healthy connection looking blocked for good.
const blockedEventBuffer = 4

// startChannelClose begins closing channel on a goroutine of its own and
// returns the channel its outcome arrives on. The close runs off the caller's
// goroutine because amqp091's Channel.Close waits for close-ok, a round trip a
// connection the broker has stopped reading never answers, and a caller whose
// context has run out must not be held by it.
//
// Producer callers receive the same buffered outcome and no finish callback,
// so their close behavior is unchanged.
func (c *conn) startChannelClose(channel *amqp.Channel) <-chan error {
	return c.startChannelCloseWithFinish(channel.Close, nil)
}

// startChannelCloseWithFinish runs closeFn and finish in the one goroutine
// tracked by detachWatch. finish runs before Done, so a lane close cannot
// release channelMu or publish its completion after Conn.Close has finished
// waiting for the detached work.
func (c *conn) startChannelCloseWithFinish(closeFn func() error, finish func(error)) <-chan error {
	done := make(chan error, 1)
	c.detachMu.Lock()
	if c.detaching {
		c.detachMu.Unlock()
		err := closeFn()
		if finish != nil {
			finish(err)
		}
		done <- err
		return done
	}
	c.detachWatch.Add(1)
	c.detachMu.Unlock()
	go func() {
		defer c.detachWatch.Done()
		err := closeFn()
		if finish != nil {
			finish(err)
		}
		done <- err
	}()
	return done
}

// waitDetached closes the registration window before waiting for detached
// channel work. If registration wins detachMu, Add makes the count non-zero
// before this Wait. If the wait wins, detaching makes a later close run
// synchronously against the already-closed AMQP connection, so it cannot add
// work after Wait returns.
func (c *conn) waitDetached() {
	c.detachMu.Lock()
	c.detaching = true
	c.detachWatch.Wait()
	c.detachMu.Unlock()
}

// awaitChannelClose waits for a close startChannelClose began, for as long as
// ctx allows, and returns the close's own error or ctx's. The result channel is
// buffered, so a close that ends after its caller gave up delivers and exits
// instead of waiting for a reader that is never coming, and its error is
// deliberately dropped: the caller asked to stop waiting for the close, not to
// be told how it went. The select is a race when both are ready, so a close
// that completed as the context ran out may be reported as the context's error.
func (c *conn) awaitChannelClose(ctx context.Context, done <-chan error) error {
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// watchBlocks keeps this connection's publishing-block state current. It is the
// only reader of the notify channel, which amqp091 closes when the connection
// shuts down: draining until that close is what keeps the frame reader from
// stalling, and it is also what ends the goroutine, so the connection owns no
// goroutine that outlives it.
func (c *conn) watchBlocks(events <-chan amqp.Blocking) {
	defer c.blockWatch.Done()
	for event := range events {
		if event.Active {
			c.block(event.Reason)
			continue
		}
		c.unblock()
	}
	// The connection is gone, so any block still outstanding is over whether
	// the broker said so or not. Without this, a waiter parked on it would wait
	// for an unblock the closed connection can no longer deliver.
	c.unblock()
}

// block records a connection.blocked notification. A repeat while a block is
// already outstanding keeps the waiter channel of the block it repeats, so the
// waiters parked on it are not orphaned by the newer reason.
func (c *conn) block(reason string) {
	c.blockMu.Lock()
	defer c.blockMu.Unlock()
	if current := c.blocked.Load(); current != nil {
		c.blocked.Store(&blockedState{reason: reason, cleared: current.cleared})
		return
	}
	c.blocked.Store(&blockedState{reason: reason, cleared: make(chan struct{})})
}

// unblock ends the publishing-block state and releases every waiter parked on
// it. The state is cleared before the channel closes, so a waiter that wakes
// re-reads it and finds the connection unblocked instead of parking again on a
// channel that will never close a second time.
func (c *conn) unblock() {
	c.blockMu.Lock()
	defer c.blockMu.Unlock()
	current := c.blocked.Load()
	if current == nil {
		return
	}
	c.blocked.Store(nil)
	close(current.cleared)
}

// awaitUnblocked blocks while the broker is blocking publishers on this
// connection and returns nil as soon as it is not. When the caller's context
// ends first it returns an error naming the block and wrapping the context's
// own error, so a caller that ran out of time can still tell why.
//
// It exists so nothing above it polls: a waiter parks on the channel the
// unblock closes, and a connection that closes under the block releases it
// through that same channel.
func (c *conn) awaitUnblocked(ctx context.Context) error {
	for {
		state := c.blocked.Load()
		if state == nil {
			return nil
		}
		select {
		case <-state.cleared:
			// The block ended, or the connection closed. Re-read the state: a
			// second block may have started while this waiter was awake, and a
			// waiter that is awake is a waiter that can wait again.
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", state.message(), ctx.Err())
		}
	}
}

var _ driver.Conn = (*conn)(nil)

func newConn(amqpConn *amqp.Connection, caps driver.Capabilities, endpoint string, cfg driver.Config, kind queueKind, trustBrokerTimestamp bool, brokerPrefetch int) (*conn, error) {
	management, err := newManagementClient(endpoint, cfg)
	if err != nil {
		return nil, err
	}
	consumerTimeout, err := resolveConsumerTimeout(cfg.DriverOptions)
	if err != nil {
		return nil, err
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	connection := &conn{
		amqp:                 amqpConn,
		caps:                 caps,
		info:                 brokerInfo(amqpConn),
		queueKind:            kind,
		trustBrokerTimestamp: trustBrokerTimestamp,
		logger:               logger,
		brokerPrefetch:       brokerPrefetch,
		consumerTimeout:      consumerTimeout,
		management:           management,
		active:               make(map[*consumer]struct{}),
		producers:            make(map[*producer]struct{}),
		deferred:             make(map[string]time.Duration),
	}
	// One subscription per connection is the whole of the block handling:
	// c.amqp is set here and never replaced, so this connection's notifications
	// arrive on this channel for as long as the connection lives, and the
	// goroutine ends with the connection that closes it.
	blocked := amqpConn.NotifyBlocked(make(chan amqp.Blocking, blockedEventBuffer))
	connection.blockWatch.Add(1)
	go connection.watchBlocks(blocked)
	return connection, nil
}

func (c *conn) log() *slog.Logger {
	if c.logger == nil {
		return slog.Default()
	}
	return c.logger
}

// reportDurabilityUpgrade records that destination was declared durable
// although the spec asked for non-durable, and warns about it once. A quorum
// queue is durable by definition, so a quorum deployment cannot honor the
// request; the destination is then not what the caller asked for, and a silent
// upgrade is a destination an operator cannot tell from the one they
// configured.
func (c *conn) reportDurabilityUpgrade(destination string) {
	c.mu.Lock()
	if c.durabilityUpgrades == nil {
		c.durabilityUpgrades = make(map[string]struct{})
	}
	_, reported := c.durabilityUpgrades[destination]
	c.durabilityUpgrades[destination] = struct{}{}
	c.mu.Unlock()
	if reported {
		return
	}
	c.log().Warn("RabbitMQ quorum queue kind declares a non-durable destination as durable", "destination", destination)
}

// Capabilities returns the capabilities of this connection.
func (c *conn) Capabilities() driver.Capabilities { return c.caps }

// BrokerInfo returns a copy of the connected broker metadata.
func (c *conn) BrokerInfo() driver.BrokerInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return copyBrokerInfo(c.info)
}

// Producer creates a producer using cfg.
func (c *conn) Producer(ctx context.Context, cfg driver.ProducerConfig) (driver.Producer, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("producer", driver.KindTransient, err)
	}
	c.mu.RLock()
	if c.closed || c.closing || c.amqp.IsClosed() {
		c.mu.RUnlock()
		return nil, classify("producer", driver.KindTransient, amqp.ErrClosed)
	}
	c.mu.RUnlock()
	producer, err := newProducer(ctx, c, cfg)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.closed || c.closing || c.amqp.IsClosed() {
		c.mu.Unlock()
		// The producer was never registered, so its channels are closed here
		// rather than by Close, and with the same bound: the connection was
		// closing when this raced it, which says nothing about whether the
		// broker is reading it. It was never handed to a caller, so the only
		// channel it opened is the first one, and this closes whichever ones
		// it has should that ever change.
		for _, done := range producer.startChannelCloses() {
			_ = c.awaitChannelClose(ctx, done)
		}
		return nil, classify("producer", driver.KindTransient, amqp.ErrClosed)
	}
	c.producers[producer] = struct{}{}
	c.mu.Unlock()
	return producer, nil
}

// Consumer creates a consumer using cfg.
func (c *conn) Consumer(ctx context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("consumer", driver.KindTransient, err)
	}
	c.mu.Lock()
	err := c.consumerAdmissionLocked(cfg)
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if len(cfg.Destinations) == 0 {
		return nil, classify("consumer", driver.KindFatal, errors.New("no destinations"))
	}

	consumer, err := newConsumer(c, cfg)
	if err != nil {
		return nil, err
	}
	if hook := consumerConstructionHook; hook != nil {
		hook(consumer, consumerConstructionReady)
	}
	c.mu.Lock()
	err = c.consumerAdmissionLocked(cfg)
	if err == nil {
		c.active[consumer] = struct{}{}
		c.mu.Unlock()
		return consumer, nil
	}
	c.mu.Unlock()
	if releaseErr := consumer.Release(context.WithoutCancel(ctx)); releaseErr != nil {
		return nil, errors.Join(err, releaseErr)
	}
	return nil, err
}

func (c *conn) consumerAdmissionLocked(cfg driver.ConsumerConfig) error {
	if c.closed || c.closing || c.amqp.IsClosed() {
		return classify("consumer", driver.KindTransient, amqp.ErrClosed)
	}
	if cfg.Exclusive {
		for _, destination := range cfg.Destinations {
			for active := range c.active {
				if active.hasDestination(destination) {
					return classify("consumer", driver.KindFatal, fmt.Errorf("exclusive consumer refused: destination %q already has a consumer", destination))
				}
			}
		}
	}
	for active := range c.active {
		if !active.cfg.Exclusive {
			continue
		}
		for _, destination := range cfg.Destinations {
			if active.hasDestination(destination) {
				return classify("consumer", driver.KindFatal, fmt.Errorf("exclusive consumer refused: destination %q already has an exclusive consumer", destination))
			}
		}
	}
	return nil
}

// Admin returns the topology administration surface for this connection.
func (c *conn) Admin() driver.Admin { return &admin{operations: &adminOperations{conn: c}} }

func (c *conn) removeConsumer(consumer *consumer) {
	c.mu.Lock()
	delete(c.active, consumer)
	c.mu.Unlock()
}

// Ping checks whether the RabbitMQ connection is reachable.
func (c *conn) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("ping", driver.KindTransient, err)
	}
	c.mu.RLock()
	if c.closed || c.closing || c.amqp.IsClosed() {
		c.mu.RUnlock()
		return classify("ping", driver.KindTransient, amqp.ErrClosed)
	}
	// The block is read here, after the closed checks and before the channel
	// round trip below: a broker that is blocking publishers has stopped
	// reading this connection, so the channel open that Ping otherwise uses to
	// prove liveness is the one call it cannot complete while blocked. The
	// state is read with a load rather than under a write lock, so the probe
	// adds no contention to the publish path that maintains it.
	if blocked := c.blocked.Load(); blocked != nil {
		c.mu.RUnlock()
		return classify("ping", driver.KindTransient, errors.New(blocked.message()))
	}
	channel, err := c.amqp.Channel()
	c.mu.RUnlock()
	if err != nil {
		return classifyAMQP("ping", driver.KindTransient, err)
	}
	if err := channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		return classifyAMQP("ping", driver.KindTransient, err)
	}
	return nil
}

// Close releases the connection after its producers and consumers have closed.
func (c *conn) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("close", driver.KindTransient, err)
	}
	amqpConn := c.amqp
	// The block watcher and any channel close that outlived its caller are the
	// goroutines this connection owns. amqp091 closes the notify channel and
	// answers a close with ErrClosed when the connection shuts down, so waiting
	// here is what makes "Close leaves nothing of ours running" true rather
	// than probable. A close attempt that did not close the connection - an
	// injected fault, or a deadline that ran out - leaves them where they
	// belong, running against a connection that is still up.
	defer func() {
		if amqpConn.IsClosed() {
			c.blockWatch.Wait()
			c.waitDetached()
		}
	}()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	if c.closeAttempt {
		c.mu.Unlock()
		return classify("close", driver.KindTransient, errors.New("close already in progress"))
	}
	// Reject the precondition before marking teardown started so admission
	// remains open when resources are still outstanding.
	if !c.closing && (len(c.active) != 0 || len(c.producers) != 0) {
		c.mu.Unlock()
		return classify("close", driver.KindFatal, driver.ErrResourcesOutstanding)
	}
	if !c.closing {
		c.closing = true
	}
	c.closeAttempt = true
	deadline, hasDeadline := ctx.Deadline()
	var injectedErr error
	if c.closeFault.Swap(false) {
		if hasDeadline {
			c.mu.Unlock()
			<-ctx.Done()
			c.mu.Lock()
			c.closeAttempt = false
			c.mu.Unlock()
			return classify("close", driver.KindTransient, ctx.Err())
		}
		injectedErr = errors.New("injected close failure")
	}
	c.mu.Unlock()

	var err error
	if injectedErr != nil {
		err = injectedErr
	} else if hasDeadline {
		err = amqpConn.CloseDeadline(deadline)
	} else {
		done := make(chan error, 1)
		go func() { done <- amqpConn.Close() }()
		select {
		case err = <-done:
		case <-ctx.Done():
			c.mu.Lock()
			c.closeAttempt = false
			c.mu.Unlock()
			return classify("close", driver.KindTransient, ctx.Err())
		}
	}
	if errors.Is(err, amqp.ErrClosed) {
		c.mu.Lock()
		c.closeAttempt = false
		c.closed = true
		c.mu.Unlock()
		return nil
	}
	if err != nil {
		c.mu.Lock()
		c.closeAttempt = false
		c.mu.Unlock()
		return classifyAMQP("close", driver.KindTransient, err)
	}
	c.mu.Lock()
	c.closeAttempt = false
	c.closed = true
	c.mu.Unlock()
	return nil
}

func dial(ctx context.Context, endpoint string, amqpConfig amqp.Config, connectTimeout time.Duration) (*amqp.Connection, error) {
	timeout := connectTimeout
	if timeout <= 0 || timeout > 30*time.Second {
		timeout = 30 * time.Second
	}
	dialer := &net.Dialer{Timeout: timeout}
	amqpConfig.Dial = dialer.Dial
	if amqpConfig.TLSClientConfig != nil {
		// amqp091 writes the dialed host into an empty ServerName. Open shares
		// one config across its endpoints, so each dial gets its own copy.
		amqpConfig.TLSClientConfig = amqpConfig.TLSClientConfig.Clone()
	}
	result := make(chan *amqp.Connection, 1)
	errResult := make(chan error, 1)
	go func() {
		connection, dialErr := amqp.DialConfig(endpoint, amqpConfig)
		if dialErr != nil {
			errResult <- dialErr
			return
		}
		result <- connection
	}()
	select {
	case connection := <-result:
		return connection, nil
	case err := <-errResult:
		return nil, err
	case <-ctx.Done():
		// The dial still ends one way or the other; wait for either outcome so
		// a dial that fails after the cancel does not strand this goroutine.
		go func() {
			select {
			case connection := <-result:
				_ = connection.Close()
			case <-errResult:
			}
		}()
		return nil, ctx.Err()
	}
}

func validateEndpoint(endpoint string) error {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return errors.New("rabbitmq: invalid endpoint")
	}
	if parsed.Hostname() == "" {
		return errUnsupportedHostlessEndpoint
	}
	if parsed.Scheme != "amqps" && !isLoopbackEndpoint(endpoint) {
		return errors.New("rabbitmq: plaintext connection to non-loopback host requires an amqps:// endpoint")
	}
	return nil
}

func makeAMQPConfig(cfg driver.Config) (amqp.Config, error) {
	config := amqp.Config{Properties: amqp.NewConnectionProperties()}
	if cfg.ClientID != "" {
		config.Properties.SetClientConnectionName(cfg.ClientID)
	}
	if cfg.SASL != nil {
		switch strings.ToLower(cfg.SASL.Mechanism) {
		case "":
		case "plain":
			config.SASL = []amqp.Authentication{&amqp.PlainAuth{
				Username: cfg.SASL.Username,
				Password: cfg.SASL.Password,
			}}
		case "amqplain":
			config.SASL = []amqp.Authentication{&amqp.AMQPlainAuth{
				Username: cfg.SASL.Username,
				Password: cfg.SASL.Password,
			}}
		case "external":
			config.SASL = []amqp.Authentication{&amqp.ExternalAuth{}}
		}
	}
	if cfg.TLS == nil || !cfg.TLS.Enabled {
		return config, nil
	}
	if cfg.TLS.InsecureSkipVerify && !isTestEndpoint(cfg.Endpoints) {
		return config, errors.New("rabbitmq: insecure TLS is only allowed for a test endpoint")
	}
	tlsConfig, err := tlsConfig(cfg.TLS)
	if err != nil {
		return config, err
	}
	config.TLSClientConfig = tlsConfig
	return config, nil
}

func validateSASL(settings *driver.SASLConfig) error {
	if settings == nil {
		return nil
	}
	switch strings.ToLower(settings.Mechanism) {
	case "", "plain", "amqplain", "external":
		return nil
	default:
		return fmt.Errorf("rabbitmq: unsupported SASL mechanism %q; supported mechanisms: PLAIN, AMQPLAIN, EXTERNAL, or empty", settings.Mechanism)
	}
}

func tlsConfig(settings *driver.TLSConfig) (*tls.Config, error) {
	config := &tls.Config{InsecureSkipVerify: settings.InsecureSkipVerify, ServerName: settings.ServerName} //nolint:gosec // explicitly controlled by the driver config
	if settings.CAFile != "" {
		pem, err := os.ReadFile(settings.CAFile)
		if err != nil {
			return nil, fmt.Errorf("rabbitmq: read TLS CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("rabbitmq: TLS CA file contains no certificates")
		}
		config.RootCAs = pool
	}
	if settings.CertFile == "" && settings.KeyFile == "" {
		return config, nil
	}
	certificate, err := tls.LoadX509KeyPair(settings.CertFile, settings.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: load TLS client certificate: %w", err)
	}
	config.Certificates = []tls.Certificate{certificate}
	return config, nil
}

// isTestEndpoint reports whether every endpoint in the list resolves to an
// exact loopback literal: the hostname "localhost" or an IP address for
// which net.IP.IsLoopback is true. An empty list or any endpoint that fails
// to parse, or whose host is not an exact loopback literal, makes the whole
// list ineligible for insecure TLS.
func isTestEndpoint(endpoints []string) bool {
	if len(endpoints) == 0 {
		return false
	}
	for _, endpoint := range endpoints {
		if !isLoopbackEndpoint(endpoint) {
			return false
		}
	}
	return true
}

// isLoopbackEndpoint reports whether the host portion of endpoint is exactly
// "localhost" or an IP address that net.IP.IsLoopback confirms is loopback.
// A hostname that merely contains "localhost" or a loopback IP as a
// substring, such as an attacker-registrable "evil-localhost.example.com",
// does not qualify.
func isLoopbackEndpoint(endpoint string) bool {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return false
	}
	host := parsed.Hostname()
	if host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func waitRetry(ctx context.Context) error {
	timer := time.NewTimer(250 * time.Millisecond) //nolint:forbidigo // connection retries wait for broker recovery and require a wall-clock delay
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func brokerInfo(connection *amqp.Connection) driver.BrokerInfo {
	version := ""
	if value, ok := connection.Properties["version"].(string); ok {
		version = value
	}
	if version == "" && (connection.Major != 0 || connection.Minor != 0) {
		version = fmt.Sprintf("%d.%d", connection.Major, connection.Minor)
	}
	return driver.BrokerInfo{Kind: "rabbitmq", Version: version}
}

func copyBrokerInfo(info driver.BrokerInfo) driver.BrokerInfo {
	info.Nodes = append([]string(nil), info.Nodes...)
	info.Extra = maps.Clone(info.Extra)
	return info
}

func classify(op string, kind driver.Kind, err error) error {
	if err == nil {
		return nil
	}
	return &driver.Error{Driver: "rabbitmq", Op: op, K: kind, Err: err}
}

func classifyAMQP(op string, fallback driver.Kind, err error) error {
	if err == nil {
		return nil
	}
	kind := fallback
	if amqpErr, ok := errors.AsType[*amqp.Error](err); ok {
		switch amqpErr.Code {
		case 403:
			kind = driver.KindPermission
		case 404:
			kind = driver.KindNotFound
			err = errors.Join(driver.ErrDestinationMissing, err)
		case 501, 502, 503, 504:
			// A client-side 501-504 is how amqp091-go reports a lost
			// transport: a failed socket read shuts the connection down with
			// FrameError (501), and ErrClosed is ChannelError (504). The core
			// must reconnect rather than stop, so the caller's fallback cannot
			// decide the kind: the Qos, Confirm and topology call sites pass
			// fatal, and a connection lost while a consumer reopens would
			// otherwise end the subscription. The broker sends these codes for
			// protocol misuse instead, and a forced close arrives as 320, so a
			// server-sent one stays fatal.
			if amqpErr.Server {
				kind = driver.KindFatal
			} else {
				kind = driver.KindTransient
			}
		}
	}
	return classify(op, kind, err)
}

// classifyPublishClose classifies the close of a channel a publish was running
// on, where a message the broker refused for its size has to be told apart from
// the many other ways a publish channel ends.
//
// Only the size refusal is classified here rather than by classifyAMQP. It is
// the one close that names the limit the broker applies, so it is the one kind
// a message's own body can answer for: a publish that read this close reports
// every undecided message over that limit as too large, whether or not it is
// the message the broker refused.
//
// Every other close keeps the transient kind a closed publish channel has
// always had, whatever code it carried. That includes a server-sent 501-504,
// where classifyAMQP would say fatal: transient is what makes the core take the
// connection down and build it again, which is the only answer to a frame the
// broker refused, and the connection is gone either way. The frame error is the
// one of those that reaches this path in practice - the broker sends it for a
// content-header frame over its frame limit, which a message with a large
// header set produces - and a reconnect is what recovers it.
func classifyPublishClose(err error) error {
	if _, refused := sizeRefusalLimit(err); refused {
		return classify("publish", driver.KindTooLarge, err)
	}
	return classify("publish", driver.KindTransient, err)
}

// sizeLimitPrefix is the phrase a size refusal states the broker's limit after,
// in both wordings of that refusal.
const sizeLimitPrefix = "max size "

// sizeRefusalLimit reports the limit the broker named, when err is the close of
// a channel it refused a publish on for the size of the message.
//
// The reply code alone cannot decide it. Every publish precondition the broker
// can fail arrives as 406 PRECONDITION_FAILED, so a message carrying an
// expiration the broker cannot parse is refused with the same code, and so is
// any other check the channel makes before routing. The reason is the only part
// that separates them, and the wording is the one RabbitMQ has used for this
// refusal since it grew the limit, unchanged from 3.8 through 4.3:
//
//	PRECONDITION_FAILED - message size 16777217 is larger than configured max size 16777216
//
// A broker that applies a limit that is not a configured one words the same
// refusal "is larger than max size N" instead, so the two phrases matched below
// are what both wordings share, and neither is a phrase another publish
// precondition uses.
//
// The number after the last "max size" is the limit the broker applies, not the
// size it refused: the size it refused appears after "message size" instead, and
// reading that one would compare every later message of a window against the
// length of the message that was refused. The limit it names is what makes a
// body length answerable for a refusal, which is the one thing a driver holding
// no declared message limit of its own can use. A 406 that carries both phrases
// but no number readable after "max size" is not treated as a size refusal:
// nothing here would then know the limit, and a refusal the driver cannot
// measure is the closed publish channel it was before.
func sizeRefusalLimit(err error) (int, bool) {
	amqpErr, ok := errors.AsType[*amqp.Error](err)
	if !ok ||
		amqpErr.Code != 406 ||
		!strings.Contains(amqpErr.Reason, "message size") ||
		!strings.Contains(amqpErr.Reason, "larger than") {
		return 0, false
	}
	index := strings.LastIndex(amqpErr.Reason, sizeLimitPrefix)
	if index < 0 {
		return 0, false
	}
	var limit int
	if _, scanErr := fmt.Sscanf(amqpErr.Reason[index+len(sizeLimitPrefix):], "%d", &limit); scanErr != nil {
		return 0, false
	}
	return limit, true
}

func (c *conn) removeProducer(producer *producer) {
	c.mu.Lock()
	delete(c.producers, producer)
	c.mu.Unlock()
}
