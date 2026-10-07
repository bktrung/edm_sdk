package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/wire"
)

// partitionKeyHeader carries OutboundMessage.Key across the wire. A fanout
// exchange ignores the AMQP routing key, so the ordering identity cannot ride
// there, and it is not an envelope attribute so it does not belong under the
// cloudEvents: prefix.
const partitionKeyHeader = "x-f1-partition-key"

var consumerSequence atomic.Uint64

type consumer struct {
	conn                 *conn
	cfg                  driver.ConsumerConfig
	clock                clock.Clock
	trustBrokerTimestamp bool
	lanes                []*lane
	byName               map[string]*lane
	messages             chan driver.InboundMessage
	errors               chan error
	// errorsMu serializes senders on errors, so a severe error's eviction and
	// send are one step that another sender cannot interleave with.
	errorsMu sync.Mutex
	stoppedC chan struct{}
	// Drain stops forwarders; stoppedC stops all local goroutines before teardown completes.
	forwarderStopC chan struct{}
	// forwarderExitHook is a test-only synchronization seam. It remains nil in
	// production and does not change the driver behavior.
	forwarderExitHook func()
	// readerSendHook is a test-only synchronization seam. It remains nil in
	// production and does not change the driver behavior.
	readerSendHook func(*lane)
	// cancelHook is a test-only synchronization seam. It remains nil in
	// production and does not change the driver behavior.
	cancelHook func()
	// channelCloseHook replaces a lane's Channel.Close call in tests. It
	// remains nil in production and does not change the driver behavior.
	channelCloseHook func(*lane) error

	mu       sync.Mutex
	draining bool
	stopped  bool
	// teardownDone is fresh for each Stop or Release teardown attempt. Closing
	// it wakes waiters to re-inspect the result, not to assume success.
	teardownDone chan struct{}
	// tearingDown keeps one owner through lane closure, joins, unregistration,
	// and output closure. stopped only prevents new admission.
	tearingDown bool
	// teardownFinished becomes true only after unregistration and both output
	// closes. A failed attempt leaves the consumer stopped but retryable.
	teardownFinished bool

	outstanding int
	settlers    map[*settler]struct{}
	// admitted counts the settlers that hold a place in cfg.Prefetch: the
	// deliveries the broker still charges to one of this consumer's windows.
	// It differs from outstanding once the broker cancels a consumer, because
	// the broker takes that consumer's unsettled deliveries back while their
	// handlers may still be running; see attachReplacement.
	admitted int

	readers           sync.WaitGroup
	forward           sync.WaitGroup
	events            sync.WaitGroup
	forwarderStopOnce sync.Once
	stoppedOnce       sync.Once
}

type lane struct {
	owner       *consumer
	destination string
	channel     *amqp.Channel
	// channelMu serializes channel RPCs because AMQP does not correlate
	// requests with their replies. closeLanes waits for it only up to
	// closeLockWait, so a stuck RPC cannot hold Channel.Close back for long.
	// It also guards tag, which changes each time a server-initiated cancel is
	// re-established.
	channelMu    sync.Mutex
	closeMu      sync.Mutex
	closeOutcome *laneCloseOutcome
	tag          string
	prefetch     int
	// deliveriesMu guards deliveries and generation. The broker can cancel this
	// lane's consumer at any time, which closes the deliveries channel the
	// library handed out; watchCancel attaches a replacement and bumps the
	// generation, and the reader picks the replacement up by comparing the
	// generation it last drained against the current one. Only watchCancel
	// writes deliveries, so a replacement installed while the reader is still
	// draining the cancelled channel cannot be overwritten, and a reader that
	// has not yet noticed the close cannot miss it: the generation moves in the
	// same critical section that installs the channel.
	deliveriesMu sync.Mutex
	deliveries   <-chan amqp.Delivery
	generation   uint64
	// replacedC carries one token per attachment, waking a reader waiting on a
	// deliveries channel that has closed.
	replacedC chan struct{}
	pending   chan laneDelivery
	resume    chan struct{}
	emitting  int
	// Admission state is guarded by owner.mu; wake is reused across settlements.
	admissionLimit int
	admitted       int
	admissionWake  chan struct{}

	mu     sync.Mutex
	paused bool
}

// laneDelivery is a delivery with the generation of the consumer it came from,
// which is what tells admission whether the broker still counts it against the
// lane's window.
type laneDelivery struct {
	amqp.Delivery
	generation uint64
}

type laneCloseOutcome struct {
	// done closes after Channel.Close returns. amqp091's Channel.Close defers
	// connection.closeChannel(ch), so its return leaves the channel locally
	// closed and a second Close returns nil (channel.go:689-702). closeLanes
	// discards that error too, so a completed outcome is successful for Release
	// finalization.
	done chan struct{}
}

var _ driver.Consumer = (*consumer)(nil)

var _ driver.BacklogReader = (*consumer)(nil)

type consumerConstructionPhase uint8

const (
	consumerConstructionStarted consumerConstructionPhase = iota
	consumerConstructionLaneReady
	consumerConstructionReady
)

// consumerConstructionHook is a test-only synchronization seam. It remains
// nil in production and does not change the driver behavior.
var consumerConstructionHook func(*consumer, consumerConstructionPhase)

func newConsumer(conn *conn, cfg driver.ConsumerConfig) (*consumer, error) {
	seen := make(map[string]struct{}, len(cfg.Destinations))
	for _, destination := range cfg.Destinations {
		if _, exists := seen[destination]; exists {
			return nil, classify("consumer", driver.KindFatal, fmt.Errorf("duplicate destination %q", destination))
		}
		seen[destination] = struct{}{}
	}
	if err := validateBrokerPrefetch(cfg, conn.brokerPrefetch); err != nil {
		return nil, classify("consumer", driver.KindFatal, err)
	}

	c := &consumer{
		conn:                 conn,
		cfg:                  cfg,
		clock:                clock.NewReal(),
		trustBrokerTimestamp: conn.trustBrokerTimestamp,
		byName:               make(map[string]*lane, len(cfg.Destinations)),
		messages:             make(chan driver.InboundMessage, totalPrefetch(cfg, conn.brokerPrefetch)),
		errors:               make(chan error, 8),
		stoppedC:             make(chan struct{}),
		forwarderStopC:       make(chan struct{}),
		settlers:             make(map[*settler]struct{}),
	}
	rollback := func(err error) (*consumer, error) {
		_ = c.rollbackConstruction()
		return nil, err
	}
	if hook := consumerConstructionHook; hook != nil {
		hook(c, consumerConstructionStarted)
	}
	for index, destination := range cfg.Destinations {
		prefetch := effectivePrefetch(cfg, destination, index, conn.brokerPrefetch)
		channel, err := conn.amqp.Channel()
		if err != nil {
			return rollback(classifyAMQP("consumer", driver.KindTransient, err))
		}
		if err := channel.Qos(prefetch, 0, false); err != nil {
			_ = channel.Close()
			return rollback(classifyAMQP("consumer", driver.KindFatal, err))
		}
		tag := nextConsumerTag()
		// Registered before the consume call so a cancel that races the attach,
		// such as a queue deleted while the consumer is being created, is not
		// missed by a listener that does not exist yet.
		cancels := channel.NotifyCancel(make(chan string, 1))
		deliveries, err := channel.Consume(destination, tag, false, cfg.Exclusive, false, false, nil)
		if err != nil {
			_ = channel.Close()
			if cfg.Exclusive && isPermission(err) {
				return rollback(classify("consumer", driver.KindFatal, fmt.Errorf("exclusive consumer refused: %w", err)))
			}
			return rollback(classifyAMQP("consumer", driver.KindNotFound, err))
		}
		lane := &lane{
			owner:          c,
			destination:    destination,
			channel:        channel,
			tag:            tag,
			prefetch:       prefetch,
			admissionLimit: cfg.DestinationPrefetch(index),
			admissionWake:  make(chan struct{}, 1),
			deliveries:     deliveries,
			generation:     1,
			replacedC:      make(chan struct{}, 1),
			pending:        make(chan laneDelivery, prefetch),
			resume:         make(chan struct{}),
		}
		if cfg.Prefetch <= 0 {
			lane.admissionLimit = 0
		}
		c.mu.Lock()
		c.lanes = append(c.lanes, lane)
		c.byName[destination] = lane
		c.mu.Unlock()
		c.readers.Add(1)
		c.forward.Add(1)
		c.events.Add(2)
		go c.readDeliveries(lane)
		go c.emitMessages(lane)
		go c.watchClose(lane)
		go c.watchCancel(lane, cancels)
		if hook := consumerConstructionHook; hook != nil {
			hook(c, consumerConstructionLaneReady)
		}
	}
	if conn.brokerPrefetch > 0 {
		conn.log().Info(
			"RabbitMQ consumer opened with broker prefetch",
			"option", brokerPrefetchOption,
			"value", conn.brokerPrefetch,
			"destinations", len(cfg.Destinations),
		)
	}
	return c, nil
}

func totalPrefetch(cfg driver.ConsumerConfig, brokerPrefetch int) int {
	total := 0
	for index, destination := range cfg.Destinations {
		total += effectivePrefetch(cfg, destination, index, brokerPrefetch)
	}
	if cfg.Prefetch > 0 && cfg.Prefetch < total {
		total = cfg.Prefetch
	}
	if total < 1 {
		return 1
	}
	return total
}

func validateBrokerPrefetch(cfg driver.ConsumerConfig, brokerPrefetch int) error {
	if brokerPrefetch == 0 {
		return nil
	}
	for index, destination := range cfg.Destinations {
		coreWindow := cfg.DestinationPrefetch(index)
		if brokerPrefetch < coreWindow {
			return fmt.Errorf(
				"rabbitmq: invalid %s %d for destination %q: must be at least core window %d",
				brokerPrefetchOption, brokerPrefetch, destination, coreWindow,
			)
		}
	}
	return nil
}

func effectivePrefetch(cfg driver.ConsumerConfig, destination string, index, brokerPrefetch int) int {
	if brokerPrefetch > 0 {
		return brokerPrefetch
	}
	return cfg.DestinationPrefetch(index)
}

// closeLockWait bounds how long closeLanes waits, across all lanes, for an
// in-flight channel RPC to finish before it closes the channel anyway.
const closeLockWait = time.Second

// closeLanes closes every lane's channel. Close is itself an RPC, and AMQP
// does not correlate replies, so a Close that overlaps an in-flight Cancel can
// have its close-ok taken by the Cancel and then wait forever. Each Close
// therefore runs under the lane's RPC lock when the lock comes free before one
// shared deadline. Past that deadline the in-flight RPC is treated as stuck and
// the channel is closed without the lock, as a stuck RPC needs.
func (c *consumer) closeLanes() {
	deadline := c.clock.Now().Add(closeLockWait)
	for _, lane := range c.lanes {
		locked := lockBefore(c.clock, &lane.channelMu, deadline)
		_ = lane.channel.Close()
		if locked {
			lane.channelMu.Unlock()
		}
	}
}

// closeLanesWithContext starts each lane close once and waits for every close
// under ctx. A close outcome remains set so a later teardown joins the same
// broker close rather than issuing another AMQP close request.
func (c *consumer) closeLanesWithContext(ctx context.Context, op string) error {
	deadline := c.clock.Now().Add(closeLockWait)
	for _, lane := range c.lanes {
		lane.closeMu.Lock()
		outcome := lane.closeOutcome
		if outcome != nil {
			lane.closeMu.Unlock()
			if err := waitLaneClose(ctx, outcome); err != nil {
				return classify(op, driver.KindTransient, err)
			}
			continue
		}
		if err := ctx.Err(); err != nil {
			lane.closeMu.Unlock()
			return classify(op, driver.KindTransient, err)
		}
		locked, forced := lockBeforeContext(ctx, c.clock, &lane.channelMu, deadline)
		if !locked && !forced {
			err := ctx.Err()
			lane.closeMu.Unlock()
			if err == nil {
				err = context.DeadlineExceeded
			}
			return classify(op, driver.KindTransient, err)
		}
		outcome = &laneCloseOutcome{done: make(chan struct{})}
		lane.closeOutcome = outcome
		lane.closeMu.Unlock()

		closeFn := lane.channel.Close
		if hook := c.channelCloseHook; hook != nil {
			closeFn = func() error { return hook(lane) }
		}
		finish := func(_ error) {
			lane.closeMu.Lock()
			if locked {
				lane.channelMu.Unlock()
			}
			close(outcome.done)
			lane.closeMu.Unlock()
		}
		c.conn.startChannelCloseWithFinish(closeFn, finish)
		if err := waitLaneClose(ctx, outcome); err != nil {
			return classify(op, driver.KindTransient, err)
		}
	}
	return nil
}

func waitLaneClose(ctx context.Context, outcome *laneCloseOutcome) error {
	select {
	case <-outcome.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// lockBeforeContext takes mu before deadline unless ctx ends first. The
// deadlineReached result is the only path allowed to start a forced close.
func lockBeforeContext(ctx context.Context, clk clock.Clock, mu *sync.Mutex, deadline time.Time) (locked, deadlineReached bool) {
	for wait := time.Millisecond; ; wait = min(2*wait, 20*time.Millisecond) {
		if err := ctx.Err(); err != nil {
			return false, false
		}
		if mu.TryLock() {
			return true, false
		}
		if !clk.Now().Before(deadline) {
			return false, true
		}
		timer := clk.Timer(wait)
		select {
		case <-timer.C:
			if err := ctx.Err(); err != nil {
				return false, false
			}
			if !clk.Now().Before(deadline) {
				return false, true
			}
		case <-ctx.Done():
			timer.Stop()
			return false, false
		}
	}
}

// lockBefore takes mu if it comes free before deadline and reports whether it
// did. sync.Mutex has no timed lock, so it polls; it runs only on teardown.
func lockBefore(clk clock.Clock, mu *sync.Mutex, deadline time.Time) bool {
	for wait := time.Millisecond; ; wait = min(2*wait, 20*time.Millisecond) {
		if mu.TryLock() {
			return true
		}
		if !clk.Now().Before(deadline) {
			return false
		}
		<-clk.Timer(wait).C
	}
}

// rollbackConstruction tears down a consumer whose construction failed partway.
// It joins without a bound rather than going through stopAndWait, because
// construction has no caller context to honour and its only caller discards the
// error, so a context here would only name a cancellation nobody can deliver.
func (c *consumer) rollbackConstruction() error {
	c.mu.Lock()
	c.stopped = true
	c.mu.Unlock()
	c.stopSignals()
	c.closeLanes()
	c.readers.Wait()
	c.forward.Wait()
	c.events.Wait()
	return nil
}

// Messages returns the channel of delivered messages. The channel closes when Stop or Release completes.
func (c *consumer) Messages() <-chan driver.InboundMessage { return c.messages }

// Errors returns asynchronous consumer errors. The channel closes when Stop or Release completes.
func (c *consumer) Errors() <-chan error { return c.errors }

// nextConsumerTag returns a fresh consumer tag for an attach. Every attach
// needs a new one: RabbitMQ answers a reuse of a cancelled tag with
// NOT_ALLOWED - attempt to reuse consumer tag and closes the channel, so
// re-establishing on the cancelled tag would turn one cancel into a dead lane.
func nextConsumerTag() string {
	return "f1-consumer-" + strconv.FormatUint(consumerSequence.Add(1), 10)
}

// deliveriesState returns the deliveries channel the reader should drain and
// its generation. The two are read together because a caller that compares the
// generation against the one it last drained must not see them from different
// attachments.
func (l *lane) deliveriesState() (<-chan amqp.Delivery, uint64) {
	l.deliveriesMu.Lock()
	defer l.deliveriesMu.Unlock()
	return l.deliveries, l.generation
}

// attachReplacement installs the deliveries channel for the lane's next
// consumer, or ends the lane when there is none: nil means no consumer is
// coming, and the reader stops rather than waiting. It is the only writer of
// lane.deliveries.
//
// The teardown state is decided here under the consumer mutex rather than
// trusted from the caller, because teardown sets it under that same mutex and
// may have begun while the re-attach RPC was in flight. A live channel
// installed after Drain's cancel pass would leave a reader waiting for a
// consumer Drain has already gone past; an ended lane leaves the reader with
// nothing to wait for, and Stop or Release closes the channel that the extra
// consumer sits on.
func (c *consumer) attachReplacement(lane *lane, deliveries <-chan amqp.Delivery) {
	c.mu.Lock()
	if c.stopped || c.draining {
		deliveries = nil
	}
	lane.deliveriesMu.Lock()
	lane.deliveries = deliveries
	lane.generation++
	lane.deliveriesMu.Unlock()
	c.releaseCancelledAdmissionsLocked(lane)
	c.mu.Unlock()
	select {
	case lane.replacedC <- struct{}{}:
	default:
	}
}

// releaseCancelledAdmissionsLocked gives back the window places held by the
// lane's deliveries from consumers older than its current generation. The
// broker cancelled those consumers and took their unsettled deliveries back, so
// it charges none of them to the replacement; counting them would hold the
// replacement's window shut until handlers the broker already gave up on
// return. With a prefetch of 1 and a handler past the broker's consumer
// timeout, that is every delivery, the redelivered one included, until the
// handler ends.
//
// The settlers themselves stay: their handlers are still running, and Stop and
// Drain wait for them as before. Walk the two orders a delivery of the old
// consumer can take: admitted before the attachment, it is counted, and the
// sweep here stops counting it; still in lane.pending at the attachment,
// emitMessages finds its generation behind the lane's and never counts it.
// Both read and move the generation under c.mu, so neither order counts it.
// The caller holds c.mu.
func (c *consumer) releaseCancelledAdmissionsLocked(lane *lane) {
	released := false
	for s := range c.settlers {
		if s.counted && s.destination == lane.destination && s.generation < lane.generation {
			s.counted = false
			c.admitted--
			lane.admitted--
			released = true
		}
	}
	if released {
		c.wakeAdmissionsLocked()
	}
}

// wakeAdmissionsLocked wakes every lane waiting for a window place. Every
// destination may be waiting on the shared cap, and a single shared token could
// wake a destination whose own window is still full and strand another. The
// caller holds c.mu.
func (c *consumer) wakeAdmissionsLocked() {
	for _, lane := range c.lanes {
		select {
		case lane.admissionWake <- struct{}{}:
		default:
		}
	}
}

// ending reports whether the consumer has begun tearing down: Stop and Release
// set stopped, Drain sets draining. Either way this lane must not attach
// another consumer, and the reader has no reason to wait for a replacement.
func (c *consumer) ending() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopped || c.draining
}

// awaitAttachment blocks until watchCancel attaches a replacement for the
// deliveries channel the reader has drained, and reports whether the lane
// should keep reading. False means the lane is finished: the consumer is
// closing, or the re-establish attempt failed and was already reported.
func (c *consumer) awaitAttachment(lane *lane, drained uint64) bool {
	for {
		if c.ending() {
			return false
		}
		deliveries, generation := lane.deliveriesState()
		if generation > drained {
			return deliveries != nil
		}
		select {
		case <-lane.replacedC:
		case <-c.stoppedC:
			return false
		case <-c.forwarderStopC:
			return false
		}
	}
}

// readDeliveries pumps every delivery the lane's consumers receive into
// lane.pending. It outlives an individual consumer because the broker can
// cancel one under it: the deliveries channel closes, the reader waits for
// watchCancel to attach a replacement, and the forwarder and the pending
// channel stay exactly as they were, so a recovered lane does not look like a
// restart to the application.
func (c *consumer) readDeliveries(lane *lane) {
	defer c.readers.Done()
	defer close(lane.pending)
	drained := uint64(0)
	for {
		deliveries, generation := lane.deliveriesState()
		if deliveries == nil || generation <= drained {
			if !c.awaitAttachment(lane, drained) {
				return
			}
			continue
		}
		drained = generation
		for delivery := range deliveries {
			if c.ending() {
				continue
			}
			if c.readerSendHook != nil {
				c.readerSendHook(lane)
			}
			select {
			case lane.pending <- laneDelivery{Delivery: delivery, generation: drained}:
			case <-c.stoppedC:
				return
			}
		}
	}
}

func (c *consumer) emitMessages(lane *lane) {
	defer func() {
		if c.forwarderExitHook != nil {
			c.forwarderExitHook()
		}
		c.forward.Done()
	}()
	for {
		var (
			item laneDelivery
			ok   bool
		)
		select {
		case item, ok = <-lane.pending:
			if !ok {
				return
			}
		case <-c.forwarderStopC:
			return
		}
		lane.mu.Lock()
		lane.emitting++
		lane.mu.Unlock()
		c.mu.Lock()
		if c.draining || c.stopped {
			c.mu.Unlock()
			lane.mu.Lock()
			lane.emitting--
			lane.mu.Unlock()
			return
		}
		c.mu.Unlock()
		for {
			c.mu.Lock()
			if c.draining || c.stopped {
				c.mu.Unlock()
				lane.mu.Lock()
				lane.emitting--
				lane.mu.Unlock()
				return
			}
			c.mu.Unlock()
			lane.mu.Lock()
			paused, resume := lane.paused, lane.resume
			lane.mu.Unlock()
			if !paused {
				break
			}
			select {
			case <-resume:
			case <-c.forwarderStopC:
				lane.mu.Lock()
				lane.emitting--
				lane.mu.Unlock()
				return
			case <-c.stoppedC:
				lane.mu.Lock()
				lane.emitting--
				lane.mu.Unlock()
				return
			}
		}
		var admitted *settler
		for admitted == nil {
			c.mu.Lock()
			if c.draining || c.stopped {
				c.mu.Unlock()
				lane.mu.Lock()
				lane.emitting--
				lane.mu.Unlock()
				return
			}
			lane.mu.Lock()
			paused := lane.paused
			var resume <-chan struct{}
			if paused {
				resume = lane.resume
			}
			lane.mu.Unlock()
			// A delivery from a consumer the broker has since cancelled was
			// taken back with that consumer, so it holds no place in any
			// window and is admitted without one. lane.generation is read
			// under c.mu, which attachReplacement also holds to move it.
			stale := item.generation < lane.generation
			if !paused && (stale || ((c.cfg.Prefetch <= 0 || c.admitted < c.cfg.Prefetch) &&
				(lane.admissionLimit <= 0 || lane.admitted < lane.admissionLimit))) {
				admitted = &settler{owner: c, destination: lane.destination, delivery: item.Delivery, generation: item.generation, counted: !stale}
				c.settlers[admitted] = struct{}{}
				c.outstanding++
				if !stale {
					c.admitted++
					lane.admitted++
				}
			}
			c.mu.Unlock()
			if admitted == nil {
				select {
				case <-lane.admissionWake:
				case <-resume:
				case <-c.forwarderStopC:
					lane.mu.Lock()
					lane.emitting--
					lane.mu.Unlock()
					return
				case <-c.stoppedC:
					lane.mu.Lock()
					lane.emitting--
					lane.mu.Unlock()
					return
				}
			}
		}
		lane.mu.Lock()
		lane.emitting--
		lane.mu.Unlock()
		message := c.inboundMessage(lane.destination, item.Delivery, admitted, c.nativeDeliveryCount(), c.trustBrokerTimestamp)
		select {
		case c.messages <- message:
		case <-c.forwarderStopC:
			c.release(admitted)
			return
		case <-c.stoppedC:
			c.release(admitted)
			return
		}
	}
}

func (c *consumer) hasDestination(destination string) bool {
	for _, lane := range c.lanes {
		if lane.destination == destination {
			return true
		}
	}
	return false
}

func (c *consumer) nativeDeliveryCount() bool {
	if c.cfg.Effective == (driver.Capabilities{}) {
		return c.conn.caps.NativeDeliveryCount
	}
	return c.cfg.Effective.NativeDeliveryCount
}

func (c *consumer) watchClose(lane *lane) {
	defer c.events.Done()
	closeErrors := make(chan *amqp.Error, 1)
	lane.channel.NotifyClose(closeErrors)
	select {
	case err, ok := <-closeErrors:
		if ok && err != nil {
			c.sendError(classifyAMQP("consumer", driver.KindTransient, err))
		}
	case <-c.stoppedC:
	}
}

// watchCancel handles a server-initiated basic.cancel by re-establishing the
// lane's consumer.
//
// RabbitMQ 4.3 cancels a consumer that holds a delivery past the queue's
// x-consumer-timeout, and the library answers by closing the deliveries channel
// it handed out while reporting nothing else. Without this, that close is
// indistinguishable from a normal end of stream: the reader stops, no error is
// sent, and the subscription silently stops delivering while every other signal
// the application has still reads healthy.
//
// The cancel is also reported. The broker cancels for a reason the application
// owns, a handler that outran the timeout, so recovering in silence would hide
// the only evidence of it, and the consumer would be cancelled again and again
// and always invisibly.
func (c *consumer) watchCancel(lane *lane, cancels <-chan string) {
	defer c.events.Done()
	for {
		select {
		case _, ok := <-cancels:
			if !ok {
				// The library closes this channel when the channel itself
				// shuts down, so no replacement is coming. Ending the lane is
				// the behaviour a death of the deliveries channel always had;
				// without it the reader would wait for a consumer that can
				// never attach, and watchClose reports the death separately.
				c.attachReplacement(lane, nil)
				return
			}
		case <-c.stoppedC:
			return
		}
		c.reestablish(lane)
	}
}

// reestablish attaches a fresh consumer to a lane the broker cancelled, and
// tells the application that the cancel happened. A failure to attach ends the
// lane and is reported as an error, because a lane that neither delivers nor
// says why is the defect this whole path exists to remove.
func (c *consumer) reestablish(lane *lane) {
	if c.ending() {
		// Teardown already began; the lane is going away because the
		// application asked it to, so there is nothing to recover and nothing
		// the application needs to hear about.
		c.attachReplacement(lane, nil)
		return
	}
	c.sendError(classify("consumer", driver.KindNotification, fmt.Errorf("rabbitmq: broker cancelled the consumer for destination %q", lane.destination)))
	lane.channelMu.Lock()
	// Drain marks the consumer draining before it cancels, and cancels under
	// this lock, so reading the mark here orders the two: either Drain has not
	// begun and cancels the tag attached below, or it has and no tag is
	// attached. Checked only above, a Drain in between would cancel the old tag
	// and leave the new one holding deliveries nobody reads.
	if c.ending() {
		lane.channelMu.Unlock()
		c.attachReplacement(lane, nil)
		return
	}
	tag := nextConsumerTag()
	deliveries, err := lane.channel.Consume(lane.destination, tag, false, c.cfg.Exclusive, false, false, nil)
	if err == nil {
		lane.tag = tag
	}
	lane.channelMu.Unlock()
	if err != nil {
		if !errors.Is(err, amqp.ErrClosed) {
			c.sendError(classifyAMQP("consumer", driver.KindNotFound, fmt.Errorf("re-establishing destination %q: %w", lane.destination, err)))
		}
		c.attachReplacement(lane, nil)
		return
	}
	c.attachReplacement(lane, deliveries)
}

// sendError reports err without blocking. A full buffer drops a transient
// error or a notification, which the errors already queued stand for. A severe
// error, one that ends a lane for good, evicts the oldest queued error instead,
// unless that one is fatal: a failed re-attach may be the only report that a
// lane stopped, and a burst of consumer cancels must not crowd it out.
func (c *consumer) sendError(err error) {
	if err == nil {
		return
	}
	c.errorsMu.Lock()
	defer c.errorsMu.Unlock()
	select {
	case c.errors <- err:
		return
	default:
	}
	kind, classified := driver.Classify(err)
	if !classified || kind == driver.KindTransient || kind == driver.KindNotification {
		return
	}
	select {
	case evicted := <-c.errors:
		if evictedKind, evictedClassified := driver.Classify(evicted); evictedClassified && evictedKind == driver.KindFatal && kind != driver.KindFatal {
			err = evicted
		}
	default:
	}
	select {
	case c.errors <- err:
	default:
	}
}

// inboundMessage builds a portable delivery using the consumer's receipt clock.
func (c *consumer) inboundMessage(destination string, delivery amqp.Delivery, settler *settler, nativeCount bool, trustBrokerTimestamp bool) driver.InboundMessage {
	return inboundMessageAt(destination, delivery, settler, nativeCount, trustBrokerTimestamp, c.clock.Now())
}

func inboundMessageAt(destination string, delivery amqp.Delivery, settler *settler, nativeCount bool, trustBrokerTimestamp bool, receivedAt time.Time) driver.InboundMessage {
	headers := amqpHeaders(delivery)
	enqueuedAt, enqueuedAtSource := inboundEnqueueTime(delivery, trustBrokerTimestamp)
	return driver.InboundMessage{
		Destination:      destination,
		Key:              headerValue(delivery.Headers[partitionKeyHeader]),
		Headers:          headers,
		Body:             append([]byte(nil), delivery.Body...),
		DeliveryCount:    deliveryCount(delivery, nativeCount),
		ReceivedAt:       receivedAt,
		EnqueuedAt:       enqueuedAt,
		EnqueuedAtSource: enqueuedAtSource,
		Ref:              driver.BrokerRef{Tag: delivery.DeliveryTag},
		Settle:           settler,
	}
}

// inboundEnqueueTime applies the approved precedence without using receipt
// time. timestamp_in_ms is broker-supplied only when the operator has enabled
// the trust assertion.
// RabbitMQ's AMQP Timestamp property has whole-second precision, so it is used
// as a producer timestamp only after the higher-precision CloudEvents header
// has been checked.
func inboundEnqueueTime(delivery amqp.Delivery, trustBrokerTimestamp bool) (time.Time, driver.EnqueueSource) {
	if trustBrokerTimestamp {
		if timestamp, ok := timestampInMilliseconds(delivery.Headers["timestamp_in_ms"]); ok {
			return time.UnixMilli(timestamp), driver.EnqueueSourceBroker
		}
	}
	if value, ok := delivery.Headers["cloudEvents:time"]; ok {
		if timestamp, err := time.Parse(time.RFC3339Nano, string(headerValue(value))); err == nil {
			return timestamp, driver.EnqueueSourceProducer
		}
	}
	if !delivery.Timestamp.IsZero() {
		return time.Unix(delivery.Timestamp.Unix(), 0), driver.EnqueueSourceProducer
	}
	return time.Time{}, driver.EnqueueSourceUnknown
}

func timestampInMilliseconds(value any) (int64, bool) {
	switch value := value.(type) {
	case int:
		return int64(value), true
	case int8:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	case uint:
		if uint64(value) > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(value), true
	case uint8:
		return int64(value), true
	case uint16:
		return int64(value), true
	case uint32:
		return int64(value), true
	case uint64:
		if value > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(value), true
	case string:
		timestamp, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		return timestamp, err == nil
	case []byte:
		timestamp, err := strconv.ParseInt(strings.TrimSpace(string(value)), 10, 64)
		return timestamp, err == nil
	default:
		return 0, false
	}
}

// Lag returns the backlog count for each destination assigned to this consumer.
func (c *consumer) Lag(ctx context.Context) (map[string]int64, error) {
	return c.readLag(ctx, "lag")
}

// Backlog reports the queued count and, for classic queues when available, the enqueue time of the oldest message.
func (c *consumer) Backlog(ctx context.Context) (map[string]driver.BacklogSample, error) {
	lag, err := c.readLag(ctx, "backlog")
	if err != nil {
		return nil, err
	}
	out := make(map[string]driver.BacklogSample, len(c.lanes))
	for _, lane := range c.lanes {
		sample := driver.BacklogSample{Lag: lag[lane.destination]}
		if c.conn.queueKind == queueKindClassic {
			if err := ctx.Err(); err != nil {
				return nil, classify("backlog", driver.KindTransient, err)
			}
			queue, err := c.conn.management.getQueue(ctx, lane.destination)
			if err == nil {
				sample.HeadEnqueuedAt, sample.HeadSource = queue.headEnqueuedAt()
			} else if ctx.Err() != nil {
				return nil, classify("backlog", driver.KindTransient, ctx.Err())
			}
		}
		out[lane.destination] = sample
	}
	return out, nil
}

func (c *consumer) readLag(ctx context.Context, operation string) (map[string]int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify(operation, driver.KindTransient, err)
	}
	if !c.cfg.Effective.LagQueryable {
		return nil, classify(operation, driver.KindFatal, driver.ErrUnsupported)
	}
	admin := &adminOperations{conn: c.conn}
	lag := make(map[string]int64, len(c.lanes))
	for _, lane := range c.lanes {
		if err := ctx.Err(); err != nil {
			return nil, classify(operation, driver.KindTransient, err)
		}
		ready, err := admin.inspectQueue(ctx, lane.destination)
		if err != nil {
			if isNotFound(err) {
				return nil, classify(operation, driver.KindNotFound, errors.Join(driver.ErrDestinationMissing, fmt.Errorf("destination %q is missing", lane.destination)))
			}
			return nil, classifyAMQP(operation, driver.KindTransient, err)
		}
		lag[lane.destination] = ready
	}
	return lag, nil
}

// deliveryCount reports the broker's redelivery counter. RabbitMQ 4.x names it
// x-acquired-count on quorum queues; x-delivery-count is the pre-4.0 name and is
// still accepted so a driver pointed at an older broker keeps working.
func deliveryCount(delivery amqp.Delivery, nativeCount bool) int {
	if !nativeCount {
		return -1
	}
	for _, name := range []string{"x-acquired-count", "x-delivery-count"} {
		if count, ok := headerCount(delivery.Headers[name]); ok {
			return count
		}
	}
	if delivery.Redelivered {
		// The broker redelivered but reported no count. Guessing 1 would state a
		// count this driver does not have, and a caller cannot tell a guess from a
		// real one; -1 is the port's value for an unavailable counter.
		return -1
	}
	// Quorum queues, the only kind this driver declares, omit the header on a
	// first delivery. That is a real count of zero, not an absent one.
	return 0
}

// maxCount is the largest redelivery count this driver can report. AMQP header
// values are wider than int, and a count that does not fit is clamped rather
// than wrapped into a small or negative number.
const maxCount = int(^uint(0) >> 1)

func headerCount(value any) (int, bool) {
	switch count := value.(type) {
	case int:
		return count, true
	case int8:
		return int(count), true
	case int16:
		return int(count), true
	case int32:
		return int(count), true
	case int64:
		if count > int64(maxCount) {
			return maxCount, true
		}
		return int(count), true
	case uint:
		if count > uint(maxCount) {
			return maxCount, true
		}
		return int(count), true
	case uint8:
		return int(count), true
	case uint16:
		return int(count), true
	case uint32:
		return int(count), true
	case uint64:
		if count > uint64(maxCount) {
			return maxCount, true
		}
		return int(count), true
	default:
		return 0, false
	}
}

func headerValue(value any) []byte {
	switch value := value.(type) {
	case nil:
		return nil
	case []byte:
		return append([]byte(nil), value...)
	case string:
		return []byte(value)
	case bool:
		return []byte(strconv.FormatBool(value))
	case int:
		return []byte(strconv.Itoa(value))
	case int8:
		return []byte(strconv.FormatInt(int64(value), 10))
	case int16:
		return []byte(strconv.FormatInt(int64(value), 10))
	case int32:
		return []byte(strconv.FormatInt(int64(value), 10))
	case int64:
		return []byte(strconv.FormatInt(value, 10))
	case uint:
		return []byte(strconv.FormatUint(uint64(value), 10))
	case uint8:
		return []byte(strconv.FormatUint(uint64(value), 10))
	case uint16:
		return []byte(strconv.FormatUint(uint64(value), 10))
	case uint32:
		return []byte(strconv.FormatUint(uint64(value), 10))
	case uint64:
		return []byte(strconv.FormatUint(value, 10))
	default:
		return fmt.Append(nil, value)
	}
}

// Pause pauses delivery from the listed destinations. With no destinations, it pauses all subscribed destinations.
func (c *consumer) Pause(destinations ...string) error {
	return c.setPaused(destinations, true)
}

// Resume resumes delivery from the listed destinations. With no destinations, it resumes all subscribed destinations.
func (c *consumer) Resume(destinations ...string) error {
	return c.setPaused(destinations, false)
}

func (c *consumer) setPaused(destinations []string, paused bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return classify("consumer", driver.KindFatal, errors.New("consumer stopped"))
	}
	targets := destinations
	if len(targets) == 0 {
		targets = c.cfg.Destinations
	}
	for _, destination := range targets {
		lane, ok := c.byName[destination]
		if !ok {
			return classify("consumer", driver.KindNotFound, driver.ErrDestinationMissing)
		}
		lane.mu.Lock()
		if lane.paused == paused {
			lane.mu.Unlock()
			continue
		}
		lane.paused = paused
		if !paused {
			close(lane.resume)
			lane.resume = make(chan struct{})
		}
		lane.mu.Unlock()
	}
	return nil
}

// Drain stops new deliveries while keeping outstanding messages settleable.
func (c *consumer) Drain(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("drain", driver.KindTransient, err)
	}
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil
	}
	c.draining = true
	c.stopForwarders()
	lanes := append([]*lane(nil), c.lanes...)
	c.mu.Unlock()

	var drainErr error
	for _, lane := range lanes {
		if err := cancelConsumer(ctx, lane); err != nil && !errors.Is(err, amqp.ErrClosed) {
			drainErr = classifyAMQP("drain", driver.KindTransient, err)
			break
		}
	}
	// forwarderStopC is closed before this join, and every blocking point in
	// emitMessages observes it. Removing an escape can make Drain hang forever.
	c.forward.Wait()
	return drainErr
}

func (c *consumer) stopForwarders() {
	c.forwarderStopOnce.Do(func() { close(c.forwarderStopC) })
}

func (c *consumer) stopSignals() {
	c.stopForwarders()
	c.stoppedOnce.Do(func() { close(c.stoppedC) })
}

// stopAndWait stops delivery, optionally closes the lanes, and joins the reader,
// forwarder and watcher goroutines under ctx. op names the operation for error
// classification, and a "stop" deadline surfaces as ErrDrainTimeout. Closing the
// lanes before the joins is what requeues their unacked deliveries and what lets
// a blocked forwarder observe forwarderStopC and stoppedC instead of waiting for
// the application to consume an abandoned message.
func (c *consumer) stopAndWait(ctx context.Context, op string, closeLanes bool) error {
	c.stopSignals()
	if closeLanes {
		if err := c.closeLanesWithContext(ctx, op); err != nil {
			return err
		}
	}
	if err := c.waitReaders(ctx, op); err != nil {
		return err
	}
	if err := c.waitForwarders(ctx, op); err != nil {
		return err
	}
	return c.waitEvents(ctx, op)
}

// waitFor joins wg under ctx. It is the one place the three teardown joins
// (readers, forwarders, watchers) are bounded, so their cancellation behaviour
// cannot drift apart. A "stop" deadline surfaces as ErrDrainTimeout; every other
// wait reports the context error under op.
func waitFor(ctx context.Context, op string, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return teardownContextError(op, ctx.Err())
	}
}

func (c *consumer) waitReaders(ctx context.Context, op string) error {
	return waitFor(ctx, op, &c.readers)
}

func (c *consumer) waitForwarders(ctx context.Context, op string) error {
	return waitFor(ctx, op, &c.forward)
}

func (c *consumer) waitEvents(ctx context.Context, op string) error {
	return waitFor(ctx, op, &c.events)
}

func cancelConsumer(ctx context.Context, lane *lane) error {
	result := make(chan error, 1)
	go func() {
		lane.channelMu.Lock()
		defer lane.channelMu.Unlock()
		if hook := lane.owner.cancelHook; hook != nil {
			hook()
		}
		result <- lane.channel.Cancel(lane.tag, false)
	}()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop drains the consumer and closes its channels after every delivered message has been settled.
// It returns an error while messages remain unsettled. Concurrent Stop and Release
// calls wait for the same teardown under their own contexts. The owning Stop's
// final lane close is unbounded, as it is after a successful drain.
func (c *consumer) Stop(ctx context.Context) error {
	return c.teardown(ctx, "stop")
}

// Release abandons unsettled deliveries, closes the consumer, and lets RabbitMQ redeliver them.
// Concurrent Stop and Release calls share teardown; a failed attempt is retryable.
func (c *consumer) Release(ctx context.Context) error {
	return c.teardown(ctx, "release")
}

// teardown coordinates final closure without merging Stop's settlement refusal
// with Release's abandonment. op also preserves their context-error contracts.
func (c *consumer) teardown(ctx context.Context, op string) error {
	var prepared bool
	var prepareErr error
	for {
		c.mu.Lock()
		if c.teardownFinished {
			c.mu.Unlock()
			return nil
		}
		if c.tearingDown {
			attempt := c.teardownDone
			c.mu.Unlock()
			// A has stopped admission and is blocked closing a lane. B must
			// wait here, not mistake stopped for completed unregistration.
			// A may fail on its context, so B re-inspects after waking.
			select {
			case <-attempt:
				continue
			case <-ctx.Done():
				return teardownContextError(op, ctx.Err())
			}
		}
		if op == "stop" && !c.stopped && prepared && prepareErr != nil {
			c.mu.Unlock()
			return prepareErr
		}
		if err := ctx.Err(); err != nil {
			c.mu.Unlock()
			return teardownContextError(op, err)
		}
		if op == "stop" && !c.stopped {
			if !prepared {
				c.mu.Unlock()
				prepareErr = c.prepareStop(ctx)
				prepared = true
				// A drains outside mu while B claims Release and stops
				// admission. A must re-check B's attempt before returning
				// its drain result or refusing B's abandoned settlements.
				continue
			}
			if c.outstanding != 0 {
				count := c.outstanding
				c.mu.Unlock()
				return classify("stop", driver.KindFatal, fmt.Errorf("%w: %d outstanding messages", driver.ErrResourcesOutstanding, count))
			}
		}
		// The outstanding check and claim share mu. If Release already
		// admitted teardown but failed, stopped skips a new drain obligation:
		// closing its lanes has already selected broker redelivery.
		c.stopped = true
		c.tearingDown = true
		c.teardownDone = make(chan struct{})
		c.mu.Unlock()

		var err error
		if op == "stop" {
			err = c.closeLanesWithContext(context.Background(), "stop") //nolint:contextcheck // preserve Stop's unbounded final lane close after drain
			if err == nil && (!prepared || prepareErr != nil) {
				// A Release may have failed before its local joins. A Stop
				// taking over closes lanes first, then joins those goroutines
				// rather than relying on its skipped drain preparation.
				err = c.stopAndWait(ctx, "stop", false)
			}
		} else {
			// Requeue before joins so an abandoned forwarder can escape
			// without the application receiving its pending delivery.
			err = c.stopAndWait(ctx, "release", true)
		}
		if err == nil {
			c.conn.removeConsumer(c)
			close(c.messages)
			close(c.errors)
		}
		c.mu.Lock()
		c.teardownFinished = err == nil
		c.tearingDown = false
		close(c.teardownDone)
		c.mu.Unlock()
		return err
	}
}

func (c *consumer) prepareStop(ctx context.Context) error {
	if err := c.Drain(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, driver.ErrDrainTimeout) {
			return teardownContextError("stop", err)
		}
		return err
	}
	return c.stopAndWait(ctx, "stop", false)
}

func teardownContextError(op string, err error) error {
	if op == "stop" && errors.Is(err, context.DeadlineExceeded) {
		err = fmt.Errorf("%w: %w", driver.ErrDrainTimeout, err)
	}
	return classify(op, driver.KindTransient, err)
}

func (c *consumer) release(settler *settler) {
	c.mu.Lock()
	if _, exists := c.settlers[settler]; !exists {
		c.mu.Unlock()
		return
	}
	delete(c.settlers, settler)
	c.outstanding--
	if settler.counted {
		c.admitted--
		if lane := c.byName[settler.destination]; lane != nil {
			lane.admitted--
		}
	}
	c.wakeAdmissionsLocked()
	c.mu.Unlock()
}

func amqpHeaders(delivery amqp.Delivery) []driver.Header {
	values := make(map[string][]byte, len(delivery.Headers)+4)
	for key, value := range delivery.Headers {
		// Only cloudEvents: headers are envelope attributes. Everything else is
		// owned by the broker or by this driver (x-delivery-count, x-death, the
		// partition key) and is not part of the portable header set. Matching on
		// the prefix rather than trimming it first keeps an attribute that is
		// itself named x-something, which the core is free to send.
		name, prefixed := strings.CutPrefix(key, "cloudEvents:")
		if !prefixed {
			continue
		}
		values[name] = headerValue(value)
	}
	// AMQP properties fill in only where the attribute header is absent. The
	// header carries the value the core sent; the property is a lossy copy for
	// AMQP-native tooling, and the timestamp property in particular is POSIX
	// seconds, so preferring it would silently truncate a sub-second time.
	setProperty := func(key, value string) {
		if value == "" {
			return
		}
		if _, ok := values[key]; ok {
			return
		}
		values[key] = []byte(value)
	}
	setProperty(wire.ID, delivery.MessageId)
	setProperty(wire.Type, delivery.Type)
	setProperty(wire.DataContentType, delivery.ContentType)
	setProperty(wire.CorrelationID, delivery.CorrelationId)
	if !delivery.Timestamp.IsZero() {
		setProperty(wire.Time, delivery.Timestamp.Format(time.RFC3339Nano))
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	headers := make([]driver.Header, 0, len(keys))
	for _, key := range keys {
		headers = append(headers, driver.Header{Key: key, Value: values[key]})
	}
	return headers
}
