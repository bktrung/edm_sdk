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
)

// partitionKeyHeader carries OutboundMessage.Key across the wire. A fanout
// exchange ignores the AMQP routing key, so the ordering identity cannot ride
// there, and it is not an envelope attribute so it does not belong under the
// cloudEvents: prefix.
const partitionKeyHeader = "x-f1-partition-key"

var consumerSequence atomic.Uint64

type consumer struct {
	conn     *conn
	cfg      driver.ConsumerConfig
	lanes    []*lane
	byName   map[string]*lane
	messages chan driver.InboundMessage
	errors   chan error
	stoppedC chan struct{}
	// Drain releases local forwarders; stoppedC remains the completed-teardown signal.
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

	mu          sync.Mutex
	draining    bool
	stopped     bool
	outstanding int
	settlers    map[*settler]struct{}

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
	// requests with their replies. Channel.Close deliberately does not take it
	// because closing releases a stuck RPC. It also guards tag, which changes
	// each time a server-initiated cancel is re-established.
	channelMu sync.Mutex
	tag       string
	prefetch  int
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
	pending   chan amqp.Delivery
	resume    chan struct{}
	emitting  int

	mu     sync.Mutex
	paused bool
}

var _ driver.Consumer = (*consumer)(nil)

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

	c := &consumer{
		conn:           conn,
		cfg:            cfg,
		byName:         make(map[string]*lane, len(cfg.Destinations)),
		messages:       make(chan driver.InboundMessage, totalPrefetch(cfg)),
		errors:         make(chan error, 8),
		stoppedC:       make(chan struct{}),
		forwarderStopC: make(chan struct{}),
		settlers:       make(map[*settler]struct{}),
	}
	rollback := func(err error) (*consumer, error) {
		_ = c.rollbackConstruction()
		return nil, err
	}
	if hook := consumerConstructionHook; hook != nil {
		hook(c, consumerConstructionStarted)
	}
	for index, destination := range cfg.Destinations {
		prefetch := destinationPrefetch(cfg, destination, index)
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
			owner:       c,
			destination: destination,
			channel:     channel,
			tag:         tag,
			prefetch:    prefetch,
			deliveries:  deliveries,
			generation:  1,
			replacedC:   make(chan struct{}, 1),
			pending:     make(chan amqp.Delivery, prefetch),
			resume:      make(chan struct{}),
		}
		c.lanes = append(c.lanes, lane)
		c.byName[destination] = lane
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
	return c, nil
}

func totalPrefetch(cfg driver.ConsumerConfig) int {
	total := 0
	for index, destination := range cfg.Destinations {
		total += destinationPrefetch(cfg, destination, index)
	}
	if total < 1 {
		return 1
	}
	return total
}

func destinationPrefetch(cfg driver.ConsumerConfig, destination string, index int) int {
	if value := cfg.PerDestination[destination]; value > 0 {
		return value
	}
	if cfg.Prefetch > 0 && len(cfg.Destinations) > 0 {
		base := cfg.Prefetch / len(cfg.Destinations)
		if index < cfg.Prefetch%len(cfg.Destinations) {
			base++
		}
		if base > 0 {
			return base
		}
	}
	return 1
}

func (c *consumer) closeLanes() {
	for _, lane := range c.lanes {
		_ = lane.channel.Close()
	}
}

func (c *consumer) rollbackConstruction() error {
	c.mu.Lock()
	c.stopped = true
	c.mu.Unlock()
	return c.stopAndWait(true, nil)
}

func (c *consumer) Messages() <-chan driver.InboundMessage { return c.messages }
func (c *consumer) Errors() <-chan error                   { return c.errors }

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
	c.mu.Unlock()
	select {
	case lane.replacedC <- struct{}{}:
	default:
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
			case lane.pending <- delivery:
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
			delivery amqp.Delivery
			ok       bool
		)
		select {
		case delivery, ok = <-lane.pending:
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
		settler := &settler{owner: c, destination: lane.destination, delivery: delivery}
		c.mu.Lock()
		if c.draining || c.stopped {
			c.mu.Unlock()
			lane.mu.Lock()
			lane.emitting--
			lane.mu.Unlock()
			return
		}
		c.settlers[settler] = struct{}{}
		c.outstanding++
		c.mu.Unlock()
		lane.mu.Lock()
		lane.emitting--
		lane.mu.Unlock()
		message := inboundMessage(lane.destination, delivery, settler, c.nativeDeliveryCount())
		select {
		case c.messages <- message:
		case <-c.forwarderStopC:
			c.release(settler)
			return
		case <-c.stoppedC:
			c.release(settler)
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

func (c *consumer) sendError(err error) {
	if err == nil {
		return
	}
	select {
	case c.errors <- err:
	default:
	}
}

func inboundMessage(destination string, delivery amqp.Delivery, settler *settler, nativeCount bool) driver.InboundMessage {
	headers := amqpHeaders(delivery)
	receivedAt := delivery.Timestamp
	if receivedAt.IsZero() {
		receivedAt = time.Now() //nolint:forbidigo // the port requires receipt time and drivers have no clock dependency
	}
	return driver.InboundMessage{
		Destination:   destination,
		Key:           headerValue(delivery.Headers[partitionKeyHeader]),
		Headers:       headers,
		Body:          append([]byte(nil), delivery.Body...),
		DeliveryCount: deliveryCount(delivery, nativeCount),
		ReceivedAt:    receivedAt,
		Ref:           driver.BrokerRef{Tag: delivery.DeliveryTag},
		Settle:        settler,
	}
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

func (c *consumer) Pause(destinations ...string) error {
	return c.setPaused(destinations, true)
}

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

func (c *consumer) stopAndWait(closeLanes bool, wait func() error) error {
	c.stopSignals()
	if closeLanes {
		c.closeLanes()
	}
	if wait != nil {
		if err := wait(); err != nil {
			return err
		}
	} else {
		c.readers.Wait()
		c.forward.Wait()
	}
	c.events.Wait()
	return nil
}

func (c *consumer) waitReaders(ctx context.Context, op string) error {
	done := make(chan struct{})
	go func() {
		c.readers.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		if op == "stop" && errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return classify("stop", driver.KindTransient, fmt.Errorf("%w: %w", driver.ErrDrainTimeout, ctx.Err()))
		}
		return classify(op, driver.KindTransient, ctx.Err())
	}
}

func (c *consumer) waitForwarders(ctx context.Context, op string) error {
	done := make(chan struct{})
	go func() {
		c.forward.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		if op == "stop" && errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return classify("stop", driver.KindTransient, fmt.Errorf("%w: %w", driver.ErrDrainTimeout, ctx.Err()))
		}
		return classify(op, driver.KindTransient, ctx.Err())
	}
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

func (c *consumer) Stop(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return classify("stop", driver.KindTransient, fmt.Errorf("%w: %w", driver.ErrDrainTimeout, err))
		}
		return classify("stop", driver.KindTransient, err)
	}
	drainErr := c.Drain(ctx)
	if drainErr != nil {
		if errors.Is(drainErr, context.DeadlineExceeded) && !errors.Is(drainErr, driver.ErrDrainTimeout) {
			return classify("stop", driver.KindTransient, fmt.Errorf("%w: %w", driver.ErrDrainTimeout, drainErr))
		}
		return drainErr
	}
	wait := func() error {
		if err := c.waitReaders(ctx, "stop"); err != nil {
			return err
		}
		return c.waitForwarders(ctx, "stop")
	}
	if err := c.stopAndWait(false, wait); err != nil {
		return err
	}
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil
	}
	if c.outstanding != 0 {
		count := c.outstanding
		c.mu.Unlock()
		return classify("stop", driver.KindFatal, fmt.Errorf("%w: %d outstanding messages", driver.ErrResourcesOutstanding, count))
	}
	c.stopped = true
	c.mu.Unlock()
	c.closeLanes()
	c.conn.removeConsumer(c)
	close(c.messages)
	close(c.errors)
	return nil
}

func (c *consumer) Release(ctx context.Context) error {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil
	}
	if err := ctx.Err(); err != nil {
		c.mu.Unlock()
		return classify("release", driver.KindTransient, err)
	}
	c.stopped = true
	c.mu.Unlock()

	// Closing the AMQP channels requeues their unacked deliveries. Do this
	// before waiting for local goroutines so a blocked forwarder can observe
	// forwarderStopC and stoppedC rather than waiting for the application to
	// consume an abandoned message.
	if err := c.stopAndWait(true, nil); err != nil {
		return err
	}
	c.conn.removeConsumer(c)
	close(c.messages)
	close(c.errors)
	return nil
}

func (c *consumer) Lag(ctx context.Context) (map[string]int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("lag", driver.KindTransient, err)
	}
	if !c.cfg.Effective.LagQueryable {
		return nil, classify("lag", driver.KindFatal, driver.ErrUnsupported)
	}
	admin := &adminOperations{conn: c.conn}
	lag := make(map[string]int64, len(c.lanes))
	for _, lane := range c.lanes {
		if err := ctx.Err(); err != nil {
			return nil, classify("lag", driver.KindTransient, err)
		}
		ready, err := admin.inspectQueue(ctx, lane.destination)
		if err != nil {
			if isNotFound(err) {
				return nil, classify("lag", driver.KindNotFound, errors.Join(driver.ErrDestinationMissing, fmt.Errorf("destination %q is missing", lane.destination)))
			}
			return nil, classifyAMQP("lag", driver.KindTransient, err)
		}
		lag[lane.destination] = ready
	}
	return lag, nil
}

func (c *consumer) release(settler *settler) {
	c.mu.Lock()
	delete(c.settlers, settler)
	if c.outstanding > 0 {
		c.outstanding--
	}
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
	setProperty("id", delivery.MessageId)
	setProperty("type", delivery.Type)
	setProperty("datacontenttype", delivery.ContentType)
	setProperty("f1correlationid", delivery.CorrelationId)
	if !delivery.Timestamp.IsZero() {
		setProperty("time", delivery.Timestamp.Format(time.RFC3339Nano))
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
