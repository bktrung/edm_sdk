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

	mu          sync.Mutex
	draining    bool
	stopped     bool
	outstanding int
	settlers    map[*settler]struct{}

	readers sync.WaitGroup
	forward sync.WaitGroup
	events  sync.WaitGroup
}

type lane struct {
	owner       *consumer
	destination string
	channel     *amqp.Channel
	tag         string
	prefetch    int
	deliveries  <-chan amqp.Delivery
	pending     chan amqp.Delivery
	resume      chan struct{}

	mu     sync.Mutex
	paused bool
}

var _ driver.Consumer = (*consumer)(nil)

func newConsumer(conn *conn, cfg driver.ConsumerConfig) (*consumer, error) {
	c := &consumer{
		conn:     conn,
		cfg:      cfg,
		byName:   make(map[string]*lane, len(cfg.Destinations)),
		messages: make(chan driver.InboundMessage, totalPrefetch(cfg)),
		errors:   make(chan error, 8),
		stoppedC: make(chan struct{}),
		settlers: make(map[*settler]struct{}),
	}
	for index, destination := range cfg.Destinations {
		if _, exists := c.byName[destination]; exists {
			return nil, classify("consumer", driver.KindFatal, fmt.Errorf("duplicate destination %q", destination))
		}
		prefetch := destinationPrefetch(cfg, destination, index)
		channel, err := conn.amqp.Channel()
		if err != nil {
			c.closeLanes()
			return nil, classifyAMQP("consumer", driver.KindTransient, err)
		}
		if err := channel.Qos(prefetch, 0, false); err != nil {
			_ = channel.Close()
			c.closeLanes()
			return nil, classifyAMQP("consumer", driver.KindFatal, err)
		}
		tag := "f1-consumer-" + strconv.FormatUint(consumerSequence.Add(1), 10)
		deliveries, err := channel.Consume(destination, tag, false, cfg.Exclusive, false, false, nil)
		if err != nil {
			_ = channel.Close()
			c.closeLanes()
			if cfg.Exclusive && isPermission(err) {
				return nil, classify("consumer", driver.KindFatal, fmt.Errorf("exclusive consumer refused: %w", err))
			}
			return nil, classifyAMQP("consumer", driver.KindNotFound, err)
		}
		lane := &lane{
			owner:       c,
			destination: destination,
			channel:     channel,
			tag:         tag,
			prefetch:    prefetch,
			deliveries:  deliveries,
			pending:     make(chan amqp.Delivery, prefetch),
			resume:      make(chan struct{}),
		}
		c.lanes = append(c.lanes, lane)
		c.byName[destination] = lane
		c.readers.Add(1)
		c.forward.Add(1)
		c.events.Add(1)
		go c.readDeliveries(lane)
		go c.emitMessages(lane)
		go c.watchClose(lane)
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

func (c *consumer) Messages() <-chan driver.InboundMessage { return c.messages }
func (c *consumer) Errors() <-chan error                   { return c.errors }

func (c *consumer) readDeliveries(lane *lane) {
	defer c.readers.Done()
	defer close(lane.pending)
	for delivery := range lane.deliveries {
		c.mu.Lock()
		stopped, draining := c.stopped, c.draining
		c.mu.Unlock()
		if stopped || draining {
			continue
		}
		select {
		case lane.pending <- delivery:
		case <-c.stoppedC:
			return
		}
	}
}

func (c *consumer) emitMessages(lane *lane) {
	defer c.forward.Done()
	for delivery := range lane.pending {
		c.mu.Lock()
		if c.draining || c.stopped {
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
		for {
			c.mu.Lock()
			if c.draining || c.stopped {
				c.mu.Unlock()
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
			case <-c.stoppedC:
				return
			}
		}
		settler := &settler{owner: c, destination: lane.destination, delivery: delivery}
		c.mu.Lock()
		if c.draining || c.stopped {
			c.mu.Unlock()
			return
		}
		c.settlers[settler] = struct{}{}
		c.outstanding++
		c.mu.Unlock()
		message := inboundMessage(lane.destination, delivery, settler, c.nativeDeliveryCount())
		select {
		case c.messages <- message:
		case <-c.stoppedC:
			return
		}
	}
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
		return []byte(fmt.Sprint(value))
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
	lanes := append([]*lane(nil), c.lanes...)
	c.mu.Unlock()

	for _, lane := range lanes {
		if err := cancelConsumer(ctx, lane.channel, lane.tag); err != nil && !errors.Is(err, amqp.ErrClosed) {
			return classifyAMQP("drain", driver.KindTransient, err)
		}
	}
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

func cancelConsumer(ctx context.Context, channel *amqp.Channel, tag string) error {
	result := make(chan error, 1)
	go func() { result <- channel.Cancel(tag, false) }()
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
	if err := c.Drain(ctx); err != nil {
		return err
	}
	if err := c.waitReaders(ctx, "stop"); err != nil {
		return err
	}
	if err := c.waitForwarders(ctx, "stop"); err != nil {
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
	close(c.stoppedC)
	c.mu.Unlock()
	for _, lane := range c.lanes {
		_ = lane.channel.Close()
	}
	c.events.Wait()
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
	admin := &admin{conn: c.conn}
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
