package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	amqp "github.com/rabbitmq/amqp091-go"
)

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
	pending     chan driver.InboundMessage
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
		deliveries, err := channel.ConsumeWithContext(context.Background(), destination, tag, false, cfg.Exclusive, false, false, nil)
		if err != nil {
			_ = channel.Close()
			c.closeLanes()
			return nil, classifyAMQP("consumer", driver.KindNotFound, err)
		}
		lane := &lane{
			owner:       c,
			destination: destination,
			channel:     channel,
			tag:         tag,
			prefetch:    prefetch,
			deliveries:  deliveries,
			pending:     make(chan driver.InboundMessage, prefetch),
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
		if c.stopped {
			c.mu.Unlock()
			continue
		}
		settler := &settler{owner: c, delivery: delivery}
		c.settlers[settler] = struct{}{}
		c.outstanding++
		c.mu.Unlock()
		lane.pending <- inboundMessage(lane.destination, delivery, settler)
	}
}

func (c *consumer) emitMessages(lane *lane) {
	defer c.forward.Done()
	for message := range lane.pending {
		for {
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
		select {
		case c.messages <- message:
		case <-c.stoppedC:
			return
		}
	}
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

func inboundMessage(destination string, delivery amqp.Delivery, settler *settler) driver.InboundMessage {
	headers := amqpHeaders(delivery.Headers)
	receivedAt := delivery.Timestamp
	if receivedAt.IsZero() {
		receivedAt = time.Now() //nolint:forbidigo // the port requires receipt time and drivers have no clock dependency
	}
	return driver.InboundMessage{
		Destination:   destination,
		Key:           headerValueByKey(headers, "cloudEvents:f1partitionkey"),
		Headers:       headers,
		Body:          append([]byte(nil), delivery.Body...),
		DeliveryCount: deliveryCount(delivery),
		ReceivedAt:    receivedAt,
		Ref:           driver.BrokerRef{Tag: delivery.DeliveryTag},
		Settle:        settler,
	}
}

func deliveryCount(delivery amqp.Delivery) int {
	value, ok := delivery.Headers["x-delivery-count"]
	if ok {
		switch count := value.(type) {
		case int:
			return count
		case int8:
			return int(count)
		case int16:
			return int(count)
		case int32:
			return int(count)
		case int64:
			return int(count)
		case uint:
			return int(count)
		case uint8:
			return int(count)
		case uint16:
			return int(count)
		case uint32:
			return int(count)
		case uint64:
			return int(count)
		}
	}
	if delivery.Redelivered {
		return 1
	}
	return 0
}

func amqpHeaders(values amqp.Table) []driver.Header {
	headers := make([]driver.Header, 0, len(values))
	for key, value := range values {
		headers = append(headers, driver.Header{Key: key, Value: headerValue(value)})
	}
	return headers
}

func headerValueByKey(headers []driver.Header, key string) []byte {
	for _, header := range headers {
		if header.Key == key {
			return append([]byte(nil), header.Value...)
		}
	}
	return nil
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
	for _, lane := range c.lanes {
		lane.mu.Lock()
		if lane.paused {
			lane.paused = false
			close(lane.resume)
			lane.resume = make(chan struct{})
		}
		lane.mu.Unlock()
	}
	lanes := append([]*lane(nil), c.lanes...)
	c.mu.Unlock()

	for _, lane := range lanes {
		if err := cancelConsumer(ctx, lane.channel, lane.tag); err != nil && !errors.Is(err, amqp.ErrClosed) {
			return classifyAMQP("drain", driver.KindTransient, err)
		}
	}
	if err := c.waitReaders(ctx, "drain"); err != nil {
		return err
	}
	return c.waitForwarders(ctx, "drain")
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
	return nil, classify("lag", driver.KindFatal, driver.ErrUnsupported)
}

func (c *consumer) release(settler *settler) {
	c.mu.Lock()
	delete(c.settlers, settler)
	if c.outstanding > 0 {
		c.outstanding--
	}
	c.mu.Unlock()
}
