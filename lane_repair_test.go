package f1

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

type laneRepairDriver struct {
	mu            sync.Mutex
	opens         int
	connections   []*laneRepairConn
	created       chan *laneRepairConsumer
	failNext      bool
	publishedConn *laneRepairConn
	pending       map[string][]*laneRepairConsumer
}

func (d *laneRepairDriver) Name() string { return "lane-repair-test" }

func (*laneRepairDriver) Capabilities() driver.Capabilities {
	return driver.Capabilities{PerMessageAck: true, NativeDeliveryCount: true}
}

func (d *laneRepairDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.opens++
	conn := &laneRepairConn{driver: d, admin: &reconnectTestAdmin{}}
	d.connections = append(d.connections, conn)
	return conn, nil
}

func (d *laneRepairDriver) openCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.opens
}

func (d *laneRepairDriver) failNextConsumer() {
	d.mu.Lock()
	d.failNext = true
	d.mu.Unlock()
}

func (d *laneRepairDriver) setPublishedConn(conn *laneRepairConn) {
	d.mu.Lock()
	d.publishedConn = conn
	d.mu.Unlock()
}

type laneRepairConn struct {
	driver   *laneRepairDriver
	admin    driver.Admin
	producer atomic.Int32
}

func (c *laneRepairConn) Capabilities() driver.Capabilities { return c.driver.Capabilities() }
func (*laneRepairConn) BrokerInfo() driver.BrokerInfo {
	return driver.BrokerInfo{Kind: "lane-repair-test", Version: "1"}
}

func (c *laneRepairConn) Producer(context.Context, driver.ProducerConfig) (driver.Producer, error) {
	c.producer.Add(1)
	return &laneRepairProducer{conn: c}, nil
}

func (c *laneRepairConn) Consumer(_ context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	c.driver.mu.Lock()
	if c.driver.failNext {
		c.driver.failNext = false
		c.driver.mu.Unlock()
		return nil, &driver.Error{Driver: c.driver.Name(), Op: "consumer", K: driver.KindTransient, Err: errors.New("replacement consumer failed")}
	}
	c.driver.mu.Unlock()
	consumer := &laneRepairConsumer{
		group:    cfg.Group,
		conn:     c,
		messages: make(chan driver.InboundMessage, 8),
		errors:   make(chan error, 8),
	}
	c.driver.created <- consumer
	return consumer, nil
}

func (c *laneRepairConn) Admin() driver.Admin       { return c.admin }
func (*laneRepairConn) Ping(context.Context) error  { return nil }
func (*laneRepairConn) Close(context.Context) error { return nil }

type laneRepairProducer struct {
	conn *laneRepairConn
}

func (p *laneRepairProducer) Publish(context.Context, ...driver.OutboundMessage) error {
	p.conn.driver.setPublishedConn(p.conn)
	return nil
}

func (p *laneRepairProducer) Flush(context.Context) error { return nil }

func (p *laneRepairProducer) Close(context.Context) error {
	p.conn.producer.Add(-1)
	return nil
}

type laneRepairConsumer struct {
	group    string
	conn     *laneRepairConn
	messages chan driver.InboundMessage
	errors   chan error
	released atomic.Bool
	once     sync.Once
}

func (c *laneRepairConsumer) Messages() <-chan driver.InboundMessage { return c.messages }
func (c *laneRepairConsumer) Errors() <-chan error                   { return c.errors }
func (*laneRepairConsumer) Pause(...string) error                    { return nil }
func (*laneRepairConsumer) Resume(...string) error                   { return nil }
func (*laneRepairConsumer) Drain(context.Context) error              { return nil }
func (c *laneRepairConsumer) Stop(context.Context) error {
	c.close()
	return nil
}

func (c *laneRepairConsumer) Release(context.Context) error {
	c.released.Store(true)
	c.close()
	return nil
}

func (*laneRepairConsumer) Lag(context.Context) (map[string]int64, error) {
	return nil, driver.ErrUnsupported
}

func (c *laneRepairConsumer) close() {
	c.once.Do(func() {
		close(c.messages)
		close(c.errors)
	})
}

func (c *laneRepairConsumer) sendError(err error)                { c.errors <- err }
func (c *laneRepairConsumer) send(message driver.InboundMessage) { c.messages <- message }

func waitLaneConsumer(t *testing.T, d *laneRepairDriver, group string) *laneRepairConsumer {
	t.Helper()
	d.mu.Lock()
	if pending := d.pending[group]; len(pending) > 0 {
		consumer := pending[0]
		d.pending[group] = pending[1:]
		d.mu.Unlock()
		return consumer
	}
	d.mu.Unlock()
	realClock := clock.NewReal()
	deadline := realClock.Timer(2 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case consumer := <-d.created:
			if consumer.group == group {
				return consumer
			}
			d.mu.Lock()
			if d.pending == nil {
				d.pending = make(map[string][]*laneRepairConsumer)
			}
			d.pending[consumer.group] = append(d.pending[consumer.group], consumer)
			d.mu.Unlock()
		case <-deadline.C:
			t.Fatalf("timed out waiting for consumer %q", group)
		}
	}
}

func laneRepairMessage(t *testing.T, id, eventType string) driver.InboundMessage {
	t.Helper()
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          id,
		Source:      "/test/orders",
		Type:        eventType,
		Priority:    PriorityNormal,
	}
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	return driver.InboundMessage{
		Destination: "f1.test." + eventType + ".normal",
		Headers:     headerSlice(headers),
		Settle:      &reconnectTestSettler{},
	}
}

func startLaneRunner(t *testing.T, client *Client, sub Subscription) (*Runner, <-chan error) {
	t.Helper()
	runner, err := client.Subscribe(context.Background(), sub)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		timer := clock.NewReal().Timer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			t.Errorf("runner %q did not stop", sub.Name)
		}
	})
	return runner, done
}

func laneRepairSubscription(name string, handled *atomic.Int32) Subscription {
	eventType := name + ".created"
	return Subscription{
		Name:           name,
		Topics:         []string{eventType},
		Concurrency:    1,
		Prefetch:       12,
		HandlerTimeout: time.Second,
		Handlers: map[string]Handler{
			eventType: HandlerFunc(func(context.Context, *Event) error {
				handled.Add(1)
				return nil
			}),
		},
	}
}

func newLaneRepairClient(t *testing.T, d *laneRepairDriver) *Client {
	t.Helper()
	client, err := New(context.Background(), testClientConfig(t), WithDriver(d))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client
}

func waitAtomicCount(t *testing.T, count *atomic.Int32, want int32) {
	t.Helper()
	realClock := clock.NewReal()
	deadline := realClock.Timer(2 * time.Second)
	defer deadline.Stop()
	ticker := realClock.Ticker(time.Millisecond)
	defer ticker.Stop()
	for {
		if count.Load() >= want {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("handled = %d, want at least %d", count.Load(), want)
		case <-ticker.C:
		}
	}
}

func transientLaneError(d *laneRepairDriver) error {
	return &driver.Error{Driver: d.Name(), Op: "consumer", K: driver.KindTransient, Err: errors.New("lane closed")}
}

func TestRunnerRepairsConsumerWithoutReplacingConnection(t *testing.T) {
	defaultOutput := captureProcessDefault(t)
	d := &laneRepairDriver{created: make(chan *laneRepairConsumer, 16)}
	client := newLaneRepairClient(t, d)
	var ordersHandled, billingHandled atomic.Int32
	orders, _ := startLaneRunner(t, client, laneRepairSubscription("orders", &ordersHandled))
	billing, _ := startLaneRunner(t, client, laneRepairSubscription("billing", &billingHandled))
	firstOrders := waitLaneConsumer(t, d, "orders")
	firstBilling := waitLaneConsumer(t, d, "billing")
	waitReconnectCondition(t, func() bool { return orders.lifecycle.Ready() && billing.lifecycle.Ready() })

	firstOrders.sendError(transientLaneError(d))
	repairedOrders := waitLaneConsumer(t, d, "orders")
	waitReconnectCondition(t, func() bool { return orders.lifecycle.Ready() && billing.lifecycle.Ready() })
	if d.openCount() != 1 {
		t.Fatalf("driver Open count = %d, want 1 during lane repair", d.openCount())
	}
	if !firstOrders.released.Load() {
		t.Fatal("old consumer was not released before replacement")
	}
	if repairedOrders == firstOrders {
		t.Fatal("lane repair reused the failed consumer")
	}
	repairedOrders.send(laneRepairMessage(t, "orders-after-repair", "orders.created"))
	firstBilling.send(laneRepairMessage(t, "billing-during-repair", "billing.created"))
	waitAtomicCount(t, &ordersHandled, 1)
	waitAtomicCount(t, &billingHandled, 1)
	if _, err := client.Publisher().Publish(context.Background(), "orders.created", map[string]string{"id": "published-during-repair"}); err != nil {
		t.Fatalf("Publish during lane repair = %v", err)
	}
	d.mu.Lock()
	publishedConn := d.publishedConn
	connections := append([]*laneRepairConn(nil), d.connections...)
	d.mu.Unlock()
	if !strings.Contains(defaultOutput.String(), "f1 consumer repaired") {
		t.Fatal("successful repair was not logged")
	}
	if publishedConn != connections[0] {
		t.Fatal("publish did not use the original connection")
	}
}

func TestRunnerEscalatesWhenConsumerRepairCannotOpen(t *testing.T) {
	d := &laneRepairDriver{created: make(chan *laneRepairConsumer, 16)}
	client := newLaneRepairClient(t, d)
	var handled atomic.Int32
	runner, _ := startLaneRunner(t, client, laneRepairSubscription("orders", &handled))
	first := waitLaneConsumer(t, d, "orders")
	waitReconnectCondition(t, runner.lifecycle.Ready)
	d.failNextConsumer()
	first.sendError(transientLaneError(d))
	second := waitLaneConsumer(t, d, "orders")
	if second == nil {
		t.Fatal("replacement consumer was not created after connection reconnect")
	}
	waitReconnectCondition(t, func() bool { return d.openCount() >= 2 })
	if d.openCount() < 2 {
		t.Fatalf("driver Open count = %d, want at least 2", d.openCount())
	}
}

func TestRunnerEscalatesAfterTwoFailedRepairCycles(t *testing.T) {
	d := &laneRepairDriver{created: make(chan *laneRepairConsumer, 16)}
	client := newLaneRepairClient(t, d)
	var handled atomic.Int32
	runner, _ := startLaneRunner(t, client, laneRepairSubscription("orders", &handled))
	first := waitLaneConsumer(t, d, "orders")
	waitReconnectCondition(t, runner.lifecycle.Ready)
	first.sendError(transientLaneError(d))
	second := waitLaneConsumer(t, d, "orders")
	second.sendError(transientLaneError(d))
	waitReconnectCondition(t, func() bool { return d.openCount() >= 2 })
	if d.openCount() < 2 {
		t.Fatalf("driver Open count = %d, want at least 2 after repeated repair failure", d.openCount())
	}
}

func TestRunnerUsefulDeliveryResetsRepairFailureCount(t *testing.T) {
	d := &laneRepairDriver{created: make(chan *laneRepairConsumer, 16)}
	client := newLaneRepairClient(t, d)
	var handled atomic.Int32
	runner, _ := startLaneRunner(t, client, laneRepairSubscription("orders", &handled))
	first := waitLaneConsumer(t, d, "orders")
	waitReconnectCondition(t, runner.lifecycle.Ready)
	first.sendError(transientLaneError(d))
	second := waitLaneConsumer(t, d, "orders")
	second.send(laneRepairMessage(t, "useful", "orders.created"))
	waitAtomicCount(t, &handled, 1)
	second.sendError(transientLaneError(d))
	third := waitLaneConsumer(t, d, "orders")
	if third == second {
		t.Fatal("repair after useful delivery reused the consumer")
	}
	if d.openCount() != 1 {
		t.Fatalf("driver Open count = %d, want 1 after useful repair cycle", d.openCount())
	}
}
