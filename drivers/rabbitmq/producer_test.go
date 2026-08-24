package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestProducerConfirmAndReturn(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-queue"
	conn, rawChannel := openProducerFixture(t, ctx, queue)
	defer func() { _ = rawChannel.Close() }()

	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	producer, err := conn.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	deliveries, err := rawChannel.Consume(queue, "producer-test", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	publishTime := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	if err := producer.Publish(ctx, driver.OutboundMessage{
		Destination: queue,
		Headers: []driver.Header{
			{Key: "id", Value: []byte("event-1")},
			{Key: "time", Value: []byte(publishTime.Format(time.RFC3339Nano))},
			{Key: "type", Value: []byte("orders.created")},
			{Key: "datacontenttype", Value: []byte("application/json")},
			{Key: "f1correlationid", Value: []byte("corr-1")},
			{Key: "f1partitionkey", Value: []byte("order-1")},
			{Key: "custom", Value: []byte("value")},
		},
		Body: []byte("payload"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case delivery := <-deliveries:
		if string(delivery.Body) != "payload" {
			t.Fatalf("Body = %q, want payload", delivery.Body)
		}
		if delivery.DeliveryMode != amqp.Persistent {
			t.Fatalf("DeliveryMode = %d, want persistent", delivery.DeliveryMode)
		}
		if delivery.MessageId != "event-1" || delivery.Type != "orders.created" || delivery.ContentType != "application/json" || delivery.CorrelationId != "corr-1" {
			t.Fatalf("AMQP properties = id %q type %q content type %q correlation %q", delivery.MessageId, delivery.Type, delivery.ContentType, delivery.CorrelationId)
		}
		if got := string(headerValue(delivery.Headers["cloudEvents:f1partitionkey"])); got != "order-1" {
			t.Fatalf("partition header = %q, want order-1", got)
		}
		if got := string(headerValue(delivery.Headers["cloudEvents:custom"])); got != "value" {
			t.Fatalf("custom header = %q, want value", got)
		}
		if err := delivery.Ack(false); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("receive: %v", ctx.Err())
	}

	err = producer.Publish(ctx, driver.OutboundMessage{Destination: queue + "-missing"})
	if err == nil {
		t.Fatal("missing Publish error = nil")
	}
	if !errors.Is(err, driver.ErrDestinationMissing) {
		t.Fatalf("missing Publish error = %v, want ErrDestinationMissing", err)
	}
	if err := producer.Close(ctx); err != nil {
		t.Fatalf("Producer.Close: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("Conn.Close: %v", err)
	}
}

func TestProducerAbandonedConfirmationRelay(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-confirm-relay"
	conn, _ := openProducerFixture(t, ctx, queue)
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	rawProducer, err := conn.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	p := rawProducer.(*producer)
	brokerConfirms := p.confirms
	controlledConfirms := make(chan amqp.Confirmation, 2)
	p.confirms = controlledConfirms
	t.Cleanup(func() { _ = rawProducer.Close(context.Background()) })

	aCtx, cancelA := context.WithCancel(ctx)
	aDone := make(chan error, 1)
	go func() {
		aDone <- rawProducer.Publish(aCtx, driver.OutboundMessage{Destination: queue, Body: []byte("message-a")})
	}()
	var aConfirmation amqp.Confirmation
	select {
	case aConfirmation = <-brokerConfirms:
	case <-ctx.Done():
		t.Fatalf("message A confirmation: %v", ctx.Err())
	}
	cancelA()
	select {
	case err := <-aDone:
		if err == nil {
			t.Fatal("message A Publish succeeded after its context was canceled")
		}
		kind, classified := driver.Classify(err)
		if !classified || kind != driver.KindTransient {
			t.Fatalf("message A error = %v, kind=%v classified=%t; want transient", err, kind, classified)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("message A error = %v; want context.Canceled", err)
		}
	case <-ctx.Done():
		t.Fatalf("message A result: %v", ctx.Err())
	}

	bCtx, cancelB := context.WithTimeout(ctx, 5*time.Second)
	defer cancelB()
	bDone := make(chan error, 1)
	go func() {
		bDone <- rawProducer.Publish(bCtx, driver.OutboundMessage{Destination: queue, Body: []byte("message-b")})
	}()
	timer := time.NewTimer(2 * time.Second) //nolint:forbidigo // bounded live-broker relay guard
	select {
	case err := <-bDone:
		if err == nil {
			t.Fatal("message B Publish succeeded through a producer with an abandoned confirmation")
		}
		kind, classified := driver.Classify(err)
		if !classified || kind != driver.KindTransient {
			t.Fatalf("message B error = %v, kind=%v classified=%t; want transient", err, kind, classified)
		}
		if !errors.Is(err, amqp.ErrClosed) {
			t.Fatalf("message B error = %v; want amqp.ErrClosed", err)
		}
	case bConfirmation, ok := <-brokerConfirms:
		if !ok {
			select {
			case err := <-bDone:
				if err == nil {
					t.Fatal("message B succeeded after the confirmation stream closed")
				}
				if !errors.Is(err, amqp.ErrClosed) {
					t.Fatalf("message B error after confirmation stream closed = %v; want amqp.ErrClosed", err)
				}
			case <-ctx.Done():
				t.Fatalf("message B result after confirmation stream closed: %v", ctx.Err())
			}
			break
		}
		if aConfirmation.DeliveryTag == bConfirmation.DeliveryTag {
			t.Fatalf("confirmation tags are equal: A=%d B=%d", aConfirmation.DeliveryTag, bConfirmation.DeliveryTag)
		}
		controlledConfirms <- aConfirmation
		select {
		case err := <-bDone:
			if err == nil {
				t.Fatalf("message B succeeded from message A confirmation: A tag=%d B tag=%d", aConfirmation.DeliveryTag, bConfirmation.DeliveryTag)
			}
			t.Fatalf("message B reached the broker after producer invalidation: %v", err)
		case <-ctx.Done():
			t.Fatalf("message B result after confirmation relay: %v", ctx.Err())
		}
	case <-timer.C:
		t.Fatal("message B did not fail after producer invalidation")
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	select {
	case confirmation, ok := <-brokerConfirms:
		if ok {
			t.Fatalf("stale confirmation remained available after message B was rejected: tag=%d", confirmation.DeliveryTag)
		}
	case <-time.After(500 * time.Millisecond): //nolint:forbidigo // bounded live-broker confirmation guard
	}
	if err := rawProducer.Close(ctx); err != nil {
		t.Fatalf("first poisoned Producer.Close: %v", err)
	}
	if err := rawProducer.Close(ctx); err != nil {
		t.Fatalf("second poisoned Producer.Close: %v", err)
	}
}

func TestProducerConfirmedSequence(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-confirmed-sequence"
	conn, _ := openProducerFixture(t, ctx, queue)
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	producer, err := conn.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	if err := producer.Publish(ctx,
		driver.OutboundMessage{Destination: queue, Body: []byte("first")},
		driver.OutboundMessage{Destination: queue, Body: []byte("second")},
	); err != nil {
		t.Fatalf("confirmed sequence Publish: %v", err)
	}
	if err := producer.Close(ctx); err != nil {
		t.Fatalf("Producer.Close: %v", err)
	}
}

func TestProducerPublishesToDeclaredFanoutExchange(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const exchange = "rabbitmq-driver-producer-exchange"
	const queueA = "rabbitmq-driver-producer-fanout-a"
	const queueB = "rabbitmq-driver-producer-fanout-b"
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })
	_ = rawChannel.ExchangeDelete(exchange, false, false)
	for _, queue := range []string{queueA, queueB} {
		_, _ = rawChannel.QueueDelete(queue, false, false, false)
	}
	t.Cleanup(func() {
		_ = rawChannel.ExchangeDelete(exchange, false, false)
		for _, queue := range []string{queueA, queueB} {
			_, _ = rawChannel.QueueDelete(queue, false, false, false)
		}
	})

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	spec := driver.TopologySpec{
		Exchanges: []driver.ExchangeSpec{{Name: exchange, Kind: "fanout", Durable: true}},
		Destinations: []driver.DestinationSpec{
			{Name: queueA, Durable: true},
			{Name: queueB, Durable: true},
		},
		Bindings: []driver.BindingSpec{
			{Source: exchange, Destination: queueA},
			{Source: exchange, Destination: queueB},
		},
	}
	if _, err := conn.Admin().EnsureTopology(ctx, spec); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	producer, err := conn.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	consume := func(queue, tag string) <-chan amqp.Delivery {
		t.Helper()
		messages, consumeErr := rawChannel.Consume(queue, tag, false, false, false, false, nil)
		if consumeErr != nil {
			t.Fatalf("Consume(%q): %v", queue, consumeErr)
		}
		return messages
	}
	deliveriesA := consume(queueA, "producer-fanout-a")
	deliveriesB := consume(queueB, "producer-fanout-b")
	if err := producer.Publish(ctx, driver.OutboundMessage{Destination: exchange, EntryPoint: true, Body: []byte("fanout")}); err != nil {
		t.Fatalf("Publish exchange: %v", err)
	}
	for name, deliveries := range map[string]<-chan amqp.Delivery{"a": deliveriesA, "b": deliveriesB} {
		select {
		case delivery := <-deliveries:
			if string(delivery.Body) != "fanout" {
				t.Fatalf("queue %s body = %q, want fanout", name, delivery.Body)
			}
			if err := delivery.Ack(false); err != nil {
				t.Fatalf("queue %s Ack: %v", name, err)
			}
		case <-ctx.Done():
			t.Fatalf("queue %s receive: %v", name, ctx.Err())
		}
	}
	if err := producer.Close(ctx); err != nil {
		t.Fatalf("Producer.Close: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("Conn.Close: %v", err)
	}
}

func openProducerFixture(t *testing.T, ctx context.Context, queue string) (driver.Conn, *amqp.Channel) {
	t.Helper()
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	channel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	_, _ = channel.QueueDelete(queue, false, false, false)
	t.Cleanup(func() {
		_, _ = channel.QueueDelete(queue, false, false, false)
		_ = channel.Close()
	})
	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	return conn, channel
}

// TestProducerTargetRoutesEntryPointToExchange proves target() routes an
// entry-point publish to its named exchange with no routing key, purely from
// OutboundMessage.EntryPoint, and offline: no live broker connection. The
// table runs once per topology policy to document that this is now true
// regardless of policy - the defect this replaces was that a pre-provisioned
// exchange only routed correctly once EnsureTopology had populated a
// process-local cache, so it broke under exactly the policy where a
// pre-provisioned exchange is the whole point (no admin call is ever made).
// target() takes no policy argument any more; the loop asserts the same
// answer under all three names so a future reintroduction of policy-derived
// routing would have to change this test to pass.
func TestProducerTargetRoutesEntryPointToExchange(t *testing.T) {
	policies := map[string]driver.TopologyPolicy{
		"TopologyNone":    driver.TopologyNone,
		"TopologyDeclare": driver.TopologyDeclare,
		"TopologyVerify":  driver.TopologyVerify,
	}
	for name, policy := range policies {
		t.Run(name, func(t *testing.T) {
			t.Logf("target() takes no policy argument; asserting under policy %d (%s) for documentation", policy, name)
			p := &producer{conn: &conn{}}
			exchange, routingKey, expiration := p.target(driver.OutboundMessage{Destination: "orders.fanout", EntryPoint: true})
			if exchange != "orders.fanout" {
				t.Fatalf("target() exchange = %q, want %q", exchange, "orders.fanout")
			}
			if routingKey != "" {
				t.Fatalf("target() routingKey = %q, want empty for a fanout exchange", routingKey)
			}
			if expiration != "" {
				t.Fatalf("target() expiration = %q, want empty for a non-deferred publish", expiration)
			}
		})
	}
}

// TestProducerTargetRetryAndDLQStayConcreteDestinations proves the fix does
// not pass by routing everything to an exchange: a retry or DLQ publish
// carries EntryPoint: false, its zero value, and must still route to the
// AMQP default exchange with the destination as routing key.
func TestProducerTargetRetryAndDLQStayConcreteDestinations(t *testing.T) {
	cases := []string{
		"f1.prod.orders.created.worker.high.retry.1",
		"f1.prod.orders.created.dlq.worker",
	}
	p := &producer{conn: &conn{}}
	for _, destination := range cases {
		t.Run(destination, func(t *testing.T) {
			exchange, routingKey, expiration := p.target(driver.OutboundMessage{Destination: destination})
			if exchange != "" {
				t.Fatalf("target() exchange = %q, want empty for a concrete destination", exchange)
			}
			if routingKey != destination {
				t.Fatalf("target() routingKey = %q, want %q", routingKey, destination)
			}
			if expiration != "" {
				t.Fatalf("target() expiration = %q, want empty for a non-deferred publish", expiration)
			}
		})
	}
}

// TestProducerTargetDeferredDestinationParks proves an explicit DelayUntil
// still parks, with no ensureParking call from the publish path: parking
// queues are declared once in EnsureTopology, and the publish path only
// ever asks for one that should already exist.
func TestProducerTargetDeferredDestinationParks(t *testing.T) {
	p := &producer{conn: &conn{}}
	due := time.Now().Add(time.Hour) //nolint:forbidigo // building a fixed future DelayUntil for the test
	exchange, routingKey, expiration := p.target(driver.OutboundMessage{Destination: "orders.deferred", DelayUntil: due})
	if exchange != "" {
		t.Fatalf("target() exchange = %q, want empty for a parked publish", exchange)
	}
	if routingKey != "orders.deferred.park" {
		t.Fatalf("target() routingKey = %q, want %q", routingKey, "orders.deferred.park")
	}
	if expiration == "" {
		t.Fatal("target() expiration is empty, want a positive TTL for a future DelayUntil")
	}
}

// TestProducerTargetDestinationDelayFallsBackWhenDelayUntilIsZero proves a
// declared destination-level delay still supplies the due time when the
// message carries no DelayUntil. The driver port conformance suite requires
// this ("a destination-level Delay supplying the due time when DelayUntil
// is zero"), so target() keeps a read of the delay EnsureTopology already
// recorded for this one purpose, even though the exchange-routing cache
// read next to it was removed.
func TestProducerTargetDestinationDelayFallsBackWhenDelayUntilIsZero(t *testing.T) {
	p := &producer{conn: &conn{deferred: map[string]time.Duration{"orders.deferred": time.Hour}}}
	exchange, routingKey, expiration := p.target(driver.OutboundMessage{Destination: "orders.deferred"})
	if exchange != "" {
		t.Fatalf("target() exchange = %q, want empty for a parked publish", exchange)
	}
	if routingKey != "orders.deferred.park" {
		t.Fatalf("target() routingKey = %q, want %q", routingKey, "orders.deferred.park")
	}
	if expiration == "" {
		t.Fatal("target() expiration is empty, want a positive TTL derived from the destination's declared delay")
	}
}
