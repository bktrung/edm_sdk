//go:build integration

package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
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

// TestProducerBatchPublishesWindowBeforeReadingConfirmation proves a batch is
// pipelined instead of confirmed one message at a time. The confirmation
// channel is handed to the test, so no message can be confirmed while the
// window is observed: a driver that published one message and waited for its
// confirmation could never get a second message onto the broker. It counts
// publishes in flight, not wall-clock time.
func TestProducerBatchPublishesWindowBeforeReadingConfirmation(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-batch-window"
	conn, rawChannel := openProducerFixture(t, ctx, queue)
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
	confirms := make(chan amqp.Confirmation, publishWindowSize)
	p.confirms = confirms
	t.Cleanup(func() { _ = rawProducer.Close(context.Background()) })
	deliveries, err := rawChannel.Consume(queue, "producer-batch-window", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	// One window, and every message carries the unique envelope id the core
	// stamps: a window holds no repeated id, so it is published in one go.
	messages := make([]driver.OutboundMessage, publishWindowSize)
	for index := range messages {
		messages[index] = driver.OutboundMessage{
			Destination: queue,
			Headers:     []driver.Header{{Key: "id", Value: fmt.Appendf(nil, "window-%d", index)}},
			Body:        fmt.Appendf(nil, "window-body-%d", index),
		}
	}
	done := make(chan error, 1)
	go func() { done <- rawProducer.Publish(ctx, messages...) }()

	// Two publishes in flight is the property that makes a batch cost less
	// than one confirm round trip per message: with a single publish
	// outstanding, the second message cannot reach the broker until the first
	// one is confirmed.
	awaitUnconfirmedDeliveries(t, ctx, deliveries, 2)

	for index := range messages {
		select {
		case confirms <- amqp.Confirmation{DeliveryTag: uint64(index + 1), Ack: true}:
		case <-ctx.Done():
			t.Fatalf("confirming the batch: %v", ctx.Err())
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("batch Publish: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("batch Publish result: %v", ctx.Err())
	}
}

// TestProducerBatchReturnFailsItsOwnIndex proves the one thing a window can get
// wrong: a broker return must fail the message it belongs to and no other.
// The returns channel is fed by the test so the interleaving is fixed - the
// return for the middle message is already queued when the first confirmation
// is read - which is exactly the interleaving that fails the first index when
// returns are matched by arrival order instead of by MessageId.
func TestProducerBatchReturnFailsItsOwnIndex(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-batch-return"
	conn, rawChannel := openProducerFixture(t, ctx, queue)
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
	confirms := make(chan amqp.Confirmation, 3)
	p.confirms = confirms
	returns := make(chan amqp.Return, 3)
	p.returns = returns
	t.Cleanup(func() { _ = rawProducer.Close(context.Background()) })
	deliveries, err := rawChannel.Consume(queue, "producer-batch-return", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	messages := make([]driver.OutboundMessage, 3)
	for index := range messages {
		messages[index] = driver.OutboundMessage{
			Destination: queue,
			Headers:     []driver.Header{{Key: "id", Value: fmt.Appendf(nil, "return-%d", index)}},
			Body:        fmt.Appendf(nil, "return-body-%d", index),
		}
	}
	done := make(chan error, 1)
	go func() { done <- rawProducer.Publish(ctx, messages...) }()
	awaitUnconfirmedDeliveries(t, ctx, deliveries, len(messages))

	select {
	case returns <- amqp.Return{ReplyCode: 312, ReplyText: "NO_ROUTE", MessageId: "return-1"}:
	case <-ctx.Done():
		t.Fatalf("returning the middle message: %v", ctx.Err())
	}
	for index := range messages {
		select {
		case confirms <- amqp.Confirmation{DeliveryTag: uint64(index + 1), Ack: true}:
		case <-ctx.Done():
			t.Fatalf("confirming the batch: %v", ctx.Err())
		}
	}

	var publishErr *driver.PublishError
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("batch Publish error = nil, want the returned message to fail")
		}
		if !errors.As(err, &publishErr) {
			t.Fatalf("batch Publish error = %v, want *driver.PublishError", err)
		}
	case <-ctx.Done():
		t.Fatalf("batch Publish result: %v", ctx.Err())
	}
	if len(publishErr.Failed) != 1 {
		t.Fatalf("failed indexes = %v, want only the returned message at index 1", publishErr.Failed)
	}
	failure, ok := publishErr.Failed[1]
	if !ok {
		t.Fatalf("failed indexes = %v, want index 1", publishErr.Failed)
	}
	if !errors.Is(failure, driver.ErrDestinationMissing) {
		t.Fatalf("failed[1] = %v, want ErrDestinationMissing", failure)
	}
	if kind, classified := driver.Classify(failure); !classified || kind != driver.KindNotFound {
		t.Fatalf("failed[1] classification = (%v, %t), want (not found, true)", kind, classified)
	}
}

// TestProducerBatchReturnFromBrokerFailsOnlyItsOwnIndex is the same property
// against a real broker return: one unroutable message in the middle of a
// batch is reported against its own index and classified not-found, and its
// neighbours are published in order.
func TestProducerBatchReturnFromBrokerFailsOnlyItsOwnIndex(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-batch-partial"
	const missing = "rabbitmq-driver-producer-batch-partial-missing"
	conn, rawChannel := openProducerFixture(t, ctx, queue)
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	rawProducer, err := conn.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = rawProducer.Close(context.Background()) })
	deliveries, err := rawChannel.Consume(queue, "producer-batch-partial", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	body := func(index int, value string) driver.OutboundMessage {
		return driver.OutboundMessage{
			Destination: queue,
			Headers:     []driver.Header{{Key: "id", Value: fmt.Appendf(nil, "partial-%d", index)}},
			Body:        []byte(value),
		}
	}
	err = rawProducer.Publish(ctx,
		body(0, "first"),
		driver.OutboundMessage{
			Destination: missing,
			Headers:     []driver.Header{{Key: "id", Value: []byte("partial-1")}},
		},
		body(2, "third"),
	)
	var publishErr *driver.PublishError
	if !errors.As(err, &publishErr) {
		t.Fatalf("batch Publish error = %v, want *driver.PublishError", err)
	}
	if len(publishErr.Failed) != 1 {
		t.Fatalf("failed indexes = %v, want only the unroutable message at index 1", publishErr.Failed)
	}
	failure, ok := publishErr.Failed[1]
	if !ok {
		t.Fatalf("failed indexes = %v, want index 1", publishErr.Failed)
	}
	if !errors.Is(failure, driver.ErrDestinationMissing) {
		t.Fatalf("failed[1] = %v, want ErrDestinationMissing", failure)
	}
	if kind, classified := driver.Classify(failure); !classified || kind != driver.KindNotFound {
		t.Fatalf("failed[1] classification = (%v, %t), want (not found, true)", kind, classified)
	}
	for _, want := range []string{"first", "third"} {
		select {
		case delivery := <-deliveries:
			if string(delivery.Body) != want {
				t.Fatalf("delivered body = %q, want %q", delivery.Body, want)
			}
			if err := delivery.Ack(false); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("receiving the published neighbours: %v", ctx.Err())
		}
	}
}

// TestProducerNegativeConfirmFailsItsOwnIndex proves a negative confirmation
// fails only the message it confirms: the two acked neighbours of a nacked
// message in the middle of a window are published and reported successful.
func TestProducerNegativeConfirmFailsItsOwnIndex(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-batch-negative"
	conn, rawChannel := openProducerFixture(t, ctx, queue)
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
	confirms := make(chan amqp.Confirmation, 3)
	p.confirms = confirms
	t.Cleanup(func() { _ = rawProducer.Close(context.Background()) })
	deliveries, err := rawChannel.Consume(queue, "producer-batch-negative", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	messages := make([]driver.OutboundMessage, 3)
	for index := range messages {
		messages[index] = driver.OutboundMessage{
			Destination: queue,
			Headers:     []driver.Header{{Key: "id", Value: fmt.Appendf(nil, "negative-%d", index)}},
			Body:        fmt.Appendf(nil, "negative-body-%d", index),
		}
	}
	done := make(chan error, 1)
	go func() { done <- rawProducer.Publish(ctx, messages...) }()
	awaitUnconfirmedDeliveries(t, ctx, deliveries, len(messages))

	for index := range messages {
		select {
		case confirms <- amqp.Confirmation{DeliveryTag: uint64(index + 1), Ack: index != 1}:
		case <-ctx.Done():
			t.Fatalf("confirming the batch: %v", ctx.Err())
		}
	}
	var publishErr *driver.PublishError
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("batch Publish error = nil, want the negatively confirmed message to fail")
		}
		if !errors.As(err, &publishErr) {
			t.Fatalf("batch Publish error = %v, want *driver.PublishError", err)
		}
	case <-ctx.Done():
		t.Fatalf("batch Publish result: %v", ctx.Err())
	}
	if len(publishErr.Failed) != 1 {
		t.Fatalf("failed indexes = %v, want only the negatively confirmed message at index 1", publishErr.Failed)
	}
	failure, ok := publishErr.Failed[1]
	if !ok {
		t.Fatalf("failed indexes = %v, want index 1", publishErr.Failed)
	}
	if kind, classified := driver.Classify(failure); !classified || kind != driver.KindTransient {
		t.Fatalf("failed[1] classification = (%v, %t), want (transient, true)", kind, classified)
	}
}

// TestProducerConfirmsArriveInPublishOrder asserts the property the window
// rests on rather than assuming it: the client re-sequences confirmations
// before delivering them, so the i-th confirmation read from the channel is
// the i-th message published. It registers a second listener on the same
// channel, because Publish consumes its own confirmations.
func TestProducerConfirmsArriveInPublishOrder(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-confirm-order"
	const batch = 8
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
	t.Cleanup(func() { _ = rawProducer.Close(context.Background()) })
	p := rawProducer.(*producer)
	captured := p.channel.NotifyPublish(make(chan amqp.Confirmation, batch))
	messages := make([]driver.OutboundMessage, batch)
	for index := range messages {
		messages[index] = driver.OutboundMessage{
			Destination: queue,
			Headers:     []driver.Header{{Key: "id", Value: fmt.Appendf(nil, "order-%d", index)}},
			Body:        fmt.Appendf(nil, "order-body-%d", index),
		}
	}
	if err := rawProducer.Publish(ctx, messages...); err != nil {
		t.Fatalf("batch Publish: %v", err)
	}
	for index := range batch {
		select {
		case confirmation := <-captured:
			if !confirmation.Ack {
				t.Fatalf("confirmation %d is negative", index)
			}
			if confirmation.DeliveryTag != uint64(index+1) {
				t.Fatalf("confirmation %d has delivery tag %d, want %d: confirmations are not delivered in publish order", index, confirmation.DeliveryTag, index+1)
			}
		case <-ctx.Done():
			t.Fatalf("reading confirmation %d: %v", index, ctx.Err())
		}
	}
}

// TestProducerCancelledWindowInvalidatesChannel proves a cancelled batch
// leaves nothing behind: every message the window published is reported
// failed, the channel is invalidated so its unread confirmations can never be
// read against a later publish, and a fresh producer on the same connection
// publishes normally.
func TestProducerCancelledWindowInvalidatesChannel(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-cancelled-window"
	conn, rawChannel := openProducerFixture(t, ctx, queue)
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
	confirms := make(chan amqp.Confirmation, publishWindowSize)
	p.confirms = confirms
	t.Cleanup(func() { _ = rawProducer.Close(context.Background()) })
	deliveries, err := rawChannel.Consume(queue, "producer-cancelled-window", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	messages := make([]driver.OutboundMessage, 4)
	for index := range messages {
		messages[index] = driver.OutboundMessage{
			Destination: queue,
			Headers:     []driver.Header{{Key: "id", Value: fmt.Appendf(nil, "cancelled-%d", index)}},
			Body:        fmt.Appendf(nil, "cancelled-body-%d", index),
		}
	}
	publishCtx, cancelPublish := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- rawProducer.Publish(publishCtx, messages...) }()
	awaitUnconfirmedDeliveries(t, ctx, deliveries, len(messages))
	cancelPublish()

	var publishErr *driver.PublishError
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled batch Publish error = nil, want the cancelled window to fail")
		}
		if !errors.As(err, &publishErr) {
			t.Fatalf("cancelled batch Publish error = %v, want *driver.PublishError", err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled batch Publish error = %v, want context.Canceled", err)
		}
	case <-ctx.Done():
		t.Fatalf("cancelled batch Publish result: %v", ctx.Err())
	}
	if len(publishErr.Failed) != len(messages) {
		t.Fatalf("failed indexes = %v, want every index of the cancelled window", publishErr.Failed)
	}
	// The confirmations owed to the cancelled window must never be read
	// against a later publish, so the channel is gone and no further publish
	// is attempted on it.
	if err := rawProducer.Publish(ctx, driver.OutboundMessage{Destination: queue}); !errors.Is(err, amqp.ErrClosed) {
		t.Fatalf("Publish after a cancelled window = %v, want amqp.ErrClosed", err)
	}

	fresh, err := conn.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	defer func() { _ = fresh.Close(ctx) }()
	if err := fresh.Publish(ctx, driver.OutboundMessage{
		Destination: queue,
		Headers:     []driver.Header{{Key: "id", Value: []byte("after-cancel")}},
		Body:        []byte("after-cancel"),
	}); err != nil {
		t.Fatalf("Publish on a fresh producer: %v", err)
	}
	select {
	case delivery := <-deliveries:
		if string(delivery.Body) != "after-cancel" {
			t.Fatalf("delivered body = %q, want after-cancel", delivery.Body)
		}
		if err := delivery.Ack(false); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("receiving the fresh producer's message: %v", ctx.Err())
	}
}

// TestProducerBatchWithoutMessageIDsFailsOnlyItsOwnIndex covers correlation
// when no envelope id is available: amqpPublishing only sets the AMQP message
// id from an "id" header, and the driver port is publishable without one. A
// basic.return carries no delivery tag, so an id-less window must never hold
// two messages: the driver shortens such a window to one, which costs one round
// trip per message and keeps every return on its own index. The batch is
// repeated because the interleaving that would break a wrongly pipelined
// id-less batch (a return for a later message arriving before an earlier
// message's confirmation) is a race between two broker frames.
func TestProducerBatchWithoutMessageIDsFailsOnlyItsOwnIndex(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-batch-no-id"
	const missing = "rabbitmq-driver-producer-batch-no-id-missing"
	conn, rawChannel := openProducerFixture(t, ctx, queue)
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	rawProducer, err := conn.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = rawProducer.Close(context.Background()) })
	deliveries, err := rawChannel.Consume(queue, "producer-batch-no-id", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	for attempt := range 5 {
		messages := make([]driver.OutboundMessage, 6)
		for index := range messages {
			messages[index] = driver.OutboundMessage{Destination: queue, Body: fmt.Appendf(nil, "no-id-%d-%d", attempt, index)}
			if index%2 == 1 {
				messages[index].Destination = missing
			}
		}
		err := rawProducer.Publish(ctx, messages...)
		var publishErr *driver.PublishError
		if !errors.As(err, &publishErr) {
			t.Fatalf("attempt %d: batch Publish error = %v, want *driver.PublishError", attempt, err)
		}
		if len(publishErr.Failed) != len(messages)/2 {
			t.Fatalf("attempt %d: failed indexes = %v, want the odd indexes only", attempt, publishErr.Failed)
		}
		for index := range messages {
			if index%2 == 0 {
				continue
			}
			failure, ok := publishErr.Failed[index]
			if !ok {
				t.Fatalf("attempt %d: failed indexes = %v, want index %d", attempt, publishErr.Failed, index)
			}
			if !errors.Is(failure, driver.ErrDestinationMissing) {
				t.Fatalf("attempt %d: failed[%d] = %v, want ErrDestinationMissing", attempt, index, failure)
			}
			if kind, classified := driver.Classify(failure); !classified || kind != driver.KindNotFound {
				t.Fatalf("attempt %d: failed[%d] classification = (%v, %t), want (not found, true)", attempt, index, kind, classified)
			}
		}
		for index := range messages {
			if index%2 == 1 {
				continue
			}
			select {
			case delivery := <-deliveries:
				if want := fmt.Appendf(nil, "no-id-%d-%d", attempt, index); string(delivery.Body) != string(want) {
					t.Fatalf("attempt %d: delivered body = %q, want %q", attempt, delivery.Body, want)
				}
				if err := delivery.Ack(false); err != nil {
					t.Fatalf("Ack: %v", err)
				}
			case <-ctx.Done():
				t.Fatalf("attempt %d: receiving the published neighbours: %v", attempt, ctx.Err())
			}
		}
	}
}

// TestProducerBatchLocalEncodingFailureDoesNotStallWindow covers a window whose
// messages on the wire are fewer than the batch: a message that fails to encode
// is never published, so it consumes no delivery tag and no confirmation for it
// will ever arrive. The reader must therefore wait for one confirmation per
// successful publish, not one per message in the batch. Getting that wrong
// blocks Publish until its context expires.
func TestProducerBatchLocalEncodingFailureDoesNotStallWindow(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-batch-encode"
	conn, rawChannel := openProducerFixture(t, ctx, queue)
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	rawProducer, err := conn.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = rawProducer.Close(context.Background()) })
	deliveries, err := rawChannel.Consume(queue, "producer-batch-encode", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	messages := []driver.OutboundMessage{
		{
			Destination: queue,
			Headers:     []driver.Header{{Key: "id", Value: []byte("encode-0")}},
			Body:        []byte("encoded-first"),
		},
		{
			Destination: queue,
			Headers: []driver.Header{
				{Key: "id", Value: []byte("encode-1")},
				{Key: "time", Value: []byte("not-a-timestamp")},
			},
		},
		{
			Destination: queue,
			Headers:     []driver.Header{{Key: "id", Value: []byte("encode-2")}},
			Body:        []byte("encoded-third"),
		},
	}
	done := make(chan error, 1)
	go func() { done <- rawProducer.Publish(ctx, messages...) }()
	for _, want := range []string{"encoded-first", "encoded-third"} {
		select {
		case delivery := <-deliveries:
			if string(delivery.Body) != want {
				t.Fatalf("delivered body = %q, want %q", delivery.Body, want)
			}
			if err := delivery.Ack(false); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("receiving the encoded neighbours: %v", ctx.Err())
		}
	}
	deadline := time.NewTimer(5 * time.Second) //nolint:forbidigo // bounded live-broker confirm guard
	defer deadline.Stop()
	var publishErr *driver.PublishError
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("batch Publish error = nil, want the unencodable message to fail")
		}
		if !errors.As(err, &publishErr) {
			t.Fatalf("batch Publish error = %v, want *driver.PublishError", err)
		}
	case <-deadline.C:
		t.Fatal("Publish did not return in time: it waited for a confirmation for a message that was never published")
	case <-ctx.Done():
		t.Fatalf("batch Publish result: %v", ctx.Err())
	}
	if len(publishErr.Failed) != 1 {
		t.Fatalf("failed indexes = %v, want only the unencodable message at index 1", publishErr.Failed)
	}
	failure, ok := publishErr.Failed[1]
	if !ok {
		t.Fatalf("failed indexes = %v, want index 1", publishErr.Failed)
	}
	if kind, classified := driver.Classify(failure); !classified || kind != driver.KindFatal {
		t.Fatalf("failed[1] classification = (%v, %t), want (fatal, true)", kind, classified)
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

// awaitUnconfirmedDeliveries consumes count deliveries from a destination,
// acking each, and fails the test if the driver did not get them onto the
// broker in time. It is used where the test withholds confirmations, so it
// bounds how long it waits for messages the driver published without waiting
// for one.
func awaitUnconfirmedDeliveries(t *testing.T, ctx context.Context, deliveries <-chan amqp.Delivery, count int) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second) //nolint:forbidigo // bounded live-broker publish guard
	defer deadline.Stop()
	for received := range count {
		select {
		case delivery := <-deliveries:
			if err := delivery.Ack(false); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		case <-deadline.C:
			t.Fatalf("%d of %d messages reached the broker while no confirmation was available", received, count)
		case <-ctx.Done():
			t.Fatalf("receiving published messages: %v", ctx.Err())
		}
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

// TestProducerParkingFailureNamesMissingQueue proves a delayed publish whose
// parking queue does not exist reports the failure against that queue itself.
// The broker's own answer is a 312 NO_ROUTE return, which names nothing, and
// under TopologyNone the parking queue is operator-provisioned topology, so the
// error is the only thing that can tell an operator what to create. The kind
// and the sentinel stay what the publish path already produced.
//
// The same producer publishes again once the queue exists, which is the
// operator's other question: creating the queue while the service runs is
// enough, and no restart is needed.
func TestProducerParkingFailureNamesMissingQueue(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const destination = "rabbitmq-driver-park-missing"
	conn, rawChannel := openProducerFixture(t, ctx, destination)
	parkName := destination + ".park"

	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	dropPark := func() {
		channel, channelErr := raw.Channel()
		if channelErr != nil {
			return
		}
		defer func() { _ = channel.Close() }()
		_, _ = channel.QueueDelete(parkName, false, false, false)
	}
	t.Cleanup(dropPark)
	// The parking queue is removed rather than merely assumed absent, so the
	// test measures the missing queue rather than the state of the fixture.
	// An undeleted leftover cannot make it pass quietly: the publish below
	// would then succeed and fail the test.
	dropPark()
	if _, err := rawChannel.QueueDeclare(destination, true, false, false, false, nil); err != nil {
		t.Fatalf("QueueDeclare(%q): %v", destination, err)
	}

	publisher, err := conn.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	parked := driver.OutboundMessage{
		Destination: destination,
		DelayUntil:  time.Now().Add(time.Hour), //nolint:forbidigo // a live delayed publish needs a future due time
		Headers:     []driver.Header{{Key: "id", Value: []byte("park-missing-1")}},
		Body:        []byte("parked"),
	}
	err = publisher.Publish(ctx, parked)
	if err == nil {
		t.Fatalf("Publish routed to the missing parking queue %q succeeded, want a failure", parkName)
	}
	if !errors.Is(err, driver.ErrDestinationMissing) {
		t.Fatalf("Publish error = %v, want ErrDestinationMissing", err)
	}
	if kind, classified := driver.Classify(err); !classified || kind != driver.KindNotFound {
		t.Fatalf("Publish classification = (%v, %t), want (not_found, true)", kind, classified)
	}
	var publishErr *driver.PublishError
	if !errors.As(err, &publishErr) || len(publishErr.Failed) != 1 {
		t.Fatalf("Publish error = %v, want exactly one failed message", err)
	}
	failure := publishErr.Failed[0]
	// The token list is the operator's checklist: the queue to create, the
	// policy split that decides who creates it, and the arguments a hand-made
	// queue must carry, including the two that only exist to keep a quorum
	// queue dead-lettering at least once.
	for _, want := range []string{parkName, "TopologyNone", "TopologyVerify", "x-dead-letter-routing-key", "x-dead-letter-strategy", "at-least-once"} {
		if !strings.Contains(failure.Error(), want) {
			t.Fatalf("parking failure %q does not name %q, so an operator cannot act on it", failure, want)
		}
	}

	if _, err := rawChannel.QueueDeclare(parkName, true, false, false, false, parkingArguments(destination, queueKindQuorum)); err != nil {
		t.Fatalf("QueueDeclare(%q): %v", parkName, err)
	}
	parked.Headers = []driver.Header{{Key: "id", Value: []byte("park-missing-2")}}
	if err := publisher.Publish(ctx, parked); err != nil {
		t.Fatalf("Publish after the parking queue exists: %v", err)
	}

	// A destination that is not a parking queue keeps the failure it already
	// produced. Rewriting it too would name a queue this publish never
	// targeted, which is a worse answer than the broker's own.
	missing := destination + "-missing"
	err = publisher.Publish(ctx, driver.OutboundMessage{Destination: missing, Body: []byte("missing")})
	if err == nil {
		t.Fatalf("Publish to the missing destination %q succeeded, want a failure", missing)
	}
	if !errors.As(err, &publishErr) || len(publishErr.Failed) != 1 {
		t.Fatalf("Publish error = %v, want exactly one failed message", err)
	}
	if failure := publishErr.Failed[0]; strings.Contains(failure.Error(), "parking destination") {
		t.Fatalf("failure for the missing destination %q claims a parking queue: %v", missing, failure)
	}
}

// TestParkingFailureRendersEveryDeclareArgument proves the message is derived
// from parkingArguments rather than restating it. The test iterates that
// function's own output, so an argument added there reaches the message with
// no second edit, and an argument removed there cannot leave the message
// telling an operator to create a queue the adapter would then report as
// drift. The two queue kinds are separate cases because the argument sets
// differ by kind.
//
// Durability is not in that argument table: it is a declare flag, and the
// adapter sets it for the parking queue under every kind, so the message has to
// state it in words and this test pins the words. An operator provisioning the
// queue under TopologyNone has only the message to read, and a non-durable
// queue there loses every parked message on a broker restart.
func TestParkingFailureRendersEveryDeclareArgument(t *testing.T) {
	const destination = "orders.deferred"
	cause := classify("publish", driver.KindNotFound,
		errors.Join(driver.ErrDestinationMissing, errors.New("rabbitmq: publish returned by broker: 312 NO_ROUTE")))

	// Both parking queue shapes are covered, because their argument sets
	// differ by more than the kind: a rung queue carries x-message-ttl and the
	// queue above the ladder does not.
	parkingQueues := map[string]string{
		"above the ladder": destination + parkingSuffix,
		"rung":             parkQueueName(destination, 2*time.Second),
	}
	for shape, parking := range parkingQueues {
		for _, kind := range []queueKind{queueKindQuorum, queueKindClassic} {
			t.Run(shape+"/"+string(kind), func(t *testing.T) {
				message := parkingFailure(parking, kind, cause).Error()
				args := parkArguments(destination, kind, parking)
				if _, isRung, ok := parkQueueParts(parking); ok && isRung > 0 {
					if _, present := args["x-message-ttl"]; !present {
						t.Fatalf("parking failure for the rung queue %q is rendered from an argument set without x-message-ttl", parking)
					}
				}
				for key, value := range args {
					rendered := fmt.Sprint(value)
					if text, ok := value.(string); ok {
						rendered = strconv.Quote(text)
					}
					if !strings.Contains(message, key+"="+rendered) {
						t.Fatalf("parking failure %q does not carry the declare argument %s=%s", message, key, rendered)
					}
				}
				for _, want := range []string{parking, "declares the queue durable", "TopologyDeclare", "TopologyVerify", "TopologyNone"} {
					if !strings.Contains(message, want) {
						t.Fatalf("parking failure %q does not name %q", message, want)
					}
				}
				if !errors.Is(parkingFailure(parking, kind, cause), driver.ErrDestinationMissing) {
					t.Fatalf("parking failure %q no longer wraps ErrDestinationMissing", message)
				}
			})
		}
	}
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

// TestProducerTargetRungsAndFallThrough pins the routing decision the ladder
// adds: a remaining delay on the ladder parks in that rung's queue and the
// message carries no expiration, because the rung queue's own TTL does the
// delaying. Above the ladder the per-message path is unchanged, which is what
// keeps a due time up to maxExpirationMillis owed instead of clamped early.
//
// The declared-delay path is used so the remaining delay is the table's own
// value: target reads one clock and derives the due time from it, so no wall
// time passes between the two.
func TestProducerTargetRungsAndFallThrough(t *testing.T) {
	cases := []struct {
		delay  time.Duration
		want   string
		onRung bool
	}{
		{delay: 500 * time.Millisecond, want: "orders.deferred.park.500ms", onRung: true},
		{delay: 501 * time.Millisecond, want: "orders.deferred.park.1s", onRung: true},
		{delay: time.Second, want: "orders.deferred.park.1s", onRung: true},
		{delay: 1500 * time.Millisecond, want: "orders.deferred.park.2s", onRung: true},
		{delay: 3 * time.Second, want: "orders.deferred.park.4s", onRung: true},
		{delay: 10 * time.Second, want: "orders.deferred.park.16s", onRung: true},
		{delay: 64 * time.Second, want: "orders.deferred.park.64s", onRung: true},
		{delay: 64*time.Second + time.Millisecond, want: "orders.deferred.park"},
		{delay: 24 * time.Hour, want: "orders.deferred.park"},
	}
	for _, test := range cases {
		t.Run(test.delay.String(), func(t *testing.T) {
			p := &producer{conn: &conn{deferred: map[string]time.Duration{"orders.deferred": test.delay}}}
			exchange, routingKey, expiration := p.target(driver.OutboundMessage{Destination: "orders.deferred"})
			if exchange != "" {
				t.Fatalf("target() exchange = %q, want empty for a parked publish", exchange)
			}
			if routingKey != test.want {
				t.Fatalf("target() routingKey = %q, want %q", routingKey, test.want)
			}
			if test.onRung && expiration != "" {
				t.Fatalf("target() expiration = %q, want empty on the ladder so the rung queue's TTL decides", expiration)
			}
			if !test.onRung && expiration == "" {
				t.Fatalf("target() expiration is empty above the ladder, want a per-message TTL for %s", test.delay)
			}
		})
	}
}

// TestParkingSuffixParsingIsStrict pins the parse that keeps a rung suffix
// from being a wildcard tail. The conformance suite declares a destination
// named topology.prune.park.eligible, and reading that name as a rung queue of
// topology.prune would both hide a queue from the orphan scan and take a rung
// queue out of the prune guard.
func TestParkingSuffixParsingIsStrict(t *testing.T) {
	cases := []struct {
		name        string
		destination string
		rung        time.Duration
		ok          bool
	}{
		{name: "orders.park", destination: "orders", ok: true},
		{name: "orders.park.1s", destination: "orders", rung: time.Second, ok: true},
		{name: "orders.park.500ms", destination: "orders", rung: 500 * time.Millisecond, ok: true},
		{name: "orders.park.64s", destination: "orders", rung: 64 * time.Second, ok: true},
		{name: "topology.prune.park.eligible"},
		{name: "orders.park.65s"},
		{name: "orders.park.1000"},
		{name: "orders.park."},
		{name: "orders.parked"},
		{name: "orders.deferred.park.1s.park.2s", destination: "orders.deferred.park.1s", rung: 2 * time.Second, ok: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			destination, rung, ok := parkQueueParts(test.name)
			if ok != test.ok || destination != test.destination || rung != test.rung {
				t.Fatalf("parkQueueParts(%q) = (%q, %s, %t), want (%q, %s, %t)",
					test.name, destination, rung, ok, test.destination, test.rung, test.ok)
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
