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

	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	producer, err := conn.Producer(ctx, driver.ProducerConfig{})
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
	producer, err := conn.Producer(ctx, driver.ProducerConfig{})
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

// TestProducerBatchReturnFromBrokerFailsOnlyItsOwnIndex proves a broker return
// fails the message it belongs to and no other: one unroutable message in the
// middle of a batch is reported against its own index and classified
// not-found, and its neighbours are published in order.
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
	rawProducer, err := conn.Producer(ctx, driver.ProducerConfig{})
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
// fails only the message it confirms. The broker nacks for real here: a queue
// declared with a length limit of one and reject-publish overflow takes the
// first message of the batch and refuses the two behind it, so the first is
// reported published and the other two fail transient on the channel that is
// still open. The queue is classic because a quorum queue applies its length
// limit to committed messages, so a batch in flight can all be accepted.
func TestProducerNegativeConfirmFailsItsOwnIndex(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-batch-negative"
	conn, rawChannel := openProducerFixture(t, ctx, queue)
	if _, err := rawChannel.QueueDeclare(queue, true, false, false, false, amqp.Table{
		"x-queue-type": "classic",
		"x-max-length": int64(1),
		"x-overflow":   "reject-publish",
	}); err != nil {
		t.Fatalf("QueueDeclare: %v", err)
	}
	producer, err := conn.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(context.Background()) })
	messages := make([]driver.OutboundMessage, 3)
	for index := range messages {
		messages[index] = driver.OutboundMessage{
			Destination: queue,
			Headers:     []driver.Header{{Key: "id", Value: fmt.Appendf(nil, "negative-%d", index)}},
			Body:        fmt.Appendf(nil, "negative-body-%d", index),
		}
	}
	err = producer.Publish(ctx, messages...)
	var publishErr *driver.PublishError
	if !errors.As(err, &publishErr) {
		t.Fatalf("batch Publish error = %v, want *driver.PublishError", err)
	}
	if len(publishErr.Failed) != 2 {
		t.Fatalf("failed indexes = %v, want the two messages over the queue's limit", publishErr.Failed)
	}
	for _, index := range []int{1, 2} {
		failure, ok := publishErr.Failed[index]
		if !ok {
			t.Fatalf("failed indexes = %v, want index %d", publishErr.Failed, index)
		}
		if kind, classified := driver.Classify(failure); !classified || kind != driver.KindTransient {
			t.Fatalf("failed[%d] classification = (%v, %t), want (transient, true)", index, kind, classified)
		}
		if !strings.Contains(failure.Error(), "negative") {
			t.Fatalf("failed[%d] = %v, want the negative confirmation, not a lost channel", index, failure)
		}
	}
}

// oversizedBodyBytes is one byte past the largest message the fixture broker
// accepts, which is what makes it refuse the publish. The fixture runs
// rabbitmq:4.3.4-management and configures no limit, so the broker's own
// max_message_size default of 16777216 bytes applies. The size is taken from
// the broker rather than from the driver because the driver declares no
// message limit, and a limit it guessed would be a second answer that could
// disagree with the one that refuses the publish.
const oversizedBodyBytes = (1 << 24) + 1

// TestProducerSizeRefusalFailsOnlyItsOwnIndex proves a publish the broker
// refuses for its size is reported against the messages whose body is too large
// and no other: the refused message is over the broker's limit, so it is too
// large, and the message behind it went down with the channel while under the
// limit, so it stays the undecided transient failure a lost channel has always
// produced. The producer then publishes again, because the channel that carried
// the refusal is gone and the next publish opens one of its own.
//
// The destination is the quorum kind this driver defaults to, and the message
// in front of the refused one is where that shows. A quorum queue commits a
// persistent message asynchronously, so the close can beat that message's
// confirmation: the publish then finds it undecided and reports it transient,
// which is the retry an unconfirmed message gets. What it must never be is too
// large, and that is what this test pins about it, because the broker published
// it and a length at or under the limit cannot be refused for its size.
func TestProducerSizeRefusalFailsOnlyItsOwnIndex(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-size-refusal"
	conn, rawChannel := openProducerFixture(t, ctx, queue)
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	rawProducer, err := conn.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = rawProducer.Close(context.Background()) })
	deliveries, err := rawChannel.Consume(queue, "producer-size-refusal", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	messages := []driver.OutboundMessage{
		{
			Destination: queue,
			Headers:     []driver.Header{{Key: "id", Value: []byte("size-0")}},
			Body:        []byte("confirmed-before-the-refusal"),
		},
		{
			Destination: queue,
			Headers:     []driver.Header{{Key: "id", Value: []byte("size-1")}},
			Body:        make([]byte, oversizedBodyBytes),
		},
		{
			Destination: queue,
			Headers:     []driver.Header{{Key: "id", Value: []byte("size-2")}},
			Body:        []byte("discarded-with-the-channel"),
		},
	}
	err = rawProducer.Publish(ctx, messages...)
	var publishErr *driver.PublishError
	if !errors.As(err, &publishErr) {
		t.Fatalf("batch Publish error = %v, want *driver.PublishError", err)
	}
	failure, ok := publishErr.Failed[1]
	if !ok {
		t.Fatalf("failed indexes = %v, want the oversized message at index 1", publishErr.Failed)
	}
	if kind, classified := driver.Classify(failure); !classified || kind != driver.KindTooLarge {
		t.Fatalf("failed[1] classification = (%v, %t), want (too large, true)", kind, classified)
	}
	undecided, ok := publishErr.Failed[2]
	if !ok {
		t.Fatalf("failed indexes = %v, want the message behind the refusal at index 2", publishErr.Failed)
	}
	if kind, classified := driver.Classify(undecided); !classified || kind != driver.KindTransient {
		t.Fatalf("failed[2] classification = (%v, %t), want (transient, true)", kind, classified)
	}
	if failure, failed := publishErr.Failed[0]; failed {
		if kind, classified := driver.Classify(failure); !classified || kind == driver.KindTooLarge {
			t.Fatalf("failed[0] classification = (%v, %t), want a message under the limit never too large", kind, classified)
		}
	}
	for index := range publishErr.Failed {
		if index > 2 {
			t.Fatalf("failed indexes = %v, want only the messages of this batch", publishErr.Failed)
		}
	}
	select {
	case delivery := <-deliveries:
		if string(delivery.Body) != "confirmed-before-the-refusal" {
			t.Fatalf("delivered body = %q, want the message in front of the refusal", delivery.Body)
		}
		if err := delivery.Ack(false); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("receiving the message in front of the refusal: %v", ctx.Err())
	}
	if err := rawProducer.Publish(ctx, driver.OutboundMessage{Destination: queue, Body: []byte("after-the-refusal")}); err != nil {
		t.Fatalf("Publish after the refusal = %v, want the producer to publish on a channel of its own", err)
	}
}

// TestProducerSizeRefusalIsTheOnlyFailure proves the smallest batch that can
// hold a refusal reports it and nothing else. One message, refused for its
// size, is the case a caller reads a failure map for: a copy that can never be
// published reaches the caller as that one entry, which is what keeps it from
// being retried forever or from stopping the subscription that produced it.
func TestProducerSizeRefusalIsTheOnlyFailure(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-producer-size-only"
	conn, _ := openProducerFixture(t, ctx, queue)
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	rawProducer, err := conn.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = rawProducer.Close(context.Background()) })

	err = rawProducer.Publish(ctx, driver.OutboundMessage{
		Destination: queue,
		Headers:     []driver.Header{{Key: "id", Value: []byte("size-only")}},
		Body:        make([]byte, oversizedBodyBytes),
	})
	var publishErr *driver.PublishError
	if !errors.As(err, &publishErr) {
		t.Fatalf("Publish error = %v, want *driver.PublishError", err)
	}
	if len(publishErr.Failed) != 1 {
		t.Fatalf("failed indexes = %v, want only the refused message", publishErr.Failed)
	}
	failure, ok := publishErr.Failed[0]
	if !ok {
		t.Fatalf("failed indexes = %v, want index 0", publishErr.Failed)
	}
	if kind, classified := driver.Classify(failure); !classified || kind != driver.KindTooLarge {
		t.Fatalf("failed[0] classification = (%v, %t), want (too large, true)", kind, classified)
	}
	// The broker's own words for the refusal are the only thing that tells an
	// operator which limit was exceeded, and they are what the driver used to
	// throw away by reporting the closed channel instead.
	if !strings.Contains(failure.Error(), "message size") {
		t.Fatalf("failed[0] = %v, want the broker's reason for the refusal", failure)
	}
}

// TestProducerBatchWithoutMessageIDsFailsOnlyItsOwnIndex covers correlation
// when no envelope id is available: amqpPublishing only sets the AMQP message
// id from an "id" header, and the driver port is publishable without one. A
// basic.return carries no delivery tag, so two id-less messages must never be
// unresolved on one channel: the driver publishes each as a segment of its
// own, which costs one round trip per message and keeps every return on its
// own index. The batch is
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
	rawProducer, err := conn.Producer(ctx, driver.ProducerConfig{})
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

// TestProducerBatchLocalEncodingFailureDoesNotStall covers a batch whose
// messages on the wire are fewer than the batch: a message that fails to encode
// is never published, so it consumes no delivery tag and no confirmation for it
// will ever arrive. The reader must therefore wait for one confirmation per
// successful publish, not one per message in the batch. Getting that wrong
// blocks Publish until its context expires.
func TestProducerBatchLocalEncodingFailureDoesNotStall(t *testing.T) {
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
	rawProducer, err := conn.Producer(ctx, driver.ProducerConfig{})
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
	producer, err := conn.Producer(ctx, driver.ProducerConfig{})
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
	t.Cleanup(func() {
		if err := raw.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("cleanup raw connection close: %v", err)
		}
	})
	channel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	if _, err := channel.QueueDelete(queue, false, false, false); err != nil && !errors.Is(err, amqp.ErrClosed) {
		t.Fatalf("initial QueueDelete(%q): %v", queue, err)
	}
	t.Cleanup(func() {
		if _, err := channel.QueueDelete(queue, false, false, false); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("cleanup QueueDelete(%q): %v", queue, err)
		}
		if err := channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("cleanup channel close: %v", err)
		}
	})
	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := conn.Close(cleanupCtx); err != nil {
			t.Errorf("cleanup connection close: %v", err)
		}
	})
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
	const delay = time.Hour
	parkName := parkQueueName(destination)

	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() {
		if err := raw.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("cleanup parking raw connection close: %v", err)
		}
	})
	dropPark := func() {
		channel, err := raw.Channel()
		if err != nil {
			t.Errorf("open parking cleanup channel: %v", err)
			return
		}
		if _, err := channel.QueueDelete(parkName, false, false, false); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("cleanup QueueDelete(%q): %v", parkName, err)
		}
		if err := channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("cleanup parking channel close: %v", err)
		}
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
	// TopologyNone records the delay for routing and declares nothing, which is
	// the operator-provisioned shape this failure is written for.
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Policy:       driver.TopologyNone,
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: delay}},
	}); err != nil {
		t.Fatalf("EnsureTopology(none): %v", err)
	}

	publisher, err := conn.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(context.Background()); err != nil {
			t.Errorf("cleanup publisher: %v", err)
		}
	})
	parked := driver.OutboundMessage{
		Destination: destination,
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

	if _, err := rawChannel.QueueDeclare(parkName, true, false, false, false, parkArguments(destination, queueKindQuorum)); err != nil {
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
// from parkArguments rather than restating it. The test iterates that
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

	parking := parkQueueName(destination)
	for _, kind := range []queueKind{queueKindQuorum, queueKindClassic} {
		t.Run(string(kind), func(t *testing.T) {
			message := parkingFailure(parking, kind, cause).Error()
			args := parkArguments(destination, kind)
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
