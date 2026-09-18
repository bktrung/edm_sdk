package f1

import (
	"context"
	"errors"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// publishFanoutDriver opens a connection reporting RabbitMQ-shaped
// capabilities: fan-out happens at publish time, so a subscription receives
// on its own consumeDestination and the shared publish entry point never
// carries a consumer delivery.
type publishFanoutDriver struct{ conn *dispatchConn }

func (*publishFanoutDriver) Name() string { return "inmem" }

func (*publishFanoutDriver) Capabilities() driver.Capabilities {
	return driver.Capabilities{Fanout: driver.FanoutAtPublish, MaxHeaderBytes: CoreMaxHeaderBytes}
}

func (d *publishFanoutDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return &publishFanoutDispatchConn{dispatchConn: d.conn}, nil
}

type publishFanoutDispatchConn struct {
	*dispatchConn
}

func (*publishFanoutDispatchConn) Capabilities() driver.Capabilities {
	return driver.Capabilities{Fanout: driver.FanoutAtPublish, MaxHeaderBytes: CoreMaxHeaderBytes}
}

// newPublishFanoutRunner mirrors newRetryBridgeRunner but pins the effective
// fanout to driver.FanoutAtPublish, the mode the RabbitMQ driver reports.
func newPublishFanoutRunner(t *testing.T, producer *dispatchProducer, topic string) (*Client, *Runner) {
	t.Helper()
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&publishFanoutDriver{conn: &dispatchConn{producer: producer}}))
	if err != nil {
		t.Fatal(err)
	}
	runner := &Runner{
		client: client,
		subscription: Subscription{
			Name:           "orders",
			Topics:         []string{topic},
			Priorities:     []Priority{PriorityHigh},
			Retry:          RetryConfig{MaxAttempts: 3, Tiers: []time.Duration{time.Second, 2 * time.Second}},
			HandlerTimeout: time.Second,
		},
	}
	return client, runner
}

// divergentBridgeMessage builds a delivery that arrives inside the
// subscription's configured topic family while carrying an envelope whose
// event type derives to a different topic. That divergence is exactly what
// WithTopic exists to produce, and it is the divergence whose successors
// must stay in the consuming family.
func divergentBridgeMessage(t *testing.T, settler driver.Settler) (Envelope, driver.InboundMessage) {
	t.Helper()
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "with-topic-divergence",
		Source:      "/test/orders",
		Type:        "payments.charged.v2",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	message := driver.InboundMessage{
		Destination: publishEntryPoint("/test/orders", "orders.created", PriorityHigh),
		Headers:     headerSlice(headers),
		Body:        []byte("{}"),
		Settle:      settler,
	}
	return envelope, message
}

func TestRetrySuccessorStaysInTheConsumingTopicFamily(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()

	settler := &dispatchSettler{}
	envelope, message := divergentBridgeMessage(t, settler)
	if !retryAndSettle(runner, context.Background(), message, envelope, errors.New("temporary")) {
		t.Fatal("retry successor was not published and settled")
	}
	if len(producer.messages) != 1 {
		t.Fatalf("retry copies = %d, want 1", len(producer.messages))
	}
	want := "f1.test.orders.created.orders.high.retry.1"
	foreign := retryDestinationFor(client.source, topicFor(envelope.Type), PriorityHigh, 1, runner.subscription.Name)
	if got := producer.messages[0].Destination; got != want {
		t.Fatalf("retry successor destination = %q, want %q; deriving from event type would have produced %q", got, want, foreign)
	}
}

func TestDeadLetterSuccessorStaysInTheConsumingTopicFamily(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()

	settler := &dispatchSettler{}
	envelope, message := divergentBridgeMessage(t, settler)
	if !deadLetterAndSettle(runner, context.Background(), message, envelope, ReasonTerminal, errors.New("terminal")) {
		t.Fatal("dead-letter successor was not published and settled")
	}
	if len(producer.messages) != 1 {
		t.Fatalf("dead-letter copies = %d, want 1", len(producer.messages))
	}
	want := "f1.test.orders.created.dlq.orders"
	foreign := deadLetterDestinationFor(client.source, topicFor(envelope.Type), runner.subscription.Name)
	if got := producer.messages[0].Destination; got != want {
		t.Fatalf("dead-letter successor destination = %q, want %q; deriving from event type would have produced %q", got, want, foreign)
	}
}

// TestSuccessorNamesAreUnchangedWhenTopicMatchesDerivedType pins the
// compatibility consequence: when the consuming topic is also
// what the event type derives to, both successor names stay byte-identical
// to the pre-ADR derivations.
func TestSuccessorNamesAreUnchangedWhenTopicMatchesDerivedType(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()

	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "matching-topic-and-type",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	message := driver.InboundMessage{
		Destination: publishEntryPoint("/test/orders", topicFor(envelope.Type), PriorityHigh),
		Headers:     headerSlice(headers),
		Body:        []byte("{}"),
		Settle:      &dispatchSettler{},
	}
	if !retryAndSettle(runner, context.Background(), message, envelope, errors.New("temporary")) {
		t.Fatal("retry successor was not published and settled")
	}
	want := "f1.test.orders.created.orders.high.retry.1"
	if got := producer.messages[0].Destination; got != want {
		t.Fatalf("retry successor destination = %q, want byte-identical %q", got, want)
	}

	deathProducer := &dispatchProducer{}
	deathClient, deathRunner := newRetryBridgeRunner(t, deathProducer, "orders.created")
	defer func() { _ = deathClient.Close(context.Background()) }()
	message.Settle = &dispatchSettler{}
	if !deadLetterAndSettle(deathRunner, context.Background(), message, envelope, ReasonTerminal, errors.New("terminal")) {
		t.Fatal("dead-letter successor was not published and settled")
	}
	want = "f1.test.orders.created.dlq.orders"
	if got := deathProducer.messages[0].Destination; got != want {
		t.Fatalf("dead-letter successor destination = %q, want byte-identical %q", got, want)
	}
}

// TestSuccessorFamilyHoldsUnderPublishTimeFanout pins the family match in
// the mode RabbitMQ actually reports. Under driver.FanoutAtPublish the
// inbound main destination is subscription-specific consumeDestination, so a
// matcher that ignores FanoutAtPublish and probes publishEntryPoint instead
// finds no owning family and lets successors derive from the event type.
// This test must fail under that mutation while the FanoutAtConsume fixtures
// above keep passing.
func TestSuccessorFamilyHoldsUnderPublishTimeFanout(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newPublishFanoutRunner(t, producer, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()

	if client.effective.Fanout != driver.FanoutAtPublish {
		t.Fatalf("effective fanout = %d, want driver.FanoutAtPublish", client.effective.Fanout)
	}

	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "publish-time-fanout-divergence",
		Source:      "/test/orders",
		Type:        "payments.charged.v2",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	main := consumeDestination(client.effective, client.source, "orders.created", PriorityHigh, runner.subscription.Name)
	if main == publishEntryPoint(client.source, "orders.created", PriorityHigh) {
		t.Fatal("precondition lost: consume destination equals the shared publish entry point")
	}
	message := driver.InboundMessage{
		Destination: main,
		Headers:     headerSlice(headers),
		Body:        []byte("{}"),
		Settle:      &dispatchSettler{},
	}
	if !retryAndSettle(runner, context.Background(), message, envelope, errors.New("temporary")) {
		t.Fatal("retry successor was not published and settled")
	}
	if len(producer.messages) != 1 {
		t.Fatalf("retry copies = %d, want 1", len(producer.messages))
	}
	want := "f1.test.orders.created.orders.high.retry.1"
	foreign := retryDestinationFor(client.source, topicFor(envelope.Type), PriorityHigh, 1, runner.subscription.Name)
	if got := producer.messages[0].Destination; got != want {
		t.Fatalf("retry successor destination = %q, want %q; deriving from event type would have produced %q", got, want, foreign)
	}

	deathProducer := &dispatchProducer{}
	deathClient, deathRunner := newPublishFanoutRunner(t, deathProducer, "orders.created")
	defer func() { _ = deathClient.Close(context.Background()) }()
	message.Settle = &dispatchSettler{}
	if !deadLetterAndSettle(deathRunner, context.Background(), message, envelope, ReasonTerminal, errors.New("terminal")) {
		t.Fatal("dead-letter successor was not published and settled")
	}
	if len(deathProducer.messages) != 1 {
		t.Fatalf("dead-letter copies = %d, want 1", len(deathProducer.messages))
	}
	want = "f1.test.orders.created.dlq.orders"
	foreign = deadLetterDestinationFor(deathClient.source, topicFor(envelope.Type), deathRunner.subscription.Name)
	if got := deathProducer.messages[0].Destination; got != want {
		t.Fatalf("dead-letter successor destination = %q, want %q; deriving from event type would have produced %q", got, want, foreign)
	}
}
