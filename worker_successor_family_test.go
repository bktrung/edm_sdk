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

// matchingBridgeMessage returns a delivery whose consuming topic is also what
// its event type derives to.
func matchingBridgeMessage(t *testing.T, settler driver.Settler) (Envelope, driver.InboundMessage) {
	t.Helper()
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
		Settle:      settler,
	}
	return envelope, message
}

// TestSuccessorStaysInTheConsumingTopicFamily pins retry and dead-letter
// successors to the consuming topic's family. When the event type derives to
// another topic, the successor must not follow it; when the consuming topic is
// also what the event type derives to, both names stay byte-identical to the
// pre-ADR derivations.
func TestSuccessorStaysInTheConsumingTopicFamily(t *testing.T) {
	for _, delivery := range []struct {
		name    string
		message func(*testing.T, driver.Settler) (Envelope, driver.InboundMessage)
	}{
		{name: "type derives to another topic", message: divergentBridgeMessage},
		{name: "topic matches derived type", message: matchingBridgeMessage},
	} {
		for _, successor := range []struct {
			name    string
			send    func(*Runner, driver.InboundMessage, Envelope) bool
			want    string
			foreign func(*Client, *Runner, Envelope) string
		}{
			{
				name: "retry",
				send: func(runner *Runner, message driver.InboundMessage, envelope Envelope) bool {
					return retryAndSettle(runner, context.Background(), message, envelope, errors.New("temporary"), &deliveryState{})
				},
				want: "f1.test.orders.created.orders.high.retry.1",
				foreign: func(client *Client, runner *Runner, envelope Envelope) string {
					return retryDestinationFor(client.source, topicFor(envelope.Type), PriorityHigh, 1, runner.subscription.Name)
				},
			},
			{
				name: "dead letter",
				send: func(runner *Runner, message driver.InboundMessage, envelope Envelope) bool {
					return deadLetterAndSettle(runner, context.Background(), message, envelope, ReasonTerminal, errors.New("terminal"), &deliveryState{})
				},
				want: "f1.test.orders.created.dlq.orders",
				foreign: func(client *Client, runner *Runner, envelope Envelope) string {
					return deadLetterDestinationFor(client.source, topicFor(envelope.Type), runner.subscription.Name)
				},
			},
		} {
			t.Run(delivery.name+"/"+successor.name, func(t *testing.T) {
				producer := &dispatchProducer{}
				client, runner := newRetryBridgeRunner(t, producer, "orders.created")
				defer func() { _ = client.Close(context.Background()) }()

				envelope, message := delivery.message(t, &dispatchSettler{})
				if !successor.send(runner, message, envelope) {
					t.Fatalf("%s successor was not published and settled", successor.name)
				}
				if len(producer.messages) != 1 {
					t.Fatalf("%s copies = %d, want 1", successor.name, len(producer.messages))
				}
				if got := producer.messages[0].Destination; got != successor.want {
					t.Fatalf("%s successor destination = %q, want %q; deriving from event type would have produced %q",
						successor.name, got, successor.want, successor.foreign(client, runner, envelope))
				}
			})
		}
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
	if !retryAndSettle(runner, context.Background(), message, envelope, errors.New("temporary"), &deliveryState{}) {
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
	if !deadLetterAndSettle(deathRunner, context.Background(), message, envelope, ReasonTerminal, errors.New("terminal"), &deliveryState{}) {
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

// TestSuccessorTopicIgnoresHeaderPriority pins that the consuming family comes
// from the destination alone. Headers that did not decode leave a zero
// envelope, whose priority is medium; a delivery from the high destination must
// still resolve to its topic, so its dead-letter copy goes to the topic's
// dead-letter destination and not to the unknown one.
func TestSuccessorTopicIgnoresHeaderPriority(t *testing.T) {
	client, runner := newRetryBridgeRunner(t, &dispatchProducer{}, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()
	_, message := divergentBridgeMessage(t, &dispatchSettler{})
	if got := resolveDeliveryTopic(runner, Envelope{}, message); got != "orders.created" {
		t.Fatalf("resolveDeliveryTopic(undecoded headers on the high destination) = %q, want orders.created", got)
	}
	runner.destinationMetadata = map[string]destinationMetadata{
		message.Destination: {topic: "orders.created", priority: PriorityHigh},
	}
	if got := resolveDeliveryTopic(runner, Envelope{Type: "payments.charged.v1", Priority: PriorityLow}, message); got != "orders.created" {
		t.Fatalf("resolveDeliveryTopic(mismatched header priority) = %q, want orders.created", got)
	}
}

// TestSuccessorOutsideEveryDeclaredFamilyDerivesFromTheEventType pins the
// fallback: a delivery whose destination belongs to no topic the subscription
// declares has no consuming family to stay in, so its retry copy is routed by
// the event type's own topic.
func TestSuccessorOutsideEveryDeclaredFamilyDerivesFromTheEventType(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()

	envelope, message := divergentBridgeMessage(t, &dispatchSettler{})
	message.Destination = "outside.declared.family"
	if !retryAndSettle(runner, context.Background(), message, envelope, errors.New("temporary"), &deliveryState{}) {
		t.Fatal("retry successor was not published and settled")
	}
	if len(producer.messages) != 1 {
		t.Fatalf("retry copies = %d, want 1", len(producer.messages))
	}
	if got, want := producer.messages[0].Destination, "f1.test.payments.charged.orders.high.retry.1"; got != want {
		t.Fatalf("retry successor destination = %q, want %q", got, want)
	}
}
