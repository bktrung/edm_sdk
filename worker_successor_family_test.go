package f1

import (
	"context"
	"errors"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

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
	want := retryDestinationFor(client.source, "orders.created", PriorityHigh, 1, runner.subscription.Name)
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
	want := deadLetterDestinationFor(client.source, "orders.created", runner.subscription.Name)
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
	want := retryDestinationFor(client.source, topicFor(envelope.Type), PriorityHigh, 1, runner.subscription.Name)
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
	want = deadLetterDestinationFor(deathClient.source, topicFor(envelope.Type), deathRunner.subscription.Name)
	if got := deathProducer.messages[0].Destination; got != want {
		t.Fatalf("dead-letter successor destination = %q, want byte-identical %q", got, want)
	}
}
