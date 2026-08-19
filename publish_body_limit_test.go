package f1

import (
	"context"
	"errors"
	"strings"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// TestPublishRejectsBodyExceedingCodecLimit proves codec.maxBodyBytes is
// enforced on a caller-initiated publish, matching how an oversized inbound
// delivery is dead-lettered.
func TestPublishRejectsBodyExceedingCodecLimit(t *testing.T) {
	t.Parallel()
	producer := &recordingProducer{}
	client := newPublishClient(t, producer)
	client.config.Codec.MaxBodyBytes = 10

	_, err := client.Publisher().Publish(context.Background(), "orders.created", strings.Repeat("x", 100))
	if err == nil {
		t.Fatal("Publish() error = nil, want a codec.maxBodyBytes rejection")
	}
	if !strings.Contains(err.Error(), "codec.maxBodyBytes") {
		t.Fatalf("Publish() error = %v, want a codec.maxBodyBytes error", err)
	}
	producer.mu.Lock()
	messages := len(producer.messages)
	producer.mu.Unlock()
	if messages != 0 {
		t.Fatalf("producer received %d message(s), want the oversized publish to never reach the driver", messages)
	}
}

// TestRetrySuccessorIgnoresCodecBodyLimit proves codec.maxBodyBytes does not
// apply to an SDK-generated retry republish, even when the already-received
// body exceeds it. Enforcing the limit here would turn a body the broker
// already accepted once into a new, silent message-loss path.
func TestRetrySuccessorIgnoresCodecBodyLimit(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.retry.created")
	defer func() { _ = client.Close(context.Background()) }()
	client.config.Codec.MaxBodyBytes = 4

	settler := &dispatchSettler{}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "oversized-retry",
		Source:      "/test/orders",
		Type:        "orders.retry.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"large":"payload-well-over-the-configured-limit"}`)
	message := driver.InboundMessage{
		Destination: publishEntryPoint("/test/orders", topicFor(envelope.Type), envelope.Priority),
		Headers:     headerSlice(headers),
		Body:        body,
		Settle:      settler,
	}
	if !retryAndSettle(runner, context.Background(), message, envelope, errors.New("temporary")) {
		t.Fatal("retry successor with an oversized body was not published and settled")
	}
	if len(producer.messages) != 1 {
		t.Fatalf("retry copies = %d, want 1: codec.maxBodyBytes must not block a successor republish", len(producer.messages))
	}
	if got := len(producer.messages[0].Body); got != len(body) {
		t.Fatalf("retry successor body length = %d, want %d (unchanged)", got, len(body))
	}
}
