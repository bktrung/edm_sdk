package f1

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/wire"
)

func TestPublishAndRetryUseOneEffectiveHeaderCap(t *testing.T) {
	t.Parallel()

	const configuredLimit = 512
	producer := &recordingProducer{}
	conn := &publishConn{
		producer: producer,
		caps: driver.Capabilities{
			MaxHeaderBytes: 4096,
		},
		info: driver.BrokerInfo{Kind: "test", Version: "1"},
	}
	cfg := testClientConfig(t)
	cfg.Codec.MaxHeaderBytes = configuredLimit
	client, err := New(context.Background(), cfg, WithDriver(&publishDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	_, err = client.Publisher().Publish(
		context.Background(),
		"orders.created",
		"payload",
		WithHeader("x-debug", strings.Repeat("x", 1000)),
	)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	producer.mu.Lock()
	published := producer.messages[0]
	producer.mu.Unlock()
	publishedHeaders := messageHeaders(published)
	if _, ok := publishedHeaders["x-debug"]; ok {
		t.Fatal("publish retained an extension beyond the configured header cap")
	}
	assertHeaderBudget(t, publishedHeaders, configuredLimit)

	envelope, err := DecodeHeaders(publishedHeaders)
	if err != nil {
		t.Fatalf("DecodeHeaders(published) error = %v", err)
	}
	runner := &Runner{
		client: client,
		subscription: Subscription{
			Name:       "orders",
			Topics:     []string{"orders.created"},
			Priorities: []Priority{PriorityMedium},
			Retry: RetryConfig{
				MaxAttempts: 3,
				Tiers:       []time.Duration{time.Second},
			},
			HandlerTimeout: time.Second,
			Handlers: map[string]Handler{
				"orders.created": HandlerFunc(func(context.Context, *Event) error {
					return errors.New("retry me")
				}),
			},
		},
	}
	settler := &dispatchSettler{}
	message := driver.InboundMessage{
		Destination: published.Destination,
		Key:         append([]byte(nil), published.Key...),
		Headers:     append([]driver.Header(nil), published.Headers...),
		Body:        append([]byte(nil), published.Body...),
		Settle:      settler,
	}
	if !dispatchMessage(runner, context.Background(), message, &envelope, new(bool), &deliveryState{}) {
		t.Fatal("dispatchMessage() did not complete retry settlement")
	}
	if !settler.acked || settler.nacked {
		t.Fatalf("settlement = acked %v, nacked %v; want ack", settler.acked, settler.nacked)
	}

	producer.mu.Lock()
	retried := producer.messages[0]
	producer.mu.Unlock()
	retriedHeaders := messageHeaders(retried)
	if _, ok := retriedHeaders["x-debug"]; ok {
		t.Fatal("retry retained an extension beyond the configured header cap")
	}
	assertHeaderBudget(t, retriedHeaders, configuredLimit)
}

func TestMalformedDeadLetterHeadersRespectCap(t *testing.T) {
	t.Parallel()
	const limit = 512
	containment := newSuccessorContainment(t, containmentOptions{headerMaxBytes: limit})
	settler := newContainmentSettler()
	message := terminalBodyMessage(t, nil, map[string]string{
		wire.SpecVersion: "1.0",
		"x-debug":        strings.Repeat("x", 2000),
	}, []byte("malformed-payload"), settler)
	message.Destination = containmentDeliveryDestination()
	if !dispatchMessage(containment.runner, context.Background(), message, &Envelope{}, new(bool), &deliveryState{}) {
		t.Fatal("malformed delivery was not settled")
	}
	settler.requireAcknowledged(t)
	published := containment.acceptedPublishes()
	if len(published) != 1 {
		t.Fatalf("published copies = %d, want one dead-letter copy", len(published))
	}
	headers := messageHeaders(published[0])
	if _, ok := headers["x-debug"]; ok {
		t.Fatal("malformed dead-letter copy retained unknown headers beyond the cap")
	}
	assertHeaderBudget(t, headers, limit)
	if headers[wire.DeathReason] != ReasonDecode.String() || headers[wire.DeathError] == "" ||
		headers[wire.DeathTime] == "" || headers[wire.OriginalDest] != message.Destination {
		t.Fatalf("dead-letter headers = %v, want decode death metadata", headers)
	}
	if published[0].Destination != deadLetterDestination(containment.runner, containmentTopic) ||
		string(published[0].Body) != string(message.Body) {
		t.Fatalf("dead-letter copy = %+v, want original body at the dead-letter destination", published[0])
	}
}

func TestUnencodableMalformedDeadLetterIsDroppedAndConsumptionContinues(t *testing.T) {
	t.Parallel()
	containment := newSuccessorContainment(t, containmentOptions{headerMaxBytes: 512})
	containment.start(t)
	settler := newContainmentSettler()
	message := terminalBodyMessage(t, nil, map[string]string{
		wire.SpecVersion:  "1.0",
		wire.PartitionKey: strings.Repeat("k", 2000),
	}, []byte("malformed-payload"), settler)
	message.Destination = containmentDeliveryDestination()
	timer := clock.NewReal().Timer(containmentTimeout)
	defer timer.Stop()
	select {
	case containment.consumer.messages <- message:
	case <-timer.C:
		t.Fatal("the running subscription did not take the malformed delivery")
	}
	_, callErr := containment.waitForReport(t)
	requireDropHeadline(t, callErr)
	if !errors.Is(callErr, ErrEnvelopeTooLarge) || !errors.Is(callErr, errSuccessorCopyUnencodable) {
		t.Fatalf("drop report = %v, want an unencodable oversized successor", callErr)
	}
	settler.requireAcknowledged(t)
	if len(containment.acceptedPublishes()) != 0 {
		t.Fatal("an oversized malformed dead-letter copy reached the broker")
	}
	containment.deliver(t, containmentEnvelope("after-malformed-drop"), newContainmentSettler())
	if got := containment.waitForHandled(t); got != "after-malformed-drop" {
		t.Fatalf("handled event = %q, want the event after the malformed drop", got)
	}
	if err := containment.health(t); err != nil {
		t.Fatalf("Health() = %v, want a healthy subscription after the drop", err)
	}
	containment.stop(t)
}

func assertHeaderBudget(t *testing.T, headers map[string]string, limit int) {
	t.Helper()
	if got := headerBytes(headers); got > limit {
		t.Fatalf("encoded headers = %d bytes, want at most %d", got, limit)
	}
}
