package f1

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
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
	if !dispatchMessage(runner, context.Background(), message, &envelope, new(bool)) {
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

func assertHeaderBudget(t *testing.T, headers map[string]string, limit int) {
	t.Helper()
	if got := headerBytes(headers); got > limit {
		t.Fatalf("encoded headers = %d bytes, want at most %d", got, limit)
	}
}
