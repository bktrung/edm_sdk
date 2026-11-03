package f1

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/bktrung/edm_sdk/driver"
	"github.com/bktrung/edm_sdk/internal/wire"
)

// TestOutboundHeadersAreSortedByKey pins that the header slice of every
// outbound copy lists its keys in ascending order. The producer receives the
// slice exactly as it is handed to the broker, so reading the keys back in
// position order catches a site that appends in map-range order.
func TestOutboundHeadersAreSortedByKey(t *testing.T) {
	t.Parallel()

	t.Run("publish", func(t *testing.T) {
		t.Parallel()
		producer, client := newHeaderOrderClient(t)
		if _, err := client.Publisher().Publish(context.Background(), "orders.created", "payload", WithKey("order-7")); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		message := producer.lastPublished(t)
		requireSortedHeaderKeys(t, message, []string{
			"datacontenttype",
			"f1attempt",
			"f1correlationid",
			"f1idempotencykey",
			"f1partitionkey",
			"f1priority",
			"f1producer",
			"id",
			"source",
			"specversion",
			"time",
			"type",
		})
		if got := messageHeaders(message)[wire.PartitionKey]; got != "order-7" {
			t.Fatalf("%s = %q, want the publish key", wire.PartitionKey, got)
		}
	})

	t.Run("retry copy", func(t *testing.T) {
		t.Parallel()
		producer, client := newHeaderOrderClient(t)
		if _, err := client.Publisher().Publish(context.Background(), "orders.created", "payload", WithKey("order-7")); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		published := producer.lastPublished(t)
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
						return errTestHeaderOrder
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
		var envelope Envelope
		if !dispatchMessage(runner, context.Background(), message, &envelope, new(bool), &deliveryState{}) {
			t.Fatal("dispatchMessage() did not complete retry settlement")
		}
		requireSortedHeaderKeys(t, producer.lastPublished(t), []string{
			"datacontenttype",
			"f1attempt",
			"f1correlationid",
			"f1duetime",
			"f1idempotencykey",
			"f1maxattempts",
			"f1originaldest",
			"f1partitionkey",
			"f1priority",
			"f1producer",
			"id",
			"source",
			"specversion",
			"time",
			"type",
		})
	})

	t.Run("dead-letter copy", func(t *testing.T) {
		t.Parallel()
		producer := &dispatchProducer{}
		client, runner := newPublishFanoutRunner(t, producer, "orders.created")
		t.Cleanup(func() { _ = client.Close(context.Background()) })
		envelope := Envelope{
			SpecVersion:  "1.0",
			ID:           "dead-letter-order",
			Source:       "orders",
			Type:         "orders.created.v1",
			Priority:     PriorityHigh,
			Attempt:      1,
			PartitionKey: "order-7",
		}
		message := driver.InboundMessage{
			Destination: consumeDestination(client.effective, client.source, "orders.created", PriorityHigh, runner.subscription.Name),
			Key:         []byte("order-7"),
			Body:        []byte(`{"id":"dead-letter-order"}`),
			Settle:      &dispatchSettler{},
		}
		if !deadLetterAndSettle(runner, context.Background(), message, envelope, ReasonTerminal, errors.New("terminal"), &deliveryState{}) {
			t.Fatal("dead-letter copy was not published and settled")
		}
		if len(producer.messages) != 1 {
			t.Fatalf("dead-letter copies = %d, want 1", len(producer.messages))
		}
		requireSortedHeaderKeys(t, producer.messages[0], []string{
			"f1attempt",
			"f1correlationid",
			"f1deatherror",
			"f1deathreason",
			"f1deathtime",
			"f1idempotencykey",
			"f1originaldest",
			"f1partitionkey",
			"f1priority",
			"id",
			"source",
			"specversion",
			"time",
			"type",
		})
	})
}

var errTestHeaderOrder = errors.New("retry me")

// newHeaderOrderClient returns a client whose producer keeps the messages it
// is handed, so a test can read back the header slice of each outbound copy.
func newHeaderOrderClient(t *testing.T) (*recordingProducer, *Client) {
	t.Helper()
	producer := &recordingProducer{}
	conn := &publishConn{
		producer: producer,
		caps:     driver.Capabilities{MaxHeaderBytes: 4096},
		info:     driver.BrokerInfo{Kind: "test", Version: "1"},
	}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&publishDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return producer, client
}

func (p *recordingProducer) lastPublished(t *testing.T) driver.OutboundMessage {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.messages) != 1 {
		t.Fatalf("producer received %d messages in the last publish, want 1", len(p.messages))
	}
	return p.messages[0]
}

// requireSortedHeaderKeys fails unless message carries exactly the keys in
// want, in that position order. want is written out by hand from the header
// names, so it is the expected order rather than a re-run of the sort.
func requireSortedHeaderKeys(t *testing.T, message driver.OutboundMessage, want []string) {
	t.Helper()
	got := make([]string, 0, len(message.Headers))
	for _, header := range message.Headers {
		got = append(got, header.Key)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("header keys = %q, want %q", got, want)
	}
}
