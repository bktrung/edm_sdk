package f1

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/codec"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type registryTestCodec struct {
	name        string
	contentType string
	decodeCalls *atomic.Int32
	reject      bool
}

func (c *registryTestCodec) Name() string {
	if c.name == "" {
		return "test-alt"
	}
	return c.name
}

func (c *registryTestCodec) ContentType() string {
	if c.contentType == "" {
		return "application/test-alt"
	}
	return c.contentType
}

func (*registryTestCodec) Encode(value any) ([]byte, error) {
	return (codec.JSON{}).Encode(value)
}

func (c *registryTestCodec) Decode(data []byte, value any) error {
	if c.decodeCalls != nil {
		c.decodeCalls.Add(1)
	}
	if c.reject {
		return errors.New("alternate codec was selected")
	}
	return (codec.JSON{}).Decode(data, value)
}

func newCodecSelectionRunner(t *testing.T, cfg Config, opts ...Option) (*Client, *Runner, *dispatchProducer) {
	t.Helper()
	producer := &dispatchProducer{}
	options := append([]Option{WithDriver(&dispatchDriver{conn: &dispatchConn{producer: producer}})}, opts...)
	client, err := New(context.Background(), cfg, options...)
	if err != nil {
		t.Fatal(err)
	}
	runner := &Runner{
		client: client,
		subscription: Subscription{
			Name:           "orders",
			Topics:         []string{"orders.created"},
			Priorities:     []Priority{PriorityMedium},
			Retry:          RetryConfig{MaxAttempts: 3, Tiers: []time.Duration{time.Second}},
			HandlerTimeout: time.Second,
		},
	}
	return client, runner, producer
}

func codecSelectionMessage(t *testing.T, envelope Envelope, body []byte, settler driver.Settler) driver.InboundMessage {
	t.Helper()
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	return driver.InboundMessage{
		Destination: "f1.test.orders.created.medium",
		Headers:     headerSlice(headers),
		Body:        body,
		Settle:      settler,
	}
}

func codecSelectionEnvelope(contentType string) Envelope {
	return Envelope{
		SpecVersion:     "1.0",
		ID:              "codec-event",
		Source:          "/test/orders",
		Type:            "orders.created.v1",
		DataContentType: contentType,
		Attempt:         1,
	}
}

func TestDispatchUsesDefaultCodecForEmptyContentType(t *testing.T) {
	for _, test := range []struct {
		name   string
		marker bool
		config func(*testing.T) Config
	}{
		{
			name:   "configured default",
			marker: true,
			config: testClientConfig,
		},
		{
			name: "hand-built config uses JSON",
			config: func(*testing.T) Config {
				cfg := Config{Env: "test", Service: "orders", Broker: BrokerConfig{Driver: "inmem"}}
				cfg.Topology.AutoCreate = true
				return cfg
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := test.config(t)
			var options []Option
			marker := &registryTestCodec{decodeCalls: new(atomic.Int32)}
			if test.marker {
				cfg.Codec.Default = marker.Name()
				options = append(options, WithCodec(marker))
			}
			client, runner, _ := newCodecSelectionRunner(t, cfg, options...)
			defer func() { _ = client.Close(context.Background()) }()

			var got struct {
				Value string `json:"value"`
			}
			var decodeErr error
			runner.subscription.Handlers = map[string]Handler{
				"orders.created.v1": HandlerFunc(func(_ context.Context, event *Event) error {
					decodeErr = event.Decode(&got)
					return decodeErr
				}),
			}
			settler := &dispatchSettler{}
			message := codecSelectionMessage(t, codecSelectionEnvelope(""), []byte(`{"value":"default"}`), settler)
			if !dispatchMessage(runner, context.Background(), message, &Envelope{}, new(bool), &deliveryState{}) {
				t.Fatal("empty-content-type message was not settled")
			}
			if decodeErr != nil {
				t.Fatalf("default codec decode error = %v", decodeErr)
			}
			if got.Value != "default" {
				t.Fatalf("decoded value = %q, want default", got.Value)
			}
			if test.marker && marker.decodeCalls.Load() != 1 {
				t.Fatalf("alternate codec decode calls = %d, want 1", marker.decodeCalls.Load())
			}
		})
	}
}

func TestUnknownContentTypeDeadLettersBeforeHandler(t *testing.T) {
	cfg := testClientConfig(t)
	client, runner, producer := newCodecSelectionRunner(t, cfg)
	defer func() { _ = client.Close(context.Background()) }()

	handled := false
	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error {
			handled = true
			return nil
		}),
	}
	settler := &dispatchSettler{}
	message := codecSelectionMessage(t, codecSelectionEnvelope("application/unknown"), []byte(`{"value":"unknown"}`), settler)
	if !dispatchMessage(runner, context.Background(), message, &Envelope{}, new(bool), &deliveryState{}) {
		t.Fatal("unknown-content-type message was not settled")
	}
	if handled {
		t.Fatal("handler ran for an unknown content type")
	}
	if !settler.acked {
		t.Fatal("unknown-content-type message was not acked after DLQ publish")
	}
	if len(producer.messages) != 1 {
		t.Fatalf("DLQ messages = %d, want 1", len(producer.messages))
	}
	if got := headerValue(producer.messages[0].Headers, "f1deathreason"); got != ReasonDecode.String() {
		t.Fatalf("death reason = %q, want %q", got, ReasonDecode)
	}
}

func TestNewRejectsUnregisteredCodecDefault(t *testing.T) {
	cfg := testClientConfig(t)
	cfg.Codec.Default = "protobuf"
	drv := &testDriver{}
	_, err := New(context.Background(), cfg, WithDriver(drv))
	if err == nil || !strings.Contains(err.Error(), "protobuf") {
		t.Fatalf("New() error = %v, want unregistered protobuf error", err)
	}
	if drv.opened {
		t.Fatal("driver opened before rejecting unregistered default codec")
	}
}

func TestWithCodecRequiresAtLeastOneCodec(t *testing.T) {
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&dispatchDriver{conn: &dispatchConn{producer: &dispatchProducer{}}}),
		WithCodec(),
	)
	if err == nil {
		_ = client.Close(context.Background())
		t.Fatal("WithCodec() was accepted without a codec")
	}
	if got, want := err.Error(), "f1: WithCodec requires at least one codec"; got != want {
		t.Fatalf("WithCodec() error = %q, want %q", got, want)
	}
}

func TestWithCodecRejectsTwoCodecsForOneContentType(t *testing.T) {
	first := &registryTestCodec{name: "first", contentType: "application/json"}
	second := &registryTestCodec{name: "second", contentType: "application/json"}
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&dispatchDriver{conn: &dispatchConn{producer: &dispatchProducer{}}}),
		WithCodec(first, second),
	)
	if err == nil {
		_ = client.Close(context.Background())
		t.Fatal("WithCodec() accepted two codecs for one content type, which would publish with the first and read with the second")
	}
	if !strings.Contains(err.Error(), "application/json") {
		t.Fatalf("WithCodec() error = %v, want the shared content type named", err)
	}
}

func TestJSONRemainsRegisteredWithAlternateCodec(t *testing.T) {
	cfg := testClientConfig(t)
	marker := &registryTestCodec{decodeCalls: new(atomic.Int32), reject: true}
	client, runner, _ := newCodecSelectionRunner(t, cfg, WithCodec(marker))
	defer func() { _ = client.Close(context.Background()) }()

	var got struct {
		Value string `json:"value"`
	}
	var decodeErr error
	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(_ context.Context, event *Event) error {
			decodeErr = event.Decode(&got)
			return decodeErr
		}),
	}
	settler := &dispatchSettler{}
	message := codecSelectionMessage(t, codecSelectionEnvelope("application/json"), []byte(`{"value":"json"}`), settler)
	if !dispatchMessage(runner, context.Background(), message, &Envelope{}, new(bool), &deliveryState{}) {
		t.Fatal("JSON message was not settled")
	}
	if decodeErr != nil {
		t.Fatalf("JSON decode error = %v", decodeErr)
	}
	if got.Value != "json" {
		t.Fatalf("decoded value = %q, want json", got.Value)
	}
	if got := marker.decodeCalls.Load(); got != 0 {
		t.Fatalf("alternate codec decode calls = %d, want 0", got)
	}
}

func TestWithCodecUsesFirstForPublishAndRegistersAllForRead(t *testing.T) {
	first := &registryTestCodec{name: "first", contentType: "application/first"}
	second := &registryTestCodec{name: "second", contentType: "application/second", decodeCalls: new(atomic.Int32)}
	client, runner, producer := newCodecSelectionRunner(t, testClientConfig(t), WithCodec(first, second))
	defer func() { _ = client.Close(context.Background()) }()

	if _, err := client.Publisher().Publish(context.Background(), "orders.created.v1", map[string]string{"value": "published"}); err != nil {
		t.Fatalf("Publish() = %v", err)
	}
	if len(producer.messages) != 1 {
		t.Fatalf("published messages = %d, want 1", len(producer.messages))
	}
	if got := headerValue(producer.messages[0].Headers, "datacontenttype"); got != first.ContentType() {
		t.Fatalf("published content type = %q, want %q", got, first.ContentType())
	}

	var got struct {
		Value string `json:"value"`
	}
	var decodeErr error
	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(_ context.Context, event *Event) error {
			decodeErr = event.Decode(&got)
			return decodeErr
		}),
	}
	settler := &dispatchSettler{}
	message := codecSelectionMessage(t, codecSelectionEnvelope(second.ContentType()), []byte(`{"value":"second"}`), settler)
	if !dispatchMessage(runner, context.Background(), message, &Envelope{}, new(bool), &deliveryState{}) {
		t.Fatal("second-codec message was not settled")
	}
	if decodeErr != nil {
		t.Fatalf("second codec decode error = %v", decodeErr)
	}
	if got.Value != "second" {
		t.Fatalf("decoded value = %q, want second", got.Value)
	}
	if got := second.decodeCalls.Load(); got != 1 {
		t.Fatalf("second codec decode calls = %d, want 1", got)
	}
}
