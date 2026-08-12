package f1

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestChain_BuiltInOrderIsFixed(t *testing.T) {
	t.Parallel()
	got := make([]string, 0, len(builtInMiddlewareSpecs()))
	for _, spec := range builtInMiddlewareSpecs() {
		got = append(got, spec.name)
	}
	want := []string{"recover", "tracing", "metrics", "logging", "timeout", "retry-classify"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("built-in middleware = %v, want %v", got, want)
	}
}

func TestChain_UserMiddlewareRunsInsideTheBuiltIns(t *testing.T) {
	producer := &dispatchProducer{}
	order := make([]string, 0, 4)
	sawRaw := false
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&dispatchDriver{conn: &dispatchConn{producer: producer}}),
		WithMiddleware(traceMiddleware("a", &order, nil)),
		WithMiddleware(traceMiddleware("b", &order, func(err error) error {
			sawRaw = errors.Is(err, errRawMiddleware)
			return Terminal(err)
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()

	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:   "orders",
		Topics: []string{"orders.created"},
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error {
				return errRawMiddleware
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope := Envelope{SpecVersion: "1.0", ID: "evt-middleware", Source: "/test/orders", Type: "orders.created", Attempt: 1}
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	settler := &dispatchSettler{}
	if !dispatchMessage(runner, context.Background(), driver.InboundMessage{
		Destination: "orders",
		Headers:     headerSlice(headers),
		Body:        []byte(`{}`),
		Settle:      settler,
	}, &Envelope{}, new(bool)) {
		t.Fatal("middleware-classified message was not settled")
	}
	if !settler.acked {
		t.Fatal("middleware-classified message was not acked after DLQ publish")
	}
	if len(producer.messages) != 1 || headerValue(producer.messages[0].Headers, "f1deathreason") != ReasonTerminal.String() {
		t.Fatalf("DLQ messages = %#v, want one terminal message", producer.messages)
	}
	if !sawRaw {
		t.Fatal("inner user middleware did not observe the raw handler error")
	}
	if want := []string{"a:in", "b:in", "b:out", "a:out"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("user middleware order = %v, want %v", order, want)
	}
}

func TestChain_UserMiddlewarePanicIsRecovered(t *testing.T) {
	producer := &dispatchProducer{}
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&dispatchDriver{conn: &dispatchConn{producer: producer}}),
		WithMiddleware(func(next Handler) Handler {
			return HandlerFunc(func(context.Context, *Event) error {
				panic("middleware boom")
			})
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()

	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:   "orders",
		Topics: []string{"orders.created"},
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error { return nil }),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope := Envelope{SpecVersion: "1.0", ID: "evt-middleware-panic", Source: "/test/orders", Type: "orders.created", Attempt: 1}
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	settler := &dispatchSettler{}
	if !dispatchMessage(runner, context.Background(), driver.InboundMessage{
		Destination: "orders",
		Headers:     headerSlice(headers),
		Body:        []byte(`{}`),
		Settle:      settler,
	}, &Envelope{}, new(bool)) {
		t.Fatal("panicking middleware message was not settled")
	}
	if !settler.acked {
		t.Fatal("panicking middleware message was not acked after DLQ publish")
	}
	if len(producer.messages) != 1 || headerValue(producer.messages[0].Headers, "f1deathreason") != ReasonPanic.String() {
		t.Fatalf("DLQ messages = %#v, want one panic message", producer.messages)
	}
	if !strings.Contains(headerValue(producer.messages[0].Headers, "f1deatherror"), "middleware boom") {
		t.Fatalf("panic error header = %q, want middleware panic", headerValue(producer.messages[0].Headers, "f1deatherror"))
	}
}

func TestChain_UserMiddlewareCanClassifyUnavailable(t *testing.T) {
	producer := &dispatchProducer{}
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&dispatchDriver{conn: &dispatchConn{producer: producer}}),
		WithMiddleware(func(next Handler) Handler {
			return HandlerFunc(func(ctx context.Context, event *Event) error {
				return Unavailable(next.Handle(ctx, event))
			})
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()

	runner, err := client.Subscribe(context.Background(), Subscription{
		Name: "orders", MaxDeferrals: 24,
		Topics: []string{"orders.created"},
		Retry:  RetryConfig{MaxAttempts: 3, InitialInterval: time.Second, Multiplier: 2, MaxInterval: time.Second},
		Handlers: map[string]Handler{
			"orders.created": HandlerFunc(func(context.Context, *Event) error { return errors.New("dependency unavailable") }),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope := Envelope{SpecVersion: "1.0", ID: "evt-middleware-unavailable", Source: "/test/orders", Type: "orders.created", Attempt: 1}
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	settler := &dispatchSettler{}
	if !dispatchMessage(runner, context.Background(), driver.InboundMessage{
		Destination: "orders",
		Headers:     headerSlice(headers),
		Body:        []byte(`{}`),
		Settle:      settler,
	}, &Envelope{}, new(bool)) {
		t.Fatal("middleware-classified unavailable message was not settled")
	}
	if !settler.acked {
		t.Fatal("middleware-classified unavailable message was not acked")
	}
	if len(producer.messages) != 1 || strings.Contains(producer.messages[0].Destination, ".dlq.") || !strings.Contains(producer.messages[0].Destination, ".retry.") {
		t.Fatalf("successor destination = %q, want retry destination", producer.messages[0].Destination)
	}
	successor, err := DecodeHeaders(inboundHeaders(producer.messages[0].Headers))
	if err != nil {
		t.Fatalf("decode successor headers: %v", err)
	}
	if successor.Deferrals != 1 || successor.Attempt != 1 {
		t.Fatalf("successor envelope = deferrals %d, attempt %d; want deferrals 1 and attempt 1", successor.Deferrals, successor.Attempt)
	}
}

var errRawMiddleware = errors.New("raw handler error")

func traceMiddleware(name string, order *[]string, classify func(error) error) Middleware {
	return func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, event *Event) error {
			*order = append(*order, name+":in")
			err := next.Handle(ctx, event)
			if classify != nil {
				err = classify(err)
			}
			*order = append(*order, name+":out")
			return err
		})
	}
}
