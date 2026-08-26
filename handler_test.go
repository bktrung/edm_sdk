package f1

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestBuildHandlerChainRecoversPanicDirectly(t *testing.T) {
	t.Parallel()
	chain := buildHandlerChain(nil, HandlerFunc(func(context.Context, *Event) error {
		panic("direct chain panic")
	}))

	err := chain.Handle(context.Background(), &Event{})
	var panicErr *handlerPanicError
	if !errors.As(err, &panicErr) {
		t.Fatalf("buildHandlerChain() error = %T %v, want *handlerPanicError", err, err)
	}
	if !strings.Contains(panicErr.Error(), "direct chain panic") {
		t.Fatalf("panic error = %q, want panic value", panicErr)
	}
}

func TestChain_UserMiddlewareOrderIsPreserved(t *testing.T) {
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
	const marker = "middleware panic marker"
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&dispatchDriver{conn: &dispatchConn{producer: producer}}),
		WithMiddleware(func(next Handler) Handler {
			return HandlerFunc(func(context.Context, *Event) error {
				panic(marker + strings.Repeat("x", 16<<10))
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
	deathError := headerValue(producer.messages[0].Headers, "f1deatherror")
	if !strings.HasPrefix(deathError, "handler panic: "+marker) || len(deathError) > 4<<10 {
		t.Fatalf("panic error header = %q, want marker prefix and <= 4 KiB", deathError)
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
