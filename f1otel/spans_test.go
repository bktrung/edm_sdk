package f1otel

import (
	"context"
	"fmt"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestSpansUseEventTimesLinksAttributesAndStatus(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	observer, err := New(
		WithTracerProvider(provider),
		WithPropagator(propagation.TraceContext{}),
	)
	require.NoError(t, err)

	base := time.Unix(100, 0)
	publishCtx, publishToken := observer.Start(context.Background(), f1.StartEvent{
		Kind:        f1.ObserverPublish,
		At:          base,
		Topic:       "orders.created",
		Priority:    f1.PriorityHigh,
		Route:       f1.PublishRoutePrimary,
		Destination: "f1.test.orders.created.high",
		BatchSize:   2,
	})
	_, firstCreateToken := observer.Start(publishCtx, f1.StartEvent{
		Kind:        f1.ObserverMessageBuilt,
		At:          base.Add(time.Second),
		Topic:       "orders.created",
		Priority:    f1.PriorityHigh,
		Destination: "f1.test.orders.created.high",
	})
	_, secondCreateToken := observer.Start(publishCtx, f1.StartEvent{
		Kind:        f1.ObserverMessageBuilt,
		At:          base.Add(2 * time.Second),
		Topic:       "orders.created",
		Priority:    f1.PriorityHigh,
		Destination: "f1.test.orders.created.high",
	})
	observer.Finish(firstCreateToken, f1.FinishEvent{
		Kind:        f1.ObserverMessageBuilt,
		At:          base.Add(1500 * time.Millisecond),
		Topic:       "orders.created",
		Priority:    f1.PriorityHigh,
		Destination: "f1.test.orders.created.high",
		Outcome:     f1.ObserverOutcomeOK,
	})
	observer.Finish(secondCreateToken, f1.FinishEvent{
		Kind:        f1.ObserverMessageBuilt,
		At:          base.Add(2500 * time.Millisecond),
		Topic:       "orders.created",
		Priority:    f1.PriorityHigh,
		Destination: "f1.test.orders.created.high",
		Outcome:     f1.ObserverOutcomeOK,
	})
	observer.Finish(publishToken, f1.FinishEvent{
		Kind:    f1.ObserverPublish,
		At:      base.Add(3 * time.Second),
		Outcome: f1.ObserverOutcomeOK,
	})

	inboundTraceID, err := trace.TraceIDFromHex("11111111111111111111111111111111")
	require.NoError(t, err)
	inboundSpanID, err := trace.SpanIDFromHex("2222222222222222")
	require.NoError(t, err)
	inboundCtx := trace.ContextWithRemoteSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    inboundTraceID,
		SpanID:     inboundSpanID,
		TraceFlags: trace.FlagsSampled,
	}))

	processCtx, processToken := observer.Start(inboundCtx, f1.StartEvent{
		Kind:         f1.ObserverProcess,
		At:           base.Add(4 * time.Second),
		Topic:        "orders.created",
		Subscription: "orders",
		Priority:     f1.PriorityHigh,
		Attempt:      2,
		Destination:  "f1.test.orders.orders",
		TraceParent:  "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		TraceState:   "rojo=1",
	})
	processSpan := trace.SpanFromContext(processCtx)
	require.True(t, processSpan.SpanContext().IsValid())
	_, handlerSpan := provider.Tracer("test").Start(processCtx, "handler")
	handlerSpan.End(trace.WithTimestamp(base.Add(4500 * time.Millisecond)))
	observer.Finish(processToken, f1.FinishEvent{
		Kind:         f1.ObserverProcess,
		At:           base.Add(5 * time.Second),
		Topic:        "orders.created",
		Subscription: "orders",
		Priority:     f1.PriorityHigh,
		Attempt:      2,
		Destination:  "f1.test.orders.orders",
		Outcome:      f1.ObserverOutcomeError,
		ErrorClass:   f1.ErrorClassRetryable,
	})

	_, republishToken := observer.Start(processCtx, f1.StartEvent{
		Kind:        f1.ObserverPublish,
		At:          base.Add(6 * time.Second),
		Topic:       "orders.created",
		Priority:    f1.PriorityHigh,
		Attempt:     2,
		Route:       f1.PublishRouteRetry,
		Destination: "f1.test.orders.retry.1",
		BatchSize:   1,
	})
	observer.Finish(republishToken, f1.FinishEvent{
		Kind:    f1.ObserverPublish,
		At:      base.Add(7 * time.Second),
		Outcome: f1.ObserverOutcomeOK,
	})

	_, settleToken := observer.Start(context.Background(), f1.StartEvent{
		Kind:        f1.ObserverSettle,
		At:          base.Add(8 * time.Second),
		Destination: "f1.test.orders.orders",
		Operation:   f1.SettleAck,
	})
	observer.Finish(settleToken, f1.FinishEvent{
		Kind:    f1.ObserverSettle,
		At:      base.Add(9 * time.Second),
		Outcome: f1.ObserverOutcomeOK,
	})

	spans := exporter.GetSpans()
	require.Len(t, spans, 7)

	publish := spanNamed(t, spans, "send orders.created", 0)
	require.Equal(t, trace.SpanKindProducer, publish.SpanKind)
	require.Equal(t, base, publish.StartTime)
	require.Equal(t, base.Add(3*time.Second), publish.EndTime)
	require.Equal(t, "primary", spanAttributes(publish)["f1.route"])
	require.Equal(t, "high", spanAttributes(publish)["f1.priority"])
	require.Equal(t, "f1.test.orders.created.high", spanAttributes(publish)["f1.destination"])
	require.Len(t, publish.Links, 2)

	create := spanNamed(t, spans, "create orders.created", 0)
	require.Equal(t, trace.SpanKindProducer, create.SpanKind)
	require.Equal(t, base.Add(time.Second), create.StartTime)
	require.Equal(t, base.Add(1500*time.Millisecond), create.EndTime)
	require.Equal(t, publish.SpanContext, create.Parent)
	require.Equal(t, "high", spanAttributes(create)["f1.priority"])
	require.Equal(t, "f1.test.orders.created.high", spanAttributes(create)["f1.destination"])

	process := spanNamed(t, spans, "process orders.created", 0)
	require.Equal(t, trace.SpanKindConsumer, process.SpanKind)
	require.False(t, process.Parent.IsValid())
	traceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	require.NoError(t, err)
	require.Equal(t, traceID, process.Links[0].SpanContext.TraceID())
	require.Equal(t, base.Add(4*time.Second), process.StartTime)
	require.Equal(t, base.Add(5*time.Second), process.EndTime)
	require.Equal(t, codes.Error, process.Status.Code)
	require.Equal(t, "f1_retryable", spanAttributes(process)["error.type"])
	require.Equal(t, "2", spanAttributes(process)["f1.attempt"])

	republish := spanNamed(t, spans, "send orders.created", 1)
	require.Equal(t, trace.SpanKindProducer, republish.SpanKind)
	require.False(t, republish.Parent.IsValid())
	require.Len(t, republish.Links, 1)
	require.Equal(t, process.SpanContext, republish.Links[0].SpanContext)
	require.Equal(t, "retry", spanAttributes(republish)["f1.route"])
	require.Equal(t, "f1.test.orders.retry.1", spanAttributes(republish)["f1.destination"])

	settle := spanNamed(t, spans, "settle", 0)
	require.Equal(t, trace.SpanKindClient, settle.SpanKind)
	require.Equal(t, base.Add(8*time.Second), settle.StartTime)
	require.Equal(t, base.Add(9*time.Second), settle.EndTime)
	require.Equal(t, "ack", spanAttributes(settle)["messaging.operation.type"])
}

func TestPrimaryPublishSpanNameUsesUniformFinishTopic(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	observer, err := New(WithTracerProvider(provider), WithCreateSpans(true))
	require.NoError(t, err)

	base := time.Unix(100, 0)
	publishCtx, publishToken := observer.Start(context.Background(), f1.StartEvent{
		Kind:      f1.ObserverPublish,
		At:        base,
		Route:     f1.PublishRoutePrimary,
		BatchSize: 2,
	})
	_, firstCreateToken := observer.Start(publishCtx, f1.StartEvent{
		Kind:  f1.ObserverMessageBuilt,
		At:    base.Add(time.Second),
		Topic: "orders.created",
	})
	_, secondCreateToken := observer.Start(publishCtx, f1.StartEvent{
		Kind:  f1.ObserverMessageBuilt,
		At:    base.Add(2 * time.Second),
		Topic: "orders.created",
	})
	observer.Finish(firstCreateToken, f1.FinishEvent{
		Kind: f1.ObserverMessageBuilt,
		At:   base.Add(1500 * time.Millisecond),
	})
	observer.Finish(secondCreateToken, f1.FinishEvent{
		Kind: f1.ObserverMessageBuilt,
		At:   base.Add(2500 * time.Millisecond),
	})
	observer.Finish(publishToken, f1.FinishEvent{
		Kind:  f1.ObserverPublish,
		At:    base.Add(3 * time.Second),
		Topic: "orders.created",
	})

	spans := exporter.GetSpans()
	require.Len(t, spans, 3)
	require.Equal(t, "send orders.created", spanNamed(t, spans, "send orders.created", 0).Name)
}

func TestPrimaryPublishSpanNameStaysBareForMixedTopics(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	observer, err := New(WithTracerProvider(provider), WithCreateSpans(true))
	require.NoError(t, err)

	base := time.Unix(100, 0)
	publishCtx, publishToken := observer.Start(context.Background(), f1.StartEvent{
		Kind:      f1.ObserverPublish,
		At:        base,
		Route:     f1.PublishRoutePrimary,
		BatchSize: 2,
	})
	_, firstCreateToken := observer.Start(publishCtx, f1.StartEvent{
		Kind:  f1.ObserverMessageBuilt,
		At:    base.Add(time.Second),
		Topic: "orders.created",
	})
	_, secondCreateToken := observer.Start(publishCtx, f1.StartEvent{
		Kind:  f1.ObserverMessageBuilt,
		At:    base.Add(2 * time.Second),
		Topic: "invoices.created",
	})
	observer.Finish(firstCreateToken, f1.FinishEvent{
		Kind: f1.ObserverMessageBuilt,
		At:   base.Add(1500 * time.Millisecond),
	})
	observer.Finish(secondCreateToken, f1.FinishEvent{
		Kind: f1.ObserverMessageBuilt,
		At:   base.Add(2500 * time.Millisecond),
	})
	observer.Finish(publishToken, f1.FinishEvent{
		Kind: f1.ObserverPublish,
		At:   base.Add(3 * time.Second),
	})

	spans := exporter.GetSpans()
	require.Len(t, spans, 3)
	require.Equal(t, "send", spanNamed(t, spans, "send", 0).Name)
}

func TestSpanAttributesUseOneConsumerGroup(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	observer, err := New(WithTracerProvider(provider))
	require.NoError(t, err)

	start := f1.StartEvent{
		Kind:          f1.ObserverProcess,
		At:            time.Unix(100, 0),
		Topic:         "orders.created",
		Subscription:  "orders",
		ConsumerGroup: "orders-workers",
	}
	rawAttrs := observer.spanAttributes(start)
	rawCount := 0
	for _, attr := range rawAttrs {
		if string(attr.Key) == "messaging.consumer.group.name" {
			rawCount++
		}
	}
	require.Equal(t, 1, rawCount)

	_, token := observer.Start(context.Background(), start)
	observer.Finish(token, f1.FinishEvent{
		Kind:          f1.ObserverProcess,
		At:            time.Unix(101, 0),
		Topic:         "orders.created",
		Subscription:  "orders",
		ConsumerGroup: "orders-workers",
		Outcome:       f1.ObserverOutcomeOK,
	})

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	process := spans[0]
	count := 0
	for _, attr := range process.Attributes {
		if string(attr.Key) == "messaging.consumer.group.name" {
			count++
		}
	}
	require.Equal(t, 1, count)
	require.Equal(t, "orders-workers", spanAttributes(process)["messaging.consumer.group.name"])
}

func TestCreateSpansCanBeDisabledAndOrphanFinishDoesNotLeak(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	observer, err := New(WithTracerProvider(provider), WithCreateSpans(false))
	require.NoError(t, err)
	ctx := context.Background()
	gotCtx, token := observer.Start(ctx, f1.StartEvent{
		Kind:  f1.ObserverMessageBuilt,
		At:    time.Unix(100, 0),
		Topic: "orders.created",
	})
	require.Equal(t, ctx, gotCtx)
	require.Zero(t, token.Handle)
	observer.Finish(token, f1.FinishEvent{Kind: f1.ObserverMessageBuilt, At: time.Unix(101, 0)})
	observer.Finish(f1.Token{Kind: f1.ObserverPublish, Handle: 999}, f1.FinishEvent{
		Kind:    f1.ObserverPublish,
		At:      time.Unix(101, 0),
		Outcome: f1.ObserverOutcomeOK,
	})
	require.Empty(t, exporter.GetSpans())
}

func TestUnknownSpanErrorClassUsesOther(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	observer, err := New(WithTracerProvider(provider))
	require.NoError(t, err)
	_, token := observer.Start(context.Background(), f1.StartEvent{
		Kind:  f1.ObserverProcess,
		At:    time.Unix(100, 0),
		Topic: "orders.created",
	})
	observer.Finish(token, f1.FinishEvent{
		Kind:       f1.ObserverProcess,
		At:         time.Unix(101, 0),
		Outcome:    f1.ObserverOutcomeError,
		ErrorClass: f1.ErrorClass("future_error"),
	})
	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	require.Equal(t, "_OTHER", spanAttributes(spans[0])["error.type"])
	require.Equal(t, codes.Error, spans[0].Status.Code)
}

func spanNamed(t *testing.T, spans tracetest.SpanStubs, name string, occurrence int) tracetest.SpanStub {
	t.Helper()
	seen := 0
	for _, span := range spans {
		if span.Name == name {
			if seen == occurrence {
				return span
			}
			seen++
		}
	}
	t.Fatalf("span %q occurrence %d not found", name, occurrence)
	return tracetest.SpanStub{}
}

func spanAttributes(span tracetest.SpanStub) map[string]string {
	attrs := make(map[string]string, len(span.Attributes))
	for _, kv := range span.Attributes {
		attrs[string(kv.Key)] = fmt.Sprint(kv.Value.AsInterface())
	}
	return attrs
}

func TestTraceOnlyObserverPutsTheEndpointOnSpans(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	observer, err := New(WithTracerProvider(provider))
	require.NoError(t, err)

	base := time.Unix(100, 0)
	observer.Record(f1.PointEvent{Kind: f1.ObserverDriverSelected, At: base, DriverName: "kafka", ServerAddress: "127.0.0.1", ServerPort: 9092})
	_, token := observer.Start(context.Background(), f1.StartEvent{Kind: f1.ObserverPublish, At: base, Topic: "orders.created", Priority: f1.PriorityHigh})
	observer.Finish(token, f1.FinishEvent{Kind: f1.ObserverPublish, At: base.Add(time.Second), Topic: "orders.created", Priority: f1.PriorityHigh, Outcome: f1.ObserverOutcomeOK})

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	attrs := map[string]string{}
	for _, attr := range spans[0].Attributes {
		attrs[string(attr.Key)] = attr.Value.String()
	}
	require.Equal(t, "127.0.0.1", attrs["server.address"], "a trace-only observer must still carry the broker endpoint")
	require.Equal(t, "9092", attrs["server.port"])
}
