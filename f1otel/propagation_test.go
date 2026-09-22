package f1otel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestInjectTraceUsesConfiguredPropagator(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	ctx, span := provider.Tracer("test").Start(context.Background(), "send")
	defer span.End()

	observer, err := New(WithTracerProvider(provider), WithPropagator(propagation.TraceContext{}))
	require.NoError(t, err)
	traceParent, traceState := observer.InjectTrace(ctx)
	require.Regexp(t, `^00-[0-9a-f]{32}-[0-9a-f]{16}-01$`, traceParent)
	require.Empty(t, traceState)

	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	require.Equal(t, carrier.Get("traceparent"), traceParent)
}

func TestInjectTraceDoesNotUseGlobalPropagatorWhenUnconfigured(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(markerPropagator{})
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })

	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	ctx, span := provider.Tracer("test").Start(context.Background(), "send")
	defer span.End()

	observer, err := New(WithTracerProvider(provider))
	require.NoError(t, err)
	traceParent, traceState := observer.InjectTrace(ctx)
	require.Empty(t, traceParent)
	require.Empty(t, traceState)
}

type markerPropagator struct{}

func (markerPropagator) Inject(context.Context, propagation.TextMapCarrier) {
	panic("global propagator was used")
}

func (markerPropagator) Extract(ctx context.Context, _ propagation.TextMapCarrier) context.Context {
	return ctx
}

func (markerPropagator) Fields() []string { return []string{"marker"} }
