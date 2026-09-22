package f1otel

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Option configures an Observer before New returns it. A nil Option is ignored.
type Option func(*config)

type config struct {
	meterProvider  metric.MeterProvider
	tracerProvider trace.TracerProvider
	propagator     propagation.TextMapPropagator
	createSpans    bool
}

// WithMeterProvider supplies the application-owned provider for metrics. A nil
// provider disables metrics and does not select the OpenTelemetry global.
func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(cfg *config) { cfg.meterProvider = provider }
}

// WithTracerProvider stores the application-owned provider that New uses to
// create the Observer's tracer. If it is nil or unset, the Observer creates
// no spans.
func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(cfg *config) { cfg.tracerProvider = provider }
}

// WithPropagator stores the application-owned propagator that New uses to
// inject and extract trace context. If it is nil or unset, propagation is
// disabled.
func WithPropagator(propagator propagation.TextMapPropagator) Option {
	return func(cfg *config) { cfg.propagator = propagator }
}

// WithCreateSpans stores whether Observer.Start may create the message_built
// span when a tracer is configured. It defaults to true; publish, process, and
// settle spans are unaffected.
func WithCreateSpans(enabled bool) Option {
	return func(cfg *config) { cfg.createSpans = enabled }
}
