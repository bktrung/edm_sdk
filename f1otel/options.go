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

// WithTracerProvider configures the provider used to create spans. A nil
// provider disables span creation.
func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(cfg *config) { cfg.tracerProvider = provider }
}

// WithPropagator configures trace context injection and extraction. A nil
// propagator disables propagation.
func WithPropagator(propagator propagation.TextMapPropagator) Option {
	return func(cfg *config) { cfg.propagator = propagator }
}

// WithCreateSpans controls whether the Observer creates spans for
// message_built events. It defaults to true and does not affect other span kinds.
func WithCreateSpans(enabled bool) Option {
	return func(cfg *config) { cfg.createSpans = enabled }
}
