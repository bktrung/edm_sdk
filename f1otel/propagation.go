package f1otel

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
)

var _ interface {
	InjectTrace(context.Context) (string, string)
} = (*Observer)(nil)

// InjectTrace serializes the span in ctx with the configured propagator. A nil
// or unconfigured propagator returns empty strings and never consults the otel
// global propagator.
func (o *Observer) InjectTrace(ctx context.Context) (traceParent, traceState string) {
	if o == nil || o.propagator == nil {
		return "", ""
	}
	if ctx == nil {
		return "", ""
	}
	carrier := propagation.MapCarrier{}
	o.propagator.Inject(ctx, carrier)
	return carrier.Get("traceparent"), carrier.Get("tracestate")
}
