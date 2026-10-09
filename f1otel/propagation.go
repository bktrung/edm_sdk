package f1otel

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
)

var _ interface {
	InjectTrace(context.Context) (string, string)
} = (*Observer)(nil)

// InjectTrace returns the traceparent and tracestate values produced by the
// configured propagator for ctx. It returns empty strings for a nil receiver,
// nil context, or missing propagator, and never uses the global propagator.
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
