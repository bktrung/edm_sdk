// Package f1otel adapts F1 observer events to OpenTelemetry metrics and spans.
//
// The package uses application-owned metric and trace providers and never
// creates an exporter. The zero Observer and a nil *Observer record no
// telemetry. Metrics and spans are configured independently with
// [WithMeterProvider] and [WithTracerProvider]; propagation is configured
// separately with [WithPropagator]. Metrics and spans are safe for concurrent
// calls. Histogram instruments use event timestamps and do not call the wall
// clock. Applications should provide a view for the duration histograms with
// seconds buckets such as 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
// 2.5, 5, 10, 30, and 60; f1otel does not install a view.
//
// # One Observer per Client
//
// Create one f1otel.New per Client and share the OpenTelemetry providers. An
// Observer reports the broker endpoint of the Client it is attached to, so it
// attaches to one live Client at a time: f1.New refuses a non-nil Observer,
// including the zero value, that another live Client already holds, before
// opening anything. A failed f1.New releases the Observer again, and so does a
// Close that completes shutdown; a Close that fails and leaves the Client
// retryable keeps it. Two Clients reporting through the same providers look
// like this:
//
//	observerA, err := f1otel.New(f1otel.WithMeterProvider(mp), f1otel.WithTracerProvider(tp))
//	// Handle err; attach observerA only to client A.
//	observerB, err := f1otel.New(f1otel.WithMeterProvider(mp), f1otel.WithTracerProvider(tp))
//	// Handle err; attach observerB only to client B. Providers remain application-owned.
//
// The binding is enforced through [Observer.BindClient]. An application
// Observer that wraps an f1otel Observer must forward BindClient to it; a
// wrapper that does not is not checked, and sharing it relabels one Client's
// telemetry with another Client's endpoint.
package f1otel
