// Package f1otel adapts F1 observer events to OpenTelemetry metrics and spans.
//
// The package uses application-owned metric and trace providers and never
// creates an exporter. A missing provider, the zero Observer, and a nil
// *Observer are no-ops. Metrics and spans are safe for concurrent calls.
// Histogram instruments use event timestamps and do not call the wall clock.
// Applications should provide a view for the duration histograms with seconds
// buckets such as 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5,
// 5, 10, 30, and 60; f1otel does not install a view.
package f1otel
