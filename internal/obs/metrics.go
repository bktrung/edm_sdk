package obs

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const meterName = "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/obs"

type metricSource struct {
	subscription      string
	divergence        func() map[string]int64
	stuckWorkers      func() int64
	retryAfterClamped func() map[string]uint64
}

// Metrics owns the observable instruments and their collection callback.
// Runtime sources provide sampled values without adding a goroutine to the
// client lifecycle.
type Metrics struct {
	mu           sync.RWMutex
	sources      map[uint64]metricSource
	nextID       uint64
	closed       bool
	registration metric.Registration
	divergence   metric.Int64ObservableGauge
	stuck        metric.Int64ObservableGauge
	clamped      metric.Int64ObservableCounter
	reconnects   metric.Int64Counter
}

// NewMetrics creates the SDK metric publisher. A nil provider leaves the
// publisher inactive so delivery does not need a nil check on every sample.
func NewMetrics(provider metric.MeterProvider) (*Metrics, error) {
	metrics := &Metrics{sources: make(map[uint64]metricSource)}
	if provider == nil {
		return metrics, nil
	}
	meter := provider.Meter(meterName)
	divergence, err := meter.Int64ObservableGauge(
		"f1_attempt_divergence",
		metric.WithDescription("Current absolute gap between handler attempts and broker delivery count."),
	)
	if err != nil {
		return nil, err
	}
	stuck, err := meter.Int64ObservableGauge(
		"f1_stuck_workers",
		metric.WithDescription("Workers that have exceeded twice the configured handler timeout."),
	)
	if err != nil {
		return nil, err
	}
	clamped, err := meter.Int64ObservableCounter(
		"f1_retry_after_clamped_total",
		metric.WithDescription("Retry-After requests clamped into a retry tier jitter band."),
	)
	if err != nil {
		return nil, err
	}
	reconnects, err := meter.Int64Counter(
		"f1_driver_reconnects_total",
		metric.WithDescription("Client connection reconnection outcomes."),
	)
	if err != nil {
		return nil, err
	}
	metrics.divergence = divergence
	metrics.stuck = stuck
	metrics.clamped = clamped
	metrics.reconnects = reconnects
	registration, err := meter.RegisterCallback(metrics.observe, divergence, stuck, clamped)
	if err != nil {
		return nil, err
	}
	metrics.registration = registration
	return metrics, nil
}

// RecordReconnect records one completed reconnect outcome.
func (m *Metrics) RecordReconnect(ctx context.Context, driverName, reason string) {
	if m == nil || m.reconnects == nil {
		return
	}
	if ctx == nil {
		return
	}
	m.reconnects.Add(ctx, 1, metric.WithAttributes(
		attribute.String("driver", driverName),
		attribute.String("reason", reason),
	))
}

// Register connects one subscription's sampled sources to the collection
// callback and returns an idempotent unregister function.
func (m *Metrics) Register(subscription string, divergence func() map[string]int64, stuckWorkers func() int64, retryAfterClamped func() map[string]uint64) func() {
	if m == nil {
		return func() {}
	}
	m.mu.Lock()
	if m.closed || m.registration == nil {
		m.mu.Unlock()
		return func() {}
	}
	m.nextID++
	id := m.nextID
	clamped := retryAfterClamped
	m.sources[id] = metricSource{subscription: subscription, divergence: divergence, stuckWorkers: stuckWorkers, retryAfterClamped: clamped}
	m.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			m.mu.Lock()
			delete(m.sources, id)
			m.mu.Unlock()
		})
	}
}

// Close unregisters the collection callback. It is safe to call repeatedly.
func (m *Metrics) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.sources = nil
	registration := m.registration
	m.registration = nil
	m.mu.Unlock()
	if registration == nil {
		return nil
	}
	return registration.Unregister()
}

func (m *Metrics) observe(_ context.Context, observer metric.Observer) error {
	m.mu.RLock()
	sources := make([]metricSource, 0, len(m.sources))
	for _, source := range m.sources {
		sources = append(sources, source)
	}
	divergence := m.divergence
	stuck := m.stuck
	clamped := m.clamped
	m.mu.RUnlock()

	divergenceValues := make(map[string]int64)
	stuckValues := make(map[string]int64, len(sources))
	type clampKey struct {
		topic        string
		subscription string
	}
	clampedValues := make(map[clampKey]uint64, len(sources))
	for _, source := range sources {
		if source.divergence != nil {
			for topic, value := range source.divergence() {
				if current, ok := divergenceValues[topic]; !ok || value > current {
					divergenceValues[topic] = value
				}
			}
		}
		value := int64(0)
		if source.stuckWorkers != nil {
			value = source.stuckWorkers()
		}
		stuckValues[source.subscription] += value
		if source.retryAfterClamped != nil {
			for topic, value := range source.retryAfterClamped() {
				clampedValues[clampKey{topic: topic, subscription: source.subscription}] += value
			}
		}
	}
	for topic, value := range divergenceValues {
		observer.ObserveInt64(divergence, value, metric.WithAttributes(attribute.String("topic", topic)))
	}
	for subscription, value := range stuckValues {
		observer.ObserveInt64(stuck, value, metric.WithAttributes(attribute.String("subscription", subscription)))
	}
	const maxMetricInt64 = uint64(1<<63 - 1)
	for key, value := range clampedValues {
		if value > maxMetricInt64 {
			value = maxMetricInt64
		}
		observer.ObserveInt64(clamped, int64(value), metric.WithAttributes(
			attribute.String("topic", key.topic),
			attribute.String("subscription", key.subscription),
		))
	}
	return nil
}
