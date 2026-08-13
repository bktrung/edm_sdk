package obs

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const meterName = "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/obs"

type metricSource struct {
	subscription string
	divergence   func() map[string]int64
	stuckWorkers func() int64
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
	metrics.divergence = divergence
	metrics.stuck = stuck
	registration, err := meter.RegisterCallback(metrics.observe, divergence, stuck)
	if err != nil {
		return nil, err
	}
	metrics.registration = registration
	return metrics, nil
}

// Register connects one subscription's sampled sources to the collection
// callback and returns an idempotent unregister function.
func (m *Metrics) Register(subscription string, divergence func() map[string]int64, stuckWorkers func() int64) func() {
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
	m.sources[id] = metricSource{subscription: subscription, divergence: divergence, stuckWorkers: stuckWorkers}
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
	m.mu.RUnlock()

	divergenceValues := make(map[string]int64)
	stuckValues := make(map[string]int64, len(sources))
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
	}
	for topic, value := range divergenceValues {
		observer.ObserveInt64(divergence, value, metric.WithAttributes(attribute.String("topic", topic)))
	}
	for subscription, value := range stuckValues {
		observer.ObserveInt64(stuck, value, metric.WithAttributes(attribute.String("subscription", subscription)))
	}
	return nil
}
