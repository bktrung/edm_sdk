package obs

import "sync"

// MetricDefinition describes one metric family's name and label keys.
//
// Instrumentation registers definitions before producing measurements. The
// cardinality check consumes this registry so a new label cannot enter the
// telemetry surface without an explicit budget entry.
type MetricDefinition struct {
	Name   string
	Labels []string
}

var (
	registryMu sync.RWMutex
	registry   []MetricDefinition
)

// Register the first M1 family here so the CI check is linked to a real
// declaration. This records a family and its bounded labels; it does not
// emit a measurement.
func init() {
	RegisterMetric("f1_capability_info", "feature", "mode")
}

// RegisterMetric records a metric family for validation and instrumentation.
// Registrations must use stable metric names and bounded label keys.
func RegisterMetric(name string, labels ...string) {
	copyLabels := append([]string(nil), labels...)
	registryMu.Lock()
	registry = append(registry, MetricDefinition{Name: name, Labels: copyLabels})
	registryMu.Unlock()
}

// MetricDefinitions returns a snapshot of all registered metric families.
func MetricDefinitions() []MetricDefinition {
	registryMu.RLock()
	defer registryMu.RUnlock()
	definitions := make([]MetricDefinition, len(registry))
	for i, definition := range registry {
		definitions[i] = MetricDefinition{
			Name:   definition.Name,
			Labels: append([]string(nil), definition.Labels...),
		}
	}
	return definitions
}
