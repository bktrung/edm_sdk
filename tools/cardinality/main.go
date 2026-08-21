// Command cardinality validates the label keys used by the SDK metric registry.
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	_ "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/obs"
)

type labelBudget struct {
	max       int
	metricSet map[string]bool
}

var budgets = map[string]labelBudget{
	"topic":        {max: 50},
	"type":         {max: 200},
	"subscription": {max: 30},
	"priority":     {max: 3},
	"tier":         {max: 6},
	"reason":       {max: 8, metricSet: map[string]bool{"f1_retry_total": true, "f1_deadletter_total": true, "f1_driver_reconnects_total": true}},
	"driver":       {max: 8},
	"partition":    {max: 100, metricSet: map[string]bool{"f1_consumer_lag": true}},
	"feature":      {max: 16, metricSet: map[string]bool{"f1_capability_info": true}},
	"mode":         {max: 3, metricSet: map[string]bool{"f1_capability_info": true}},
}

var forbiddenLabels = map[string]bool{
	"event_id":        true,
	"idempotency_key": true,
	"partition_key":   true,
	"correlation_id":  true,
}

func main() {
	flag.Parse()
	definitions := obs.MetricDefinitions()
	violations := validate(definitions)
	if len(violations) != 0 {
		for _, violation := range violations {
			fmt.Fprintln(os.Stderr, violation)
		}
		os.Exit(1)
	}
	labels := make(map[string]bool)
	for _, definition := range definitions {
		for _, label := range definition.Labels {
			labels[label] = true
		}
	}
	_, _ = fmt.Fprintf(os.Stdout, "cardinality: checked %d metric registrations and %d label keys\n", len(definitions), len(labels))
}

func validate(definitions []obs.MetricDefinition) []string {
	if len(definitions) == 0 {
		return []string{"metric registry contains no registrations"}
	}
	var violations []string
	seenMetrics := make(map[string]bool, len(definitions))
	for _, definition := range definitions {
		if definition.Name == "" {
			violations = append(violations, "metric registration has an empty name")
			continue
		}
		if seenMetrics[definition.Name] {
			violations = append(violations, fmt.Sprintf("metric %q is registered more than once", definition.Name))
		}
		seenMetrics[definition.Name] = true
		seenLabels := make(map[string]bool, len(definition.Labels))
		for _, label := range definition.Labels {
			if seenLabels[label] {
				violations = append(violations, fmt.Sprintf("metric %q repeats label %q", definition.Name, label))
			}
			seenLabels[label] = true
			if isForbidden(label) {
				violations = append(violations, fmt.Sprintf("metric %q uses forbidden label %q", definition.Name, label))
				continue
			}
			budget, ok := budgets[label]
			if !ok {
				violations = append(violations, fmt.Sprintf("metric %q label %q has no cardinality budget", definition.Name, label))
				continue
			}
			if len(budget.metricSet) != 0 && !budget.metricSet[definition.Name] {
				violations = append(violations, fmt.Sprintf("metric %q uses label %q outside its restricted family", definition.Name, label))
			}
		}
	}
	sort.Strings(violations)
	return violations
}

func isForbidden(label string) bool {
	if forbiddenLabels[label] {
		return true
	}
	lower := strings.ToLower(label)
	if strings.Contains(lower, "customer") || strings.Contains(lower, "tenant") || strings.Contains(lower, "account") || strings.Contains(lower, "user") || strings.Contains(lower, "correlation") || strings.Contains(lower, "idempotency") {
		return true
	}
	return strings.Contains(lower, "error") && strings.Contains(lower, "message")
}
