package main

import (
	"strings"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/obs"
)

func TestValidateAcceptsBoundedMetricLabels(t *testing.T) {
	violations := validate([]obs.MetricDefinition{
		{Name: "f1_retry_total", Labels: []string{"topic", "subscription", "tier", "reason"}},
		{Name: "f1_consumer_lag", Labels: []string{"topic", "priority", "partition"}},
		{Name: "f1_capability_info", Labels: []string{"feature", "mode"}},
	})
	if len(violations) != 0 {
		t.Fatalf("valid metric labels rejected: %v", violations)
	}
}

func TestValidateRejectsEmptyRegistry(t *testing.T) {
	violations := validate(nil)
	if len(violations) != 1 || violations[0] != "metric registry contains no registrations" {
		t.Fatalf("violations=%v, want empty-registry failure", violations)
	}
}

func TestValidateRejectsUnbudgetedAndForbiddenLabels(t *testing.T) {
	violations := validate([]obs.MetricDefinition{{
		Name:   "f1_publish_total",
		Labels: []string{"topic", "request_id", "customer_id", "error_message"},
	}})
	joined := strings.Join(violations, "\n")
	for _, want := range []string{"label \"request_id\" has no cardinality budget", "customer_id", "error_message"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("violations=%v, missing %q", violations, want)
		}
	}
}

func TestValidateRejectsRestrictedLabelsOutsideTheirFamily(t *testing.T) {
	violations := validate([]obs.MetricDefinition{{
		Name:   "f1_retry_total",
		Labels: []string{"partition", "feature", "mode"},
	}})
	if len(violations) != 3 {
		t.Fatalf("violations=%v, want three restricted-label errors", violations)
	}
}

func TestValidateRejectsDuplicateMetricAndLabelRegistrations(t *testing.T) {
	violations := validate([]obs.MetricDefinition{
		{Name: "f1_publish_total", Labels: []string{"topic", "topic"}},
		{Name: "f1_publish_total", Labels: []string{"topic"}},
	})
	joined := strings.Join(violations, "\n")
	for _, want := range []string{"registered more than once", "repeats label"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("violations=%v, missing %q", violations, want)
		}
	}
}
