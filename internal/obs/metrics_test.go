package obs

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestNewMetricsWithoutProviderIsNoop(t *testing.T) {
	metrics, err := NewMetrics(nil)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	unregister := metrics.Register("orders", func() map[string]int64 {
		called = true
		return map[string]int64{"orders.created": 1}
	}, func() int64 {
		called = true
		return 1
	}, nil)
	unregister()
	if called {
		t.Fatal("metric sources ran without a meter provider")
	}
	if err := metrics.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMetricsPublishesRetryAfterClamp(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	defer func() { _ = provider.Shutdown(context.Background()) }()
	metrics, err := NewMetrics(provider)
	if err != nil {
		t.Fatal(err)
	}
	metrics.Register("orders", nil, nil, func() map[string]uint64 { return map[string]uint64{"orders.created": 3} })
	var resourceMetrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &resourceMetrics); err != nil {
		t.Fatal(err)
	}
	for _, scope := range resourceMetrics.ScopeMetrics {
		for _, produced := range scope.Metrics {
			if produced.Name != "f1_retry_after_clamped_total" {
				continue
			}
			sum, ok := produced.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("clamp metric data = %T, want sum", produced.Data)
			}
			for _, point := range sum.DataPoints {
				topic, topicOK := point.Attributes.Value(attribute.Key("topic"))
				subscription, subscriptionOK := point.Attributes.Value(attribute.Key("subscription"))
				if topicOK && subscriptionOK && topic.AsString() == "orders.created" && subscription.AsString() == "orders" && point.Value == 3 {
					return
				}
			}
		}
	}
	t.Fatal("clamp metric did not contain orders=3")
}
