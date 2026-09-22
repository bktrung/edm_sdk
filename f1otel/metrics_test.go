package f1otel

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/f1test"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestMetricsUseEventTimesAndBoundedAttributes(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	observer, err := New(WithMeterProvider(provider))
	require.NoError(t, err)

	base := time.Unix(100, 0)
	observer.Record(f1.PointEvent{
		Kind:          f1.ObserverDriverSelected,
		At:            base,
		DriverName:    "inmem",
		ServerAddress: "127.0.0.1",
		ServerPort:    19092,
	})

	publishStart := f1.StartEvent{
		Kind:     f1.ObserverPublish,
		At:       base,
		Topic:    "orders.created",
		Priority: f1.PriorityHigh,
	}
	_, publishToken := observer.Start(context.Background(), publishStart)
	observer.Finish(publishToken, f1.FinishEvent{
		Kind:     f1.ObserverPublish,
		At:       base.Add(2 * time.Second),
		Topic:    publishStart.Topic,
		Priority: publishStart.Priority,
		Outcome:  f1.ObserverOutcomeOK,
		Results: []f1.MessageResult{
			{ID: "published-1"},
			{ID: "published-2"},
			{Err: errors.New("publish failed")},
		},
	})

	processStart := f1.StartEvent{
		Kind:          f1.ObserverProcess,
		At:            base.Add(3 * time.Second),
		Topic:         "orders.created",
		Subscription:  "orders",
		ConsumerGroup: "orders",
		Priority:      f1.PriorityHigh,
		Attempt:       2,
	}
	_, processToken := observer.Start(context.Background(), processStart)
	observer.Finish(processToken, f1.FinishEvent{
		Kind:          f1.ObserverProcess,
		At:            base.Add(4 * time.Second),
		Topic:         processStart.Topic,
		Subscription:  processStart.Subscription,
		ConsumerGroup: processStart.ConsumerGroup,
		Priority:      processStart.Priority,
		Attempt:       2,
		Outcome:       f1.ObserverOutcomeError,
		ErrorClass:    f1.ErrorClassRetryable,
		Terminal:      false,
	})

	settleStart := f1.StartEvent{
		Kind:          f1.ObserverSettle,
		At:            base.Add(5 * time.Second),
		Topic:         "orders.created",
		Subscription:  "orders",
		ConsumerGroup: "orders",
		Priority:      f1.PriorityHigh,
	}
	_, settleToken := observer.Start(context.Background(), settleStart)
	observer.Finish(settleToken, f1.FinishEvent{
		Kind:          f1.ObserverSettle,
		At:            base.Add(6 * time.Second),
		Topic:         settleStart.Topic,
		Subscription:  settleStart.Subscription,
		ConsumerGroup: settleStart.ConsumerGroup,
		Priority:      settleStart.Priority,
		Outcome:       f1.ObserverOutcomeOK,
	})

	observer.Record(f1.PointEvent{
		Kind:             f1.ObserverDeliveryReceived,
		At:               base.Add(8 * time.Second),
		Topic:            "orders.created",
		Subscription:     "orders",
		ConsumerGroup:    "orders",
		Priority:         f1.PriorityHigh,
		EnqueuedAt:       base.Add(7 * time.Second),
		EnqueuedAtSource: f1.EnqueuedAtBroker,
	})
	observer.Record(f1.PointEvent{
		Kind:             f1.ObserverDeliveryReceived,
		At:               base.Add(8 * time.Second),
		Topic:            "orders.created",
		Subscription:     "orders",
		ConsumerGroup:    "orders",
		Priority:         f1.PriorityHigh,
		EnqueuedAt:       base.Add(7 * time.Second),
		EnqueuedAtSource: f1.EnqueuedAtProducer,
	})
	observer.Record(f1.PointEvent{
		Kind:             f1.ObserverBacklogSampled,
		At:               base.Add(9 * time.Second),
		Topic:            "orders.created",
		Subscription:     "orders",
		Priority:         f1.PriorityHigh,
		Backlog:          7,
		HeadAge:          1500 * time.Millisecond,
		HeadAgeKnown:     true,
		EnqueuedAtSource: f1.EnqueuedAtBroker,
	})
	observer.Record(f1.PointEvent{
		Kind:         f1.ObserverDeadlinePromoted,
		At:           base.Add(10 * time.Second),
		Topic:        "orders.created",
		Subscription: "orders",
		Priority:     f1.PriorityHigh,
		LaneWait:     250 * time.Millisecond,
		Suppressed:   2,
	})
	observer.Record(f1.PointEvent{
		Kind:         f1.ObserverRetryScheduled,
		At:           base.Add(11 * time.Second),
		Topic:        "orders.created",
		Subscription: "orders",
		Priority:     f1.PriorityHigh,
		ErrorClass:   f1.ErrorClassRetryable,
	})
	observer.Record(f1.PointEvent{
		Kind:         f1.ObserverDeadLetterPublished,
		At:           base.Add(12 * time.Second),
		Topic:        "orders.created",
		Subscription: "orders",
		Priority:     f1.PriorityHigh,
		ErrorClass:   f1.ErrorClassMaxAttempts,
		Reason:       f1.ReasonMaxAttempts,
	})

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &metrics))

	expected := map[string]struct {
		unit string
		kind string
	}{
		"messaging.client.sent.messages":      {unit: "{message}", kind: "sum"},
		"messaging.client.consumed.messages":  {unit: "{message}", kind: "sum"},
		"messaging.client.operation.duration": {unit: "s", kind: "histogram"},
		"messaging.process.duration":          {unit: "s", kind: "histogram"},
		"f1.messaging.broker.wait.duration":   {unit: "s", kind: "histogram"},
		"f1.messaging.backlog.messages":       {unit: "", kind: "gauge"},
		"f1.messaging.backlog.oldest.age":     {unit: "s", kind: "gauge"},
		"f1.messaging.deadline.promotions":    {unit: "", kind: "sum"},
		"f1.messaging.lane.wait.duration":     {unit: "s", kind: "histogram"},
		"f1.messaging.retries":                {unit: "", kind: "sum"},
		"f1.messaging.dead_letters":           {unit: "", kind: "sum"},
	}
	seen := make(map[string]metricdata.Metrics)
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			seen[metric.Name] = metric
		}
	}
	require.Len(t, seen, len(expected))
	for name, want := range expected {
		metric, ok := seen[name]
		require.True(t, ok, "missing metric %q", name)
		require.Equal(t, want.unit, metric.Unit, name)
		switch want.kind {
		case "sum":
			_, ok = metric.Data.(metricdata.Sum[int64])
		case "gauge":
			if name == "f1.messaging.backlog.oldest.age" {
				_, ok = metric.Data.(metricdata.Gauge[float64])
			} else {
				_, ok = metric.Data.(metricdata.Gauge[int64])
			}
		case "histogram":
			_, ok = metric.Data.(metricdata.Histogram[float64])
		}
		require.True(t, ok, "%s has unexpected aggregation %T", name, metric.Data)
	}

	sent := asSum[int64](t, seen["messaging.client.sent.messages"])
	require.Len(t, sent.DataPoints, 1)
	require.Equal(t, int64(2), sent.DataPoints[0].Value)
	require.Equal(t, map[string]string{
		"f1.priority":                "high",
		"messaging.destination.name": "orders.created",
		"messaging.operation.name":   "publish",
		"messaging.system":           "f1.inmem",
		"server.address":             "127.0.0.1",
		"server.port":                "19092",
	}, stringAttributes(sent.DataPoints[0].Attributes))

	consumed := asSum[int64](t, seen["messaging.client.consumed.messages"])
	require.Len(t, consumed.DataPoints, 1)
	require.Equal(t, int64(2), consumed.DataPoints[0].Value)
	require.Equal(t, map[string]string{
		"f1.priority":                   "high",
		"messaging.consumer.group.name": "orders",
		"messaging.destination.name":    "orders.created",
		"messaging.operation.name":      "receive",
		"messaging.system":              "f1.inmem",
		"server.address":                "127.0.0.1",
		"server.port":                   "19092",
	}, stringAttributes(consumed.DataPoints[0].Attributes))

	wait := asHistogram(t, seen["f1.messaging.broker.wait.duration"])
	require.Len(t, wait.DataPoints, 1)
	require.Equal(t, uint64(1), wait.DataPoints[0].Count)
	require.InDelta(t, 1.0, wait.DataPoints[0].Sum, 0.000001)
	require.Equal(t, map[string]string{
		"f1.priority":                   "high",
		"messaging.consumer.group.name": "orders",
		"messaging.destination.name":    "orders.created",
		"messaging.system":              "f1.inmem",
		"server.address":                "127.0.0.1",
		"server.port":                   "19092",
	}, stringAttributes(wait.DataPoints[0].Attributes))

	process := asHistogram(t, seen["messaging.process.duration"])
	require.Len(t, process.DataPoints, 1)
	require.Equal(t, uint64(1), process.DataPoints[0].Count)
	require.InDelta(t, 1.0, process.DataPoints[0].Sum, 0.000001)
	require.Equal(t, map[string]string{
		"error.type":                    "f1_retryable",
		"f1.priority":                   "high",
		"messaging.consumer.group.name": "orders",
		"messaging.destination.name":    "orders.created",
		"messaging.operation.name":      "process",
		"messaging.system":              "f1.inmem",
		"server.address":                "127.0.0.1",
		"server.port":                   "19092",
	}, stringAttributes(process.DataPoints[0].Attributes))
	operation := asHistogram(t, seen["messaging.client.operation.duration"])
	require.Len(t, operation.DataPoints, 2)
	for _, dataPoint := range operation.DataPoints {
		attrs := stringAttributes(dataPoint.Attributes)
		switch attrs["messaging.operation.name"] {
		case "publish":
			require.Equal(t, uint64(1), dataPoint.Count)
			require.InDelta(t, 2.0, dataPoint.Sum, 0.000001)
			require.Equal(t, map[string]string{
				"f1.priority":                "high",
				"messaging.destination.name": "orders.created",
				"messaging.operation.name":   "publish",
				"messaging.system":           "f1.inmem",
				"server.address":             "127.0.0.1",
				"server.port":                "19092",
			}, attrs)
		case "settle":
			require.Equal(t, uint64(1), dataPoint.Count)
			require.InDelta(t, 1.0, dataPoint.Sum, 0.000001)
			require.Equal(t, map[string]string{
				"f1.priority":                   "high",
				"messaging.consumer.group.name": "orders",
				"messaging.destination.name":    "orders.created",
				"messaging.operation.name":      "settle",
				"messaging.system":              "f1.inmem",
				"server.address":                "127.0.0.1",
				"server.port":                   "19092",
			}, attrs)
		default:
			t.Fatalf("unexpected operation name %q", attrs["messaging.operation.name"])
		}
	}

	commonPointAttrs := map[string]string{
		"f1.priority":                   "high",
		"messaging.consumer.group.name": "orders",
		"messaging.destination.name":    "orders.created",
		"messaging.system":              "f1.inmem",
		"server.address":                "127.0.0.1",
		"server.port":                   "19092",
	}
	backlog := asGauge[int64](t, seen["f1.messaging.backlog.messages"])
	require.Len(t, backlog.DataPoints, 1)
	require.Equal(t, int64(7), backlog.DataPoints[0].Value)
	require.Equal(t, commonPointAttrs, stringAttributes(backlog.DataPoints[0].Attributes))
	oldest := asGauge[float64](t, seen["f1.messaging.backlog.oldest.age"])
	require.Len(t, oldest.DataPoints, 1)
	require.InDelta(t, 1.5, oldest.DataPoints[0].Value, 0.000001)
	require.Equal(t, commonPointAttrs, stringAttributes(oldest.DataPoints[0].Attributes))
	promotions := asSum[int64](t, seen["f1.messaging.deadline.promotions"])
	require.Equal(t, int64(3), promotions.DataPoints[0].Value)
	require.Equal(t, commonPointAttrs, stringAttributes(promotions.DataPoints[0].Attributes))
	laneWait := asHistogram(t, seen["f1.messaging.lane.wait.duration"])
	require.Len(t, laneWait.DataPoints, 1)
	require.Equal(t, uint64(1), laneWait.DataPoints[0].Count)
	require.InDelta(t, 0.25, laneWait.DataPoints[0].Sum, 0.000001)
	require.Equal(t, commonPointAttrs, stringAttributes(laneWait.DataPoints[0].Attributes))
	retries := asSum[int64](t, seen["f1.messaging.retries"])
	require.Equal(t, int64(1), retries.DataPoints[0].Value)
	retryAttrs := make(map[string]string, len(commonPointAttrs)+1)
	for key, value := range commonPointAttrs {
		retryAttrs[key] = value
	}
	retryAttrs["error.type"] = "f1_retryable"
	require.Equal(t, retryAttrs, stringAttributes(retries.DataPoints[0].Attributes))
	deadLetters := asSum[int64](t, seen["f1.messaging.dead_letters"])
	require.Equal(t, int64(1), deadLetters.DataPoints[0].Value)
	require.Equal(t, map[string]string{
		"error.type":                    "f1_max_attempts",
		"f1.priority":                   "high",
		"messaging.consumer.group.name": "orders",
		"messaging.destination.name":    "orders.created",
		"messaging.system":              "f1.inmem",
		"reason":                        "max_attempts",
		"server.address":                "127.0.0.1",
		"server.port":                   "19092",
	}, stringAttributes(deadLetters.DataPoints[0].Attributes))
}

func TestBothProvidersRecordMetricAndSpanKinds(t *testing.T) {
	tests := []struct {
		name       string
		kind       f1.ObserverKind
		metricName string
		spanName   string
	}{
		{
			name:       "publish",
			kind:       f1.ObserverPublish,
			metricName: "messaging.client.operation.duration",
			spanName:   "send orders.created",
		},
		{
			name:       "process",
			kind:       f1.ObserverProcess,
			metricName: "messaging.process.duration",
			spanName:   "process orders.created",
		},
		{
			name:       "settle",
			kind:       f1.ObserverSettle,
			metricName: "messaging.client.operation.duration",
			spanName:   "settle",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { require.NoError(t, meterProvider.Shutdown(context.Background())) })

			exporter := tracetest.NewInMemoryExporter()
			tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
			t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })

			observer, err := New(
				WithMeterProvider(meterProvider),
				WithTracerProvider(tracerProvider),
			)
			require.NoError(t, err)

			base := time.Unix(100, 0)
			start := f1.StartEvent{
				Kind:         test.kind,
				At:           base,
				Topic:        "orders.created",
				Subscription: "orders",
			}
			_, token := observer.Start(context.Background(), start)
			observer.Finish(token, f1.FinishEvent{
				Kind:         test.kind,
				At:           base.Add(time.Second),
				Topic:        start.Topic,
				Subscription: start.Subscription,
				Outcome:      f1.ObserverOutcomeOK,
			})

			var collected metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(context.Background(), &collected))
			seen := make(map[string]metricdata.Metrics)
			for _, scope := range collected.ScopeMetrics {
				for _, metric := range scope.Metrics {
					seen[metric.Name] = metric
				}
			}
			histogram := asHistogram(t, seen[test.metricName])
			require.Len(t, histogram.DataPoints, 1)
			require.Equal(t, uint64(1), histogram.DataPoints[0].Count)
			require.InDelta(t, 1.0, histogram.DataPoints[0].Sum, 0.000001)

			spans := exporter.GetSpans()
			require.Len(t, spans, 1)
			require.Equal(t, test.spanName, spans[0].Name)
		})
	}
}

func TestUnknownErrorClassAndDeathReasonUseOther(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	observer, err := New(WithMeterProvider(provider))
	require.NoError(t, err)
	base := time.Unix(100, 0)
	_, token := observer.Start(context.Background(), f1.StartEvent{
		Kind: f1.ObserverProcess,
		At:   base,
	})
	observer.Finish(token, f1.FinishEvent{
		Kind:       f1.ObserverProcess,
		At:         base.Add(time.Second),
		Outcome:    f1.ObserverOutcomeError,
		ErrorClass: f1.ErrorClass("future_error"),
	})
	observer.Record(f1.PointEvent{
		Kind:       f1.ObserverDeadLetterPublished,
		At:         base.Add(2 * time.Second),
		ErrorClass: "",
		Reason:     f1.DeathReason("future_reason"),
	})

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &collected))
	seen := make(map[string]metricdata.Metrics)
	for _, scope := range collected.ScopeMetrics {
		for _, metric := range scope.Metrics {
			seen[metric.Name] = metric
		}
	}
	process := asHistogram(t, seen["messaging.process.duration"])
	require.Len(t, process.DataPoints, 1)
	require.Equal(t, "_OTHER", stringAttributes(process.DataPoints[0].Attributes)["error.type"])
	deadLetters := asSum[int64](t, seen["f1.messaging.dead_letters"])
	require.Len(t, deadLetters.DataPoints, 1)
	require.Equal(t, "_OTHER", stringAttributes(deadLetters.DataPoints[0].Attributes)["error.type"])
}

func TestMetricsOnlyForeignTokenRequiresStartTime(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	observer, err := New(WithMeterProvider(provider))
	require.NoError(t, err)

	base := time.Unix(100, 0)
	observer.Finish(
		f1.Token{Kind: f1.ObserverProcess, Handle: 999},
		f1.FinishEvent{Kind: f1.ObserverProcess, At: base.Add(time.Second)},
	)
	observer.Finish(
		f1.Token{Kind: f1.ObserverProcess, Start: base, Handle: 999},
		f1.FinishEvent{Kind: f1.ObserverProcess, At: base.Add(time.Second)},
	)

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &collected))
	seen := make(map[string]metricdata.Metrics)
	for _, scope := range collected.ScopeMetrics {
		for _, metric := range scope.Metrics {
			seen[metric.Name] = metric
		}
	}
	process := asHistogram(t, seen["messaging.process.duration"])
	require.Len(t, process.DataPoints, 1)
	require.Equal(t, uint64(1), process.DataPoints[0].Count)
	require.InDelta(t, 1.0, process.DataPoints[0].Sum, 0.000001)
}

func TestMetricsOnlyMismatchedFinishKindIsIgnored(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	observer, err := New(WithMeterProvider(provider))
	require.NoError(t, err)

	base := time.Unix(100, 0)
	_, processToken := observer.Start(context.Background(), f1.StartEvent{
		Kind: f1.ObserverProcess,
		At:   base,
	})
	observer.Finish(processToken, f1.FinishEvent{
		Kind: f1.ObserverProcess,
		At:   base.Add(time.Second),
	})
	_, publishToken := observer.Start(context.Background(), f1.StartEvent{
		Kind: f1.ObserverPublish,
		At:   base,
	})
	observer.Finish(publishToken, f1.FinishEvent{
		Kind:    f1.ObserverPublish,
		At:      base.Add(time.Second),
		Outcome: f1.ObserverOutcomeOK,
	})

	collect := func() map[string]metricdata.Metrics {
		var collected metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &collected))
		seen := make(map[string]metricdata.Metrics)
		for _, scope := range collected.ScopeMetrics {
			for _, metric := range scope.Metrics {
				seen[metric.Name] = metric
			}
		}
		return seen
	}

	before := collect()
	beforeProcess := asHistogram(t, before["messaging.process.duration"])
	beforeOperation := asHistogram(t, before["messaging.client.operation.duration"])
	require.Len(t, beforeProcess.DataPoints, 1)
	require.Len(t, beforeOperation.DataPoints, 1)

	observer.Finish(
		f1.Token{Kind: f1.ObserverProcess, Start: base, Handle: 999},
		f1.FinishEvent{Kind: f1.ObserverPublish, At: base.Add(time.Second)},
	)

	after := collect()
	afterProcess := asHistogram(t, after["messaging.process.duration"])
	afterOperation := asHistogram(t, after["messaging.client.operation.duration"])
	require.Equal(t, beforeProcess.DataPoints[0].Count, afterProcess.DataPoints[0].Count)
	require.Equal(t, beforeProcess.DataPoints[0].Sum, afterProcess.DataPoints[0].Sum)
	require.Equal(t, beforeOperation.DataPoints[0].Count, afterOperation.DataPoints[0].Count)
	require.Equal(t, beforeOperation.DataPoints[0].Sum, afterOperation.DataPoints[0].Sum)
}

func TestInternalPublishSentMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	observer, err := New(WithMeterProvider(provider))
	require.NoError(t, err)
	base := time.Unix(200, 0)
	observer.Record(f1.PointEvent{
		Kind:       f1.ObserverDriverSelected,
		At:         base,
		DriverName: "inmem",
	})
	_, token := observer.Start(context.Background(), f1.StartEvent{
		Kind:      f1.ObserverPublish,
		At:        base,
		Topic:     "orders.retry",
		Priority:  f1.PriorityLow,
		Route:     f1.PublishRouteRetry,
		BatchSize: 1,
	})
	observer.Finish(token, f1.FinishEvent{
		Kind:     f1.ObserverPublish,
		At:       base.Add(time.Second),
		Topic:    "orders.retry",
		Priority: f1.PriorityLow,
		Outcome:  f1.ObserverOutcomeOK,
	})

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &metrics))
	var sent metricdata.Metrics
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name == "messaging.client.sent.messages" {
				sent = metric
			}
		}
	}
	require.Equal(t, "messaging.client.sent.messages", sent.Name)
	data := asSum[int64](t, sent)
	require.Len(t, data.DataPoints, 1)
	require.Equal(t, int64(1), data.DataPoints[0].Value)
	require.Equal(t, map[string]string{
		"f1.priority":                "low",
		"messaging.destination.name": "orders.retry",
		"messaging.operation.name":   "publish",
		"messaging.system":           "f1.inmem",
	}, stringAttributes(data.DataPoints[0].Attributes))
}

func asSum[N int64 | float64](t *testing.T, metric metricdata.Metrics) metricdata.Sum[N] {
	t.Helper()
	value, ok := metric.Data.(metricdata.Sum[N])
	require.True(t, ok, "metric %q data = %T", metric.Name, metric.Data)
	return value
}

func asHistogram(t *testing.T, metric metricdata.Metrics) metricdata.Histogram[float64] {
	t.Helper()
	value, ok := metric.Data.(metricdata.Histogram[float64])
	require.True(t, ok, "metric %q data = %T", metric.Name, metric.Data)
	return value
}

func asGauge[N int64 | float64](t *testing.T, metric metricdata.Metrics) metricdata.Gauge[N] {
	t.Helper()
	value, ok := metric.Data.(metricdata.Gauge[N])
	require.True(t, ok, "metric %q data = %T", metric.Name, metric.Data)
	return value
}

func stringAttributes(set attribute.Set) map[string]string {
	out := make(map[string]string)
	for _, kv := range set.ToSlice() {
		out[string(kv.Key)] = kv.Value.String()
	}
	return out
}

func TestMetricsOnRealF1Scenarios(t *testing.T) {
	t.Parallel()

	scenarios := []struct {
		name string
		run  func(*testing.T, *f1test.Client, *f1test.Recorder)
	}{
		{name: "success", run: runRealSuccess},
		{name: "retry", run: runRealRetry},
		{name: "dead letter", run: runRealDeadLetter},
		{name: "panic", run: runRealPanic},
		{name: "drain", run: runRealDrain},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

			observer, err := New(WithMeterProvider(provider))
			require.NoError(t, err)
			recorder := f1test.NewRecorder()
			client := f1test.NewClient(t, f1.WithObserver(&realObserver{
				metrics:  observer,
				recorder: recorder,
				tokens:   make(map[uint64]realTokens),
			}), f1.WithBacklogPollInterval(-1))
			scenario.run(t, client, recorder)

			var collected metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(context.Background(), &collected))
			names := make(map[string]bool)
			for _, scope := range collected.ScopeMetrics {
				for _, metric := range scope.Metrics {
					names[metric.Name] = true
				}
			}
			require.True(t, names["messaging.client.consumed.messages"])
			require.True(t, names["messaging.process.duration"])
		})
	}
}

type realRunner struct {
	runner *f1.Runner
}

type realTokens struct {
	metrics  f1.Token
	recorder f1.Token
}

type realObserver struct {
	metrics  f1.Observer
	recorder *f1test.Recorder

	mu     sync.Mutex
	next   uint64
	tokens map[uint64]realTokens
}

func (o *realObserver) Start(ctx context.Context, event f1.StartEvent) (context.Context, f1.Token) {
	ctx, metricsToken := o.metrics.Start(ctx, event)
	ctx, recorderToken := o.recorder.Start(ctx, event)
	o.mu.Lock()
	o.next++
	handle := o.next
	o.tokens[handle] = realTokens{metrics: metricsToken, recorder: recorderToken}
	o.mu.Unlock()
	return ctx, f1.Token{Kind: event.Kind, Start: event.At, Handle: handle}
}

func (o *realObserver) Finish(token f1.Token, event f1.FinishEvent) {
	o.mu.Lock()
	tokens, ok := o.tokens[token.Handle]
	if ok {
		delete(o.tokens, token.Handle)
	}
	o.mu.Unlock()
	if !ok {
		return
	}
	o.metrics.Finish(tokens.metrics, event)
	o.recorder.Finish(tokens.recorder, event)
}

func (o *realObserver) Record(event f1.PointEvent) {
	o.metrics.Record(event)
	o.recorder.Record(event)
}

func realSubscription(handler f1.HandlerFunc, maxAttempts int) f1.Subscription {
	return f1.Subscription{
		Name:            "orders",
		Topics:          []string{"orders.created"},
		Concurrency:     1,
		Prefetch:        4,
		Priorities:      []f1.Priority{f1.PriorityMedium},
		Retry:           f1.RetryConfig{MaxAttempts: maxAttempts, Tiers: []time.Duration{time.Second}},
		HandlerTimeout:  time.Second,
		UnmatchedPolicy: f1.Ignore,
		Handlers:        map[string]f1.Handler{"orders.created.v1": handler},
	}
}

func startRealRunner(t *testing.T, client *f1test.Client, subscription f1.Subscription) realRunner {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	runner, err := client.Subscribe(ctx, subscription)
	require.NoError(t, err)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runner.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return realRunner{runner: runner}
}

func waitRealCounts(t *testing.T, recorder *f1test.Recorder, want map[f1.ObserverKind]int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := recorder.Wait(ctx, func(calls []f1test.ObserverCall) bool {
		got := make(map[f1.ObserverKind]int)
		for _, call := range calls {
			got[call.Kind]++
		}
		for kind, count := range want {
			if got[kind] < count {
				return false
			}
		}
		return true
	})
	require.NoError(t, err)
}

func runRealSuccess(t *testing.T, client *f1test.Client, recorder *f1test.Recorder) {
	t.Helper()
	handled := make(chan struct{})
	startRealRunner(t, client, realSubscription(func(context.Context, *f1.Event) error {
		close(handled)
		return nil
	}, 1))
	client.Deliver(t, "orders.created.v1", map[string]string{"id": "success"})
	<-handled
	waitRealCounts(t, recorder, map[f1.ObserverKind]int{
		f1.ObserverDriverSelected:   1,
		f1.ObserverPublish:          2,
		f1.ObserverMessageBuilt:     2,
		f1.ObserverDeliveryReceived: 1,
		f1.ObserverProcess:          1,
		f1.ObserverSettle:           1,
	})
}

func runRealRetry(t *testing.T, client *f1test.Client, recorder *f1test.Recorder) {
	t.Helper()
	first, second := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	attempts := 0
	startRealRunner(t, client, realSubscription(func(context.Context, *f1.Event) error {
		mu.Lock()
		attempts++
		attempt := attempts
		mu.Unlock()
		if attempt == 1 {
			close(first)
			return errors.New("retry")
		}
		close(second)
		return nil
	}, 2))
	client.Deliver(t, "orders.created.v1", map[string]string{"id": "retry"})
	<-first
	waitRealCounts(t, recorder, map[f1.ObserverKind]int{
		f1.ObserverRetryScheduled: 1,
		f1.ObserverSettle:         2,
	})
	client.Advance(time.Second)
	<-second
	waitRealCounts(t, recorder, map[f1.ObserverKind]int{
		f1.ObserverProcess: 2,
	})
}

func runRealDeadLetter(t *testing.T, client *f1test.Client, recorder *f1test.Recorder) {
	t.Helper()
	handled := make(chan struct{})
	startRealRunner(t, client, realSubscription(func(context.Context, *f1.Event) error {
		close(handled)
		return errors.New("dead letter")
	}, 1))
	client.Deliver(t, "orders.created.v1", map[string]string{"id": "dead-letter"})
	<-handled
	waitRealCounts(t, recorder, map[f1.ObserverKind]int{
		f1.ObserverDeadLetterPublished: 1,
		f1.ObserverSettle:              1,
	})
}

func runRealPanic(t *testing.T, client *f1test.Client, recorder *f1test.Recorder) {
	t.Helper()
	handled := make(chan struct{})
	startRealRunner(t, client, realSubscription(func(context.Context, *f1.Event) error {
		defer close(handled)
		panic("handler panic")
	}, 1))
	client.Deliver(t, "orders.created.v1", map[string]string{"id": "panic"})
	<-handled
	waitRealCounts(t, recorder, map[f1.ObserverKind]int{
		f1.ObserverDeadLetterPublished: 1,
		f1.ObserverSettle:              1,
	})
}

func runRealDrain(t *testing.T, client *f1test.Client, recorder *f1test.Recorder) {
	t.Helper()
	started, release := make(chan struct{}), make(chan struct{})
	run := startRealRunner(t, client, realSubscription(func(context.Context, *f1.Event) error {
		close(started)
		<-release
		return nil
	}, 1))
	client.Deliver(t, "orders.created.v1", map[string]string{"id": "drain"})
	<-started
	drained := make(chan error, 1)
	go func() { drained <- run.runner.Drain(context.Background()) }()
	waitRealCounts(t, recorder, map[f1.ObserverKind]int{
		f1.ObserverDrain: 1,
	})
	close(release)
	require.NoError(t, <-drained)
	waitRealCounts(t, recorder, map[f1.ObserverKind]int{
		f1.ObserverProcess: 1,
		f1.ObserverSettle:  1,
	})
}
