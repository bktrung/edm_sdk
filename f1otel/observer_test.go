package f1otel

import (
	"context"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestObserverNoOpValuesAcceptEveryEvent(t *testing.T) {
	t.Parallel()

	kinds := []f1.ObserverKind{
		f1.ObserverPublish,
		f1.ObserverMessageBuilt,
		f1.ObserverProcess,
		f1.ObserverSettle,
		f1.ObserverDrain,
		f1.ObserverDeliveryReceived,
		f1.ObserverRetryScheduled,
		f1.ObserverDeadLetterDecided,
		f1.ObserverDeadLetterPublished,
		f1.ObserverDeadLetterFailed,
		f1.ObserverPoisonRejected,
		f1.ObserverBacklogSampled,
		f1.ObserverConnectionLost,
		f1.ObserverConnectionRestored,
		f1.ObserverDriverSelected,
		f1.ObserverDeadlinePromoted,
	}
	observers := []*Observer{nil, {}, providerlessObserver(t)}
	for _, observer := range observers {
		for _, kind := range kinds {
			ctx, token := observer.Start(context.Background(), f1.StartEvent{
				Kind: kind,
				At:   time.Unix(1, 0),
			})
			if ctx == nil {
				t.Fatalf("Start(%q) returned nil context", kind)
			}
			observer.Finish(token, f1.FinishEvent{
				Kind:    kind,
				At:      time.Unix(2, 0),
				Outcome: f1.ObserverOutcomeOK,
			})
			observer.Record(f1.PointEvent{Kind: kind, At: time.Unix(3, 0)})
		}
	}
}

func TestFinishDeletesSpanStateForMismatchedKind(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	observer, err := New(WithTracerProvider(provider))
	require.NoError(t, err)

	base := time.Unix(100, 0)
	_, token := observer.Start(context.Background(), f1.StartEvent{
		Kind: f1.ObserverProcess,
		At:   base,
	})
	require.Len(t, observer.starts, 1)

	observer.Finish(token, f1.FinishEvent{
		Kind: f1.ObserverPublish,
		At:   base.Add(time.Second),
	})
	require.Empty(t, observer.starts)
	observer.Finish(token, f1.FinishEvent{
		Kind: f1.ObserverProcess,
		At:   base.Add(2 * time.Second),
	})
	require.Empty(t, observer.starts)
	require.Empty(t, exporter.GetSpans())
}

func providerlessObserver(t *testing.T) *Observer {
	t.Helper()
	observer, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return observer
}

func TestMetricsOnlyFinishHasNoExemplar(t *testing.T) {
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
		Kind:    f1.ObserverProcess,
		At:      base.Add(time.Second),
		Outcome: f1.ObserverOutcomeOK,
	})

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &collected))
	var process metricdata.Metrics
	for _, scope := range collected.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name == "messaging.process.duration" {
				process = metric
			}
		}
	}
	histogram, ok := process.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, histogram.DataPoints, 1)
	require.Empty(t, histogram.DataPoints[0].Exemplars)
}
