// Package otlpboundary_test observes metric data after OTLP export.

package otlpboundary_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/f1otel"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/f1test"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	collectormetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

func TestOTLPBoundaryRecordsWireMetrics(t *testing.T) {
	received := make(chan *collectormetricspb.ExportMetricsServiceRequest, 8)
	server := httptest.NewServer(metricsReceiver(received))
	t.Cleanup(server.Close)

	exporter, err := otlpmetrichttp.New(context.Background(),
		otlpmetrichttp.WithEndpointURL(server.URL),
		otlpmetrichttp.WithInsecure(),
		otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression),
	)
	if err != nil {
		t.Fatal(err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)))
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})

	observer, err := f1otel.New(f1otel.WithMeterProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	recorder := f1test.NewRecorder()
	client := f1test.NewClient(t,
		f1.WithObserver(&recordingObserver{
			metrics:  observer,
			recorder: recorder,
			tokens:   make(map[uint64]recordedTokens),
		}),
		f1.WithBacklogPollInterval(time.Second),
	)
	driveInMemoryScenarios(t, client, recorder)

	flushCtx, cancelFlush := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFlush()
	if err := provider.ForceFlush(flushCtx); err != nil {
		t.Fatal(err)
	}
	request := receiveRequest(t, received)
	metricCount, names, attributeKeys, resourceAttributeKeys := wireEvidence(request)
	if metricCount == 0 {
		t.Fatalf("receiver observed zero metrics")
	}
	t.Logf("wire metric count: %d", metricCount)
	for _, name := range names {
		t.Logf("wire metric name: %s", name)
	}
	for _, key := range attributeKeys {
		t.Logf("wire data point attribute key: %s", key)
	}
	for _, key := range resourceAttributeKeys {
		t.Logf("wire resource attribute key: %s", key)
	}
}

func TestOTLPBoundaryAttachesProcessExemplarToOperationSpan(t *testing.T) {
	received := make(chan *collectormetricspb.ExportMetricsServiceRequest, 1)
	server := httptest.NewServer(metricsReceiver(received))
	t.Cleanup(server.Close)

	exporter, err := otlpmetrichttp.New(context.Background(),
		otlpmetrichttp.WithEndpointURL(server.URL),
		otlpmetrichttp.WithInsecure(),
	)
	if err != nil {
		t.Fatal(err)
	}
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)))
	t.Cleanup(func() {
		if err := meterProvider.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})

	tracerExporter := tracetest.NewInMemoryExporter()
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSyncer(tracerExporter),
	)
	t.Cleanup(func() {
		if err := tracerProvider.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})

	observer, err := f1otel.New(
		f1otel.WithMeterProvider(meterProvider),
		f1otel.WithTracerProvider(tracerProvider),
	)
	if err != nil {
		t.Fatal(err)
	}

	base := time.Unix(100, 0)
	_, publishToken := observer.Start(context.Background(), f1.StartEvent{
		Kind:  f1.ObserverPublish,
		At:    base,
		Topic: "orders.created",
	})
	observer.Finish(publishToken, f1.FinishEvent{
		Kind:    f1.ObserverPublish,
		At:      base.Add(time.Second),
		Topic:   "orders.created",
		Outcome: f1.ObserverOutcomeOK,
		Results: []f1.MessageResult{{ID: "published"}},
	})
	processCtx, parentSpan := tracerProvider.Tracer("test").Start(context.Background(), "parent")
	_, processToken := observer.Start(processCtx, f1.StartEvent{
		Kind:         f1.ObserverProcess,
		At:           base,
		Topic:        "orders.created",
		Subscription: "orders",
	})
	observer.Finish(processToken, f1.FinishEvent{
		Kind:         f1.ObserverProcess,
		At:           base.Add(time.Second),
		Topic:        "orders.created",
		Subscription: "orders",
		Outcome:      f1.ObserverOutcomeOK,
	})
	parentSpan.End()

	flushCtx, cancelFlush := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFlush()
	if err := meterProvider.ForceFlush(flushCtx); err != nil {
		t.Fatal(err)
	}
	request := receiveRequest(t, received)
	processMetric := findMetric(request, "messaging.process.duration")
	if processMetric == nil {
		t.Fatal("process duration metric was not exported")
	}
	histogram := processMetric.GetHistogram()
	if histogram == nil {
		t.Fatal("process duration metric was not a histogram")
	}
	dataPoints := histogram.GetDataPoints()
	if len(dataPoints) != 1 {
		t.Fatalf("process duration data point count = %d, want 1", len(dataPoints))
	}
	exemplars := dataPoints[0].GetExemplars()
	if len(exemplars) != 1 {
		t.Fatalf("process duration exemplar count = %d, want 1", len(exemplars))
	}

	var processSpan tracetest.SpanStub
	foundProcessSpan := false
	for _, span := range tracerExporter.GetSpans() {
		if span.Name == "process orders.created" {
			processSpan = span
			foundProcessSpan = true
			break
		}
	}
	if !foundProcessSpan {
		t.Fatal("process span was not exported")
	}
	spanID := processSpan.SpanContext.SpanID()
	if got, want := exemplars[0].GetSpanId(), spanID[:]; !bytes.Equal(got, want) {
		t.Fatalf("exemplar span ID = %x, want %x", got, want)
	}
	traceID := processSpan.SpanContext.TraceID()
	if got, want := exemplars[0].GetTraceId(), traceID[:]; !bytes.Equal(got, want) {
		t.Fatalf("exemplar trace ID = %x, want %x", got, want)
	}
}

func findMetric(request *collectormetricspb.ExportMetricsServiceRequest, name string) *metricspb.Metric {
	for _, resource := range request.GetResourceMetrics() {
		for _, scope := range resource.GetScopeMetrics() {
			for _, metric := range scope.GetMetrics() {
				if metric.GetName() == name {
					return metric
				}
			}
		}
	}
	return nil
}

func metricsReceiver(received chan<- *collectormetricspb.ExportMetricsServiceRequest) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if strings.EqualFold(strings.TrimSpace(r.Header.Get("Content-Encoding")), "gzip") {
			reader, err := gzip.NewReader(bytes.NewReader(body))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			body, err = io.ReadAll(reader)
			closeErr := reader.Close()
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if closeErr != nil {
				http.Error(w, closeErr.Error(), http.StatusBadRequest)
				return
			}
		}
		request := new(collectormetricspb.ExportMetricsServiceRequest)
		if err := proto.Unmarshal(body, request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		received <- request
		response, err := proto.Marshal(new(collectormetricspb.ExportMetricsServiceResponse))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(response)
	})
}

func receiveRequest(t *testing.T, received <-chan *collectormetricspb.ExportMetricsServiceRequest) *collectormetricspb.ExportMetricsServiceRequest {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case request := <-received:
		return request
	case <-ctx.Done():
		t.Fatalf("receiver observed zero requests before deadline")
		return nil
	}
}

func driveInMemoryScenarios(t *testing.T, client *f1test.Client, recorder *f1test.Recorder) {
	t.Helper()

	started := make(chan struct{})
	release := make(chan struct{})
	lowHandled := make(chan struct{})
	var highOnce sync.Once
	var lowOnce sync.Once
	waitSubscription := f1.Subscription{
		Name:        "waiters",
		Topics:      []string{"wait.created"},
		Concurrency: 1,
		Prefetch:    12,
		Priorities:  []f1.Priority{f1.PriorityHigh, f1.PriorityLow},
		Fairness: f1.FairnessConfig{
			Weights: map[f1.Priority]int{
				f1.PriorityHigh: 8,
				f1.PriorityLow:  1,
			},
			Budgets: map[f1.Priority]time.Duration{
				f1.PriorityHigh: time.Hour,
				f1.PriorityLow:  time.Second,
			},
			PrefetchFactor: 2,
		},
		Retry:          f1.RetryConfig{MaxAttempts: 1},
		HandlerTimeout: 10 * time.Second,
		Handlers: map[string]f1.Handler{
			"wait.created.v1": f1.HandlerFunc(func(_ context.Context, event *f1.Event) error {
				if event.Priority() == f1.PriorityHigh {
					highOnce.Do(func() {
						close(started)
						<-release
					})
					return nil
				}
				lowOnce.Do(func() { close(lowHandled) })
				return nil
			}),
		},
	}
	startRunner(t, client, waitSubscription)
	client.Deliver(t, "wait.created.v1", map[string]string{"id": "high"}, f1.WithPriority(f1.PriorityHigh))
	waitForSignal(t, started, "high-priority handler did not start")
	client.Deliver(t, "wait.created.v1", map[string]string{"id": "low"}, f1.WithPriority(f1.PriorityLow))
	for range 8 {
		client.Deliver(t, "wait.created.v1", map[string]string{"id": "high-queued"}, f1.WithPriority(f1.PriorityHigh))
	}
	client.Advance(2 * time.Second)
	close(release)
	waitForSignal(t, lowHandled, "low-priority handler did not run")
	waitForObserverKind(t, recorder, f1.ObserverDeadlinePromoted, "deadline promotion did not arrive")

	backlogStarted := make(chan struct{})
	backlogRelease := make(chan struct{})
	backlogSecond := make(chan struct{})
	var backlogMu sync.Mutex
	backlogCalls := 0
	backlogSubscription := f1.Subscription{
		Name:           "backlogers",
		Topics:         []string{"backlog.created"},
		Concurrency:    1,
		Prefetch:       1,
		Priorities:     []f1.Priority{f1.PriorityMedium},
		Retry:          f1.RetryConfig{MaxAttempts: 1},
		HandlerTimeout: 10 * time.Second,
		Handlers: map[string]f1.Handler{
			"backlog.created.v1": f1.HandlerFunc(func(context.Context, *f1.Event) error {
				backlogMu.Lock()
				backlogCalls++
				call := backlogCalls
				backlogMu.Unlock()
				if call == 1 {
					close(backlogStarted)
					<-backlogRelease
					return nil
				}
				close(backlogSecond)
				return nil
			}),
		},
	}
	startRunner(t, client, backlogSubscription)
	client.Deliver(t, "backlog.created.v1", map[string]string{"id": "first"})
	waitForSignal(t, backlogStarted, "backlog handler did not start")
	client.Deliver(t, "backlog.created.v1", map[string]string{"id": "second"})
	client.Advance(2 * time.Second)
	waitForBacklogAge(t, recorder)
	close(backlogRelease)
	waitForSignal(t, backlogSecond, "backlog second handler did not run")

	retryFirst := make(chan struct{})
	retrySecond := make(chan struct{})
	var retryMu sync.Mutex
	retryAttempts := 0
	retrySubscription := singleSubscription("retryers", "retry.created", 2, f1.HandlerFunc(func(context.Context, *f1.Event) error {
		retryMu.Lock()
		retryAttempts++
		attempt := retryAttempts
		retryMu.Unlock()
		if attempt == 1 {
			close(retryFirst)
			return errors.New("retry")
		}
		close(retrySecond)
		return nil
	}))
	startRunner(t, client, retrySubscription)
	client.Deliver(t, "retry.created.v1", map[string]string{"id": "retry"})
	waitForSignal(t, retryFirst, "retry handler did not fail first attempt")
	waitForObserverKind(t, recorder, f1.ObserverRetryScheduled, "retry schedule did not arrive")
	client.Advance(time.Second)
	waitForSignal(t, retrySecond, "retry handler did not complete second attempt")

	deadLettered := make(chan struct{})
	deadSubscription := singleSubscription("dead-letterers", "dead.created", 1, f1.HandlerFunc(func(context.Context, *f1.Event) error {
		close(deadLettered)
		return errors.New("dead letter")
	}))
	startRunner(t, client, deadSubscription)
	client.Deliver(t, "dead.created.v1", map[string]string{"id": "dead"})
	waitForSignal(t, deadLettered, "dead-letter handler did not run")
	waitForObserverKind(t, recorder, f1.ObserverDeadLetterPublished, "dead-letter publication did not arrive")
}

func singleSubscription(name, topic string, maxAttempts int, handler f1.Handler) f1.Subscription {
	return f1.Subscription{
		Name:           name,
		Topics:         []string{topic},
		Concurrency:    1,
		Prefetch:       4,
		Priorities:     []f1.Priority{f1.PriorityMedium},
		Retry:          f1.RetryConfig{MaxAttempts: maxAttempts, Tiers: []time.Duration{time.Second}},
		HandlerTimeout: time.Second,
		Handlers:       map[string]f1.Handler{topic + ".v1": handler},
	}
}

func startRunner(t *testing.T, client *f1test.Client, subscription f1.Subscription) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	runner, err := client.Subscribe(ctx, subscription)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		waitForRunner(t, done)
	})
}

func waitForRunner(t *testing.T, done <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("runner returned error: %v", err)
		}
	case <-ctx.Done():
		t.Errorf("runner did not stop before deadline")
	}
}

func waitForSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatal(message)
	}
}

type recordedTokens struct {
	metrics  f1.Token
	recorder f1.Token
}

type recordingObserver struct {
	metrics  f1.Observer
	recorder *f1test.Recorder

	mu     sync.Mutex
	next   uint64
	tokens map[uint64]recordedTokens
}

func (o *recordingObserver) Start(ctx context.Context, event f1.StartEvent) (context.Context, f1.Token) {
	ctx, metricsToken := o.metrics.Start(ctx, event)
	ctx, recorderToken := o.recorder.Start(ctx, event)
	o.mu.Lock()
	o.next++
	handle := o.next
	o.tokens[handle] = recordedTokens{metrics: metricsToken, recorder: recorderToken}
	o.mu.Unlock()
	return ctx, f1.Token{Kind: event.Kind, Start: event.At, Handle: handle}
}

func (o *recordingObserver) Finish(token f1.Token, event f1.FinishEvent) {
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

func (o *recordingObserver) Record(event f1.PointEvent) {
	o.metrics.Record(event)
	o.recorder.Record(event)
}

func waitForObserverKind(t *testing.T, recorder *f1test.Recorder, kind f1.ObserverKind, message string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := recorder.Wait(ctx, func(calls []f1test.ObserverCall) bool {
		for _, call := range calls {
			if call.Op == f1test.ObserverOpRecord && call.Kind == kind {
				return true
			}
		}
		return false
	})
	if err != nil {
		t.Fatalf("%s: %v", message, err)
	}
}

func waitForBacklogAge(t *testing.T, recorder *f1test.Recorder) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := recorder.Wait(ctx, func(calls []f1test.ObserverCall) bool {
		for _, call := range calls {
			if call.Op == f1test.ObserverOpRecord &&
				call.Kind == f1.ObserverBacklogSampled &&
				call.Point.HeadAgeKnown {
				return true
			}
		}
		return false
	})
	if err != nil {
		t.Fatalf("backlog age did not arrive: %v", err)
	}
}

func wireEvidence(request *collectormetricspb.ExportMetricsServiceRequest) (int, []string, []string, []string) {
	metricCount := 0
	names := make(map[string]struct{})
	attributeKeys := make(map[string]struct{})
	resourceAttributeKeys := make(map[string]struct{})
	for _, resource := range request.GetResourceMetrics() {
		collectAttributes(resource.GetResource().GetAttributes(), resourceAttributeKeys)
		for _, scope := range resource.GetScopeMetrics() {
			for _, metric := range scope.GetMetrics() {
				metricCount++
				names[metric.GetName()] = struct{}{}
				collectMetricAttributes(metric, attributeKeys)
			}
		}
	}
	return metricCount, sortedSet(names), sortedSet(attributeKeys), sortedSet(resourceAttributeKeys)
}

func collectMetricAttributes(metric *metricspb.Metric, keys map[string]struct{}) {
	if gauge := metric.GetGauge(); gauge != nil {
		for _, point := range gauge.GetDataPoints() {
			collectAttributes(point.GetAttributes(), keys)
		}
	}
	if sum := metric.GetSum(); sum != nil {
		for _, point := range sum.GetDataPoints() {
			collectAttributes(point.GetAttributes(), keys)
		}
	}
	if histogram := metric.GetHistogram(); histogram != nil {
		for _, point := range histogram.GetDataPoints() {
			collectAttributes(point.GetAttributes(), keys)
		}
	}
	if histogram := metric.GetExponentialHistogram(); histogram != nil {
		for _, point := range histogram.GetDataPoints() {
			collectAttributes(point.GetAttributes(), keys)
		}
	}
	if summary := metric.GetSummary(); summary != nil {
		for _, point := range summary.GetDataPoints() {
			collectAttributes(point.GetAttributes(), keys)
		}
	}
}

func collectAttributes(attributes []*commonpb.KeyValue, keys map[string]struct{}) {
	for _, attribute := range attributes {
		keys[attribute.GetKey()] = struct{}{}
	}
}

func sortedSet(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
