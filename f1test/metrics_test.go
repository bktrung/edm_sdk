package f1test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestMetricsPublishesAttemptDivergence(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	c := NewClient(t, f1.WithMeterProvider(provider))

	normalHandled := make(chan struct{})
	firstHighAttempt := make(chan struct{})
	secondHighAttempt := make(chan struct{})
	retryRelease := make(chan struct{})
	var retryReleaseOnce sync.Once
	releaseRetry := func() { retryReleaseOnce.Do(func() { close(retryRelease) }) }
	runner, err := c.Subscribe(context.Background(), f1.Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Concurrency:    1,
		Prefetch:       4,
		Priorities:     []f1.Priority{f1.PriorityHigh, f1.PriorityNormal},
		Retry:          f1.RetryConfig{MaxAttempts: 2, Tiers: []time.Duration{time.Second}},
		MaxDeferrals:   2,
		HandlerTimeout: time.Second,
		Handlers: map[string]f1.Handler{
			"orders.created.v1": f1.HandlerFunc(func(_ context.Context, event *f1.Event) error {
				if event.Priority() == f1.PriorityHigh {
					if event.Attempt() == 1 {
						close(firstHighAttempt)
						<-retryRelease
						return errors.New("retry")
					}
					close(secondHighAttempt)
					return nil
				}
				close(normalHandled)
				return nil
			}),
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = runner.Run(ctx)
	}()
	t.Cleanup(func() {
		releaseRetry()
		cancel()
		timer := clock.NewReal().Timer(time.Second)
		select {
		case <-runDone:
		case <-timer.C:
			t.Errorf("runner did not stop")
		}
	})

	c.Deliver(t, "orders.created.v1", map[string]string{"id": "normal"})
	timer := clock.NewReal().Timer(time.Second)
	select {
	case <-normalHandled:
	case <-timer.C:
		t.Fatal("normal event was not handled")
	}
	c.Published()
	c.Deliver(t, "orders.created.v1", map[string]string{"id": "high"}, f1.WithPriority(f1.PriorityHigh))
	timer = clock.NewReal().Timer(time.Second)
	select {
	case <-firstHighAttempt:
	case <-timer.C:
		t.Fatal("first high-priority attempt was not handled")
	}
	c.Published()
	releaseRetry()
	waitCtx, cancelWait := context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, c.captures.waitPublished(waitCtx))
	cancelWait()
	c.Advance(time.Second)
	timer = clock.NewReal().Timer(time.Second)
	select {
	case <-secondHighAttempt:
	case <-timer.C:
		t.Fatal("retry attempt was not handled")
	}

	value, ok := readGaugeValue(t, reader, "f1_attempt_divergence", "topic", "orders.created")
	assert.True(t, ok)
	assert.Equal(t, int64(2), value)

	value, ok = readGaugeValue(t, reader, "f1_attempt_divergence", "topic", "orders.created")
	assert.True(t, ok)
	assert.Equal(t, int64(0), value)
}

func TestMetricsPublishesCurrentStuckWorkers(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	c := NewClient(t, f1.WithMeterProvider(provider))

	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	returned := make(chan struct{})
	runner, err := c.Subscribe(context.Background(), f1.Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Concurrency:    1,
		Prefetch:       1,
		Priorities:     []f1.Priority{f1.PriorityNormal},
		Retry:          f1.RetryConfig{MaxAttempts: 1},
		MaxDeferrals:   1,
		HandlerTimeout: 10 * time.Millisecond,
		Handlers: map[string]f1.Handler{
			"orders.created.v1": f1.HandlerFunc(func(context.Context, *f1.Event) error {
				close(started)
				<-release
				close(returned)
				return nil
			}),
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = runner.Run(ctx)
	}()
	t.Cleanup(func() {
		select {
		case <-returned:
		default:
			releaseHandler()
		}
		cancel()
		timer := clock.NewReal().Timer(time.Second)
		select {
		case <-runDone:
		case <-timer.C:
			t.Errorf("runner did not stop")
		}
	})

	c.Deliver(t, "orders.created.v1", map[string]string{"id": "stuck"})
	timer := clock.NewReal().Timer(time.Second)
	select {
	case <-started:
	case <-timer.C:
		t.Fatal("handler did not start")
	}
	c.Advance(20 * time.Millisecond)

	for i := 0; i < 2; i++ {
		assert.Eventually(t, func() bool {
			value, ok := readGaugeValue(t, reader, "f1_stuck_workers", "subscription", "orders")
			return ok && value == 1
		}, time.Second, time.Millisecond)
		value, ok := readGaugeValue(t, reader, "f1_stuck_workers", "subscription", "orders")
		assert.True(t, ok)
		assert.Equal(t, int64(1), value)
	}

	releaseHandler()
	timer = clock.NewReal().Timer(time.Second)
	select {
	case <-returned:
	case <-timer.C:
		t.Fatal("stuck handler did not return")
	}
	assert.Eventually(t, func() bool {
		value, ok := readGaugeValue(t, reader, "f1_stuck_workers", "subscription", "orders")
		return ok && value == 0
	}, time.Second, time.Millisecond)
}

func readGaugeValue(t *testing.T, reader *metric.ManualReader, name, labelKey, labelValue string) (int64, bool) {
	t.Helper()
	var resourceMetrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &resourceMetrics); err != nil {
		return 0, false
	}
	for _, scope := range resourceMetrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != name {
				continue
			}
			gauge, ok := metric.Data.(metricdata.Gauge[int64])
			if !ok {
				return 0, false
			}
			for _, point := range gauge.DataPoints {
				if point.Attributes.Len() != 1 {
					continue
				}
				value, ok := point.Attributes.Value(attribute.Key(labelKey))
				if ok && value.AsString() == labelValue {
					return point.Value, true
				}
			}
		}
	}
	return 0, false
}
