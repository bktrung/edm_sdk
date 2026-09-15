package bench_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/bench"
)

const (
	// corpus is small on purpose: this test proves the harness's correctness
	// assertion, not a rate, and the race-enabled suite pays for every message.
	corpus = 20
	// retryTier is a short delay the in-memory driver's deferred delivery
	// honours, so the retry path returns without waiting out a shipped tier.
	retryTier = 5 * time.Millisecond
	// stepTimeout bounds one measurement, including its warm-up and its drain.
	stepTimeout = 30 * time.Second
	// testTimeout bounds the whole test, which runs four measurements.
	testTimeout = 2 * time.Minute
)

// TestHarnessOnInmem runs every measurement the broker benchmarks run, against
// the in-memory driver and without a broker.
//
// The harness asserts its own correctness claim: at the instant the clock
// stops, every published message has settled exactly once per attempt, and
// nothing that was not published arrived. Running it here is what keeps that
// assertion covered by the default gate, on a driver that needs no fixture.
func TestHarnessOnInmem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	namespace := benchNamespace(t)

	t.Run("publish", func(t *testing.T) {
		h := newHarness(t, inmem.New(), bench.Config{
			Namespace:  namespace,
			Messages:   corpus,
			Publishers: 4,
			Timeout:    stepTimeout,
		})
		defer closeHarness(ctx, t, h)
		result, err := h.Publish(ctx)
		if err != nil {
			t.Fatalf("publish: %v", err)
		}
		if result.Messages != corpus {
			t.Fatalf("messages = %d, want %d", result.Messages, corpus)
		}
		if result.Rate() <= 0 {
			t.Fatalf("rate = %v, want a positive publish rate", result.Rate())
		}
	})

	t.Run("consume", func(t *testing.T) {
		h := newHarness(t, inmem.New(), bench.Config{
			Namespace:   namespace,
			Messages:    corpus,
			Publishers:  4,
			Concurrency: 1,
			Timeout:     stepTimeout,
		})
		defer closeHarness(ctx, t, h)
		result, err := h.Consume(ctx)
		if err != nil {
			t.Fatalf("consume: %v", err)
		}
		if result.Messages != corpus {
			t.Fatalf("messages = %d, want %d", result.Messages, corpus)
		}
		if result.Rate() <= 0 {
			t.Fatalf("rate = %v, want a positive consume rate", result.Rate())
		}
	})

	t.Run("latency", func(t *testing.T) {
		h := newHarness(t, inmem.New(), bench.Config{
			Namespace:   namespace,
			Messages:    corpus,
			Publishers:  1,
			Concurrency: 1,
			Timeout:     stepTimeout,
		})
		defer closeHarness(ctx, t, h)
		result, err := h.Latency(ctx)
		if err != nil {
			t.Fatalf("latency: %v", err)
		}
		if len(result.Latencies) != corpus {
			t.Fatalf("latencies = %d, want %d", len(result.Latencies), corpus)
		}
		if result.Percentile(50) <= 0 || result.Percentile(99) <= 0 {
			t.Fatalf("p50 = %s, p99 = %s, want positive round trips", result.Percentile(50), result.Percentile(99))
		}
	})

	t.Run("retry", func(t *testing.T) {
		h := newHarness(t, inmem.New(), bench.Config{
			Namespace:   namespace,
			Messages:    corpus,
			Publishers:  4,
			Concurrency: 1,
			RetryTier:   retryTier,
			Timeout:     stepTimeout,
		})
		defer closeHarness(ctx, t, h)
		result, err := h.Retry(ctx)
		if err != nil {
			t.Fatalf("retry: %v", err)
		}
		if result.Messages != corpus {
			t.Fatalf("messages = %d, want %d", result.Messages, corpus)
		}
		if result.RetryTier != retryTier {
			t.Fatalf("retry tier = %s, want %s", result.RetryTier, retryTier)
		}
		if result.Rate() <= 0 {
			t.Fatalf("rate = %v, want a positive retry-path rate", result.Rate())
		}
	})
}

func newHarness(t *testing.T, drv driver.Driver, cfg bench.Config) *bench.Harness {
	t.Helper()
	h, err := bench.New(drv, cfg)
	if err != nil {
		t.Fatalf("bench.New: %v", err)
	}
	return h
}

func closeHarness(ctx context.Context, t *testing.T, h *bench.Harness) {
	t.Helper()
	if err := h.Close(ctx); err != nil {
		t.Errorf("bench.Close: %v", err)
	}
}

// benchNamespace returns a name unique to one run of this binary, derived the
// way the driver-acceptance harness derives its own: the temporary directory
// the testing package names for this test carries a suffix that differs per
// run, so two binaries measuring at the same time do not collide on a broker.
// The harness appends its own counter, so repeated runs of one namespace are
// distinct too.
func benchNamespace(tb testing.TB) string {
	tb.Helper()
	return strings.ToLower(filepath.Base(filepath.Dir(tb.TempDir())))
}
