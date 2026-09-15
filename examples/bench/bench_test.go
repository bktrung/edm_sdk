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
	// handlerWork is the hold time the slow-handler test configures. It is an
	// order of magnitude above what the harness itself costs per message, so
	// the measured window is the handler's holds rather than the harness's own
	// work.
	handlerWork = 10 * time.Millisecond
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

// TestHarnessHandlerWorkOnInmem proves what the slow-handler consume shape
// measures: at one handler at a time the configured holds are what the measured
// window contains, so the window is at least the corpus times that hold, and the
// result carries the hold so a printed rate names its shape.
//
// The hold is the whole point of the shape. A harness that configured it and
// never waited would still report a positive rate, which is why the window and
// not the rate is what this test asserts.
func TestHarnessHandlerWorkOnInmem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	h := newHarness(t, inmem.New(), bench.Config{
		Namespace:   benchNamespace(t),
		Messages:    corpus,
		Publishers:  4,
		Concurrency: 1,
		HandlerWork: handlerWork,
		Timeout:     stepTimeout,
	})
	defer closeHarness(ctx, t, h)
	result, err := h.Consume(ctx)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if result.HandlerWork != handlerWork {
		t.Fatalf("handler work = %s, want %s", result.HandlerWork, handlerWork)
	}
	// One handler at a time can only hold one message at a time, so the window
	// contains at least the corpus's holds. Publishing and settling the corpus
	// around them add to it and never take from it.
	want := time.Duration(corpus) * handlerWork
	if result.Elapsed < want {
		t.Fatalf("elapsed = %s for %d messages held %s each, want at least %s",
			result.Elapsed, corpus, handlerWork, want)
	}
}

// TestNewRefusesNegativeHandlerWork proves the harness refuses a hold time that
// cannot mean anything. A negative work is a wait that never happens, so a
// caller who passes one has a defect, and measuring a handler the caller did not
// configure would put a wrong shape beside a real number.
func TestNewRefusesNegativeHandlerWork(t *testing.T) {
	_, err := bench.New(inmem.New(), bench.Config{
		Namespace:   benchNamespace(t),
		Messages:    corpus,
		Publishers:  1,
		HandlerWork: -handlerWork,
		Timeout:     stepTimeout,
	})
	if err == nil {
		t.Fatal("bench.New accepted a negative handler work")
	}
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
