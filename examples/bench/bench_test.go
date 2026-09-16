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
		// The heap figure is sampled over the window rather than derived from
		// the corpus, so a metric read that produced nothing would leave it at
		// zero and still pass every other check here.
		if result.HeapMax == 0 {
			t.Fatal("heap max = 0, want the live heap the measurement sampled")
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

// TestHarnessCountsDuplicatesOnInmem proves that a delivery the shape did not
// expect is a figure a measurement can report instead of a failure, which is
// what a path that trades duplicates for something else needs from the harness.
//
// The driver hands one corpus message to the core twice. The run still has to
// end on the corpus it expected and settle every message exactly once: a count
// that let the copy end the window would report a rate for a corpus that had not
// settled, and a count that left the copy out of the window would report zero
// for a run that produced one.
func TestHarnessCountsDuplicatesOnInmem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	h := newHarness(t, &duplicatingDriver{Driver: inmem.New()}, bench.Config{
		Namespace:       benchNamespace(t),
		Messages:        corpus,
		Publishers:      1,
		Concurrency:     1,
		CountDuplicates: true,
		Timeout:         stepTimeout,
	})
	defer closeHarness(ctx, t, h)
	result, err := h.Consume(ctx)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if result.Duplicates != 1 {
		t.Fatalf("duplicates = %d, want 1", result.Duplicates)
	}
	if result.Messages != corpus {
		t.Fatalf("messages = %d, want %d", result.Messages, corpus)
	}
}

// duplicatingDriver hands one delivery to the core twice, which is what a
// measurement of a path that trades duplicates for a bound has to be able to
// report.
type duplicatingDriver struct {
	driver.Driver
}

func (d *duplicatingDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	conn, err := d.Driver.Open(ctx, cfg)
	if err != nil || conn == nil {
		return conn, err
	}
	return &duplicatingConn{Conn: conn}, nil
}

type duplicatingConn struct {
	driver.Conn
}

func (c *duplicatingConn) Consumer(ctx context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	consumer, err := c.Conn.Consumer(ctx, cfg)
	if err != nil || consumer == nil {
		return consumer, err
	}
	return &duplicatingConsumer{Consumer: consumer}, nil
}

// duplicatingConsumer copies the second delivery it sees and leaves the rest
// alone. The first delivery of a run is the warm-up message the harness settles
// before its window opens, so the copy is of a message the measurement counts.
//
// The copy carries a settlement that reports success without reaching the
// driver. The driver's record is settled by the delivery it belongs to, and a
// second settlement of one record is a driver error rather than anything the
// harness counts: the figure under test is a delivery, so the copy has to be
// one without disturbing what the driver tracks.
type duplicatingConsumer struct {
	driver.Consumer
}

func (c *duplicatingConsumer) Messages() <-chan driver.InboundMessage {
	source := c.Consumer.Messages()
	out := make(chan driver.InboundMessage)
	go func() {
		defer close(out)
		var warmUp, duplicated bool
		for message := range source {
			out <- message
			if !warmUp {
				warmUp = true
				continue
			}
			if duplicated {
				continue
			}
			duplicated = true
			message.Settle = settledAnyway{}
			out <- message
		}
	}()
	return out
}

// settledAnyway reports a settlement without reaching the driver.
type settledAnyway struct{}

func (settledAnyway) Ack(context.Context) error { return nil }

func (settledAnyway) Nack(context.Context, driver.NackOptions) error { return nil }

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
