// Package bench measures an f1 client against a driver through the public API.
//
// Every operation it performs crosses the public surface - New, Publish,
// Subscribe, Run - so a number it reports describes what a service author
// gets, not what a driver's own Consumer costs. It is handed a driver.Driver
// instead of choosing one, which keeps it driver-agnostic by construction and
// inside the import boundary that keeps core code away from concrete adapters.
//
// Settlement is the one thing the public API does not expose. The harness
// therefore wraps the driver at the port, as the test helper package does, and
// counts a delivery as settled only when its Ack returns nil. Counting
// anywhere earlier - at delivery, or on handler entry - reports the rate of
// the step before settlement, which is a bigger number about a different
// thing, so the harness checks where its clock stopped rather than trusting it.
package bench

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

const (
	// lane is the one delivery lane every measurement uses. Multi-lane
	// contention is a separate question from throughput, and the port has no
	// cross-driver lane semantics to compare yet.
	lane = f1.PriorityMedium

	// connectTimeout bounds one client's own connect attempts, so an
	// unreachable endpoint fails the measurement instead of waiting out the
	// caller's deadline.
	connectTimeout = 10 * time.Second

	// handlerTimeout stays under drainTimeout because the core refuses a
	// subscription whose handler timeout the drain budget does not exceed.
	handlerTimeout = 5 * time.Second
	drainTimeout   = 30 * time.Second
	closeTimeout   = 10 * time.Second

	// pruneAttempts bounds how often cleanup retries a delete that a consumer
	// group still holds, and pruneWait is the delay between those attempts. A
	// group keeps membership for a moment after its process exits, and the
	// port's delete guard refuses until that clears.
	pruneAttempts = 10
	pruneWait     = 500 * time.Millisecond

	// cleanupTimeout bounds one cleanup, which runs after the caller's context
	// may already be done.
	cleanupTimeout = 2 * time.Minute
)

// sequence numbers each harness, so two harnesses built from one namespace
// still create different broker destinations.
var sequence atomic.Int64

// Config describes one measurement's client and subscription shape.
//
// A zero Concurrency keeps the shipped handler concurrency. The harness sets a
// shape only where a measurement needs one the shipped defaults do not have.
type Config struct {
	// Namespace names everything one run creates. It must be unique per
	// process; the harness appends a counter so several runs of one namespace
	// still differ. Only lower-case letters, digits, dot, underscore and dash
	// can reach a broker, so anything else is refused before a client is built.
	Namespace string
	// Endpoint is the broker endpoint the client connects to. It is empty for
	// the in-memory driver, which dials nothing.
	Endpoint string
	// Messages is the corpus size of one measurement.
	Messages int
	// Publishers is the number of goroutines that publish the corpus.
	Publishers int
	// Concurrency is the handler concurrency of the single lane.
	Concurrency int
	// HandlerWork is how long the consume measurement's handler holds each
	// delivery before returning. It applies to the consume measurement alone:
	// the publish, latency and retry measurements keep the handler that returns
	// at once, so their numbers stay comparable with the ones taken before this
	// field existed. A zero value is that handler.
	HandlerWork time.Duration
	// RetryTier is the retry delay the retry measurement configures as the only
	// tier of a two-attempt ladder, so the handler's failed first attempt comes
	// back after exactly this delay. Other measurements keep the shipped
	// ladder, and a retry measurement without a tier is refused.
	RetryTier time.Duration
	// Timeout bounds one measurement, from client construction to drain.
	Timeout time.Duration
}

// Result is one measurement's output.
type Result struct {
	// Messages is the corpus size of the measurement.
	Messages int
	// Publishers is the number of concurrent publishers that loaded the corpus.
	Publishers int
	// Concurrency is the handler concurrency of the one lane.
	Concurrency int
	// HandlerWork is the handler hold time the measurement configured. The
	// consume measurement is the one that applies it, and a zero value is a
	// handler that returns at once.
	HandlerWork time.Duration
	// Elapsed is the measured window.
	Elapsed time.Duration
	// Latencies is one publish-to-ack duration per message, in sequence order.
	// It is populated only by the latency measurement.
	Latencies []time.Duration
	// RetryTier is the retry delay the measurement configured, and zero when it
	// kept the shipped ladder.
	RetryTier time.Duration
}

// Rate reports messages per second over the measured window.
func (r Result) Rate() float64 {
	if r.Elapsed <= 0 {
		return 0
	}
	return float64(r.Messages) / r.Elapsed.Seconds()
}

// Percentile returns the p-th percentile of the recorded latencies by nearest
// rank: the smallest recorded latency that at least p percent of the samples
// are at or below. A result without latencies has no percentile and returns
// zero.
func (r Result) Percentile(p float64) time.Duration {
	if len(r.Latencies) == 0 {
		return 0
	}
	ranked := slices.Clone(r.Latencies)
	slices.Sort(ranked)
	rank := int(math.Ceil(p / 100 * float64(len(ranked))))
	rank = min(max(rank, 1), len(ranked))
	return ranked[rank-1]
}

// Harness runs one measurement at a time for one driver and one namespace.
//
// A harness is not safe for concurrent use, and it is meant to be used once:
// it names its topic and subscription when it is built, deletes them when
// Close is called, and one measurement per name is what keeps a later run from
// reading an earlier run's messages or offsets.
type Harness struct {
	cfg          Config
	drv          driver.Driver
	clk          clock.Clock
	topic        string
	subscription string

	// declared holds every destination the core asked the driver to create, so
	// cleanup deletes exactly what this run made. Reading the names back from
	// the topology specs keeps the harness from restating the core's naming
	// rules, which is what a hand-written name list would drift from.
	mu       sync.Mutex
	declared map[string]struct{}
}

// New returns a harness for one driver and one measurement shape.
func New(drv driver.Driver, cfg Config) (*Harness, error) {
	if drv == nil {
		return nil, errors.New("bench: driver is nil")
	}
	if err := validateNamespace(cfg.Namespace); err != nil {
		return nil, err
	}
	if cfg.Messages < 1 {
		return nil, fmt.Errorf("bench: messages must be at least 1, got %d", cfg.Messages)
	}
	if cfg.Publishers < 1 {
		return nil, fmt.Errorf("bench: publishers must be at least 1, got %d", cfg.Publishers)
	}
	if cfg.Concurrency < 0 {
		return nil, fmt.Errorf("bench: concurrency must not be negative, got %d", cfg.Concurrency)
	}
	if cfg.RetryTier < 0 {
		return nil, fmt.Errorf("bench: retry tier must not be negative, got %s", cfg.RetryTier)
	}
	if cfg.HandlerWork < 0 {
		return nil, fmt.Errorf("bench: handler work must not be negative, got %s", cfg.HandlerWork)
	}
	if cfg.Timeout <= 0 {
		return nil, errors.New("bench: timeout must be positive")
	}
	name := fmt.Sprintf("%s-%d", cfg.Namespace, sequence.Add(1))
	return &Harness{
		cfg:          cfg,
		drv:          drv,
		clk:          clock.NewReal(),
		topic:        name,
		subscription: name,
		declared:     make(map[string]struct{}),
	}, nil
}

// Publish measures single-message publish throughput with cfg.Publishers
// concurrent publishers and no consumer.
//
// The window starts immediately before the first publish of the corpus and
// stops when the last publish call returns.
func (h *Harness) Publish(ctx context.Context) (Result, error) {
	return h.measure(ctx, modePublish)
}

// Consume measures consume-and-settle throughput for one runner on one lane.
//
// The window starts immediately before the first publish of the corpus, after
// the subscription is declared and the runner is ready, and stops when the
// last of the corpus has settled.
func (h *Harness) Consume(ctx context.Context) (Result, error) {
	return h.measure(ctx, modeConsume)
}

// Latency measures end-to-end latency, from the publish call to the returned
// ack.
//
// The window starts immediately before the first publish of the corpus and
// stops when the last of the corpus has settled. Each message is published
// only after the previous one settled, so the recorded durations are the round
// trips a caller would see rather than the arrival spread of a burst.
func (h *Harness) Latency(ctx context.Context) (Result, error) {
	return h.measure(ctx, modeLatency)
}

// Retry measures retry-path throughput, with a handler that fails every
// message once and succeeds on its retry copy.
//
// The window starts immediately before the first publish of the corpus and
// stops when the last message's retry copy has settled, so the elapsed time
// contains one tier delay per message. The tier delay is reported beside the
// rate rather than subtracted from it.
func (h *Harness) Retry(ctx context.Context) (Result, error) {
	if h.cfg.RetryTier <= 0 {
		return Result{}, errors.New("bench: retry measurement needs a retry tier")
	}
	return h.measure(ctx, modeRetry)
}

// Close deletes the destinations this harness's runs created, so a later run
// never reads this run's messages or offsets. A destination that was never
// created is skipped: the run did not make it.
func (h *Harness) Close(ctx context.Context) error {
	names := h.declaredNames()
	if len(names) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	conn, err := h.drv.Open(ctx, driver.Config{
		Endpoints:      h.endpoints(),
		ClientID:       "f1-bench-cleanup",
		ConnectTimeout: connectTimeout,
	})
	if err != nil {
		return fmt.Errorf("bench: cleanup open %s: %w", h.drv.Name(), err)
	}
	defer func() {
		// A connection that will not close still leaves the destinations
		// deleted on the broker, which is what cleanup owes.
		_ = conn.Close(context.WithoutCancel(ctx))
	}()
	maintenance, ok := conn.Admin().(driver.Maintenance)
	if !ok {
		return fmt.Errorf("bench: %s admin does not implement driver.Maintenance", h.drv.Name())
	}
	remaining := purge(ctx, maintenance, names)
	for attempt := 0; attempt < pruneAttempts && len(remaining) > 0; attempt++ {
		remaining = prune(ctx, maintenance, remaining)
		if len(remaining) == 0 {
			return nil
		}
		if err := h.clk.Sleep(ctx, pruneWait); err != nil {
			break
		}
	}
	if len(remaining) == 0 {
		return nil
	}
	return fmt.Errorf("bench: %d destinations still present after %d prune attempts: %s",
		len(remaining), pruneAttempts, strings.Join(remaining, ", "))
}

// purge empties the named destinations and returns the ones that may still be
// deleted. A destination that could not be emptied is kept rather than
// dropped: pruning refuses one that holds messages, and the retry in Close is
// what turns that refusal into a result instead of a silent leak.
func purge(ctx context.Context, maintenance driver.Maintenance, names []string) []string {
	remaining := make([]string, 0, len(names))
	for _, name := range names {
		if _, err := maintenance.Purge(ctx, name); err != nil && errors.Is(err, driver.ErrDestinationMissing) {
			continue
		}
		remaining = append(remaining, name)
	}
	return remaining
}

func prune(ctx context.Context, maintenance driver.Maintenance, names []string) []string {
	results, err := maintenance.Prune(ctx, names)
	if err != nil {
		return names
	}
	deleted := make(map[string]struct{}, len(results))
	for _, result := range results {
		if result.Deleted {
			deleted[result.Name] = struct{}{}
		}
	}
	remaining := make([]string, 0, len(names))
	for _, name := range names {
		if _, ok := deleted[name]; !ok {
			remaining = append(remaining, name)
		}
	}
	return remaining
}

func (h *Harness) declaredNames() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	names := make([]string, 0, len(h.declared))
	for name := range h.declared {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func (h *Harness) noteDeclared(destinations []driver.DestinationSpec) {
	if len(destinations) == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, destination := range destinations {
		h.declared[destination.Name] = struct{}{}
	}
}

func (h *Harness) endpoints() []string {
	if h.cfg.Endpoint == "" {
		return nil
	}
	return []string{h.cfg.Endpoint}
}

func validateNamespace(namespace string) error {
	if namespace == "" {
		return errors.New("bench: namespace must not be empty")
	}
	for _, char := range namespace {
		switch {
		case char >= 'a' && char <= 'z', char >= '0' && char <= '9':
		case char == '.', char == '_', char == '-':
		default:
			return fmt.Errorf("bench: namespace %q contains %q, which a broker destination name cannot carry", namespace, char)
		}
	}
	return nil
}

// clientConfig is the configuration every measurement runs on: the shipped
// defaults except where a measurement needs a known shape, and one lane.
func (h *Harness) clientConfig() f1.Config {
	return f1.Config{
		Env:        "bench",
		Service:    "bench",
		InstanceID: h.subscription,
		Broker: f1.BrokerConfig{
			Driver:         h.drv.Name(),
			Endpoints:      h.endpoints(),
			ConnectTimeout: connectTimeout,
		},
		Topology: f1.TopologyConfig{
			AutoCreate: true,
			Priorities: []f1.Priority{lane},
		},
		Codec: f1.CodecConfig{
			Default:        "json",
			ContentMode:    "binary",
			MaxHeaderBytes: f1.CoreMaxHeaderBytes,
			MaxBodyBytes:   1 << 20,
		},
		Lifecycle: f1.LifecycleConfig{
			DrainTimeout: drainTimeout,
			HandlerGrace: handlerTimeout,
			CloseTimeout: closeTimeout,
		},
	}
}

// mode selects one measurement's handler policy and publish shape.
type mode int

const (
	modePublish mode = iota
	modeConsume
	modeLatency
	modeRetry
)

// attempts is the number of times one corpus message is published, delivered
// and settled in this measurement. The retry path is the one shape where that
// is more than one: the core republishes the body to a retry destination and
// settles both deliveries.
func (m mode) attempts() int {
	if m == modeRetry {
		return 2
	}
	return 1
}

// run is one measurement's live state: the tracker that counts at the port, the
// client, and the subscription runner.
type run struct {
	h      *Harness
	m      mode
	tr     *tracker
	client *f1.Client

	ready     chan struct{}
	readyOnce sync.Once
	runner    *f1.Runner
	runDone   chan struct{}
	runErr    error
	// drained reports that the subscription's consumer has already been
	// stopped, so stop does not drain a runner that is finished.
	drained bool

	stopOnce sync.Once
	stopErr  error
}

func (h *Harness) measure(ctx context.Context, m mode) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, h.cfg.Timeout)
	defer cancel()
	r := &run{
		h:     h,
		m:     m,
		tr:    newTracker(h.clk, h.cfg.Messages, h.cfg.Messages*m.attempts()),
		ready: make(chan struct{}),
	}
	client, err := f1.New(ctx, h.clientConfig(),
		f1.WithDriver(r.wrappedDriver()),
		f1.WithPublishTopics(h.topic),
		f1.WithLogger(benchmarkLogger()),
	)
	if err != nil {
		return Result{}, fmt.Errorf("bench: %s new client: %w", h.drv.Name(), err)
	}
	r.client = client
	defer func() {
		// The explicit stops below carry the error that matters; this backstop
		// only keeps a connection from outliving a measurement that returned
		// before reaching one.
		_ = r.stop(ctx)
	}()
	if err := r.subscribe(ctx); err != nil {
		return Result{}, errors.Join(err, r.stop(ctx))
	}
	// One warm-up message settles outside the window, before the clocks below
	// start. It proves the entry point routes to a bound destination and the
	// consumer is attached and joined: a broker whose subscription becomes
	// ready when its topology is declared, and not when its consumer joins,
	// would otherwise have that join charged to the first message of the
	// corpus. The tracker records nothing before the window opens, so the
	// warm-up message and its settlement - one settlement, or two on the retry
	// path - stay out of the corpus count.
	if err := r.warmUp(ctx); err != nil {
		return Result{}, errors.Join(err, r.stop(ctx))
	}
	if m == modePublish {
		// A publish measurement needs the subscription's topology and not its
		// consumer. On a broker that fans out at publish time the entry point
		// is an exchange, and a publish nothing is bound to comes back
		// unroutable, so the subscription has to be declared first. Its
		// consumer is drained away once the warm-up has proved it is joined:
		// leaving it running would put a second workload on the machine whose
		// publish rate is being measured, and a group left before it has
		// joined cannot be left at all.
		if err := r.drainRunner(ctx); err != nil {
			return Result{}, errors.Join(err, r.stop(ctx))
		}
	}
	start := h.clk.Now()
	r.tr.arm()
	if m == modeLatency {
		err = r.publishSequential(ctx)
	} else {
		err = r.publishBulk(ctx)
	}
	if err != nil {
		return Result{}, errors.Join(err, r.stop(ctx))
	}
	if m == modePublish {
		elapsed := h.clk.Since(start)
		atEnd := r.tr.snapshot()
		if err := r.stop(ctx); err != nil {
			return Result{}, err
		}
		if err := atEnd.verifyPublished(h.cfg.Messages, "when the measurement ended"); err != nil {
			return Result{}, err
		}
		return Result{
			Messages:    h.cfg.Messages,
			Publishers:  h.cfg.Publishers,
			Concurrency: h.cfg.Concurrency,
			Elapsed:     elapsed,
		}, nil
	}
	if err := r.await(ctx, r.tr.hasStopped); err != nil {
		return Result{}, errors.Join(err, r.stop(ctx))
	}
	elapsed := r.tr.stopTime().Sub(start)
	atStop := r.tr.snapshot()
	if err := r.stop(ctx); err != nil {
		return Result{}, err
	}
	if err := atStop.verify(h.cfg.Messages, m.attempts(), "when the clock stopped"); err != nil {
		return Result{}, err
	}
	if err := r.tr.snapshot().verify(h.cfg.Messages, m.attempts(), "after the run stopped"); err != nil {
		return Result{}, err
	}
	return Result{
		Messages:    h.cfg.Messages,
		Publishers:  h.cfg.Publishers,
		Concurrency: h.cfg.Concurrency,
		HandlerWork: h.cfg.HandlerWork,
		Elapsed:     elapsed,
		Latencies:   r.tr.latencies(),
		RetryTier:   h.cfg.RetryTier,
	}, nil
}

func (r *run) wrappedDriver() *countingDriver {
	return &countingDriver{
		inner:     r.h.drv,
		tr:        r.tr,
		ready:     r.ready,
		readyOnce: &r.readyOnce,
		onSpec:    r.h.noteDeclared,
	}
}

// subscribe starts the measurement's subscription and blocks until its
// topology has been declared. Every measurement needs the subscription: the
// publish measurement for the destination its entry point routes to, and the
// others for the consumer that settles the corpus.
func (r *run) subscribe(ctx context.Context) error {
	sub := f1.Subscription{
		Name:           r.h.subscription,
		Topics:         []string{r.h.topic},
		Priorities:     []f1.Priority{lane},
		Concurrency:    r.h.cfg.Concurrency,
		HandlerTimeout: handlerTimeout,
		Handlers:       map[string]f1.Handler{r.h.topic: f1.HandlerFunc(r.handle)},
	}
	if r.m == modeRetry {
		// Two attempts, one tier: the first delivery fails and the retry copy
		// arrives after exactly the delay the measurement reports.
		sub.Retry = f1.RetryConfig{MaxAttempts: 2, Tiers: []time.Duration{r.h.cfg.RetryTier}}
	}
	runner, err := r.client.Subscribe(ctx, sub)
	if err != nil {
		return fmt.Errorf("bench: subscribe: %w", err)
	}
	r.runner = runner
	r.runDone = make(chan struct{})
	go func() {
		defer close(r.runDone)
		r.runErr = runner.Run(ctx)
	}()
	select {
	case <-r.ready:
		return nil
	case <-r.runDone:
		return fmt.Errorf("bench: subscription stopped before it was declared: %w", r.runError())
	case <-ctx.Done():
		return fmt.Errorf("bench: subscription was not declared: %w", ctx.Err())
	}
}

// handle is the subscription's handler. It records the delivery, holds it for
// the configured work when this measurement applies one, and for the retry
// measurement fails every first attempt.
//
// The hold is a wait on the harness clock, and the context's error is returned
// when that wait is cut short: a delivery whose wait was interrupted was not
// handled, and settling it would count a message the handler never finished.
func (r *run) handle(ctx context.Context, event *f1.Event) error {
	r.tr.noteDelivery(string(event.Raw()))
	if r.m == modeConsume && r.h.cfg.HandlerWork > 0 {
		if err := r.h.clk.Sleep(ctx, r.h.cfg.HandlerWork); err != nil {
			return err
		}
	}
	if r.m == modeRetry && event.Attempt() < 2 {
		return errors.New("bench: forced retry")
	}
	return nil
}

// publishBulk distributes the corpus across the configured publishers.
func (r *run) publishBulk(ctx context.Context) error {
	var (
		wait   sync.WaitGroup
		mu     sync.Mutex
		failed error
	)
	for publisher := range r.h.cfg.Publishers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for seq := publisher; seq < r.h.cfg.Messages; seq += r.h.cfg.Publishers {
				if _, err := r.client.Publisher().Publish(ctx, r.h.topic, payload{Seq: seq}); err != nil {
					mu.Lock()
					failed = errors.Join(failed, fmt.Errorf("bench: publish %d: %w", seq, err))
					mu.Unlock()
					return
				}
			}
		}()
	}
	wait.Wait()
	return failed
}

// publishSequential publishes one message at a time and waits for each one to
// settle before publishing the next, which is what makes the recorded
// durations round trips rather than queue depths.
func (r *run) publishSequential(ctx context.Context) error {
	for seq := range r.h.cfg.Messages {
		r.tr.notePublishedAt(seq, r.h.clk.Now())
		if _, err := r.client.Publisher().Publish(ctx, r.h.topic, payload{Seq: seq}); err != nil {
			return fmt.Errorf("bench: publish %d: %w", seq, err)
		}
		settled := seq + 1
		if err := r.await(ctx, func() bool { return r.tr.settledTotal() >= settled }); err != nil {
			return err
		}
	}
	return nil
}

// await blocks until check holds, the subscription stops, or ctx expires. It
// wakes on the tracker's own signal rather than re-checking on a timer, so a
// measurement never spends a core asking whether it is finished.
func (r *run) await(ctx context.Context, check func() bool) error {
	for {
		changed := r.tr.changed()
		if check() {
			return nil
		}
		select {
		case <-changed:
		case <-r.runDone:
			return fmt.Errorf("bench: subscription stopped before the measurement finished: %w", r.runError())
		case <-ctx.Done():
			return fmt.Errorf("bench: awaiting settlement: %w", ctx.Err())
		}
	}
}

func (r *run) runError() error {
	if r.runErr == nil {
		return errors.New("runner returned")
	}
	return r.runErr
}

// warmUp publishes one corpus message and waits for it to settle, which proves
// the entry point routes to a bound destination and the consumer is attached.
// It runs outside every measured window.
func (r *run) warmUp(ctx context.Context) error {
	if _, err := r.client.Publisher().Publish(ctx, r.h.topic, payload{Seq: 0}); err != nil {
		return fmt.Errorf("bench: warm-up publish: %w", err)
	}
	want := r.m.attempts()
	return r.await(ctx, func() bool { return r.tr.preludeSettlements() >= want })
}

// drainRunner stops the subscription's consumer and leaves the client, and the
// destinations the subscription declared, in place. A runner that ended on its
// own reports why: a failed release leaves the consumer open, and a driver that
// refuses to close a connection with a consumer on it would otherwise surface
// only as a cleanup failure.
func (r *run) drainRunner(ctx context.Context) error {
	if r.runner == nil {
		return nil
	}
	drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), drainTimeout)
	defer cancel()
	if err := r.runner.Drain(drainCtx); err != nil {
		return fmt.Errorf("bench: drain before publishing: %w", err)
	}
	select {
	case <-r.runDone:
	case <-drainCtx.Done():
		return fmt.Errorf("bench: runner did not stop after draining: %w", drainCtx.Err())
	}
	r.drained = true
	return r.runErr
}

// stop drains the subscription and closes the client exactly once.
func (r *run) stop(ctx context.Context) error {
	r.stopOnce.Do(func() {
		r.stopErr = r.stopNow(ctx)
	})
	return r.stopErr
}

func (r *run) stopNow(ctx context.Context) error {
	var problems []error
	parent := context.WithoutCancel(ctx)
	if r.runner != nil && !r.drained {
		drainCtx, cancel := context.WithTimeout(parent, drainTimeout)
		if err := r.runner.Drain(drainCtx); err != nil {
			problems = append(problems, fmt.Errorf("bench: drain: %w", err))
		}
		select {
		case <-r.runDone:
		case <-drainCtx.Done():
			problems = append(problems, fmt.Errorf("bench: runner did not stop: %w", drainCtx.Err()))
		}
		cancel()
	}
	if r.client != nil {
		closeCtx, cancel := context.WithTimeout(parent, closeTimeout)
		if err := r.client.Close(closeCtx); err != nil {
			problems = append(problems, fmt.Errorf("bench: close client: %w", err))
		}
		cancel()
	}
	return errors.Join(problems...)
}

// benchmarkLogger reports the driver diagnostics a benchmark run has to show.
// The core logs a routine notification - a Kafka group assignment, among
// others - at info level for every consumer it opens, and a benchmark's output
// is the numbers it reports. Warnings and errors still reach stderr: a
// capability the driver does not have, or a condition it survived, is a reason
// to read a number differently.
func benchmarkLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// payload is one corpus message. Its encoded form is what the tracker keys a
// publish, a delivery and a settlement on, so a message and its retry copy -
// republished with the same body and one more attempt - land on one sequence.
type payload struct {
	Seq int `json:"seq"`
}

// corpusBody is the encoded form of one corpus payload. The core encodes the
// payload with the JSON codec, whose encoding of this single-field struct is
// exactly this, so the harness recognises a delivery without decoding every
// body. A mismatch is not silent: the body is then unknown to the tracker and
// the correctness check fails on it.
func corpusBody(seq int) string {
	return fmt.Sprintf(`{"seq":%d}`, seq)
}

// tracker records what one measurement published, delivered and settled, keyed
// by the corpus sequence a body carries.
type tracker struct {
	clk  clock.Clock
	want int

	mu          sync.Mutex
	seqByBody   map[string]int
	publishes   []int
	deliveries  []int
	settles     []int
	publishedAt []time.Time
	settledAt   []time.Time

	unknownPublishes   int
	unknownDeliveries  int
	unknownSettlements int

	stopped       bool
	stopAt        time.Time
	settledAtStop int

	// recording is false until the measured window opens, so the warm-up a
	// measurement may run before it is not counted as part of the corpus.
	recording bool

	// prelude counts settlements recorded before the window opened. A
	// measurement waits for its warm-up to finish through this counter, so the
	// wait does not depend on the corpus counts the warm-up is excluded from.
	prelude int

	change chan struct{}
}

func newTracker(clk clock.Clock, corpus, want int) *tracker {
	seqByBody := make(map[string]int, corpus)
	for seq := range corpus {
		seqByBody[corpusBody(seq)] = seq
	}
	return &tracker{
		clk:         clk,
		want:        want,
		seqByBody:   seqByBody,
		publishes:   make([]int, corpus),
		deliveries:  make([]int, corpus),
		settles:     make([]int, corpus),
		publishedAt: make([]time.Time, corpus),
		settledAt:   make([]time.Time, corpus),
		change:      make(chan struct{}),
	}
}

// arm opens the measured window. Everything recorded before it - the warm-up a
// publish measurement runs, and the settlement it waits for - is dropped, so a
// corpus count covers exactly one window.
func (t *tracker) arm() {
	t.mu.Lock()
	defer t.mu.Unlock()
	clear(t.publishes)
	clear(t.deliveries)
	clear(t.settles)
	clear(t.publishedAt)
	clear(t.settledAt)
	t.prelude = 0
	t.recording = true
	t.signalLocked()
}

// preludeSettlements reports how many settlements were recorded before the
// measured window opened.
func (t *tracker) preludeSettlements() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.prelude
}

// changed returns a channel that the next recorded event closes.
func (t *tracker) changed() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.change
}

func (t *tracker) signalLocked() {
	close(t.change)
	t.change = make(chan struct{})
}

func (t *tracker) notePublishedAt(seq int, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.recording {
		return
	}
	t.publishedAt[seq] = at
}

// notePublish records the messages one producer call published. A partial
// failure published only the indexes the driver did not report as failed.
func (t *tracker) notePublish(messages []driver.OutboundMessage, err error) {
	var failed map[int]error
	if partial, ok := errors.AsType[*driver.PublishError](err); ok {
		failed = partial.Failed
	} else if err != nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.recording {
		return
	}
	for index, message := range messages {
		if _, skip := failed[index]; skip {
			continue
		}
		seq, known := t.seqByBody[string(message.Body)]
		if !known {
			t.unknownPublishes++
			continue
		}
		t.publishes[seq]++
	}
	t.signalLocked()
}

func (t *tracker) noteDelivery(body string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.recording {
		return
	}
	seq, known := t.seqByBody[body]
	if !known {
		t.unknownDeliveries++
	} else {
		t.deliveries[seq]++
	}
	t.signalLocked()
}

// noteSettled records one Ack that returned nil and stops the measurement when
// the expected number of settlements has returned. This is the only place a
// measurement ends: a delivery, or a handler that has been entered, is a step
// before settlement, and stopping there would report a rate for work that has
// not finished.
func (t *tracker) noteSettled(body string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.recording {
		t.prelude++
		t.signalLocked()
		return
	}
	seq, known := t.seqByBody[body]
	if !known {
		t.unknownSettlements++
		t.signalLocked()
		return
	}
	t.settles[seq]++
	if t.settledAt[seq].IsZero() {
		t.settledAt[seq] = t.clk.Now()
	}
	t.stopIfReachedLocked(t.settledTotalLocked())
	t.signalLocked()
}

// stopIfReachedLocked stops the measurement once count has reached the number
// of settlements the corpus expects.
func (t *tracker) stopIfReachedLocked(count int) {
	if t.stopped || count < t.want {
		return
	}
	t.stopped = true
	t.stopAt = t.clk.Now()
	t.settledAtStop = t.settledTotalLocked()
}

func (t *tracker) settledTotalLocked() int {
	total := 0
	for _, count := range t.settles {
		total += count
	}
	return total
}

func (t *tracker) settledTotal() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.settledTotalLocked()
}

func (t *tracker) hasStopped() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopped
}

func (t *tracker) stopTime() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopAt
}

// latencies returns each message's publish-to-ack duration, in sequence order.
func (t *tracker) latencies() []time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	latencies := make([]time.Duration, 0, len(t.settles))
	for seq := range t.settles {
		if t.settledAt[seq].IsZero() || t.publishedAt[seq].IsZero() {
			continue
		}
		latencies = append(latencies, t.settledAt[seq].Sub(t.publishedAt[seq]))
	}
	return latencies
}

// snapshot is the tracker's state at one instant.
type snapshot struct {
	publishes  []int
	deliveries []int
	settles    []int

	unknownPublishes   int
	unknownDeliveries  int
	unknownSettlements int

	stopped       bool
	settledAtStop int
}

func (t *tracker) snapshot() snapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	return snapshot{
		publishes:          slices.Clone(t.publishes),
		deliveries:         slices.Clone(t.deliveries),
		settles:            slices.Clone(t.settles),
		unknownPublishes:   t.unknownPublishes,
		unknownDeliveries:  t.unknownDeliveries,
		unknownSettlements: t.unknownSettlements,
		stopped:            t.stopped,
		settledAtStop:      t.settledAtStop,
	}
}

// verifyPublished checks what a publish measurement can claim: every corpus
// message was published exactly once, and nothing else was.
func (s snapshot) verifyPublished(corpus int, when string) error {
	var problems []error
	for seq := range corpus {
		if s.publishes[seq] != 1 {
			problems = append(problems, fmt.Errorf("%s: message %d was published %d times, want 1", when, seq, s.publishes[seq]))
		}
	}
	if s.unknownPublishes != 0 {
		problems = append(problems, fmt.Errorf("%s: %d published bodies were not corpus messages", when, s.unknownPublishes))
	}
	return errors.Join(problems...)
}

// verify checks the measurement's correctness claim at one instant: at the
// instant the clock stopped every corpus message had settled, and every corpus
// message was published, delivered and settled exactly once per attempt, with
// nothing that was not published arriving at all. when names the instant, so a
// failure says which check saw it.
func (s snapshot) verify(corpus, attempts int, when string) error {
	var problems []error
	if s.stopped && s.settledAtStop != corpus*attempts {
		problems = append(problems, fmt.Errorf(
			"%s: %d deliveries had settled, want %d: the clock stopped before every message settled",
			when, s.settledAtStop, corpus*attempts))
	}
	for seq := range corpus {
		if s.publishes[seq] != attempts {
			problems = append(problems, fmt.Errorf("%s: message %d was published %d times, want %d", when, seq, s.publishes[seq], attempts))
		}
		if s.deliveries[seq] != attempts {
			problems = append(problems, fmt.Errorf("%s: message %d was delivered %d times, want %d", when, seq, s.deliveries[seq], attempts))
		}
		if s.settles[seq] != attempts {
			problems = append(problems, fmt.Errorf("%s: message %d was settled %d times, want %d", when, seq, s.settles[seq], attempts))
		}
	}
	if s.unknownPublishes != 0 {
		problems = append(problems, fmt.Errorf("%s: %d published bodies were not corpus messages", when, s.unknownPublishes))
	}
	if s.unknownDeliveries != 0 {
		problems = append(problems, fmt.Errorf("%s: %d deliveries carried a body that was never published", when, s.unknownDeliveries))
	}
	if s.unknownSettlements != 0 {
		problems = append(problems, fmt.Errorf("%s: %d settlements carried a body that was never published", when, s.unknownSettlements))
	}
	return errors.Join(problems...)
}

// countingDriver wraps the driver under test. It changes no behaviour: it
// forwards the driver value, the connection, and the admin with their meaning
// intact, and only records what crosses the port.
type countingDriver struct {
	inner     driver.Driver
	tr        *tracker
	ready     chan struct{}
	readyOnce *sync.Once
	onSpec    func([]driver.DestinationSpec)
}

func (d *countingDriver) Name() string { return d.inner.Name() }

func (d *countingDriver) Capabilities() driver.Capabilities { return d.inner.Capabilities() }

func (d *countingDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	conn, err := d.inner.Open(ctx, cfg)
	if err != nil || conn == nil {
		return conn, err
	}
	return &countingConn{Conn: conn, tr: d.tr, ready: d.ready, readyOnce: d.readyOnce, onSpec: d.onSpec}, nil
}

type countingConn struct {
	driver.Conn
	tr        *tracker
	ready     chan struct{}
	readyOnce *sync.Once
	onSpec    func([]driver.DestinationSpec)
}

func (c *countingConn) Admin() driver.Admin {
	admin := c.Conn.Admin()
	if admin == nil {
		return nil
	}
	return &countingAdmin{Admin: admin, ready: c.ready, readyOnce: c.readyOnce, onSpec: c.onSpec}
}

func (c *countingConn) Producer(ctx context.Context, cfg driver.ProducerConfig) (driver.Producer, error) {
	producer, err := c.Conn.Producer(ctx, cfg)
	if err != nil || producer == nil {
		return producer, err
	}
	return &countingProducer{Producer: producer, tr: c.tr}, nil
}

func (c *countingConn) Consumer(ctx context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	consumer, err := c.Conn.Consumer(ctx, cfg)
	if err != nil || consumer == nil {
		return consumer, err
	}
	wrapped := &countingConsumer{
		Consumer: consumer,
		tr:       c.tr,
		out:      make(chan driver.InboundMessage),
		stopped:  make(chan struct{}),
	}
	go wrapped.pump()
	return wrapped, nil
}

type countingProducer struct {
	driver.Producer
	tr *tracker
}

func (p *countingProducer) Publish(ctx context.Context, messages ...driver.OutboundMessage) error {
	err := p.Producer.Publish(ctx, messages...)
	p.tr.notePublish(messages, err)
	return err
}

// countingAdmin records the destinations the core asks for, which is what
// cleanup later deletes, and signals readiness when the subscription's own
// topology has been declared.
//
// The scope is the marker: the core sets it only on a subscription's topology,
// and the runner returns from this call before it opens a consumer, so a
// publish that waits for this signal cannot arrive before the destinations and
// their bindings exist.
type countingAdmin struct {
	driver.Admin
	ready     chan struct{}
	readyOnce *sync.Once
	onSpec    func([]driver.DestinationSpec)
}

func (a *countingAdmin) EnsureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	diff, err := a.Admin.EnsureTopology(ctx, spec)
	if err != nil {
		return diff, err
	}
	if a.onSpec != nil {
		a.onSpec(spec.Destinations)
	}
	if a.ready != nil && len(spec.Scope) > 0 {
		a.readyOnce.Do(func() { close(a.ready) })
	}
	return diff, nil
}

// countingConsumer forwards every consumer verb and rewrites the settlement of
// each delivery, so a message counts as settled only when its Ack returns nil.
type countingConsumer struct {
	driver.Consumer
	tr      *tracker
	out     chan driver.InboundMessage
	stopped chan struct{}
	once    sync.Once
}

func (c *countingConsumer) Messages() <-chan driver.InboundMessage { return c.out }

// pump copies deliveries from the driver onto the channel the core reads, which
// is the only place the harness can reach the Settle of a delivery it did not
// create.
//
// It stops when the driver closes its channel, and also when Stop or Release
// has returned, so a core that has stopped reading cannot leave this goroutine
// blocked on a send that will never be taken.
func (c *countingConsumer) pump() {
	defer close(c.out)
	source := c.Consumer.Messages()
	for {
		select {
		case message, ok := <-source:
			if !ok {
				return
			}
			if message.Settle != nil {
				message.Settle = countingSettler{inner: message.Settle, tr: c.tr, body: string(message.Body)}
			}
			select {
			case c.out <- message:
			case <-c.stopped:
				return
			}
		case <-c.stopped:
			return
		}
	}
}

func (c *countingConsumer) Stop(ctx context.Context) error {
	err := c.Consumer.Stop(ctx)
	c.once.Do(func() { close(c.stopped) })
	return err
}

func (c *countingConsumer) Release(ctx context.Context) error {
	err := c.Consumer.Release(ctx)
	c.once.Do(func() { close(c.stopped) })
	return err
}

// countingSettler records a settlement when Ack returns nil. Nack is forwarded
// untouched: a delivery the broker was told to take back was not settled, and
// counting it would report a rate the settlement path never reached.
type countingSettler struct {
	inner driver.Settler
	tr    *tracker
	body  string
}

func (s countingSettler) Ack(ctx context.Context) error {
	err := s.inner.Ack(ctx)
	if err == nil {
		s.tr.noteSettled(s.body)
	}
	return err
}

func (s countingSettler) Nack(ctx context.Context, options driver.NackOptions) error {
	return s.inner.Nack(ctx, options)
}
