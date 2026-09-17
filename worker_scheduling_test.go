package f1_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"

	//nolint:depguard // these tests exercise the runner through the in-memory driver.
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/testhook"
)

const (
	schedulingWindow  = 100
	schedulingBacklog = 200
	// schedulingPace keeps the handler slower than the in-memory pump's refill,
	// so both lanes stay backlogged for the whole window and the window measures
	// the scheduler's choice. Unpaced, the runner drains the lane the pump
	// prefers first and the window measures the feed instead: the same weights
	// measured 53:47 to 78:22 across runs, against a deterministic 89:11 paced.
	schedulingPace = 50 * time.Microsecond
	// schedulingShareTolerance is 5 points around the weight ratio. One deficit
	// round is 9 picks (8 favoured, 1 other) and 100 picks is 11 rounds plus one,
	// so the measured count is 88 or 89 depending on where the cursor starts and
	// which lane fills first. The pre-change pipeline measured 64:36 here, which
	// stays outside the band.
	schedulingShareTolerance = 5
	// schedulingAgeMargin is the clock step the ageing test applies per handled
	// message: larger than the low priority's budget and far smaller than the
	// high priority's, so exactly the low item can be promoted.
	schedulingAgeMargin = 200 * time.Millisecond
	// schedulingAgeBound is how many dispatches the aged low item may sit
	// behind: the pick after the advance that ages it is the dispatch after
	// next, and the feed can take up to two dispatches to place it in a lane.
	// Measured 3-4 over 40 runs with promotion, against 9-10 without it, where
	// the item waits for the high slot's 8-pick deficit round instead.
	schedulingAgeBound = 6
)

func TestRunnerSchedulerWeightedShare(t *testing.T) {
	first := runPriorityShare(t, map[f1.Priority]int{
		f1.PriorityHigh: 8,
		f1.PriorityLow:  1,
	})
	high := countPriority(first, f1.PriorityHigh)
	low := countPriority(first, f1.PriorityLow)
	const wantHigh = 89
	if high < wantHigh-schedulingShareTolerance || high > wantHigh+schedulingShareTolerance || high+low != schedulingWindow {
		t.Fatalf("first %d priorities = high:%d low:%d, want high:%d +/- %d", schedulingWindow, high, low, wantHigh, schedulingShareTolerance)
	}
}

func TestRunnerSchedulerWeightedShareReversed(t *testing.T) {
	first := runPriorityShare(t, map[f1.Priority]int{
		f1.PriorityHigh: 1,
		f1.PriorityLow:  8,
	})
	high := countPriority(first, f1.PriorityHigh)
	low := countPriority(first, f1.PriorityLow)
	const wantLow = 89
	if low < wantLow-schedulingShareTolerance || low > wantLow+schedulingShareTolerance || high+low != schedulingWindow {
		t.Fatalf("first %d priorities = high:%d low:%d, want low:%d +/- %d", schedulingWindow, high, low, wantLow, schedulingShareTolerance)
	}
}

func TestRunnerSchedulerAgesLowPriorityWork(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := f1.New(ctx, schedulingConfig(), f1.WithDriver(testhook.Driver(fake)), testhook.ClientOption(fake).(f1.Option))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()

	const highBacklog = 32
	started := make(chan struct{})
	release := make(chan struct{})
	dispatched := make(chan f1.Priority, schedulingAgeBound)
	var highHandled atomic.Int64
	var position atomic.Int64
	var firstOnce sync.Once
	sub := schedulingSubscription(map[f1.Priority]int{
		f1.PriorityHigh: 8,
		f1.PriorityLow:  1,
	}, map[f1.Priority]time.Duration{
		f1.PriorityHigh: time.Hour,
		f1.PriorityLow:  schedulingAgeMargin / 2,
	}, func(_ context.Context, event *f1.Event) error {
		firstOnce.Do(func() {
			close(started)
			// Held until the backlog below is published, so the low item is
			// already in a lane when the clock moves past its budget.
			<-release
		})
		if event.Priority() == f1.PriorityHigh {
			highHandled.Add(1)
		}
		if position.Add(1) <= schedulingAgeBound {
			dispatched <- event.Priority()
		}
		// The runner stamps EnqueuedAt from this clock, and the pick for the
		// next dispatch happens after this handler returns. Advancing here
		// therefore ages whatever is already queued before that pick, whichever
		// way the publish-to-lane race falls.
		fake.Advance(schedulingAgeMargin)
		return nil
	})
	// Lane capacity 8 covers the high slot's whole deficit round, so an
	// unpromoted low item waits for the round to spend itself instead of
	// arriving early because the high lane ran dry.
	sub.Fairness.PrefetchFactor = 4
	runner, err := client.Subscribe(ctx, sub)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()

	publishPriorityBatch(t, client, ctx, []f1.Priority{f1.PriorityHigh})
	waitSchedulingSignalOrRunner(t, started, runDone, "first high-priority handler did not start")
	publishPriorityBatch(t, client, ctx, append([]f1.Priority{f1.PriorityLow}, repeatPriority(f1.PriorityHigh, highBacklog)...))
	close(release)

	timer := clock.NewReal().Timer(5 * time.Second)
	defer timer.Stop()
	for range schedulingAgeBound {
		select {
		case got := <-dispatched:
			if got != f1.PriorityLow {
				continue
			}
			if remaining := highBacklog - int(highHandled.Load()); remaining <= 0 {
				t.Fatalf("high backlog had %d items unhandled when the aged low item ran, want the low item ahead of backlogged %d-item high work", remaining, highBacklog)
			}
			cancel()
			waitRunnerDone(t, runDone)
			return
		case err := <-runDone:
			t.Fatalf("aged low item: Runner.Run() error = %v", err)
		case <-timer.C:
			t.Fatal("aged item was not dispatched")
		}
	}
	t.Fatalf("aged low item was not dispatched within the first %d dispatches, want it ahead of the %d-item high backlog", schedulingAgeBound, highBacklog)
}

func TestRunnerSchedulerPreservesOrderedKeys(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := f1.New(ctx, schedulingConfig(), f1.WithDriver(inmem.New()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()

	var mu sync.Mutex
	sequence := make([]int, 0, 10)
	handledDone := make(chan struct{})
	var handledOnce sync.Once
	var active atomic.Int64
	overlap := atomic.Bool{}
	runner, err := client.Subscribe(ctx, schedulingSubscriptionWithMode(f1.OrderedByKey, map[f1.Priority]int{
		f1.PriorityHigh: 1,
	}, nil, func(_ context.Context, event *f1.Event) error {
		if active.Add(1) != 1 {
			overlap.Store(true)
		}
		defer active.Add(-1)
		var payload struct {
			Sequence int `json:"sequence"`
		}
		if err := event.Decode(&payload); err != nil {
			return err
		}
		mu.Lock()
		sequence = append(sequence, payload.Sequence)
		count := len(sequence)
		mu.Unlock()
		if count == 10 {
			handledOnce.Do(func() { close(handledDone) })
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()

	messages := make([]f1.Message, 10)
	for i := range messages {
		messages[i] = f1.Message{
			EventType: "orders.created",
			Payload:   map[string]int{"sequence": i},
			Opts:      []f1.PublishOption{f1.WithPriority(f1.PriorityHigh), f1.WithKey("same-key")},
		}
	}
	publishOneByOne(t, client, ctx, messages)
	waitSchedulingSignalOrRunner(t, handledDone, runDone, "ordered messages were not handled")
	cancel()
	waitRunnerDone(t, runDone)
	if overlap.Load() {
		t.Fatal("equal-key handlers overlapped")
	}
	mu.Lock()
	defer mu.Unlock()
	for i, got := range sequence {
		if got != i {
			t.Fatalf("ordered sequence = %v, first mismatch at %d", sequence, i)
		}
	}
}

func TestRunnerSchedulerRetriesThroughPipeline(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := f1.New(ctx, schedulingConfig(), f1.WithDriver(testhook.Driver(fake)), testhook.ClientOption(fake).(f1.Option))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()

	first := make(chan struct{})
	second := make(chan int, 1)
	var firstOnce sync.Once
	runner, err := client.Subscribe(ctx, schedulingSubscriptionWithRetry(func(_ context.Context, event *f1.Event) error {
		if event.Attempt() == 1 {
			firstOnce.Do(func() { close(first) })
			return errors.New("retry me")
		}
		second <- event.Attempt()
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()
	publishPriorityBatch(t, client, ctx, []f1.Priority{f1.PriorityHigh})
	waitSchedulingSignalOrRunner(t, first, runDone, "initial retry attempt did not run")
	// The retry publish stamps its due time from this same clock, and it runs
	// after the first attempt fails, so the delay has to be stepped past rather
	// than advanced once: a single advance could land before the stamp and leave
	// the retry waiting on a timer nothing will fire again.
	timer := clock.NewReal().Timer(5 * time.Second)
	ticker := clock.NewReal().Ticker(time.Millisecond)
	defer timer.Stop()
	defer ticker.Stop()
	for {
		select {
		case attempt := <-second:
			if attempt != 2 {
				t.Fatalf("retry attempt = %d, want 2", attempt)
			}
			cancel()
			waitRunnerDone(t, runDone)
			return
		case err := <-runDone:
			t.Fatalf("retry destination: Runner.Run() error = %v", err)
		case <-timer.C:
			t.Fatal("retry destination did not return through the pipeline")
		case <-ticker.C:
			fake.Advance(2 * time.Second)
		}
	}
}

func runPriorityShare(t *testing.T, weights map[f1.Priority]int) []f1.Priority {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := f1.New(ctx, schedulingConfig(), f1.WithDriver(inmem.New()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()
	var mu sync.Mutex
	first := make([]f1.Priority, 0, schedulingWindow)
	windowDone := make(chan struct{})
	var windowOnce sync.Once
	runner, err := client.Subscribe(ctx, schedulingSubscription(weights, nil, func(handlerCtx context.Context, event *f1.Event) error {
		mu.Lock()
		first = append(first, event.Priority())
		position := len(first)
		mu.Unlock()
		if position == schedulingWindow {
			windowOnce.Do(func() { close(windowDone) })
		}
		return clock.NewReal().Sleep(handlerCtx, schedulingPace)
	}))
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()
	priorities := append(repeatPriority(f1.PriorityLow, schedulingBacklog), repeatPriority(f1.PriorityHigh, schedulingBacklog)...)
	publishPriorityBatch(t, client, ctx, priorities)
	waitSchedulingSignalOrRunner(t, windowDone, runDone, "priority backlog did not fill the window")
	cancel()
	waitRunnerDone(t, runDone)
	mu.Lock()
	defer mu.Unlock()
	if len(first) > schedulingWindow {
		first = first[:schedulingWindow]
	}
	return append([]f1.Priority(nil), first...)
}

func schedulingSubscription(weights map[f1.Priority]int, budgets map[f1.Priority]time.Duration, handler f1.HandlerFunc) f1.Subscription {
	return schedulingSubscriptionWithMode(f1.Unordered, weights, budgets, handler)
}

func schedulingSubscriptionWithMode(mode f1.Mode, weights map[f1.Priority]int, budgets map[f1.Priority]time.Duration, handler f1.HandlerFunc) f1.Subscription {
	return f1.Subscription{
		Name:           "scheduler-test",
		Topics:         []string{"orders.created"},
		Mode:           mode,
		Concurrency:    1,
		Prefetch:       64,
		Priorities:     []f1.Priority{f1.PriorityHigh, f1.PriorityLow},
		Fairness:       f1.FairnessConfig{Weights: weights, Budgets: budgets, DisableAging: budgets == nil, PrefetchFactor: 2},
		Retry:          f1.RetryConfig{MaxAttempts: 1},
		HandlerTimeout: time.Second,
		Handlers:       map[string]f1.Handler{"orders.created": handler},
	}
}

func schedulingSubscriptionWithRetry(handler f1.HandlerFunc) f1.Subscription {
	sub := schedulingSubscription(map[f1.Priority]int{f1.PriorityHigh: 1}, nil, handler)
	sub.Priorities = []f1.Priority{f1.PriorityHigh}
	sub.Retry = f1.RetryConfig{MaxAttempts: 2, Tiers: []time.Duration{time.Second}}
	return sub
}

func schedulingConfig() f1.Config {
	return f1.Config{
		Env: "test", Service: "scheduler", InstanceID: "worker-scheduling",
		Broker:    f1.BrokerConfig{Driver: "inmem", DefaultPrefetch: 64},
		Topology:  f1.TopologyConfig{AutoCreate: true, VerifyOnStart: true, Priorities: []f1.Priority{f1.PriorityHigh, f1.PriorityLow}},
		Codec:     f1.CodecConfig{Default: "json", ContentMode: "binary", MaxHeaderBytes: f1.CoreMaxHeaderBytes, MaxBodyBytes: 1 << 20},
		Lifecycle: f1.LifecycleConfig{DrainTimeout: 10 * time.Second, HandlerGrace: time.Second, CloseTimeout: time.Second},
	}
}

// publishPriorityBatch publishes one message per priority against the topology
// the runner declares inside Run.
func publishPriorityBatch(t *testing.T, client *f1.Client, ctx context.Context, priorities []f1.Priority) {
	t.Helper()
	messages := make([]f1.Message, len(priorities))
	for i, priority := range priorities {
		messages[i] = f1.Message{EventType: "orders.created", Payload: map[string]int{"sequence": i}, Opts: []f1.PublishOption{f1.WithPriority(priority)}}
	}
	publishMessages(t, client, ctx, messages)
}

// publishMessages publishes against the topology the runner declares inside Run.
// A publish that lands before that declaration fails per message
// (ErrDestinationMissing) with no batch-level error, so the helper retries only
// the failed messages to a deadline and then fails loudly: a silent miss turns
// into a delivery that never arrives, which reads as a scheduling failure.
func publishMessages(t *testing.T, client *f1.Client, ctx context.Context, messages []f1.Message) {
	t.Helper()
	deadline := clock.NewReal().Timer(5 * time.Second)
	ticker := clock.NewReal().Ticker(time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for pending := messages; ; {
		result, err := client.Publisher().PublishBatch(ctx, pending)
		if err != nil {
			t.Fatalf("publish batch: %v", err)
		}
		failed := result.Failed()
		if len(failed) == 0 {
			return
		}
		retry := make([]f1.Message, 0, len(failed))
		for _, index := range failed {
			retry = append(retry, pending[index])
		}
		firstErr := result.Results[failed[0]].Err
		pending = retry
		select {
		case <-deadline.C:
			t.Fatalf("publish did not reach the topology: %v", firstErr)
		case <-ticker.C:
		}
	}
}

// publishOneByOne publishes each message in its own call, waiting for each to
// land before the next. A batch that partially fails is retried by
// republishing only the failures, after the ones that landed, so a partial
// first attempt can put a later message ahead of an earlier one. The ordered
// test asserts the handled order equals the index order, so it cannot publish
// in a way that lets that happen.
func publishOneByOne(t *testing.T, client *f1.Client, ctx context.Context, messages []f1.Message) {
	t.Helper()
	for i := range messages {
		publishMessages(t, client, ctx, messages[i:i+1])
	}
}

func repeatPriority(priority f1.Priority, count int) []f1.Priority {
	priorities := make([]f1.Priority, count)
	for i := range priorities {
		priorities[i] = priority
	}
	return priorities
}

func countPriority(priorities []f1.Priority, wanted f1.Priority) int {
	count := 0
	for _, priority := range priorities {
		if priority == wanted {
			count++
		}
	}
	return count
}

func waitSchedulingSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	timer := clock.NewReal().Timer(5 * time.Second)
	select {
	case <-signal:
	case <-timer.C:
		t.Fatal(message)
	}
}

func waitSchedulingSignalOrRunner(t *testing.T, signal <-chan struct{}, done <-chan error, message string) {
	t.Helper()
	timer := clock.NewReal().Timer(5 * time.Second)
	select {
	case <-signal:
	case err := <-done:
		t.Fatalf("%s: Runner.Run() error = %v", message, err)
	case <-timer.C:
		t.Fatal(message)
	}
}

func waitRunnerDone(t *testing.T, done <-chan error) {
	t.Helper()
	timer := clock.NewReal().Timer(5 * time.Second)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Runner.Run() error = %v", err)
		}
	case <-timer.C:
		t.Fatal("Runner.Run() did not return")
	}
}

// windowDriver records the consumer configuration the core hands the driver.
// The destination windows are the contract with the driver: they are what it
// may hold outstanding for one destination, and what the core sizes to keep a
// handler from waiting on a broker round trip. A test reads them here rather
// than inferring them from a broker, which is what makes the window assertable
// without one.
type windowDriver struct {
	driver.Driver
	configs chan driver.ConsumerConfig
}

// Open forwards to the wrapped driver and wraps the connection, so the
// consumer the core builds on it is the one the test observes.
func (d *windowDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	conn, err := d.Driver.Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &windowConn{Conn: conn, configs: d.configs}, nil
}

// windowConn records every consumer configuration built on the connection.
type windowConn struct {
	driver.Conn
	configs chan driver.ConsumerConfig
}

// Consumer builds the wrapped consumer and records cfg only after a successful
// build, because the caller cancels its context as soon as it receives cfg.
func (c *windowConn) Consumer(ctx context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	consumer, err := c.Conn.Consumer(ctx, cfg)
	if err != nil {
		return consumer, err
	}
	select {
	case c.configs <- cfg:
	default:
	}
	return consumer, nil
}

// windowSubscription is the shipped shape with one priority and the shipped
// retry ladder, which is the shape the consume measurements run: one main
// destination and one destination per retry tier, weighted 4 against 2, so the
// main lane's share of one handler slot is one unit.
func windowSubscription(concurrency, prefetch int) f1.Subscription {
	return f1.Subscription{
		Name:           "scheduler-window-test",
		Topics:         []string{"orders.created"},
		Concurrency:    concurrency,
		Prefetch:       prefetch,
		Priorities:     []f1.Priority{f1.PriorityMedium},
		HandlerTimeout: time.Second,
		Handlers:       map[string]f1.Handler{"orders.created": f1.HandlerFunc(func(context.Context, *f1.Event) error { return nil })},
	}
}

// runWindowSubscription starts sub against the in-memory driver and returns the
// consumer configuration the core handed it, plus a sink holding the runner's
// log output.
func runWindowSubscription(t *testing.T, sub f1.Subscription) (driver.ConsumerConfig, *lockedLogSink) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var logs lockedLogSink
	configs := make(chan driver.ConsumerConfig, 1)
	client, err := f1.New(ctx, schedulingConfig(),
		f1.WithDriver(&windowDriver{Driver: inmem.New(), configs: configs}),
		f1.WithLogger(slog.New(slog.NewTextHandler(&logs, nil))),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()
	runner, err := client.Subscribe(ctx, sub)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()

	timer := clock.NewReal().Timer(5 * time.Second)
	defer timer.Stop()
	select {
	case cfg := <-configs:
		cancel()
		waitRunnerDone(t, runDone)
		return cfg, &logs
	case err := <-runDone:
		t.Fatalf("Runner.Run() error = %v", err)
	case <-timer.C:
		t.Fatal("consumer was not created")
	}
	return driver.ConsumerConfig{}, nil
}

// TestRunnerSchedulerSizesDestinationWindows pins the window each destination
// gets. At one and three handler slots the weighted share is one and two units,
// so the floor is what the window holds, and a floor of two units left two of
// three slots waiting a broker round trip behind their own acknowledgements
// instead of finding work in hand. The shipped concurrency keeps the share it
// had, which is what the same test says for the retry destinations.
func TestRunnerSchedulerSizesDestinationWindows(t *testing.T) {
	for _, tc := range []struct {
		concurrency int
		want        map[string]int
	}{
		{concurrency: 1, want: map[string]int{".medium": 6, ".medium.retry.1": 6, ".medium.retry.2": 6, ".medium.retry.3": 6}},
		{concurrency: 3, want: map[string]int{".medium": 6, ".medium.retry.1": 6, ".medium.retry.2": 6, ".medium.retry.3": 6}},
		{concurrency: 16, want: map[string]int{".medium": 22, ".medium.retry.1": 12, ".medium.retry.2": 12, ".medium.retry.3": 12}},
	} {
		t.Run(strconv.Itoa(tc.concurrency), func(t *testing.T) {
			cfg, _ := runWindowSubscription(t, windowSubscription(tc.concurrency, 0))
			for destination := range tc.want {
				found := false
				for _, declared := range cfg.Destinations {
					if strings.HasSuffix(declared, destination) {
						found = true
						if got := cfg.PerDestination[declared]; got != tc.want[destination] {
							t.Errorf("window for %s = %d, want %d", declared, got, tc.want[destination])
						}
					}
				}
				if !found {
					t.Errorf("no destination ending in %q: declared %v", destination, cfg.Destinations)
				}
			}
		})
	}
}

// TestRunnerSchedulerReportsPrefetchCappedByTheLaneTotal pins what a caller
// learns when the prefetch they configured is more than the destination
// windows add up to. The consumer gets the lane total, which is deliberate,
// and the warning is what keeps that from being silent: the caller named a
// budget and the destinations carry less. A prefetch the windows can carry is
// not reported, so the line means the cap and nothing else.
func TestRunnerSchedulerReportsPrefetchCappedByTheLaneTotal(t *testing.T) {
	const laneTotal = 24
	for _, tc := range []struct {
		name       string
		prefetch   int
		wantReport bool
	}{
		{name: "at-the-lane-total", prefetch: laneTotal},
		{name: "above-the-lane-total", prefetch: laneTotal + 1, wantReport: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, logs := runWindowSubscription(t, windowSubscription(1, tc.prefetch))
			if cfg.Prefetch != laneTotal {
				t.Errorf("consumer prefetch = %d, want the lane total %d", cfg.Prefetch, laneTotal)
			}
			output := logs.String()
			if !tc.wantReport {
				if strings.Contains(output, "configured prefetch exceeds the destination windows") {
					t.Fatalf("log output = %q, want no cap report for a prefetch the windows carry", output)
				}
				return
			}
			for _, want := range []string{
				"configured prefetch exceeds the destination windows",
				"configured=" + strconv.Itoa(tc.prefetch),
				"effective=" + strconv.Itoa(laneTotal),
			} {
				if !strings.Contains(output, want) {
					t.Fatalf("log output = %q, missing %q", output, want)
				}
			}
		})
	}
}

// lockedLogSink collects a logger's output for a test whose runner logs from
// its own goroutine, so reading it after the runner stopped is not a race.
type lockedLogSink struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (s *lockedLogSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buffer.Write(p)
}

func (s *lockedLogSink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buffer.String()
}

// TestRunnerSchedulerReportsOnlyAPrefetchTheCallerSet pins which subscriptions
// hear that the lane windows cap a prefetch. A budget the caller named is
// reported, including when the number they named happens to equal the broker
// default; a budget nobody named is silent, because the default is not the
// caller's decision and a line that fires on every subscription is what leaves
// it unread when a caller's own budget is capped. The cap itself stays
// unconditional, which the unset row pins by its budget.
func TestRunnerSchedulerReportsOnlyAPrefetchTheCallerSet(t *testing.T) {
	const laneTotal = 24
	for _, tc := range []struct {
		name       string
		prefetch   int
		wantBudget int
		wantReport bool
	}{
		{name: "unset-takes-the-broker-default", prefetch: 0, wantBudget: laneTotal},
		{name: "named-below-the-lane-total", prefetch: 8, wantBudget: 8},
		{name: "named-at-the-broker-default", prefetch: 64, wantBudget: laneTotal, wantReport: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, logs := runWindowSubscription(t, windowSubscription(1, tc.prefetch))
			if cfg.Prefetch != tc.wantBudget {
				t.Fatalf("consumer prefetch = %d, want %d", cfg.Prefetch, tc.wantBudget)
			}
			output := logs.String()
			reported := strings.Contains(output, "configured prefetch exceeds the destination windows")
			if tc.wantReport && !reported {
				t.Fatalf("log output = %q, want the cap report for a prefetch the caller named", output)
			}
			if !tc.wantReport && reported {
				t.Fatalf("log output = %q, want no cap report for a prefetch nobody named", output)
			}
		})
	}
}
