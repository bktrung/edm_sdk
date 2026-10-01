package f1_test

import (
	"context"
	"errors"
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
	// schedulingPromotionAdvance is the clock step the deadline promotion test
	// applies per handled message: larger than the low priority's budget and far
	// smaller than the high priority's, so exactly the low item can be promoted.
	schedulingPromotionAdvance = 200 * time.Millisecond
	// schedulingPromotionBound is how many dispatches the overdue low item may sit
	// behind: the pick after the advance that makes it overdue is the dispatch
	// after next, and the feed can take up to two dispatches to place it in a lane.
	// Measured 3-4 over 40 runs with promotion, against 9-10 without it, where
	// the item waits for the high slot's 8-pick deficit round instead.
	schedulingPromotionBound = 6
)

func TestRunnerSchedulerWeightedShare(t *testing.T) {
	for _, test := range []struct {
		name     string
		weights  map[f1.Priority]int
		favoured f1.Priority
	}{
		{name: "high 8 low 1", weights: map[f1.Priority]int{f1.PriorityHigh: 8, f1.PriorityLow: 1}, favoured: f1.PriorityHigh},
		{name: "high 1 low 8", weights: map[f1.Priority]int{f1.PriorityHigh: 1, f1.PriorityLow: 8}, favoured: f1.PriorityLow},
	} {
		t.Run(test.name, func(t *testing.T) {
			first := runPriorityShare(t, test.weights)
			high := countPriority(first, f1.PriorityHigh)
			low := countPriority(first, f1.PriorityLow)
			favoured := countPriority(first, test.favoured)
			const want = 89
			if favoured < want-schedulingShareTolerance || favoured > want+schedulingShareTolerance || high+low != schedulingWindow {
				t.Fatalf("first %d priorities = high:%d low:%d, want %s:%d +/- %d", schedulingWindow, high, low, test.favoured, want, schedulingShareTolerance)
			}
		})
	}
}

func TestRunnerSchedulerPromotesOverdueLowPriorityWork(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lowQueued := make(chan struct{})
	client, err := f1.New(ctx, schedulingConfig(), f1.WithDriver(&promotionDriver{
		Driver:    testhook.Driver(fake),
		lowQueued: lowQueued,
	}), testhook.ClientOption(fake).(f1.Option))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()

	const highBacklog = 32
	started := make(chan struct{})
	release := make(chan struct{})
	allHighHandled := make(chan struct{})
	var allHighOnce sync.Once
	dispatched := make(chan f1.Priority, schedulingPromotionBound)
	var highHandled atomic.Int64
	var position atomic.Int64
	var firstOnce sync.Once
	sub := schedulingSubscription(map[f1.Priority]int{
		f1.PriorityHigh: 8,
		f1.PriorityLow:  1,
	}, map[f1.Priority]time.Duration{
		f1.PriorityHigh: time.Hour,
		f1.PriorityLow:  schedulingPromotionAdvance / 2,
	}, func(_ context.Context, event *f1.Event) error {
		firstOnce.Do(func() {
			close(started)
			// Held until the low item is in the scheduler lane and the backlog
			// below is published, so the clock move ages the queued low item.
			<-release
		})
		if event.Priority() == f1.PriorityHigh {
			handled := highHandled.Add(1)
			if handled >= highBacklog+1 {
				allHighOnce.Do(func() { close(allHighHandled) })
			}
		}
		if position.Add(1) <= schedulingPromotionBound {
			dispatched <- event.Priority()
		}
		// The runner stamps EnqueuedAt from this clock, and the pick for the
		// next dispatch happens after this handler returns. Advancing here
		// therefore makes the low item overdue after it is queued, before that pick.
		fake.Advance(schedulingPromotionAdvance)
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
	publishPriorityBatch(t, client, ctx, []f1.Priority{f1.PriorityLow})
	publishPriorityBatch(t, client, ctx, repeatPriority(f1.PriorityHigh, highBacklog))
	waitSchedulingSignalOrRunner(t, lowQueued, runDone, "low-priority item did not reach its scheduler lane")
	close(release)

	timer := clock.NewReal().Timer(5 * time.Second)
	defer timer.Stop()
	for range schedulingPromotionBound {
		select {
		case got := <-dispatched:
			if got != f1.PriorityLow {
				continue
			}
			if remaining := highBacklog - int(highHandled.Load()); remaining <= 0 {
				t.Fatalf("high backlog had %d items unhandled when the overdue low item ran, want the low item ahead of backlogged %d-item high work", remaining, highBacklog)
			}
			waitSchedulingSignal(t, allHighHandled, "high backlog did not drain after overdue low item ran")
			cancel()
			waitRunnerDone(t, runDone)
			return
		case err := <-runDone:
			t.Fatalf("overdue low item: Runner.Run() error = %v", err)
		case <-timer.C:
			t.Fatal("overdue item was not dispatched")
		}
	}
	t.Fatalf("overdue low item was not dispatched within the first %d dispatches, want it ahead of the %d-item high backlog", schedulingPromotionBound, highBacklog)
}

type promotionDriver struct {
	driver.Driver
	lowQueued chan struct{}
}

func (d *promotionDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	conn, err := d.Driver.Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &promotionConn{Conn: conn, lowQueued: d.lowQueued}, nil
}

type promotionConn struct {
	driver.Conn
	lowQueued chan struct{}
}

func (c *promotionConn) Consumer(ctx context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	consumer, err := c.Conn.Consumer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &promotionConsumer{
		Consumer:  consumer,
		lowQueued: c.lowQueued,
		stopRelay: make(chan struct{}),
		relayDone: make(chan struct{}),
	}, nil
}

type promotionConsumer struct {
	driver.Consumer
	lowQueued     chan struct{}
	lowSeen       atomic.Bool
	highAfterLow  atomic.Int32
	stopRelay     chan struct{}
	relayDone     chan struct{}
	stopRelayOnce sync.Once
}

func (c *promotionConsumer) Messages() <-chan driver.InboundMessage {
	messages := make(chan driver.InboundMessage)
	go func() {
		defer close(messages)
		defer close(c.relayDone)
		for {
			select {
			case <-c.stopRelay:
				c.drainInner()
				return
			case message, ok := <-c.Consumer.Messages():
				if !ok {
					return
				}
				low := strings.HasSuffix(message.Destination, ".low")
				high := strings.HasSuffix(message.Destination, ".high")
				select {
				case messages <- message:
					if low {
						c.lowSeen.Store(true)
					}
					// Three following handoffs force the pipeline to enqueue the low item before it can receive the second following high item.
					if high && c.lowSeen.Load() && c.highAfterLow.Add(1) == 3 {
						close(c.lowQueued)
					}
				case <-c.stopRelay:
					if message.Settle != nil {
						_ = message.Settle.Nack(context.Background(), driver.NackOptions{Requeue: true})
					}
					c.drainInner()
					return
				}
			}
		}
	}()
	return messages
}

func (c *promotionConsumer) Stop(ctx context.Context) error {
	if err := c.Drain(ctx); err != nil {
		return err
	}
	c.stopRelayOnce.Do(func() { close(c.stopRelay) })
	<-c.relayDone
	return c.Consumer.Stop(ctx)
}

func (c *promotionConsumer) Release(ctx context.Context) error {
	c.stopRelayOnce.Do(func() { close(c.stopRelay) })
	<-c.relayDone
	return c.Consumer.Release(ctx)
}

func (c *promotionConsumer) drainInner() {
	for {
		select {
		case message, ok := <-c.Consumer.Messages():
			if !ok {
				return
			}
			if message.Settle != nil {
				_ = message.Settle.Nack(context.Background(), driver.NackOptions{Requeue: true})
			}
		default:
			return
		}
	}
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
		Fairness:       f1.FairnessConfig{Weights: weights, Budgets: budgets, DisableDeadlinePromotion: budgets == nil, PrefetchFactor: 2},
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
		Codec:     f1.CodecConfig{Default: "json", MaxHeaderBytes: f1.CoreMaxHeaderBytes, MaxBodyBytes: 1 << 20},
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
		failed := make([]int, 0)
		for index, message := range result.Results {
			if message.Err != nil {
				failed = append(failed, index)
			}
		}
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
