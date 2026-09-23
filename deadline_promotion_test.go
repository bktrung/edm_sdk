package f1

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	//nolint:depguard // promotion dispatch tests exercise the runner through the in-memory driver.
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/sched"
)

func promotionTestRunner(fake *clock.Fake, observer Observer, sub Subscription) *Runner {
	return &Runner{
		client:       &Client{options: clientOptions{clock: fake}, observer: observer},
		subscription: sub,
	}
}

func promotionTestSubscription() Subscription {
	return Subscription{
		Name:        "orders",
		Topics:      []string{"orders.created"},
		Priorities:  []Priority{PriorityMedium},
		Concurrency: 1,
		Fairness: FairnessConfig{
			Weights:        map[Priority]int{PriorityMedium: 1},
			Budgets:        map[Priority]time.Duration{PriorityMedium: time.Second},
			PrefetchFactor: 1,
		},
		Retry: RetryConfig{MaxAttempts: 1},
	}
}

func TestPromotionLimiterRecordsFirstPromotion(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	runner := promotionTestRunner(fake, &recordingObserver{}, promotionTestSubscription())
	limiter := newPromotionLimiter(runner)
	if limiter == nil {
		t.Fatal("limiter is nil with observer set")
	}
	laneID := schedulerLaneID("orders.created", PriorityMedium, 0)
	enqueuedAt := fake.Now()
	fake.Advance(time.Second)
	now := fake.Now()
	event, ok := limiter.promotionEvent(sched.Promotion{LaneID: laneID, Depth: 3}, enqueuedAt, now)
	if !ok {
		t.Fatal("first promotion was not recorded")
	}
	if event.Kind != ObserverDeadlinePromoted {
		t.Fatalf("kind = %q, want deadline_promoted", event.Kind)
	}
	if !event.At.Equal(now) {
		t.Fatalf("at = %v, want %v", event.At, now)
	}
	if event.Topic != "orders.created" {
		t.Fatalf("topic = %q, want orders.created", event.Topic)
	}
	if event.Priority != PriorityMedium {
		t.Fatalf("priority = %v, want medium", event.Priority)
	}
	if event.Subscription != "orders" {
		t.Fatalf("subscription = %q, want orders", event.Subscription)
	}
	if event.LaneWait != time.Second {
		t.Fatalf("lane wait = %v, want 1s", event.LaneWait)
	}
	if event.LaneDepth != 3 {
		t.Fatalf("lane depth = %d, want 3", event.LaneDepth)
	}
	if event.Suppressed != 0 {
		t.Fatalf("suppressed = %d, want 0", event.Suppressed)
	}
}

func TestPromotionLimiterSuppressesInsideWindow(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	runner := promotionTestRunner(fake, &recordingObserver{}, promotionTestSubscription())
	limiter := newPromotionLimiter(runner)
	laneID := schedulerLaneID("orders.created", PriorityMedium, 0)
	enqueuedAt := fake.Now()
	fake.Advance(time.Second)
	firstAt := fake.Now()
	if _, ok := limiter.promotionEvent(sched.Promotion{LaneID: laneID, Depth: 2}, enqueuedAt, firstAt); !ok {
		t.Fatal("first promotion was not recorded")
	}
	fake.Advance(time.Second)
	if _, ok := limiter.promotionEvent(sched.Promotion{LaneID: laneID, Depth: 2}, enqueuedAt, fake.Now()); ok {
		t.Fatal("second promotion inside window was recorded")
	}
	fake.Advance(time.Second)
	if _, ok := limiter.promotionEvent(sched.Promotion{LaneID: laneID, Depth: 1}, enqueuedAt, fake.Now()); ok {
		t.Fatal("third promotion inside window was recorded")
	}
	fake.Advance(13 * time.Second)
	now := fake.Now()
	event, ok := limiter.promotionEvent(sched.Promotion{LaneID: laneID, Depth: 1}, enqueuedAt, now)
	if !ok {
		t.Fatal("promotion after window was not recorded")
	}
	if event.Suppressed != 2 {
		t.Fatalf("suppressed = %d, want 2", event.Suppressed)
	}
	if !event.At.Equal(now) {
		t.Fatalf("at = %v, want %v", event.At, now)
	}
	fake.Advance(time.Second)
	event, ok = limiter.promotionEvent(sched.Promotion{LaneID: laneID, Depth: 1}, enqueuedAt, fake.Now())
	if ok {
		t.Fatal("promotion inside new window was recorded")
	}
	_ = event
}

func TestPromotionLimiterCarriesAndResetsSuppressed(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	runner := promotionTestRunner(fake, &recordingObserver{}, promotionTestSubscription())
	limiter := newPromotionLimiter(runner)
	laneID := schedulerLaneID("orders.created", PriorityMedium, 0)
	enqueuedAt := fake.Now()
	fake.Advance(time.Second)
	if _, ok := limiter.promotionEvent(sched.Promotion{LaneID: laneID, Depth: 1}, enqueuedAt, fake.Now()); !ok {
		t.Fatal("first promotion was not recorded")
	}
	fake.Advance(time.Second)
	if _, ok := limiter.promotionEvent(sched.Promotion{LaneID: laneID, Depth: 1}, enqueuedAt, fake.Now()); ok {
		t.Fatal("suppressed promotion was recorded")
	}
	fake.Advance(14 * time.Second)
	event, ok := limiter.promotionEvent(sched.Promotion{LaneID: laneID, Depth: 1}, enqueuedAt, fake.Now())
	if !ok || event.Suppressed != 1 {
		t.Fatalf("after window ok = %v suppressed = %d, want true 1", ok, event.Suppressed)
	}
	fake.Advance(15 * time.Second)
	event, ok = limiter.promotionEvent(sched.Promotion{LaneID: laneID, Depth: 1}, enqueuedAt, fake.Now())
	if !ok || event.Suppressed != 0 {
		t.Fatalf("next window ok = %v suppressed = %d, want true 0", ok, event.Suppressed)
	}
}

func TestPromotionLimiterLimitsLanesIndependently(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	sub := promotionTestSubscription()
	sub.Topics = []string{"orders.created"}
	sub.Priorities = []Priority{PriorityHigh, PriorityLow}
	sub.Fairness.Weights = map[Priority]int{PriorityHigh: 1, PriorityLow: 1}
	sub.Fairness.Budgets = map[Priority]time.Duration{PriorityHigh: time.Second, PriorityLow: time.Second}
	runner := promotionTestRunner(fake, &recordingObserver{}, sub)
	limiter := newPromotionLimiter(runner)
	highID := schedulerLaneID("orders.created", PriorityHigh, 0)
	lowID := schedulerLaneID("orders.created", PriorityLow, 0)
	enqueuedAt := fake.Now()
	fake.Advance(2 * time.Second)
	now := fake.Now()
	if _, ok := limiter.promotionEvent(sched.Promotion{LaneID: highID, Depth: 1}, enqueuedAt, now); !ok {
		t.Fatal("high promotion was not recorded")
	}
	if _, ok := limiter.promotionEvent(sched.Promotion{LaneID: lowID, Depth: 1}, enqueuedAt, now); !ok {
		t.Fatal("low promotion was not recorded independently")
	}
	if _, ok := limiter.promotionEvent(sched.Promotion{LaneID: highID, Depth: 1}, enqueuedAt, now); ok {
		t.Fatal("second high promotion inside window was recorded")
	}
	if _, ok := limiter.promotionEvent(sched.Promotion{LaneID: lowID, Depth: 1}, enqueuedAt, now); ok {
		t.Fatal("second low promotion inside window was recorded")
	}
}

func TestPromotionLimiterIgnoresNonPromotionAndUnknownLane(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	runner := promotionTestRunner(fake, &recordingObserver{}, promotionTestSubscription())
	limiter := newPromotionLimiter(runner)
	if _, ok := limiter.promotionEvent(sched.Promotion{}, fake.Now(), fake.Now()); ok {
		t.Fatal("zero promotion was recorded")
	}
	if _, ok := limiter.promotionEvent(sched.Promotion{LaneID: "no.such.lane", Depth: 1}, fake.Now(), fake.Now()); ok {
		t.Fatal("unknown lane promotion was recorded")
	}
}

// TestPromotionDispatchLoopNoObserverEmitsNothing is the worker-path half of
// the old nil-limiter assertion: with no observer the dispatch loop builds no
// limiter, records nothing, and promotions on the hot path must not panic.
func TestPromotionDispatchLoopNoObserverEmitsNothing(t *testing.T) {
	rig := newPromotionDispatchRig(t, nil)
	const highBacklog = 8
	gate := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(open)
	rig.block.Store(&gate)
	publishPromotionMessage(t, rig.client, rig.ctx, PriorityHigh)
	waitPromotionSignal(t, rig.runDone, rig.blocked, "blocking handler did not start")
	publishPromotionMessage(t, rig.client, rig.ctx, PriorityLow)
	for range highBacklog {
		publishPromotionMessage(t, rig.client, rig.ctx, PriorityHigh)
	}
	waitPromotionSignal(t, rig.runDone, rig.relayed, "low head did not reach the pipeline")
	open()
	rig.block.Store(nil)
	waitPromotionCondition(t, rig.runDone, "backlog was not handled", func() bool {
		return rig.handled.Load() == int64(2+highBacklog)
	})
}

func TestPromotionThroughRunnerScheduler(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	sub := Subscription{
		Name:        "orders",
		Topics:      []string{"orders.created"},
		Priorities:  []Priority{PriorityHigh, PriorityLow},
		Concurrency: 4,
		Fairness: FairnessConfig{
			Weights:        map[Priority]int{PriorityHigh: 8, PriorityLow: 1},
			Budgets:        map[Priority]time.Duration{PriorityHigh: time.Hour, PriorityLow: time.Second},
			PrefetchFactor: 2,
		},
		Retry: RetryConfig{MaxAttempts: 1},
	}
	runner := promotionTestRunner(fake, &recordingObserver{}, sub)
	scheduler, err := newRunnerScheduler(runner)
	if err != nil {
		t.Fatal(err)
	}
	limiter := newPromotionLimiter(runner)
	if limiter == nil {
		t.Fatal("limiter is nil with observer set")
	}
	lowID := schedulerLaneID("orders.created", PriorityLow, 0)
	if err := scheduler.Enqueue(lowID, sched.Item{Value: "low", EnqueuedAt: start}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Enqueue(lowID, sched.Item{Value: "low2", EnqueuedAt: start}); err != nil {
		t.Fatal(err)
	}
	highID := schedulerLaneID("orders.created", PriorityHigh, 0)
	if err := scheduler.Enqueue(highID, sched.Item{Value: "high", EnqueuedAt: start}); err != nil {
		t.Fatal(err)
	}
	fake.Advance(2 * time.Second)
	now := fake.Now()
	item, prom, ok := scheduler.Next()
	if !ok || item.Value != "low" {
		t.Fatalf("promoted item = %#v, %v; want low", item.Value, ok)
	}
	if prom.LaneID != lowID {
		t.Fatalf("promotion lane = %q, want %q", prom.LaneID, lowID)
	}
	if prom.Depth != 2 {
		t.Fatalf("promotion depth = %d, want 2", prom.Depth)
	}
	event, ok := limiter.promotionEvent(prom, item.EnqueuedAt, now)
	if !ok {
		t.Fatal("runner promotion was not recorded")
	}
	if event.Topic != "orders.created" || event.Priority != PriorityLow || event.Subscription != "orders" {
		t.Fatalf("event identity = %q %v %q, want orders.created low orders", event.Topic, event.Priority, event.Subscription)
	}
	if event.LaneWait != 2*time.Second {
		t.Fatalf("lane wait = %v, want 2s", event.LaneWait)
	}
	if event.LaneDepth != 2 {
		t.Fatalf("lane depth = %d, want 2", event.LaneDepth)
	}
}

func TestPromotionRetryLaneReportsTopicAndPriority(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	sub := promotionTestSubscription()
	sub.Topics = []string{"orders.created"}
	sub.Priorities = []Priority{PriorityHigh}
	sub.Retry = RetryConfig{MaxAttempts: 2}
	runner := promotionTestRunner(fake, &recordingObserver{}, sub)
	limiter := newPromotionLimiter(runner)
	retryID := schedulerLaneID("orders.created", PriorityHigh, 1)
	enqueuedAt := fake.Now()
	fake.Advance(time.Second)
	event, ok := limiter.promotionEvent(sched.Promotion{LaneID: retryID, Depth: 1}, enqueuedAt, fake.Now())
	if !ok {
		t.Fatal("retry promotion was not recorded")
	}
	if event.Topic != "orders.created" || event.Priority != PriorityHigh {
		t.Fatalf("retry event identity = %q %v, want orders.created high", event.Topic, event.Priority)
	}
}

// promotionRelayDriver wraps the in-memory driver so the dispatch test knows
// when the low lane's head has reached the pipeline. Fetch hands messages to
// the dispatch loop in channel order, so three high handoffs after the low
// head prove the loop received the low head and queued it in its scheduler
// lane before the test releases the blocking handler.
type promotionRelayDriver struct {
	driver.Driver
	relayed chan struct{}
}

func (d *promotionRelayDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	conn, err := d.Driver.Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &promotionRelayConn{Conn: conn, relayed: d.relayed}, nil
}

type promotionRelayConn struct {
	driver.Conn
	relayed chan struct{}
}

func (c *promotionRelayConn) Consumer(ctx context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	consumer, err := c.Conn.Consumer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &promotionRelayConsumer{
		Consumer: consumer,
		relayed:  c.relayed,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}, nil
}

type promotionRelayConsumer struct {
	driver.Consumer
	relayed chan struct{}
	lowSeen atomic.Bool
	highs   atomic.Int32
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
}

func (c *promotionRelayConsumer) Messages() <-chan driver.InboundMessage {
	out := make(chan driver.InboundMessage)
	go func() {
		defer close(out)
		defer close(c.done)
		for {
			select {
			case <-c.stop:
				c.drain()
				return
			case msg, ok := <-c.Consumer.Messages():
				if !ok {
					return
				}
				low := strings.HasSuffix(msg.Destination, ".low")
				high := strings.HasSuffix(msg.Destination, ".high")
				select {
				case out <- msg:
					if low {
						c.lowSeen.Store(true)
					}
					if high && c.lowSeen.Load() && c.highs.Add(1)%3 == 0 {
						select {
						case c.relayed <- struct{}{}:
						default:
						}
					}
				case <-c.stop:
					if msg.Settle != nil {
						_ = msg.Settle.Nack(context.Background(), driver.NackOptions{Requeue: true})
					}
					c.drain()
					return
				}
			}
		}
	}()
	return out
}

func (c *promotionRelayConsumer) Stop(ctx context.Context) error {
	if err := c.Drain(ctx); err != nil {
		return err
	}
	c.once.Do(func() { close(c.stop) })
	<-c.done
	return c.Consumer.Stop(ctx)
}

func (c *promotionRelayConsumer) Release(ctx context.Context) error {
	c.once.Do(func() { close(c.stop) })
	<-c.done
	return c.Consumer.Release(ctx)
}

func (c *promotionRelayConsumer) drain() {
	for {
		select {
		case msg, ok := <-c.Consumer.Messages():
			if !ok {
				return
			}
			if msg.Settle != nil {
				_ = msg.Settle.Nack(context.Background(), driver.NackOptions{Requeue: true})
			}
		default:
			return
		}
	}
}

// promotionDispatchRig runs a real runner on the fake clock with a handler
// that blocks while the test queues a backlog, then advances the fake clock
// past the low lane's budget on every invocation. The advances keep the queued
// low head overdue while high work remains, so the dispatch loop promotes it.
type promotionDispatchRig struct {
	client  *Client
	ctx     context.Context
	cancel  context.CancelFunc
	runner  *Runner
	runDone chan error
	fake    *clock.Fake
	rec     *consumeRecordingObserver
	block   atomic.Pointer[chan struct{}]
	blocked chan struct{}
	relayed chan struct{}
	handled atomic.Int64
}

func newPromotionDispatchRig(t *testing.T, rec *consumeRecordingObserver) *promotionDispatchRig {
	t.Helper()
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	relayed := make(chan struct{}, 16)
	opts := []Option{
		WithDriver(&promotionRelayDriver{Driver: inmem.New(), relayed: relayed}),
		withClock(fake),
		WithPublishTopics("orders.created"),
	}
	if rec != nil {
		opts = append(opts, WithObserver(rec))
	}
	client, err := New(context.Background(), testClientConfig(t), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	rig := &promotionDispatchRig{
		client:  client,
		fake:    fake,
		rec:     rec,
		blocked: make(chan struct{}, 16),
		relayed: relayed,
	}
	handler := HandlerFunc(func(ctx context.Context, _ *Event) error {
		if ch := rig.block.Load(); ch != nil {
			select {
			case rig.blocked <- struct{}{}:
			default:
			}
			select {
			case <-*ch:
			case <-ctx.Done():
			}
		}
		// One second per handled message: the queued low head is overdue on
		// the second pick already, before its round-robin turn can arrive no
		// matter the deficit the previous phase left behind. Twenty messages
		// move the clock ten seconds per phase, inside one 15 s window.
		rig.fake.Advance(time.Second)
		rig.handled.Add(1)
		return nil
	})
	sub := Subscription{
		Name:        "orders",
		Topics:      []string{"orders.created"},
		Mode:        Unordered,
		Concurrency: 1,
		Prefetch:    64,
		Priorities:  []Priority{PriorityHigh, PriorityLow},
		Fairness: FairnessConfig{
			Weights:        map[Priority]int{PriorityHigh: 8, PriorityLow: 1},
			Budgets:        map[Priority]time.Duration{PriorityHigh: time.Hour, PriorityLow: time.Second},
			PrefetchFactor: 2,
		},
		Retry:          RetryConfig{MaxAttempts: 1},
		HandlerTimeout: 10 * time.Second,
		Handlers:       map[string]Handler{"orders.created": handler},
	}
	ctx, cancel := context.WithCancel(context.Background())
	runner, err := client.Subscribe(ctx, sub)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	rig.ctx = ctx
	rig.cancel = cancel
	rig.runner = runner
	rig.runDone = make(chan error, 1)
	go func() { rig.runDone <- runner.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-rig.runDone:
		case <-clock.NewReal().Timer(5 * time.Second).C:
		}
	})
	waitOwnerReady(t, runner)
	return rig
}

func publishPromotionMessage(t *testing.T, client *Client, ctx context.Context, priority Priority) {
	t.Helper()
	msgs := []Message{{
		EventType: "orders.created",
		Payload:   map[string]int{"priority": int(priority)},
		Opts:      []PublishOption{WithPriority(priority)},
	}}
	deadline := clock.NewReal().Timer(5 * time.Second)
	defer deadline.Stop()
	ticker := clock.NewReal().Ticker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		result, err := client.Publisher().PublishBatch(ctx, msgs)
		if err != nil {
			t.Fatalf("PublishBatch() error = %v", err)
		}
		if len(failedIndexes(result)) == 0 {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("publish did not reach the topology: %v", result.Results[failedIndexes(result)[0]].Err)
			return
		case <-ticker.C:
		}
	}
}

func waitPromotionSignal(t *testing.T, runDone <-chan error, signal <-chan struct{}, msg string) {
	t.Helper()
	deadline := clock.NewReal().Timer(5 * time.Second)
	defer deadline.Stop()
	select {
	case <-signal:
		return
	case err := <-runDone:
		t.Fatalf("runner exited while waiting: %v", err)
	case <-deadline.C:
		t.Fatal(msg)
	}
}

func waitPromotionCondition(t *testing.T, runDone <-chan error, msg string, cond func() bool) {
	t.Helper()
	deadline := clock.NewReal().Timer(10 * time.Second)
	defer deadline.Stop()
	ticker := clock.NewReal().Ticker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if cond() {
			return
		}
		select {
		case err := <-runDone:
			t.Fatalf("runner exited while waiting for %s: %v", msg, err)
		case <-deadline.C:
			t.Fatal(msg)
			return
		case <-ticker.C:
		}
	}
}

func promotionPoints(rec *consumeRecordingObserver) []PointEvent {
	records, _, _, _ := rec.snapshot()
	var out []PointEvent
	for _, event := range records {
		if event.Kind == ObserverDeadlinePromoted {
			out = append(out, event)
		}
	}
	return out
}

func drainRelayed(relayed chan struct{}) {
	for {
		select {
		case <-relayed:
		default:
			return
		}
	}
}

// TestPromotionDispatchLoopRecordsOnePerWindow runs a real runner whose low
// lane budget the fake clock overruns while high work stays backlogged. The
// observer must see exactly one deadline_promoted for the low lane with its
// topic, priority and subscription; promoting again inside 15 s adds none.
func TestPromotionDispatchLoopRecordsOnePerWindow(t *testing.T) {
	rec := &consumeRecordingObserver{}
	rig := newPromotionDispatchRig(t, rec)
	const highBacklog = 8
	const perPhase = 2 + highBacklog

	runPhase := func() {
		gate := make(chan struct{})
		var once sync.Once
		open := func() { once.Do(func() { close(gate) }) }
		t.Cleanup(open)
		rig.block.Store(&gate)
		drainRelayed(rig.relayed)
		publishPromotionMessage(t, rig.client, rig.ctx, PriorityHigh)
		waitPromotionSignal(t, rig.runDone, rig.blocked, "blocking handler did not start")
		publishPromotionMessage(t, rig.client, rig.ctx, PriorityLow)
		for range highBacklog {
			publishPromotionMessage(t, rig.client, rig.ctx, PriorityHigh)
		}
		waitPromotionSignal(t, rig.runDone, rig.relayed, "low head did not reach the pipeline")
		open()
		rig.block.Store(nil)
	}

	runPhase()
	waitPromotionCondition(t, rig.runDone, "first deadline_promoted did not arrive", func() bool {
		return len(promotionPoints(rec)) == 1 && rig.handled.Load() == perPhase
	})
	first := promotionPoints(rec)[0]
	if first.Topic != "orders.created" || first.Priority != PriorityLow || first.Subscription != "orders" {
		t.Fatalf("event identity = %q %v %q, want orders.created low orders", first.Topic, first.Priority, first.Subscription)
	}
	if first.LaneDepth != 1 {
		t.Fatalf("lane depth = %d, want 1", first.LaneDepth)
	}
	if first.LaneWait < time.Second {
		t.Fatalf("lane wait = %v, want at least 1s", first.LaneWait)
	}
	if first.Suppressed != 0 {
		t.Fatalf("suppressed = %d, want 0", first.Suppressed)
	}

	runPhase()
	waitPromotionCondition(t, rig.runDone, "second phase did not drain", func() bool {
		return rig.handled.Load() == 2*perPhase
	})
	waitPromotionCondition(t, rig.runDone, "suppressed promotion was recorded", func() bool {
		return len(promotionPoints(rec)) == 1
	})
	if got := len(promotionPoints(rec)); got != 1 {
		t.Fatalf("deadline_promoted count = %d, want 1", got)
	}
	records, _, _, _ := rec.snapshot()
	for _, event := range records {
		if event.Kind == "" {
			t.Fatal("observer recorded an event with an empty kind")
		}
	}
}
