package f1

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/dispatch"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/obs"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/retry"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/sched"
)

type delivery struct {
	id      uint64
	message driver.InboundMessage
}

type deliveryState struct {
	id        uint64
	attempted bool
	settled   bool
	// operation records which settlement call was last made, so that a failed
	// one can be retried in kind rather than guessed at.
	operation settlementOperation
}

type deliveryMetrics struct {
	attemptDivergence obs.SampledGaugeSet
	topics            []string
	stuckWorkers      atomic.Int64
	activeHandlers    atomic.Int64
	retired           atomic.Bool
	unregisterOnce    sync.Once
	unregister        func()
}

const deliveryPriorityLanes = 3

func newDeliveryMetrics(topics []string) deliveryMetrics {
	names := make([]string, len(topics))
	for i, topic := range topics {
		names[i] = topicFor(topic)
	}
	sort.Strings(names)
	unique := names[:0]
	for _, name := range names {
		if len(unique) == 0 || unique[len(unique)-1] != name {
			unique = append(unique, name)
		}
	}
	return deliveryMetrics{
		attemptDivergence: obs.NewSampledGaugeSet(unique, deliveryPriorityLanes),
		topics:            append([]string(nil), unique...),
	}
}

func (m *deliveryMetrics) observeAttemptDivergence(eventType string, priority Priority, value uint64) {
	if priority.Valid() {
		m.attemptDivergence.Observe(topicFor(eventType), int(priority), value)
	}
}

func (m *deliveryMetrics) sampleAttemptDivergence(topic string, priority Priority) uint64 {
	if !priority.Valid() {
		return 0
	}
	return m.attemptDivergence.Sample(topic, int(priority))
}

func (m *deliveryMetrics) sampleAttemptDivergenceByTopic() map[string]int64 {
	values := make(map[string]int64, len(m.topics))
	for _, topic := range m.topics {
		var highWater uint64
		for lane := 0; lane < deliveryPriorityLanes; lane++ {
			if value := m.attemptDivergence.Sample(topic, lane); value > highWater {
				highWater = value
			}
		}
		values[topic] = int64(highWater)
	}
	return values
}

func (m *deliveryMetrics) currentStuckWorkers() int64 {
	return m.stuckWorkers.Load()
}

type handlerActivity struct {
	metrics *deliveryMetrics
	mu      sync.Mutex
	stuck   bool
	done    bool
}

func (m *deliveryMetrics) beginHandler() *handlerActivity {
	m.activeHandlers.Add(1)
	return &handlerActivity{metrics: m}
}

func (a *handlerActivity) markStuck() {
	a.mu.Lock()
	if !a.done && !a.stuck {
		a.stuck = true
		a.metrics.stuckWorkers.Add(1)
	}
	a.mu.Unlock()
}

func (a *handlerActivity) finish() {
	a.mu.Lock()
	if a.done {
		a.mu.Unlock()
		return
	}
	a.done = true
	if a.stuck {
		a.metrics.stuckWorkers.Add(-1)
	}
	a.mu.Unlock()
	a.metrics.endHandler()
}

func (m *deliveryMetrics) endHandler() {
	if m.activeHandlers.Add(-1) == 0 {
		m.unregisterIfReady()
	}
}

func (m *deliveryMetrics) retire() {
	m.retired.Store(true)
	m.unregisterIfReady()
}

func (m *deliveryMetrics) unregisterIfReady() {
	if !m.retired.Load() || m.activeHandlers.Load() != 0 {
		return
	}
	m.unregisterOnce.Do(func() {
		if m.unregister != nil {
			m.unregister()
		}
	})
}

// Run starts the consumer, owns its fetcher and workers, and returns when the
// consumer stops, the caller cancels ctx, or a driver error requests shutdown.
func (r *Runner) Run(ctx context.Context) error {
	if r == nil {
		return errors.New("f1: runner is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.client.mu.Lock()
	if r.client.closed || r.client.closing || r.client.conn == nil {
		r.client.mu.Unlock()
		return errors.New("f1: client is closing")
	}
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		r.client.mu.Unlock()
		return errors.New("f1: runner is already running")
	}
	r.started = true
	r.done = make(chan struct{})
	r.inflight = newInflightRegistry()
	r.lifecycle = lifecycle.New()
	r.accounting = lifecycle.NewAccounting(r.inflight.registry)
	r.deliveryIDs = nil
	runCtx, cancel := context.WithCancel(ctx)
	r.runCtx, r.cancel = runCtx, cancel
	handlerCtx, handlerCancel := context.WithCancel(ctx)
	r.handlerCtx, r.handlerCancel = handlerCtx, handlerCancel
	handlerShutdownCtx, handlerShutdownCancel := context.WithCancel(context.Background())
	r.handlerShutdownCtx, r.handlerShutdownCancel = handlerShutdownCtx, handlerShutdownCancel
	r.asyncGroup = new(errgroup.Group)
	group := new(errgroup.Group)
	r.group = group
	r.mu.Unlock()
	r.client.mu.Unlock()

	defer finishRunner(r)
	consumer, err := openRunnerConsumer(r, runCtx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.consumer = consumer
	r.mu.Unlock()

	if err := r.lifecycle.Transition(lifecycle.Ready); err != nil {
		return err
	}
	deliveries := make(chan delivery, r.subscription.Concurrency)
	group.Go(func() error { return fetchRunner(r, runCtx, deliveries) })
	group.Go(func() error { return runDispatchPipeline(r, runCtx, deliveries) })
	group.Go(func() error { return consumeRunnerErrors(r, runCtx) })
	err = group.Wait()
	if err == nil {
		err = runnerError(r)
	}
	shutdownCtx := context.WithoutCancel(runnerSettlementContext(r, runCtx))
	shutdownErr := r.lifecycle.Drain(shutdownCtx, lifecycle.Config{
		Clock:        r.client.options.clock,
		DrainTimeout: r.client.config.Lifecycle.DrainTimeout,
		HandlerGrace: r.client.config.Lifecycle.HandlerGrace,
		FlushTimeout: r.client.config.Lifecycle.FlushTimeout,
		CloseTimeout: r.client.config.Lifecycle.CloseTimeout,
	}, lifecycle.Hooks{
		WaitSettled: func(ctx context.Context) error {
			return r.inflight.WaitFor(ctx, runnerDeliveryIDs(r))
		},
		Close: func(ctx context.Context) error {
			return stopRunnerConsumer(r, ctx)
		},
	})
	if err == nil {
		err = shutdownErr
	} else if shutdownErr != nil {
		err = errors.Join(err, shutdownErr)
	}
	return err
}

func runDispatchPipeline(r *Runner, ctx context.Context, deliveries <-chan delivery) error {
	scheduler, err := newRunnerScheduler(r)
	if err != nil {
		return err
	}
	pipelineCtx := context.WithoutCancel(ctx)
	pool, err := dispatch.NewPool(pipelineCtx, r.subscription.Concurrency, r.subscription.Mode == OrderedByKey, r.subscription.Prefetch)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.dispatchPool = pool
	r.mu.Unlock()
	defer func() {
		pool.Close()
		r.mu.Lock()
		r.dispatchPool = nil
		r.mu.Unlock()
	}()

	var pending *delivery
	open := true
	for open || pending != nil || schedulerHasItems(scheduler) {
		if pending != nil {
			laneID := deliveryLane(r, pending.message)
			err := scheduler.Enqueue(laneID, sched.Item{Value: *pending, EnqueuedAt: r.client.options.clock.Now()})
			if err == nil {
				pending = nil
			} else if !errors.Is(err, sched.ErrLaneFull) {
				return err
			}
		}
		if item, ok := scheduler.Next(); ok {
			work := item.Value.(delivery)
			if err := pool.Submit(pipelineCtx, dispatch.Work{
				Key: append([]byte(nil), work.message.Key...),
				Run: func(context.Context) { processDelivery(r, ctx, work) },
			}); err != nil {
				return err
			}
			continue
		}
		if pending != nil {
			if err := pool.WaitFree(pipelineCtx); err != nil {
				return err
			}
			continue
		}
		if !open {
			break
		}
		item, ok := <-deliveries
		if !ok {
			open = false
			continue
		}
		pending = &item
	}
	return nil
}

func schedulerHasItems(scheduler *sched.Scheduler) bool {
	return scheduler != nil && scheduler.Pending() > 0
}

func newRunnerScheduler(r *Runner) (*sched.Scheduler, error) {
	weights := r.subscription.Fairness.Weights
	budgets := r.subscription.Fairness.Budgets
	divisor := r.subscription.Fairness.RetryWeightDivisor
	if divisor < 1 {
		divisor = 2
	}
	factor := r.subscription.Fairness.PrefetchFactor
	if factor < 1 {
		factor = 2
	}
	type laneMeta struct {
		group  string
		weight int
		budget time.Duration
	}
	meta := make(map[string]laneMeta, len(r.subscription.Topics)*len(r.subscription.Priorities)*(1+retryTiers(r.subscription.Retry)))
	groups := make(map[string]struct{})
	totalWeight := 0
	for _, topic := range r.subscription.Topics {
		for _, priority := range r.subscription.Priorities {
			weight := weights[priority]
			if weight < 1 {
				weight = 1
			}
			budget := budgets[priority]
			for tier := 0; tier <= retryTiers(r.subscription.Retry); tier++ {
				laneID := schedulerLaneID(topicFor(topic), priority, tier)
				group := laneID
				if tier > 0 {
					group = schedulerRetryGroupID(topicFor(topic), priority)
				}
				laneWeight, laneBudget := weight, budget
				if tier > 0 {
					laneWeight /= divisor
					if laneWeight < 1 {
						laneWeight = 1
					}
					laneBudget *= 2
				}
				meta[laneID] = laneMeta{group: group, weight: laneWeight, budget: laneBudget}
				if _, exists := groups[group]; !exists {
					groups[group] = struct{}{}
					totalWeight += laneWeight
				}
			}
		}
	}
	if totalWeight < 1 {
		totalWeight = 1
	}
	specs := make([]sched.LaneSpec, 0, len(meta))
	ids := make([]string, 0, len(meta))
	for id := range meta {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		lane := meta[id]
		capacity := (r.subscription.Concurrency*lane.weight + totalWeight - 1) / totalWeight
		if capacity < 2 {
			capacity = 2
		}
		capacity *= factor
		specs = append(specs, sched.LaneSpec{ID: id, Group: lane.group, Weight: lane.weight, Budget: lane.budget, Capacity: capacity})
	}
	return sched.New(specs, r.client.options.clock, r.subscription.Fairness.AgingEnabled)
}

func schedulerLaneID(topic string, priority Priority, tier int) string {
	if tier > 0 {
		return fmt.Sprintf("%s.%s.retry.%d", topic, priority, tier)
	}
	return fmt.Sprintf("%s.%s.main", topic, priority)
}

func schedulerRetryGroupID(topic string, priority Priority) string {
	return fmt.Sprintf("%s.%s.retry", topic, priority)
}

func deliveryLane(r *Runner, message driver.InboundMessage) string {
	envelope, err := DecodeHeaders(inboundHeaders(message.Headers))
	if err == nil {
		tier := retryTierForDestination(message.Destination)
		topic := topicFor(envelope.Type)
		effective := r.client.effective
		for _, configured := range r.subscription.Topics {
			logical := topicFor(configured)
			if tier == 0 {
				if consumeDestination(effective, r.client.source, logical, envelope.Priority, r.subscription.Name) == message.Destination {
					topic = logical
					break
				}
				continue
			}
			if retryDestinationFor(r.client.source, logical, envelope.Priority, tier, r.subscription.Name) == message.Destination {
				topic = logical
				break
			}
		}
		return schedulerLaneID(topic, envelope.Priority, tier)
	}
	if len(r.subscription.Topics) > 0 && len(r.subscription.Priorities) > 0 {
		return schedulerLaneID(topicFor(r.subscription.Topics[0]), r.subscription.Priorities[0], 0)
	}
	return ""
}

func retryTierForDestination(destination string) int {
	index := strings.LastIndex(destination, ".retry.")
	if index < 0 {
		return 0
	}
	rest := destination[index+len(".retry."):]
	if end := strings.IndexByte(rest, '.'); end >= 0 {
		rest = rest[:end]
	}
	tier, err := strconv.Atoi(rest)
	if err != nil || tier < 1 {
		return 0
	}
	return tier
}

// Drain stops fetching and waits for all worker deliveries to settle. A
// runner that has not started is already drained.
func (r *Runner) Drain(ctx context.Context) error {
	if r == nil {
		return errors.New("f1: runner is nil")
	}
	r.mu.Lock()
	if !r.started || r.finished {
		r.mu.Unlock()
		return nil
	}
	cancel := r.cancel
	handlerCancel := r.handlerCancel
	handlerShutdownCancel := r.handlerShutdownCancel
	done := r.done
	if r.lifecycle != nil {
		switch r.lifecycle.State() {
		case lifecycle.Ready:
			if err := r.lifecycle.Transition(lifecycle.Draining); err != nil {
				r.mu.Unlock()
				return err
			}
		case lifecycle.Starting:
		case lifecycle.Draining, lifecycle.Settling, lifecycle.Flushing:
		case lifecycle.Closed:
			r.mu.Unlock()
			return nil
		default:
			r.mu.Unlock()
			return fmt.Errorf("f1: runner cannot drain from lifecycle state %s", r.lifecycle.State())
		}
	}
	r.draining = true
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if handlerCancel != nil {
		drainTimeout := r.client.config.Lifecycle.DrainTimeout
		if drainTimeout <= 0 {
			drainTimeout = time.Minute
		}
		grace := r.client.config.Lifecycle.HandlerGrace
		if grace < 0 || grace >= drainTimeout {
			grace = 0
		}
		delay := drainTimeout - grace
		// The grace timer must not join the group whose completion it waits for.
		// It watches done, and done is closed only after that group's Wait returns,
		// so putting it there makes Wait block for the whole grace delay even when
		// every delivery has already finished. asyncGroup is never waited by the
		// runner, which is exactly what this goroutine needs.
		r.mu.Lock()
		group := r.asyncGroup
		r.mu.Unlock()
		if delay <= 0 || group == nil {
			handlerCancel()
			if handlerShutdownCancel != nil {
				handlerShutdownCancel()
			}
		} else {
			group.Go(func() error {
				timer := r.client.options.clock.Timer(delay)
				defer timer.Stop()
				select {
				case <-timer.C:
					handlerCancel()
					if handlerShutdownCancel != nil {
						handlerShutdownCancel()
					}
				case <-done:
				}
				return nil
			})
		}
	}
	select {
	case <-done:
		return runnerError(r)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func finishRunner(r *Runner) {
	r.mu.Lock()
	if r.finished {
		r.mu.Unlock()
		return
	}
	r.finished = true
	if r.settleCancel != nil {
		r.settleCancel()
	}
	if r.handlerCancel != nil {
		r.handlerCancel()
	}
	if r.handlerShutdownCancel != nil {
		r.handlerShutdownCancel()
	}
	// asyncGroup is intentionally not waited here. Handler and callback code may
	// ignore cancellation; waiting for a non-cooperative handler would make the
	// runner outlive its shutdown bound. Terminal callbacks are bounded before
	// settlement, while any callback that exceeds that bound is allowed to finish
	// independently after ownership of the runner has ended.
	if r.done != nil {
		close(r.done)
	}
	r.mu.Unlock()
	if r.client != nil {
		r.client.mu.Lock()
		delete(r.client.runners, r)
		r.client.mu.Unlock()
	}
	r.metrics.retire()
}

func runnerError(r *Runner) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.runErr
}

func setRunnerError(r *Runner, err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	if r.runErr == nil {
		r.runErr = err
	}
	r.mu.Unlock()
}

func runnerDeliveryIDs(r *Runner) []uint64 {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.deliveryIDs...)
}

//nolint:contextcheck // helper preserves the caller context while using an injected timer.
func waitForClock(ctx context.Context, clk clock.Clock, delay time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay <= 0 {
		return nil
	}
	if clk == nil {
		clk = clock.NewReal()
	}
	timer := clk.Timer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

//nolint:contextcheck // helper derives the phase context from the caller.
func runWithClockTimeout(parent context.Context, clk clock.Clock, timeout time.Duration, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}
	if parent == nil {
		parent = context.Background()
	}
	if timeout <= 0 {
		return fn(parent)
	}
	if clk == nil {
		clk = clock.NewReal()
	}
	phaseCtx, cancel := context.WithCancel(parent)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fn(phaseCtx) }()
	timer := clk.Timer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return context.DeadlineExceeded
	case <-parent.Done():
		return parent.Err()
	}
}

func runnerSettlementContext(r *Runner, fallback context.Context) context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.settleCtx != nil {
		return r.settleCtx
	}
	if fallback != nil && fallback.Err() != nil {
		drainTimeout := r.client.config.Lifecycle.DrainTimeout
		if drainTimeout <= 0 {
			drainTimeout = time.Minute
		}
		base := context.WithoutCancel(fallback)
		r.settleCtx, r.settleCancel = context.WithTimeout(base, drainTimeout)
		return r.settleCtx
	}
	return fallback
}

func runnerLogger(r *Runner) *slog.Logger {
	if r == nil || r.client == nil || r.client.options.logger == nil {
		return slog.Default()
	}
	return r.client.options.logger
}

func openRunnerConsumer(r *Runner, ctx context.Context) (driver.Consumer, error) {
	r.client.mu.Lock()
	conn := r.client.conn
	effective := r.client.effective
	source := r.client.source
	r.client.mu.Unlock()
	policy := r.client.topologyPolicy()
	if conn == nil {
		return nil, errors.New("f1: client is not connected")
	}
	destinations := subscriptionDestinations(effective, source, r.subscription)
	if policy != driver.TopologyNone {
		if admin := conn.Admin(); admin != nil {
			topology := subscriptionTopologySpecs(effective, source, r.subscription)
			topology.Policy = policy
			if _, err := admin.EnsureTopology(ctx, topology); err != nil {
				return nil, fmt.Errorf("f1: ensure subscription topology: %w", err)
			}
		}
	}
	perDestination := make(map[string]int, len(destinations))
	remaining := r.config.Prefetch
	for i, destination := range destinations {
		share := 1
		if remaining > len(destinations)-i {
			share = remaining / (len(destinations) - i)
			if share < 1 {
				share = 1
			}
		}
		perDestination[destination] = share
		remaining -= share
	}
	consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{
		Group:          r.subscription.Name,
		Destinations:   destinations,
		Prefetch:       r.config.Prefetch,
		PerDestination: perDestination,
		Effective:      effective,
		StartAt:        driver.StartEarliest,
	})
	if err != nil {
		return nil, fmt.Errorf("f1: create subscription %s: %w", r.subscription.Name, err)
	}
	if consumer == nil {
		return nil, errors.New("f1: driver returned a nil consumer")
	}
	return consumer, nil
}

func consumeRunnerErrors(r *Runner, ctx context.Context) error {
	r.mu.Lock()
	consumer := r.consumer
	r.mu.Unlock()
	if consumer == nil || consumer.Errors() == nil {
		<-ctx.Done()
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case err, ok := <-consumer.Errors():
			if !ok {
				return nil
			}
			if err == nil {
				continue
			}
			runnerLogger(r).Error("f1 consumer error", "subscription", r.subscription.Name, "error", err)
			setRunnerError(r, err)
			if r.cancel != nil {
				r.cancel()
			}
			return nil
		}
	}
}

func fetchRunner(r *Runner, ctx context.Context, dispatch chan<- delivery) error {
	r.mu.Lock()
	consumer := r.consumer
	r.mu.Unlock()
	messages := consumer.Messages()
	for {
		select {
		case <-ctx.Done():
			return fetchRunnerAfterCancel(r, ctx, messages, dispatch)
		case message, ok := <-messages:
			if !ok {
				if r.cancel != nil {
					r.cancel()
				}
				close(dispatch)
				return nil
			}
			if !enqueueDelivery(r, ctx, dispatch, message) {
				close(dispatch)
				return nil
			}
		}
	}
}

func fetchRunnerAfterCancel(r *Runner, parent context.Context, messages <-chan driver.InboundMessage, dispatch chan<- delivery) error {
	drainTimeout := r.client.config.Lifecycle.DrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = time.Minute
	}
	drainBase := context.WithoutCancel(parent)
	drainCtx, cancel := context.WithTimeout(drainBase, drainTimeout)
	defer cancel()
	r.mu.Lock()
	if r.settleCancel != nil {
		r.settleCancel()
	}
	r.settleCtx, r.settleCancel = context.WithTimeout(drainBase, drainTimeout)
	r.mu.Unlock()
	r.mu.Lock()
	consumer := r.consumer
	r.mu.Unlock()
	if err := consumer.Drain(drainCtx); err != nil {
		setRunnerError(r, err)
	}
	for {
		select {
		case message, ok := <-messages:
			if !ok {
				close(dispatch)
				return nil
			}
			if !enqueueDelivery(r, drainCtx, dispatch, message) {
				close(dispatch)
				return nil
			}
		default:
			close(dispatch)
			return nil
		}
	}
}

func enqueueDelivery(r *Runner, ctx context.Context, dispatch chan<- delivery, message driver.InboundMessage) bool {
	id := r.inflight.Add(message)
	item := delivery{id: id, message: message}
	r.mu.Lock()
	r.deliveryIDs = append(r.deliveryIDs, id)
	r.mu.Unlock()
	select {
	case dispatch <- item:
		return true
	case <-ctx.Done():
		// A cancellation path may still have a delivery in the channel. Put it
		// back through the broker rather than losing it locally.
		state := &deliveryState{id: id}
		err := nackDelivery(r, runnerSettlementContext(r, ctx), message, driver.NackOptions{Requeue: true}, state)
		outcome := settlementOutcomeRequeued
		if err != nil {
			outcome = settlementOutcomeUnknown
		}
		r.inflight.RemoveAs(id, outcome)
		return false
	}
}

func processDelivery(r *Runner, ctx context.Context, item delivery) {
	abandoned := false
	state := &deliveryState{id: item.id}
	var envelope Envelope
	defer func() {
		if recovered := recover(); recovered != nil {
			err := fmt.Errorf("handler panic: %v\n%s", recovered, debug.Stack())
			deadLetterAndSettle(r, ctx, item.message, envelope, ReasonPanic, err, state)
		}
		if !state.settled && !state.attempted {
			_ = nackDelivery(r, runnerSettlementContext(r, ctx), item.message, driver.NackOptions{Requeue: true}, state)
		}
		operation := state.operation
		if !state.settled {
			switch operation {
			case settlementOperationAck:
				_ = ackDelivery(r, runnerSettlementContext(r, ctx), item.message, state)
			case settlementOperationNack:
				_ = nackDelivery(r, runnerSettlementContext(r, ctx), item.message, driver.NackOptions{Requeue: true}, state)
			}
		}
		if !state.settled && operation == settlementOperationAck {
			_ = nackDelivery(r, runnerSettlementContext(r, ctx), item.message, driver.NackOptions{Requeue: true}, state)
		}
		operation = state.operation
		outcome := settlementOutcomeUnknown
		if abandoned {
			outcome = settlementOutcomeAbandoned
		} else if state.settled {
			outcome = settlementOutcomeSettled
			if operation == settlementOperationNack {
				outcome = settlementOutcomeRequeued
			}
		}
		r.inflight.RemoveAs(item.id, outcome)
	}()
	dispatchMessage(r, ctx, item.message, &envelope, &abandoned, state)
}

func dispatchMessage(r *Runner, ctx context.Context, message driver.InboundMessage, envelopeOut *Envelope, abandoned *bool, states ...*deliveryState) bool {
	state := stateFor(states)
	headers := inboundHeaders(message.Headers)
	envelope, err := DecodeHeaders(headers)
	if err != nil {
		return deadLetterAndSettle(r, ctx, message, Envelope{}, ReasonDecode, err, state)
	}
	if envelope.Attempt < 1 {
		envelope.Attempt = 1
	}
	*envelopeOut = envelope
	// Record at receipt, before handler work or settlement: a failed settlement
	// can cause the broker to redeliver this delivery.
	recordAttemptDivergence(r, envelope.Type, envelope.Priority, envelope.Attempt, message.DeliveryCount)
	if max := r.client.config.Codec.MaxBodyBytes; max > 0 && len(message.Body) > max {
		return deadLetterAndSettle(r, ctx, message, envelope, ReasonDecode, fmt.Errorf("body exceeds codec.maxBodyBytes (%d)", max), state)
	}
	if envelope.Expiry != nil && !envelope.Expiry.After(r.client.options.clock.Now()) {
		return deadLetterAndSettle(r, ctx, message, envelope, ReasonExpired, errors.New("event expired before handling"), state)
	}
	handler := matchHandler(r.subscription.Handlers, envelope.Type)
	if handler == nil {
		if r.subscription.UnmatchedPolicy == DeadLetter {
			return deadLetterAndSettle(r, ctx, message, envelope, ReasonUnmatched, errors.New("no handler matched event type"), state)
		}
		runnerNotifyDiscarded(r, runnerSettlementContext(r, ctx), discardUnmatched(envelope))
		return ackDelivery(r, runnerSettlementContext(r, ctx), message, state)
	}
	event := &Event{envelope: envelope, raw: append([]byte(nil), message.Body...), codec: r.client.options.codec, headers: headers}
	result := invokeHandler(r, ctx, handler, event)
	if result.stuck {
		*abandoned = true
		return false
	}
	if result.panic != nil {
		return deadLetterAndSettle(r, ctx, message, envelope, ReasonPanic, result.panic, state)
	}
	if result.err == nil {
		return ackDelivery(r, runnerSettlementContext(r, ctx), message, state)
	}
	outcome := classifyRetryError(result.err)
	switch outcome.Kind {
	case retry.Drop:
		runnerNotifyDiscarded(r, runnerSettlementContext(r, ctx), Discarded{Envelope: envelope, Reason: DiscardDropped, Err: result.err})
		return ackDelivery(r, runnerSettlementContext(r, ctx), message, state)
	case retry.Terminal:
		return deadLetterAndSettle(r, ctx, message, envelope, ReasonTerminal, result.err, state)
	case retry.Unavailable:
		if retryTiers(r.subscription.Retry) == 0 {
			return deadLetterAndSettle(r, ctx, message, envelope, ReasonDependencyUnavailable, result.err, state)
		}
		deferrals := envelope.Deferrals + 1
		if deferrals > r.subscription.MaxDeferrals {
			return deadLetterAndSettle(r, ctx, message, envelope, ReasonDependencyUnavailable, result.err, state)
		}
		return retryAndSettle(r, ctx, message, envelope, result.err, deferrals, false, state)
	}
	maxAttempts := envelope.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = r.subscription.Retry.MaxAttempts
	}
	if envelope.Attempt >= maxAttempts {
		return deadLetterAndSettle(r, ctx, message, envelope, ReasonMaxAttempts, result.err, state)
	}
	return retryAndSettle(r, ctx, message, envelope, result.err, 0, true, state)
}

func classifyRetryError(err error) retry.Outcome {
	switch {
	case IsTerminal(err):
		return retry.Outcome{Kind: retry.Terminal, Err: err}
	case IsDropped(err):
		return retry.Outcome{Kind: retry.Drop, Err: err}
	case IsUnavailable(err):
		return retry.Outcome{Kind: retry.Unavailable, Err: err}
	}
	if delay, ok := RetryDelay(err); ok {
		if delay < 0 {
			delay = 0
		}
		return retry.Outcome{Kind: retry.RetryAfter, Delay: delay, Err: err}
	}
	return retry.Classify(err)
}

func recordAttemptDivergence(r *Runner, eventType string, priority Priority, attempt, deliveryCount int) {
	if r == nil || deliveryCount < 0 {
		return
	}
	var difference int
	if attempt >= deliveryCount {
		difference = attempt - deliveryCount
	} else {
		difference = deliveryCount - attempt
	}
	r.metrics.observeAttemptDivergence(eventType, priority, uint64(uint(difference)))
}

func stateFor(states []*deliveryState) *deliveryState {
	if len(states) > 0 && states[0] != nil {
		return states[0]
	}
	return &deliveryState{}
}

type handlerResult struct {
	err   error
	panic error
	stuck bool
}

func invokeHandler(r *Runner, parent context.Context, handler Handler, event *Event) handlerResult {
	timeout := r.subscription.HandlerTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	r.mu.Lock()
	base := r.handlerCtx //nolint:contextcheck // handlerCtx is derived from the Run context and survives the drain grace window.
	shutdownCtx := r.handlerShutdownCtx
	draining := r.draining
	if base == nil {
		base = parent
	}
	r.mu.Unlock()
	handlerCtx, cancel := context.WithTimeout(base, timeout)
	defer cancel()
	done := make(chan handlerResult, 1)
	r.mu.Lock()
	group := r.asyncGroup
	if group == nil {
		group = r.group
	}
	if group == nil {
		group = new(errgroup.Group)
	}
	r.mu.Unlock()
	activity := r.metrics.beginHandler()
	group.Go(func() error {
		defer activity.finish()
		result := handlerResult{}
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					result.panic = fmt.Errorf("handler panic: %v\n%s", recovered, debug.Stack())
				}
			}()
			result.err = handler.Handle(handlerCtx, event)
		}()
		var panicErr *handlerPanicError
		if errors.As(result.err, &panicErr) {
			result.panic = panicErr
			result.err = nil
		}
		done <- result
		return nil
	})
	stuck := r.client.options.clock.Timer(timeout * 2)
	defer stuck.Stop()
	parentDone := parent.Done()
	if draining {
		parentDone = nil
	}
	var shutdownDone <-chan struct{}
	if shutdownCtx != nil {
		shutdownDone = shutdownCtx.Done()
	}
	select {
	case result := <-done:
		return result
	case <-parentDone:
		return handlerResult{stuck: true}
	case <-shutdownDone:
		return handlerResult{stuck: true}
	case <-stuck.C:
		activity.markStuck()
		runnerLogger(r).Warn("f1 stuck worker", "metric", "f1_stuck_workers", "subscription", r.subscription.Name, "threshold", 2*timeout)
	}
	stackTimer := r.client.options.clock.Timer(timeout * 2)
	defer stackTimer.Stop()
	select {
	case result := <-done:
		return result
	case <-parentDone:
		return handlerResult{stuck: true}
	case <-shutdownDone:
		return handlerResult{stuck: true}
	case <-stackTimer.C:
		buffer := make([]byte, 64<<10)
		n := runtime.Stack(buffer, true)
		runnerLogger(r).Error("f1 worker exceeded stuck threshold", "metric", "f1_stuck_workers", "subscription", r.subscription.Name, "threshold", 4*timeout, "stack", string(buffer[:n]))
		return handlerResult{stuck: true}
	}
}

func ackDelivery(r *Runner, ctx context.Context, message driver.InboundMessage, states ...*deliveryState) bool {
	return ackDeliveryAs(r, ctx, message, lifecycle.Handled, states...)
}

func ackDeliveryAs(r *Runner, ctx context.Context, message driver.InboundMessage, disposition lifecycle.Disposition, states ...*deliveryState) bool {
	state := stateFor(states)
	if message.Settle == nil {
		return false
	}
	state.operation = settlementOperationAck
	if r != nil && r.inflight != nil {
		r.inflight.SetDisposition(state.id, dispatch.Disposition(disposition))
	}
	err := message.Settle.Ack(ctx)
	state.attempted = true
	state.settled = err == nil
	return state.settled
}

func nackDelivery(r *Runner, ctx context.Context, message driver.InboundMessage, options driver.NackOptions, states ...*deliveryState) error {
	state := stateFor(states)
	if message.Settle == nil {
		return errors.New("f1: delivered message has no settler")
	}
	state.operation = settlementOperationNack
	if r != nil && r.inflight != nil {
		r.inflight.SetDisposition(state.id, dispatch.DispositionRequeued)
	}
	err := message.Settle.Nack(ctx, options)
	state.attempted = true
	state.settled = err == nil
	return err
}

func deadLetterAndSettle(r *Runner, ctx context.Context, message driver.InboundMessage, envelope Envelope, reason DeathReason, lastErr error, states ...*deliveryState) bool {
	state := stateFor(states)
	if err := deadLetter(r, ctx, message, envelope, reason, lastErr); err != nil {
		return false
	}
	return ackDeliveryAs(r, runnerSettlementContext(r, ctx), message, lifecycle.DeadLettered, state)
}

func deadLetter(r *Runner, ctx context.Context, message driver.InboundMessage, envelope Envelope, reason DeathReason, lastErr error) error {
	if reason == ReasonDecode && envelope.ID == "" {
		// Preserve the raw headers when the envelope itself could not be decoded.
		headers := inboundHeaders(message.Headers)
		destination := deadLetterDestination(r, envelope, message)
		setDeathHeaders(headers, reason, lastErr, r.client.options.clock.Now().UTC(), message.Destination)
		if err := publishMessages(r.client, runnerSettlementContext(r, ctx), true, driver.OutboundMessage{Destination: destination, Key: append([]byte(nil), message.Key...), Headers: headerSlice(headers), Body: append([]byte(nil), message.Body...)}); err != nil {
			return err
		}
		runnerNotifyDeadLetter(r, runnerSettlementContext(r, ctx), DeadLettered{Envelope: Envelope{}, Reason: reason, Attempt: 0, LastErr: lastErr, Destination: destination})
		return nil
	}
	death := envelope
	if death.Attempt < 1 {
		death.Attempt = 1
	}
	if death.OriginalDest == "" {
		death.OriginalDest = message.Destination
	}
	death.DeathReason = reason
	death.DeathError = truncateError(lastErr)
	now := r.client.options.clock.Now().UTC()
	death.DeathTime = &now
	destination := deadLetterDestination(r, death, message)
	encoded, err := death.EncodeHeaders(r.client.config.Codec.MaxHeaderBytes)
	if err != nil {
		return err
	}
	out := driver.OutboundMessage{Destination: destination, Key: append([]byte(nil), message.Key...), Body: append([]byte(nil), message.Body...)}
	for key, value := range encoded {
		out.Headers = append(out.Headers, driver.Header{Key: key, Value: []byte(value)})
	}
	sort.Slice(out.Headers, func(i, j int) bool { return out.Headers[i].Key < out.Headers[j].Key })
	if err := publishMessages(r.client, runnerSettlementContext(r, ctx), true, out); err != nil {
		return err
	}
	runnerNotifyDeadLetter(r, runnerSettlementContext(r, ctx), DeadLettered{Envelope: death, Reason: reason, Attempt: death.Attempt, LastErr: lastErr, Destination: destination})
	return nil
}

func retryAndSettle(r *Runner, ctx context.Context, message driver.InboundMessage, envelope Envelope, lastErr error, deferrals int, attempt bool, states ...*deliveryState) bool {
	state := stateFor(states)
	copyEnvelope := envelope
	if copyEnvelope.OriginalDest == "" {
		copyEnvelope.OriginalDest = message.Destination
	}
	copyEnvelope.DeathError = ""
	copyEnvelope.DeathReason = ReasonUnspecified
	copyEnvelope.DeathTime = nil
	if deferrals > 0 {
		copyEnvelope.Deferrals = deferrals
	} else if attempt {
		copyEnvelope.Attempt++
	}
	if copyEnvelope.MaxAttempts == 0 {
		copyEnvelope.MaxAttempts = r.subscription.Retry.MaxAttempts
	}
	retryConfig := retry.Config{
		MaxAttempts:     r.subscription.Retry.MaxAttempts,
		InitialInterval: r.subscription.Retry.InitialInterval,
		Multiplier:      r.subscription.Retry.Multiplier,
		MaxInterval:     r.subscription.Retry.MaxInterval,
		Jitter:          r.subscription.Retry.Jitter,
		Tiers:           r.subscription.Retry.Tiers,
	}
	tiers := retryConfig.TierCount()
	if tiers == 0 {
		return deadLetterAndSettle(r, ctx, message, envelope, ReasonMaxAttempts, lastErr, state)
	}
	tier := retry.ResolveTier(retryConfig, envelope.Attempt)
	if deferrals > 0 {
		tier = retry.DeferralTier(deferrals, tiers)
	}
	delay := retryConfig.DelayFor(tier)
	if requested, ok := RetryDelay(lastErr); ok {
		resolvedTier, resolvedDelay, _ := retry.ResolveRetryAfter(retryConfig, requested)
		tier, delay = resolvedTier, resolvedDelay
	}
	now := r.client.options.clock.Now().UTC()
	due := now.Add(delay)
	copyEnvelope.DueTime = &due
	encoded, err := copyEnvelope.EncodeHeaders(r.client.config.Codec.MaxHeaderBytes)
	if err != nil {
		return false
	}
	destination := retryDestination(r, copyEnvelope, tier)
	out := driver.OutboundMessage{Destination: destination, Key: append([]byte(nil), message.Key...), Body: append([]byte(nil), message.Body...), DelayUntil: due}
	for key, value := range encoded {
		out.Headers = append(out.Headers, driver.Header{Key: key, Value: []byte(value)})
	}
	sort.Slice(out.Headers, func(i, j int) bool { return out.Headers[i].Key < out.Headers[j].Key })
	if err := publishMessages(r.client, runnerSettlementContext(r, ctx), true, out); err != nil {
		return false
	}
	return ackDeliveryAs(r, runnerSettlementContext(r, ctx), message, lifecycle.Retried, state)
}

func stopRunnerConsumer(r *Runner, ctx context.Context) error {
	r.mu.Lock()
	consumer := r.consumer
	r.mu.Unlock()
	if consumer == nil {
		return nil
	}
	if err := consumer.Stop(ctx); err != nil {
		return err
	}
	return nil
}

func matchHandler(handlers map[string]Handler, eventType string) Handler {
	if handler := handlers[eventType]; handler != nil {
		return handler
	}
	var match Handler
	longest := -1
	for pattern, handler := range handlers {
		if handler == nil || !strings.HasSuffix(pattern, "*") {
			continue
		}
		prefix := strings.TrimSuffix(pattern, "*")
		if strings.HasPrefix(eventType, prefix) && len(prefix) > longest {
			match, longest = handler, len(prefix)
		}
	}
	return match
}

func inboundHeaders(headers []driver.Header) map[string]string {
	result := make(map[string]string, len(headers))
	for _, header := range headers {
		result[header.Key] = string(header.Value)
	}
	return result
}

func headerSlice(headers map[string]string) []driver.Header {
	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]driver.Header, 0, len(keys))
	for _, key := range keys {
		result = append(result, driver.Header{Key: key, Value: []byte(headers[key])})
	}
	return result
}

func setDeathHeaders(headers map[string]string, reason DeathReason, err error, at time.Time, original string) {
	headers["f1deathreason"] = reason.String()
	headers["f1deatherror"] = truncateError(err)
	headers["f1deathtime"] = at.Format(time.RFC3339Nano)
	headers["f1originaldest"] = original
}

func truncateError(err error) string {
	if err == nil {
		return ""
	}
	value := err.Error()
	if len(value) > 4<<10 {
		return value[:4<<10]
	}
	return value
}

func subscriptionDestinations(effective driver.Capabilities, source string, sub Subscription) []string {
	set := make(map[string]struct{})
	for _, topic := range sub.Topics {
		logical := topicFor(topic)
		for _, priority := range sub.Priorities {
			set[consumeDestination(effective, source, logical, priority, sub.Name)] = struct{}{}
			for tier := 1; tier <= retryTiers(sub.Retry); tier++ {
				set[retryDestinationFor(source, logical, priority, tier, sub.Name)] = struct{}{}
			}
		}
	}
	result := make([]string, 0, len(set))
	for name := range set {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

func destinationScope(source string, sub Subscription) []string {
	env := sourceEnvironment(source)
	set := make(map[string]struct{})
	for _, topic := range sub.Topics {
		logical := topicFor(topic)
		set[fmt.Sprintf("f1.%s.%s.%s.", env, logical, sub.Name)] = struct{}{}
		set[fmt.Sprintf("f1.%s.%s.dlq.%s.", env, logical, sub.Name)] = struct{}{}
	}
	result := make([]string, 0, len(set))
	for scope := range set {
		result = append(result, scope)
	}
	sort.Strings(result)
	return result
}

func subscriptionTopologySpecs(effective driver.Capabilities, source string, sub Subscription) driver.TopologySpec {
	result := driver.TopologySpec{Effective: effective, Scope: destinationScope(source, sub)}
	seen := make(map[string]struct{})
	exchanges := make(map[string]struct{})
	add := func(spec driver.DestinationSpec) {
		if _, ok := seen[spec.Name]; ok {
			return
		}
		seen[spec.Name] = struct{}{}
		result.Destinations = append(result.Destinations, spec)
	}
	addExchange := func(spec driver.ExchangeSpec) {
		if _, ok := exchanges[spec.Name]; ok {
			return
		}
		exchanges[spec.Name] = struct{}{}
		result.Exchanges = append(result.Exchanges, spec)
	}
	for _, topic := range sub.Topics {
		logical := topicFor(topic)
		for _, priority := range sub.Priorities {
			entryPoint := publishEntryPoint(source, logical, priority)
			main := consumeDestination(effective, source, logical, priority, sub.Name)
			// The backstop catches messages the BROKER dead-lettered on its own
			// delivery counter, which the core never classified. Only a broker
			// that routes to a dead-letter target on a delivery limit can put a
			// message there, so on any other broker the route, the counter and
			// the queue would all be inert - and the queue would sit empty
			// forever, which reads as a bug to whoever finds it.
			backstop := deadLetterDestinationFor(source, logical, sub.Name) + ".backstop"
			var route *driver.Route
			limit := 0
			if effective.NativeDLQ {
				route = &driver.Route{Key: backstop}
				// Headroom over the core's own ladder: the core dead-letters
				// first, so this firing at all means the two disagree.
				limit = sub.Retry.MaxAttempts + 5
			}
			add(driver.DestinationSpec{Name: main, Kind: driver.DestMain, Durable: true, DeadLetter: route, DeliveryLimit: limit})
			if effective.Fanout == driver.FanoutAtPublish {
				addExchange(driver.ExchangeSpec{Name: entryPoint, Kind: "fanout", Durable: true})
				result.Bindings = append(result.Bindings, driver.BindingSpec{Source: entryPoint, Destination: main})
			}
			for tier := 1; tier <= retryTiers(sub.Retry); tier++ {
				add(driver.DestinationSpec{Name: retryDestinationFor(source, logical, priority, tier, sub.Name), Kind: driver.DestRetry, Durable: true, Delay: sub.Retry.DelayFor(tier), DeadLetter: route, DeliveryLimit: limit})
			}
		}
		add(driver.DestinationSpec{Name: deadLetterDestinationFor(source, logical, sub.Name), Kind: driver.DestDLQ, Durable: true})
		if effective.NativeDLQ {
			add(driver.DestinationSpec{Name: deadLetterDestinationFor(source, logical, sub.Name) + ".backstop", Kind: driver.DestBackstopDLQ, Durable: true})
		}
	}
	// Decode failures may not yield an event type, so keep a stable DLQ for
	// malformed messages whose topic cannot be recovered from the envelope.
	add(driver.DestinationSpec{Name: deadLetterDestinationFor(source, "unknown", sub.Name), Kind: driver.DestDLQ, Durable: true})
	sort.Slice(result.Destinations, func(i, j int) bool { return result.Destinations[i].Name < result.Destinations[j].Name })
	sort.Slice(result.Exchanges, func(i, j int) bool { return result.Exchanges[i].Name < result.Exchanges[j].Name })
	sort.Slice(result.Bindings, func(i, j int) bool {
		if result.Bindings[i].Source == result.Bindings[j].Source {
			return result.Bindings[i].Destination < result.Bindings[j].Destination
		}
		return result.Bindings[i].Source < result.Bindings[j].Source
	})
	return result
}

func deadLetterDestination(r *Runner, envelope Envelope, message driver.InboundMessage) string {
	_ = message
	topic := topicFor(envelope.Type)
	if topic == "" {
		topic = "unknown"
	}
	return deadLetterDestinationFor(r.client.source, topic, r.subscription.Name)
}

func retryDestination(r *Runner, envelope Envelope, tier int) string {
	return retryDestinationFor(r.client.source, topicFor(envelope.Type), envelope.Priority, tier, r.subscription.Name)
}

func deadLetterDestinationFor(source, topic, subscription string) string {
	return fmt.Sprintf("f1.%s.%s.dlq.%s", sourceEnvironment(source), topic, subscription)
}

func retryDestinationFor(source, topic string, priority Priority, tier int, subscription string) string {
	return fmt.Sprintf("f1.%s.%s.%s.%s.retry.%d", sourceEnvironment(source), topic, subscription, priority, tier)
}

func consumeDestination(effective driver.Capabilities, source, topic string, priority Priority, subscription string) string {
	if effective.Fanout == driver.FanoutAtPublish {
		return fmt.Sprintf("f1.%s.%s.%s.%s", sourceEnvironment(source), topic, subscription, priority)
	}
	return publishEntryPoint(source, topic, priority)
}
