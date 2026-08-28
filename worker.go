package f1

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/dispatch"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/retry"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/sched"
)

const (
	defaultHandlerTimeout         = 30 * time.Second
	defaultRetryWeightDivisor     = 2
	defaultPrefetchFactor         = 2
	retryBudgetMultiplier         = 2
	minimumLaneCapacity           = 2
	stuckPhaseMultiplier          = 2
	stuckAbortMultiplier          = 4
	deathErrorCap                 = 4 << 10
	topologyDeliveryLimitHeadroom = 5
	maxLoggedDeathDetailKeys      = 8
)

// --- Types and settlement contracts ---

type delivery struct {
	id      uint64
	message driver.InboundMessage
}

// deliveryState records attempted versus settled ownership. operation and
// nackOptions preserve the exact settlement operation for deferred retries.
type deliveryState struct {
	id        uint64
	attempted bool
	settled   bool
	// operation records which settlement call was last made, so that a failed
	// one can be retried in kind rather than guessed at.
	operation   settlementOperation
	nackOptions driver.NackOptions
	poisonDrop  *poisonDropReport
}

type poisonDropReport struct {
	envelope Envelope
	cause    error
}

// --- Public runner API and generation lifecycle ---

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
	if r.client.closed || r.client.shutdownStarted || r.client.conn == nil {
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
	r.drainStarted = make(chan struct{})
	r.inflight = newInflightRegistry()
	r.lifecycle = lifecycle.New()
	r.accounting = lifecycle.NewAccounting(r.inflight.registry)
	handlerCtx, handlerCancel := context.WithCancel(ctx)
	r.handlerCtx, r.handlerCancel = handlerCtx, handlerCancel
	handlerShutdownCtx, handlerShutdownCancel := context.WithCancel(context.Background())
	r.handlerShutdownCtx, r.handlerShutdownCancel = handlerShutdownCtx, handlerShutdownCancel
	r.asyncGroup = new(errgroup.Group)
	r.errorGroup = newErrorHandlerGroup(r.subscription.Concurrency)
	r.mu.Unlock()
	r.client.mu.Unlock()

	defer finishRunner(r)
	var runErr error
	var runCtx context.Context
	var cancel context.CancelFunc
	generation := 0
	var repairCause error
	for {
		r.mu.Lock()
		failed := r.lifecycle != nil && r.lifecycle.State() == lifecycle.Failed
		r.mu.Unlock()
		if failed {
			break
		}
		runCtx, cancel = context.WithCancel(ctx)
		group := beginRunnerGeneration(r, runCtx, cancel)

		consumer, err := openRunnerConsumer(r, runCtx)
		if err != nil {
			repairCause = nil
			if generation == 0 {
				return err
			}
			kind, classified := driver.Classify(err)
			if classified && kind != driver.KindTransient {
				runErr = err
				break
			}
			r.mu.Lock()
			r.reconnectCause = err
			r.mu.Unlock()
			r.transitionToReconnecting()
			if reconnectErr := r.requestAndWaitReconnect(ctx, err); reconnectErr != nil {
				runErr = reconnectErr
				break
			}
			continue
		}
		generation++
		if repairCause != nil {
			lastResortRunnerLogger(r).Info("f1 consumer repaired", "subscription", r.subscription.Name, "cause", repairCause)
			repairCause = nil
		}
		r.mu.Lock()
		r.consumer = consumer
		r.mu.Unlock()

		switch r.lifecycle.State() {
		case lifecycle.Starting, lifecycle.Reconnecting:
			r.mu.Lock()
			r.repairCycleActive = false
			r.failedRepairCycles = 0
			r.mu.Unlock()
			if err := r.lifecycle.Transition(lifecycle.Ready); err != nil {
				runErr = err
			}
		case lifecycle.Ready:
		default:
			runErr = fmt.Errorf("f1: runner cannot start from lifecycle state %s", r.lifecycle.State())
		}
		if runErr != nil {
			break
		}

		deliveries := make(chan delivery, r.subscription.Concurrency)
		group.Go(func() error { return fetchRunner(r, runCtx, deliveries) })
		group.Go(func() error { return runDispatchPipeline(r, runCtx, deliveries) })
		group.Go(func() error { return consumeRunnerErrors(r, runCtx) })
		generationErr := group.Wait()
		if generationErr == nil {
			generationErr = runnerError(r)
		}
		// After group.Wait, this snapshot is the only reader window for the
		// generation result. The later repair write-back remains a separate lock.
		r.mu.Lock()
		reconnectCause := r.reconnectCause
		consumerError := r.consumerError
		successfulDelivery := r.successfulDelivery
		repairCycleActive := r.repairCycleActive
		failedRepairCycles := r.failedRepairCycles
		draining := r.draining
		failedLifecycle := r.lifecycle != nil && r.lifecycle.State() == lifecycle.Failed
		r.mu.Unlock()
		if failedLifecycle {
			runErr = generationErr
			break
		}
		clientReconnecting := r.client.isReconnecting()
		if ctx.Err() != nil || draining || (!clientReconnecting && reconnectCause == nil) {
			runErr = generationErr
			break
		}
		if consumerError && !clientReconnecting {
			if successfulDelivery {
				failedRepairCycles = 0
				repairCycleActive = false
			}
			if repairCycleActive {
				failedRepairCycles++
			}
			repairCycleActive = true
			r.mu.Lock()
			r.repairCycleActive = repairCycleActive
			r.failedRepairCycles = failedRepairCycles
			r.mu.Unlock()
			if failedRepairCycles < 2 {
				if releaseErr := releaseRunnerConsumer(r, context.WithoutCancel(runCtx)); releaseErr == nil {
					repairCause = reconnectCause
					continue
				} else {
					reconnectCause = errors.Join(reconnectCause, releaseErr)
				}
			}
		}
		if reconnectCause == nil {
			reconnectCause = generationErr
		}
		if reconnectCause == nil {
			reconnectCause = errClientReconnecting
		}
		r.transitionToReconnecting()
		if reconnectErr := r.requestAndWaitReconnect(ctx, reconnectCause); reconnectErr != nil {
			runErr = reconnectErr
			break
		}
	}

	shutdownErr := r.drainAfterRun(runCtx)
	if runErr == nil {
		runErr = shutdownErr
	} else if shutdownErr != nil {
		runErr = errors.Join(runErr, shutdownErr)
	}
	return runErr
}

func (r *Runner) requestAndWaitReconnect(ctx context.Context, cause error) error {
	attempt, err := r.client.requestReconnect(cause)
	if err != nil {
		return err
	}
	return r.client.waitReconnect(ctx, attempt)
}

// beginRunnerGeneration resets the per-generation runner state at the top of
// Run's loop, where the generation's run context is created. The settlement
// context belongs to this reset: first-wins installation is correct within
// one drain, but a context installed during a previous generation's
// abandonment carries a drain budget that started back then, so every later
// generation would inherit an already-expired budget and fail every drain
// settlement at once. The field is cleared without calling the old cancel: a
// cleanup from the previous generation captured the context value, not the
// field, and cancelling it under such a caller is the defect first-wins
// exists to prevent. The dropped cancel function is safe to lose because the
// context was built over an uncancelable base: with a nil Done channel there
// is nothing for cancellation to propagate through, no goroutine watching
// the parent, and the context becomes unreachable once no caller references
// it. When DrainTimeout is positive the context also carries its own
// deadline, so its timer fires within one budget period and releases
// whatever the deadline still holds; that timer is an additional guarantee,
// not the protection, and it does not exist when the budget is zero.
func beginRunnerGeneration(r *Runner, runCtx context.Context, cancel context.CancelFunc) *errgroup.Group {
	r.mu.Lock()
	r.runCtx = runCtx
	r.cancel = cancel
	r.settleCtx = nil
	r.settleCancel = nil
	r.reconnectCause = nil
	r.consumerError = false
	r.successfulDelivery = false
	group := new(errgroup.Group)
	r.group = group
	r.mu.Unlock()
	return group
}

// --- Dispatch and scheduling ---

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

// newRunnerScheduler converts fairness weights into lane capacity and applies
// the retry divisor and prefetch factor without changing the scheduling graph.
func newRunnerScheduler(r *Runner) (*sched.Scheduler, error) {
	weights := r.subscription.Fairness.Weights
	budgets := r.subscription.Fairness.Budgets
	divisor := r.subscription.Fairness.RetryWeightDivisor
	if divisor < 1 {
		divisor = defaultRetryWeightDivisor
	}
	factor := r.subscription.Fairness.PrefetchFactor
	if factor < 1 {
		factor = defaultPrefetchFactor
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
			weight := max(weights[priority], 1)
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
					laneBudget *= retryBudgetMultiplier
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
		capacity := max((r.subscription.Concurrency*lane.weight+totalWeight-1)/totalWeight, minimumLaneCapacity)
		capacity *= factor
		specs = append(specs, sched.LaneSpec{ID: id, Group: lane.group, Weight: lane.weight, Budget: lane.budget, Capacity: capacity})
	}
	return sched.New(specs, r.client.options.clock, r.subscription.Fairness.AgingEnabled)
}

func schedulerLaneID(topic string, priority Priority, tier int) string {
	if tier > 0 {
		return fmt.Sprintf("%s.%s.%s.%d", topic, priority, retryDestinationSegment, tier)
	}
	return fmt.Sprintf("%s.%s.main", topic, priority)
}

func schedulerRetryGroupID(topic string, priority Priority) string {
	return fmt.Sprintf("%s.%s.%s", topic, priority, retryDestinationSegment)
}

func deliveryLane(r *Runner, message driver.InboundMessage) string {
	envelope, err := DecodeHeaders(inboundHeaders(message.Headers))
	if err == nil {
		r.mu.Lock()
		tier := r.retryDestinationTiers[message.Destination]
		r.mu.Unlock()
		topic := topicFor(envelope.Type)
		r.client.mu.Lock()
		effective := r.client.effective
		source := r.client.source
		r.client.mu.Unlock()
		for _, configured := range r.subscription.Topics {
			logical := topicFor(configured)
			if tier == 0 {
				if consumeDestination(effective, source, logical, envelope.Priority, r.subscription.Name) == message.Destination {
					topic = logical
					break
				}
				continue
			}
			if retryDestinationFor(source, logical, envelope.Priority, tier, r.subscription.Name) == message.Destination {
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

// consumingTopicFamily resolves the logical topic whose declared destination
// family the delivery came from. Subscription topology is declared from
// Subscription.Topics, so a successor must stay inside the family the
// message was consumed from: WithTopic and fan-out make the envelope's event
// type an unreliable guide to that family. Matching reuses the exact naming
// helpers that declare topology, so destination strings are never parsed and
// nothing is derived from the event type here. The bool is false when no
// configured family owns the destination, letting callers keep their
// historical derivation for deliveries outside every declared family.
func consumingTopicFamily(r *Runner, priority Priority, destination string) (string, bool) {
	r.client.mu.Lock()
	effective := r.client.effective
	source := r.client.source
	r.client.mu.Unlock()
	for _, configured := range r.subscription.Topics {
		logical := topicFor(configured)
		if consumeDestination(effective, source, logical, priority, r.subscription.Name) == destination {
			return logical, true
		}
		for tier := 1; tier <= retryTiers(r.subscription.Retry); tier++ {
			if retryDestinationFor(source, logical, priority, tier, r.subscription.Name) == destination {
				return logical, true
			}
		}
	}
	return "", false
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
		case lifecycle.Reconnecting:
			if err := r.lifecycle.Transition(lifecycle.Draining); err != nil {
				r.mu.Unlock()
				return err
			}
		case lifecycle.Starting:
		case lifecycle.Failed:
			r.mu.Unlock()
			select {
			case <-done:
				return runnerError(r)
			case <-ctx.Done():
				return ctx.Err()
			}
		case lifecycle.Draining, lifecycle.Settling, lifecycle.Flushing:
		case lifecycle.Closed:
			r.mu.Unlock()
			return nil
		default:
			r.mu.Unlock()
			return fmt.Errorf("f1: runner cannot drain from lifecycle state %s", r.lifecycle.State())
		}
	}
	if !r.draining {
		r.draining = true
		if r.drainStarted != nil {
			close(r.drainStarted)
		}
	}
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if handlerCancel != nil {
		drainTimeout := r.client.config.Lifecycle.DrainTimeout
		if drainTimeout < 0 {
			return fmt.Errorf("f1: lifecycle.drainTimeout must not be negative")
		}
		if drainTimeout > 0 {
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
							graceTimer := r.client.options.clock.Timer(grace)
							defer graceTimer.Stop()
							select {
							case <-graceTimer.C:
								handlerShutdownCancel()
							case <-done:
							}
						}
					case <-done:
					}
					return nil
				})
			}
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
	// runner outlive its shutdown bound. Terminal callbacks receive a bounded
	// context and are allowed to finish independently after settlement and after
	// ownership of the runner has ended.
	if r.done != nil {
		close(r.done)
	}
	r.mu.Unlock()
	if r.client != nil {
		r.client.mu.Lock()
		delete(r.client.runners, r)
		r.client.mu.Unlock()
	}
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

//nolint:contextcheck // helper derives the phase context from the caller.
func runWithClockTimeout(parent context.Context, clk clock.Clock, timeout time.Duration, phase string, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}
	if parent == nil {
		parent = context.Background()
	}
	if timeout < 0 {
		return fmt.Errorf("f1: shutdown %s phase timeout must not be negative", phase)
	}
	if timeout == 0 {
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
		return fmt.Errorf("f1: shutdown %s phase timed out: %w", phase, context.DeadlineExceeded)
	case <-parent.Done():
		return parent.Err()
	}
}

// startPhase launches fn on ctx and returns a channel that receives its
// single result. It exists so a shutdown phase that must not be abandoned on
// timeout (see joinPhase) can be started once and rejoined across repeated
// calls, instead of being started again on every retry.
//
//nolint:contextcheck // helper derives the phase context from the caller.
func startPhase(ctx context.Context, fn func(context.Context) error) <-chan error {
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan error, 1)
	go func() { done <- fn(ctx) }()
	return done
}

// joinPhase waits for a phase started by startPhase to report its result on
// done, bounded by timeout on clk and by parent. resolved is false when the
// wait ended by timeout or by parent's cancellation rather than the phase
// itself finishing; done is still live in that case, and the caller must
// keep it and rejoin later rather than starting the phase again.
//
//nolint:contextcheck // helper derives the phase context from the caller.
func joinPhase(parent context.Context, clk clock.Clock, timeout time.Duration, phase string, done <-chan error) (err error, resolved bool) {
	if parent == nil {
		parent = context.Background()
	}
	if timeout < 0 {
		return fmt.Errorf("f1: shutdown %s phase timeout must not be negative", phase), false
	}
	if timeout == 0 {
		select {
		case err = <-done:
			return err, true
		case <-parent.Done():
			return parent.Err(), false
		}
	}
	if clk == nil {
		clk = clock.NewReal()
	}
	timer := clk.Timer(timeout)
	defer timer.Stop()
	select {
	case err = <-done:
		return err, true
	case <-timer.C:
		return fmt.Errorf("f1: shutdown %s phase timed out: %w", phase, context.DeadlineExceeded), false
	case <-parent.Done():
		return parent.Err(), false
	}
}

func contextWithOptionalTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout == 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}

func runnerSettlementContext(r *Runner, fallback context.Context) context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.settleCtx != nil {
		return r.settleCtx
	}
	if fallback != nil && fallback.Err() != nil {
		drainTimeout := r.client.config.Lifecycle.DrainTimeout
		base := context.WithoutCancel(fallback)
		r.settleCtx, r.settleCancel = contextWithOptionalTimeout(base, drainTimeout)
		return r.settleCtx
	}
	return fallback
}

func configuredRunnerLogger(r *Runner) *slog.Logger {
	if r == nil || r.client == nil {
		return nil
	}
	return configuredClientLogger(r.client)
}

func lastResortRunnerLogger(r *Runner) *slog.Logger {
	if logger := configuredRunnerLogger(r); logger != nil {
		return logger
	}
	return slog.Default()
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
	retryDestinationTiers := retryDestinationTierMap(source, r.subscription)
	r.mu.Lock()
	r.retryDestinationTiers = retryDestinationTiers
	r.mu.Unlock()
	if policy != driver.TopologyNone {
		admin := conn.Admin()
		if admin == nil {
			return nil, errors.New("f1: consumer topology requires driver admin")
		}
		topology := subscriptionTopologySpecs(effective, source, r.subscription)
		topology.Policy = policy
		diff, err := admin.EnsureTopology(ctx, topology)
		if err != nil {
			return nil, fmt.Errorf("f1: ensure subscription topology: %w", err)
		}
		logTopologyDrift(lastResortRunnerLogger(r), diff)
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
		Exclusive:      r.subscription.Mode == OrderedByKey,
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
			select {
			case err, ok := <-consumer.Errors():
				if ok && err != nil {
					kind, classified := driver.Classify(err)
					if classified && kind == driver.KindFatal {
						recordFatalConsumerError(r, err)
						runnerNotifyError(r, context.WithoutCancel(ctx), nil, err)
					}
				}
			default:
			}
			return nil
		case err, ok := <-consumer.Errors():
			if !ok {
				return nil
			}
			if err == nil {
				continue
			}
			if r.client.options.errorHandler == nil {
				lastResortRunnerLogger(r).Error("f1 consumer error", "subscription", r.subscription.Name, "error", err)
			}
			kind, classified := driver.Classify(err)
			var cancel context.CancelFunc
			if !classified || kind == driver.KindTransient {
				r.mu.Lock()
				if kind == driver.KindTransient {
					r.consumerError = true
				}
				if r.reconnectCause == nil {
					r.reconnectCause = err
				}
				cancel = r.cancel
				r.mu.Unlock()
			} else {
				setRunnerError(r, err)
				if kind == driver.KindFatal {
					recordFatalConsumerError(r, err)
					r.mu.Lock()
					cancel = r.cancel
					r.mu.Unlock()
				}
			}
			// r.cancel below is about to cancel ctx, so the notification gets its
			// own detached context: the handler's terminalNotificationTimeout
			// budget must not collapse to zero just because the same error that
			// is being reported is also what triggers shutdown.
			runnerNotifyError(r, context.WithoutCancel(ctx), nil, err)
			if cancel != nil {
				cancel()
			}
			return nil
		}
	}
}

func recordFatalConsumerError(r *Runner, err error) {
	setRunnerError(r, err)
	if r.lifecycle != nil {
		if transitionErr := r.lifecycle.Transition(lifecycle.Failed); transitionErr != nil {
			setRunnerError(r, errors.Join(err, transitionErr))
		}
	}
	r.client.recordFailedRunner(r, err)
}

// --- Intake and cancellation ---

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
				// The generation barrier owns cancellation; fetchRunner reads the
				// generation's r.cancel instead of closing it from the consumer path.
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
	if drainTimeout < 0 {
		return fmt.Errorf("f1: lifecycle.drainTimeout must not be negative")
	}
	drainBase := context.WithoutCancel(parent)
	drainCtx, cancel := contextWithOptionalTimeout(drainBase, drainTimeout)
	defer cancel()
	r.mu.Lock()
	// First installation wins. A settlement context installed earlier was
	// built from an uncancelable base with its own drain budget, so it is
	// already fit for use; cancelling and replacing it would pull the ground
	// out from under a settlement call that captured the old context and is
	// about to reach the driver on it.
	if r.settleCtx == nil {
		r.settleCtx, r.settleCancel = contextWithOptionalTimeout(drainBase, drainTimeout)
	}
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

// --- Handler invocation and delivery processing ---

func processDelivery(r *Runner, ctx context.Context, item delivery) {
	abandoned := false
	state := &deliveryState{id: item.id}
	var envelope Envelope
	defer func() {
		if recovered := recover(); recovered != nil {
			err := fmt.Errorf("handler panic: %v\n%s", recovered, debug.Stack())
			deadLetterAndSettle(r, ctx, item.message, envelope, ReasonPanic, err, state)
		}
		if !state.settled && !state.attempted && abandoned {
			_ = nackDelivery(r, runnerSettlementContext(r, ctx), item.message, driver.NackOptions{Requeue: true}, state)
		}
		retryDeliverySettlement(r, ctx, item.message, state)
		// The outcome names the delivery's terminal result: a delivery whose
		// delayed cleanup eventually settled reports that settlement, and
		// abandoned is reserved for one that never settled at all.
		operation := state.operation
		outcome := settlementOutcomeUnknown
		switch {
		case state.settled && operation == settlementOperationNack:
			outcome = settlementOutcomeRequeued
		case state.settled:
			outcome = settlementOutcomeSettled
		case abandoned:
			outcome = settlementOutcomeAbandoned
		}
		r.inflight.RemoveAs(item.id, outcome)
	}()
	dispatchMessage(r, ctx, item.message, &envelope, &abandoned, state)
}

// settlementRetryAttempts bounds how many rounds of retry the deferred
// cleanup of a delivery gets on top of its inline attempts. It stays small
// on purpose: a drain must stay bounded even against settlement calls that
// keep failing transiently.
const settlementRetryAttempts = 3

// settlementRetryBackoff bounds the wait between deferred settlement retry
// rounds. Like every wait in this path it is short and fixed: a delivery
// held unsettled may only ever be delayed by something bounded and small.
const settlementRetryBackoff = 20 * time.Millisecond

// retryDeliverySettlement finishes the settlement of a delivery whose
// cleanup attempts failed. The driver retains an unsettled delivery on a
// failed settlement call, so the runner owes more attempts while any of its
// bounded settlement budget remains: giving up after one retry would let the
// registry report zero for work the driver still owns. Each round retries
// the last operation in kind; a failed ack falls back to a requeue nack once
// per round. The loop ends when the delivery settles, when the attempt bound
// is reached, or when the settlement context ends, whichever comes first.
func retryDeliverySettlement(r *Runner, ctx context.Context, message driver.InboundMessage, state *deliveryState) {
	for round := 0; !state.settled && round < settlementRetryAttempts; round++ {
		sctx := runnerSettlementContext(r, ctx)
		switch state.operation {
		case settlementOperationAck:
			ackDelivery(r, sctx, message, state)
			if state.settled {
				reportPendingPoisonDrop(r, sctx, message, state)
				return
			}
			_ = nackDelivery(r, sctx, message, driver.NackOptions{Requeue: true}, state)
			if state.settled {
				state.poisonDrop = nil
			}
		case settlementOperationNack:
			_ = nackDelivery(r, sctx, message, state.nackOptions, state)
			if state.settled {
				state.poisonDrop = nil
			}
		default:
			return
		}
		if state.settled {
			return
		}
		if err := r.client.options.clock.Sleep(sctx, settlementRetryBackoff); err != nil {
			return
		}
	}
}

// dispatchMessage returns true only after the delivery's settlement path has
// completed; classification and settlement stay outside middleware.
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
	eventCodec, err := r.client.codecForContentType(envelope.DataContentType)
	if err != nil {
		return deadLetterAndSettle(r, ctx, message, envelope, ReasonDecode, err, state)
	}
	*envelopeOut = envelope
	maxAttempts := envelope.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = r.subscription.Retry.MaxAttempts
	}
	if retry.CounterRunaway(envelope.Attempt, maxAttempts) {
		return deadLetterAndSettle(r, ctx, message, envelope, ReasonPoison, errors.New("retry counter exceeded sanity margin"), state)
	}
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
		runnerNotifyDiscarded(r, runnerSettlementContext(r, ctx), discardUnmatched(envelope, append([]byte(nil), message.Body...)))
		return ackDelivery(r, runnerSettlementContext(r, ctx), message, state)
	}
	event := &Event{envelope: envelope, raw: append([]byte(nil), message.Body...), codec: eventCodec, headers: headers}
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
		runnerNotifyDiscarded(r, runnerSettlementContext(r, ctx), Discarded{Envelope: envelope, Body: append([]byte(nil), message.Body...), Reason: DiscardDropped, Err: result.err})
		return ackDelivery(r, runnerSettlementContext(r, ctx), message, state)
	case retry.Terminal:
		return deadLetterAndSettle(r, ctx, message, envelope, ReasonTerminal, result.err, state)
	}
	if envelope.Attempt >= maxAttempts {
		return deadLetterAndSettle(r, ctx, message, envelope, ReasonMaxAttempts, result.err, state)
	}
	return retryAndSettle(r, ctx, message, envelope, result.err, state)
}

func classifyRetryError(err error) retry.Outcome {
	switch {
	case IsTerminal(err):
		return retry.Outcome{Kind: retry.Terminal, Err: err}
	case IsDropped(err):
		return retry.Outcome{Kind: retry.Drop, Err: err}
	}
	if delay, ok := RetryDelay(err); ok {
		if delay < 0 {
			delay = 0
		}
		return retry.Outcome{Kind: retry.RetryAfter, Delay: delay, Err: err}
	}
	return retry.Classify(err)
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
		timeout = defaultHandlerTimeout
	}
	r.mu.Lock()
	base := r.handlerCtx //nolint:contextcheck // handlerCtx is derived from the Run context and survives the drain grace window.
	shutdownCtx := r.handlerShutdownCtx
	draining := r.draining
	drainStarted := r.drainStarted
	if base == nil {
		base = parent
	}
	r.mu.Unlock()
	handlerCtx, cancel := context.WithTimeout(base, timeout)
	defer cancel()
	done := make(chan handlerResult, 1)
	// Normal runs attach handlers to asyncGroup; direct tests and pre-Run
	// dispatch use the generation group, then a local fallback if neither exists.
	r.mu.Lock()
	group := r.asyncGroup
	if group == nil {
		group = r.group
	}
	if group == nil {
		group = new(errgroup.Group)
	}
	r.mu.Unlock()
	group.Go(func() error {
		result := handlerResult{}
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					result.panic = fmt.Errorf("handler panic: %v\n%s", recovered, debug.Stack())
				}
			}()
			result.err = handler.Handle(handlerCtx, event)
		}()
		if panicErr, ok := errors.AsType[*handlerPanicError](result.err); ok {
			result.panic = panicErr
			result.err = nil
		}
		done <- result
		return nil
	})
	stuck := r.client.options.clock.Timer(timeout * stuckPhaseMultiplier)
	defer stuck.Stop()
	parentDone := parent.Done()
	if draining {
		parentDone = nil
		drainStarted = nil
	}
	var shutdownDone <-chan struct{}
	if shutdownCtx != nil {
		shutdownDone = shutdownCtx.Done()
	}
	// The first explicit 2x phase warns; only after it completes does the
	// second explicit 2x phase begin, making the terminal threshold cumulative 4x.
firstPhase:
	for {
		select {
		case result := <-done:
			return result
		case <-drainStarted:
			parentDone = nil
			drainStarted = nil
		case <-parentDone:
			if runnerIsDraining(r) {
				parentDone = nil
				continue
			}
			return handlerResult{stuck: true}
		case <-shutdownDone:
			return handlerResult{stuck: true}
		case <-stuck.C:
			lastResortRunnerLogger(r).Warn("f1 stuck worker", "subscription", r.subscription.Name, "threshold", stuckPhaseMultiplier*timeout)
			break firstPhase
		}
	}
	stackTimer := r.client.options.clock.Timer(timeout * stuckPhaseMultiplier)
	defer stackTimer.Stop()
	// Keep result, parent cancellation, shutdown cancellation, and timer order
	// explicit: this is the second sequential stuck phase.
	for {
		select {
		case result := <-done:
			return result
		case <-drainStarted:
			parentDone = nil
			drainStarted = nil
		case <-parentDone:
			if runnerIsDraining(r) {
				parentDone = nil
				continue
			}
			return handlerResult{stuck: true}
		case <-shutdownDone:
			return handlerResult{stuck: true}
		case <-stackTimer.C:
			lastResortRunnerLogger(r).Error("f1 worker exceeded stuck threshold", "subscription", r.subscription.Name, "threshold", stuckAbortMultiplier*timeout)
			return handlerResult{stuck: true}
		}
	}
}

func runnerIsDraining(r *Runner) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.draining
}

// --- Settlement primitives ---

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
	if r != nil && state.settled && disposition == lifecycle.Handled {
		r.mu.Lock()
		r.successfulDelivery = true
		r.mu.Unlock()
	}
	return state.settled
}

func nackDelivery(r *Runner, ctx context.Context, message driver.InboundMessage, options driver.NackOptions, states ...*deliveryState) error {
	state := stateFor(states)
	if message.Settle == nil {
		return errors.New("f1: delivered message has no settler")
	}
	state.operation = settlementOperationNack
	state.nackOptions = options
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
		if reason == ReasonPoison && isMissingDeadLetterRoute(err) {
			state.poisonDrop = &poisonDropReport{envelope: envelope, cause: err}
			sctx := runnerSettlementContext(r, ctx)
			settled := ackDeliveryAs(r, sctx, message, lifecycle.Handled, state)
			if settled {
				reportPendingPoisonDrop(r, sctx, message, state)
			}
			return settled
		}
		failSuccessorHandoff(r, runnerSettlementContext(r, ctx), "dead_letter", eventFromDelivery(r, message, envelope), err)
		return false
	}
	return ackDeliveryAs(r, runnerSettlementContext(r, ctx), message, lifecycle.DeadLettered, state)
}

func isMissingDeadLetterRoute(err error) bool {
	if errors.Is(err, driver.ErrDestinationMissing) {
		return true
	}
	kind, classified := driver.Classify(err)
	return classified && kind == driver.KindNotFound
}

func reportPoisonDrop(r *Runner, ctx context.Context, message driver.InboundMessage, envelope Envelope, cause error) {
	dropErr := fmt.Errorf("f1: poison message dropped: event_id=%q destination=%q attempt=%d: no dead-letter route available: %w", envelope.ID, message.Destination, envelope.Attempt, cause)
	if r.client.options.errorHandler == nil {
		lastResortRunnerLogger(r).Error("f1 poison message dropped; no dead-letter route",
			"event_id", envelope.ID,
			"destination", message.Destination,
			"attempt", envelope.Attempt,
			"reason", ReasonPoison,
			"error", cause,
		)
		return
	}
	runnerNotifyError(r, ctx, eventFromDelivery(r, message, envelope), dropErr)
}

func reportPendingPoisonDrop(r *Runner, ctx context.Context, message driver.InboundMessage, state *deliveryState) {
	if state == nil || state.poisonDrop == nil {
		return
	}
	report := state.poisonDrop
	state.poisonDrop = nil
	reportPoisonDrop(r, ctx, message, report.envelope, report.cause)
}

// deadLetter republishes message to its dead-letter destination, carrying
// the already-received body forward unchanged. It deliberately does not
// apply codec.maxBodyBytes: that limit is a caller-facing guardrail
// enforced only on a publish the application originated (publisher.go). A
// body reaching this function was already accepted onto the broker once;
// rejecting the same bytes here on their way to the dead-letter destination
// would turn a successful delivery into a silent message-loss path instead
// of the visible one dead-lettering exists to provide.
func deadLetter(r *Runner, ctx context.Context, message driver.InboundMessage, envelope Envelope, reason DeathReason, lastErr error) error {
	if reason == ReasonDecode && envelope.ID == "" {
		// Preserve the raw headers when the envelope itself could not be decoded.
		headers := inboundHeaders(message.Headers)
		for key := range headers {
			if strings.HasPrefix(key, "f1detail") {
				delete(headers, key)
			}
		}
		destination := deadLetterDestination(r, envelope, message)
		setDeathHeaders(headers, reason, lastErr, r.client.options.clock.Now().UTC(), message.Destination)
		if err := publishSuccessor(r, runnerSettlementContext(r, ctx), driver.OutboundMessage{Destination: destination, Key: append([]byte(nil), message.Key...), Headers: headerSlice(headers), Body: append([]byte(nil), message.Body...)}); err != nil {
			return err
		}
		runnerNotifyDeadLetter(r, runnerSettlementContext(r, ctx), DeadLettered{Envelope: Envelope{}, Body: append([]byte(nil), message.Body...), Reason: reason, Attempt: 0, LastErr: lastErr, Destination: destination})
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
	if reason == ReasonTerminal || reason == ReasonMaxAttempts {
		details, discarded := collectDeathDetails(lastErr)
		death.DeathDetails = details
		if len(discarded) > 0 {
			sort.Strings(discarded)
			logged := discarded
			if len(logged) > maxLoggedDeathDetailKeys {
				logged = logged[:maxLoggedDeathDetailKeys]
			}
			lastResortRunnerLogger(r).Warn("f1 death details discarded",
				"subscription", r.subscription.Name,
				"event_id", envelope.ID,
				"keys", logged,
				"count", len(discarded),
			)
		}
	} else {
		death.DeathDetails = nil
	}
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
	if err := publishSuccessor(r, runnerSettlementContext(r, ctx), out); err != nil {
		return err
	}
	runnerNotifyDeadLetter(r, runnerSettlementContext(r, ctx), DeadLettered{Envelope: death, Body: append([]byte(nil), message.Body...), Reason: reason, Attempt: death.Attempt, LastErr: lastErr, Destination: destination})
	return nil
}

// --- Successor publishing ---

// maxSuccessorPublishAttempts bounds how many times the SDK retries
// publishing a retry or dead-letter successor before treating the
// settlement budget as exhausted. It stays small on purpose: the whole
// chain - successor publish, then original settlement - must complete
// inside the broker's consumer-liveness window, and every extra attempt
// spends part of that window.
const maxSuccessorPublishAttempts = 3

// successorPublishBackoff bounds the wait between successor publish
// retries. It is short and fixed: a delivery held unsettled may only ever
// wait for something bounded and short, never open-ended.
const successorPublishBackoff = 100 * time.Millisecond

// publishSuccessor publishes a retry or dead-letter copy, retrying up to
// maxSuccessorPublishAttempts times with a short fixed backoff between
// attempts. It stops as soon as ctx ends, so it never outlives the caller's
// remaining settlement budget.
func publishSuccessor(r *Runner, ctx context.Context, messages ...driver.OutboundMessage) error {
	var lastErr error
	for attempt := 1; attempt <= maxSuccessorPublishAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr == nil {
				lastErr = err
			}
			break
		}
		err := publishMessages(r.client, ctx, true, messages...)
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt == maxSuccessorPublishAttempts {
			break
		}
		if sleepErr := r.client.options.clock.Sleep(ctx, successorPublishBackoff); sleepErr != nil {
			break
		}
	}
	return lastErr
}

// eventFromDelivery builds the read-only Event view of an inbound delivery,
// for handing to code outside the normal handler dispatch path (such as the
// error handler) that still needs to identify which message an async
// failure is about.
func eventFromDelivery(r *Runner, message driver.InboundMessage, envelope Envelope) *Event {
	eventCodec, _ := r.client.codecForContentType(envelope.DataContentType)
	return &Event{envelope: envelope, raw: append([]byte(nil), message.Body...), codec: eventCodec, headers: inboundHeaders(message.Headers)}
}

// failSuccessorHandoff runs once a retry or dead-letter successor could not
// be published within its bounded republish budget. The consume-side
// ordering invariant only allows releasing the original delivery once every
// possible successor is confirmed durable, so this never settles the
// delivery: it releases the runner's consumer instead. Release leaves the
// delivery unsettled, closes the consumer, and lets the broker redeliver the
// message, the same mechanism relied on for a crash.
func failSuccessorHandoff(r *Runner, ctx context.Context, op string, event *Event, cause error) {
	kind, _ := driver.Classify(cause)
	classified := &driver.Error{Driver: r.client.options.driver.Name(), Op: op, K: kind, Err: cause}
	setRunnerError(r, classified)
	runnerNotifyError(r, ctx, event, classified)
	if err := releaseRunnerConsumer(r, ctx); err != nil {
		if r.client.options.errorHandler == nil {
			lastResortRunnerLogger(r).Error("f1 failed to release consumer after a successor publish exhausted its republish budget", "op", op, "cause", classified, "release_error", err)
		}
		return
	}
	if r.client.options.errorHandler == nil {
		lastResortRunnerLogger(r).Error("f1 successor publish exhausted its republish budget; consumer released", "op", op, "cause", classified)
	}
}

// retryAndSettle republishes message to its retry destination, carrying the
// already-received body forward unchanged. Like deadLetter, it deliberately
// does not apply codec.maxBodyBytes: that limit only guards a publish the
// application originated, and this body was already accepted once.
func retryAndSettle(r *Runner, ctx context.Context, message driver.InboundMessage, envelope Envelope, lastErr error, states ...*deliveryState) bool {
	state := stateFor(states)
	copyEnvelope := envelope
	if copyEnvelope.OriginalDest == "" {
		copyEnvelope.OriginalDest = message.Destination
	}
	copyEnvelope.DeathError = ""
	copyEnvelope.DeathReason = ReasonUnspecified
	copyEnvelope.DeathTime = nil
	copyEnvelope.DeathDetails = nil
	copyEnvelope.Attempt++
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
		_ = nackDelivery(r, runnerSettlementContext(r, ctx), message, driver.NackOptions{CountAsFailure: true}, state)
		return false
	}
	destination := retryDestination(r, copyEnvelope, message, tier)
	out := driver.OutboundMessage{Destination: destination, Key: append([]byte(nil), message.Key...), Body: append([]byte(nil), message.Body...), DelayUntil: due}
	for key, value := range encoded {
		out.Headers = append(out.Headers, driver.Header{Key: key, Value: []byte(value)})
	}
	sort.Slice(out.Headers, func(i, j int) bool { return out.Headers[i].Key < out.Headers[j].Key })
	if err := publishSuccessor(r, runnerSettlementContext(r, ctx), out); err != nil {
		failSuccessorHandoff(r, runnerSettlementContext(r, ctx), "retry", eventFromDelivery(r, message, envelope), err)
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
	return consumer.Stop(ctx)
}

func releaseRunnerConsumer(r *Runner, ctx context.Context) error {
	r.mu.Lock()
	consumer := r.consumer
	r.mu.Unlock()
	if consumer == nil {
		return nil
	}
	err := consumer.Release(ctx)
	if err != nil && !errors.Is(err, driver.ErrUnsupported) {
		return err
	}
	// There is no safe fallback: this path has an unsettled delivery, so
	// Stop is forbidden. A driver without Release support cannot recover it.
	return err
}

// --- Shared utilities ---

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
	if len(value) > deathErrorCap {
		return value[:deathErrorCap]
	}
	return value
}

func retryDestinationTierMap(source string, sub Subscription) map[string]int {
	result := make(map[string]int)
	for _, topic := range sub.Topics {
		logical := topicFor(topic)
		for _, priority := range sub.Priorities {
			for tier := 1; tier <= retryTiers(sub.Retry); tier++ {
				result[retryDestinationFor(source, logical, priority, tier, sub.Name)] = tier
			}
		}
	}
	return result
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

// --- Topology and destination naming ---

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
				limit = sub.Retry.MaxAttempts + topologyDeliveryLimitHeadroom
			}
			add(driver.DestinationSpec{Name: main, Kind: driver.DestMain, Durable: true, DeadLetter: route, DeliveryLimit: limit})
			if isFanoutEntryPoint(effective) {
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
	topic, ok := consumingTopicFamily(r, envelope.Priority, message.Destination)
	if !ok {
		topic = topicFor(envelope.Type)
	}
	if topic == "" {
		topic = "unknown"
	}
	return deadLetterDestinationFor(r.client.source, topic, r.subscription.Name)
}

func retryDestination(r *Runner, envelope Envelope, message driver.InboundMessage, tier int) string {
	topic, ok := consumingTopicFamily(r, envelope.Priority, message.Destination)
	if !ok {
		topic = topicFor(envelope.Type)
	}
	return retryDestinationFor(r.client.source, topic, envelope.Priority, tier, r.subscription.Name)
}

func deadLetterDestinationFor(source, topic, subscription string) string {
	return fmt.Sprintf("f1.%s.%s.dlq.%s", sourceEnvironment(source), topic, subscription)
}

func retryDestinationFor(source, topic string, priority Priority, tier int, subscription string) string {
	return fmt.Sprintf("f1.%s.%s.%s.%s.%s.%d", sourceEnvironment(source), topic, subscription, priority, retryDestinationSegment, tier)
}

func consumeDestination(effective driver.Capabilities, source, topic string, priority Priority, subscription string) string {
	if isFanoutEntryPoint(effective) {
		return fmt.Sprintf("f1.%s.%s.%s.%s", sourceEnvironment(source), topic, subscription, priority)
	}
	return publishEntryPoint(source, topic, priority)
}
