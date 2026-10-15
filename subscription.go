package f1

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/dispatch"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
)

// Subscription declares one consumer group, its topics and delivery policy,
// handlers, and lifecycle callbacks. Client.Subscribe validates it and returns
// a Runner that has not started consuming.
type Subscription struct {
	Name        string
	Topics      []string
	Mode        Mode
	Concurrency int
	// Prefetch caps total SDK-admitted unsettled deliveries. Zero leaves
	// configuration overrides in effect, then uses a positive broker fallback
	// or automatic sizing from the resolved lane capacities.
	// Broker/client transport buffering is separate from this admission cap.
	Prefetch   int
	Priorities []Priority
	Fairness   FairnessConfig
	Retry      RetryConfig
	// HandlerTimeout bounds the handler context; zero uses the resolved default.
	// A handler still running at four times this timeout counts as leftover
	// until it returns. Terminal callbacks share this runner-lifetime count.
	// Reaching the resolved Concurrency latches failure: no new handler or
	// terminal callback is admitted, Run returns a fatal classified error, and
	// Health reports the subscription as failed. Already-admitted work can
	// exceed this threshold. Returning leftovers do not clear the failure.
	// A runner that is already draining or has stopped counts nothing, so a
	// subscription that only abandons handlers on the way out still stops
	// cleanly. Drain and Close never wait for leftover user code.
	HandlerTimeout  time.Duration
	UnmatchedPolicy UnmatchedPolicy
	// OnDeadLetter runs asynchronously after the dead-letter publish is
	// confirmed, even if acknowledging the original delivery later fails.
	// Its context has a one-second deadline and inherits parent cancellation.
	// A callback still running when that deadline passes counts toward the
	// shared HandlerTimeout leftover threshold until it returns, except on a
	// runner that is already draining or has stopped. A parent cancellation
	// alone does not count it: it counts only if it is still running when its
	// own one-second deadline passes. After failure is latched no new callback
	// is admitted; Drain and Close never wait for it.
	OnDeadLetter func(context.Context, DeadLettered)
	// OnDiscarded runs asynchronously only after acknowledging a discarded
	// delivery succeeds. Its context has a one-second deadline and inherits
	// parent cancellation. A callback still running when that deadline passes
	// counts toward the shared HandlerTimeout leftover threshold until it
	// returns, except on a runner that is already draining or has stopped. A
	// parent cancellation alone does not count it: it counts only if it is
	// still running when its own one-second deadline passes. Already-admitted
	// callbacks can exceed that threshold; after failure is latched no new
	// callback is admitted. Drain and Close never wait for it.
	OnDiscarded func(context.Context, Discarded)
	Handlers    map[string]Handler
}

// DeadLettered carries the message data passed to OnDeadLetter after its
// dead-letter copy is confirmed.
type DeadLettered struct {
	Envelope Envelope
	// Body is an independent copy of the message payload, safe to read after
	// this callback returns and after settlement completes.
	Body        []byte
	Reason      DeathReason
	Attempt     int
	LastErr     error
	Destination string
}

// Discarded carries the message data passed to OnDiscarded when a message is
// acknowledged without applying its effect or retaining a copy. The callback
// is scheduled at most once per delivery, only after an inline or deferred Ack
// returns success. A requeue fallback or unsettled delivery never schedules it.
type Discarded struct {
	Envelope Envelope
	// Body is an independent copy of the message payload, safe to read after
	// this callback returns and after settlement completes.
	Body   []byte
	Reason DiscardReason
	Err    error
}

// DiscardReason identifies why an event without a retained copy was
// acknowledged. Its zero value means no reason was recorded.
type DiscardReason string

const (
	// DiscardUnmatched means no handler matched the event type.
	DiscardUnmatched DiscardReason = "unmatched"
	// DiscardDropped means a handler explicitly dropped the event.
	DiscardDropped DiscardReason = "dropped"
)

func discardUnmatched(envelope Envelope, body []byte) Discarded {
	return Discarded{Envelope: envelope, Body: body, Reason: DiscardUnmatched}
}

// Runner executes the validated Subscription returned by Client.Subscribe.
// Call Run once to start delivery and Drain to stop it gracefully. Its zero
// value is not usable.
type Runner struct {
	client       *Client
	subscription Subscription
	config       SubscriptionConfig
	mu           sync.Mutex
	consumer     driver.Consumer
	// consumerEpoch is the connection incarnation the stored consumer was
	// opened on. A failed Release keeps the consumer against that connection,
	// because that is the one the driver still carries it on.
	consumerEpoch uint64
	// teardown is the one teardown the stored consumer gets: the first of the
	// terminal drain, a reconnect abandon, a lane-repair release and a
	// successor-handoff release claims it, and the rest join its result. The
	// run loop clears it when it admits a consumer, so an attempt always names
	// the driver object whose sequence must happen once and never a consumer
	// the runner has replaced. It is guarded by mu.
	teardown *consumerTeardown
	group    *errgroup.Group
	// asyncGroup owns handler and terminal-callback goroutines. finishRunner
	// cancels their contexts but does not wait: a non-cooperative handler or
	// callback cannot be force-stopped, and waiting would violate drain's bound.
	asyncGroup *errgroup.Group
	// Leftover accounting belongs to the runner, not a consumer generation:
	// a rebuilt consumer cannot release user goroutines from its predecessor.
	leftoverCount  int
	leftoverFailed bool
	leftoverLimit  chan struct{}
	// errorGroup owns only error-handler notifications. It is bounded and is
	// never waited by the runner, because a non-cooperative callback cannot be
	// force-stopped without extending shutdown.
	errorGroup            *errgroup.Group
	handlerCtx            context.Context
	handlerCancel         context.CancelFunc
	handlerShutdownCtx    context.Context
	handlerShutdownCancel context.CancelFunc
	settleCtx             context.Context
	settleCancel          context.CancelFunc
	cancel                context.CancelFunc
	done                  chan struct{}
	// events carries every report the runner's other goroutines make to the
	// one goroutine that owns its state. Run creates it before any source
	// starts and never replaces it, so a source always has somewhere to report
	// and the owner always has exactly one thing to read.
	events       chan runnerEvent
	started      bool
	draining     bool
	drainStarted chan struct{}
	finished     bool
	runErr       error
	// recordedFailure reports whether this runner has already recorded a
	// failure against its subscription name. It is guarded by client.mu, not
	// by mu: every record site holds that lock, and Run refuses a second start,
	// so a runner records at most once. The exit record reads it in the same
	// critical section that decides whether the entry it would replace is still
	// this runner's.
	recordedFailure     bool
	inflight            *inflightRegistry
	destinationMetadata map[string]destinationMetadata
	// lanePlan is the lane plan the runner's consumer open established: its
	// destinations are the ones that open declared and its capacities are the
	// ones the driver's per-destination caps were keyed on. It is nil until the
	// first open, so a runner that has not opened derives a plan from the
	// client's live capabilities instead.
	lanePlan  []runnerLane
	lifecycle *lifecycle.Machine
}

const terminalNotificationTimeout = time.Second

// userInvocation is guarded by Runner.mu, including after its user code returns.
type userInvocation struct {
	returned bool
	leftover bool
}

// startUserInvocation admits one piece of user work and launches it on group,
// returning the state its caller marks as abandoned or completes. It returns nil
// once the leftover threshold has latched, so every handler and terminal
// callback funnels its admission decision through here.
//
// group must be non-nil: both call sites resolve one, and a runner built without
// Run gets its throwaway group from the caller, which is where that fallback
// belongs. A nil is a caller bug, not a state to absorb here.
func (r *Runner) startUserInvocation(group *errgroup.Group, run func(*userInvocation) error) *userInvocation {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.leftoverFailed {
		return nil
	}
	inv := new(userInvocation)
	// Launch under mu so admission and the failure latch have one ordering.
	// Already-admitted work may run after the latch; no user code holds mu.
	group.Go(func() error {
		defer r.completeUserInvocation(inv)
		return run(inv)
	})
	return inv
}

// markUserLeftover records that user code has outlived the runner's patience
// with it: a handler abandoned at four times its timeout, or a terminal callback
// whose context ended. It counts at most once per invocation, and it is where
// the threshold latches.
func (r *Runner) markUserLeftover(inv *userInvocation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// A timeout watcher can select expiry just as user code returns. If the
	// user's completion acquires mu first, returned prevents a stale increment.
	// If this watcher acquires it first, completion removes that increment.
	if inv.returned || inv.leftover {
		return
	}
	// A runner that is already stopping takes no further messages, so a
	// leftover appearing now cannot make it consume less than it will anyway,
	// and counting it would fail a subscription that is merely shutting down:
	// Drain and Close abandon in-flight handlers on their own budget, and those
	// exits must stay successful. The count that means something is the one a
	// consuming runner accumulates. Drain sets draining under this same mutex
	// before it cancels anything, so a mark that interleaves with the start of
	// a drain sees either the whole count or no drain at all, never a count the
	// drain itself caused.
	if r.runnerStoppingLocked() {
		return
	}
	inv.leftover = true
	r.leftoverCount++
	limit := max(r.subscription.Concurrency, 1)
	if r.leftoverCount >= limit && !r.leftoverFailed {
		r.leftoverFailed = true
		if r.leftoverLimit == nil {
			r.leftoverLimit = make(chan struct{})
		}
		close(r.leftoverLimit)
	}
}

// runnerStoppingLocked reports that this runner is ending, so it will consume
// nothing further and a leftover cannot be a reason to stop it. The caller holds
// r.mu.
//
// Four ways in, and each covers a stop the others miss. Drain sets draining
// before it cancels anything. The lifecycle reaches Draining on the runner's own
// end-of-run drain, which the exported flag never marks, and reaches Failed when
// the runner already has a reason to stop. Run's cancellation and the exit of
// Run reach the two handler contexts, which is the only signal left once the
// lifecycle has stopped moving.
//
// A generation that ends while the runner still expects to consume - a consumer
// error awaiting repair, or a stream that closed - is deliberately not in this
// set: its leftovers are exactly what the threshold exists to count.
func (r *Runner) runnerStoppingLocked() bool {
	if r.draining {
		return true
	}
	if r.lifecycle != nil {
		switch r.lifecycle.State() {
		case lifecycle.Draining, lifecycle.Failed, lifecycle.Closed, lifecycle.Aborted:
			return true
		}
	}
	if r.handlerCtx != nil && r.handlerCtx.Err() != nil {
		return true
	}
	return r.handlerShutdownCtx != nil && r.handlerShutdownCtx.Err() != nil
}

// completeUserInvocation records that user code actually returned. It is
// idempotent, because both the launch wrapper and the user path call it, and it
// releases a leftover's slot exactly once. It never clears the failure latch:
// work that returns after the threshold does not reopen admission.
func (r *Runner) completeUserInvocation(inv *userInvocation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if inv.returned {
		return
	}
	inv.returned = true
	if inv.leftover {
		r.leftoverCount--
	}
}

// terminalCallbackContext advertises the injected-clock deadline without
// installing a real-time context timer. The existing callback watcher expires it.
type terminalCallbackContext struct {
	context.Context
	deadline time.Time
}

func (ctx terminalCallbackContext) Deadline() (time.Time, bool) {
	return ctx.deadline, true
}

func (ctx terminalCallbackContext) Err() error {
	if errors.Is(context.Cause(ctx.Context), context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return ctx.Context.Err()
}

func newErrorHandlerGroup(concurrency int) *errgroup.Group {
	if concurrency < 1 {
		concurrency = 1
	}
	group := new(errgroup.Group)
	group.SetLimit(concurrency)
	return group
}

func runnerNotifyDeadLetter(r *Runner, parent context.Context, payload DeadLettered) {
	if r == nil || r.subscription.OnDeadLetter == nil {
		return
	}
	runnerNotify(r, parent, func(ctx context.Context) { r.subscription.OnDeadLetter(ctx, payload) }, "dead-letter")
}

func runnerNotifyDiscarded(r *Runner, parent context.Context, payload Discarded) {
	if r == nil || r.subscription.OnDiscarded == nil {
		return
	}
	runnerNotify(r, parent, func(ctx context.Context) { r.subscription.OnDiscarded(ctx, payload) }, "discarded")
}

// runnerNotifyError delivers an asynchronous error to the client's
// WithErrorHandler callback, if one is configured. event is nil for a
// connection-level error that belongs to no message.
//
// Unlike runnerNotify above, this call never waits on the handler: one of
// its two call sites is failSuccessorHandoff, on the synchronous
// delivery/settlement path, so waiting even briefly here would let a slow
// handler stall a delivery. The handler runs on r.errorGroup, on its own
// goroutine, with panic recovery and a terminalNotificationTimeout deadline
// passed through its context; the deadline is advisory (Go cannot force-stop
// a goroutine that ignores ctx), but the caller itself never blocks on it.
func runnerNotifyError(r *Runner, parent context.Context, event *Event, cause error) {
	if r == nil || r.client == nil || r.client.options.errorHandler == nil || cause == nil || parent == nil {
		return
	}
	handler := r.client.options.errorHandler
	r.mu.Lock()
	group := r.errorGroup
	if group == nil {
		group = newErrorHandlerGroup(r.subscription.Concurrency)
		r.errorGroup = group
	}
	r.mu.Unlock()
	logger := lastResortRunnerLogger(r)
	if !group.TryGo(func() (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.LogAttrs(parent, slog.LevelWarn, "f1 error handler panicked",
					slog.Any("panic", recovered))
			}
		}()
		ctx, cancel := context.WithTimeout(parent, terminalNotificationTimeout)
		defer cancel()
		handler(ctx, event, cause)
		return nil
	}) {
		logger.LogAttrs(parent, slog.LevelError, "f1 error handler notification dropped",
			slog.String("subscription", r.subscription.Name),
			slog.Any("cause", cause))
	}
}

func runnerNotify(r *Runner, parent context.Context, callback func(context.Context), kind string) {
	if parent == nil {
		return
	}
	r.mu.Lock()
	group := r.asyncGroup
	if group == nil {
		group = new(errgroup.Group)
		r.asyncGroup = group
	}
	r.mu.Unlock()
	notifyOwned(r, parent, callback, group, lastResortRunnerLogger(r), kind)
}

func notifyOwned(r *Runner, parent context.Context, callback func(context.Context), group *errgroup.Group, logger *slog.Logger, kind string) {
	if callback == nil || group == nil || parent == nil {
		return
	}
	var clk clock.Clock = clock.Real{}
	if r.client != nil && r.client.options.clock != nil {
		clk = r.client.options.clock
	}
	deadline := clk.Now().Add(terminalNotificationTimeout)
	if parentDeadline, ok := parent.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	base, cancel := context.WithCancelCause(parent)
	ctx := terminalCallbackContext{Context: base, deadline: deadline}
	timer := clk.Timer(max(deadline.Sub(clk.Now()), 0))
	done := make(chan struct{})
	inv := r.startUserInvocation(group, func(inv *userInvocation) (err error) {
		defer close(done)
		// Publish completion accounting before done: otherwise the watcher
		// could observe expiry after callback return but before the wrapper's
		// defer and incorrectly count a callback that has already finished.
		defer r.completeUserInvocation(inv)
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.LogAttrs(parent, slog.LevelWarn, "f1 terminal notification panicked",
					slog.String("kind", kind),
					slog.Any("panic", recovered))
			}
		}()
		// terminalCallbackContext carries the runner clock's deadline, and the
		// watcher below is its only expiry, so a fake clock drives it.
		callback(ctx) //nolint:contextcheck // the callback context is deliberate, not derived here.
		return nil
	})
	if inv == nil {
		timer.Stop()
		cancel(context.Canceled)
		return
	}
	group.Go(func() error {
		defer timer.Stop()
		defer cancel(context.Canceled)
		select {
		case <-done:
			return nil
		case <-timer.C:
			cancel(context.DeadlineExceeded)
		case <-ctx.Done():
			// The parent ended, so the callback's context is cancelled, but the
			// count waits for the callback's own deadline. It is the asymmetry
			// handlers already have: a handler is counted only once it outlives
			// its own phase timer, never for a parent cancellation. Keep
			// waiting for the callback to return or for that deadline to pass.
			select {
			case <-done:
				return nil
			case <-timer.C:
				cancel(context.DeadlineExceeded)
			}
		}
		r.markUserLeftover(inv)
		logger.LogAttrs(parent, slog.LevelWarn, "f1 terminal notification timed out",
			slog.String("kind", kind),
			slog.Duration("timeout", terminalNotificationTimeout))
		return nil
	})
}

// Subscribe validates and registers sub, then returns a Runner that has not
// started consuming. Loaded configuration and environment values are applied
// before non-zero fields in sub override them.
func (c *Client) Subscribe(ctx context.Context, sub Subscription) (*Runner, error) {
	if c == nil {
		return nil, errors.New("f1: client is not connected")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if err := c.admit(workSubscribe, 0); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.mu.Unlock()

	resolved, err := resolveSubscription(c, sub)
	if err != nil {
		return nil, err
	}
	if err := validateSubscription(c.config, c.driverName, sub.Name, resolved); err != nil {
		return nil, err
	}
	c.mu.Lock()
	effectiveCapabilities := c.effective
	c.mu.Unlock()
	if resolved.Mode == OrderedByKey && !effectiveCapabilities.OrderedByKey {
		return nil, fmt.Errorf("f1: subscription %s requests ordered_by_key, but feature is unavailable", sub.Name)
	}
	effective := sub
	effective.Topics = append([]string(nil), resolved.Topics...)
	effective.Mode = resolved.Mode
	effective.Concurrency = resolved.Concurrency
	effective.Prefetch = resolved.Prefetch
	effective.Priorities = append([]Priority(nil), resolved.Priorities...)
	effective.Fairness = cloneFairness(resolved.Fairness)
	effective.Retry = cloneRetry(resolved.Retry)
	effective.HandlerTimeout = resolved.HandlerTimeout
	effective.UnmatchedPolicy = resolved.UnmatchedPolicy
	effective.Handlers = wrapHandlers(c.options.middleware, sub.Handlers)
	runner := &Runner{client: c, subscription: effective, config: resolved}
	c.mu.Lock()
	if err := c.admit(workSubscribe, 0); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.runners[runner] = struct{}{}
	c.mu.Unlock()
	return runner, nil
}

func wrapHandlers(middleware []Middleware, handlers map[string]Handler) map[string]Handler {
	if handlers == nil {
		return nil
	}
	wrapped := make(map[string]Handler, len(handlers))
	for pattern, handler := range handlers {
		wrapped[pattern] = buildHandlerChain(middleware, handler)
	}
	return wrapped
}

func resolveSubscription(c *Client, sub Subscription) (SubscriptionConfig, error) {
	resolved := defaultSubscription()
	if loaded, ok := c.config.Subscriptions[sub.Name]; ok {
		overlayLoadedSubscription(&resolved, loaded)
	}
	if err := applySubscriptionEnvironment(sub.Name, &resolved); err != nil {
		return SubscriptionConfig{}, err
	}
	overlayExplicitSubscription(&resolved, sub)
	resolved.Prefetch = resolvePrefetch(resolved.Prefetch, c.config.Broker.DefaultPrefetch)
	return resolved, nil
}

func overlayLoadedSubscription(dst *SubscriptionConfig, src SubscriptionConfig) {
	p := src.presence
	if p.Topics || len(src.Topics) > 0 {
		dst.Topics = append([]string(nil), src.Topics...)
	}
	if p.Mode || src.Mode != Unordered {
		dst.Mode = src.Mode
	}
	if p.Concurrency || src.Concurrency != 0 {
		dst.Concurrency = src.Concurrency
	}
	if p.Prefetch || src.Prefetch != 0 {
		dst.Prefetch = src.Prefetch
	}
	if p.Priorities || len(src.Priorities) > 0 {
		dst.Priorities = append([]Priority(nil), src.Priorities...)
	}
	// A YAML-named fairness or retry block is decoded over the full defaults,
	// so the struct is complete and replaces the default wholesale, which is
	// what preserves an explicit YAML zero. A Go-built config sets no presence
	// and its struct is partial, so it merges field by field, the rule the
	// explicit subscription overlay already uses.
	if p.Fairness || !fairnessZero(src.Fairness) {
		if p.Fairness {
			dst.Fairness = cloneFairness(src.Fairness)
		} else {
			dst.Fairness = mergeFairness(dst.Fairness, src.Fairness)
		}
	}
	if p.Retry || !retryZero(src.Retry) {
		if p.Retry {
			dst.Retry = cloneRetry(src.Retry)
		} else {
			dst.Retry = mergeRetry(dst.Retry, src.Retry)
		}
	}
	if p.HandlerTimeout || src.HandlerTimeout != 0 {
		dst.HandlerTimeout = src.HandlerTimeout
	}
	if p.UnmatchedPolicy || src.UnmatchedPolicy != Ignore {
		dst.UnmatchedPolicy = src.UnmatchedPolicy
	}
}

func overlayExplicitSubscription(dst *SubscriptionConfig, src Subscription) {
	if len(src.Topics) > 0 {
		dst.Topics = append([]string(nil), src.Topics...)
	}
	if src.Mode != Unordered {
		dst.Mode = src.Mode
	}
	if src.Concurrency != 0 {
		dst.Concurrency = src.Concurrency
	}
	if src.Prefetch != 0 {
		dst.Prefetch = src.Prefetch
	}
	if len(src.Priorities) > 0 {
		dst.Priorities = append([]Priority(nil), src.Priorities...)
	}
	if !fairnessZero(src.Fairness) {
		dst.Fairness = mergeFairness(dst.Fairness, src.Fairness)
	}
	if !retryZero(src.Retry) {
		dst.Retry = mergeRetry(dst.Retry, src.Retry)
	}
	if src.HandlerTimeout != 0 {
		dst.HandlerTimeout = src.HandlerTimeout
	}
	if src.UnmatchedPolicy != Ignore {
		dst.UnmatchedPolicy = src.UnmatchedPolicy
	}
}

// validateSubscription checks one subscription's settings. Config loading and
// Subscribe both call it, so a subscription that loads is one that subscribes.
func validateSubscription(cfg Config, driverName, name string, sub SubscriptionConfig) error {
	if name == "" {
		return errors.New("f1: subscription name must not be empty")
	}
	if !isNameSegment(name) {
		return fmt.Errorf("f1: subscription name %q must contain only letters, digits, '-' and '_'", name)
	}
	if len(sub.Topics) == 0 {
		return fmt.Errorf("f1: subscriptions.%s.topics must not be empty", name)
	}
	topics := make(map[string]string, len(sub.Topics))
	for _, topic := range sub.Topics {
		if strings.TrimSpace(topic) == "" {
			return fmt.Errorf("f1: subscriptions.%s.topics must not contain an empty topic", name)
		}
		// Two entries naming one topic would share its lanes while the
		// prefetch floor below counted them twice.
		if first, ok := topics[topicFor(topic)]; ok {
			return fmt.Errorf("f1: subscriptions.%s.topics %q and %q name the same topic", name, first, topic)
		}
		topics[topicFor(topic)] = topic
	}
	if sub.Concurrency < 1 || sub.Concurrency > 1024 {
		return fmt.Errorf("f1: subscriptions.%s.concurrency must be between 1 and 1024", name)
	}
	if err := validateSubscriptionModeAndPolicy(name, sub.Mode, sub.UnmatchedPolicy); err != nil {
		return err
	}
	if err := validatePriorityList("subscriptions."+name+".priorities", sub.Priorities); err != nil {
		return err
	}
	if err := validateRetryConfig("subscriptions."+name+".retry", sub.Retry); err != nil {
		return err
	}
	if err := validatePrefetch(name, sub.Prefetch); err != nil {
		return err
	}
	if sub.HandlerTimeout <= 0 {
		return fmt.Errorf("f1: subscriptions.%s.handlerTimeout must be positive", name)
	}
	if cfg.Lifecycle.DrainTimeout <= sub.HandlerTimeout {
		return fmt.Errorf("f1: lifecycle.drainTimeout must exceed subscriptions.%s.handlerTimeout", name)
	}
	for priority, weight := range sub.Fairness.Weights {
		if !priority.Valid() || weight < 1 || weight > maxFairnessWeight {
			return fmt.Errorf("f1: subscriptions.%s.fairness.weights.%s must be between 1 and %d", name, priority, maxFairnessWeight)
		}
	}
	for priority, budget := range sub.Fairness.Budgets {
		if !priority.Valid() || budget < 0 {
			return fmt.Errorf("f1: subscriptions.%s.fairness.budgets.%s must not be negative", name, priority)
		}
	}
	// Zero leaves the default in place; a negative value is a typo the lane
	// plan would otherwise replace with the default without a word.
	if sub.Fairness.RetryWeightDivisor < 0 {
		return fmt.Errorf("f1: subscriptions.%s.fairness.retryWeightDivisor must not be negative", name)
	}
	if sub.Fairness.PrefetchFactor < 0 {
		return fmt.Errorf("f1: subscriptions.%s.fairness.prefetchFactor must not be negative", name)
	}
	prefetch := sub.Prefetch
	if prefetch == 0 {
		prefetch = automaticSubscriptionPrefetch(sub)
		if err := validatePrefetch(name, prefetch); err != nil {
			return err
		}
	}
	if sub.Mode == OrderedByKey && sub.Concurrency > dispatch.MaxOrderedBufferEntries/prefetch {
		return fmt.Errorf("f1: subscriptions.%s: ordered mode needs concurrency x prefetch at most %d, got %d x %d", name, dispatch.MaxOrderedBufferEntries, sub.Concurrency, prefetch)
	}
	if driverName == "rabbitmq" {
		consumerTimeout, err := durationOption(cfg.Broker.DriverOptions, "rabbitmq.consumerTimeout", 90*time.Second)
		if err != nil {
			return err
		}
		if sub.HandlerTimeout > consumerTimeout/3 {
			return fmt.Errorf("f1: broker.rabbitmq.consumerTimeout must be at least subscriptions.%s.handlerTimeout x 3", name)
		}
	}
	return nil
}

func validateSubscriptionModeAndPolicy(name string, mode Mode, policy UnmatchedPolicy) error {
	if mode != Unordered && mode != OrderedByKey {
		return fmt.Errorf("f1: subscriptions.%s.mode is unsupported", name)
	}
	if policy != Ignore && policy != DeadLetter {
		return fmt.Errorf("f1: subscriptions.%s.unmatchedPolicy is unsupported", name)
	}
	return nil
}

func cloneFairness(value FairnessConfig) FairnessConfig {
	value.Weights = maps.Clone(value.Weights)
	value.Budgets = maps.Clone(value.Budgets)
	return value
}

func cloneRetry(value RetryConfig) RetryConfig {
	value.Tiers = append([]time.Duration(nil), value.Tiers...)
	return value
}

func mergeFairness(base, override FairnessConfig) FairnessConfig {
	if override.Weights != nil {
		base.Weights = maps.Clone(override.Weights)
	}
	if override.Budgets != nil {
		base.Budgets = maps.Clone(override.Budgets)
	}
	if override.RetryWeightDivisor != 0 {
		base.RetryWeightDivisor = override.RetryWeightDivisor
	}
	if override.PrefetchFactor != 0 {
		base.PrefetchFactor = override.PrefetchFactor
	}
	if override.DisableDeadlinePromotion {
		base.DisableDeadlinePromotion = true
	}
	return base
}

func mergeRetry(base, override RetryConfig) RetryConfig {
	if override.MaxAttempts != 0 {
		base.MaxAttempts = override.MaxAttempts
	}
	if override.InitialInterval != 0 {
		base.InitialInterval = override.InitialInterval
	}
	if override.Multiplier != 0 {
		base.Multiplier = override.Multiplier
	}
	if override.MaxInterval != 0 {
		base.MaxInterval = override.MaxInterval
	}
	if override.Tiers != nil {
		base.Tiers = append([]time.Duration(nil), override.Tiers...)
	}
	return base
}

func fairnessZero(value FairnessConfig) bool {
	return value.Weights == nil && value.Budgets == nil && value.RetryWeightDivisor == 0 && value.PrefetchFactor == 0 && !value.DisableDeadlinePromotion
}

func retryZero(value RetryConfig) bool {
	return value.MaxAttempts == 0 && value.InitialInterval == 0 && value.Multiplier == 0 && value.MaxInterval == 0 && value.Tiers == nil
}
