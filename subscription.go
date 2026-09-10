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
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/dispatch"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
)

// Subscription declares one consumer group, its delivery policy, and the
// handlers and lifecycle notifications attached to it.
type Subscription struct {
	Name            string
	Topics          []string
	Mode            Mode
	Concurrency     int
	Prefetch        int
	Priorities      []Priority
	Fairness        FairnessConfig
	Retry           RetryConfig
	HandlerTimeout  time.Duration
	UnmatchedPolicy UnmatchedPolicy
	OnDeadLetter    func(context.Context, DeadLettered)
	OnDiscarded     func(context.Context, Discarded)
	Handlers        map[string]Handler
}

// DeadLettered describes one message whose confirmed copy reached a DLQ.
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

// Discarded describes one message acknowledged without applying its effect or
// retaining a copy.
type Discarded struct {
	Envelope Envelope
	// Body is an independent copy of the message payload, safe to read after
	// this callback returns and after settlement completes.
	Body   []byte
	Reason DiscardReason
	Err    error
}

// DiscardReason identifies why a message was acknowledged without a retained
// copy.
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

// Runner owns a validated subscription. Construction and notifications live
// in subscription.go; dispatch and settlement live in worker.go; abandon and
// drain-after-run ownership lives in reconnect.go.
type Runner struct {
	client       *Client
	subscription Subscription
	config       SubscriptionConfig
	mu           sync.Mutex
	consumer     driver.Consumer
	group        *errgroup.Group
	// asyncGroup owns handler and terminal-callback goroutines. finishRunner
	// cancels their contexts but does not wait: a non-cooperative handler or
	// callback cannot be force-stopped, and waiting would violate drain's bound.
	asyncGroup *errgroup.Group
	// errorGroup owns only error-handler notifications. It is bounded and is
	// never waited by the runner, because a non-cooperative callback cannot be
	// force-stopped without extending shutdown.
	errorGroup            *errgroup.Group
	runCtx                context.Context
	handlerCtx            context.Context
	handlerCancel         context.CancelFunc
	handlerShutdownCtx    context.Context
	handlerShutdownCancel context.CancelFunc
	settleCtx             context.Context
	settleCancel          context.CancelFunc
	cancel                context.CancelFunc
	done                  chan struct{}
	started               bool
	draining              bool
	drainStarted          chan struct{}
	finished              bool
	runErr                error
	reconnectCause        error
	// consumerError identifies a transient failure from Consumer.Errors in this generation.
	consumerError bool
	// successfulDelivery means this generation completed at least one handled delivery.
	successfulDelivery bool
	// repairCycleActive means a replacement consumer has been built and the next
	// transient consumer failure can complete a failed repair cycle.
	repairCycleActive bool
	// failedRepairCycles counts consecutive replacement consumers that fail before
	// any handled delivery; a handled delivery resets it.
	failedRepairCycles    int
	inflight              *inflightRegistry
	retryDestinationTiers map[string]int
	lifecycle             *lifecycle.Machine
	accounting            *lifecycle.Accounting
	dispatchPool          *dispatch.Pool
}

const terminalNotificationTimeout = time.Second

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
				logger.Warn("f1 error handler panicked", "panic", recovered)
			}
		}()
		ctx, cancel := context.WithTimeout(parent, terminalNotificationTimeout)
		defer cancel()
		handler(ctx, event, cause)
		return nil
	}) {
		logger.Error("f1 error handler notification dropped", "subscription", r.subscription.Name, "cause", cause)
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
	notifyOwned(parent, callback, group, lastResortRunnerLogger(r), kind)
}

func notifyOwned(parent context.Context, callback func(context.Context), group *errgroup.Group, logger *slog.Logger, kind string) {
	if callback == nil {
		return
	}
	if group == nil {
		return
	}
	ctx, cancel := context.WithTimeout(parent, terminalNotificationTimeout)
	done := make(chan struct{})
	group.Go(func() (err error) {
		defer close(done)
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.Warn("f1 terminal notification panicked", "kind", kind, "panic", recovered)
			}
		}()
		callback(ctx)
		return nil
	})
	group.Go(func() error {
		select {
		case <-done:
		case <-ctx.Done():
			logger.Warn("f1 terminal notification timed out", "kind", kind, "timeout", terminalNotificationTimeout)
		}
		cancel()
		return nil
	})
}

// Subscribe validates sub after applying the subscription-specific config
// precedence and returns a runner ready for the worker phase.
func (c *Client) Subscribe(ctx context.Context, sub Subscription) (*Runner, error) {
	if c == nil {
		return nil, errors.New("f1: client is not connected")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.closed || c.conn == nil {
		c.mu.Unlock()
		return nil, errors.New("f1: client is closed")
	}
	if c.shutdownStarted {
		c.mu.Unlock()
		return nil, errors.New("f1: client is closing")
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
	c.mu.Lock()
	if c.closed || c.conn == nil {
		c.mu.Unlock()
		return nil, errors.New("f1: client is closed")
	}
	if c.shutdownStarted {
		c.mu.Unlock()
		return nil, errors.New("f1: client is closing")
	}
	c.mu.Unlock()
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
	if c.closed || c.shutdownStarted || c.conn == nil {
		c.mu.Unlock()
		return nil, errors.New("f1: client is closing")
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
	defaults := defaultSubscription()
	resolved := defaults
	if loaded, ok := c.config.Subscriptions[sub.Name]; ok {
		overlayLoadedSubscription(&resolved, loaded)
	}
	if err := applySubscriptionEnvironment(sub.Name, &resolved); err != nil {
		return SubscriptionConfig{}, err
	}
	overlayExplicitSubscription(&resolved, sub)
	if resolved.Prefetch == 0 {
		resolved.Prefetch = resolvePrefetch(resolved.Prefetch, c.config.Broker.DefaultPrefetch)
	}
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
	if p.Prefetch {
		dst.Prefetch = src.Prefetch
	} else if src.Prefetch != 0 {
		dst.Prefetch = src.Prefetch
	}
	if p.Priorities || len(src.Priorities) > 0 {
		dst.Priorities = append([]Priority(nil), src.Priorities...)
	}
	if p.Fairness || !fairnessZero(src.Fairness) {
		dst.Fairness = cloneFairness(src.Fairness)
	}
	if p.Retry || !retryZero(src.Retry) {
		dst.Retry = cloneRetry(src.Retry)
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

func validateSubscription(cfg Config, driverName, name string, sub SubscriptionConfig) error {
	if name == "" {
		return errors.New("f1: subscription name must not be empty")
	}
	if len(sub.Topics) == 0 {
		return fmt.Errorf("f1: subscriptions.%s.topics must not be empty", name)
	}
	for _, topic := range sub.Topics {
		if strings.TrimSpace(topic) == "" {
			return fmt.Errorf("f1: subscriptions.%s.topics must not contain an empty topic", name)
		}
	}
	if sub.Concurrency < 1 || sub.Concurrency > 1024 {
		return fmt.Errorf("f1: subscriptions.%s.concurrency must be between 1 and 1024", name)
	}
	if err := validateSubscriptionModeAndPolicy(name, sub.Mode, sub.UnmatchedPolicy); err != nil {
		return err
	}
	if len(sub.Priorities) == 0 {
		return fmt.Errorf("f1: subscriptions.%s.priorities must not be empty", name)
	}
	seen := make(map[Priority]struct{}, len(sub.Priorities))
	for _, priority := range sub.Priorities {
		if !priority.Valid() {
			return fmt.Errorf("f1: subscriptions.%s.priorities contains invalid priority %q", name, priority)
		}
		if _, ok := seen[priority]; ok {
			return fmt.Errorf("f1: subscriptions.%s.priorities contains duplicate %s", name, priority)
		}
		seen[priority] = struct{}{}
	}
	if err := validateRetryConfig("subscriptions."+name+".retry", sub.Retry); err != nil {
		return err
	}
	tiers := retryTiers(sub.Retry)
	lanes := len(sub.Topics) * len(sub.Priorities) * (1 + tiers)
	if err := validatePrefetch(name, sub.Prefetch, lanes); err != nil {
		return err
	}
	if sub.HandlerTimeout <= 0 {
		return fmt.Errorf("f1: subscriptions.%s.handlerTimeout must be positive", name)
	}
	if cfg.Lifecycle.DrainTimeout <= sub.HandlerTimeout {
		return fmt.Errorf("f1: lifecycle.drainTimeout must exceed subscriptions.%s.handlerTimeout", name)
	}
	for priority, weight := range sub.Fairness.Weights {
		if !priority.Valid() || weight < 1 {
			return fmt.Errorf("f1: subscriptions.%s.fairness.weights.%s must be at least 1", name, priority)
		}
	}
	for priority, budget := range sub.Fairness.Budgets {
		if !priority.Valid() || budget < 0 {
			return fmt.Errorf("f1: subscriptions.%s.fairness.budgets.%s must not be negative", name, priority)
		}
	}
	if driverName == "rabbitmq" {
		consumerTimeout, err := durationOption(cfg.Broker.DriverOptions, "rabbitmq.consumerTimeout", 90*time.Second)
		if err != nil {
			return err
		}
		if consumerTimeout < sub.HandlerTimeout*3 {
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
	value.Weights = clonePriorityWeights(value.Weights)
	value.Budgets = clonePriorityBudgets(value.Budgets)
	return value
}

func cloneRetry(value RetryConfig) RetryConfig {
	value.Tiers = append([]time.Duration(nil), value.Tiers...)
	return value
}

func mergeFairness(base, override FairnessConfig) FairnessConfig {
	if override.Weights != nil {
		base.Weights = clonePriorityWeights(override.Weights)
	}
	if override.Budgets != nil {
		base.Budgets = clonePriorityBudgets(override.Budgets)
	}
	if override.RetryWeightDivisor != 0 {
		base.RetryWeightDivisor = override.RetryWeightDivisor
	}
	if override.CostModel != "" {
		base.CostModel = override.CostModel
	}
	if override.PrefetchFactor != 0 {
		base.PrefetchFactor = override.PrefetchFactor
	}
	if override.AgingEnabled {
		base.AgingEnabled = true
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
	if override.Jitter != 0 {
		base.Jitter = override.Jitter
	}
	if override.Tiers != nil {
		base.Tiers = append([]time.Duration(nil), override.Tiers...)
	}
	return base
}

func fairnessZero(value FairnessConfig) bool {
	return value.Weights == nil && value.Budgets == nil && value.RetryWeightDivisor == 0 && value.CostModel == "" && value.PrefetchFactor == 0 && !value.AgingEnabled
}

func retryZero(value RetryConfig) bool {
	return value.MaxAttempts == 0 && value.InitialInterval == 0 && value.Multiplier == 0 && value.MaxInterval == 0 && value.Jitter == 0 && value.Tiers == nil
}

func clonePriorityWeights(value map[Priority]int) map[Priority]int {
	if value == nil {
		return nil
	}
	result := make(map[Priority]int, len(value))
	maps.Copy(result, value)
	return result
}

func clonePriorityBudgets(value map[Priority]time.Duration) map[Priority]time.Duration {
	if value == nil {
		return nil
	}
	result := make(map[Priority]time.Duration, len(value))
	maps.Copy(result, value)
	return result
}
