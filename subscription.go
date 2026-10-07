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
	group         *errgroup.Group
	// asyncGroup owns handler and terminal-callback goroutines. finishRunner
	// cancels their contexts but does not wait: a non-cooperative handler or
	// callback cannot be force-stopped, and waiting would violate drain's bound.
	asyncGroup *errgroup.Group
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
				logger.LogAttrs(parent, slog.LevelWarn, "f1 terminal notification panicked",
					slog.String("kind", kind),
					slog.Any("panic", recovered))
			}
		}()
		callback(ctx)
		return nil
	})
	group.Go(func() error {
		select {
		case <-done:
		case <-ctx.Done():
			logger.LogAttrs(parent, slog.LevelWarn, "f1 terminal notification timed out",
				slog.String("kind", kind),
				slog.Duration("timeout", terminalNotificationTimeout))
		}
		cancel()
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
