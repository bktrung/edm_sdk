package f1

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/obs"
)

type delivery struct {
	id      uint64
	message driver.InboundMessage
}

type deliveryState struct {
	attempted bool
	settled   bool
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

	dispatch := make(chan delivery, r.subscription.Concurrency)
	group.Go(func() error { return fetchRunner(r, runCtx, dispatch) })
	for i := 0; i < r.subscription.Concurrency; i++ {
		group.Go(func() error {
			for item := range dispatch {
				processDelivery(r, runCtx, item)
			}
			return nil
		})
	}
	group.Go(func() error { return consumeRunnerErrors(r, runCtx) })
	err = group.Wait()
	if err == nil {
		err = runnerError(r)
	}
	if r.inflight != nil {
		waitTimeout := r.client.config.Lifecycle.DrainTimeout
		if waitTimeout <= 0 {
			waitTimeout = time.Minute
		}
		waitCtx, cancel := context.WithTimeout(runnerSettlementContext(r, runCtx), waitTimeout)
		if waitErr := r.inflight.WaitZero(waitCtx); err == nil && waitErr != nil {
			err = waitErr
		}
		cancel()
	}
	if err != nil && r.inflight.Len() != 0 {
		return err
	}
	closeTimeout := r.client.config.Lifecycle.CloseTimeout
	if closeTimeout <= 0 {
		closeTimeout = time.Minute
	}
	stopCtx, cancel := context.WithTimeout(runnerSettlementContext(r, runCtx), closeTimeout)
	defer cancel()
	stopErr := stopRunnerConsumer(r, stopCtx)
	if err != nil {
		return err
	}
	return stopErr
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
		r.mu.Lock()
		group := r.group
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
	if conn == nil {
		return nil, errors.New("f1: client is not connected")
	}
	destinations := subscriptionDestinations(effective, source, r.subscription)
	if admin := conn.Admin(); admin != nil {
		topology := subscriptionTopologySpecs(effective, source, r.subscription)
		if _, err := admin.EnsureTopology(ctx, topology); err != nil {
			return nil, fmt.Errorf("f1: ensure subscription topology: %w", err)
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
	select {
	case dispatch <- item:
		return true
	case <-ctx.Done():
		// A cancellation path may still have a delivery in the channel. Put it
		// back through the broker rather than losing it locally.
		_ = nackDelivery(r, runnerSettlementContext(r, ctx), message, driver.NackOptions{Requeue: true})
		r.inflight.Remove(id)
		return false
	}
}

func processDelivery(r *Runner, ctx context.Context, item delivery) {
	abandoned := false
	state := &deliveryState{}
	var envelope Envelope
	defer func() {
		if recovered := recover(); recovered != nil {
			err := fmt.Errorf("handler panic: %v\n%s", recovered, debug.Stack())
			deadLetterAndSettle(r, ctx, item.message, envelope, ReasonPanic, err, state)
		}
		if !state.settled && !state.attempted && !abandoned {
			_ = nackDelivery(r, runnerSettlementContext(r, ctx), item.message, driver.NackOptions{Requeue: true}, state)
		}
		if state.settled {
			r.inflight.Remove(item.id)
		}
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
	if IsDropped(result.err) {
		runnerNotifyDiscarded(r, runnerSettlementContext(r, ctx), Discarded{Envelope: envelope, Reason: DiscardDropped, Err: result.err})
		return ackDelivery(r, runnerSettlementContext(r, ctx), message, state)
	}
	if IsTerminal(result.err) {
		return deadLetterAndSettle(r, ctx, message, envelope, ReasonTerminal, result.err, state)
	}
	if IsUnavailable(result.err) {
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

func ackDelivery(_ *Runner, ctx context.Context, message driver.InboundMessage, states ...*deliveryState) bool {
	state := stateFor(states)
	state.attempted = true
	if message.Settle == nil {
		return false
	}
	state.settled = message.Settle.Ack(ctx) == nil
	return state.settled
}

func nackDelivery(_ *Runner, ctx context.Context, message driver.InboundMessage, options driver.NackOptions, states ...*deliveryState) error {
	state := stateFor(states)
	state.attempted = true
	if message.Settle == nil {
		return errors.New("f1: delivered message has no settler")
	}
	err := message.Settle.Nack(ctx, options)
	state.settled = err == nil
	return err
}

func deadLetterAndSettle(r *Runner, ctx context.Context, message driver.InboundMessage, envelope Envelope, reason DeathReason, lastErr error, states ...*deliveryState) bool {
	state := stateFor(states)
	if err := deadLetter(r, ctx, message, envelope, reason, lastErr); err != nil {
		return false
	}
	return ackDelivery(r, runnerSettlementContext(r, ctx), message, state)
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
	tier := deferrals
	if tier == 0 {
		tier = envelope.Attempt
	}
	if tier < 1 {
		tier = 1
	}
	tiers := retryTiers(r.subscription.Retry)
	if tiers == 0 {
		return deadLetterAndSettle(r, ctx, message, envelope, ReasonMaxAttempts, lastErr, state)
	}
	if tier > tiers {
		tier = tiers
	}
	delay := r.subscription.Retry.DelayFor(tier)
	if requested, ok := RetryDelay(lastErr); ok {
		if requested < 0 {
			requested = 0
		}
		if max := r.subscription.Retry.MaxInterval; max > 0 && requested > max {
			requested = max
		}
		delay = requested
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
	return ackDelivery(r, runnerSettlementContext(r, ctx), message, state)
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
