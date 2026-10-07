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
	"unicode/utf8"

	"golang.org/x/sync/errgroup"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/dispatch"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/retry"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/sched"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/wire"
)

const (
	defaultHandlerTimeout         = 30 * time.Second
	defaultRetryWeightDivisor     = 2
	defaultPrefetchFactor         = 2
	retryBudgetMultiplier         = 2
	minimumLaneCapacity           = 3
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
	// enqueuedAt is when the delivery left the fetch channel: the instant its
	// lane's deadline-promotion budget starts. It is stamped there instead of
	// on the attempt that finds lane room, so the time a delivery spends
	// waiting for a full lane counts toward the budget and is reported in the
	// promoted lane's wait.
	enqueuedAt time.Time
}

// settlementOperation identifies the kind of settlement call made for a
// delivery, so a failed one can be retried in kind rather than guessed at. The
// zero value means no settlement call has been made.
type settlementOperation uint8

const (
	// settlementOperationAck means the delivery was asked to be acknowledged.
	settlementOperationAck settlementOperation = iota + 1
	// settlementOperationNack means the delivery was asked to be negatively
	// acknowledged.
	settlementOperationNack
)

// deliveryState records attempted versus settled ownership. operation and
// nackOptions preserve the exact settlement operation for deferred retries.
type deliveryState struct {
	id        uint64
	attempted bool
	settled   bool
	// operation records which settlement call was last made, so that a failed
	// one can be retried in kind rather than guessed at.
	operation      settlementOperation
	nackOptions    driver.NackOptions
	poisonDrop     *poisonDropReport
	discard        Discarded
	discardPending bool
	// processCtx carries the context Start of ObserverProcess returned, stored
	// only when an observer is set. The successor publish start uses it as its
	// context when set and the context at hand otherwise.
	processCtx       context.Context
	headerMaxBytes   int
	envelopeDecoded  bool
	envelopePriority Priority
}

type poisonDropReport struct {
	envelope Envelope
	// topic is the delivery's resolved successor topic, carried so the report
	// does not resolve it again when it emits the poison-rejected event.
	topic string
	// reason is the death reason the dropped delivery was settling with.
	reason DeathReason
	// headline leads both the report and the last-resort log line, so a drop
	// that is not a missing route does not borrow the poison wording.
	headline string
	// description fills the report's slot after the destination and logMessage
	// is the last-resort logger's text. They are separate because the
	// missing-route drop has a different phrase on each surface today, and
	// generalizing the report must not edit either one.
	description string
	logMessage  string
	cause       error
}

// --- Public runner API and generation lifecycle ---

// runnerEvent is one report a runner's other goroutines make to the goroutine
// calling Run. Exactly one goroutine reads the channel, and every source sends
// its report from a deferred function, so a source that panics still reports
// instead of leaving the owner waiting for an event that never comes.
type runnerEvent struct {
	kind runnerEventKind
	err  error
	// transient marks a consumer error a replacement consumer may fix, as
	// opposed to one the runner has to stop for.
	transient bool
	// consumer and epoch carry what an open reported: the consumer itself, and
	// the connection incarnation it was opened on.
	consumer driver.Consumer
	epoch    uint64
}

// runnerEventKind identifies what a runner event reports.
type runnerEventKind uint8

const (
	// runnerEventSourceDone reports that one of a generation's source
	// goroutines has finished, carrying the error it finished with.
	runnerEventSourceDone runnerEventKind = iota
	// runnerEventHandledDelivery reports that a handled delivery was
	// acknowledged. It is what lets the next transient consumer failure reset
	// the repair budget instead of counting against it.
	runnerEventHandledDelivery
	// runnerEventConsumerError reports an error the consumer-error source read.
	// transient marks the failure a replacement consumer may fix; the error is
	// the cause this runner asks a rebuild for, and the failure it reports if
	// the rebuild hands one back.
	runnerEventConsumerError
	// runnerEventConsumerOpened reports the result of opening a generation's
	// consumer. The open runs on its own goroutine, so the owner can answer a
	// drain or an abandon while a broker call is in flight.
	runnerEventConsumerOpened
	// runnerEventRebuilt reports the result of waiting for a connection
	// rebuild. That wait runs on its own goroutine for the same reason: an
	// attempt replacing the connection abandons this runner first, and an owner
	// parked in the wait could not receive the abandon that ends it.
	runnerEventRebuilt
	// runnerEventAbandon reports that a reconnect attempt is replacing the
	// connection and has released this runner's consumer.
	runnerEventAbandon
	// runnerEventDrain reports that a caller asked the runner to drain.
	runnerEventDrain
	// runnerEventReleased reports the result of releasing the generation's
	// consumer. The release runs on its own goroutine, so a broker call does
	// not hold the owner.
	runnerEventReleased
	// runnerEventDrainDone reports that the runner's one terminal drain has
	// finished.
	runnerEventDrainDone
	// runnerEventError reports a failure one of the runner's own goroutines
	// saw. The owner keeps the first one, and it is what the runner reports
	// through Drain.
	runnerEventError
)

// sourcePanicError converts a recovered panic from a source goroutine into the
// error its report carries. A source runs the fetcher, the dispatch pipeline
// or the consumer-error reader, so its panic is the generation's failure and
// not something a caller can tell apart from an ordinary one.
func sourcePanicError(recovered any) error {
	return fmt.Errorf("f1: runner source panic: %v\n%s", recovered, debug.Stack())
}

// report hands one report to the runner's owner. A runner with no owner has
// nowhere to send: this package's tests call the delivery path directly on a
// runner they built by hand, and Run is the only thing that creates the owner.
// Dropping the report there loses nothing, because the owner is the only
// reader of what it carries.
func (r *Runner) report(event runnerEvent) {
	if r.events != nil {
		r.events <- event
	}
}

// runnerOwner is the one goroutine that owns a runner's state. A runner
// creates one per Run call, and every other goroutine of the runner reports
// through its event channel instead of writing what the owner keeps.
type runnerOwner struct {
	runner *Runner
	events chan runnerEvent

	// outstanding counts the generation's sources that have not reported yet.
	outstanding int
	// sourceErr is the first failure a source of this generation reported.
	sourceErr error
	// successfulDelivery records that this generation completed at least one
	// handled delivery, which is what resets a repair budget.
	successfulDelivery bool
	// consumerError records that the consumer's error stream produced a
	// transient failure in this generation.
	consumerError bool
	// failureCause is this runner's own failure: the transient consumer or
	// open error that a rebuild is asked for, and the failure the runner
	// reports when a rebuild hands one back. It is a plain error with no
	// attempt attached, and the supervisor never writes it, so a cause another
	// runner's reconnect produces cannot replace it.
	failureCause error
	// abandoned records that an attempt replacing the connection released this
	// runner's consumer. It is what tells a generation that ended without a
	// failure of its own apart from one that simply finished.
	abandoned bool
	// runErr is the first failure a source of this run reported. It is never
	// cleared, so it is the error the runner reports through Drain.
	runErr error
	// repairCycleActive means a replacement consumer has been built and the next
	// transient consumer failure can complete a failed repair cycle.
	repairCycleActive bool
	// failedRepairCycles counts consecutive replacement consumers that fail before
	// any handled delivery; a handled delivery resets it.
	failedRepairCycles int

	// base is the context the terminal drain runs on: the current generation's
	// run context, whose values a settlement context has to carry.
	base context.Context
	// cancel stops the current generation's sources. The owner holds it so a
	// drain request stops the generation itself rather than reaching for a
	// cancel function another goroutine set.
	cancel context.CancelFunc
	// openPending is true while an open is in flight.
	openPending bool
	// opened records that the opener has reported.
	opened bool
	// openConsumer, openEpoch and openErr are what the opener reported.
	openConsumer driver.Consumer
	openEpoch    uint64
	openErr      error
	// rebuilt records that the rebuild wait has reported.
	rebuilt    bool
	rebuiltErr error
	// released records that an owner-started release has reported.
	released   bool
	releaseErr error

	// draining records that a drain was requested. Every caller's wait ends
	// when the runner reaches terminal, which the one drain below is what
	// reaches it.
	draining bool
	// drained records that the one terminal drain has started, so a second
	// request joins it instead of releasing the consumer again.
	drained   bool
	drainDone bool
	drainErr  error
}

// beginGeneration resets what describes one generation. It is the owner's own
// reset, so no other goroutine has to reach into state it does not own to
// start the next one.
func (o *runnerOwner) beginGeneration() {
	o.outstanding = 0
	o.sourceErr = nil
	o.successfulDelivery = false
	o.consumerError = false
	o.failureCause = nil
	o.abandoned = false
}

// pumpUntil reads and handles events until ready reports true. It is the
// owner's only blocking read: everything the runner's other goroutines want
// the owner to know arrives here, and a report this wait is not itself
// waiting for is handled rather than left for later.
func (o *runnerOwner) pumpUntil(ready func() bool) {
	for !ready() {
		o.handle(<-o.events)
	}
}

// handle applies one report to the owner's state.
func (o *runnerOwner) handle(event runnerEvent) {
	switch event.kind {
	case runnerEventSourceDone:
		o.outstanding--
		if event.err != nil && o.sourceErr == nil {
			o.sourceErr = event.err
		}
	case runnerEventHandledDelivery:
		o.successfulDelivery = true
	case runnerEventConsumerError:
		if event.transient {
			o.consumerError = true
		}
		if o.failureCause == nil {
			o.failureCause = event.err
		}
	case runnerEventError:
		if o.runErr == nil {
			o.runErr = event.err
		}
	case runnerEventAbandon:
		o.abandoned = true
	case runnerEventConsumerOpened:
		o.openPending = false
		o.opened = true
		o.openConsumer, o.openEpoch, o.openErr = event.consumer, event.epoch, event.err
	case runnerEventRebuilt:
		o.rebuilt = true
		o.rebuiltErr = event.err
	case runnerEventReleased:
		o.released = true
		o.releaseErr = event.err
	case runnerEventDrain:
		o.requestDrain()
	case runnerEventDrainDone:
		o.drainDone = true
		o.drainErr = event.err
	}
}

// startOpen opens the generation's consumer on the owner's own goroutine pool.
// A broker call must not hold the owner: an attempt that replaces the
// connection abandons this runner first, and an owner parked inside the open
// could not receive the abandon that ends it.
func (o *runnerOwner) startOpen(waitCtx context.Context, prefetch int) {
	o.openPending = true
	o.opened = false
	o.openConsumer, o.openEpoch, o.openErr = nil, 0, nil
	runCtx := o.base
	o.runner.asyncGroup.Go(func() error {
		consumer, epoch, err := openRunnerConsumerWith(o.runner, waitCtx, runCtx, prefetch)
		o.events <- runnerEvent{kind: runnerEventConsumerOpened, consumer: consumer, epoch: epoch, err: err}
		return nil
	})
}

// startRebuild waits for the connection rebuild a generation needs on its own
// goroutine, for the same reason the open runs on one. cause is the failure
// the runner asks a rebuild for, and nil for a runner that only has a change
// to wait for.
func (o *runnerOwner) startRebuild(ctx context.Context, cause error, epoch uint64) {
	o.rebuilt = false
	o.rebuiltErr = nil
	o.runner.asyncGroup.Go(func() error {
		err := o.runner.client.awaitRebuild(ctx, cause, epoch, o.runner.drainStarted)
		o.events <- runnerEvent{kind: runnerEventRebuilt, err: err}
		return nil
	})
}

// releaseConsumer releases the generation's consumer on its own goroutine. The
// caller waits for the result, because what it decides next depends on whether
// the consumer went back to the broker.
func (o *runnerOwner) releaseConsumer(ctx context.Context) error {
	o.released = false
	o.releaseErr = nil
	o.runner.asyncGroup.Go(func() error {
		o.events <- runnerEvent{kind: runnerEventReleased, err: releaseRunnerConsumer(o.runner, ctx)}
		return nil
	})
	o.pumpUntil(func() bool { return o.released })
	return o.releaseErr
}

// requestDrain records that the runner must end and stops the generation. A
// request that arrives while the generation is still running is recorded, and
// the loop reaches its drain when the generation stops; one that arrives after
// the generation stopped is the drain, taken here. startDrain makes a later
// request join the first.
//
// The settlement window is computed here and nowhere else: this is the moment
// drain starts, so this is the moment a settlement budget begins. The window
// is installed before the generation is cancelled, so a settlement the
// cancellation triggers already runs on the drain's deadline rather than on a
// context the cancellation ends.
func (o *runnerOwner) requestDrain() {
	if !o.draining {
		o.draining = true
		o.runner.beginSettlementWindow(o.base)
		if o.cancel != nil {
			o.cancel()
		}
	}
	if o.generationEnded() {
		o.startDrain()
	}
}

// generationEnded reports that no source and no opener of the current
// generation is left to report.
func (o *runnerOwner) generationEnded() bool {
	return o.outstanding == 0 && !o.openPending
}

// startDrain begins the runner's one terminal drain. It is idempotent, and
// that is what makes drain one event: the first caller starts it and every
// later request joins it, so this teardown runs once however many of Close,
// the supervisor's abandon and the end of Run arrive at once. An abandon that
// overlaps the drain still calls Release itself, because the supervisor gives
// the consumer back to the connection it is leaving before the runner reopens
// on the replacement: ending the runner and handing a consumer back are not
// the same operation, and the drain is only the first of them.
func (o *runnerOwner) startDrain() {
	if o.drained {
		return
	}
	o.drained = true
	o.runner.asyncGroup.Go(func() error {
		o.events <- runnerEvent{kind: runnerEventDrainDone, err: o.runner.drainAfterRun(o.base)}
		return nil
	})
}

// drain starts the runner's one terminal drain and waits for it to finish.
// startDrain is what joins this call to a drain a request already began: the
// request path leaves a mid-generation drain recorded but unstarted, and the
// loop reaches this call exactly when the generation has ended.
func (o *runnerOwner) drain(base context.Context) error {
	o.base = base
	o.requestDrain()
	o.startDrain()
	o.pumpUntil(func() bool { return o.drainDone })
	return o.drainErr
}

// waitSources pumps events until every source of the generation has reported,
// and returns the first error any of them reported.
func (o *runnerOwner) waitSources() error {
	o.pumpUntil(func() bool { return o.outstanding == 0 })
	return o.sourceErr
}

// Run starts the consumer and returns when it stops, ctx is canceled, or a
// driver error requests shutdown. Canceling ctx stops the runner immediately:
// in-flight handlers see cancellation and HandlerGrace does not apply. Use
// Drain for a graceful stop. A non-nil error after startup is recorded against
// the subscription for Client.Health, except after caller cancellation, drain,
// or client shutdown. A Runner can be started only once.
func (r *Runner) Run(ctx context.Context) (runErr error) {
	if r == nil {
		return errors.New("f1: runner is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.client.mu.Lock()
	if err := r.client.admit(workRun, 0); err != nil {
		r.client.mu.Unlock()
		return err
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
	handlerCtx, handlerCancel := context.WithCancel(ctx)
	r.handlerCtx, r.handlerCancel = handlerCtx, handlerCancel
	handlerShutdownCtx, handlerShutdownCancel := context.WithCancel(context.Background())
	r.handlerShutdownCtx, r.handlerShutdownCancel = handlerShutdownCtx, handlerShutdownCancel
	r.asyncGroup = new(errgroup.Group)
	r.errorGroup = newErrorHandlerGroup(r.subscription.Concurrency)
	r.events = make(chan runnerEvent)
	r.mu.Unlock()
	r.client.mu.Unlock()

	defer finishRunner(r)
	// Registered after the pre-start refusals, so a refused Run never records
	// anything, and before the first return that follows a start. Deferred
	// after finishRunner so it runs first and reads the result Run is about to
	// hand back, including an error joined in by drainAfterRun.
	defer func() { r.recordRunExit(ctx, runErr) }()
	owner := &runnerOwner{runner: r, events: r.events, base: ctx}
	// The owner's error is published to the runner before the runner's done
	// channel closes, so a Drain that returns because the runner ended reads
	// the same failure the run ended with.
	defer func() {
		if owner.runErr != nil {
			r.mu.Lock()
			r.runErr = owner.runErr
			r.mu.Unlock()
		}
	}()
	var runCtx context.Context
	var cancel context.CancelFunc
	generation := 0
	var repairCause error
	for {
		r.mu.Lock()
		failed := r.lifecycle != nil && r.lifecycle.State() == lifecycle.Failed
		r.mu.Unlock()
		if failed || owner.draining {
			break
		}
		runCtx, cancel = context.WithCancel(ctx)
		owner.base = runCtx
		owner.cancel = cancel
		group := beginRunnerGeneration(r, cancel)
		owner.beginGeneration()

		// The consumer's total in-flight budget, derived once per generation.
		// The driver enforces it as its outstanding limit, and the pipeline
		// sizes the ordered worker queues from it, so both read the same number
		// instead of deriving it again from a formula that could drift.
		prefetch := runnerConsumerPrefetch(r, r.config.Prefetch, runnerLanePlan(r))
		owner.startOpen(ctx, prefetch)
		owner.pumpUntil(func() bool { return owner.opened })
		consumer, openedEpoch, err := owner.openConsumer, owner.openEpoch, owner.openErr
		if owner.draining {
			// The runner began draining while the consumer was opening, so the
			// consumer belongs to no generation: it is released here rather
			// than becoming the drain's work, and the drain itself waits for
			// nothing because this generation never delivered anything.
			if consumer != nil {
				if closeErr := releaseUnadmittedConsumer(r, context.WithoutCancel(runCtx), consumer, openedEpoch); closeErr != nil {
					// Published to the runner too, so a Drain caller sees the
					// release failure the Run caller does.
					owner.runErr = errors.Join(owner.runErr, closeErr)
					return closeErr
				}
			}
			return nil
		}
		if err != nil {
			repairCause = nil
			if errors.Is(err, errRunnerDraining) {
				return nil
			}
			if owner.abandoned {
				// An attempt in flight released this consumer, and the open it
				// cancelled is not a failure of the runner's: it waits for the
				// attempt and opens again on what it leaves behind.
				owner.startRebuild(ctx, nil, 0)
				owner.pumpUntil(func() bool { return owner.rebuilt })
				if reconnectErr := owner.rebuiltErr; reconnectErr != nil {
					if errors.Is(reconnectErr, errRunnerDraining) {
						return nil
					}
					runErr = reconnectErr
					break
				}
				continue
			}
			kind, classified := driver.Classify(err)
			if generation == 0 {
				return err
			}
			if classified && kind != driver.KindTransient {
				runErr = err
				break
			}
			// The failure that could not open the consumer is the cause the
			// rebuild is asked for, and it is what this runner reports if the
			// rebuild hands one back.
			r.transitionToReconnecting()
			owner.startRebuild(ctx, err, 0)
			owner.pumpUntil(func() bool { return owner.rebuilt })
			if reconnectErr := owner.rebuiltErr; reconnectErr != nil {
				if errors.Is(reconnectErr, errRunnerDraining) {
					return nil
				}
				runErr = reconnectErr
				break
			}
			continue
		}
		// Admit only a consumer opened on the connection that is still live.
		// requestReconnect marks the client before abandoning runners, so this
		// critical section either rejects and releases this consumer or records
		// it before abandonForReconnect can release it. The epoch the runner
		// captured with the connection is what the admission compares, and the
		// current one is read under the same lock, so a rejection is reported
		// against the incarnation the rejection was taken against.
		r.client.mu.Lock()
		r.mu.Lock()
		admissionErr := r.client.admit(workConsumerAdmission, openedEpoch)
		currentEpoch := r.client.current.epoch
		if admissionErr == nil {
			r.consumer = consumer
			r.consumerEpoch = openedEpoch
		}
		r.mu.Unlock()
		r.client.mu.Unlock()
		if admissionErr != nil {
			if currentEpoch != openedEpoch {
				// A stale open is repaired at once by the next iteration, so it
				// earns a debug line and not a Health entry: a surface that
				// reports self-healing states teaches people to ignore it.
				lastResortRunnerLogger(r).Debug("f1 consumer discarded after the connection was replaced",
					"subscription", r.subscription.Name, "opened_epoch", openedEpoch, "current_epoch", currentEpoch)
			}
			if closeErr := releaseUnadmittedConsumer(r, context.WithoutCancel(runCtx), consumer, openedEpoch); closeErr != nil {
				lastResortRunnerLogger(r).Warn("f1 consumer release failed during reconnect", "subscription", r.subscription.Name, "error", closeErr)
			}
			cancel()
			continue
		}

		generation++
		if repairCause != nil {
			lastResortRunnerLogger(r).Info("f1 consumer repaired", "subscription", r.subscription.Name, "cause", repairCause)
			repairCause = nil
		}
		state := r.lifecycle.State()
		switch state {
		case lifecycle.Starting, lifecycle.Reconnecting:
			owner.repairCycleActive = false
			owner.failedRepairCycles = 0
			if err := r.lifecycle.Transition(lifecycle.Ready); err != nil {
				runErr = err
			} else if state == lifecycle.Starting {
				// A runner that started afresh under this name has replaced the
				// one whose failure is recorded. A runner returning to ready
				// from reconnecting has replaced nothing.
				r.client.clearFailedSubscription(r.subscription.Name)
			}
		case lifecycle.Ready:
		default:
			runErr = fmt.Errorf("f1: runner cannot start from lifecycle state %s", r.lifecycle.State())
		}
		if runErr != nil {
			break
		}

		deliveries := make(chan delivery, r.subscription.Concurrency)
		runPipeline := func(run func() error) error {
			if err := run(); err != nil {
				cancel()
				return err
			}
			return nil
		}
		// Each source reports its own end from a deferred function, which runs
		// on the way out of a panic too. The report is what the owner waits
		// for, so a source that dies abnormally still ends the generation
		// instead of stranding the owner on an event that never arrives.
		startSource := func(run func() error) {
			owner.outstanding++
			group.Go(func() (err error) {
				defer func() {
					if recovered := recover(); recovered != nil {
						// The generation is shared: the sources hand each other
						// the channels they read, so the one that panicked is
						// the one that would have closed what the others are
						// waiting on. Cancel the generation the way an error
						// return does, or the surviving sources wait for a
						// close that is never coming and the report below ends
						// only this goroutine.
						cancel()
						err = errors.Join(err, sourcePanicError(recovered))
					}
					r.report(runnerEvent{kind: runnerEventSourceDone, err: err})
				}()
				return runPipeline(run)
			})
		}
		startSource(func() error { return fetchRunner(r, runCtx, deliveries) })
		startSource(func() error { return runDispatchPipeline(r, runCtx, deliveries, prefetch) })
		startSource(func() error { return consumeRunnerErrors(r, runCtx) })

		// The backlog poll is per generation but not a source: its end must
		// not end the generation, and the generation must not end while it
		// still runs. It runs on its own child context of the generation's
		// run context, so the teardown stops the poll without touching the
		// generation's own cancel: whether an observer is set must not
		// change the runner's state. The teardown cancels that context and
		// waits, without counting the poll in outstanding. The nil path
		// starts no goroutine.
		var pollDone chan struct{}
		pollCtx, pollCancel := context.WithCancel(runCtx)
		if client := r.client; client != nil && client.observer != nil && client.backlogPollInterval > 0 {
			pollDone = make(chan struct{})
			pollConsumer := consumer
			go func() {
				defer close(pollDone)
				defer func() {
					if recovered := recover(); recovered != nil {
						lastResortRunnerLogger(r).Warn("f1 backlog poll panicked",
							"subscription", r.subscription.Name,
							"panic", fmt.Sprint(recovered))
					}
				}()
				pollBacklog(r, pollCtx, pollConsumer)
			}()
		}

		generationErr := owner.waitSources()
		pollCancel()
		if pollDone != nil {
			<-pollDone
		}
		if generationErr == nil {
			generationErr = owner.runErr
		}
		// The lifecycle is the one piece of the generation result that lives
		// outside the owner: a fatal consumer error transitions it from the
		// goroutine that saw the error, so it keeps its own lock and this reads
		// it directly. Everything else the generation decided is on the owner
		// and is read as a field, with no reader window to arrange.
		if r.lifecycle != nil && r.lifecycle.State() == lifecycle.Failed {
			runErr = generationErr
			break
		}
		// The runner's own failure is what a rebuild is asked for. A generation
		// that failed for any other reason - a pipeline or fetcher error, or
		// the cancellation an attempt abandoning this runner produces - is not
		// a cause, and the runner stops for that error instead of asking for a
		// rebuild on its behalf.
		cause := owner.failureCause
		clientReconnecting := r.client.isReconnecting()
		// Three things keep a stopped generation alive. A generation whose
		// connection has already been replaced reopens on the replacement,
		// whether or not an attempt is still running for it. One abandoned by an
		// attempt in flight waits that attempt out and reports its outcome,
		// which is how a fatal rebuild stops every runner it abandoned. And one
		// that failed for its own reason asks for a rebuild.
		if ctx.Err() != nil || owner.draining ||
			(!clientReconnecting && cause == nil && !owner.abandoned && !r.client.claimReplaced(openedEpoch)) {
			runErr = generationErr
			break
		}
		if owner.consumerError && !clientReconnecting {
			if owner.successfulDelivery {
				owner.failedRepairCycles = 0
				owner.repairCycleActive = false
			}
			if owner.repairCycleActive {
				owner.failedRepairCycles++
			}
			owner.repairCycleActive = true
			if owner.failedRepairCycles < 2 {
				if releaseErr := owner.releaseConsumer(context.WithoutCancel(runCtx)); releaseErr == nil {
					repairCause = cause
					continue
				} else {
					cause = errors.Join(cause, releaseErr)
				}
			}
		}
		r.transitionToReconnecting()
		owner.startRebuild(ctx, cause, openedEpoch)
		owner.pumpUntil(func() bool { return owner.rebuilt })
		if reconnectErr := owner.rebuiltErr; reconnectErr != nil {
			if errors.Is(reconnectErr, errRunnerDraining) {
				return nil
			}
			runErr = reconnectErr
			break
		}
	}

	shutdownErr := owner.drain(runCtx)
	// The drain's own failure is published to the runner with the run's, so a
	// Drain caller sees the same teardown failure the Run caller does.
	if shutdownErr != nil {
		owner.runErr = errors.Join(owner.runErr, shutdownErr)
	}
	if runErr == nil {
		runErr = shutdownErr
	} else if shutdownErr != nil {
		runErr = errors.Join(runErr, shutdownErr)
	}
	return runErr
}

// recordRunExit records a stopped runner against its subscription name, so a
// subscription that ended on its own is visible through Client.Health rather
// than only as a drained client with no consumer. It runs as Run returns, so
// it sees the value the caller receives.
//
// Three exits are not subscription failures and stay unrecorded: the caller's
// cancel, a drain, and a client that has begun shutting down. The caller's
// cancel is read from ctx rather than from the result, because a cancel can
// surface as nil or as a context error depending on which branch observed it
// first.
//
// Record ownership decides the rest, and it is compared by owner rather than
// by error: the error here can be joined with a drain error, so it is not the
// value the runner recorded early. A runner that recorded early keeps only the
// entry it still owns, and a runner that never recorded writes one.
func (r *Runner) recordRunExit(ctx context.Context, err error) {
	if err == nil || ctx.Err() != nil {
		return
	}
	r.mu.Lock()
	draining := r.draining
	r.mu.Unlock()
	if draining {
		return
	}
	c := r.client
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lifecycleLocked() != lifecycle.Ready {
		return
	}
	c.recordRunnerExitLocked(r, err)
}

var errRunnerDraining = errors.New("f1: runner is draining")

// beginRunnerGeneration starts a generation: it records the cancel that stops
// its sources and the group that owns them. It resets nothing else. The
// settlement window is deliberately not part of it: the window belongs to the
// drain, and starting a generation is not the drain.
func beginRunnerGeneration(r *Runner, cancel context.CancelFunc) *errgroup.Group {
	r.mu.Lock()
	r.cancel = cancel
	group := new(errgroup.Group)
	r.group = group
	r.mu.Unlock()
	return group
}

// --- Dispatch and scheduling ---

// runDispatchPipeline chooses work from the scheduler and hands it to the pool.
// prefetch is the consumer's total in-flight budget for this generation, the
// value the driver was given, and it is the ordered worker queue depth.
func runDispatchPipeline(r *Runner, ctx context.Context, deliveries <-chan delivery, prefetch int) error {
	scheduler, err := newRunnerScheduler(r)
	if err != nil {
		return err
	}
	pipelineCtx := context.WithoutCancel(ctx)
	// Unordered mode queues at concurrency depth. The loop submits only while
	// the pool reports it can accept work, so a deeper queue would not hold
	// more: it would only let the loop read a prefetch's worth of the
	// scheduler's earlier picks into a first-in, first-out buffer in front of
	// the workers, where the next choice cannot correct them.
	//
	// Ordered mode queues at that same budget. This is not the first-in,
	// first-out buffer the paragraph above refuses, and the difference is
	// where the bound sits. Free in ordered mode reports idle workers, so it
	// stops the loop choosing when every worker has work, not when a count is
	// reached; the queue then holds only items the scheduler already committed
	// to a busy key's worker, in the order it committed them, which is exactly
	// the per-key order the mode promises. It cannot hold more than the pool
	// can be given, and the pool cannot be given more than the driver has
	// handed over, so Submit does not block.
	queueDepth := r.subscription.Concurrency
	if r.subscription.Mode == OrderedByKey {
		queueDepth = prefetch
	}
	pool, err := dispatch.NewPool(pipelineCtx, r.subscription.Concurrency, r.subscription.Mode == OrderedByKey, queueDepth)
	if err != nil {
		return err
	}
	defer pool.Close()

	var limiter *promotionLimiter
	if r.client != nil && r.client.observer != nil {
		limiter = newPromotionLimiter(r)
	}

	var pending *delivery
	pendingLane := ""
	open := true
	for {
		// Choose only what the pool can start now. The scheduler is asked for
		// an item after the pool reports room, so its answer stays current
		// instead of being queued behind earlier picks.
		for pool.Free() {
			item, prom, ok := scheduler.Next()
			if !ok {
				break
			}
			if limiter != nil && prom.LaneID != "" {
				now := r.client.options.clock.Now()
				if event, record := limiter.promotionEvent(prom, item.EnqueuedAt, now); record {
					r.client.observeRecord(event)
				}
			}
			work := item.Value.(*delivery)
			if err := pool.Submit(pipelineCtx, dispatch.Work{
				Key: work.message.Key,
				Run: func(context.Context) { processDelivery(r, ctx, *work) },
			}); err != nil {
				return err
			}
		}
		// A delivery whose lane is full stays here, and is retried after the
		// dispatches above have made room. It is the only reachable
		// ErrLaneFull: the fallback lane can take deliveries several
		// destinations overflow into.
		if pending != nil {
			laneID, queued, err := enqueuePendingDelivery(r, scheduler, pending, pendingLane)
			if err != nil {
				return err
			}
			if queued {
				pending = nil
				pendingLane = ""
				// The dispatch pass above ran before this lane took the item,
				// so a free worker has not been offered it yet. Parking here
				// would wait for a completion that cannot arrive: the item is
				// in a lane, not with a worker, and the only other wake-up is
				// another delivery. One delivery read while the pool is free
				// and no further traffic is a reachable quiescent state (the
				// runner is idle with work in hand), so offer it now.
				continue
			}
			pendingLane = laneID
		}
		if !open && pending == nil && !schedulerHasItems(scheduler) {
			return nil
		}
		// Read more deliveries only while the channel is open and nothing is
		// held back: a held delivery must reach its lane before later ones,
		// and reading past it could wedge a full lane behind its own refill.
		var incoming <-chan delivery
		if open && pending == nil {
			incoming = deliveries
		}
		select {
		case <-pool.FreeSignal():
		case item, ok := <-incoming:
			if !ok {
				open = false
				continue
			}
			// The lane's deadline-promotion budget starts here, when the
			// delivery leaves the fetch channel, and not on the attempt that
			// finally finds room in a full lane: the wait for that room is
			// time the delivery spent queued, and it is what the promotion
			// decision and the reported lane wait measure.
			item.enqueuedAt = r.client.options.clock.Now()
			pending = &item
			pendingLane = ""
		}
	}
}

func enqueuePendingDelivery(r *Runner, scheduler *sched.Scheduler, pending *delivery, pendingLane string) (string, bool, error) {
	laneID := pendingLane
	if laneID == "" {
		laneID = deliveryLane(r, pending.message)
	}
	item := sched.Item{Value: pending, EnqueuedAt: pending.enqueuedAt}
	err := scheduler.Enqueue(laneID, item)
	if err == nil {
		return "", true, nil
	}
	if errors.Is(err, sched.ErrLaneFull) {
		return laneID, false, nil
	}

	fallbackLane := fallbackDeliveryLane(r)
	lastResortRunnerLogger(r).Warn("f1 unknown delivery lane; routing to fallback lane",
		"subscription", r.subscription.Name,
		"lane", laneID,
	)
	err = scheduler.Enqueue(fallbackLane, item)
	if err == nil {
		return "", true, nil
	}
	if errors.Is(err, sched.ErrLaneFull) {
		return fallbackLane, false, nil
	}
	return fallbackLane, false, err
}

func schedulerHasItems(scheduler *sched.Scheduler) bool {
	return scheduler != nil && scheduler.Pending() > 0
}

// runnerLane is one delivery lane: the scheduler's ID for it, the broker
// destination that feeds it, and the spec the scheduler is built from. Topic
// is the logical topic and priority the delivery lane, carried for the
// deadline promotion event without re-deriving them from the lane ID.
type runnerLane struct {
	id          string
	group       string
	destination string
	weight      int
	budget      time.Duration
	capacity    int
	topic       string
	priority    Priority
}

type destinationMetadata struct {
	topic    string
	priority Priority
	tier     int
}

// runnerLanePlan returns the plan the pipeline is built from: the plan the
// runner's consumer open established, or one derived from the client's live
// capabilities for a runner that has not opened yet.
//
// Returning the open's plan is what keeps one computation behind the
// scheduler's lane capacities and the driver's per-destination caps, so a lane
// can never be asked to hold more than the destination feeding it may have
// outstanding. The cached plan also keeps those caps keyed on the destination
// names this consumer was opened with; a second plan derived from capabilities
// that moved since the open could name other destinations.
//
// A lane's capacity is its weighted share of the handler concurrency, times
// the prefetch factor, and never less than minimumLaneCapacity shares. The
// floor is what binds at one and three handler slots, where the share is one
// and two units: a window of two leaves two of three slots waiting a broker
// round trip behind their own acknowledgements instead of finding work in
// hand, which a 5ms handler measures as 2.5ms of stall per message. Three
// covers that round trip and binds only where the share is smaller than it,
// so the shipped concurrency keeps the window it had.
//
// Lane IDs and destination names are different strings: a lane ID is
// topic.priority.main or topic.priority.retry.N, while the destination comes
// from the naming helpers that declare topology. The two are resolved here,
// in one place, so neither caller has to re-derive the other.
func runnerLanePlan(r *Runner) []runnerLane {
	r.mu.Lock()
	opened := r.lanePlan
	r.mu.Unlock()
	if opened != nil {
		return opened
	}
	r.client.mu.Lock()
	effective := r.client.effective
	source := r.client.source
	r.client.mu.Unlock()
	return runnerLanePlanFor(r, effective, source)
}

// runnerLanePlanFor builds the lane plan from capabilities the caller already
// read, so a consumer open sizes its caps from the same read that named its
// destinations.
func runnerLanePlanFor(r *Runner, effective driver.Capabilities, source string) []runnerLane {
	budgets := r.subscription.Fairness.Budgets
	sizing := subscriptionLaneSizing(SubscriptionConfig{
		Topics: r.subscription.Topics, Priorities: r.subscription.Priorities,
		Concurrency: r.subscription.Concurrency, Fairness: r.subscription.Fairness,
		Retry: r.subscription.Retry,
	})
	meta := make(map[string]runnerLane, len(r.subscription.Topics)*len(r.subscription.Priorities)*(1+r.subscription.Retry.tierCount()))
	forEachDestination(effective, source, r.subscription, func(logical string, priority Priority, tier int, destination string) {
		laneID := schedulerLaneID(logical, priority, tier)
		group := laneID
		laneWeight, laneBudget := sizing.mainWeight(priority), budgets[priority]
		if tier > 0 {
			group = schedulerRetryGroupID(logical, priority)
			laneWeight = sizing.retryWeight(priority)
			laneBudget *= retryBudgetMultiplier
		}
		meta[laneID] = runnerLane{
			id: laneID, group: group, destination: destination,
			weight: laneWeight, budget: laneBudget,
			topic: logical, priority: priority,
		}
	})
	ids := make([]string, 0, len(meta))
	for id := range meta {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	plan := make([]runnerLane, 0, len(ids))
	for _, id := range ids {
		lane := meta[id]
		lane.capacity = sizing.capacity(lane.weight)
		plan = append(plan, lane)
	}
	return plan
}

// runnerConsumerPrefetch resolves automatic sizing to the lane-capacity sum.
// A positive configured budget is an admission ceiling; the lane windows may
// impose a smaller effective ceiling, which is reported rather than hidden.
func runnerConsumerPrefetch(r *Runner, configured int, lanes []runnerLane) int {
	total := 0
	for _, lane := range lanes {
		total += lane.capacity
	}
	if total < 1 {
		return configured
	}
	if configured == 0 {
		return total
	}
	if configured > total {
		lastResortRunnerLogger(r).Warn("f1 configured prefetch exceeds the destination windows",
			"subscription", r.subscription.Name,
			"configured", configured,
			"effective", total,
		)
	}
	return min(configured, total)
}

// newRunnerScheduler converts fairness weights into lane capacity and applies
// the retry divisor and prefetch factor without changing the scheduling graph.
func newRunnerScheduler(r *Runner) (*sched.Scheduler, error) {
	lanes := runnerLanePlan(r)
	specs := make([]sched.LaneSpec, 0, len(lanes))
	for _, lane := range lanes {
		specs = append(specs, sched.LaneSpec{
			ID: lane.id, Group: lane.group, Weight: lane.weight,
			Budget: lane.budget, Capacity: lane.capacity,
		})
	}
	return sched.New(specs, r.client.options.clock, !r.subscription.Fairness.DisableDeadlinePromotion)
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
		metadata, known := r.destinationMetadata[message.Destination]
		r.mu.Unlock()
		tier := 0
		if known {
			tier = metadata.tier
			if metadata.priority == envelope.Priority {
				return schedulerLaneID(metadata.topic, metadata.priority, metadata.tier)
			}
		}
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
	return fallbackDeliveryLane(r)
}

func fallbackDeliveryLane(r *Runner) string {
	if len(r.subscription.Topics) > 0 && len(r.subscription.Priorities) > 0 {
		return schedulerLaneID(topicFor(r.subscription.Topics[0]), r.subscription.Priorities[0], 0)
	}
	return ""
}

// consumingTopicFamily resolves the logical topic whose declared destination
// family the delivery came from. Subscription topology is declared from
// Subscription.Topics, so a successor must stay inside the family the
// message was consumed from: WithTopic and fan-out make the envelope's event
// type an unreliable guide to that family. The destination alone decides:
// the priority and event type come from headers, which a producer controls and
// which may not decode at all. Matching reuses the exact naming helpers that
// declare topology, so destination strings are never parsed. The bool is false
// when no configured family owns the destination, letting callers keep their
// historical derivation for deliveries outside every declared family.
func consumingTopicFamily(r *Runner, destination string) (string, bool) {
	r.mu.Lock()
	metadata, known := r.destinationMetadata[destination]
	r.mu.Unlock()
	if known {
		return metadata.topic, true
	}
	r.client.mu.Lock()
	effective := r.client.effective
	source := r.client.source
	r.client.mu.Unlock()
	family, found := "", false
	forEachDestination(effective, source, r.subscription, func(topic string, _ Priority, _ int, name string) {
		if !found && name == destination {
			family, found = topic, true
		}
	})
	return family, found
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
	handlerCancel := r.handlerCancel
	handlerShutdownCancel := r.handlerShutdownCancel
	done := r.done
	cancel := r.cancel
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
		case lifecycle.Draining:
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
	events := r.events
	r.mu.Unlock()
	drainObserved := r.client != nil && r.client.observer != nil
	drainInflight := 0
	var drainTimeoutObserved time.Duration
	var drainGuard observerFinishGuard
	if drainObserved {
		drainTimeoutObserved = r.client.config.Lifecycle.DrainTimeout
		drainInflight = r.inflight.Len()
		start := StartEvent{
			Kind:  ObserverDrain,
			At:    r.client.options.clock.Now(),
			Drain: DrainCounts{InFlight: drainInflight, Timeout: drainTimeoutObserved},
		}
		_, token := r.client.observeStart(ctx, start)
		drainGuard = r.client.newObserverGuard(ObserverDrain, token)
		defer drainGuard.abandon()
	}
	// Tell the owner the runner is ending. The field above is what handler
	// admission reads immediately; this report is what makes the owner compute
	// the settlement window, stop the generation, and run its one drain. The
	// report waits only for the owner to read it, and the owner reads its
	// events between reports rather than inside a broker call, so a request
	// cannot be waiting for an attempt or a consumer operation this call is
	// supposed to end. A request that arrives once the runner is past the point
	// of answering it has nothing to ask for, and the runner's own end releases
	// this wait.
	if events != nil {
		select {
		case events <- runnerEvent{kind: runnerEventDrain}:
		case <-done:
		}
	}
	// The generation is stopped here as well as by the owner, and cancelling
	// twice is what makes the request work for a runner whose owner is not
	// running the loop: a request that arrives at the end of a run has no
	// generation left to stop, and a runner built by a test has no owner at
	// all. The cancel is idempotent, so the second call is free, and a
	// settlement the cancellation unblocks runs on the runner's own window
	// whether the owner has computed the drain's yet or not.
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
		runnerErr := runnerError(r)
		if drainObserved {
			finishDrainObservation(&drainGuard, drainInflight, drainTimeoutObserved, r.inflight.Len(), runnerErr)
		}
		return runnerErr
	case <-ctx.Done():
		ctxErr := ctx.Err()
		if drainObserved {
			finishDrainObservation(&drainGuard, drainInflight, drainTimeoutObserved, r.inflight.Len(), ctxErr)
		}
		return ctxErr
	}
}

// finishDrainObservation finishes one started drain with the counts the finish
// carries. Drained is the start count minus what is still held, floored at
// zero so a delivery accepted between the two reads cannot turn the count
// negative. A nil error finishes ok, else error with its class.
func finishDrainObservation(guard *observerFinishGuard, inflight int, timeout time.Duration, remaining int, err error) {
	drained := max(inflight-remaining, 0)
	counts := DrainCounts{InFlight: inflight, Drained: drained, Remaining: remaining, Timeout: timeout}
	if err == nil {
		guard.finishWith(FinishEvent{Outcome: ObserverOutcomeOK, Drain: counts})
	} else {
		guard.finishWith(FinishEvent{Outcome: ObserverOutcomeError, ErrorClass: errorClassOf(err), Drain: counts})
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

// reportRunnerError hands the owner the first failure a source saw. The owner
// keeps the first one it is handed and never clears it, so the error a runner
// reports through Drain is the first failure of the run and not the last.
func reportRunnerError(r *Runner, err error) {
	if err == nil {
		return
	}
	r.report(runnerEvent{kind: runnerEventError, err: err})
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
		return phaseRaceResult(done, fmt.Errorf("f1: shutdown %s phase timed out: %w", phase, context.DeadlineExceeded))
	case <-parent.Done():
		return phaseRaceResult(done, parent.Err())
	}
}

// phaseRaceResult prefers a phase result that is already available to the
// timeout or cancellation that raced it. A phase that finished must report its
// own result, so a caller that classifies the error cannot mistake a completed
// call for one the budget cut short.
func phaseRaceResult(done <-chan error, raced error) error {
	select {
	case err := <-done:
		return err
	default:
		return raced
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

// runnerSettlementContext returns the context a settlement call runs on.
//
// A live settlement window governs: once the runner has one, every call runs on
// it, and nothing replaces it while it is live. Before that the caller's own
// context governs, which keeps a settlement on a healthy generation bounded by
// the delivery it belongs to.
//
// A call whose own context is already finished is the case that needs the
// runner's window: settling on a context that is already done fails as a
// cancellation the runner caused rather than as a bounded budget doing its job,
// so the call is given the runner's own window instead. That rescue happens
// while the runner is still running, because there is a generation left to
// keep working; a drain does not do it, because a settlement that starts after
// the drain's deadline is refused with that deadline rather than moved onto a
// fresh budget every time it is attempted.
func runnerSettlementContext(r *Runner, fallback context.Context) context.Context {
	r.mu.Lock()
	window := r.settleCtx
	draining := r.draining
	r.mu.Unlock()
	switch {
	case window != nil && window.Err() == nil:
		return window
	case fallback != nil && fallback.Err() == nil:
		return fallback
	}
	if draining && window != nil {
		return window
	}
	r.beginSettlementWindow(fallback)
	r.mu.Lock()
	window = r.settleCtx
	r.mu.Unlock()
	return window
}

// beginSettlementWindow installs a fresh settlement window for the runner.
//
// The window is built over a base whose cancellation has been removed and
// bounded by DrainTimeout, so its own timer is the only thing that ends it and
// the cancellation of whatever context it was derived from cannot reach a
// settlement it is bounding.
//
// A window it replaces is retired, never cancelled: a settlement may be holding
// it, and cancelling it there would turn a call that is about to reach the
// driver into a context failure the runner caused. The retired window keeps its
// own timer, so within one budget period it fires and releases what is left of
// it.
//
// The owner installs the drain's window when the drain starts, so from the
// drain on every settlement runs on the deadline the drain computed once.
// Installations before that are the rescue of a settlement whose own context
// finished first. finishRunner releases the last window's timer at teardown.
func (r *Runner) beginSettlementWindow(base context.Context) { //nolint:contextcheck // the window is deliberately built with WithoutCancel: the base's cancellation must not reach a settlement the window bounds.
	if base == nil {
		base = context.Background()
	}
	drainTimeout := r.client.config.Lifecycle.DrainTimeout
	r.mu.Lock()
	r.settleCtx, r.settleCancel = contextWithOptionalTimeout(context.WithoutCancel(base), drainTimeout)
	r.mu.Unlock()
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

// openRunnerConsumerWith opens the consumer for a generation whose total
// in-flight budget is prefetch, as derived by Run, and returns the connection
// incarnation it opened it on. The connection and the incarnation are one value
// on the client, so the number the caller admits its consumer with later is the
// number that belongs to the connection it actually used.
//
// waitCtx is the runner's own context and genCtx is the generation's. The wait
// for a rebuild ends on genCtx only when the caller cancels the runner: an
// attempt that abandons this runner cancels genCtx to release the topology call
// or consumer open in flight, and a runner waiting for that attempt has to keep
// waiting, because the attempt is rebuilding the connection it is about to open
// on. Its drain is what ends that wait early.
func openRunnerConsumerWith(r *Runner, waitCtx, genCtx context.Context, prefetch int) (driver.Consumer, uint64, error) {
	var conn driver.Conn
	var epoch uint64
	var effective driver.Capabilities
	var source string
	for {
		r.client.mu.Lock()
		if r.client.conn == connReconnecting {
			r.client.mu.Unlock()
			// The runner asks for nothing here: an attempt is already
			// rebuilding the connection, and this runner waits for it before
			// it opens a consumer on an incarnation the swap is about to
			// retire.
			if err := r.client.awaitRebuild(waitCtx, nil, 0, r.drainStarted); err != nil {
				return nil, 0, err
			}
			continue
		}
		if r.client.connStateLocked() == connFailed {
			// The connection was given up and nothing is rebuilding it. The
			// runner needs one to open a consumer, and the decision that gave
			// it up is what it reports instead of opening on a connection the
			// client has already declared broken.
			failed := r.client.reconnectErr
			r.client.mu.Unlock()
			return nil, 0, failed
		}
		current := r.client.current
		conn = current.conn
		epoch = current.epoch
		effective = r.client.effective
		source = r.client.source
		r.client.mu.Unlock()
		break
	}
	policy := r.client.topologyPolicy()
	if conn == nil {
		return nil, 0, errors.New("f1: client is not connected")
	}
	destinations := subscriptionDestinations(effective, source, r.subscription)
	destinationMetadata := buildDestinationMetadataMap(effective, source, r.subscription)
	// The plan is built from the effective and source read with conn, so a swap
	// during EnsureTopology cannot rename the destinations this open declares
	// and caps. It is stored beside the destination metadata so the scheduler
	// and the promotion table read back the plan this consumer was built from
	// instead of deriving a second plan from capabilities that have moved.
	//
	// Each destination's cap is its lane's capacity, so a lane can never hold
	// more than the driver lets that destination have outstanding. Reading the
	// shared delivery channel into lanes then cannot jam behind one full lane
	// while another lane's work waits in the channel.
	lanes := runnerLanePlanFor(r, effective, source)
	perDestination := make(map[string]int, len(lanes))
	for _, lane := range lanes {
		perDestination[lane.destination] = lane.capacity
	}
	r.mu.Lock()
	r.destinationMetadata = destinationMetadata
	r.lanePlan = lanes
	r.mu.Unlock()
	// Every policy goes through the admin, TopologyNone included: under None the
	// driver does no broker work but still records what the spec said, and a
	// consumer's retry delay must not depend on whether the core decided to make
	// the call.
	admin := conn.Admin()
	if admin == nil {
		return nil, 0, errors.New("f1: consumer topology requires driver admin")
	}
	topology := subscriptionTopologySpecs(effective, source, r.subscription)
	topology.Policy = policy
	diff, err := admin.EnsureTopology(genCtx, topology)
	if err != nil {
		return nil, 0, fmt.Errorf("f1: ensure subscription topology: %w", err)
	}
	logTopologyDrift(lastResortRunnerLogger(r), diff)
	consumer, err := conn.Consumer(genCtx, driver.ConsumerConfig{
		Group:          r.subscription.Name,
		Destinations:   destinations,
		Prefetch:       prefetch,
		PerDestination: perDestination,
		Delays:         destinationDelays(topology),
		Exclusive:      r.subscription.Mode == OrderedByKey,
		Effective:      effective,
		StartAt:        driver.StartEarliest,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("f1: create subscription %s: %w", r.subscription.Name, err)
	}
	if consumer == nil {
		return nil, 0, errors.New("f1: driver returned a nil consumer")
	}
	return consumer, epoch, nil
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
			// Classify before logging: a notification is routine lifecycle
			// traffic, not a failure, and must not reach the error stream. A
			// Kafka group join reports every partition assignment this way,
			// and a driver's consumer-cancel notification fires once per
			// timeout interval for a slow handler.
			kind, classified := driver.Classify(err)
			if classified && kind == driver.KindNotification {
				if r.client.options.errorHandler == nil {
					lastResortRunnerLogger(r).Info("f1 consumer notification", "subscription", r.subscription.Name, "error", err)
				}
				runnerNotifyError(r, context.WithoutCancel(ctx), nil, err)
				continue
			}
			if r.client.options.errorHandler == nil {
				lastResortRunnerLogger(r).Error("f1 consumer error", "subscription", r.subscription.Name, "error", err)
			}
			var cancel context.CancelFunc
			switch {
			case !classified || kind == driver.KindTransient:
				r.report(runnerEvent{
					kind:      runnerEventConsumerError,
					err:       err,
					transient: kind == driver.KindTransient,
				})
				r.mu.Lock()
				cancel = r.cancel
				r.mu.Unlock()
			case kind == driver.KindFatal:
				recordFatalConsumerError(r, err)
				r.mu.Lock()
				cancel = r.cancel
				r.mu.Unlock()
			default:
				reportRunnerError(r, err)
			}
			// r.cancel below is about to cancel ctx, so the notification gets its
			// own detached context: the handler's terminalNotificationTimeout
			// budget must not collapse to zero just because the same error that
			// is being reported is also what triggers shutdown.
			runnerNotifyError(r, context.WithoutCancel(ctx), nil, err)
			if classified && kind != driver.KindTransient && kind != driver.KindFatal {
				// A not-found, too-large or permission error is reported without
				// ending the generation, so keep reading: a later fatal error
				// must still fail the subscription.
				continue
			}
			if cancel != nil {
				cancel()
			}
			return nil
		}
	}
}

func recordFatalConsumerError(r *Runner, err error) {
	reportRunnerError(r, err)
	if r.lifecycle != nil {
		if transitionErr := r.lifecycle.Transition(lifecycle.Failed); transitionErr != nil {
			reportRunnerError(r, errors.Join(err, transitionErr))
		}
	}
	r.client.recordFailedSubscription(r.subscription.Name, err, r)
}

// --- Intake and cancellation ---

// fetchRunner reads the consumer and hands deliveries to the pipeline. It is
// also the only closer of the channel it hands them over on, and it closes
// that channel from a deferred function so every way out closes it, a panic
// included: the pipeline reads the channel until it is closed, so a fetch
// source that died without closing would leave the pipeline waiting on a
// channel no one owns any more, and the generation would never end.
func fetchRunner(r *Runner, ctx context.Context, dispatch chan<- delivery) error {
	defer close(dispatch)
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
				return nil
			}
			if !enqueueDelivery(r, ctx, dispatch, message) {
				return fetchRunnerAfterCancel(r, ctx, messages, dispatch)
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
	// This source installs no settlement context. The drain's settlement window
	// is the owner's, computed when the drain starts, and installing one here
	// would put a second deadline under settlements the owner is already
	// bounding.
	r.mu.Lock()
	consumer := r.consumer
	r.mu.Unlock()
	if err := consumer.Drain(drainCtx); err != nil {
		reportRunnerError(r, err)
	}
	for {
		select {
		case message, ok := <-messages:
			if !ok {
				return nil
			}
			if !enqueueDelivery(r, drainCtx, dispatch, message) {
				return nil
			}
		default:
			return nil
		}
	}
}

func runnerDestinationObservation(r *Runner, destination string) (string, Priority) {
	if r == nil {
		return "", 0
	}
	r.mu.Lock()
	metadata, ok := r.destinationMetadata[destination]
	r.mu.Unlock()
	if !ok {
		return "", 0
	}
	return metadata.topic, metadata.priority
}

func enqueueDelivery(r *Runner, ctx context.Context, dispatch chan<- delivery, message driver.InboundMessage) bool {
	id := r.inflight.Add()
	item := delivery{id: id, message: message}
	if r.client != nil && r.client.observer != nil {
		var messageID, correlationID string
		for _, header := range message.Headers {
			switch header.Key {
			case wire.ID:
				messageID = string(header.Value)
			case wire.CorrelationID:
				correlationID = string(header.Value)
			}
		}
		enqueuedAt, enqueuedAtSource := observerEnqueueFields(message)
		topic, priority := runnerDestinationObservation(r, message.Destination)
		r.client.observeRecord(PointEvent{
			Kind:             ObserverDeliveryReceived,
			At:               r.client.options.clock.Now(),
			Topic:            topic,
			Subscription:     r.subscription.Name,
			ConsumerGroup:    r.subscription.Name,
			Priority:         priority,
			Destination:      message.Destination,
			MessageID:        messageID,
			CorrelationID:    correlationID,
			DeliveryCount:    message.DeliveryCount,
			EnqueuedAt:       enqueuedAt,
			EnqueuedAtSource: enqueuedAtSource,
		})

	}
	select {
	case dispatch <- item:
		return true
	case <-ctx.Done():
		// A cancellation path may still have a delivery in the channel. Put it
		// back through the broker rather than losing it locally, with the same
		// bounded cleanup the delivery path uses: the registry entry leaves
		// only once the settlement path finished, so a drain never reads zero
		// while the broker still owns the message.
		state := &deliveryState{id: id}
		_ = nackDelivery(r, runnerSettlementContext(r, ctx), message, driver.NackOptions{Requeue: true}, state)
		retryDeliverySettlement(r, ctx, message, state)
		r.inflight.Remove(id)
		return false
	}
}

func observerEnqueueFields(message driver.InboundMessage) (time.Time, EnqueuedAtSource) {
	if message.EnqueuedAt.IsZero() {
		return time.Time{}, EnqueuedAtUnknown
	}
	return message.EnqueuedAt, observerEnqueueSource(message.EnqueuedAtSource)
}

func observerEnqueueSource(source driver.EnqueueSource) EnqueuedAtSource {
	switch source {
	case driver.EnqueueSourceProducer:
		return EnqueuedAtProducer
	case driver.EnqueueSourceBroker:
		return EnqueuedAtBroker
	default:
		return EnqueuedAtUnknown
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
		// The registry entry leaves only after the delivery's settlement
		// path finished, so a drain neither reads zero while the broker
		// still owns the message nor waits on an entry nothing will
		// remove. An abandoned delivery leaves at the end of the bounded
		// budget.
		r.inflight.Remove(item.id)
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
	// The requeue fallback records a nack in state.operation, so the round's
	// operation is fixed here to keep retrying a failed ack as an ack.
	operation := state.operation
	for round := 0; !state.settled && round < settlementRetryAttempts; round++ {
		sctx := runnerSettlementContext(r, ctx)
		switch operation {
		case settlementOperationAck:
			ackDelivery(r, sctx, message, state)
			if state.settled {
				return
			}
			_ = nackDelivery(r, sctx, message, driver.NackOptions{Requeue: true}, state)
		case settlementOperationNack:
			_ = nackDelivery(r, sctx, message, state.nackOptions, state)
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

func effectiveMaxAttempts(eventMaxAttempts, policyMaxAttempts int) int {
	if eventMaxAttempts > 0 && eventMaxAttempts < policyMaxAttempts {
		return eventMaxAttempts
	}
	return policyMaxAttempts
}

// dispatchMessage returns true only after the delivery's settlement path has
// completed; classification and settlement stay outside middleware. state
// carries the delivery's settlement bookkeeping and is required: it is where
// the decode outcome, the header limit and the pending drop report are
// recorded, so a call site must hold the delivery's own state rather than a
// temporary nothing else sees.
func dispatchMessage(r *Runner, ctx context.Context, message driver.InboundMessage, envelopeOut *Envelope, abandoned *bool, state *deliveryState) bool {
	r.client.mu.Lock()
	headerMaxBytes := effectiveHeaderLimit(r.client.config.Codec.MaxHeaderBytes, r.client.effective.MaxHeaderBytes)
	r.client.mu.Unlock()
	state.headerMaxBytes = headerMaxBytes
	headers := inboundHeaders(message.Headers)
	envelope, err := DecodeHeaders(headers)
	if err != nil {
		return deadLetterAndSettle(r, ctx, message, Envelope{}, ReasonDecode, err, state)
	}
	state.envelopeDecoded = true
	state.envelopePriority = envelope.Priority
	if envelope.Attempt < 1 {
		envelope.Attempt = 1
	}
	eventCodec, err := r.client.codecForContentType(envelope.DataContentType)
	if err != nil {
		return deadLetterAndSettle(r, ctx, message, envelope, ReasonDecode, err, state)
	}
	maxAttempts := effectiveMaxAttempts(envelope.MaxAttempts, r.subscription.Retry.MaxAttempts)
	*envelopeOut = envelope
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
		return discardAndSettle(r, runnerSettlementContext(r, ctx), message, discardUnmatched(envelope, nil), state)
	}
	eventEnvelope := envelope
	eventEnvelope.MaxAttempts = maxAttempts
	event := &Event{envelope: eventEnvelope, raw: append([]byte(nil), message.Body...), codec: eventCodec, headers: headers}
	result := invokeHandlerMessage(r, ctx, handler, event, message)

	if result.processCtx != nil {
		state.processCtx = result.processCtx
	}
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
	switch outcome {
	case retryOutcomeDrop:
		return discardAndSettle(r, runnerSettlementContext(r, ctx), message, Discarded{Envelope: envelope, Reason: DiscardDropped, Err: result.err}, state)
	case retryOutcomeTerminal:
		return deadLetterAndSettle(r, ctx, message, envelope, ReasonTerminal, result.err, state)
	}
	if envelope.Attempt >= maxAttempts {
		return deadLetterAndSettle(r, ctx, message, envelope, ReasonMaxAttempts, result.err, state)
	}
	return retryAndSettle(r, ctx, message, envelope, result.err, state)
}

// retryOutcome names the settlement path a handler error takes: retried
// within the attempt budget, acknowledged without a successor copy, or
// dead-lettered without another attempt.
type retryOutcome uint8

const (
	// retryOutcomeRetry publishes a successor copy so the delivery is
	// attempted again.
	retryOutcomeRetry retryOutcome = iota
	// retryOutcomeDrop acknowledges the delivery without a successor copy.
	retryOutcomeDrop
	// retryOutcomeTerminal dead-letters the delivery without another attempt.
	retryOutcomeTerminal
)

// classifyRetryError maps a handler error to the settlement path it asks
// for: a terminal error is dead-lettered without another attempt, a dropped
// error is acknowledged without a successor copy, and anything else is
// retried.
func classifyRetryError(err error) retryOutcome {
	switch {
	case IsTerminal(err):
		return retryOutcomeTerminal
	case IsDropped(err):
		return retryOutcomeDrop
	}
	return retryOutcomeRetry
}

// processOutcome maps a handler result to the observer finish it asks for:
// ok with no class when neither err nor panic is set, f1_panic for a panic,
// f1_terminal for a terminal error, f1_dropped for a dropped error,
// f1_max_attempts when a retryable error arrives at the last attempt, and
// f1_retryable otherwise. Terminal is true unless the class is f1_retryable.
func processOutcome(result handlerResult, event *Event) (ObserverOutcome, ErrorClass, bool) {
	if result.panic != nil {
		return ObserverOutcomeError, ErrorClassPanic, true
	}
	if result.err == nil {
		return ObserverOutcomeOK, "", true
	}
	if IsTerminal(result.err) {
		return ObserverOutcomeError, ErrorClassTerminal, true
	}
	if IsDropped(result.err) {
		return ObserverOutcomeError, ErrorClassDropped, true
	}
	if event != nil && event.MaxAttempts() > 0 && event.Attempt() >= event.MaxAttempts() {
		return ObserverOutcomeError, ErrorClassMaxAttempts, true
	}
	return ObserverOutcomeError, ErrorClassRetryable, false
}

// finishProcessResult emits the process finish for a handler result that is
// not stuck. A stuck result returns without finishing, so the deferred
// abandon on the guard emits it instead.
func finishProcessResult(guard *observerFinishGuard, base FinishEvent, result handlerResult, event *Event) {
	outcome, class, terminal := processOutcome(result, event)
	finish := base
	finish.Outcome = outcome
	finish.ErrorClass = class
	finish.Terminal = terminal
	guard.finishWith(finish)
}

// deathReasonClass maps a death reason to the bounded error class carried on
// poison rejected events. Anything outside the known reasons is _OTHER.
func deathReasonClass(reason DeathReason) ErrorClass {
	switch reason {
	case ReasonDecode:
		return ErrorClassDecode
	case ReasonPanic:
		return ErrorClassPanic
	case ReasonExpired:
		return ErrorClassExpired
	case ReasonUnmatched:
		return ErrorClassUnmatched
	case ReasonMaxAttempts:
		return ErrorClassMaxAttempts
	case ReasonTerminal:
		return ErrorClassTerminal
	case ReasonPoison:
		return ErrorClassPoison
	default:
		return ErrorClassOther
	}
}

type handlerResult struct {
	err error
	// processCtx carries the context Start of ObserverProcess returned, set
	// only when an observer is set.
	processCtx context.Context
	panic      error
	stuck      bool
}

func invokeHandlerMessage(r *Runner, parent context.Context, handler Handler, event *Event, message driver.InboundMessage) handlerResult {
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
	if !draining && parent.Err() != nil {
		r.mu.Unlock()
		return handlerResult{stuck: true}
	}
	r.mu.Unlock()
	observerCtx := base
	eventID := ""
	if event != nil {
		eventID = event.envelope.ID
	}
	topic := ""
	topicResolved := false
	resolveTopic := func() string {
		if topicResolved {
			return topic
		}
		topicResolved = true
		if event == nil {
			return topic
		}
		// The topic is the family the delivery came from, not the event
		// type: a publish with a topic override lands on another family's
		// destination while keeping its event type. Fall back to the
		// historical derivation outside every declared family.
		topic = topicFor(event.envelope.Type)
		if family, ok := consumingTopicFamily(r, message.Destination); ok {
			topic = family
		}
		return topic
	}
	var processGuard observerFinishGuard
	var processBase FinishEvent
	observed := false
	if r.client != nil && r.client.observer != nil && event != nil && base != nil {
		eventType := event.envelope.Type
		enqueuedAt, enqueuedAtSource := observerEnqueueFields(message)
		priority := event.envelope.Priority
		attempt := event.envelope.Attempt
		messageID := event.envelope.ID
		correlationID := event.envelope.CorrelationID
		if correlationID == "" {
			correlationID = messageID
		}
		resolvedTopic := resolveTopic()
		start := StartEvent{
			Kind:          ObserverProcess,
			At:            r.client.options.clock.Now(),
			Topic:         resolvedTopic,
			Subscription:  r.subscription.Name,
			ConsumerGroup: r.subscription.Name,
			EventType:     eventType,
			Priority:      priority,
			Attempt:       attempt,
			Destination:   message.Destination,

			MessageID:        messageID,
			CorrelationID:    correlationID,
			DeliveryCount:    message.DeliveryCount,
			EnqueuedAt:       enqueuedAt,
			EnqueuedAtSource: enqueuedAtSource,

			TraceParent: event.envelope.TraceParent,
			TraceState:  event.envelope.TraceState,
		}
		nextCtx, token := r.client.observeStart(base, start)
		observerCtx = nextCtx
		processBase = FinishEvent{
			Topic:         resolvedTopic,
			Subscription:  r.subscription.Name,
			ConsumerGroup: r.subscription.Name,
			EventType:     eventType,
			Priority:      priority,
			Attempt:       attempt,
			Destination:   message.Destination,

			MessageID:     messageID,
			CorrelationID: correlationID,
		}
		processGuard = r.client.newObserverGuard(ObserverProcess, token)
		observed = true
		defer processGuard.abandon()
	}
	handlerCtx, cancel := context.WithTimeout(observerCtx, timeout)
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
	// finish records the process finish for a result that is not stuck; a
	// stuck result keeps the deferred abandon on the guard.
	finish := func(result handlerResult) handlerResult {
		if observed && !result.stuck {
			finishProcessResult(&processGuard, processBase, result, event)
			result.processCtx = observerCtx
		}
		return result
	}
	// The first explicit 2x phase warns; only after it completes does the
	// second explicit 2x phase begin, making the terminal threshold cumulative 4x.
	if result, ended := awaitHandlerPhase(r, done, &parentDone, &drainStarted, shutdownDone, stuck.C); ended {
		return finish(result)
	}
	lastResortRunnerLogger(r).LogAttrs(handlerCtx, slog.LevelWarn, "f1 stuck worker",
		slog.String("subscription", r.subscription.Name),
		slog.Duration("threshold", stuckPhaseMultiplier*timeout),
		slog.String("event_id", eventID),
		slog.String("topic", resolveTopic()))
	stackTimer := r.client.options.clock.Timer(timeout * stuckPhaseMultiplier)
	defer stackTimer.Stop()
	if result, ended := awaitHandlerPhase(r, done, &parentDone, &drainStarted, shutdownDone, stackTimer.C); ended {
		return finish(result)
	}
	lastResortRunnerLogger(r).LogAttrs(handlerCtx, slog.LevelError, "f1 worker exceeded stuck threshold",
		slog.String("subscription", r.subscription.Name),
		slog.Duration("threshold", stuckAbortMultiplier*timeout),
		slog.String("event_id", eventID),
		slog.String("topic", resolveTopic()))
	return handlerResult{stuck: true}
}

// awaitHandlerPhase waits inside one stuck phase for a handler that has not
// returned. It returns the handler's result and true when that result or a
// shutdown ends the phase, and false when the phase's own timer fired, which
// makes the caller move to the next phase.
//
// parentDone and drainStarted are pointers because the phase clears each one
// once it has acted on it: both stay selected on every iteration, so a closed
// channel left in place would spin this loop. Shutdown ends the phase as stuck
// and needs no clearing, because it returns.
func awaitHandlerPhase(r *Runner, done <-chan handlerResult, parentDone *<-chan struct{}, drainStarted *chan struct{}, shutdownDone <-chan struct{}, phase <-chan time.Time) (handlerResult, bool) {
	for {
		select {
		case result := <-done:
			return result, true
		case <-*drainStarted:
			*parentDone = nil
			*drainStarted = nil
		case <-*parentDone:
			if runnerIsDraining(r) {
				*parentDone = nil
				continue
			}
			return handlerResult{stuck: true}, true
		case <-shutdownDone:
			return handlerResult{stuck: true}, true
		case <-phase:
			return handlerResult{}, false
		}
	}
}

func runnerIsDraining(r *Runner) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.draining
}

// --- Settlement primitives ---

func discardAndSettle(r *Runner, ctx context.Context, message driver.InboundMessage, payload Discarded, state *deliveryState) bool {
	if r.subscription.OnDiscarded != nil {
		payload.Body = append([]byte(nil), message.Body...)
		state.discard = payload
		state.discardPending = true
	}
	return ackDelivery(r, ctx, message, state)
}

// ackDelivery acknowledges the delivery, recording the outcome on state. state
// is required: it is the delivery's own settlement bookkeeping, and the
// deferred cleanup reads the operation it records here.
func ackDelivery(r *Runner, ctx context.Context, message driver.InboundMessage, state *deliveryState) bool {
	return ackDeliveryAs(r, ctx, message, true, state)
}

// startSettleObservation starts one settle pair for a single broker settle
// call. It returns the guard for its finish, the finish identity, and whether
// observation is on. A nil runner or a nil observer starts nothing.
func startSettleObservation(r *Runner, ctx context.Context, message driver.InboundMessage, op SettleOperation, state *deliveryState) (observerFinishGuard, FinishEvent, bool) {
	if r == nil || r.client == nil || r.client.observer == nil {
		return observerFinishGuard{}, FinishEvent{}, false
	}
	topic, priority := runnerDestinationObservation(r, message.Destination)
	start := StartEvent{
		Kind:          ObserverSettle,
		At:            r.client.options.clock.Now(),
		Topic:         topic,
		Subscription:  r.subscription.Name,
		ConsumerGroup: r.subscription.Name,
		Priority:      priority,
		Destination:   message.Destination,
		Operation:     op,
	}
	_, token := r.client.observeStart(ctx, start)
	guard := r.client.newObserverGuard(ObserverSettle, token)
	base := FinishEvent{
		Topic:         topic,
		Subscription:  r.subscription.Name,
		ConsumerGroup: r.subscription.Name,
		Priority:      priority,
		Destination:   message.Destination,
	}
	return guard, base, true
}

// finishSettleObservation emits the settle finish for one broker settle call.
func finishSettleObservation(guard *observerFinishGuard, base FinishEvent, err error) {
	finish := base
	if err == nil {
		finish.Outcome = ObserverOutcomeOK
	} else {
		finish.Outcome = ObserverOutcomeError
		finish.ErrorClass = errorClassOf(err)
	}
	guard.finishWith(finish)
}

// ackDeliveryAs acknowledges the delivery. handled records whether the ack
// ends a handled delivery rather than a dead-lettered or retried one; only a
// handled ack counts the generation's successfulDelivery, which the
// reconnect decision reads. state is the delivery's own settlement
// bookkeeping and is required.
func ackDeliveryAs(r *Runner, ctx context.Context, message driver.InboundMessage, handled bool, state *deliveryState) bool {
	if message.Settle == nil {
		return false
	}
	state.operation = settlementOperationAck
	settleGuard, settleBase, observed := startSettleObservation(r, ctx, message, SettleAck, state)
	if observed {
		defer settleGuard.abandon()
	}
	err := message.Settle.Ack(ctx)
	state.attempted = true
	state.settled = err == nil
	if observed {
		finishSettleObservation(&settleGuard, settleBase, err)
	}
	if r != nil && state.settled && handled {
		r.report(runnerEvent{kind: runnerEventHandledDelivery})
	}
	if state.settled {
		reportPendingAfterAck(r, ctx, message, state)
	}
	return state.settled
}

// nackDelivery negatively acknowledges the delivery, recording the operation
// and its options on state so a deferred retry repeats exactly this call. state
// is required.
func nackDelivery(r *Runner, ctx context.Context, message driver.InboundMessage, options driver.NackOptions, state *deliveryState) error {
	if message.Settle == nil {
		return errors.New("f1: delivered message has no settler")
	}
	state.operation = settlementOperationNack
	state.nackOptions = options
	settleGuard, settleBase, observed := startSettleObservation(r, ctx, message, SettleNack, state)
	if observed {
		defer settleGuard.abandon()
	}
	err := message.Settle.Nack(ctx, options)
	state.attempted = true
	state.settled = err == nil
	if observed {
		finishSettleObservation(&settleGuard, settleBase, err)
	}
	if state.settled {
		state.poisonDrop = nil
		state.discard = Discarded{}
		state.discardPending = false
	}
	return err
}

// successorPoint returns the shared identity for consume-side point events.
// Callers set only their own fields. topic is the delivery's resolved successor
// topic, passed in so a failed delivery resolves it once rather than per event.
// Call it only behind the observer check; it must not run on the nil path.
func successorPoint(r *Runner, kind ObserverKind, message driver.InboundMessage, envelope Envelope, topic string) PointEvent {
	enqueuedAt, enqueuedAtSource := observerEnqueueFields(message)

	return PointEvent{
		Kind:             kind,
		At:               r.client.options.clock.Now(),
		Topic:            topic,
		Subscription:     r.subscription.Name,
		EventType:        envelope.Type,
		Priority:         envelope.Priority,
		Attempt:          successorAttempt(envelope),
		Destination:      message.Destination,
		MessageID:        envelope.ID,
		CorrelationID:    successorCorrelationID(envelope),
		DeliveryCount:    message.DeliveryCount,
		EnqueuedAt:       enqueuedAt,
		EnqueuedAtSource: enqueuedAtSource,
	}
}

func deadLetterAndSettle(r *Runner, ctx context.Context, message driver.InboundMessage, envelope Envelope, reason DeathReason, lastErr error, state *deliveryState) bool {
	// One resolution for every successor decision this delivery makes: the
	// decided and failed events, the dead-letter destination and the copy the
	// dead-letter publish names.
	topic := resolveDeliveryTopic(r, envelope, message)
	if r.client != nil && r.client.observer != nil {
		destination := deadLetterDestination(r, topic)
		event := successorPoint(r, ObserverDeadLetterDecided, message, envelope, topic)
		event.Reason = reason
		event.DeadLetterDestination = destination
		r.client.observeRecord(event)
	}
	if err := deadLetter(r, ctx, message, envelope, reason, lastErr, topic, state); err != nil {
		if r.client != nil && r.client.observer != nil {
			destination := deadLetterDestination(r, topic)
			event := successorPoint(r, ObserverDeadLetterFailed, message, envelope, topic)
			event.ErrorClass = errorClassOf(err)
			event.Reason = reason
			event.DeadLetterDestination = destination
			r.client.observeRecord(event)
		}
		switch {
		case reason == ReasonPoison && isMissingDeadLetterRoute(err):
			state.poisonDrop = &poisonDropReport{
				envelope:    envelope,
				topic:       topic,
				reason:      reason,
				headline:    "poison message dropped",
				description: "no dead-letter route available",
				logMessage:  "no dead-letter route",
				cause:       err,
			}
		case successorNeverPublishable(err):
			dropped := successorDropDescription(reason, err)
			state.poisonDrop = &poisonDropReport{
				envelope:    envelope,
				topic:       topic,
				reason:      reason,
				headline:    "message dropped",
				description: dropped,
				logMessage:  dropped,
				cause:       err,
			}
		default:
			failSuccessorHandoff(r, runnerSettlementContext(r, ctx), "dead_letter", eventFromDelivery(r, message, envelope), err)
			return false
		}
		return ackDeliveryAs(r, runnerSettlementContext(r, ctx), message, true, state)
	}
	return ackDeliveryAs(r, runnerSettlementContext(r, ctx), message, false, state)
}

func isMissingDeadLetterRoute(err error) bool {
	if errors.Is(err, driver.ErrDestinationMissing) {
		return true
	}
	kind, classified := driver.Classify(err)
	return classified && kind == driver.KindNotFound
}

// errSuccessorCopyUnencodable marks a successor copy this process could not
// encode at all, as opposed to one a broker refused. It is unexported because
// only the drop decision in this file reads it; the encoding error it wraps
// keeps ErrEnvelopeTooLarge reachable through errors.Is.
var errSuccessorCopyUnencodable = errors.New("f1: successor copy cannot be encoded")

// successorNeverPublishable reports whether err says the copy itself will never
// be accepted, rather than that its destination is temporarily unable to take
// it: the broker refused the copy as too large, or the copy could not be
// encoded at all. Either repeats on every redelivery and after every restart,
// so such a delivery is settled instead of stopping the subscription.
func successorNeverPublishable(err error) bool {
	if errors.Is(err, errSuccessorCopyUnencodable) {
		return true
	}
	kind, classified := driver.Classify(err)
	return classified && kind == driver.KindTooLarge
}

// successorDropDescription names, for the drop report, why a successor that can
// never be published was dropped. It fills the slot the missing-route drop
// fills with the route it could not find.
func successorDropDescription(reason DeathReason, err error) string {
	shape := "dead-letter copy exceeds the broker limit"
	if errors.Is(err, errSuccessorCopyUnencodable) {
		shape = "dead-letter copy cannot be encoded"
	}
	return fmt.Sprintf("%s (death reason %s)", shape, reason)
}

func reportPoisonDrop(r *Runner, ctx context.Context, message driver.InboundMessage, report poisonDropReport) {
	if r.client != nil && r.client.observer != nil {
		event := successorPoint(r, ObserverPoisonRejected, message, report.envelope, report.topic)
		event.ErrorClass = deathReasonClass(report.reason)
		event.Reason = report.reason
		r.client.observeRecord(event)
	}
	dropErr := fmt.Errorf("f1: %s: event_id=%q destination=%q attempt=%d: %s: %w", report.headline, report.envelope.ID, message.Destination, report.envelope.Attempt, report.description, report.cause)
	if r.client.options.errorHandler == nil {
		lastResortRunnerLogger(r).LogAttrs(ctx, slog.LevelError, "f1 "+report.headline+"; "+report.logMessage,
			slog.String("event_id", report.envelope.ID),
			slog.String("destination", message.Destination),
			slog.Int("attempt", report.envelope.Attempt),
			slog.String("reason", report.reason.String()),
			slog.Any("error", report.cause))
		return
	}
	runnerNotifyError(r, ctx, eventFromDelivery(r, message, report.envelope), dropErr)
}

func reportPendingAfterAck(r *Runner, ctx context.Context, message driver.InboundMessage, state *deliveryState) {
	if state == nil {
		return
	}
	poison := state.poisonDrop
	discard, discardPending := state.discard, state.discardPending
	// Consume intent before scheduling callbacks; subsequent settlement cleanup
	// must not report the same delivery a second time.
	state.poisonDrop = nil
	state.discard = Discarded{}
	state.discardPending = false
	if poison != nil {
		reportPoisonDrop(r, ctx, message, *poison)
	}
	if discardPending {
		runnerNotifyDiscarded(r, ctx, discard)
	}
}

// deadLetter republishes message to its dead-letter destination, carrying
// the already-received body forward unchanged. topic is the delivery's
// resolved successor topic, so this path never derives it again. It
// deliberately does not apply codec.maxBodyBytes: that limit is a
// caller-facing guardrail enforced only on a publish the application
// originated (publisher.go). A body reaching this function was already
// accepted onto the broker once; rejecting the same bytes here on their way to
// the dead-letter destination would turn a successful delivery into a silent
// message-loss path instead of the visible one dead-lettering exists to
// provide.
func deadLetter(r *Runner, ctx context.Context, message driver.InboundMessage, envelope Envelope, reason DeathReason, lastErr error, topic string, state *deliveryState) error {
	headerMaxBytes := 0
	if state != nil {
		headerMaxBytes = state.headerMaxBytes
	}
	if reason == ReasonDecode && envelope.ID == "" {
		// Preserve the raw headers when the envelope itself could not be decoded.
		headers := inboundHeaders(message.Headers)
		for key := range headers {
			if strings.HasPrefix(key, wire.DetailPrefix) {
				delete(headers, key)
			}
		}
		destination := deadLetterDestination(r, topic)
		// The raw-header branch carries headers as received, so it gets the
		// pair but no injection.
		_, publishGuard, publishBase, observed := startSuccessorPublish(r, ctx, PublishRouteDeadLetter, topic, destination, envelope.ID, successorCorrelationID(envelope), envelope.Type, envelope.Priority, successorAttempt(envelope), state)
		if observed {
			defer publishGuard.abandon()
		}
		setDeathHeaders(headers, reason, lastErr, r.client.options.clock.Now().UTC(), message.Destination)
		if err := publishSuccessor(r, runnerSettlementContext(r, ctx), driver.OutboundMessage{Destination: destination, Key: append([]byte(nil), message.Key...), Headers: headerSlice(headers), Body: append([]byte(nil), message.Body...)}); err != nil {
			if observed {
				finishSuccessorPublish(&publishGuard, publishBase, err)
			}
			return err
		}
		if observed {
			finishSuccessorPublish(&publishGuard, publishBase, nil)
			observeDeadLetterPublished(r, message, envelope, reason, destination, topic)
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
			lastResortRunnerLogger(r).LogAttrs(ctx, slog.LevelWarn, "f1 death details discarded",
				slog.String("subscription", r.subscription.Name),
				slog.String("event_id", envelope.ID),
				slog.Any("keys", logged),
				slog.Int("count", len(discarded)))
		}
	} else {
		death.DeathDetails = nil
	}
	now := r.client.options.clock.Now().UTC()
	death.DeathTime = &now
	destination := deadLetterDestination(r, topic)
	messageID := death.ID
	correlationID := successorCorrelationID(death)
	inboundAttempt := successorAttempt(envelope)
	injectCtx, publishGuard, publishBase, observed := startSuccessorPublish(r, ctx, PublishRouteDeadLetter, topic, destination, messageID, correlationID, death.Type, death.Priority, inboundAttempt, state)
	if observed {
		defer publishGuard.abandon()
	}
	if r.client != nil && r.client.traceInjector != nil && observed {
		traceParent, traceState := r.client.injectTrace(injectCtx)
		if traceParent != "" {
			death.TraceParent = traceParent
			death.TraceState = traceState
		}
	}
	encoded, err := death.EncodeHeaders(headerMaxBytes)
	if err != nil {
		if observed {
			finishSuccessorPublish(&publishGuard, publishBase, err)
		}
		// Marked rather than returned bare: the caller decides what to do with
		// an unencodable copy, and it must not have to match on the text to
		// tell this apart from a destination refusing the copy.
		return fmt.Errorf("%w: %w", errSuccessorCopyUnencodable, err)
	}
	out := driver.OutboundMessage{Destination: destination, Key: append([]byte(nil), message.Key...), Body: append([]byte(nil), message.Body...)}
	for key, value := range encoded {
		out.Headers = append(out.Headers, driver.Header{Key: key, Value: []byte(value)})
	}
	sort.Slice(out.Headers, func(i, j int) bool { return out.Headers[i].Key < out.Headers[j].Key })
	if err := publishSuccessor(r, runnerSettlementContext(r, ctx), out); err != nil {
		if observed {
			finishSuccessorPublish(&publishGuard, publishBase, err)
		}
		return err
	}
	if observed {
		finishSuccessorPublish(&publishGuard, publishBase, nil)
		observeDeadLetterPublished(r, message, envelope, reason, destination, topic)
	}
	runnerNotifyDeadLetter(r, runnerSettlementContext(r, ctx), DeadLettered{Envelope: death, Body: append([]byte(nil), message.Body...), Reason: reason, Attempt: death.Attempt, LastErr: lastErr, Destination: destination})
	return nil
}

// observeDeadLetterPublished emits dead letter published on a confirmed
// successor publish. topic is the delivery's resolved successor topic. Callers
// check for nil before building the event.
func observeDeadLetterPublished(r *Runner, message driver.InboundMessage, envelope Envelope, reason DeathReason, destination, topic string) {
	if r == nil || r.client == nil || r.client.observer == nil {
		return
	}
	event := successorPoint(r, ObserverDeadLetterPublished, message, envelope, topic)
	event.Reason = reason
	event.DeadLetterDestination = destination
	r.client.observeRecord(event)
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
		err := publishMessages(r.client, ctx, messages...)
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
	eventEnvelope := envelope
	eventEnvelope.MaxAttempts = effectiveMaxAttempts(envelope.MaxAttempts, r.subscription.Retry.MaxAttempts)
	return &Event{envelope: eventEnvelope, raw: append([]byte(nil), message.Body...), codec: eventCodec, headers: inboundHeaders(message.Headers)}
}

// successorHandoffError reports a successor copy the core could not publish
// within its bounded republish budget. It is the core's own failure, not a
// driver operation: op names the core operation ("retry" or "dead_letter"), and
// the error claims no driver op that no driver performed. The cause stays
// reachable through errors.Is, errors.As and driver.Classify, so a classified
// cause keeps its kind and an unclassified one is reported as unclassified
// instead of being presented as a classified transient driver failure.
type successorHandoffError struct {
	op    string
	cause error
}

// Error returns the handoff failure in one line.
func (e *successorHandoffError) Error() string {
	return fmt.Sprintf("f1: %s successor handoff failed: %v", e.op, e.cause)
}

// Unwrap returns the cause the handoff carried. It is what keeps the cause's
// own classification readable on this error.
func (e *successorHandoffError) Unwrap() error {
	return e.cause
}

// failSuccessorHandoff runs once a retry or dead-letter successor could not
// be published within its bounded republish budget. The consume-side
// ordering invariant only allows releasing the original delivery once every
// possible successor is confirmed durable, so this never settles the
// delivery: it releases the runner's consumer instead. Release leaves the
// delivery unsettled, closes the consumer, and lets the broker redeliver the
// message, the same mechanism relied on for a crash.
func failSuccessorHandoff(r *Runner, ctx context.Context, op string, event *Event, cause error) {
	failed := &successorHandoffError{op: op, cause: cause}
	reportRunnerError(r, failed)
	runnerNotifyError(r, ctx, event, failed)
	if err := releaseRunnerConsumer(r, ctx); err != nil {
		if r.client.options.errorHandler == nil {
			lastResortRunnerLogger(r).LogAttrs(ctx, slog.LevelError, "f1 failed to release consumer after a successor publish exhausted its republish budget",
				slog.String("op", op),
				slog.Any("cause", failed),
				slog.Any("release_error", err))
		}
		// A released consumer closes its stream, which ends the generation.
		// This one is still open and still holds the delivery, so the
		// generation is ended here instead: the runner stops with the hand-off
		// failure it would have stopped with, and the client keeps the consumer
		// to release again.
		r.mu.Lock()
		cancel := r.cancel
		r.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return
	}
	if r.client.options.errorHandler == nil {
		lastResortRunnerLogger(r).LogAttrs(ctx, slog.LevelError, "f1 successor publish exhausted its republish budget; consumer released",
			slog.String("op", op),
			slog.Any("cause", failed))
	}
}

// successorObserveCtx returns the context the successor publish start uses:
// the handed-off process context when set, else the context at hand. Callers
// pass the result to observeStart only; settlement, the error handler and the
// dead-letter hook keep the original context.
func successorObserveCtx(ctx context.Context, state *deliveryState) context.Context {
	if state != nil && state.processCtx != nil {
		return state.processCtx
	}
	return ctx
}

// resolveDeliveryTopic returns the logical topic a delivery's successors belong
// to: the family the delivery was consumed from, which the declared
// destinations own, so a copy stays inside the family the message came from.
// Outside every declared family it falls back to the derivation from the event
// type. Resolving it reads the runner's destination table and, on a miss, the
// client's capabilities plus a naming loop per configured topic, so a failed
// delivery resolves it once and hands the result to every successor decision
// instead of paying for it once per decision.
func resolveDeliveryTopic(r *Runner, envelope Envelope, message driver.InboundMessage) string {
	if family, ok := consumingTopicFamily(r, message.Destination); ok {
		return family
	}
	return topicFor(envelope.Type)
}

// successorCorrelationID returns the correlation ID for successor events,
// falling back to the message ID like the process start does.
func successorCorrelationID(envelope Envelope) string {
	if envelope.CorrelationID != "" {
		return envelope.CorrelationID
	}
	return envelope.ID
}

// successorAttempt normalizes the inbound attempt for successor events.
func successorAttempt(envelope Envelope) int {
	if envelope.Attempt < 1 {
		return 1
	}
	return envelope.Attempt
}

// startSuccessorPublish starts one publish pair for a successor handoff. It
// returns the injector context, the guard for its finish, the finish identity
// and whether observation is on. A nil observer starts nothing.
func startSuccessorPublish(r *Runner, ctx context.Context, route PublishRoute, topic, destination, messageID, correlationID, eventType string, priority Priority, attempt int, state *deliveryState) (context.Context, observerFinishGuard, FinishEvent, bool) {
	if r == nil || r.client == nil || r.client.observer == nil {
		return ctx, observerFinishGuard{}, FinishEvent{}, false
	}
	start := StartEvent{
		Kind:          ObserverPublish,
		At:            r.client.options.clock.Now(),
		Topic:         topic,
		EventType:     eventType,
		Priority:      priority,
		Attempt:       attempt,
		Destination:   destination,
		MessageID:     messageID,
		CorrelationID: correlationID,
		Route:         route,
		BatchSize:     1,
	}
	observeCtx := successorObserveCtx(ctx, state)
	nextCtx, token := r.client.observeStart(observeCtx, start)
	guard := r.client.newObserverGuard(ObserverPublish, token)
	base := FinishEvent{
		Topic:         topic,
		EventType:     eventType,
		Priority:      priority,
		Attempt:       attempt,
		Destination:   destination,
		MessageID:     messageID,
		CorrelationID: correlationID,
	}
	return nextCtx, guard, base, true
}

// finishSuccessorPublish emits the publish finish for one successor handoff.
func finishSuccessorPublish(guard *observerFinishGuard, base FinishEvent, err error) {
	finish := base
	if err == nil {
		finish.Outcome = ObserverOutcomeOK
	} else {
		finish.Outcome = ObserverOutcomeError
		finish.ErrorClass = errorClassOf(err)
	}
	guard.finishWith(finish)
}

// retryAndSettle republishes message to its retry destination, carrying the
// already-received body forward unchanged. Like deadLetter, it deliberately
// does not apply codec.maxBodyBytes: that limit only guards a publish the
// application originated, and this body was already accepted once. A retry
// copy that cannot be encoded, or that the broker refuses as too large, is
// handed to the dead-letter path rather than settled: a copy the message
// itself makes unacceptable fails the same way on every redelivery.
func retryAndSettle(r *Runner, ctx context.Context, message driver.InboundMessage, envelope Envelope, lastErr error, state *deliveryState) bool {
	copyEnvelope := envelope
	if copyEnvelope.OriginalDest == "" {
		copyEnvelope.OriginalDest = message.Destination
	}
	copyEnvelope.DeathError = ""
	copyEnvelope.DeathReason = ReasonUnspecified
	copyEnvelope.DeathTime = nil
	copyEnvelope.DeathDetails = nil
	copyEnvelope.Attempt++
	copyEnvelope.MaxAttempts = effectiveMaxAttempts(copyEnvelope.MaxAttempts, r.subscription.Retry.MaxAttempts)
	retryConfig := r.subscription.Retry.ladder()
	tiers := retryConfig.TierCount()
	if tiers == 0 {
		return deadLetterAndSettle(r, ctx, message, envelope, ReasonMaxAttempts, lastErr, state)
	}
	tier := retry.ResolveTier(retryConfig, envelope.Attempt)
	delay := retryConfig.DelayFor(tier)
	now := r.client.options.clock.Now().UTC()
	due := now.Add(delay)
	copyEnvelope.DueTime = &due
	// The destination is computed once whether or not an observer is set, so
	// the nil path adds no allocation and the start carries the same value
	// the copy is published to.
	topic := resolveDeliveryTopic(r, envelope, message)
	destination := retryDestination(r, topic, copyEnvelope.Priority, tier)
	messageID := copyEnvelope.ID
	correlationID := successorCorrelationID(copyEnvelope)
	inboundAttempt := successorAttempt(envelope)
	injectCtx, publishGuard, publishBase, observed := startSuccessorPublish(r, ctx, PublishRouteRetry, topic, destination, messageID, correlationID, copyEnvelope.Type, copyEnvelope.Priority, inboundAttempt, state)
	if observed {
		defer publishGuard.abandon()
	}
	if r.client != nil && r.client.traceInjector != nil && observed {
		traceParent, traceState := r.client.injectTrace(injectCtx)
		if traceParent != "" {
			copyEnvelope.TraceParent = traceParent
			copyEnvelope.TraceState = traceState
		}
	}
	encoded, err := copyEnvelope.EncodeHeaders(state.headerMaxBytes)
	if err != nil {
		if observed {
			finishSuccessorPublish(&publishGuard, publishBase, err)
		}
		return deadLetterAndSettle(r, ctx, message, envelope, ReasonTerminal,
			errors.Join(lastErr, fmt.Errorf("f1: retry copy cannot be encoded: %w", err)), state)
	}
	out := driver.OutboundMessage{Destination: destination, Key: append([]byte(nil), message.Key...), Body: append([]byte(nil), message.Body...)}
	for key, value := range encoded {
		out.Headers = append(out.Headers, driver.Header{Key: key, Value: []byte(value)})
	}
	sort.Slice(out.Headers, func(i, j int) bool { return out.Headers[i].Key < out.Headers[j].Key })
	if err := publishSuccessor(r, runnerSettlementContext(r, ctx), out); err != nil {
		if observed {
			finishSuccessorPublish(&publishGuard, publishBase, err)
		}
		if successorNeverPublishable(err) {
			// The copy is unacceptable on its own terms, so it would be refused
			// again on every redelivery: hand the message to the dead-letter
			// path instead of stopping the subscription over it.
			return deadLetterAndSettle(r, ctx, message, envelope, ReasonTerminal,
				errors.Join(lastErr, fmt.Errorf("f1: retry copy cannot be published: %w", err)), state)
		}
		failSuccessorHandoff(r, runnerSettlementContext(r, ctx), "retry", eventFromDelivery(r, message, envelope), err)
		return false
	}
	if observed {
		finishSuccessorPublish(&publishGuard, publishBase, nil)
		event := successorPoint(r, ObserverRetryScheduled, message, envelope, topic)
		event.ErrorClass = ErrorClassRetryable
		event.NextAttempt = copyEnvelope.Attempt
		event.MaxAttempts = copyEnvelope.MaxAttempts
		event.Backoff = delay
		r.client.observeRecord(event)
	}
	return ackDeliveryAs(r, runnerSettlementContext(r, ctx), message, false, state)
}

// A Stop refusal for outstanding messages is a redelivery case, not a retry
// case: Release gives those deliveries back to the broker. Every other Stop
// failure releases too, because a driver whose Stop did not finish keeps the
// consumer registered until it receives Release, and nothing calls it after
// this terminal drain. A Stop timeout or cancellation releases on a fresh,
// bounded context. Join the Stop error with any Release error so teardown
// failures reach the caller.
func stopRunnerConsumer(r *Runner, ctx context.Context) error {
	r.mu.Lock()
	consumer := r.consumer
	epoch := r.consumerEpoch
	r.mu.Unlock()
	if consumer == nil {
		return nil
	}
	var phaseClock clock.Clock
	closeTimeout := time.Duration(0)
	if r.client != nil {
		phaseClock = r.client.options.clock
		closeTimeout = r.client.config.Lifecycle.CloseTimeout
	}
	stopErr := runWithClockTimeout(ctx, phaseClock, closeTimeout, "close", consumer.Stop)
	if stopErr == nil {
		return nil
	}
	releaseCtx := ctx
	if errors.Is(stopErr, driver.ErrDrainTimeout) ||
		errors.Is(stopErr, context.DeadlineExceeded) ||
		errors.Is(stopErr, context.Canceled) {
		releaseCtx = context.WithoutCancel(ctx)
	}
	releaseErr := runWithClockTimeout(releaseCtx, phaseClock, closeTimeout, "close", consumer.Release)
	if releaseErr != nil && !errors.Is(releaseErr, driver.ErrUnsupported) && r.client != nil {
		// The driver still has this consumer registered, so the connection it
		// was opened on refuses to close while it is. Keep it, with that
		// connection, for the release a later close owes it.
		r.client.keepUnreleasedConsumer(consumer, epoch)
	}
	return errors.Join(stopErr, releaseErr)
}

// releaseUnadmittedConsumer releases a consumer that never became the
// runner's, bounded by Lifecycle.CloseTimeout. Like releaseRunnerConsumer, a
// release that fails keeps the consumer on the client with the epoch it was
// opened on, because the connection carrying it refuses to close until a
// later release succeeds.
func releaseUnadmittedConsumer(r *Runner, ctx context.Context, consumer driver.Consumer, epoch uint64) error {
	err := runWithClockTimeout(ctx, r.client.options.clock, r.client.config.Lifecycle.CloseTimeout, "consumer release", consumer.Release)
	if err != nil && !errors.Is(err, driver.ErrUnsupported) {
		r.client.keepUnreleasedConsumer(consumer, epoch)
	}
	return err
}

// releaseRunnerConsumer releases the runner's consumer, bounded by
// Lifecycle.CloseTimeout the way stopRunnerConsumer is. It runs on the
// reconnect and lane-repair paths, which bound the work they own by their own
// context, so a driver Release that ignores that context must not hold a
// reconnect open behind it.
//
// A release that fails keeps the consumer on the client with the epoch it was
// opened on: the driver still has it registered, so the connection carrying it
// refuses to close until a later release succeeds.
func releaseRunnerConsumer(r *Runner, ctx context.Context) error {
	r.mu.Lock()
	consumer := r.consumer
	epoch := r.consumerEpoch
	r.mu.Unlock()
	if consumer == nil {
		return nil
	}
	var phaseClock clock.Clock
	closeTimeout := time.Duration(0)
	if r.client != nil {
		phaseClock = r.client.options.clock
		closeTimeout = r.client.config.Lifecycle.CloseTimeout
	}
	err := runWithClockTimeout(ctx, phaseClock, closeTimeout, "close", consumer.Release)
	if err != nil && !errors.Is(err, driver.ErrUnsupported) {
		if r.client != nil {
			r.client.keepUnreleasedConsumer(consumer, epoch)
		}
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
	headers[wire.DeathReason] = reason.String()
	headers[wire.DeathError] = truncateError(err)
	headers[wire.DeathTime] = at.Format(time.RFC3339Nano)
	headers[wire.OriginalDest] = original
}

// truncateError returns error text capped at deathErrorCap bytes. If the cap falls inside
// a UTF-8 rune, it backs up to the preceding rune boundary. The result may therefore be
// shorter than the cap.
func truncateError(err error) string {
	if err == nil {
		return ""
	}
	value := err.Error()
	if len(value) > deathErrorCap {
		end := deathErrorCap
		for end > 0 && !utf8.RuneStart(value[end]) {
			end--
		}
		return value[:end]
	}
	return value
}

func buildDestinationMetadataMap(effective driver.Capabilities, source string, sub Subscription) map[string]destinationMetadata {
	result := make(map[string]destinationMetadata, len(sub.Topics)*len(sub.Priorities)*(1+sub.Retry.tierCount()))
	forEachDestination(effective, source, sub, func(logical string, priority Priority, tier int, destination string) {
		result[destination] = destinationMetadata{topic: logical, priority: priority, tier: tier}
	})
	return result
}

// forEachDestination calls fn once for every destination sub consumes from:
// for each topic and priority, the main destination as tier 0 and then each
// retry tier's destination. It is the one walk of that set, so the lane plan,
// the destination metadata and the consumer's destination list cannot drift.
func forEachDestination(effective driver.Capabilities, source string, sub Subscription, fn func(topic string, priority Priority, tier int, destination string)) {
	for _, topic := range sub.Topics {
		logical := topicFor(topic)
		for _, priority := range sub.Priorities {
			fn(logical, priority, 0, consumeDestination(effective, source, logical, priority, sub.Name))
			for tier := 1; tier <= sub.Retry.tierCount(); tier++ {
				fn(logical, priority, tier, retryDestinationFor(source, logical, priority, tier, sub.Name))
			}
		}
	}
}

// destinationDelays collects the delay each destination in spec declares, keyed
// by the physical destination name its messages carry. A destination with no
// declared delay is left out, which is how the port says it has none.
func destinationDelays(spec driver.TopologySpec) map[string]time.Duration {
	delays := make(map[string]time.Duration, len(spec.Destinations))
	for _, destination := range spec.Destinations {
		if destination.Delay > 0 {
			delays[destination.Name] = destination.Delay
		}
	}
	if len(delays) == 0 {
		return nil
	}
	return delays
}

func subscriptionDestinations(effective driver.Capabilities, source string, sub Subscription) []string {
	set := make(map[string]struct{})
	forEachDestination(effective, source, sub, func(_ string, _ Priority, _ int, destination string) {
		set[destination] = struct{}{}
	})
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
			for tier := 1; tier <= sub.Retry.tierCount(); tier++ {
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

// deadLetterDestination returns the dead-letter destination for a delivery's
// resolved topic. A delivery whose topic is empty, typically one whose event
// type names no family because its envelope could not be decoded, still needs
// a stable destination, so the empty topic becomes "unknown".
func deadLetterDestination(r *Runner, topic string) string {
	if topic == "" {
		topic = "unknown"
	}
	return deadLetterDestinationFor(r.client.source, topic, r.subscription.Name)
}

// retryDestination returns the retry destination for a delivery's resolved
// topic at one tier.
func retryDestination(r *Runner, topic string, priority Priority, tier int) string {
	return retryDestinationFor(r.client.source, topic, priority, tier, r.subscription.Name)
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
