package f1

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/retry"
)

var reconnectPolicy = retry.Config{
	InitialInterval: reconnectDriverInitialInterval,
	Multiplier:      2,
	MaxInterval:     reconnectDriverMaxInterval,
}

// retirementCeiling is the number of owned retirements at which a reconnect is
// refused. Every retired connection and every discarded producer whose teardown
// has not succeeded is one entry, so the ceiling bounds the handles the client
// can hold on a broker that refuses to close them. Past it the client gives up
// the connection and stays that way until a restart, which is the trade the
// design accepted over an unbounded leak.
const retirementCeiling = 8

// maxRetirementRetryInterval caps the doubling retry backoff. A minute keeps a
// broker that is briefly unable to close from being hammered while still
// releasing a handle soon after it recovers.
const maxRetirementRetryInterval = time.Minute

// errRetirementsExhausted is the reason half of the error a refused reconnect
// retains, so a caller can test for the ceiling with errors.Is without
// depending on the count.
var errRetirementsExhausted = errors.New("retirement ceiling reached")

// retireCeilingError is the retained error the refused reconnect leaves and
// Health reports, for example "f1: 8 retired connections failed to close:
// retirement ceiling reached".
func retireCeilingError(count int) error {
	return fmt.Errorf("f1: %d retired connections failed to close: %w", count, errRetirementsExhausted)
}

// retirementBackoff is how long after a failure the next attempt waits. retries
// is the number of attempts that have already failed: the first retry waits one
// Lifecycle.CloseTimeout, and each later one waits twice the last, capped at a
// minute.
func retirementBackoff(retries int, closeTimeout time.Duration) time.Duration {
	d := closeTimeout
	for range retries {
		if d >= maxRetirementRetryInterval/2 {
			return maxRetirementRetryInterval
		}
		d *= 2
	}
	return d
}

func (c *Client) reconnectSupervisor() {
	defer close(c.supervisorDone)
	// The supervisor's own exit ends whatever attempt was in flight, so the
	// wake is released here too, however the loop ended: a cancelled context, a
	// request that arrived after it, or a panic unwinding through reconnectOnce.
	// Every release replaces the channel, so an exit that follows an attempt
	// end releases the waiters that arrived in between rather than closing a
	// channel that is already closed.
	defer func() {
		c.mu.Lock()
		c.finishReconnectLocked(c.supervisorCtx.Err())
		c.mu.Unlock()
	}()
	for {
		select {
		case request := <-c.reconnectRequests:
			// The epoch the request names is the connection the caller wants
			// rebuilt. A swap that happened while the request waited is the
			// answer to it: the caller was released by that swap, and running
			// an attempt now would rebuild a connection nobody holds.
			c.mu.Lock()
			stale := c.staleClaimLocked(request.epoch)
			c.mu.Unlock()
			if stale {
				c.finishReconnect(nil)
				continue
			}
			observer := c.observer
			var lostAt time.Time
			if observer != nil {
				lostAt = c.options.clock.Now()
				c.observeRecord(PointEvent{
					Kind:          ObserverConnectionLost,
					At:            lostAt,
					ErrorClass:    errorClassOf(request.cause),
					ServerAddress: c.serverAddress,
					ServerPort:    c.serverPort,
				})
			}
			lastResortClientLogger(c).Warn("f1 reconnect started", "error", request.cause)
			reconnectErr := c.reconnectOnce(c.supervisorCtx, request.cause)
			if reconnectErr == nil && observer != nil {
				restoredAt := c.options.clock.Now()
				c.observeRecord(PointEvent{
					Kind:          ObserverConnectionRestored,
					At:            restoredAt,
					Downtime:      restoredAt.Sub(lostAt),
					ServerAddress: c.serverAddress,
					ServerPort:    c.serverPort,
				})
			}
			c.finishReconnect(reconnectErr)
		case <-c.supervisorCtx.Done():
			c.mu.Lock()
			c.finishReconnectLocked(c.supervisorCtx.Err())
			c.mu.Unlock()
			return
		}
	}
}

// wakeLocked returns the wake a waiter parks on, creating it for a client that
// has not released one yet. The zero Client is a shape this package's tests
// build directly, and a nil channel could not be released.
func (c *Client) wakeLocked() chan struct{} {
	if c.attemptEnded == nil {
		c.attemptEnded = make(chan struct{})
	}
	return c.attemptEnded
}

// wakeWaitersLocked stores err as the outcome of the attempt that is ending and
// releases every waiter parked on the current wake, then installs the next one.
// The caller holds c.mu, so a waiter that observes the release reads an
// attemptErr that belongs to the change that released it and not to a later
// one.
//
// It is called from the two places a waiter's view of the client changes: the
// swap, which moves the epoch in the same critical section, and the end of
// every attempt, which is where an attempt that failed gives up no connection
// at all and the waiter must read the connection state instead of waiting
// again.
func (c *Client) wakeWaitersLocked(err error) {
	c.attemptErr = err
	close(c.wakeLocked())
	c.attemptEnded = make(chan struct{})
}

// requestReconnect asks the supervisor to rebuild the connection the caller
// holds, naming that connection by the epoch it was read with.
//
// A nil error means the caller's wait on the client's wake is the answer,
// whether the request was delivered or not: a request whose epoch a swap has
// already replaced, and a request made while an attempt is rebuilding the
// connection, are both answered by the wake that ends that change. A non-nil
// error is the client's gate refusing the request, and is what the caller
// returns.
func (c *Client) requestReconnect(cause error, epoch uint64) error {
	if cause == nil {
		cause = errClientReconnecting
	}
	c.mu.Lock()
	if err := c.admit(workReconnect, 0); err != nil {
		c.mu.Unlock()
		return err
	}
	if c.conn == connReconnecting || c.staleClaimLocked(epoch) {
		// An attempt already owns the client, or the swap has already replaced
		// the connection the caller asked about. Either way a second request
		// would start a second attempt for a change the caller is waiting on.
		c.mu.Unlock()
		return nil
	}
	// The client reports the reconnect from here rather than from the
	// supervisor's receive: the request is in flight from this point, and a
	// runner that starts now must wait for it instead of opening a consumer on
	// the connection this attempt is about to replace.
	c.conn = connReconnecting
	c.mu.Unlock()

	select {
	case c.reconnectRequests <- reconnectRequest{cause: cause, epoch: epoch}:
		// The request is in the channel, and a supervisor still running will
		// serve it. One that is already leaving will not: it cannot be told
		// apart from a request that arrived after its last receive, and a
		// caller left waiting for an attempt that will never run would hold a
		// runner in Reconnecting until its own context ends.
		if err := c.supervisorCtx.Err(); err == nil {
			return nil
		} else {
			c.releaseUnservedRequest(epoch, err)
			return err
		}
	case <-c.supervisorCtx.Done():
		err := c.supervisorCtx.Err()
		c.releaseUnservedRequest(epoch, err)
		return err
	}
}

// releaseUnservedRequest undoes the reconnect claim a request that no
// supervisor will serve recorded, and releases the callers waiting on it. The
// epoch guards the claim: a connection installed since the request was made
// means the claim belongs to a change that already ended, and clearing it here
// would report the client live while an attempt is rebuilding it.
func (c *Client) releaseUnservedRequest(epoch uint64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.staleClaimLocked(epoch) {
		return
	}
	c.finishReconnectLocked(err)
}

func (c *Client) finishReconnect(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.finishReconnectLocked(err)
}

// finishReconnectLocked ends the attempt in flight: the client stops reporting
// a reconnect, and every waiter parked on the attempt's wake is released with
// err. The caller holds c.mu.
//
// Every path out of reconnectOnce ends here, and the supervisor's exit ends
// here too, so no outcome of an attempt leaves a caller waiting. It runs even
// when nothing is rebuilding the connection, because a caller parks as soon as
// it has asked and the supervisor's exit is then the only thing left that can
// release it: that is the window between a delivered request and the attempt
// that would have served it.
func (c *Client) finishReconnectLocked(err error) {
	c.conn = connLive
	// A waiter that parked during the attempt learns how the attempt ended
	// here. One that failed leaves the epoch alone, and the waiter reads the
	// connection state and this error rather than waiting for a connection that
	// is not coming.
	c.wakeWaitersLocked(err)
}

// reconnectRefusedAtCeiling reports the refusal error when the client already
// holds the ceiling's worth of failed retirements, and nil otherwise. The
// reconnect supervisor calls it at the start of every attempt, before the
// replacement is opened, so a broker that will not close connections cannot
// make the client accumulate more of them. It is also what moves the client
// into its failed connection state: the retained error is the connection axis
// value that Health and every publish read.
func (c *Client) reconnectRefusedAtCeiling() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.retirements) < retirementCeiling {
		return nil
	}
	c.reconnectErr = retireCeilingError(len(c.retirements))
	return c.reconnectErr
}

func (c *Client) reconnectOnce(ctx context.Context, cause error) error {
	c.abandonRunners(ctx)
	if err := c.publishQuiescence(ctx, nil); err != nil {
		return err
	}

	c.mu.Lock()
	oldConn := c.current.conn
	oldEpoch := c.current.epoch
	oldProducer := c.producerHandle
	c.mu.Unlock()

	for attempt := 1; ; attempt++ {
		if err := c.reconnectRefusedAtCeiling(); err != nil {
			if logger := configuredClientLogger(c); logger != nil {
				logger.Error("f1 reconnect refused: retirement ceiling reached", "error", err)
			}
			return err
		}
		delay := retry.FullJitter(reconnectPolicy.DelayFor(attempt), c.reconnectSample())
		if err := c.options.clock.Sleep(ctx, delay); err != nil {
			return err
		}
		connection, err := c.options.driver.Open(ctx, driverConfig(c.config, c.options.logger))
		if err == nil {
			if connection == nil {
				err = errors.New("reconnect returned no connection")
			} else {
				capabilities := connection.Capabilities()
				effective := capabilities
				if c.options.strictPortability {
					effective = effective.Strict()
				}
				err = c.ensurePublisherTopologyOn(ctx, connection, effective)
				if err == nil {
					err = c.ensureSubscriptionTopologiesOn(ctx, connection, effective)
				}
				if err == nil {
					c.mu.Lock()
					if c.lifecycleLocked() != lifecycle.Ready {
						c.mu.Unlock()
						c.closeOrRetain(ctx, &retiredConnection{conn: connection})
						return errors.New("f1: client is closing")
					}
					// The connection and the number of its incarnation are
					// installed as one value here, inside this one critical
					// section, so a reader that holds mu sees the new connection
					// with the new number and never the old one with it, and a
					// waiter parked on the old incarnation is released with the
					// number of the connection that replaced it.
					c.current = currentConnection{conn: connection, epoch: c.current.epoch + 1}
					c.effective = effective
					c.limits = limitsFor(c.options.driver.Name(), connection.BrokerInfo(), effective)
					c.reconnectErr = nil
					c.producerHandle = nil
					c.wakeWaitersLocked(nil)
					c.mu.Unlock()
					c.retireConnection(ctx, oldProducer, oldConn, oldEpoch)
					return nil
				}
			}
		}
		if connection != nil {
			c.closeOrRetain(ctx, &retiredConnection{conn: connection})
		}
		if err == nil {
			err = errors.New("f1: reconnect returned no connection")
		}
		kind, classified := driver.Classify(err)
		if classified && kind != driver.KindTransient {
			c.mu.Lock()
			c.reconnectErr = err
			c.mu.Unlock()
			if logger := configuredClientLogger(c); logger != nil {
				logger.Error("f1 fatal reconnect failure", "error", err)
			}
			return err
		}
		if c.config.Broker.MaxReconnectAttempts > 0 && attempt >= c.config.Broker.MaxReconnectAttempts {
			reconnectErr := &driver.Error{
				Driver: c.options.driver.Name(),
				Op:     "reconnect",
				K:      driver.KindFatal,
				Err:    fmt.Errorf("reconnect attempts exhausted after %d attempt(s): %w", attempt, err),
			}
			c.mu.Lock()
			c.reconnectErr = reconnectErr
			c.mu.Unlock()
			if logger := configuredClientLogger(c); logger != nil {
				logger.Error("f1 reconnect attempts exhausted", "error", reconnectErr)
			}
			return reconnectErr
		}
	}
}

// awaitRebuild asks the supervisor to rebuild the connection the caller holds
// and waits until that attempt ends, then reports what the state the wake
// carries says the caller does next.
//
// epoch is the incarnation the caller holds, and it is what the request names;
// the zero value is a caller that holds no connection and asks no question
// about one, which is a caller that only needs the connection to be live. cause
// is the caller's own failure, and it is what a request reports. A nil cause,
// or a claim the swap has already replaced, asks for nothing: a caller that was
// abandoned by an attempt in flight, and one whose connection was rebuilt while
// it was not looking, both have a change to wait for rather than a request to
// make. drain ends the wait for a runner that is drained before it owns a
// consumer, and is nil for the waits inside a running generation.
//
// The caller names no attempt. The epoch and the wake that belongs to it are
// captured under one hold of the client's lock, and the supervisor releases
// that wake at every attempt's end, at the swap, and at its own exit, so the
// wait ends. What happens next is read from the state and never from an
// attempt: a replaced claim is a rebuilt connection to reopen on, a failed
// connection is the retained error, an ended attempt is its own outcome, and a
// client that has begun closing stops the caller.
func (c *Client) awaitRebuild(ctx context.Context, cause error, epoch uint64, drain <-chan struct{}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	ended := c.wakeLocked()
	reconnecting := c.conn == connReconnecting
	if epoch == 0 {
		// A caller with no incarnation of its own holds the current one: the
		// change it is waiting for is the next swap, which is what a caller
		// that only needs the connection to be live waits for.
		epoch = c.current.epoch
	}
	ask := cause != nil && !reconnecting && !c.staleClaimLocked(epoch)
	c.mu.Unlock()
	if ask {
		if err := c.requestReconnect(cause, epoch); err != nil {
			return err
		}
	}
	// A wait parks only where something will release it. An attempt in flight
	// releases the wake captured here, and a request this call sends is served
	// by the attempt it starts or dropped by the swap that answered it. With
	// neither, nothing would ever release the park, so the state is read
	// instead: that is a caller arriving after the change it would have waited
	// for has already happened.
	if reconnecting || ask {
		select {
		case <-ended:
		case <-ctx.Done():
			return ctx.Err()
		case <-drain:
			return errRunnerDraining
		}
	}
	c.mu.Lock()
	moved := c.staleClaimLocked(epoch)
	state := c.connStateLocked()
	life := c.lifecycleLocked()
	attemptErr := c.attemptErr
	reconnectErr := c.reconnectErr
	c.mu.Unlock()
	switch {
	case moved:
		return nil
	case state == connFailed:
		return reconnectErr
	case attemptErr != nil:
		// The attempt ended without replacing the connection, so its own
		// outcome is what the caller gets: a fatal failure, an exhausted
		// budget, or the cancellation that ended the attempt.
		return attemptErr
	case life != lifecycle.Ready:
		return errRunnerDraining
	case cause != nil:
		return cause
	default:
		return nil
	}
}

// claimReplaced reports whether the connection a caller captured has been
// replaced. It is staleClaimLocked under the lock, for a caller that reads it
// as a decision of its own rather than as part of a client critical section.
func (c *Client) claimReplaced(epoch uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.staleClaimLocked(epoch)
}

func (c *Client) reconnectSample() float64 {
	if c.reconnectRandom != nil {
		return c.reconnectRandom()
	}
	return rand.Float64() //nolint:gosec // jitter needs a fast non-cryptographic sample
}

// abandonRunners hands every runner to the attempt that is replacing the
// connection and gives back the deliveries they hold unsettled. A failed
// Release is logged and the abandon continues: the connection is being
// replaced because it may be broken, so its teardown calls can fail for the
// same reason, and every other step that retires that connection is already
// logged and continued. Aborting instead would strand the runners already
// cancelled on a client that stays healthy and leave the rest un-abandoned. A
// consumer left open by its failed Release is kept on the client with the
// connection it was opened on, and the swap retries that release before it
// closes that connection.
func (c *Client) abandonRunners(ctx context.Context) {
	c.mu.Lock()
	runners := make([]*Runner, 0, len(c.runners))
	for runner := range c.runners {
		runners = append(runners, runner)
	}
	c.mu.Unlock()
	for _, runner := range runners {
		if err := runner.abandonForReconnect(ctx); err != nil {
			lastResortClientLogger(c).Warn("f1 subscription release failed during reconnect", "subscription", runner.subscription.Name, "error", err)
		}
	}
}

// topologyReplayConcurrency bounds how many subscription topologies one
// reconnect replays at once. The replay is the longest step of a swap on a
// broker that opens a channel per destination, and it is bounded because every
// replay call opens broker resources on the connection the swap is about to
// install.
const topologyReplayConcurrency = 4

// ensureSubscriptionTopologiesOn replays every subscription's topology on a
// replacement connection before it is installed. A driver learns how to route
// a delayed destination, such as a retry tier's parking queue, from the
// EnsureTopology call made on that connection, and a handler still running
// when the swap lands publishes its retry or dead-letter successor on the new
// connection before its runner reopens and replays the topology itself.
// Without this replay that successor is routed as if the destination had no
// declared delay and can reach a queue that was never declared.
//
// The replays run concurrently, at most topologyReplayConcurrency at a time,
// because the swap rebrands one destination at a time otherwise and the
// downtime grows with the subscription count. The outcome is the one the
// serial replay gave: a refusal the broker makes for one subscription is
// logged and skipped, leaving that subscription to fail its own reopen, and a
// transient failure ends the attempt with the error that names its
// subscription. That failure stops further replays from starting and cancels
// the ones in flight, so an aborted attempt does not keep declaring topology
// on the connection it is about to throw away; the first such failure is the
// one reported.
func (c *Client) ensureSubscriptionTopologiesOn(ctx context.Context, conn driver.Conn, effective driver.Capabilities) error {
	c.mu.Lock()
	runners := make([]*Runner, 0, len(c.runners))
	for runner := range c.runners {
		runners = append(runners, runner)
	}
	source := c.source
	c.mu.Unlock()
	if len(runners) == 0 {
		return nil
	}
	admin := conn.Admin()
	if admin == nil {
		return errors.New("f1: consumer topology requires driver admin")
	}
	policy := c.topologyPolicy()
	replayCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	slots := make(chan struct{}, topologyReplayConcurrency)
	var (
		mu      sync.Mutex
		failed  error
		running sync.WaitGroup
	)
	stopped := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return failed != nil
	}
	for _, runner := range runners {
		// The slot is taken before the stop is read, so a replay that has been
		// granted capacity never starts after the attempt gave up.
		slots <- struct{}{}
		if stopped() {
			<-slots
			break
		}
		running.Go(func() {
			defer func() { <-slots }()
			spec := subscriptionTopologySpecs(effective, source, runner.subscription)
			spec.Policy = policy
			if _, err := admin.EnsureTopology(replayCtx, spec); err != nil {
				if kind, classified := driver.Classify(err); classified && kind != driver.KindTransient {
					lastResortClientLogger(c).Warn("f1 subscription topology replay refused during reconnect", "subscription", runner.subscription.Name, "error", err)
					return
				}
				mu.Lock()
				if failed == nil {
					failed = fmt.Errorf("f1: ensure subscription %s topology: %w", runner.subscription.Name, err)
				}
				mu.Unlock()
				cancel()
			}
		})
	}
	running.Wait()
	mu.Lock()
	defer mu.Unlock()
	return failed
}

// retiredConnection stays owned until its sequential teardown succeeds. c.mu
// guards progress and the attempt; only the running attempt advances handles.
type retiredConnection struct {
	epoch    uint64
	producer driver.Producer
	conn     driver.Conn
	attempt  *retirementAttempt
	err      error
	// retries counts the completed attempts this entry's teardown has needed,
	// and retryAt is when the next one may start. Both are set by
	// completeRetirement when an attempt fails and the failure is recorded for
	// the first time, and are read by the retry loop. A zero retryAt means no
	// retry is scheduled, which is the state while an attempt is running or
	// before the first failure. c.mu guards both.
	retries int
	retryAt time.Time
}

// retirementAttempt broadcasts actual completion, not the bounded wait's
// outcome. A publisher can time out while its Close is still at the driver;
// the supervisor must then observe the same result without consuming it before
// Client.Close rejoins. c.mu guards installation and result acknowledgement.
type retirementAttempt struct {
	done   chan struct{}
	err    error
	cancel context.CancelFunc
	// accounted records that the entry's retry state was updated for this
	// attempt's failure. A publisher and the reaper can both acknowledge the
	// same completion, so the update has to happen once per attempt rather
	// than once per reader. c.mu guards it.
	accounted bool
}

// retirementWakeLocked wakes the retirement retry loop after the list it reads
// changed. The channel holds one pending signal, so many changes across one
// pass collapse into the single wake that makes the loop rebuild its view. The
// caller holds c.mu.
func (c *Client) retirementWakeLocked() {
	if c.retirementWake == nil {
		c.retirementWake = make(chan struct{}, 1)
	}
	select {
	case c.retirementWake <- struct{}{}:
	default:
	}
}

// retirementRetryLoop retries failed retirements while the client runs. It
// owns no entry: it starts an attempt under c.mu only where attempt is nil and
// the entry's backoff has elapsed, so Close's closeRetirements and this loop
// cannot both start one. It exits when the supervisor context is cancelled,
// which is the first thing Close does, so Close's closeRetirements is the only
// retry driver once shutdown begins.
//
// The loop retries while the client is in its failed connection state as well
// as while it is live: the entries are still held there, and releasing them is
// the only thing that shrinks the count. It never clears that state; a refused
// client stays refused until a restart.
func (c *Client) retirementRetryLoop() {
	defer close(c.retirementLoopDone)
	ctx := c.supervisorCtx
	for {
		if ctx.Err() != nil || !c.retirementRetryShouldRun() {
			return
		}
		c.reapRetirements()
		c.startDueRetirements(ctx)
		wait, scheduled := c.nextRetirementRetryWait()
		if !scheduled {
			if c.hasHeldRetirements() {
				// Entries are held but none has a retry time yet: an attempt is
				// already running, or one was just started. Wait for its
				// completion to signal the wake rather than spin on the clock.
				select {
				case <-c.retirementWaker():
				case <-ctx.Done():
					return
				}
				continue
			}
			// Nothing is held: block until an entry is appended.
			select {
			case <-c.retirementWaker():
			case <-ctx.Done():
				return
			}
			continue
		}
		if wait == 0 {
			// A retry is due now; the next pass starts it.
			continue
		}
		timer := c.options.clock.Timer(wait)
		select {
		case <-timer.C:
		case <-c.retirementWaker():
		case <-ctx.Done():
			timer.Stop()
			return
		}
		timer.Stop()
	}
}

// hasHeldRetirements reports whether the client owns any retirement.
func (c *Client) hasHeldRetirements() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.retirements) > 0
}

// retirementRetryShouldRun reports that retries belong to a live client. It
// reads the lifecycle and not the connection axis, because a failed connection
// still owns its retirements and still needs them released.
func (c *Client) retirementRetryShouldRun() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lifecycleLocked() == lifecycle.Ready
}

// retirementWaker returns the loop's wake channel under c.mu.
func (c *Client) retirementWaker() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retirementWake == nil {
		c.retirementWake = make(chan struct{}, 1)
	}
	return c.retirementWake
}

// startDueRetirements starts an attempt for every entry whose retry is due. It
// starts one per lock acquisition, so an attempt that completes while this runs
// is reaped by the next pass rather than missed, and the retry counter advances
// only where an attempt is actually started.
func (c *Client) startDueRetirements(ctx context.Context) {
	for {
		c.mu.Lock()
		now := c.options.clock.Now()
		var due *retiredConnection
		for _, entry := range c.retirements {
			if entry.attempt != nil || entry.retryAt.IsZero() || now.Before(entry.retryAt) {
				continue
			}
			due = entry
			break
		}
		if due == nil {
			c.mu.Unlock()
			return
		}
		// Clear the due time in the same section that decides to start, so the
		// next pass cannot count this attempt again.
		due.retries++
		due.retryAt = time.Time{}
		c.mu.Unlock()
		c.startRetirement(context.WithoutCancel(ctx), due)
	}
}

// nextRetirementRetryWait is when the earliest scheduled retry may start, and
// whether one is scheduled at all. An entry whose attempt is running has no
// scheduled retry and is skipped: the loop is not waiting for that attempt here,
// it is waiting for the reaping pass that follows it.
func (c *Client) nextRetirementRetryWait() (time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.options.clock.Now()
	var wait time.Duration
	scheduled := false
	for _, entry := range c.retirements {
		if entry.attempt != nil || entry.retryAt.IsZero() {
			continue
		}
		d := max(entry.retryAt.Sub(now), 0)
		if !scheduled || d < wait {
			wait, scheduled = d, true
		}
	}
	return wait, scheduled
}

// closeOrRetain owns one teardown of a connection or producer the client gave
// up on and, when it fails, leaves the entry for the retry loop.
func (c *Client) closeOrRetain(ctx context.Context, retired *retiredConnection) {
	c.mu.Lock()
	c.retirements = append(c.retirements, retired)
	c.retirementWakeLocked()
	c.mu.Unlock()
	c.startRetirement(context.WithoutCancel(ctx), retired)
	if err, _ := c.joinRetirement(context.WithoutCancel(ctx), retired); err != nil {
		lastResortClientLogger(c).Warn("f1 retired teardown failed", "epoch", retired.epoch, "error", err)
	}
}

func (c *Client) retireConnection(ctx context.Context, producer driver.Producer, oldConn driver.Conn, oldEpoch uint64) {
	c.reapRetirements()
	c.mu.Lock()
	if !c.staleClaimLocked(oldEpoch) {
		c.mu.Unlock()
		return
	}
	retired := &retiredConnection{epoch: oldEpoch, producer: producer, conn: oldConn}
	c.retirements = append(c.retirements, retired)
	c.retirementWakeLocked()
	c.mu.Unlock()
	c.startRetirement(context.WithoutCancel(ctx), retired)
	// The replacement is installed but publishes stay refused until reconnect
	// returns. Bound the whole attempt so a dead old broker cannot keep the
	// healthy replacement unavailable; its unfinished teardown remains owned.
	err, _ := c.joinRetirement(context.WithoutCancel(ctx), retired)
	if err != nil {
		lastResortClientLogger(c).Warn("f1 retired teardown failed", "epoch", oldEpoch, "error", err)
	}
}

func (c *Client) startRetirement(ctx context.Context, retired *retiredConnection) {
	c.mu.Lock()
	if retired.attempt != nil || !slices.Contains(c.retirements, retired) {
		c.mu.Unlock()
		return
	}
	closeCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	attempt := &retirementAttempt{done: make(chan struct{}), cancel: cancel}
	retired.attempt = attempt
	c.mu.Unlock()
	go func() {
		err := c.teardownRetirement(closeCtx, retired)
		cancel()
		c.mu.Lock()
		attempt.err = err
		close(attempt.done)
		// Wake the retry loop so it reaps this completion. A watcher that
		// joined the attempt also records the result, but a retry the loop
		// started itself has no watcher, and without this wake its entry would
		// stay held until the next unrelated change to the list.
		c.retirementWakeLocked()
		c.mu.Unlock()
	}()
}

func (c *Client) teardownRetirement(ctx context.Context, retired *retiredConnection) error {
	c.mu.Lock()
	producer, conn := retired.producer, retired.conn
	c.mu.Unlock()

	if producer != nil {
		if err := producer.Close(ctx); err != nil {
			return err
		}
		c.mu.Lock()
		retired.producer = nil
		c.mu.Unlock()
	}
	if conn == nil {
		return nil
	}
	if retired.epoch != 0 {
		// A discarded producer can finish its bounded wait while raw Close
		// remains at the driver. If a reconnect swaps its parent in between,
		// the old parent's teardown must join that producer before Conn.Close.
		c.mu.Lock()
		discarded := make([]*retiredConnection, 0)
		for _, entry := range c.retirements {
			if entry != retired && entry.epoch == retired.epoch && entry.conn == nil {
				discarded = append(discarded, entry)
			}
		}
		c.mu.Unlock()
		for _, entry := range discarded {
			c.mu.Lock()
			unstarted := entry.attempt == nil && entry.err == nil && entry.producer != nil
			c.mu.Unlock()
			if unstarted {
				c.startRetirement(ctx, entry)
			}
			if err, _ := c.joinRetirement(ctx, entry); err != nil {
				return err
			}
		}
		if err := c.retireKeptConsumers(ctx, retired.epoch); err != nil {
			return err
		}
	}
	if err := conn.Close(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	retired.conn = nil
	c.mu.Unlock()
	return nil
}

func (c *Client) completeRetirement(retired *retiredConnection, attempt *retirementAttempt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// A publisher and reaper may both observe this completion. If Close has
	// acknowledged its failure and installed a retry before the older reader
	// arrives here, that reader must not clear the newer running attempt.
	if retired.attempt == attempt {
		retired.attempt = nil
		retired.err = attempt.err
		if attempt.err == nil {
			retired.retryAt = time.Time{}
			c.retirements = slices.DeleteFunc(c.retirements, func(entry *retiredConnection) bool {
				return entry == retired
			})
		} else if !attempt.accounted {
			// Record the failure once per attempt. A second reader of the same
			// attempt must not schedule another retry, or the count of
			// completed attempts would grow without an attempt.
			attempt.accounted = true
			retired.retryAt = c.options.clock.Now().Add(retirementBackoff(retired.retries, c.config.Lifecycle.CloseTimeout))
			// Wake the loop where the retry is scheduled. The attempt's own
			// goroutine also wakes, but only after it closes done; a watcher
			// that acknowledges the failure in between can leave the loop
			// blocked on a view that predates this retry.
			c.retirementWakeLocked()
		}
	}
	return attempt.err
}

func (c *Client) joinRetirement(ctx context.Context, retired *retiredConnection) (error, bool) {
	c.mu.Lock()
	attempt := retired.attempt
	lastErr := retired.err
	c.mu.Unlock()
	if attempt == nil {
		return lastErr, true
	}
	// The timeout cancels the driver's detached context, but cannot declare
	// its raw call finished. Close may arrive after this wait expires and
	// before the driver returns; it must join done instead of starting again.
	err := runWithClockTimeout(ctx, c.options.clock, c.config.Lifecycle.CloseTimeout, "retired teardown", func(waitCtx context.Context) error {
		select {
		case <-attempt.done:
			return nil
		case <-waitCtx.Done():
			return waitCtx.Err()
		}
	})
	attempt.cancel()
	select {
	case <-attempt.done:
		return c.completeRetirement(retired, attempt), true
	default:
		return err, false
	}
}

// reapRetirements only acknowledges actual completion. A publisher may remove
// its completed producer while the supervisor is walking the registry, so use
// a pointer snapshot rather than indices that shift after that removal.
func (c *Client) reapRetirements() {
	c.mu.Lock()
	retirements := slices.Clone(c.retirements)
	c.mu.Unlock()
	for _, retired := range retirements {
		c.mu.Lock()
		attempt := retired.attempt
		c.mu.Unlock()
		if attempt == nil {
			continue
		}
		select {
		case <-attempt.done:
			if err := c.completeRetirement(retired, attempt); err != nil {
				lastResortClientLogger(c).Warn("f1 retired teardown failed", "epoch", retired.epoch, "error", err)
			}
		default:
		}
	}
}

func (c *Client) closeRetirements(ctx context.Context) error {
	for {
		c.mu.Lock()
		if len(c.retirements) == 0 {
			c.mu.Unlock()
			return nil
		}
		retired := c.retirements[0]
		pending := retired.attempt != nil
		c.mu.Unlock()
		// The supervisor has exited, so it can no longer read this attempt's
		// result. A timeout leaves the same attempt for the next Close; a
		// returned error permits one new attempt, never an unbounded loop.
		if pending {
			err, resolved := c.joinRetirement(ctx, retired)
			if !resolved {
				return err
			}
			if err == nil {
				continue
			}
		}
		c.startRetirement(context.WithoutCancel(ctx), retired)
		err, _ := c.joinRetirement(ctx, retired)
		if err != nil {
			return err
		}
	}
}

// abandonForReconnect hands this runner's consumer back to the connection the
// attempt is replacing, and is how a runner still on that connection is told
// the generation is over. It reports the abandon to the owner and cancels the
// generation before it runs the release, and the release itself is the
// runner's one teardown: if the terminal drain, a lane repair or a handoff
// claimed the consumer first, this joins that claim and reports its result
// rather than releasing a consumer someone else already owns. The claim is
// bounded by the caller's context and Lifecycle.CloseTimeout, which is what
// keeps a driver that ignores its context from holding a reconnect open.
func (r *Runner) abandonForReconnect(ctx context.Context) error {
	r.mu.Lock()
	if r.lifecycle != nil && r.lifecycle.State() == lifecycle.Failed {
		r.mu.Unlock()
		return releaseRunnerConsumer(r, ctx)
	}
	if r.lifecycle != nil && r.lifecycle.State() == lifecycle.Ready {
		_ = r.lifecycle.Transition(lifecycle.Reconnecting)
	}
	// The owner is told before the generation is cancelled and before the
	// consumer is released: the generation ends because of this call, so the
	// record has to exist by the time its owner reads it. The owner reads its
	// events and never blocks on a broker call, so this report cannot be
	// waiting on an attempt the supervisor has not started yet.
	events, done := r.events, r.done
	cancel := r.cancel
	r.mu.Unlock()
	if events != nil {
		select {
		case events <- runnerEvent{kind: runnerEventAbandon}:
		case <-done:
		}
	}
	if cancel != nil {
		cancel()
	}
	return releaseRunnerConsumer(r, ctx)
}

func (c *Client) reconnectingError(op string) error {
	return &driver.Error{Driver: c.options.driver.Name(), Op: op, K: driver.KindTransient, Err: errClientReconnecting}
}

func (c *Client) isReconnecting() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn == connReconnecting
}

func (r *Runner) transitionToReconnecting() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lifecycle != nil && r.lifecycle.State() == lifecycle.Ready {
		_ = r.lifecycle.Transition(lifecycle.Reconnecting)
	}
}

// drainAfterRun releases a runner at the end of Run: it waits for in-flight
// deliveries to leave the registry within DrainTimeout and then stops or
// releases the consumer, bounding each teardown call by CloseTimeout.
//
// The stop is the runner's one consumer teardown. An abandon, a lane repair or
// a handoff that claimed the consumer first owns it instead, and this joins
// that claim and reports its result without touching the driver; a successful
// join ends the runner Closed the way its own completed Stop would.
//
// The release runs even when the wait fails. A consumer left open past the
// wait keeps redelivering work that nothing is settling, so the failure path
// releases on a context detached from the settlement context: the wait ended
// because that context was cancelled or its deadline passed, and the release
// must not inherit the cancellation that ended the wait and return at once.
func (r *Runner) drainAfterRun(runCtx context.Context) error {
	shutdownCtx := runnerSettlementContext(r, runCtx)
	machine := r.lifecycle
	if machine != nil {
		switch machine.State() {
		case lifecycle.Ready, lifecycle.Reconnecting:
			if err := machine.Transition(lifecycle.Draining); err != nil {
				return err
			}
		case lifecycle.Draining, lifecycle.Failed:
		default:
			return fmt.Errorf("f1: runner cannot drain from lifecycle state %s", machine.State())
		}
	}
	phaseClock := r.client.options.clock
	drainTimeout := r.client.config.Lifecycle.DrainTimeout
	if waitErr := runWithClockTimeout(shutdownCtx, phaseClock, drainTimeout, "drain", r.inflight.WaitZero); waitErr != nil {
		releaseErr := stopRunnerConsumer(r, context.WithoutCancel(shutdownCtx))
		if machine != nil {
			if err := machine.Transition(lifecycle.Aborted); err != nil {
				return errors.Join(waitErr, releaseErr, err)
			}
		}
		return errors.Join(waitErr, releaseErr)
	}
	if releaseErr := stopRunnerConsumer(r, shutdownCtx); releaseErr != nil {
		if machine != nil {
			if err := machine.Transition(lifecycle.Aborted); err != nil {
				return errors.Join(releaseErr, err)
			}
		}
		return releaseErr
	}
	if machine == nil || machine.State() == lifecycle.Failed {
		return nil
	}
	return machine.Transition(lifecycle.Closed)
}
