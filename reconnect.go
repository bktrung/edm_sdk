package f1

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/retry"
)

var reconnectPolicy = retry.Config{
	InitialInterval: reconnectDriverInitialInterval,
	Multiplier:      2,
	MaxInterval:     reconnectDriverMaxInterval,
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
			lastResortClientLogger(c).Warn("f1 reconnect started", "error", request.cause)
			c.finishReconnect(c.reconnectOnce(c.supervisorCtx, request.cause))
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

func (c *Client) reconnectOnce(ctx context.Context, cause error) error {
	c.abandonRunners(ctx)
	if err := c.waitPublishIdle(ctx); err != nil {
		return err
	}

	c.mu.Lock()
	oldConn := c.current.conn
	oldEpoch := c.current.epoch
	oldProducer := c.producerHandle
	c.mu.Unlock()

	for attempt := 1; ; attempt++ {
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
					c.mu.Lock()
					if c.lifecycleLocked() != lifecycle.Ready {
						c.mu.Unlock()
						_ = connection.Close(context.WithoutCancel(ctx))
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
			_ = connection.Close(context.WithoutCancel(ctx))
		}
		if err == nil {
			err = errors.New("f1: reconnect returned no connection")
		}
		kind, classified := driver.Classify(err)
		if classified && kind != driver.KindTransient {
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
// consumer left open by its failed Release is the same leak the retiring
// connection already logs, not a second defect.
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

func (c *Client) waitPublishIdle(ctx context.Context) error {
	return c.publishQuiescence(ctx, nil)
}

// retireConnection closes what the swap replaced: the producer that was built
// on the old connection, and the old connection itself when it belongs to an
// incarnation the client has left. The swap installs the replacement under the
// next epoch, so an old epoch that is no longer the client's is a connection
// nothing will use again, and its consumers have already been released.
func (c *Client) retireConnection(ctx context.Context, producer driver.Producer, oldConn driver.Conn, oldEpoch uint64) {
	closeCtx := context.WithoutCancel(ctx)
	if producer != nil {
		if err := producer.Close(closeCtx); err != nil {
			lastResortClientLogger(c).Warn("f1 retired producer close failed", "error", err)
		}
	}
	if oldConn == nil {
		return
	}
	c.mu.Lock()
	retired := c.staleClaimLocked(oldEpoch)
	c.mu.Unlock()
	if !retired {
		return
	}
	if err := oldConn.Close(closeCtx); err != nil {
		lastResortClientLogger(c).Warn("f1 retired connection close failed", "error", err)
	}
}

func (r *Runner) abandonForReconnect(ctx context.Context) error {
	r.mu.Lock()
	if r.lifecycle != nil && r.lifecycle.State() == lifecycle.Failed {
		consumer := r.consumer
		r.mu.Unlock()
		if consumer == nil {
			return nil
		}
		return consumer.Release(ctx)
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
	consumer := r.consumer
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
	if consumer == nil {
		return nil
	}
	return consumer.Release(ctx)
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
// deliveries to leave the registry within DrainTimeout and then releases the
// consumer within CloseTimeout.
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
	closeTimeout := r.client.config.Lifecycle.CloseTimeout
	release := func(ctx context.Context) error { return stopRunnerConsumer(r, ctx) }
	if waitErr := runWithClockTimeout(shutdownCtx, phaseClock, drainTimeout, "drain", r.inflight.WaitZero); waitErr != nil {
		releaseErr := runWithClockTimeout(context.WithoutCancel(shutdownCtx), phaseClock, closeTimeout, "close", release)
		if machine != nil {
			if err := machine.Transition(lifecycle.Aborted); err != nil {
				return errors.Join(waitErr, releaseErr, err)
			}
		}
		return errors.Join(waitErr, releaseErr)
	}
	if releaseErr := runWithClockTimeout(shutdownCtx, phaseClock, closeTimeout, "close", release); releaseErr != nil {
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
