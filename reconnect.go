package f1

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"

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
		c.wakeWaitersLocked(c.supervisorCtx.Err())
		c.mu.Unlock()
	}()
	for {
		select {
		case request := <-c.reconnectRequests:
			c.mu.Lock()
			attempt := c.reconnect
			c.mu.Unlock()
			if attempt == nil {
				continue
			}
			lastResortClientLogger(c).Warn("f1 reconnect started", "error", request.cause)
			err := c.reconnectOnce(c.supervisorCtx, request.cause, attempt)
			c.finishReconnect(attempt, err)
		case <-c.supervisorCtx.Done():
			err := c.supervisorCtx.Err()
			c.mu.Lock()
			c.finishReconnectLocked(c.reconnect, err)
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

func (c *Client) requestReconnect(cause error) (*reconnectAttempt, error) {
	if cause == nil {
		cause = errClientReconnecting
	}
	c.mu.Lock()
	if err := c.admit(workReconnect, 0); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if c.connStateLocked() == connReconnecting {
		// An attempt already owns the client. The caller joins it instead of
		// starting a second one, and the supervisor owns it until it ends.
		attempt := c.reconnect
		c.mu.Unlock()
		return attempt, nil
	}
	attempt := &reconnectAttempt{done: make(chan struct{})}
	c.reconnecting = true
	c.reconnect = attempt
	c.mu.Unlock()

	select {
	case c.reconnectRequests <- reconnectRequest{cause: cause}:
		return attempt, nil
	case <-c.supervisorCtx.Done():
		c.finishReconnect(attempt, c.supervisorCtx.Err())
		return nil, c.supervisorCtx.Err()
	}
}

func (c *Client) finishReconnect(attempt *reconnectAttempt, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.finishReconnectLocked(attempt, err)
}

func (c *Client) finishReconnectLocked(attempt *reconnectAttempt, err error) {
	if attempt == nil || c.reconnect != attempt {
		return
	}
	attempt.err = err
	c.reconnect = nil
	c.reconnecting = false
	close(attempt.done)
	// Every path out of reconnectOnce ends here, so this is where a waiter that
	// parked during the attempt learns how the attempt ended. One that failed
	// leaves the epoch alone, and the waiter must read the connection state and
	// this error rather than wait for a connection that is not coming.
	c.wakeWaitersLocked(err)
}

func (c *Client) waitReconnect(ctx context.Context, attempt *reconnectAttempt) error {
	if attempt == nil {
		return nil
	}
	select {
	case <-attempt.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	c.mu.Lock()
	err := attempt.err
	c.mu.Unlock()
	return err
}

func (c *Client) reconnectOnce(ctx context.Context, cause error, attempt *reconnectAttempt) error {
	c.abandonRunners(ctx, attempt)
	if err := c.waitPublishIdle(ctx); err != nil {
		return err
	}

	c.mu.Lock()
	oldConn := c.conn
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
					if c.closed || c.shutdownStarted {
						c.mu.Unlock()
						_ = connection.Close(context.WithoutCancel(ctx))
						return errors.New("f1: client is closing")
					}
					c.conn = connection
					c.effective = effective
					c.limits = limitsFor(c.options.driver.Name(), connection.BrokerInfo(), effective)
					c.reconnectErr = nil
					c.producerHandle = nil
					// The epoch and the wake move with the pointer inside this
					// one critical section, so a reader that holds mu sees the
					// new connection with the new number and never the old one
					// with it, and a waiter parked on the old incarnation is
					// released with the number of the connection that replaced
					// it.
					c.epoch++
					c.wakeWaitersLocked(nil)
					c.mu.Unlock()
					c.retireConnection(ctx, oldProducer, oldConn, connection)
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

func (c *Client) reconnectSample() float64 {
	if c.reconnectRandom != nil {
		return c.reconnectRandom()
	}
	return rand.Float64() //nolint:gosec // jitter needs a fast non-cryptographic sample
}

// abandonRunners hands every runner to attempt and gives back the deliveries
// they hold unsettled. A failed Release is logged and the abandon continues:
// the connection is being replaced because it may be broken, so its teardown
// calls can fail for the same reason, and every other step that retires that
// connection is already logged and continued. Aborting instead would strand
// the runners already cancelled on a client that stays healthy and leave the
// rest un-abandoned. A consumer left open by its failed Release is the same
// leak the retiring connection already logs, not a second defect.
func (c *Client) abandonRunners(ctx context.Context, attempt *reconnectAttempt) {
	c.mu.Lock()
	runners := make([]*Runner, 0, len(c.runners))
	for runner := range c.runners {
		runners = append(runners, runner)
	}
	c.mu.Unlock()
	for _, runner := range runners {
		if err := runner.abandonForReconnect(ctx, attempt); err != nil {
			lastResortClientLogger(c).Warn("f1 subscription release failed during reconnect", "subscription", runner.subscription.Name, "error", err)
		}
	}
}

func (c *Client) waitPublishIdle(ctx context.Context) error {
	return c.publishQuiescence(ctx, nil)
}

func (c *Client) retireConnection(ctx context.Context, producer driver.Producer, oldConn, newConn driver.Conn) {
	closeCtx := context.WithoutCancel(ctx)
	if producer != nil {
		if err := producer.Close(closeCtx); err != nil {
			lastResortClientLogger(c).Warn("f1 retired producer close failed", "error", err)
		}
	}
	if oldConn != nil && !sameConnection(oldConn, newConn) {
		if err := oldConn.Close(closeCtx); err != nil {
			lastResortClientLogger(c).Warn("f1 retired connection close failed", "error", err)
		}
	}
}

func sameConnection(left, right driver.Conn) bool {
	if left == nil || right == nil {
		return left == right
	}
	leftValue := reflect.ValueOf(left)
	rightValue := reflect.ValueOf(right)
	if leftValue.Type() != rightValue.Type() || !leftValue.Type().Comparable() {
		return false
	}
	return leftValue.Interface() == rightValue.Interface()
}

func (r *Runner) abandonForReconnect(ctx context.Context, attempt *reconnectAttempt) error {
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
	if r.reconnectCause == nil {
		r.reconnectCause = errClientReconnecting
		r.reconnectCauseAttempt = attempt
	} else if r.reconnectCause == errClientReconnecting && r.reconnectCauseAttempt != nil && attempt != nil && r.reconnectCauseAttempt != attempt { //nolint:errorlint // exact sentinel identifies supervisor ownership
		r.reconnectCauseAttempt = attempt
	}
	cancel := r.cancel
	consumer := r.consumer
	r.mu.Unlock()
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
	return c.reconnecting
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
