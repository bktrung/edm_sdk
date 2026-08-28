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
			err := c.reconnectOnce(c.supervisorCtx, request.cause)
			c.finishReconnect(attempt, err)
		case <-c.supervisorCtx.Done():
			return
		}
	}
}

func (c *Client) requestReconnect(cause error) (*reconnectAttempt, error) {
	if cause == nil {
		cause = errClientReconnecting
	}
	c.mu.Lock()
	if c.closed || c.shutdownStarted {
		c.mu.Unlock()
		return nil, errors.New("f1: client is closing")
	}
	if c.reconnectErr != nil {
		err := c.reconnectErr
		c.mu.Unlock()
		return nil, err
	}
	if c.reconnecting && c.reconnect != nil {
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
	if c.reconnect != attempt {
		return
	}
	attempt.err = err
	c.reconnect = nil
	c.reconnecting = false
	close(attempt.done)
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

func (c *Client) reconnectOnce(ctx context.Context, cause error) error {
	if err := c.abandonRunners(ctx); err != nil {
		return err
	}
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
		connection, err := c.options.driver.Open(ctx, driverConfig(c.config))
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

func (c *Client) abandonRunners(ctx context.Context) error {
	c.mu.Lock()
	runners := make([]*Runner, 0, len(c.runners))
	for runner := range c.runners {
		runners = append(runners, runner)
	}
	c.mu.Unlock()
	for _, runner := range runners {
		if err := runner.abandonForReconnect(ctx); err != nil {
			return err
		}
	}
	return nil
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
	if r.reconnectCause == nil {
		r.reconnectCause = errClientReconnecting
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

func (r *Runner) drainAfterRun(runCtx context.Context) error {
	shutdownCtx := runnerSettlementContext(r, runCtx)
	return r.lifecycle.Drain(shutdownCtx, lifecycle.Config{
		Clock:        r.client.options.clock,
		DrainTimeout: r.client.config.Lifecycle.DrainTimeout,
		FlushTimeout: r.client.config.Lifecycle.FlushTimeout,
		CloseTimeout: r.client.config.Lifecycle.CloseTimeout,
	}, lifecycle.Hooks{
		WaitSettled: func(ctx context.Context) error {
			return r.inflight.WaitZero(ctx)
		},
		Close: func(ctx context.Context) error {
			return stopRunnerConsumer(r, ctx)
		},
	})
}
