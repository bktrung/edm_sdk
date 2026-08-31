package lifecycle

import (
	"context"
	"errors"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// Hooks are the resource operations owned by a runner or client.
type Hooks struct {
	Drain       func(context.Context) error
	WaitSettled func(context.Context) error
	Flush       func(context.Context) error
	Close       func(context.Context) error
}

// Config defines the wall-clock budgets of the shutdown phases. The budgets
// are not all independent: DrainTimeout bounds both the drain hook and the
// settlement wait, one after the other, so those two phases can consume up
// to twice DrainTimeout together, while FlushTimeout and CloseTimeout each
// bound a single phase. A zero budget disables its deadline; negative
// budgets are invalid.
type Config struct {
	// Clock provides deterministic timing for shutdown phases.
	Clock        clock.Clock
	DrainTimeout time.Duration
	FlushTimeout time.Duration
	CloseTimeout time.Duration
}

// Drain runs the five lifecycle phases. It uses context deadlines for the
// shutdown budget so injected scheduling clocks cannot freeze termination.
//
//nolint:contextcheck // the caller owns the shutdown context and its deadline.
func (m *Machine) Drain(ctx context.Context, cfg Config, hooks Hooks) error {
	if m == nil {
		return errors.New("lifecycle: nil machine")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	phaseClock := cfg.Clock
	if phaseClock == nil {
		phaseClock = clock.NewReal()
	}
	state := m.State()
	if state == Ready || state == Reconnecting {
		if err := m.Transition(Draining); err != nil {
			return err
		}
	} else if state != Draining && state != Failed {
		return errors.New("lifecycle: drain requires ready or draining state")
	}
	if m.State() != Failed && hooks.Drain != nil {
		if err := runWithTimeout(ctx, cfg.DrainTimeout, hooks.Drain, phaseClock); err != nil {
			return m.abortWithClose(ctx, cfg.CloseTimeout, hooks.Close, phaseClock, err)
		}
	}
	if m.State() != Failed {
		if err := m.Transition(Settling); err != nil {
			return err
		}
	}
	if hooks.WaitSettled != nil {
		if err := runWithTimeout(ctx, cfg.DrainTimeout, hooks.WaitSettled, phaseClock); err != nil {
			return m.abortWithClose(ctx, cfg.CloseTimeout, hooks.Close, phaseClock, err)
		}
	}
	if m.State() != Failed {
		if err := m.Transition(Flushing); err != nil {
			return err
		}
	}
	if hooks.Flush != nil {
		if err := runWithTimeout(ctx, cfg.FlushTimeout, hooks.Flush, phaseClock); err != nil {
			return m.abortWithClose(ctx, cfg.CloseTimeout, hooks.Close, phaseClock, err)
		}
	}
	if hooks.Close != nil {
		if err := runWithTimeout(ctx, cfg.CloseTimeout, hooks.Close, phaseClock); err != nil {
			return m.abort(err)
		}
	}
	if m.State() == Failed {
		return nil
	}
	return m.Transition(Closed)
}

func (m *Machine) abort(err error) error {
	transitionErr := m.Transition(Aborted)
	return errors.Join(err, transitionErr)
}

func (m *Machine) abortWithClose(parent context.Context, closeTimeout time.Duration, closeHook func(context.Context) error, phaseClock clock.Clock, err error) error {
	// Abort often follows parent cancellation, so teardown must outlive that cancellation.
	// A zero CloseTimeout intentionally remains unbounded per Config's documented semantics.
	closeErr := runWithTimeout(context.WithoutCancel(parent), closeTimeout, closeHook, phaseClock)
	return m.abort(errors.Join(err, closeErr))
}

func runWithTimeout(parent context.Context, timeout time.Duration, fn func(context.Context) error, clk clock.Clock) error {
	if fn == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	if timeout < 0 {
		return errors.New("lifecycle: timeout must not be negative")
	}
	if timeout == 0 {
		return fn(ctx)
	}
	done := make(chan error, 1)
	go func() { done <- fn(ctx) }()
	timer := clk.Timer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return context.DeadlineExceeded
	case <-parent.Done():
		return parent.Err()
	}
}
