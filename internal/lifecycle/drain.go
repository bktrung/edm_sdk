package lifecycle

import (
	"context"
	"errors"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// Hooks are the resource operations owned by a runner or client.
type Hooks struct {
	Drain         func(context.Context) error
	CancelHandler func()
	WaitSettled   func(context.Context) error
	Flush         func(context.Context) error
	Close         func(context.Context) error
}

// Config defines independent wall-clock shutdown budgets.
type Config struct {
	// Clock provides deterministic timing for shutdown phases.
	Clock        clock.Clock
	DrainTimeout time.Duration
	HandlerGrace time.Duration
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
	if state == Ready {
		if err := m.Transition(Draining); err != nil {
			return err
		}
	} else if state != Draining {
		return errors.New("lifecycle: drain requires ready or draining state")
	}
	if hooks.Drain != nil {
		if err := runWithTimeout(ctx, cfg.DrainTimeout, hooks.Drain, phaseClock); err != nil {
			return m.abort(err)
		}
	}
	if err := m.Transition(Settling); err != nil {
		return err
	}
	if hooks.CancelHandler != nil {
		grace := cfg.HandlerGrace
		if cfg.DrainTimeout > 0 && grace >= cfg.DrainTimeout {
			grace = 0
		}
		if cfg.DrainTimeout > grace {
			timer := phaseClock.Timer(cfg.DrainTimeout - grace)
			select {
			case <-timer.C:
				hooks.CancelHandler()
			case <-ctx.Done():
				timer.Stop()
				return m.abort(ctx.Err())
			}
		} else {
			hooks.CancelHandler()
		}
	}
	if hooks.WaitSettled != nil {
		if err := runWithTimeout(ctx, cfg.DrainTimeout, hooks.WaitSettled, phaseClock); err != nil {
			return m.abort(err)
		}
	}
	if err := m.Transition(Flushing); err != nil {
		return err
	}
	if hooks.Flush != nil {
		if err := runWithTimeout(ctx, cfg.FlushTimeout, hooks.Flush, phaseClock); err != nil {
			return m.abort(err)
		}
	}
	if hooks.Close != nil {
		if err := runWithTimeout(ctx, cfg.CloseTimeout, hooks.Close, phaseClock); err != nil {
			return m.abort(err)
		}
	}
	return m.Transition(Closed)
}

func (m *Machine) abort(err error) error {
	transitionErr := m.Transition(Aborted)
	return errors.Join(err, transitionErr)
}

func runWithTimeout(parent context.Context, timeout time.Duration, fn func(context.Context) error, clk clock.Clock) error {
	if fn == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	if timeout <= 0 {
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
