package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestDrainTransitionsImmediatelyAndBoundsFlushWithFakeClock(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	flushStarted := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- machine.Drain(context.Background(), Config{
			Clock:        fake,
			FlushTimeout: 7 * time.Second,
		}, Hooks{
			Drain: func(context.Context) error {
				if machine.State() != Draining {
					return errors.New("drain hook ran outside draining state")
				}
				return nil
			},
			WaitSettled: func(context.Context) error {
				if machine.State() != Settling {
					return errors.New("settle hook ran outside settling state")
				}
				return nil
			},
			Flush: func(ctx context.Context) error {
				if machine.State() != Flushing {
					return errors.New("flush hook ran outside flushing state")
				}
				close(flushStarted)
				<-ctx.Done()
				return ctx.Err()
			},
		})
	}()
	<-flushStarted
	fake.BlockUntil(1)
	fake.Advance(7 * time.Second)
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain() error = %v, want deadline exceeded", err)
	}
	if got := machine.State(); got != Aborted {
		t.Fatalf("state after flush deadline = %s, want aborted", got)
	}
}

func TestDrainBoundsCloseWithFakeClock(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- machine.Drain(context.Background(), Config{Clock: fake, CloseTimeout: 11 * time.Second}, Hooks{
			Close: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
		})
	}()
	fake.BlockUntil(1)
	fake.Advance(11 * time.Second)
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain() error = %v, want deadline exceeded", err)
	}
	if got := machine.State(); got != Aborted {
		t.Fatalf("state after close deadline = %s, want aborted", got)
	}
}

func TestSettlementTimeoutStillRunsClose(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	settleStarted := make(chan struct{})
	closeRan := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- machine.Drain(context.Background(), Config{
			Clock:        fake,
			DrainTimeout: 7 * time.Second,
		}, Hooks{
			WaitSettled: func(ctx context.Context) error {
				close(settleStarted)
				<-ctx.Done()
				return ctx.Err()
			},
			Close: func(context.Context) error {
				close(closeRan)
				return nil
			},
		})
	}()
	<-settleStarted
	fake.BlockUntil(1)
	fake.Advance(7 * time.Second)
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain() error = %v, want deadline exceeded", err)
	}
	if got := machine.State(); got != Aborted {
		t.Fatalf("state after settlement deadline = %s, want aborted", got)
	}
	select {
	case <-closeRan:
	default:
		t.Fatal("close hook did not run after settlement timeout")
	}
}

func TestFlushTimeoutStillRunsClose(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	flushStarted := make(chan struct{})
	closeRan := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- machine.Drain(context.Background(), Config{
			Clock:        fake,
			FlushTimeout: 7 * time.Second,
		}, Hooks{
			Flush: func(ctx context.Context) error {
				close(flushStarted)
				<-ctx.Done()
				return ctx.Err()
			},
			Close: func(context.Context) error {
				close(closeRan)
				return nil
			},
		})
	}()
	<-flushStarted
	fake.BlockUntil(1)
	fake.Advance(7 * time.Second)
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain() error = %v, want deadline exceeded", err)
	}
	if got := machine.State(); got != Aborted {
		t.Fatalf("state after flush deadline = %s, want aborted", got)
	}
	select {
	case <-closeRan:
	default:
		t.Fatal("close hook did not run after flush timeout")
	}
}

func TestDrainHookAbortStillRunsClose(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	drainStarted := make(chan struct{})
	closeRan := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- machine.Drain(context.Background(), Config{
			Clock:        fake,
			DrainTimeout: 7 * time.Second,
		}, Hooks{
			Drain: func(ctx context.Context) error {
				close(drainStarted)
				<-ctx.Done()
				return ctx.Err()
			},
			Close: func(context.Context) error {
				close(closeRan)
				return nil
			},
		})
	}()
	<-drainStarted
	fake.BlockUntil(1)
	fake.Advance(7 * time.Second)
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain() error = %v, want deadline exceeded", err)
	}
	if got := machine.State(); got != Aborted {
		t.Fatalf("state after drain deadline = %s, want aborted", got)
	}
	select {
	case <-closeRan:
	default:
		t.Fatal("close hook did not run after drain timeout")
	}
}

func TestAbortPathCloseGetsALiveContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	var closeContextErr error
	err := machine.Drain(ctx, Config{}, Hooks{
		Drain: func(ctx context.Context) error {
			return ctx.Err()
		},
		Close: func(ctx context.Context) error {
			closeContextErr = ctx.Err()
			return nil
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Drain() error = %v, want context canceled", err)
	}
	if closeContextErr != nil {
		t.Fatalf("close hook context error = %v, want nil", closeContextErr)
	}
	if got := machine.State(); got != Aborted {
		t.Fatalf("state after cancelled drain = %s, want aborted", got)
	}
}

func TestAbortPathCloseIsBoundedByCloseTimeout(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	realClock := clock.NewReal()
	closeStarted := make(chan struct{})
	releaseClose := make(chan struct{})
	release := func() {
		select {
		case <-releaseClose:
		default:
			close(releaseClose)
		}
	}
	t.Cleanup(release)
	done := make(chan error, 1)
	go func() {
		done <- machine.Drain(parent, Config{
			Clock:        fake,
			CloseTimeout: 7 * time.Second,
		}, Hooks{
			Drain: func(ctx context.Context) error {
				return ctx.Err()
			},
			Close: func(ctx context.Context) error {
				close(closeStarted)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-releaseClose:
					return nil
				}
			},
		})
	}()
	waitForClose := realClock.Timer(time.Second)
	defer waitForClose.Stop()
	select {
	case <-closeStarted:
	case <-waitForClose.C:
		release()
		waitForDone := realClock.Timer(time.Second)
		defer waitForDone.Stop()
		select {
		case <-done:
		case <-waitForDone.C:
		}
		t.Fatal("close hook did not start")
	}
	timerReady := make(chan struct{})
	go func() {
		fake.BlockUntil(1)
		close(timerReady)
	}()
	waitForTimer := realClock.Timer(time.Second)
	defer waitForTimer.Stop()
	select {
	case <-timerReady:
		fake.Advance(7 * time.Second)
	case <-waitForTimer.C:
		wake := fake.Timer(0)
		<-timerReady
		wake.Stop()
		release()
		waitForDoneAfterMissingTimer := realClock.Timer(time.Second)
		defer waitForDoneAfterMissingTimer.Stop()
		select {
		case <-done:
		case <-waitForDoneAfterMissingTimer.C:
		}
		t.Fatal("CloseTimeout timer was not registered")
	}
	waitForDrain := realClock.Timer(time.Second)
	defer waitForDrain.Stop()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Drain() error = %v, want context canceled", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Drain() error = %v, want close deadline exceeded", err)
		}
	case <-waitForDrain.C:
		release()
		waitForDoneAfterTimeout := realClock.Timer(time.Second)
		defer waitForDoneAfterTimeout.Stop()
		select {
		case <-done:
		case <-waitForDoneAfterTimeout.C:
		}
		t.Fatal("Drain() did not return after CloseTimeout")
	}
	if got := machine.State(); got != Aborted {
		t.Fatalf("state after abort close timeout = %s, want aborted", got)
	}
}

func TestSuccessPathCloseHonoursTheCallerContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	var closeContextErr error
	err := machine.Drain(ctx, Config{}, Hooks{
		Close: func(ctx context.Context) error {
			closeContextErr = ctx.Err()
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Drain() error = %v, want nil", err)
	}
	if closeContextErr != context.Canceled {
		t.Fatalf("close hook context error = %v, want context canceled", closeContextErr)
	}
	if got := machine.State(); got != Closed {
		t.Fatalf("state after successful drain = %s, want closed", got)
	}
}

func TestAbortPathCloseErrorReachesTheCaller(t *testing.T) {
	phaseErr := errors.New("drain failed")
	closeErr := errors.New("close failed")
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	err := machine.Drain(context.Background(), Config{}, Hooks{
		Drain: func(context.Context) error {
			return phaseErr
		},
		Close: func(context.Context) error {
			return closeErr
		},
	})
	if !errors.Is(err, phaseErr) {
		t.Fatalf("Drain() error = %v, want phase error %v", err, phaseErr)
	}
	if !errors.Is(err, closeErr) {
		t.Fatalf("Drain() error = %v, want close error %v", err, closeErr)
	}
	if got := machine.State(); got != Aborted {
		t.Fatalf("state after aborted drain = %s, want aborted", got)
	}
}

func TestDrainAbortsOnDeadline(t *testing.T) {
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	err := machine.Drain(context.Background(), Config{DrainTimeout: 10 * time.Millisecond}, Hooks{
		Drain: func(ctx context.Context) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		},
	})
	if !errors.Is(err, context.DeadlineExceeded) || machine.State() != Aborted {
		t.Fatalf("Drain() = %v, state = %s", err, machine.State())
	}
	select {
	case <-entered:
	default:
		t.Fatal("drain hook did not run")
	}
}

func TestDrainDeadlineUsesInjectedClock(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- machine.Drain(context.Background(), Config{Clock: fake, DrainTimeout: 2 * time.Second}, Hooks{
			Drain: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
		})
	}()
	fake.BlockUntil(1)
	fake.Advance(2 * time.Second)
	err := <-done
	if !errors.Is(err, context.DeadlineExceeded) || machine.State() != Aborted {
		t.Fatalf("Drain() = %v, state = %s", err, machine.State())
	}
}

func TestDrainRunsFlushErrors(t *testing.T) {
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	if err := machine.Drain(context.Background(), Config{}, Hooks{Drain: func(context.Context) error { return nil }, Flush: func(context.Context) error { return errors.New("flush failed") }}); err == nil || machine.State() != Aborted {
		t.Fatalf("flush failure = %v, state = %s", err, machine.State())
	}
}

func TestDrainRejectsNegativeTimeout(t *testing.T) {
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	called := false
	err := machine.Drain(context.Background(), Config{DrainTimeout: -time.Second}, Hooks{
		Drain: func(context.Context) error {
			called = true
			return nil
		},
	})
	if err == nil || called || machine.State() != Aborted {
		t.Fatalf("Drain() error = %v, called = %v, state = %s; want negative timeout rejection", err, called, machine.State())
	}
}

func TestDrainPassesIndependentHookTimeouts(t *testing.T) {
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	err := machine.Drain(context.Background(), Config{FlushTimeout: 10 * time.Millisecond}, Hooks{Flush: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}})
	if !errors.Is(err, context.DeadlineExceeded) || machine.State() != Aborted {
		t.Fatalf("flush timeout = %v, state = %s", err, machine.State())
	}
}

func TestDrainRejectsInvalidStartingState(t *testing.T) {
	if err := New().Drain(context.Background(), Config{}, Hooks{}); err == nil {
		t.Fatal("starting drain must be rejected")
	}
	var machine *Machine
	if err := machine.Drain(context.Background(), Config{}, Hooks{}); err == nil {
		t.Fatal("nil machine drain must fail")
	}
}

func TestDrainAbortsOnEachHookError(t *testing.T) {
	for _, phase := range []string{"drain", "settle", "flush", "close"} {
		t.Run(phase, func(t *testing.T) {
			machine := New()
			if err := machine.Transition(Ready); err != nil {
				t.Fatal(err)
			}
			hooks := Hooks{}
			fail := func(context.Context) error { return errors.New(phase + " failed") }
			switch phase {
			case "drain":
				hooks.Drain = fail
			case "settle":
				hooks.WaitSettled = fail
			case "flush":
				hooks.Flush = fail
			case "close":
				hooks.Close = fail
			}
			if err := machine.Drain(context.Background(), Config{}, hooks); err == nil || machine.State() != Aborted {
				t.Fatalf("Drain() = %v, state = %s", err, machine.State())
			}
		})
	}
}

func TestDrainReturnsHookAndParentErrors(t *testing.T) {
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	want := errors.New("flush failed before timeout")
	if err := machine.Drain(context.Background(), Config{FlushTimeout: time.Hour}, Hooks{Flush: func(context.Context) error { return want }}); !errors.Is(err, want) {
		t.Fatalf("Drain() error = %v, want %v", err, want)
	}

	machine = New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := machine.Drain(ctx, Config{DrainTimeout: time.Hour}, Hooks{Drain: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Drain() cancelled error = %v, want context canceled", err)
	}
	if machine.State() != Aborted {
		t.Fatalf("cancelled drain state = %s, want aborted", machine.State())
	}
}

func TestDrainFinishesWhenFailureOccursDuringSettlement(t *testing.T) {
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	var flushRan, closeRan bool
	err := machine.Drain(context.Background(), Config{}, Hooks{
		WaitSettled: func(context.Context) error {
			return machine.Transition(Failed)
		},
		Flush: func(context.Context) error {
			flushRan = true
			return nil
		},
		Close: func(context.Context) error {
			closeRan = true
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Drain() error = %v, want nil", err)
	}
	if got := machine.State(); got != Failed {
		t.Fatalf("state after failed settlement = %s, want failed", got)
	}
	if !flushRan || !closeRan {
		t.Fatalf("hooks ran: flush=%t close=%t, want both true", flushRan, closeRan)
	}
}

func TestDrainPreservesAlreadyFailedBehavior(t *testing.T) {
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	if err := machine.Transition(Failed); err != nil {
		t.Fatal(err)
	}
	var drainRan, waitRan, flushRan, closeRan bool
	err := machine.Drain(context.Background(), Config{}, Hooks{
		Drain: func(context.Context) error {
			drainRan = true
			return nil
		},
		WaitSettled: func(context.Context) error {
			waitRan = true
			return nil
		},
		Flush: func(context.Context) error {
			flushRan = true
			return nil
		},
		Close: func(context.Context) error {
			closeRan = true
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Drain() error = %v, want nil", err)
	}
	if got := machine.State(); got != Failed {
		t.Fatalf("state after already-failed drain = %s, want failed", got)
	}
	if drainRan || !waitRan || !flushRan || !closeRan {
		t.Fatalf("hooks ran: drain=%t wait=%t flush=%t close=%t", drainRan, waitRan, flushRan, closeRan)
	}
}

func TestDrainTransitionsOrdinaryPathToClosed(t *testing.T) {
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	states := make([]State, 0, 4)
	record := func(want State) func(context.Context) error {
		return func(context.Context) error {
			states = append(states, machine.State())
			if got := machine.State(); got != want {
				return errors.New("unexpected lifecycle state: " + got.String())
			}
			return nil
		}
	}
	if err := machine.Drain(context.Background(), Config{}, Hooks{
		Drain:       record(Draining),
		WaitSettled: record(Settling),
		Flush:       record(Flushing),
		Close:       record(Flushing),
	}); err != nil {
		t.Fatalf("Drain() error = %v, want nil", err)
	}
	want := []State{Draining, Settling, Flushing, Flushing}
	if len(states) != len(want) {
		t.Fatalf("hook states = %v, want %v", states, want)
	}
	for i := range want {
		if states[i] != want[i] {
			t.Fatalf("hook states = %v, want %v", states, want)
		}
	}
	if got := machine.State(); got != Closed {
		t.Fatalf("ordinary drain state = %s, want closed", got)
	}
}
