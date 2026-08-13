package lifecycle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestDrainRunsPhasesInOrder(t *testing.T) {
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	var phases []string
	add := func(name string) func(context.Context) error {
		return func(context.Context) error { phases = append(phases, name); return nil }
	}
	var cancelled atomic.Bool
	err := machine.Drain(context.Background(), Config{}, Hooks{
		Drain:         add("drain"),
		CancelHandler: func() { phases = append(phases, "cancel"); cancelled.Store(true) },
		WaitSettled:   add("settle"),
		Flush:         add("flush"),
		Close:         add("close"),
	})
	if err != nil || machine.State() != Closed || !cancelled.Load() {
		t.Fatalf("Drain() = %v, state = %s, cancelled = %v", err, machine.State(), cancelled.Load())
	}
	want := []string{"drain", "cancel", "settle", "flush", "close"}
	if got := phases; len(got) != len(want) {
		t.Fatalf("phases = %v, want %v", got, want)
	}
	for i := range phases {
		if phases[i] != want[i] {
			t.Fatalf("phases = %v, want %v", phases, want)
		}
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

func TestDrainRunsPreStopAndFlushErrors(t *testing.T) {
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	if err := machine.Drain(context.Background(), Config{PreStopDelay: time.Millisecond}, Hooks{Drain: func(context.Context) error { return nil }, Flush: func(context.Context) error { return errors.New("flush failed") }}); err == nil || machine.State() != Aborted {
		t.Fatalf("flush failure = %v, state = %s", err, machine.State())
	}
}

func TestDrainHonorsCancellationDuringPreStop(t *testing.T) {
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := machine.Drain(ctx, Config{PreStopDelay: time.Second}, Hooks{}); !errors.Is(err, context.Canceled) || machine.State() != Aborted {
		t.Fatalf("cancelled drain = %v, state = %s", err, machine.State())
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

func TestDrainCancelsHandlerAfterGrace(t *testing.T) {
	machine := New()
	if err := machine.Transition(Ready); err != nil {
		t.Fatal(err)
	}
	called := make(chan struct{})
	err := machine.Drain(context.Background(), Config{DrainTimeout: 5 * time.Millisecond, HandlerGrace: 4 * time.Millisecond}, Hooks{CancelHandler: func() { close(called) }})
	if err != nil || machine.State() != Closed {
		t.Fatalf("Drain() = %v, state = %s", err, machine.State())
	}
	select {
	case <-called:
	default:
		t.Fatal("handler cancellation was not called")
	}
}
