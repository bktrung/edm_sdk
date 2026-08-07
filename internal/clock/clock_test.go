package clock_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestFake_AdvanceFiresTimersInOrder(t *testing.T) {
	t.Parallel()

	fc := clock.NewFake(time.Unix(0, 0))

	// Register out of due-time order to verify firing uses due time.
	t3 := fc.Timer(3 * time.Second)
	t1 := fc.Timer(1 * time.Second)
	t2 := fc.Timer(2 * time.Second)

	fc.Advance(1 * time.Second)
	requireFired(t, t1.C)
	requireNotFired(t, t2.C)
	requireNotFired(t, t3.C)

	fc.Advance(1 * time.Second)
	requireFired(t, t2.C)
	requireNotFired(t, t3.C)

	fc.Advance(1 * time.Second)
	requireFired(t, t3.C)
}

func TestFake_SleepIsDeterministicUnderParallel(t *testing.T) {
	t.Parallel()

	fc := clock.NewFake(time.Unix(0, 0))

	const n = 50
	results := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			results <- fc.Sleep(context.Background(), time.Second)
		}()
	}

	// Synchronize registration before advancing the clock.
	fc.BlockUntil(n)
	fc.Advance(time.Second)

	for i := 0; i < n; i++ {
		require.NoError(t, <-results)
	}
}

func TestFake_NowAndSinceTrackAdvance(t *testing.T) {
	t.Parallel()

	start := time.Unix(0, 0)
	fc := clock.NewFake(start)
	require.Equal(t, start, fc.Now())

	fc.Advance(90 * time.Second)
	require.Equal(t, start.Add(90*time.Second), fc.Now())
	require.Equal(t, 90*time.Second, fc.Since(start))
}

func TestFake_TimerStopPreventsFiring(t *testing.T) {
	t.Parallel()

	fc := clock.NewFake(time.Unix(0, 0))
	tm := fc.Timer(time.Second)

	require.True(t, tm.Stop(), "Stop before firing must report true")
	require.False(t, tm.Stop(), "a second Stop on an already-removed Timer must report false")

	fc.Advance(2 * time.Second)
	requireNotFired(t, tm.C)
}

func TestFake_TickerStopStopsFiring(t *testing.T) {
	t.Parallel()

	fc := clock.NewFake(time.Unix(0, 0))
	tk := fc.Ticker(time.Second)

	fc.Advance(time.Second)
	requireFired(t, tk.C)

	tk.Stop()
	fc.Advance(2 * time.Second)
	requireNotFired(t, tk.C)
}

func TestFake_TickerFiresOnEveryInterval(t *testing.T) {
	t.Parallel()

	fc := clock.NewFake(time.Unix(0, 0))
	tk := fc.Ticker(time.Second)

	// Each advance should reschedule the ticker.
	for i := 0; i < 3; i++ {
		fc.Advance(time.Second)
		requireFired(t, tk.C)
	}
}

func TestFake_TickerCatchesUpButDropsExtraTicks(t *testing.T) {
	t.Parallel()

	fc := clock.NewFake(time.Unix(0, 0))
	tk := fc.Ticker(time.Second)

	// A long advance catches up the ticker, but the buffered channel keeps one tick.
	fc.Advance(3 * time.Second)
	requireFired(t, tk.C)
	requireNotFired(t, tk.C)

	// Catch-up should leave the next due time one interval ahead.
	fc.Advance(time.Second)
	requireFired(t, tk.C)
}

func TestFake_TickerPanicsOnNonPositiveInterval(t *testing.T) {
	t.Parallel()

	fc := clock.NewFake(time.Unix(0, 0))
	require.Panics(t, func() { fc.Ticker(0) })
	require.Panics(t, func() { fc.Ticker(-time.Second) })
}

func TestFake_SleepReturnsCtxErrorOnCancel(t *testing.T) {
	t.Parallel()

	fc := clock.NewFake(time.Unix(0, 0))
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() { errCh <- fc.Sleep(ctx, time.Hour) }()

	fc.BlockUntil(1)
	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)

	// Cancellation must remove the waiter.
	require.Zero(t, fc.NumWaiters())
}

func TestFake_StopUnregistersWaiters(t *testing.T) {
	t.Parallel()

	fc := clock.NewFake(time.Unix(0, 0))
	tm := fc.Timer(time.Hour)
	tk := fc.Ticker(time.Hour)
	require.Equal(t, 2, fc.NumWaiters())

	require.True(t, tm.Stop())
	tk.Stop()
	require.Zero(t, fc.NumWaiters())
}

func TestReal_NowIsBetweenCallBounds(t *testing.T) {
	t.Parallel()

	r := clock.NewReal()
	before := time.Now()
	got := r.Now()
	after := time.Now()

	require.False(t, got.Before(before))
	require.False(t, got.After(after))
}

func TestReal_SinceMeasuresElapsedTime(t *testing.T) {
	t.Parallel()

	r := clock.NewReal()
	past := r.Now().Add(-time.Hour)
	require.InDelta(t, time.Hour.Seconds(), r.Since(past).Seconds(), 1)
}

func TestReal_TimerFiresAndStops(t *testing.T) {
	t.Parallel()

	r := clock.NewReal()

	tm := r.Timer(5 * time.Millisecond)
	select {
	case <-tm.C:
	case <-time.After(time.Second):
		t.Fatal("timer did not fire")
	}

	// A fired timer cannot be stopped.
	require.False(t, tm.Stop())
}

func TestReal_TickerFiresRepeatedly(t *testing.T) {
	t.Parallel()

	r := clock.NewReal()
	tk := r.Ticker(5 * time.Millisecond)
	defer tk.Stop()

	for i := 0; i < 2; i++ {
		select {
		case <-tk.C:
		case <-time.After(time.Second):
			t.Fatal("ticker did not fire")
		}
	}
}

func TestReal_TickerPanicsOnNonPositiveInterval(t *testing.T) {
	t.Parallel()

	r := clock.NewReal()
	require.Panics(t, func() { r.Ticker(0) })
}

func TestReal_SleepReturnsAfterDuration(t *testing.T) {
	t.Parallel()

	r := clock.NewReal()
	require.NoError(t, r.Sleep(context.Background(), 5*time.Millisecond))
}

func TestReal_SleepReturnsCtxErrorOnCancel(t *testing.T) {
	t.Parallel()

	r := clock.NewReal()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, r.Sleep(ctx, time.Hour), context.Canceled)
}

func requireFired(t *testing.T, c <-chan time.Time) {
	t.Helper()
	select {
	case <-c:
	default:
		t.Fatal("expected the timer to have fired")
	}
}

func requireNotFired(t *testing.T, c <-chan time.Time) {
	t.Helper()
	select {
	case <-c:
		t.Fatal("expected the timer not to have fired yet")
	default:
	}
}
