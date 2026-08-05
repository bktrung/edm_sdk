package clock

import (
	"context"
	"time"
)

// Clock is the time source every timing-dependent component uses. Real
// wraps the standard library; Fake advances deterministically under test
// control, which is what makes the scheduler, retry ladder and drain FSM
// testable in milliseconds instead of minutes (doc 11 §2, doc 15 §7.4).
type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	Timer(d time.Duration) Timer
	Ticker(d time.Duration) Ticker
	Sleep(ctx context.Context, d time.Duration) error
}

// Timer mirrors the usable surface of time.Timer: a channel that fires
// once, and Stop to cancel it before it does.
type Timer struct {
	C    <-chan time.Time
	stop func() bool
}

// Stop prevents the Timer from firing, if it has not fired already. It
// returns false if the timer already fired or was already stopped.
func (t Timer) Stop() bool { return t.stop() }

// Ticker mirrors the usable surface of time.Ticker: a channel that fires
// repeatedly, and Stop to cancel it. Stop does not close C.
type Ticker struct {
	C    <-chan time.Time
	stop func()
}

// Stop turns off the ticker. It does not close C.
func (t Ticker) Stop() { t.stop() }

var (
	_ Clock = Real{}
	_ Clock = (*Fake)(nil)
)
