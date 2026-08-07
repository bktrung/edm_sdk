package clock

import (
	"context"
	"time"
)

// Clock provides time operations for timing-dependent components.
type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	Timer(d time.Duration) Timer
	Ticker(d time.Duration) Ticker
	Sleep(ctx context.Context, d time.Duration) error
}

// Timer fires once and can be stopped before it fires.
type Timer struct {
	C    <-chan time.Time
	stop func() bool
}

// Stop prevents the Timer from firing and reports whether it was stopped.
func (t Timer) Stop() bool { return t.stop() }

// Ticker fires repeatedly until stopped. Stop does not close C.
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
