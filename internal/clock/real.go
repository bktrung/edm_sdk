package clock

import (
	"context"
	"time"
)

// Real is a Clock backed by the standard library.
type Real struct{}

// NewReal returns a real-time Clock.
func NewReal() Real { return Real{} }

// Now returns the current time.
func (Real) Now() time.Time { return time.Now() }

// Since returns the elapsed time since t.
func (Real) Since(t time.Time) time.Duration { return time.Since(t) }

// Timer returns a timer that fires once after d.
func (Real) Timer(d time.Duration) Timer {
	rt := time.NewTimer(d)
	return Timer{C: rt.C, stop: rt.Stop}
}

// Ticker returns a ticker that fires every d.
func (Real) Ticker(d time.Duration) Ticker {
	rt := time.NewTicker(d)
	return Ticker{C: rt.C, stop: rt.Stop}
}

// Sleep waits for d or until ctx is done.
func (Real) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
