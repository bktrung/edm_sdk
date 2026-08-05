package clock

import (
	"context"
	"time"
)

// Real is Clock backed by the standard library. Production code uses this;
// tests use Fake instead (doc 15 §7.4).
type Real struct{}

// NewReal returns the standard-library-backed Clock.
func NewReal() Real { return Real{} }

// Now returns the current wall-clock time.
func (Real) Now() time.Time { return time.Now() }

// Since returns the time elapsed since t.
func (Real) Since(t time.Time) time.Duration { return time.Since(t) }

// Timer starts a real timer that fires once after d.
func (Real) Timer(d time.Duration) Timer {
	rt := time.NewTimer(d)
	return Timer{C: rt.C, stop: rt.Stop}
}

// Ticker starts a real ticker that fires every d.
func (Real) Ticker(d time.Duration) Ticker {
	rt := time.NewTicker(d)
	return Ticker{C: rt.C, stop: rt.Stop}
}

// Sleep blocks for d, or until ctx is done, whichever comes first.
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
