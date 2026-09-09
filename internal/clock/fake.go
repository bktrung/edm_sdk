package clock

import (
	"context"
	"sync"
	"time"
)

// Fake is a manually advanced Clock for deterministic tests.
type Fake struct {
	mu      sync.Mutex
	cond    *sync.Cond
	now     time.Time
	waiters []*waiter
}

type waiter struct {
	due      time.Time
	interval time.Duration // Zero for one-shot waiters.
	c        chan time.Time
}

// NewFake returns a Fake initialized at start.
func NewFake(start time.Time) *Fake {
	f := &Fake{now: start}
	f.cond = sync.NewCond(&f.mu)
	return f
}

// Now returns the current fake time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Since returns the elapsed fake time since t.
func (f *Fake) Since(t time.Time) time.Duration {
	return f.Now().Sub(t)
}

// Timer returns a one-shot timer due after d.
func (f *Fake) Timer(d time.Duration) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := f.registerLocked(d, 0)
	return Timer{C: w.c, stop: func() bool { return f.remove(w) }}
}

// Ticker returns a repeating ticker. It panics when d is not positive.
func (f *Fake) Ticker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: non-positive interval for Ticker")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	w := f.registerLocked(d, d)
	return Ticker{C: w.c, stop: func() { f.remove(w) }}
}

// Sleep waits until Advance reaches d or until ctx is done. Use BlockUntil
// before advancing when Sleep is started in another goroutine.
func (f *Fake) Sleep(ctx context.Context, d time.Duration) error {
	return f.sleep(ctx, d, nil)
}

// SleepWithRegistration waits like Sleep and calls registered exactly once after
// its waiter is registered, before it waits for the waiter to fire or ctx. The
// callback runs without Fake's mutex held and may call Fake methods. A nil
// callback is allowed; use NewFake rather than a zero Fake.
func (f *Fake) SleepWithRegistration(ctx context.Context, d time.Duration, registered func()) error {
	return f.sleep(ctx, d, registered)
}

func (f *Fake) sleep(ctx context.Context, d time.Duration, registered func()) error {
	f.mu.Lock()
	w := f.registerLocked(d, 0)
	f.mu.Unlock()
	if registered != nil {
		registered()
	}

	select {
	case <-w.c:
		return nil
	case <-ctx.Done():
		f.remove(w)
		return ctx.Err()
	}
}

// BlockUntil waits until at least n waiters are registered.
func (f *Fake) BlockUntil(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for len(f.waiters) < n {
		f.cond.Wait()
	}
}

// NumWaiters returns the number of registered waiters.
func (f *Fake) NumWaiters() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.waiters)
}

// Advance moves time forward and fires every due waiter. Tickers are
// rescheduled; one-shot waiters are removed. Sends are non-blocking, and
// waiters due at the same time have unspecified relative order.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.now = f.now.Add(d)

	for {
		i := earliestDue(f.waiters, f.now)
		if i == -1 {
			break
		}
		w := f.waiters[i]
		select {
		case w.c <- f.now:
		default:
		}
		if w.interval > 0 {
			w.due = w.due.Add(w.interval)
		} else {
			f.waiters = append(f.waiters[:i], f.waiters[i+1:]...)
		}
	}
	f.cond.Broadcast()
}

func (f *Fake) registerLocked(d, interval time.Duration) *waiter {
	w := &waiter{
		due:      f.now.Add(d),
		interval: interval,
		c:        make(chan time.Time, 1),
	}
	f.waiters = append(f.waiters, w)
	f.cond.Broadcast()
	return w
}

func (f *Fake) remove(w *waiter) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, cur := range f.waiters {
		if cur == w {
			f.waiters = append(f.waiters[:i], f.waiters[i+1:]...)
			f.cond.Broadcast()
			return true
		}
	}
	return false
}

func earliestDue(waiters []*waiter, now time.Time) int {
	idx := -1
	for i, w := range waiters {
		if w.due.After(now) {
			continue
		}
		if idx == -1 || w.due.Before(waiters[idx].due) {
			idx = i
		}
	}
	return idx
}
