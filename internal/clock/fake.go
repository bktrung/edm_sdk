package clock

import (
	"context"
	"sync"
	"time"
)

// Fake is Clock under full test control: time only moves when Advance is
// called, so timer, ticker and sleep behaviour is deterministic (doc 11 §2,
// doc 15 §7.4).
type Fake struct {
	mu      sync.Mutex
	cond    *sync.Cond
	now     time.Time
	waiters []*waiter
}

type waiter struct {
	due      time.Time
	interval time.Duration // 0 for a one-shot Timer/Sleep; >0 for a Ticker
	c        chan time.Time
}

// NewFake returns a Fake starting at start. Pick a fixed value, not
// time.Now(), so a failing test is reproducible.
func NewFake(start time.Time) *Fake {
	f := &Fake{now: start}
	f.cond = sync.NewCond(&f.mu)
	return f
}

// Now returns the fake clock's current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Since returns the fake clock's current time minus t.
func (f *Fake) Since(t time.Time) time.Duration {
	return f.Now().Sub(t)
}

// Timer registers a one-shot waiter due in d, relative to the fake clock's
// current time. It fires when Advance passes its due time.
func (f *Fake) Timer(d time.Duration) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := f.registerLocked(d, 0)
	return Timer{C: w.c, stop: func() bool { return f.remove(w) }}
}

// Ticker registers a repeating waiter, first due in d and then every d
// after that, relative to the fake clock's current time. It panics for
// d <= 0, matching time.NewTicker - a Fake that instead degraded to a
// one-shot would let a computed-zero interval pass under test and panic in
// production.
func (f *Fake) Ticker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: non-positive interval for Ticker")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	w := f.registerLocked(d, d)
	return Ticker{C: w.c, stop: func() { f.remove(w) }}
}

// Sleep blocks until Advance passes a due time d after the current fake
// time, or until ctx is done. Callers that need to synchronize with a
// concurrent Advance call must use BlockUntil first - there is no other way
// to know a Sleep call has registered.
func (f *Fake) Sleep(ctx context.Context, d time.Duration) error {
	f.mu.Lock()
	w := f.registerLocked(d, 0)
	f.mu.Unlock()

	select {
	case <-w.c:
		return nil
	case <-ctx.Done():
		f.remove(w)
		return ctx.Err()
	}
}

// BlockUntil blocks the calling goroutine until at least n goroutines are
// registered as waiters on this Fake (via Timer, Ticker or Sleep). Tests
// use it to synchronize with concurrently started goroutines before calling
// Advance - without it there is no way to know a concurrent Sleep call has
// actually registered before the clock moves. >= rather than == so a test
// whose waiter count overshoots n fails forward instead of deadlocking.
func (f *Fake) BlockUntil(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for len(f.waiters) < n {
		f.cond.Wait()
	}
}

// NumWaiters returns how many waiters are currently registered. BlockUntil
// cannot answer this - it waits for at least n, so BlockUntil(0) is vacuously
// true and would pass against a waiter that leaked. Tests assert cleanup with
// this instead of reaching into the struct.
func (f *Fake) NumWaiters() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.waiters)
}

// Advance moves the fake clock forward by d, then fires every waiter whose
// due time is now at or before the new time, in due-time order - the
// earliest-due waiter fires first, regardless of registration order. Firing
// a waiter is a non-blocking send: a slow reader on a waiter that fires more
// than once before being read drops the earlier value, matching
// time.Ticker's own documented behaviour. Waiters due at the exact same
// instant fire in an unspecified relative order.
//
// A Ticker waiter is rescheduled for its next interval after firing, so a
// long Advance can fire it more than once, catching it up. A Timer or Sleep
// waiter is removed after firing.
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
