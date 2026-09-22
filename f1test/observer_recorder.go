package f1test

import (
	"context"
	"sync"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
)

// Recorder is a concurrency-safe f1.Observer that keeps every call in call order.
// The zero value is not usable; construct one with NewRecorder. Recorder methods
// may be called concurrently from any goroutine.
type Recorder struct {
	mu      sync.Mutex
	calls   []ObserverCall
	changed chan struct{}
	next    uint64
}

// NewRecorder returns an initialized Recorder ready for concurrent observer calls.
func NewRecorder() *Recorder {
	return &Recorder{changed: make(chan struct{})}
}

// Start records a start call and returns ctx unchanged with a token whose handle
// counts starts from one. Start is safe for concurrent calls.
func (r *Recorder) Start(ctx context.Context, event f1.StartEvent) (context.Context, f1.Token) {
	r.mu.Lock()
	r.next++
	token := f1.Token{Kind: event.Kind, Start: event.At, Handle: r.next}
	r.appendLocked(ObserverCall{
		Op:    ObserverOpStart,
		Kind:  event.Kind,
		Token: token,
		Start: event,
	})
	r.mu.Unlock()
	return ctx, token
}

// Finish records a finish call with its token. Finish is safe for concurrent calls.
func (r *Recorder) Finish(token f1.Token, event f1.FinishEvent) {
	r.mu.Lock()
	r.appendLocked(ObserverCall{
		Op:     ObserverOpFinish,
		Kind:   event.Kind,
		Token:  token,
		Finish: event,
	})
	r.mu.Unlock()
}

// Record records a point call. Record is safe for concurrent calls.
func (r *Recorder) Record(event f1.PointEvent) {
	r.mu.Lock()
	r.appendLocked(ObserverCall{
		Op:    ObserverOpRecord,
		Kind:  event.Kind,
		Point: event,
	})
	r.mu.Unlock()
}

// Calls returns a copy of every call so far, in call order. Calls is safe for
// concurrent use and does not expose Recorder's internal slice or finish results.
func (r *Recorder) Calls() []ObserverCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneCalls(r.calls)
}

// Wait blocks until done reports true for the calls so far, or ctx ends. Wait
// invokes done outside Recorder's mutex and is safe to use concurrently with
// observer calls and other Wait calls.
func (r *Recorder) Wait(ctx context.Context, done func([]ObserverCall) bool) error {
	for {
		r.mu.Lock()
		changed := r.changed
		calls := cloneCalls(r.calls)
		r.mu.Unlock()
		if done(calls) {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ObserverOp identifies the operation represented by an ObserverCall.
type ObserverOp string

const (
	// ObserverOpStart identifies a recorded Observer.Start call.
	ObserverOpStart ObserverOp = "start"
	// ObserverOpFinish identifies a recorded Observer.Finish call.
	ObserverOpFinish ObserverOp = "finish"
	// ObserverOpRecord identifies a recorded Observer.Record call.
	ObserverOpRecord ObserverOp = "record"
)

// ObserverCall is one observer call. Start, Finish and Point hold the event of
// the matching Op; the other two are zero. Its zero value represents no call.
type ObserverCall struct {
	Op     ObserverOp
	Kind   f1.ObserverKind
	Token  f1.Token
	Start  f1.StartEvent
	Finish f1.FinishEvent
	Point  f1.PointEvent
}

func (r *Recorder) appendLocked(call ObserverCall) {
	r.calls = append(r.calls, cloneCall(call))
	if r.changed == nil {
		return
	}
	close(r.changed)
	r.changed = make(chan struct{})
}

func cloneCalls(calls []ObserverCall) []ObserverCall {
	if calls == nil {
		return nil
	}
	out := make([]ObserverCall, len(calls))
	for i, call := range calls {
		out[i] = cloneCall(call)
	}
	return out
}

func cloneCall(call ObserverCall) ObserverCall {
	if call.Finish.Results != nil {
		call.Finish.Results = append([]f1.MessageResult(nil), call.Finish.Results...)
	}
	return call
}
