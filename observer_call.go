package f1

import (
	"context"
	"fmt"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// observeStart invokes Start on the client's observer and falls back to the
// caller context when the observer returns nil. The caller checks for nil
// before building the event, so this helper assumes a non-nil observer.
func (c *Client) observeStart(ctx context.Context, event StartEvent) (context.Context, Token) {
	next := ctx
	token := Token{Kind: event.Kind, Start: event.At}
	callObserver(c, event.Kind, "start", func(observer Observer) {
		next, token = observer.Start(ctx, event)
	})
	if next == nil {
		next = ctx
	}
	return next, token
}

// observeFinish invokes Finish on the client's observer. The caller checks
// for nil before building the event, so this helper assumes a non-nil
// observer.
func (c *Client) observeFinish(token Token, event FinishEvent) {
	callObserver(c, event.Kind, "finish", func(observer Observer) {
		observer.Finish(token, event)
	})
}

// observeRecord invokes Record on the client's observer. The caller checks
// for nil before building the event, so this helper assumes a non-nil
// observer.
func (c *Client) observeRecord(event PointEvent) {
	callObserver(c, event.Kind, "record", func(observer Observer) {
		observer.Record(event)
	})
}

// callObserver runs call against the client's observer and contains any
// panic, reporting it once per kind through the client logger. The message
// outcome is unchanged.
func callObserver(c *Client, kind ObserverKind, phase string, call func(Observer)) {
	observer := c.observerRef()
	if observer == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			c.recordObserverPanic(kind, phase, recovered)
		}
	}()
	call(observer)
}

// observerRef returns the client's observer, or nil when none is set.
func (c *Client) observerRef() Observer {
	if c == nil {
		return nil
	}
	return c.observer
}

// recordObserverPanic logs the first panic per kind. It takes c.mu and never
// calls the observer while holding it.
func (c *Client) recordObserverPanic(kind ObserverKind, phase string, recovered any) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if _, seen := c.observerPanics[kind]; seen {
		c.mu.Unlock()
		return
	}
	c.observerPanics[kind] = struct{}{}
	c.mu.Unlock()
	lastResortClientLogger(c).Warn("f1 observer panicked", "kind", kind, "phase", phase, "panic", fmt.Sprint(recovered))
}

// observerFinishGuard ties one Start to its exactly-once Finish. Hold it as
// an addressable value and defer abandon directly so the deferred call
// observes a later finish:
//
//	guard := c.newObserverGuard(kind, token)
//	defer guard.abandon()
//	guard.finishWith(FinishEvent{Outcome: ObserverOutcomeOK})
//
// abandon has a pointer receiver, so the defer takes &guard at the defer
// statement and sees a later finishWith. A closure works too but is needless.
type observerFinishGuard struct {
	client   *Client
	kind     ObserverKind
	token    Token
	finished bool
}

// newObserverGuard returns the guard for one started stage.
func (c *Client) newObserverGuard(kind ObserverKind, token Token) observerFinishGuard {
	return observerFinishGuard{client: c, kind: kind, token: token}
}

// finishWith emits one Finish carrying the caller-built fields. It fills
// Kind and At from the guard and the client clock, emits once, and marks
// the guard finished so a deferred abandon is a no-op.
func (g *observerFinishGuard) finishWith(event FinishEvent) {
	if g == nil || g.client == nil {
		return
	}
	if g.finished {
		return
	}
	g.finished = true
	event.Kind = g.kind
	event.At = g.client.options.clock.Now()
	g.client.observeFinish(g.token, event)
}

// abandon emits an abandoned Finish unless finish already ran.
func (g *observerFinishGuard) abandon() {
	if g == nil || g.client == nil {
		return
	}
	if g.finished {
		return
	}
	g.finished = true
	event := FinishEvent{
		Kind:    g.kind,
		At:      g.client.options.clock.Now(),
		Outcome: ObserverOutcomeAbandoned,
	}
	g.client.observeFinish(g.token, event)
}

// errorClassOf maps err to the bounded ErrorClass carried on events. Nil
// gives the empty class. It tests the handler's own classification first, in
// the same order processOutcome uses: IsTerminal gives f1_terminal and
// IsDropped gives f1_dropped, and an error carrying both a driver kind and one
// of those markers reports the marker wherever it appears. Otherwise a
// driver.Classify classified error gives driver_<kind>, and anything else
// gives _OTHER. Death reasons ride on later consume-path events, not here.
func errorClassOf(err error) ErrorClass {
	if err == nil {
		return ""
	}
	if IsTerminal(err) {
		return ErrorClassTerminal
	}
	if IsDropped(err) {
		return ErrorClassDropped
	}
	if kind, classified := driver.Classify(err); classified {
		switch kind {
		case driver.KindFatal:
			return ErrorClassDriverFatal
		case driver.KindNotFound:
			return ErrorClassDriverNotFound
		case driver.KindTooLarge:
			return ErrorClassDriverTooLarge
		case driver.KindPermission:
			return ErrorClassDriverPermission
		case driver.KindNotification:
			return ErrorClassDriverNotification
		default:
			return ErrorClassDriverTransient
		}
	}
	return ErrorClassOther
}

// injectTrace calls InjectTrace on the client's injector and contains any
// panic, recording it once per kind through the client logger. On panic or
// a nil injector it returns empty strings so the caller keeps the caused-by
// copy and the publish never fails on injection.
func (c *Client) injectTrace(ctx context.Context) (traceParent, traceState string) {
	if c == nil {
		return "", ""
	}
	injector := c.traceInjector
	if injector == nil {
		return "", ""
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			c.recordObserverPanic(ObserverMessageBuilt, "inject", recovered)
			traceParent, traceState = "", ""
		}
	}()
	return injector.InjectTrace(ctx)
}
