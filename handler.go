package f1

import (
	"context"
	"fmt"
	"runtime/debug"
	"slices"
)

// Handler processes one delivered Event. Returning nil acknowledges the
// delivery; errors are retried unless marked with [Terminal] or [Drop]. F1 may
// call Handle concurrently according to Subscription.Concurrency.
type Handler interface {
	// Handle processes one delivery and returns its outcome.
	Handle(context.Context, *Event) error
}

// HandlerFunc adapts a function to Handler. Its zero value is nil and panics
// if called.
type HandlerFunc func(context.Context, *Event) error

// Handle calls f with ctx and event and returns its error unchanged.
func (f HandlerFunc) Handle(ctx context.Context, event *Event) error {
	return f(ctx, event)
}

// Typed adapts a typed payload function to a Handler. On success, it decodes
// the payload into T and calls fn with the same *Event a plain Handler would
// receive, so the event ID, attempt and idempotency key remain reachable. On a
// decode failure, Typed returns a terminal error so the delivery reaches the
// dead-letter destination on the first attempt instead of being retried.
func Typed[T any](fn func(context.Context, *Event, T) error) Handler {
	return HandlerFunc(func(ctx context.Context, event *Event) error {
		var payload T
		if err := event.Decode(&payload); err != nil {
			return Terminal(err)
		}
		return fn(ctx, event, payload)
	})
}

// Middleware wraps a Handler. Middleware supplied to WithMiddleware is applied
// in order, with the first middleware outermost.
type Middleware func(Handler) Handler

// buildHandlerChain applies user middleware in submission order and keeps
// panic recovery outermost. Classification and settlement remain outside the
// chain in dispatchMessage.
func buildHandlerChain(user []Middleware, handler Handler) Handler {
	if handler == nil {
		return nil
	}
	chain := handler
	for _, u := range slices.Backward(user) {
		if u != nil {
			chain = u(chain)
		}
	}
	return recoverMiddleware(chain)
}

type handlerPanicError struct {
	value any
	stack []byte
}

func (e *handlerPanicError) Error() string {
	return fmt.Sprintf("handler panic: %v\n%s", e.value, e.stack)
}

func recoverMiddleware(next Handler) Handler {
	return HandlerFunc(func(ctx context.Context, event *Event) (err error) {
		defer func() {
			if value := recover(); value != nil {
				err = &handlerPanicError{value: value, stack: debug.Stack()}
			}
		}()
		return next.Handle(ctx, event)
	})
}
