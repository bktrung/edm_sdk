package f1

import (
	"context"
	"fmt"
	"runtime/debug"
)

// Handler processes one delivered Event.
type Handler interface {
	Handle(context.Context, *Event) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context, *Event) error

// Handle invokes f for the event.
func (f HandlerFunc) Handle(ctx context.Context, event *Event) error {
	return f(ctx, event)
}

// Middleware wraps a Handler in the Client's dispatch chain.
type Middleware func(Handler) Handler

type middlewareSpec struct {
	name string
	wrap Middleware
}

// builtInMiddlewareSpecs is the immutable part of the handler pipeline. The
// order is the public contract: user middleware is wrapped inside this list.
func builtInMiddlewareSpecs() []middlewareSpec {
	return []middlewareSpec{
		{name: "recover", wrap: recoverMiddleware},
		{name: "tracing", wrap: tracingMiddleware},
		{name: "metrics", wrap: metricsMiddleware},
		{name: "logging", wrap: loggingMiddleware},
		{name: "timeout", wrap: timeoutMiddleware},
		{name: "retry-classify", wrap: retryClassifyMiddleware},
	}
}

// Invariant: buildHandlerChain always keeps the six built-in stages in this
// order and wraps user middleware inside them; settlement remains outside the
// handler chain.
func buildHandlerChain(user []Middleware, handler Handler) Handler {
	if handler == nil {
		return nil
	}
	chain := handler
	for i := len(user) - 1; i >= 0; i-- {
		if user[i] != nil {
			chain = user[i](chain)
		}
	}
	specs := builtInMiddlewareSpecs()
	for i := len(specs) - 1; i >= 0; i-- {
		chain = specs[i].wrap(chain)
	}
	return chain
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

// The observability layers are structural in the core skeleton. Their
// providers and instruments are wired by the observability phase; keeping the
// layers here fixes the public ordering without duplicating dispatch logic.
func tracingMiddleware(next Handler) Handler { return identityMiddleware(next) }

func metricsMiddleware(next Handler) Handler { return identityMiddleware(next) }

func loggingMiddleware(next Handler) Handler { return identityMiddleware(next) }

// invokeHandler owns the cooperative timeout and stuck-worker policy. The
// timeout layer in the chain is therefore structural and receives the same
// deadline through handlerCtx rather than creating a second goroutine.
func timeoutMiddleware(next Handler) Handler { return identityMiddleware(next) }

// dispatchMessage owns error classification and settlement. This layer keeps
// that decision at the settle-last boundary while fixing its position in the
// handler chain.
func retryClassifyMiddleware(next Handler) Handler { return identityMiddleware(next) }

func identityMiddleware(next Handler) Handler {
	return HandlerFunc(func(ctx context.Context, event *Event) error {
		return next.Handle(ctx, event)
	})
}
