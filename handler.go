package f1

import (
	"context"
	"fmt"
	"runtime/debug"
	"slices"
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
