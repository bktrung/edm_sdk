package f1

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/codec"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// Option configures a Client during New.
type Option func(*clientOptions) error

type clientOptions struct {
	driver            driver.Driver
	codec             codec.Codec
	logger            *slog.Logger
	meterProvider     metric.MeterProvider
	tracerProvider    trace.TracerProvider
	clock             clock.Clock
	strictPortability bool
	middleware        []Middleware
	errorHandler      func(context.Context, *Event, error)
}

// WithDriver supplies the broker driver New opens eagerly.
func WithDriver(d driver.Driver) Option {
	return func(options *clientOptions) error {
		if isNil(d) {
			return fmt.Errorf("f1: WithDriver requires a non-nil driver")
		}
		options.driver = d
		return nil
	}
}

// WithCodec selects the codec used by publishers and consumers.
func WithCodec(c codec.Codec) Option {
	return func(options *clientOptions) error {
		if isNil(c) {
			return fmt.Errorf("f1: WithCodec requires a non-nil codec")
		}
		options.codec = c
		return nil
	}
}

// WithLogger sets the structured logger used by the Client.
func WithLogger(logger *slog.Logger) Option {
	return func(options *clientOptions) error {
		if logger == nil {
			return fmt.Errorf("f1: WithLogger requires a non-nil logger")
		}
		options.logger = logger
		return nil
	}
}

// WithMeterProvider records a meter provider for the observability layer.
// It is retained until the metrics integration is implemented.
func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(options *clientOptions) error {
		if isNil(provider) {
			return fmt.Errorf("f1: WithMeterProvider requires a non-nil provider")
		}
		options.meterProvider = provider
		return nil
	}
}

// WithTracerProvider records a tracer provider for the observability layer.
// It is retained until the tracing integration is implemented.
func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(options *clientOptions) error {
		if isNil(provider) {
			return fmt.Errorf("f1: WithTracerProvider requires a non-nil provider")
		}
		options.tracerProvider = provider
		return nil
	}
}

// WithClock supplies the clock used by time-dependent core behavior in tests.
func WithClock(c clock.Clock) Option {
	return func(options *clientOptions) error {
		if isNil(c) {
			return fmt.Errorf("f1: WithClock requires a non-nil clock")
		}
		options.clock = c
		return nil
	}
}

// WithStrictPortability disables native capability shortcuts for this Client.
func WithStrictPortability() Option {
	return func(options *clientOptions) error {
		options.strictPortability = true
		return nil
	}
}

// WithMiddleware records middleware for the handler pipeline.
// Handler composition is implemented with dispatch.
func WithMiddleware(middleware ...Middleware) Option {
	return func(options *clientOptions) error {
		for _, item := range middleware {
			if isNil(item) {
				return fmt.Errorf("f1: WithMiddleware requires non-nil middleware")
			}
		}
		options.middleware = append(options.middleware, middleware...)
		return nil
	}
}

// WithErrorHandler records an asynchronous driver-error callback.
// Dispatch wires it when consumers are introduced.
func WithErrorHandler(handler func(context.Context, *Event, error)) Option {
	return func(options *clientOptions) error {
		if handler == nil {
			return fmt.Errorf("f1: WithErrorHandler requires a non-nil handler")
		}
		options.errorHandler = handler
		return nil
	}
}

// Event is the message view passed to a Handler.
// Dispatch fills its data when subscriptions are introduced.
type Event struct{}

// Handler processes one delivered Event.
type Handler interface {
	Handle(context.Context, *Event) error
}

// Middleware wraps a Handler in the Client's dispatch chain.
type Middleware func(Handler) Handler

func isNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
