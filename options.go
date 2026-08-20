package f1

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"

	"go.opentelemetry.io/otel/metric"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/codec"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// Option configures a Client during New.
type Option func(*clientOptions) error

// TopologyPolicy selects how the SDK handles required broker topology.
type TopologyPolicy = driver.TopologyPolicy

const (
	// TopologyDeclare creates missing topology. Effective policy selection gives
	// an explicit WithTopology override priority, then uses topology.autoCreate;
	// topology.verifyOnStart selects verification, otherwise TopologyNone applies.
	TopologyDeclare = driver.TopologyDeclare
	// TopologyVerify checks that topology exists and creates nothing.
	TopologyVerify = driver.TopologyVerify
	// TopologyNone assumes topology exists and makes no broker round trip.
	TopologyNone = driver.TopologyNone
)

// PublisherOption configures a Publisher. No per-publisher controls are
// defined in this version; the type reserves the API extension point.
type PublisherOption struct{}

type clientOptions struct {
	driver            driver.Driver
	codec             codec.Codec
	logger            *slog.Logger
	meterProvider     metric.MeterProvider
	clock             clock.Clock
	strictPortability bool
	middleware        []Middleware
	errorHandler      func(context.Context, *Event, error)
	publishTopics     []string
	publishTopicsSet  bool
	topologyPolicy    driver.TopologyPolicy
	topologyPolicySet bool
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

// WithMeterProvider configures the meter provider used by the observability
// layer.
func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(options *clientOptions) error {
		if isNil(provider) {
			return fmt.Errorf("f1: WithMeterProvider requires a non-nil provider")
		}
		options.meterProvider = provider
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

// WithErrorHandler records a callback for asynchronous errors the caller has
// no other way to observe: driver-level errors read from the consumer's
// error stream, and a retry or dead-letter successor publish that exhausts
// its bounded republish budget. The event argument identifies which
// delivery the error is about; it is nil for a connection-level error that
// belongs to no message. The handler never receives an error returned by a
// subscription's own handler function - those already flow through the
// retry and dead-letter ladder, and reporting them here too would double
// them. The handler runs on its own goroutine with a bounded deadline: a
// slow or panicking handler is logged and abandoned, never allowed to stall
// delivery, settlement, or shutdown.
func WithErrorHandler(handler func(context.Context, *Event, error)) Option {
	return func(options *clientOptions) error {
		if handler == nil {
			return fmt.Errorf("f1: WithErrorHandler requires a non-nil handler")
		}
		options.errorHandler = handler
		return nil
	}
}

// WithPublishTopics names the logical topics this client publishes.
func WithPublishTopics(topics ...string) Option {
	return func(options *clientOptions) error {
		seen := make(map[string]struct{}, len(options.publishTopics)+len(topics))
		for _, topic := range options.publishTopics {
			seen[topic] = struct{}{}
		}
		canonical := make([]string, 0, len(topics))
		for _, topic := range topics {
			if topic == "" {
				return fmt.Errorf("f1: WithPublishTopics topic must not be empty")
			}
			logical := topicFor(topic)
			if _, ok := seen[logical]; ok {
				return fmt.Errorf("f1: WithPublishTopics contains duplicate topic %q", topic)
			}
			seen[logical] = struct{}{}
			canonical = append(canonical, logical)
		}
		options.publishTopics = append(options.publishTopics, canonical...)
		options.publishTopicsSet = true
		return nil
	}
}

// WithTopology selects how the SDK handles required broker topology and
// overrides configured policy. Without it, autoCreate selects declare,
// verifyOnStart selects verify, and neither setting selects none.
func WithTopology(p TopologyPolicy) Option {
	return func(options *clientOptions) error {
		if p < TopologyDeclare || p > TopologyNone {
			return fmt.Errorf("f1: WithTopology received unsupported policy %d", p)
		}
		options.topologyPolicy = p
		options.topologyPolicySet = true
		return nil
	}
}

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
