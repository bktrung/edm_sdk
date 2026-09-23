package f1

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/codec"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/testhook"
)

// Option configures a Client during New. A nil Option is rejected by New.
type Option func(*clientOptions) error

// TopologyPolicy selects how the SDK handles required broker topology. Its zero
// value, TopologyDeclare, creates missing topology.
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

type clientOptions struct {
	driver              driver.Driver
	codec               codec.Codec
	codecsByContentType map[string]codec.Codec
	codecsByName        map[string]codec.Codec
	logger              *slog.Logger
	clock               clock.Clock
	strictPortability   bool
	middleware          []Middleware
	errorHandler        func(context.Context, *Event, error)
	publishTopics       []string
	publishTopicsSet    bool
	topologyPolicy      driver.TopologyPolicy
	topologyPolicySet   bool
	observer            Observer
	backlogPollInterval time.Duration
}

// WithDriver supplies the non-nil broker driver that New opens before it
// returns.
func WithDriver(d driver.Driver) Option {
	return func(options *clientOptions) error {
		if isNil(d) {
			return fmt.Errorf("f1: WithDriver requires a non-nil driver")
		}
		options.driver = d
		return nil
	}
}

// WithCodec registers codecs for inbound decoding. The first codec is used for
// publishing; reads select a codec by content type. New rejects an empty list,
// a nil codec, and two codecs in the list that share a content type or a name,
// since publishing would use the first and reading the last. A codec may
// replace the built-in JSON codec by using its content type.
func WithCodec(codecs ...codec.Codec) Option {
	return func(options *clientOptions) error {
		if len(codecs) == 0 {
			return fmt.Errorf("f1: WithCodec requires at least one codec")
		}
		contentTypes := make(map[string]struct{}, len(codecs))
		names := make(map[string]struct{}, len(codecs))
		for _, c := range codecs {
			if isNil(c) {
				return fmt.Errorf("f1: WithCodec requires a non-nil codec")
			}
			if _, dup := contentTypes[c.ContentType()]; dup {
				return fmt.Errorf("f1: WithCodec lists two codecs for content type %q", c.ContentType())
			}
			if _, dup := names[c.Name()]; dup {
				return fmt.Errorf("f1: WithCodec lists two codecs named %q", c.Name())
			}
			contentTypes[c.ContentType()] = struct{}{}
			names[c.Name()] = struct{}{}
		}
		if options.codecsByContentType == nil {
			options.codecsByContentType = make(map[string]codec.Codec)
		}
		if options.codecsByName == nil {
			options.codecsByName = make(map[string]codec.Codec)
		}
		options.codec = codecs[0]
		for _, c := range codecs {
			options.codecsByContentType[c.ContentType()] = c
			options.codecsByName[c.Name()] = c
		}
		return nil
	}
}

// WithLogger sets the non-nil structured logger used by the Client.
func WithLogger(logger *slog.Logger) Option {
	return func(options *clientOptions) error {
		if logger == nil {
			return fmt.Errorf("f1: WithLogger requires a non-nil logger")
		}
		options.logger = logger
		return nil
	}
}

// withClock returns the option that runs time-dependent core behavior on c.
//
// It is unexported because the clock type is internal: an outside module cannot
// name it, so a public option could only ever be handed nil. Package f1
// registers it with internal/testhook during init for the module's own tests,
// and the f1 tests call it directly. A nil clock is refused here rather than
// stored, because the zero value would panic at the first Now or Timer instead
// of failing New with a message that names the option.
func withClock(c clock.Clock) Option {
	return func(options *clientOptions) error {
		if isNil(c) {
			return fmt.Errorf("f1: withClock requires a non-nil clock")
		}
		options.clock = c
		return nil
	}
}

func init() { testhook.RegisterClientOption(func(c clock.Clock) any { return withClock(c) }) }

// WithStrictPortability asks the Client to use portable implementations instead
// of optional native driver capabilities. Physical broker constraints remain
// in force.
func WithStrictPortability() Option {
	return func(options *clientOptions) error {
		options.strictPortability = true
		return nil
	}
}

// WithMiddleware adds middleware to the handler chain in the order supplied.
// The first middleware wraps the later middleware and handler. A nil middleware
// returns an error when New applies this option.
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

// WithPublishTopics names the logical topics this client publishes. New
// declares an entry point for each topic and declared priority, and Publish
// rejects a topic that was not named here.
//
// Each value goes through the same derivation Publish applies to an event
// type, so "order.created.v1" and "order.created" name the same topic.
func WithPublishTopics(topics ...string) Option {
	return func(options *clientOptions) error {
		seen := make(map[string]struct{}, len(options.publishTopics)+len(topics))
		for _, topic := range options.publishTopics {
			seen[topic] = struct{}{}
		}
		canonical := make([]string, 0, len(topics))
		for _, topic := range topics {
			if strings.TrimSpace(topic) == "" {
				return fmt.Errorf("f1: WithPublishTopics topic must not be empty or whitespace-only")
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
// verifyOnStart selects verify, and neither setting selects none. An unsupported
// policy returns an error when New applies this option.
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

// WithObserver installs the observer that receives this Client's lifecycle and
// point events. A nil observer disables these callbacks.
func WithObserver(observer Observer) Option {
	return func(options *clientOptions) error {
		options.observer = observer
		return nil
	}
}

// WithBacklogPollInterval sets how often the backlog poll loop samples one
// destination. Zero selects the default. A negative value disables the loop.
// Values in (0, 1s) are rejected by New.
func WithBacklogPollInterval(interval time.Duration) Option {
	return func(options *clientOptions) error {
		options.backlogPollInterval = interval
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
