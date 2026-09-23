package f1otel

import (
	"context"
	"sync"
	"sync/atomic"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/version"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/semconv/v1.43.0/messagingconv"
	"go.opentelemetry.io/otel/trace"
)

var (
	_              f1.Observer = (*Observer)(nil)
	observerHandle atomic.Uint64
)

type endpointInfo struct {
	system     string
	serverAddr string
	serverPort int
}

// Observer implements f1.Observer with OpenTelemetry metrics and spans. Observer
// methods are safe for concurrent calls. The zero value and a nil *Observer are
// no-ops.
type Observer struct {
	metrics *metrics

	tracer      trace.Tracer
	propagator  propagation.TextMapPropagator
	createSpans bool

	mu       sync.Mutex
	starts   map[uint64]startState
	endpoint atomic.Pointer[endpointInfo]
}

type startState struct {
	kind       f1.ObserverKind
	span       trace.Span
	parentSpan trace.Span
}

// instrumentationName names the meter and tracer this package creates.
const instrumentationName = "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/f1otel"

// New returns an Observer configured with application-owned providers. New
// creates no exporter, never reads a global provider, and reports instrument
// creation errors through otel.Handle while returning the usable Observer.
func New(opts ...Option) (*Observer, error) {
	cfg := config{createSpans: true}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	observer := &Observer{
		propagator:  cfg.propagator,
		createSpans: cfg.createSpans,
	}
	if cfg.meterProvider != nil {
		observer.metrics = newMetrics(cfg.meterProvider.Meter(instrumentationName, metric.WithInstrumentationVersion(version.SDK())))
	}
	if cfg.tracerProvider != nil {
		observer.tracer = cfg.tracerProvider.Tracer(instrumentationName, trace.WithInstrumentationVersion(version.SDK()))
	}
	if observer.tracer != nil {
		observer.starts = make(map[uint64]startState)
	}
	return observer, nil
}

// Start returns a token when the event starts a metric-bearing or span-bearing
// stage; otherwise it returns a zero token. It starts spans for publish,
// message_built, process, and settle events when a tracer is configured, except
// that message_built spans can be disabled with [WithCreateSpans]. The returned
// context carries a span when one is started. Process spans are roots linked
// to valid inbound trace context. A nil receiver returns ctx and a zero token.
func (o *Observer) Start(ctx context.Context, event f1.StartEvent) (context.Context, f1.Token) {
	if o == nil {
		return ctx, f1.Token{}
	}
	next := ctx
	var token f1.Token
	if o.metrics != nil && metricStartKind(event.Kind) {
		token = f1.Token{
			Kind:   event.Kind,
			Start:  event.At,
			Handle: observerHandle.Add(1),
		}
	}
	if o.tracer != nil && spanStartKind(event.Kind) && (event.Kind != f1.ObserverMessageBuilt || o.createSpans) {
		var span trace.Span
		var parentSpan trace.Span
		next, span, parentSpan = o.startSpan(ctx, event)
		if token.Handle == 0 {
			token = f1.Token{
				Kind:   event.Kind,
				Start:  event.At,
				Handle: observerHandle.Add(1),
			}
		}
		o.mu.Lock()
		if o.starts == nil {
			o.starts = make(map[uint64]startState)
		}
		o.starts[token.Handle] = startState{
			kind:       event.Kind,
			span:       span,
			parentSpan: parentSpan,
		}
		o.mu.Unlock()
	}
	return next, token
}

// Finish records configured metrics for supported kinds and ends a span
// associated with a matching token. With metrics but no tracing, a nonzero
// handle, nonzero start time, and matching kind make publish, process, and
// settle finishes eligible for metric recording. With tracing, the token must
// identify a start recorded by this Observer; tokens without a recorded start
// are ignored. The first Finish call that finds a recorded start consumes it,
// even if its kind mismatches; a mismatched span is then dropped without being
// ended, so it is never exported, and no metric is recorded.
func (o *Observer) Finish(token f1.Token, event f1.FinishEvent) {
	if o == nil || token.Handle == 0 {
		return
	}

	var start startState
	if o.tracer != nil {
		var ok bool
		o.mu.Lock()
		start, ok = o.starts[token.Handle]
		if ok {
			delete(o.starts, token.Handle)
		}
		o.mu.Unlock()
		if !ok || token.Kind != event.Kind || start.kind != token.Kind {
			return
		}
	} else if o.metrics == nil || token.Start.IsZero() || token.Kind != event.Kind {
		return
	}

	if o.metrics != nil {
		endpoint := o.endpoint.Load()
		attrs := o.attrs(event.Kind, event.Topic, event.Subscription, event.Priority, event.ErrorClass, "", endpoint)
		system := messagingconv.SystemAttr(o.systemAttr(endpoint))
		seconds := event.At.Sub(token.Start).Seconds()
		recordingCtx := context.Background()
		if start.span != nil && metricStartKind(event.Kind) {
			recordingCtx = trace.ContextWithSpan(recordingCtx, start.span)
		}
		switch event.Kind {
		case f1.ObserverPublish:
			if o.metrics.sent != nil {
				var successful int64
				for _, result := range event.Results {
					if result.Err == nil && result.ID != "" {
						successful++
					}
				}
				if successful == 0 && len(event.Results) == 0 && event.Outcome == f1.ObserverOutcomeOK {
					successful = 1
				}
				if successful != 0 {
					o.metrics.sent.Add(recordingCtx, successful, "publish", system, attrs...)
				}
			}
			if o.metrics.operation != nil {
				o.metrics.operation.Record(recordingCtx, seconds, "publish", system, attrs...)
			}
		case f1.ObserverProcess:
			if o.metrics.process != nil {
				o.metrics.process.Record(recordingCtx, seconds, "process", system, attrs...)
			}
		case f1.ObserverSettle:
			if o.metrics.operation != nil {
				o.metrics.operation.Record(recordingCtx, seconds, "settle", system, attrs...)
			}
		}
	}
	o.finishSpan(token, event, start)
}

// Record emits metrics for supported point events when metrics are configured.
// Driver-selection events update the endpoint data that metrics and spans
// carry, with or without metrics. Missing broker enqueue time
// metadata omits the broker-wait histogram but not the delivery count. For
// backlog samples, missing broker enqueue time or an unknown head age omits
// only the age histogram, not the backlog message gauge.
func (o *Observer) Record(event f1.PointEvent) {
	if o == nil {
		return
	}
	// The endpoint is recorded whether or not metrics are configured: spans
	// read it for server.address and server.port too.
	if event.Kind == f1.ObserverDriverSelected {
		o.recordDriver(event)
		return
	}
	if o.metrics == nil {
		return
	}

	endpoint := o.endpoint.Load()
	attrs := o.attrs(event.Kind, event.Topic, event.Subscription, event.Priority, event.ErrorClass, event.Reason, endpoint)
	system := messagingconv.SystemAttr(o.systemAttr(endpoint))
	customAttrs := customMetricAttributes(attrs, system)
	ctx := context.Background()
	switch event.Kind {
	case f1.ObserverDeliveryReceived:
		if o.metrics.consumed != nil {
			o.metrics.consumed.Add(ctx, 1, "receive", system, attrs...)
		}
		if o.metrics.brokerWait != nil && event.EnqueuedAtSource == f1.EnqueuedAtBroker && !event.EnqueuedAt.IsZero() {
			o.metrics.brokerWait.Record(ctx, event.At.Sub(event.EnqueuedAt).Seconds(), metric.WithAttributes(customAttrs...))
		}
	case f1.ObserverBacklogSampled:
		if o.metrics.backlogMessages != nil {
			o.metrics.backlogMessages.Record(ctx, event.Backlog, metric.WithAttributes(customAttrs...))
		}
		if o.metrics.backlogOldestAge != nil && event.EnqueuedAtSource == f1.EnqueuedAtBroker && event.HeadAgeKnown {
			o.metrics.backlogOldestAge.Record(ctx, event.HeadAge.Seconds(), metric.WithAttributes(customAttrs...))
		}
	case f1.ObserverDeadlinePromoted:
		if o.metrics.deadlinePromotions != nil {
			o.metrics.deadlinePromotions.Add(ctx, int64(1+event.Suppressed), metric.WithAttributes(customAttrs...))
		}
		if o.metrics.laneWait != nil {
			o.metrics.laneWait.Record(ctx, event.LaneWait.Seconds(), metric.WithAttributes(customAttrs...))
		}
	case f1.ObserverRetryScheduled:
		if o.metrics.retries != nil {
			o.metrics.retries.Add(ctx, 1, metric.WithAttributes(customAttrs...))
		}
	case f1.ObserverDeadLetterPublished:
		if o.metrics.deadLetters != nil {
			o.metrics.deadLetters.Add(ctx, 1, metric.WithAttributes(customAttrs...))
		}
	}
}

func (o *Observer) recordDriver(event f1.PointEvent) {
	system := "f1.unknown"
	if event.DriverName != "" {
		system = "f1." + event.DriverName
	}
	o.endpoint.Store(&endpointInfo{
		system:     system,
		serverAddr: event.ServerAddress,
		serverPort: event.ServerPort,
	})
}

func (o *Observer) systemAttr(endpoint *endpointInfo) string {
	if endpoint == nil || endpoint.system == "" {
		return "f1.unknown"
	}
	return endpoint.system
}

func (o *Observer) attrs(kind f1.ObserverKind, topic, subscription string, priority f1.Priority, class f1.ErrorClass, reason f1.DeathReason, endpoint *endpointInfo) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 6)
	if topic != "" {
		attrs = append(attrs, attribute.String("messaging.destination.name", topic))
	}
	if subscription != "" {
		attrs = append(attrs, attribute.String("messaging.consumer.group.name", subscription))
	}
	if metricCarriesPriority(kind) {
		attrs = append(attrs, attribute.String("f1.priority", priority.String()))
	}
	if class == "" && reason != "" {
		class = classForReason(reason)
	}
	if class != "" {
		attrs = append(attrs, attribute.String("error.type", string(normalizeErrorClass(class))))
	}
	if kind == f1.ObserverDeadLetterPublished && reason != "" {
		attrs = append(attrs, attribute.String("reason", reason.String()))
	}

	if endpoint != nil && endpoint.serverAddr != "" {
		attrs = append(attrs, attribute.String("server.address", endpoint.serverAddr))
	}
	if endpoint != nil && endpoint.serverPort != 0 {
		attrs = append(attrs, attribute.Int("server.port", endpoint.serverPort))
	}
	return attrs
}

func customMetricAttributes(attrs []attribute.KeyValue, system messagingconv.SystemAttr) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs)+1)
	out = append(out, attrs...)
	return append(out, attribute.String("messaging.system", string(system)))
}

func metricStartKind(kind f1.ObserverKind) bool {
	switch kind {
	case f1.ObserverPublish, f1.ObserverProcess, f1.ObserverSettle:
		return true
	default:
		return false
	}
}

func metricCarriesPriority(kind f1.ObserverKind) bool {
	switch kind {
	case f1.ObserverPublish, f1.ObserverProcess, f1.ObserverSettle,
		f1.ObserverDeliveryReceived, f1.ObserverRetryScheduled,
		f1.ObserverDeadLetterDecided, f1.ObserverDeadLetterPublished,
		f1.ObserverDeadLetterFailed, f1.ObserverPoisonRejected,
		f1.ObserverBacklogSampled, f1.ObserverDeadlinePromoted:
		return true
	default:
		return false
	}
}

func classForReason(reason f1.DeathReason) f1.ErrorClass {
	switch reason {
	case f1.ReasonDecode:
		return f1.ErrorClassDecode
	case f1.ReasonExpired:
		return f1.ErrorClassExpired
	case f1.ReasonMaxAttempts:
		return f1.ErrorClassMaxAttempts
	case f1.ReasonPanic:
		return f1.ErrorClassPanic
	case f1.ReasonPoison:
		return f1.ErrorClassPoison
	case f1.ReasonTerminal:
		return f1.ErrorClassTerminal
	case f1.ReasonUnmatched:
		return f1.ErrorClassUnmatched
	default:
		return f1.ErrorClassOther
	}
}

func normalizeErrorClass(class f1.ErrorClass) f1.ErrorClass {
	switch class {
	case "":
		return ""
	case f1.ErrorClassRetryable, f1.ErrorClassTerminal, f1.ErrorClassDropped,
		f1.ErrorClassDecode, f1.ErrorClassPanic, f1.ErrorClassExpired,
		f1.ErrorClassUnmatched, f1.ErrorClassMaxAttempts, f1.ErrorClassPoison,
		f1.ErrorClassDriverTransient, f1.ErrorClassDriverFatal,
		f1.ErrorClassDriverNotFound, f1.ErrorClassDriverTooLarge,
		f1.ErrorClassDriverPermission, f1.ErrorClassDriverNotification,
		f1.ErrorClassOther:
		return class
	default:
		return f1.ErrorClassOther
	}
}
