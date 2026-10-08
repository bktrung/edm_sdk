package f1otel

import (
	"context"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/semconv/v1.43.0/messagingconv"
	"go.opentelemetry.io/otel/trace"
)

func (o *Observer) startSpan(ctx context.Context, event f1.StartEvent) (context.Context, trace.Span, trace.Span) {
	if o == nil || o.tracer == nil || !spanStartKind(event.Kind) {
		return ctx, nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	name := spanName(event)
	options := []trace.SpanStartOption{
		trace.WithSpanKind(spanKind(event.Kind)),
		trace.WithTimestamp(event.At),
		trace.WithAttributes(o.spanAttributes(event)...),
	}
	if event.Kind == f1.ObserverProcess {
		options = append(options, trace.WithNewRoot())
		if linkCtx := o.extractContext(event); trace.SpanContextFromContext(linkCtx).IsValid() {
			options = append(options, trace.WithLinks(trace.LinkFromContext(linkCtx)))
		}
	} else if event.Kind == f1.ObserverPublish && event.Route != f1.PublishRoutePrimary {
		options = append(options, trace.WithNewRoot())
		if trace.SpanContextFromContext(ctx).IsValid() {
			options = append(options, trace.WithLinks(trace.LinkFromContext(ctx)))
		}
	}
	spanCtx, span := o.tracer.Start(ctx, name, options...)
	return spanCtx, span, trace.SpanFromContext(ctx)
}

func (o *Observer) finishSpan(token f1.Token, event f1.FinishEvent, start startState) {
	if start.span == nil {
		return
	}

	if event.Kind == f1.ObserverPublish && event.Topic != "" {
		start.span.SetName("send " + event.Topic)
	}
	if event.Kind == f1.ObserverPublish && event.PriorityKnown {
		start.span.SetAttributes(attribute.String("f1.priority", event.Priority.String()))
	}

	if event.Kind == f1.ObserverMessageBuilt && start.parentSpan != nil && start.span.SpanContext().IsValid() {
		start.parentSpan.AddLink(trace.Link{SpanContext: start.span.SpanContext()})
	}
	if event.ErrorClass != "" {
		class := normalizeErrorClass(event.ErrorClass)
		start.span.SetAttributes(attribute.String("error.type", string(class)))
	}
	if event.Outcome == f1.ObserverOutcomeError {
		class := normalizeErrorClass(event.ErrorClass)
		if class == "" {
			class = f1.ErrorClassOther
			start.span.SetAttributes(attribute.String("error.type", string(class)))
		}
		start.span.SetStatus(codes.Error, string(class))
	}
	start.span.End(trace.WithTimestamp(event.At))
}

func spanStartKind(kind f1.ObserverKind) bool {
	switch kind {
	case f1.ObserverPublish, f1.ObserverMessageBuilt, f1.ObserverProcess, f1.ObserverSettle:
		return true
	default:
		return false
	}
}

func spanKind(kind f1.ObserverKind) trace.SpanKind {
	switch kind {
	case f1.ObserverPublish, f1.ObserverMessageBuilt:
		return trace.SpanKindProducer
	case f1.ObserverProcess:
		return trace.SpanKindConsumer
	case f1.ObserverSettle:
		return trace.SpanKindClient
	default:
		return trace.SpanKindInternal
	}
}

func spanName(event f1.StartEvent) string {
	switch event.Kind {
	case f1.ObserverPublish:
		if event.Topic != "" {
			return "send " + event.Topic
		}
		return "send"
	case f1.ObserverMessageBuilt:
		return "create " + event.Topic
	case f1.ObserverProcess:
		return "process " + event.Topic
	case f1.ObserverSettle:
		return "settle"
	default:
		return string(event.Kind)
	}
}

func (o *Observer) spanAttributes(event f1.StartEvent) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 10)
	if event.Topic != "" {
		attrs = append(attrs, attribute.String("messaging.destination.name", event.Topic))
	}
	if event.MessageID != "" {
		attrs = append(attrs, attribute.String("messaging.message.id", event.MessageID))
	}
	consumerGroup := resolvedConsumerGroup(event.ConsumerGroup, event.Subscription)
	if consumerGroup != "" {
		attrs = append(attrs, attribute.String("messaging.consumer.group.name", consumerGroup))
	}
	if operation := spanOperation(event); operation != "" {
		attrs = append(attrs, attribute.String("messaging.operation.type", operation))
	}
	if event.Priority.Valid() && (event.Kind != f1.ObserverPublish || (event.Route != f1.PublishRoutePrimary && event.Route != "")) {
		attrs = append(attrs, attribute.String("f1.priority", event.Priority.String()))
	}
	if event.Attempt > 0 {
		attrs = append(attrs, attribute.Int("f1.attempt", event.Attempt))
	}
	if event.Route != "" {
		attrs = append(attrs, attribute.String("f1.route", string(event.Route)))
	}
	if event.Destination != "" {
		attrs = append(attrs, attribute.String("f1.destination", event.Destination))
	}

	endpoint := o.endpoint.Load()
	if endpoint != nil && endpoint.serverAddr != "" {
		attrs = append(attrs, attribute.String("server.address", endpoint.serverAddr))
	}
	if endpoint != nil && endpoint.serverPort != 0 {
		attrs = append(attrs, attribute.Int("server.port", endpoint.serverPort))
	}
	return attrs
}

func spanOperation(event f1.StartEvent) string {
	switch event.Kind {
	case f1.ObserverPublish:
		return string(messagingconv.OperationTypeSend)
	case f1.ObserverMessageBuilt:
		return string(messagingconv.OperationTypeCreate)
	case f1.ObserverProcess:
		return string(messagingconv.OperationTypeProcess)
	case f1.ObserverSettle:
		if event.Operation != "" {
			return string(event.Operation)
		}
		return string(messagingconv.OperationTypeSettle)
	default:
		return ""
	}
}

func (o *Observer) extractContext(event f1.StartEvent) context.Context {
	if o == nil || o.propagator == nil || event.TraceParent == "" {
		return context.Background()
	}
	carrier := propagation.MapCarrier{"traceparent": event.TraceParent}
	if event.TraceState != "" {
		carrier["tracestate"] = event.TraceState
	}
	return o.propagator.Extract(context.Background(), carrier)
}
