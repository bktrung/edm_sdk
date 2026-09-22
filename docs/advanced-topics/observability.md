# Observability

Attach one observer to a client and F1 emits typed lifecycle events without
adding metrics code to handlers. The `f1otel` adapter turns those events into
standard OpenTelemetry metrics using an application-owned `MeterProvider`.

For the complete event and field reference, see [Observer events](/development/observer-events).

## Install the metrics adapter

Create and configure the provider in the application, then pass the adapter to
`f1.New` with `f1.WithObserver`:

```go
import (
    "context"

    f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
    "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/f1otel"
    sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

meterProvider := sdkmetric.NewMeterProvider()
observer, err := f1otel.New(f1otel.WithMeterProvider(meterProvider))
if err != nil {
    return err
}

client, err := f1.New(
    context.Background(),
    cfg,
    f1.WithDriver(driver),
    f1.WithObserver(observer),
)
if err != nil {
    return err
}
```

The provider owns exporters and shutdown. `f1otel.New` creates no exporter and
never selects the OpenTelemetry global provider. A nil provider, the zero
`f1otel.Observer`, and a nil observer are no-ops. The adapter is safe for
concurrent calls and uses event timestamps rather than the wall clock.

The metrics adapter uses OpenTelemetry semantic conventions v1.43.0. Configure
views in the application when duration histograms need explicit buckets;
`f1otel` does not install a view.

## Writing an observer

The `Observer` interface, its three methods, the token that matches a `Finish` to its `Start`, and
the rules an implementation must follow are in [Observer](/basics/observer). An adapter that records
more than `f1otel` does implements that interface directly.

## OpenTelemetry metrics

For alert examples built from these instruments, see [Alerts](/advanced-topics/alerts).

`f1otel` records eleven instruments. The attribute names below are the exact
names emitted by the adapter. `messaging.destination.name` is the logical
topic, not a physical broker destination. `messaging.system` is `f1.<driver>`
when the driver is known. `server.address` and `server.port` are included when
the driver reports them.

| Name | Kind | Unit | Attributes | When recorded |
| --- | --- | --- | --- | --- |
| `messaging.client.sent.messages` | sum | `{message}` | `messaging.system`, `messaging.destination.name`, `messaging.operation.name=publish`, `f1.priority`, optional server attributes | On a publish finish, for successfully published messages. A confirmed successor with no result slice counts as one. |
| `messaging.client.consumed.messages` | sum | `{message}` | `messaging.system`, `messaging.destination.name`, `messaging.operation.name=receive`, `messaging.consumer.group.name`, `f1.priority`, optional server attributes | On `ObserverDeliveryReceived`. |
| `messaging.client.operation.duration` | histogram | `s` | `messaging.system`, `messaging.destination.name`, `messaging.operation.name=publish|settle`, `messaging.consumer.group.name` for settle, `f1.priority`, optional server attributes | On publish and settle finishes, using the event timestamp minus the matching start timestamp. |
| `messaging.process.duration` | histogram | `s` | `messaging.system`, `messaging.destination.name`, `messaging.operation.name=process`, `messaging.consumer.group.name`, `f1.priority`, optional server attributes, `error.type` when classified | On a process finish, using event timestamps. |
| `f1.messaging.broker.wait.duration` | histogram | `s` | `messaging.system`, `messaging.destination.name`, `messaging.consumer.group.name`, `f1.priority`, optional server attributes | On delivery receipt only when the enqueue source is the broker and its timestamp is known. |
| `f1.messaging.backlog.messages` | gauge | none | `messaging.system`, `messaging.destination.name`, `messaging.consumer.group.name`, `f1.priority`, optional server attributes | On each backlog sample, with the sampled lag. |
| `f1.messaging.backlog.oldest.age` | gauge | `s` | `messaging.system`, `messaging.destination.name`, `messaging.consumer.group.name`, `f1.priority`, optional server attributes | On a backlog sample when the head timestamp is known and its source is the broker. |
| `f1.messaging.deadline.promotions` | sum | none | `messaging.system`, `messaging.destination.name`, `messaging.consumer.group.name`, `f1.priority`, optional server attributes | On a deadline promotion. The value includes the promotion and any suppressed promotions reported by the event. |
| `f1.messaging.lane.wait.duration` | histogram | `s` | `messaging.system`, `messaging.destination.name`, `messaging.consumer.group.name`, `f1.priority`, optional server attributes | On a deadline promotion, using the reported lane wait. |
| `f1.messaging.retries` | sum | none | `messaging.system`, `messaging.destination.name`, `messaging.consumer.group.name`, `f1.priority`, optional server attributes, `error.type` | On a confirmed `ObserverRetryScheduled` point. |
| `f1.messaging.dead_letters` | sum | none | `messaging.system`, `messaging.destination.name`, `messaging.consumer.group.name`, `f1.priority`, optional server attributes, `error.type`, optional `reason` | On a confirmed `ObserverDeadLetterPublished` point. |

`messaging.operation.name` identifies the operation as `publish`, `receive`,
`process`, or `settle`. Attributes that are empty or not defined for an event
kind are omitted. The adapter does not emit metrics for unknown kinds, and it
does not emit a duration when its required timestamp source is unavailable.

Remaining gap: mixed-priority publish calls and destinations shared by several priorities report
`medium`; topic is empty for mixed-topic publish calls.

## Enqueue time, backlog, and oldest age

F1 carries the enqueue timestamp with its source:

- `producer` is the event's creation time. On a retry or dead-letter copy,
  producer time includes all earlier attempts.
- `broker` is the timestamp supplied by the broker. Use it for per-hop broker
  wait.
- `unknown` means the timestamp is absent or its source is unavailable.

The broker-wait histogram uses only `source=broker`. A producer timestamp is
still carried on the observer event, but it does not represent the current
broker hop. A backlog sample always records lag when the driver supports
backlog reads. The oldest-age gauge is omitted when the broker gives no head
timestamp, so an unknown age is not reported as zero.

`f1.WithBacklogPollInterval` controls how often the client samples a
subscription. Zero selects the 15s default, a negative interval disables
polling, and a positive interval below 1s is rejected by `New`.

## Kafka operator setting

Kafka uses producer `CreateTime` by default. For broker-clock wait, configure
the destination with:

```text
message.timestamp.type=LogAppendTime
```

`LogAppendTime` makes the Kafka adapter report the broker append timestamp as
the enqueue source. Apply the topic setting to every destination whose broker
clock you want to measure.

## RabbitMQ operator setting

Set `broker.rabbitmq.trustBrokerTimestamp` to `true` only when the RabbitMQ
`set_header_timestamp` incoming interceptor is configured with
`message_interceptors.incoming.set_header_timestamp.overwrite = true`.
Without overwrite mode, `timestamp_in_ms` is publisher-controlled and is not a
trusted broker timestamp. The default is `false`.

RabbitMQ resolves enqueue time in this order:

1. A trusted `timestamp_in_ms` header, marked `broker`.
2. The `cloudEvents:time` header, marked `producer`.
3. The AMQP timestamp property, with whole-second precision, marked `producer`.
4. No timestamp, marked `unknown`.

RabbitMQ's backlog count is the same value as `Lag`. The head time comes from
the management API for classic queues only. The default quorum queue type has
an unknown head time. A management head timestamp is always marked
`producer`, so RabbitMQ does not report the oldest-age metric.

## OpenTelemetry tracing

Configure an application-owned tracer provider and propagator when creating the
`f1otel` observer:

```go
import (
    "context"

    f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
    "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/f1otel"
    "go.opentelemetry.io/otel/propagation"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

tracerProvider := sdktrace.NewTracerProvider()
observer, err := f1otel.New(
    f1otel.WithTracerProvider(tracerProvider),
    f1otel.WithPropagator(propagation.TraceContext{}),
)
if err != nil {
    return err
}

client, err := f1.New(
    context.Background(),
    cfg,
    f1.WithDriver(driver),
    f1.WithObserver(observer),
)
```

The five span stages are:

| Span | Kind | Parent or link | Starts and ends |
| --- | --- | --- | --- |
| `send <topic>` or `send` | producer | Parent is the caller context for a primary publish | Starts after publish admission; ends after the producer result |
| `create <topic>` | producer | Child of the primary `send` span | Starts after message identity is known; ends after envelope headers are encoded |
| `process <topic>` | consumer | New root with a link to the inbound trace context | Starts before the handler; ends after the handler outcome |
| `settle` | client | Parent is the context used for Ack or Nack | Starts before the broker settlement call; ends after it returns |
| `send <topic>` or `send` for retry/DLQ | producer | New root with a link to the incoming process context | Starts before the successor publish; ends after it is confirmed or fails |

A primary send span is named `send <topic>` when its publish finishes with a uniform topic; it
remains `send` for a mixed-topic batch. Retry and dead-letter sends use the topic known at their
start.
Span timestamps come from the observer events, not the wall clock at export.
The process span is a new root linked to the inbound context, and the context
returned by `Start` reaches the handler, so handler spans are its children.
Retry and dead-letter sends are new roots linked to the incoming process
context. `WithCreateSpans(false)` removes the `create` span; the default is to
create it.

`InjectTrace` writes `traceparent` and `tracestate` only when a propagator was
configured with `WithPropagator`. It never reads the OpenTelemetry global
propagator. Spans carry `f1.priority`, `f1.attempt`, `f1.route`, and
`f1.destination`; metrics carry only `f1.priority` among these F1 attributes.
