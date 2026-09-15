# Publisher and subscriber

F1's pub/sub model separates application work from transport details. An
application publishes typed events and registers handlers; the selected driver
connects those operations to the messaging system used by the deployment.

This page explains the boundary between the two sides. For the event envelope,
payload, headers, and message types themselves, see [Message](/basics/message).

## The F1 boundary

F1 does not expose one generic `Subscriber` object. The public application
surface is split into a client, a publisher, a subscription, and a runner:

| Responsibility | F1 entry point | What it owns |
| --- | --- | --- |
| Connect to the selected transport | [`f1.New`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/client.go) with [`f1.WithDriver`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/options.go) | Driver connection and client resources |
| Publish events | [`Client.Publisher`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/client.go) | Encoding, envelope construction, and durable publish acknowledgement |
| Declare consumption | [`Client.Subscribe`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/subscription.go) | Subscription validation, topics, handlers, and delivery policy |
| Run consumption | [`Runner.Run`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/worker.go) | Fetching, dispatch, retries, settlement, and reconnect behavior |
| Stop consumption | [`Runner.Drain`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/worker.go) or [`Client.Close`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/client.go) | Stop fetching, settle in-flight work, and release resources |

The application-facing code stays on the F1 side of this boundary. A concrete
driver is selected in composition code and passed through `WithDriver`; handler
code should not import a concrete driver package.

## Publish contract

Use `Publisher.Publish` for one event:

```go
eventID, err := client.Publisher().Publish(
	ctx,
	"orders.placed.v1",
	OrderPlaced{OrderID: orderID},
	f1.WithKey(orderID),
	f1.WithIdempotencyKey("order-placed:"+orderID),
)
if err != nil {
	return fmt.Errorf("publish order: %w", err)
}
log.Printf("published event %s", eventID)
```

`Publish` encodes the payload, builds its envelope, sends it through the
selected driver, and waits for durable acknowledgement before returning. Its
context controls the publish operation; use a context with an appropriate
deadline rather than an unbounded background operation.

The event type is the handler lookup key. F1 derives the logical topic by
removing a trailing version segment such as `.v1`; use `WithTopic` only when an
explicit topic is part of the application contract. Routing and workflow
metadata are added with `PublishOption` constructors such as `WithKey`,
`WithSubject`, `WithIdempotencyKey`, and `WithCorrelationID`. The complete
option contract lives in [`publisher.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/publisher.go).

### Batch publishing

Use `PublishBatch` when several independent events should be submitted in one
call:

```go
result, err := client.Publisher().PublishBatch(ctx, []f1.Message{
	{
		EventType: "orders.placed.v1",
		Payload:   OrderPlaced{OrderID: orderID},
		Opts: []f1.PublishOption{
			f1.WithKey(orderID),
		},
	},
	{
		EventType: "orders.audit.v1",
		Payload:   map[string]string{"orderId": orderID},
	},
})
if err != nil {
	return fmt.Errorf("publish batch: %w", err)
}
```

Batch publishing preserves input order in `BatchResult.Results` and makes no
atomicity claim. Inspect each `MessageResult`; a transport-wide error is the
method error, while an individual item failure is attached to that item. See
the [publish flow](/development/publish-flow) for topology timing, partial failure,
and close interaction.

## Declare and run a subscription

`Client.Subscribe` validates a `Subscription` and returns a `Runner`. It does
not start fetching messages. The runner starts only when `Run` is called:

```go
runner, err := client.Subscribe(ctx, f1.Subscription{
	Name:   "order-projector",
	Topics: []string{"orders.placed"},
	Handlers: map[string]f1.Handler{
		"orders.placed.v1": f1.HandlerFunc(handleOrderPlaced),
	},
})
if err != nil {
	return fmt.Errorf("create subscription: %w", err)
}

runErr := runner.Run(ctx)
if runErr != nil && !errors.Is(runErr, context.Canceled) {
	return fmt.Errorf("run subscription: %w", runErr)
}
```

The subscription name identifies the consumer group or equivalent driver
ownership scope. Topics select the logical destinations to consume. The
handler map selects a handler by the exact event type emitted by the publisher.
Retry, concurrency, prefetch, ordering, and unmatched-event behavior are
subscription policy; their owning fields are documented in
[`Subscription`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/subscription.go).

The handler receives an [`f1.Event`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/event.go), not a driver message. It
can decode the payload and inspect F1 metadata while remaining independent of
transport-specific delivery references.

## Handler results settle deliveries

F1 does not require application handlers to call `Ack` or `Nack`. The returned
error classifies the outcome, and the runtime performs the corresponding
settlement:

| Handler result | Delivery behavior |
| --- | --- |
| `nil` | Acknowledge the event as handled. |
| Ordinary error | Apply the subscription retry policy. |
| `f1.RetryAfter(err, delay)` | Retry at the ladder tier nearest the requested delay. |
| `f1.Terminal(err)` | Stop retrying and dead-letter the event. |
| `f1.Drop(err)` | Acknowledge the event without applying its effect or retaining a copy. |

Decode failures and permanent validation failures should normally be terminal.
Transient downstream failures should return their ordinary error so the retry
policy can decide when to try again. Return `nil` only after the application
effect has completed.

The classification helpers are defined in [`errors.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/errors.go), and
the dispatch path that applies them is owned by [`worker.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/worker.go).

## At-least-once delivery

F1 is designed for at-least-once handling. A handler may receive the same event
again after a retry, reconnect, or process restart. Make the application side
effect idempotent:

- use `Event.ID()` for event-instance logging and tracing;
- use `Event.IdempotencyKey()` for business-effect deduplication;
- use `Event.Attempt()` to understand the current delivery attempt, not as a
  stable identity; and
- use `WithKey` consistently when the application needs per-key routing or
  ordering and the connected driver advertises that capability.

F1 does not maintain a universal deduplication store for application effects.
The service that owns the side effect owns its idempotency decision. The
[message guide](/basics/message) covers the envelope and identity fields in detail.

## Lifecycle and shutdown

Use `Runner.Drain` when stopping one subscription while the client remains in
use. Drain stops fetching new messages and waits for in-flight deliveries to
settle.

Use `Client.Close` at the process boundary. It drains registered runners, waits
for accepted publishes, and closes the driver's producer and connection
resources:

```go
shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
defer cancel()

if err := client.Close(shutdownCtx); err != nil {
	return fmt.Errorf("close F1 client: %w", err)
}
```

For a complete signal-driven shutdown sequence, see [Getting started](/learn/getting-started)
and the [graceful shutdown guide](/user-guide/graceful-shutdown). The
client lifecycle contract is implemented in [`client.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/client.go) and
the runner drain contract in [`worker.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/worker.go).

## Driver portability

The [`driver.Driver`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/driver/driver.go) interface is the transport
boundary. F1 opens the selected driver, creates producer and consumer resources,
and keeps envelope, routing, retry, and handler semantics in the core.

This separation lets the same publisher and subscription code run with a
different adapter when deployment requirements change. Driver-specific
capabilities are exposed through `Client.Limits()` and must be treated as
capabilities, not assumptions. The [driver and capabilities guide](/drivers-and-capabilities)
explains that boundary and its portability rules.

When implementing a new driver, use the public driver interfaces and the
[conformance package](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/driver/conformance) as the executable contract.
Do not make application handlers depend on driver transport types merely to
access broker-specific behavior.

## Testing the pub/sub boundary

Use [`f1test`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/f1test/f1test.go) for deterministic handler tests. It
provides the normal client and runner APIs backed by an in-memory driver, plus
helpers for delivering events, observing accepted messages, advancing deferred
delivery time, and inspecting dead-letter output.

Use driver conformance tests when validating a driver implementation. Keep
application behavior tests focused on event contracts, handler outcomes,
idempotency, and shutdown rather than on broker-specific delivery references.

## Continue from here

- [Message](/basics/message) - event payloads, envelopes, headers, metadata, and
  delivery identity;
- [Publishing events](/user-guide/publishing-events) - publish options,
  routing metadata, and batches;
- [Consuming events](/user-guide/consuming-events) - handler registration,
  event decoding, ordering, and capabilities;
- [Publish flow](/development/publish-flow) - publisher internals and acknowledgement
  timing; and
- [Consume flow](/development/consume-flow) - runner startup, dispatch, settlement,
  and reconnect behavior.
