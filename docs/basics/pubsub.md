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
| Connect to the selected transport | [`f1.New`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/client.go) with `f1.WithDriver` | Driver connection and client resources |
| Publish events | `Client.Publisher` | Encoding, envelope construction, and durable publish acknowledgement |
| Declare consumption | [`Client.Subscribe`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/subscription.go) | Subscription validation, topics, handlers, and delivery policy |
| Run consumption | [`Runner.Run`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/worker.go) | Fetching, dispatch, retries, [the final ack or nack](/learn/glossary#settlement), and reconnect behavior |
| Stop consumption | [`Runner.Drain`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/worker.go) or [`Client.Close`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/client.go) | Stop fetching, finish in-flight work, and release resources |

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

The event type is the handler lookup key. F1 derives the [topic](/learn/glossary#logical-topic) by
removing a trailing version segment such as `.v1`; use `WithTopic` only when an
explicit topic is part of the application contract. Routing and workflow
metadata are added with `PublishOption` constructors such as `WithKey`,
`WithSubject`, `WithIdempotencyKey`, and `WithCorrelationID`. The complete
option contract lives in `publisher.go`.

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
for i, item := range result.Results {
	if item.Err != nil {
		return fmt.Errorf("publish batch item %d: %w", i, item.Err)
	}
}
```

Batch publishing preserves input order in `BatchResult.Results`, and it is not
atomic. A nil error from `PublishBatch` says the call itself went through, not
that every message was published: a failure that affected the whole call is
returned as the method error, while a message the broker rejected on its own
appears as that item's `MessageResult.Err`, which the loop above checks. See
the [publish flow](/development/publish-flow) for topology timing, partial failure,
and close interaction.

## Declare and run a subscription

`Client.Subscribe` validates a `Subscription` and returns a `Runner`. It does
not start fetching messages. The runner starts only when `Run` is called:

```go
runner, err := client.Subscribe(ctx, f1.Subscription{
	Name:           "order-projector",
	Topics:         []string{"orders.placed"},
	Concurrency:    4,
	Prefetch:       16,
	Retry:          f1.RetryConfig{MaxAttempts: 3},
	HandlerTimeout: 10 * time.Second,
	Handlers: map[string]f1.Handler{
		"orders.placed.v1":  f1.HandlerFunc(handleOrderPlaced),
		"orders.shipped.v1": f1.Typed(handleOrderShipped),
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

The subscription name identifies the [consumer group](/learn/glossary#consumer-group) or equivalent driver ownership
scope. A new subscription with the same name joins that scope; a different name creates an independent
scope. The name becomes one segment of the destination names, so it may use only letters, digits, `-`, and `_`. Topics select the destinations to consume, and each topic may appear once: `orders` and `orders.v2` name the same topic. The handler map first checks for an exact
event-type key. When no exact key exists, a key ending in `*` matches by prefix, and the longest matching
prefix wins. If no key matches, `UnmatchedPolicy` decides whether F1 ignores or dead-letters the event.

| Handler keys | Event type | Handler |
| --- | --- | --- |
| `orders.placed.v1`, `orders.*` | `orders.placed.v1` | `orders.placed.v1` (exact) |
| `orders.*`, `orders.placed.*` | `orders.placed.v2` | `orders.placed.*` (longest prefix) |
| `orders.*` | `payments.refunded.v1` | None (`UnmatchedPolicy`) |

Retry, concurrency, prefetch, ordering, and unmatched-event behavior are
subscription policy; their owning fields are documented in `Subscription`,
and [Ordering and scheduling](/advanced-topics/ordering-and-scheduling) explains how the
concurrency, prefetch, and fairness fields interact.

The handler receives an `*f1.Event`, not a driver message. Decode the payload into a service-owned
type and return an error when the effect was not completed:

```go
func handleOrderCreated(ctx context.Context, event *f1.Event) error {
	var payload OrderCreated
	if err := event.Decode(&payload); err != nil {
		return f1.Terminal(fmt.Errorf("decode order event: %w", err))
	}
	if err := processOrder(ctx, payload); err != nil {
		return err
	}
	return nil
}
```

For a known payload type, `f1.Typed` performs the decode and makes a decode failure terminal on the
first attempt:

```go
func handleOrderShipped(ctx context.Context, event *f1.Event, payload OrderShipped) error {
	return processOrder(ctx, payload)
}
```

A handler can read the event ID, type, subject, attempt number, priority, headers, correlation and
causation identifiers, the raw payload, and the stable idempotency key. Use the idempotency key when
applying an effect that must be safe across redelivery.

## Handler results decide the ack

F1 does not require application handlers to call `Ack` or `Nack`. The returned
error decides the outcome, and F1 performs the matching ack or nack. The full
table of results is in [Message](/basics/message#the-handler-result-decides-the-ack).

Decode failures and permanent validation failures should normally be terminal.
Transient downstream failures should return their ordinary error so the retry
policy can decide when to try again. Return `nil` only after the application
effect has completed.

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
be acked.

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
and [Lifecycle and shutdown](/advanced-topics/lifecycle-and-shutdown).

## Driver portability

The `driver.Driver` interface is the transport
boundary. F1 opens the selected driver, creates producer and consumer resources,
and keeps envelope, routing, retry, and handler semantics in the core.

This separation lets the same publisher and subscription code run with a
different adapter when deployment requirements change. Driver-specific
capabilities are exposed through `Client.Limits()` and must be treated as
capabilities, not assumptions. The [driver and capabilities guide](/drivers-and-capabilities)
explains that boundary and its portability rules.

When implementing a new driver, use the public driver interfaces and the
conformance package as the executable contract.
Do not make application handlers depend on driver transport types merely to
access broker-specific behavior.

## Testing the pub/sub boundary

Use `f1test` for deterministic handler tests. It
provides the normal client and runner APIs backed by an in-memory driver, plus
helpers for delivering events, observing accepted messages, advancing deferred
delivery time, and inspecting dead-letter output.

Use driver conformance tests when validating a driver implementation. Keep
application behavior tests focused on event contracts, handler outcomes,
idempotency, and shutdown rather than on broker-specific delivery references.

## Go further

- [Message](/basics/message) - publish options, routing metadata, and batches;
- [Failure handling](/advanced-topics/failure-handling) - retry, terminal, drop, and dead-letter;
- [Ordering and scheduling](/advanced-topics/ordering-and-scheduling) - ordered mode and the
  concurrency and prefetch knobs;
- [Publish flow](/development/publish-flow) - publisher internals and acknowledgement
  timing; and
- [Consume flow](/development/consume-flow) - runner startup, dispatch, acks,
  and reconnect behavior.
