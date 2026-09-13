# Message

F1 carries an event from a publisher to a handler through a broker-neutral
message model. The model separates the application contract, the wire
metadata, and the driver transport so application code does not need to know
which messaging system is underneath it.

If you are new to F1, read [Getting started](/learn/getting-started) first. This
page explains what the message-related types mean and how to use them safely.

## Start with the right type

F1 has several types with "message" in their role. They are not interchangeable:

| You need to... | Use | Owned by |
| --- | --- | --- |
| Handle one delivered event | [`f1.Event`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/event.go) | Application handler |
| Describe one item in a publish batch | [`f1.Message`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/publisher.go) | Publisher API |
| Carry canonical metadata | [`f1.Envelope`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/envelope.go) | F1 wire contract |
| Adapt to a transport | [`driver.InboundMessage`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/driver/message.go) and [`driver.OutboundMessage`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/driver/message.go) | Driver implementation |

The usual application path is:

1. `Publisher.Publish` or `Publisher.PublishBatch` accepts a typed payload.
2. F1 encodes the payload and builds an envelope.
3. The selected driver transports the encoded body and headers.
4. F1 reconstructs an `Event` for the matching handler.
5. The handler returns an outcome, and F1 settles the delivery from that
   outcome.

Application handlers should work with `Event`. Driver implementations work
with `driver.InboundMessage` and `driver.OutboundMessage`. Most application
code never needs to construct either driver type.

## Publish a message

`Publisher.Publish` takes an event type, a payload, and optional message
metadata. The payload is encoded by the client's configured codec; a new client
uses JSON by default.

```go
type OrderPlaced struct {
	OrderID string `json:"orderId"`
}

id, err := client.Publisher().Publish(
	ctx,
	"orders.placed.v1",
	OrderPlaced{OrderID: "order-123"},
	f1.WithKey("order-123"),
	f1.WithSubject("order:order-123"),
	f1.WithIdempotencyKey("order-placed:order-123"),
	f1.WithHeader("x-request-id", "request-123"),
)
if err != nil {
	return fmt.Errorf("publish order: %w", err)
}
log.Printf("published event %s", id)
```

`Publish` returns after the selected driver reports durable acknowledgement. It
returns the generated event ID, not the idempotency key. These identifiers have
different jobs:

- **Event ID** identifies this published event instance and is available from
  `Event.ID()`.
- **Idempotency key** identifies the business operation that a handler should
  apply at most once. `Event.IdempotencyKey()` returns the producer-provided
  key, or the event ID when no key was provided.

The most common metadata options are:

- `WithKey` - sets the partition/routing key and takes precedence over the
  subject and generated event ID;
- `WithSubject` - records the business subject and supplies the default key
  when no key is provided;
- `WithIdempotencyKey` - carries the stable application deduplication key;
- `WithHeader` - adds a user-defined extension header;
- `WithCorrelationID` and `WithCausedBy` - connect events in one workflow; and
- `WithExpiry` and `WithMaxAttempts` - constrain delivery lifetime and retry
  attempts.

See [`PublishOption`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/publisher.go) for the complete option contract.

## Publish a batch

`f1.Message` is the input descriptor for `PublishBatch`; it is not the object
passed to a handler. Each item contains an event type, a payload, and options
for that item:

```go
result, err := client.Publisher().PublishBatch(ctx, []f1.Message{
	{
		EventType: "orders.placed.v1",
		Payload:   OrderPlaced{OrderID: "order-123"},
		Opts: []f1.PublishOption{
			f1.WithKey("order-123"),
			f1.WithIdempotencyKey("order-placed:order-123"),
		},
	},
	{
		EventType: "orders.audit.v1",
		Payload:   map[string]string{"orderId": "order-123"},
	},
})
if err != nil {
	return fmt.Errorf("publish batch: %w", err)
}
for index, item := range result.Results {
	if item.Err != nil {
		log.Printf("message %d failed: %v", index, item.Err)
		continue
	}
	log.Printf("message %d published as %s", index, item.ID)
}
```

Batch publishing preserves input order in `BatchResult.Results` and makes no
atomicity claim. A transport-wide failure is returned as the method error; an
individual item failure is reported in its `MessageResult`.

## Consume a message

Handlers receive `*f1.Event`, which gives access to the decoded message view
without exposing the driver settlement mechanism:

```go
func handleOrder(ctx context.Context, event *f1.Event) error {
	var placed OrderPlaced
	if err := event.Decode(&placed); err != nil {
		return f1.Terminal(fmt.Errorf("decode %s: %w", event.Type(), err))
	}

	// Make the application side effect idempotent with this key.
	if err := applyOrder(ctx, event.IdempotencyKey(), placed); err != nil {
		return err
	}
	return nil
}
```

`Event.Decode` uses the codec selected for that event. Decode into a typed
payload that matches the event contract instead of making handlers depend on
raw transport bytes. `Event.Raw()` is available when an application genuinely
needs the undecoded body, and returns an independent copy.

The event type is the handler-routing key. Register the same versioned type
that the publisher emits:

```go
runner, err := client.Subscribe(ctx, f1.Subscription{
	Name:   "order-projector",
	Topics: []string{"orders.placed"},
	Handlers: map[string]f1.Handler{
		"orders.placed.v1": f1.HandlerFunc(handleOrder),
	},
})
```

F1 derives the logical topic `orders.placed` from the versioned event type
`orders.placed.v1` by removing the trailing `.v1` segment. Use
`f1.WithTopic` when publishing only if the application intentionally uses a
different logical topic.

## Metadata and the envelope

An [`Envelope`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/envelope.go) is the canonical metadata F1 serializes as
message headers. It is separate from the payload body. The envelope carries
several kinds of information:

| Category | Examples | Handler access |
| --- | --- | --- |
| Identity | `ID`, `Type`, `Source`, `Time` | `ID()`, `Type()`, `Source()`, `Time()` |
| Business context | `Subject`, `DataSchema` | `Subject()` or `Header()` |
| Delivery | `Priority`, `Attempt`, `MaxAttempts`, `Expiry` | `Priority()`, `Attempt()`, `MaxAttempts()` |
| Workflow | `CorrelationID`, `CausationID` | `CorrelationID()`, `CausationID()` |
| Reliability | `IdempotencyKey`, `OriginalDest`, `DueTime` | `IdempotencyKey()` or `Header()` |
| Diagnostics | dead-letter reason, error, time, and details | dead-letter callback payload |
| Tracing and extensions | trace context and user headers | `Header()` |

Use the `Event` accessors in application code. Use `Envelope.EncodeHeaders` and
`DecodeHeaders` only when implementing or testing a wire boundary. The driver
is responsible for moving the encoded headers and body; it must not reinterpret
F1's destination or envelope fields.

### Read a header

`Event.Header` reads a canonical header or an extension header without exposing
the internal header map:

```go
requestID, ok := event.Header("x-request-id")
if ok {
	log.Printf("handling request %s", requestID)
}
```

Headers are useful for small routing, tracing, and diagnostic values. Put the
business payload in the typed body so it can be versioned and decoded as a
contract.

## Context belongs to the operation

F1 does not store a `context.Context` inside `Event`. Instead:

- publishing accepts a context that controls encoding, transport, and
  acknowledgement;
- handlers receive a context that carries cancellation and deadlines for the
  current delivery; and
- shutdown cancels handler work while the runner drains in-flight deliveries.

Pass the handler context to downstream calls:

```go
func handleOrder(ctx context.Context, event *f1.Event) error {
	var placed OrderPlaced
	if err := event.Decode(&placed); err != nil {
		return f1.Terminal(err)
	}
	return applyOrder(ctx, event.IdempotencyKey(), placed)
}
```

Do not save the handler context for later work. Start asynchronous work only
when the application owns its lifecycle and can define what delivery
completion means for that work.

## Settlement is driven by the handler result

F1 intentionally does not expose explicit `Ack()` and `Nack()` methods
on `Event`. The handler's return value is the application-level settlement
decision:

| Handler result | F1 behavior |
| --- | --- |
| `nil` | Acknowledge the delivery as handled. |
| Ordinary error | Apply the subscription retry policy. |
| `f1.RetryAfter(err, delay)` | Retry with an explicit delay for this attempt. |
| `f1.Terminal(err)` | Stop retrying and dead-letter the event. |
| `f1.Drop(err)` | Acknowledge the event without applying its effect or retaining a copy. |

Malformed payloads and permanent validation failures should normally be
terminal. Transient downstream failures should return their ordinary error so
the configured retry policy can decide when to try again. Successful handlers
must return `nil` only after their idempotent side effect has completed.

## Delivery identity and redelivery

F1 provides at-least-once delivery. The same event can be delivered again after
a retry, reconnect, or process restart. Design handlers around stable identity:

- use `Event.ID()` to identify the event instance in logs and traces;
- use `Event.IdempotencyKey()` to deduplicate the business effect;
- use `Event.Attempt()` to understand the current delivery attempt, not as a
  deduplication key; and
- treat `Event.Type()` as the schema/handler contract version.

If the application needs to retain a dead-lettered message, use the
subscription's `OnDeadLetter` callback. Its `f1.DeadLettered` value contains a
copy of the envelope and body plus the failure reason and last error.

## Source contracts

Read these symbols when the behavior matters:

- [`Event`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/event.go) - handler-facing identity, metadata, raw body, and
  decoding;
- [`Envelope`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/envelope.go) - canonical wire metadata and header limits;
- [`Publisher`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/publisher.go) - publish, batch results, and message
  options;
- [`HandlerFunc`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/handler.go) - function handlers and context delivery;
- [`Subscription`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/subscription.go) - routing, retry, and unmatched
  event policy;
- [`driver.InboundMessage`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/driver/message.go) - the lower-level driver
  transport contract; and
- [`codec.Codec`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/codec/codec.go) - the payload encoding and decoding
  contract.
