# Publishing events

Use `Client.Publisher().Publish` for a single event or `PublishBatch` for a
set of independent messages. Both wait for durable broker acknowledgement;
`Publish` returns the generated event ID after acknowledgement.

## Publish a versioned event

```go
type OrderCreated struct {
    OrderID string `json:"orderId"`
}

publisher := client.Publisher()
id, err := publisher.Publish(ctx, "orders.created.v1", OrderCreated{
    OrderID: orderID,
})
if err != nil {
    return fmt.Errorf("publish order created: %w", err)
}
log.Info("published order event", "id", id)
```

The event type is part of the envelope and is the handler lookup key on the
consumer side. A trailing `.v<N>` version segment is supported; use
`WithTopic` when the logical destination should not be derived from the event
type.

## Add routing and workflow metadata

Pass `PublishOption` values when the event needs explicit metadata:

```go
id, err := client.Publisher().Publish(ctx, "orders.created.v1", payload,
    f1.WithKey(orderID),
    f1.WithSubject("order/"+orderID),
    f1.WithIdempotencyKey("order-created/"+orderID),
    f1.WithCorrelationID(workflowID),
)
```

`WithKey` enables partition or per-key ordering where the connected driver
supports it. `WithIdempotencyKey` supplies the application key that a handler
can read through `Event.IdempotencyKey()`; F1 does not store seen keys or
deduplicate application effects.

Other supported options include priority, expiry, custom headers, causation,
an explicit topic, and a per-event maximum attempt count. Their constructors
and contracts live in [`publisher.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/publisher.go).

## Publish batches

`PublishBatch` reports one `MessageResult` per input message. It does not claim
atomicity: inspect each result and use `BatchResult.Failed()` when partial
failure handling matters.

For the full message path, topology timing, and close interaction, see the
[publish runtime guide](/development/publish-flow).
