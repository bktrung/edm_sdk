# Consuming events

Create a `Subscription` with the topics it reads and a handler map keyed by
event type. `Subscribe` validates the subscription and returns a `Runner`; the
runner does not fetch messages until `Run` starts.

## Register a handler

```go
runner, err := client.Subscribe(ctx, f1.Subscription{
    Name:           "orders-worker",
    Topics:         []string{"orders.created"},
    Concurrency:    4,
    Prefetch:       16,
    Retry:          f1.RetryConfig{MaxAttempts: 3},
    HandlerTimeout: 10 * time.Second,
    Handlers: map[string]f1.Handler{
        "orders.created.v1": f1.HandlerFunc(handleOrderCreated),
    },
})
if err != nil {
    return err
}

runDone := make(chan error, 1)
go func() { runDone <- runner.Run(ctx) }()
```

The handler receives an `*f1.Event`. Decode the payload into a service-owned
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

Returning `nil` acknowledges the delivery. Failure classification is covered
in [Handling failures](handling-failures.md).

## Read event metadata

Handlers can inspect the event ID, type, subject, attempt number, priority,
headers, correlation and causation identifiers, raw payload, and the stable
idempotency key. Use the idempotency key when applying an effect that must be
safe across redelivery.

## Configure ordering

Use `Mode: f1.OrderedByKey` when the connected driver advertises the feature,
and publish the same logical key with `f1.WithKey`. F1 preserves order per key;
it does not provide global ordering across partitions or queues. Check
`client.Limits()` when a deployment must make a capability decision explicit.

For worker, settlement, and dispatch internals, see the [consume flow](../consume-flow.md)
and [dispatch guide](../dispatch-and-scheduling.md).
