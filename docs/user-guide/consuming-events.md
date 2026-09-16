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
in [Handling failures](/user-guide/handling-failures).

## Size the prefetch window

`Concurrency` is how many handlers run at once. `Prefetch` is the ceiling on
the subscription's in-flight budget, not the amount the broker is asked to
hand over at a time: each destination gets a window derived from the handler
concurrency and the fairness weights, and it is that window, not the
configured prefetch, that lets a handler that takes time find the next
delivery waiting instead of waiting on the broker round trip that follows its
own acknowledgement.

When the configured `Prefetch` is larger than those windows add up to, the
consumer is capped at the lower total rather than left to fetch work ahead of
the lanes that can hold it, and F1 logs a warning naming the configured value
and the effective one. That warning is about a budget you named: a subscription
that names no `Prefetch` takes the broker default silently, and is not told its
budget was capped, because the default is F1's number rather than yours. A
prefetch that was not honoured is therefore visible at startup rather than only
in a rate that never arrives. A prefetch smaller than the windows add up to
does not shrink them: each destination keeps the window its lanes need, and the
lanes, not the total, are what stop the driver fetching ahead of the work the
subscription can run.

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

Per-key order does not survive a retry. A delivery that fails is acknowledged
as soon as its retry copy is stored, which releases the key, so the next
message with that key is handled before the retry comes back. When a key must
not be handled out of order at all, either set `RetryConfig{MaxAttempts: 1}` so
a failure goes straight to the dead-letter destination, or make the handler
tolerate the reorder.

For worker, settlement, dispatch, and scheduling internals, see the
[consume flow](/development/consume-flow) and
[ordering and scheduling guide](/advanced-topics/ordering-and-scheduling).
