# Handling failures

F1 delivers messages at least once. A handler can therefore see the same event
more than once, including after a successful effect followed by a settlement
failure. Make externally visible effects idempotent with the stable value from
`event.IdempotencyKey()`.

## Choose an outcome

| Handler result | Effect |
| --- | --- |
| `nil` | Acknowledge the delivery. |
| Ordinary error | Retry according to the subscription retry ladder. |
| `f1.RetryAfter(err, delay)` | Retry with an explicit delay, subject to configured limits. |
| `f1.Terminal(err)` | Skip retries and route the event to the F1 dead-letter destination. |
| `f1.Drop(err)` | Acknowledge without applying the effect or retaining a copy. |

Use terminal classification for invalid payloads, unsupported versions, and
other failures that will not succeed on another attempt:

```go
if err := event.Decode(&payload); err != nil {
    return f1.Terminal(fmt.Errorf("invalid order payload: %w", err))
}
```

Use an explicit retry delay when the service knows when a transient dependency
may recover:

```go
if err := reserveInventory(ctx, payload); err != nil {
    return f1.RetryAfter(err, 30*time.Second)
}
```

## Observe terminal outcomes

Register `OnDeadLetter` and `OnDiscarded` callbacks on the subscription for
service-level logging, metrics, or alert handoff. Keep callbacks short and
non-blocking; F1 invokes them asynchronously with a bounded notification
context.

Set `UnmatchedPolicy` to `f1.DeadLetter` when an event without a registered
handler must be retained. Use `f1.Ignore` only when losing unmatched events is
an intentional policy.

For the durable successor-before-ack rule and settlement states, see
[Settlement and shutdown](../settlement-and-shutdown.md). The root README lists
the delivery guarantees and non-goals that service behavior must respect.
