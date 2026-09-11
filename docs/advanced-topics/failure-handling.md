# Failure handling

Failure is part of normal message delivery. A handler tells F1 what should
happen by returning an error; F1 classifies that result, publishes any retry
or dead-letter successor, and settles the original delivery. The handler does
not acknowledge, reject, or requeue a driver message directly.

F1 provides at-least-once delivery. A successful handler run and its business
side effects can still be followed by a process or connection failure before
the original delivery is settled, so handlers that change external state
should use a stable idempotency key. See [Message](../basics/message.md) and
[Publisher and subscriber](../basics/pubsub.md) for the delivery model.

## Choose the outcome

Return the outcome that matches the business meaning of the failure:

| Handler result | F1 outcome |
| --- | --- |
| `nil` | Acknowledge the delivery as handled. |
| An ordinary error | Retry according to the subscription ladder, then dead-letter when the effective attempt limit is reached. |
| `f1.RetryAfter(err, delay)` | Retry with an explicit delay for this attempt, still subject to the attempt limit. |
| `f1.Terminal(err)` | Stop retrying and publish the message to the dead-letter destination. |
| `f1.Drop(err)` | Acknowledge the delivery without applying its effect or retaining a dead-letter copy. |

For example, a temporary downstream outage should remain retryable:

```go
func handleOrder(ctx context.Context, event *f1.Event) error {
	var order Order
	if err := event.Decode(&order); err != nil {
		return f1.Terminal(fmt.Errorf("decode order: %w", err))
	}

	if err := reserveInventory(ctx, order); err != nil {
		return f1.RetryAfter(err, 30*time.Second)
	}
	return nil
}
```

`reserveInventory` is application code. The important distinction is that a
temporary dependency failure asks F1 to try again, while a payload that cannot
be decoded cannot become valid on a later attempt and should be terminal.

Return the error instead of logging and returning `nil`. Returning `nil`
changes the delivery outcome to success.

### Preserve classification while adding context

Classification helpers work through wrapped errors. Add context with `%w` so
F1 can still find `Terminal`, `Drop`, or `RetryAfter` in the error chain:

```go
if err := validateOrder(order); err != nil {
	return f1.WithDetails(
		f1.Terminal(fmt.Errorf("validate order: %w", err)),
		map[string]string{"validation": "order"},
	)
}
```

`f1.WithDetails` adds diagnostic values to a terminal or max-attempts
dead-letter copy. Details do not affect routing, retries, settlement, or
ordering. Keys must be lowercase alphanumeric; F1 accepts at most 16 keys and
1 KiB of encoded detail data. Invalid or excess details are discarded and
reported when the message is dead-lettered. See [`WithDetails`](../../errors.go)
for the exact contract.

## Retry policy

The subscription's [`RetryConfig`](../../config.go) defines the retry ladder:

```go
runner, err := client.Subscribe(ctx, f1.Subscription{
	Name:  "orders-worker",
	Topics: []string{"orders.created"},
	Retry: f1.RetryConfig{
		MaxAttempts:     5,
		InitialInterval: time.Second,
		Multiplier:      2,
		MaxInterval:     time.Minute,
		Jitter:          0.2,
	},
	Handlers: map[string]f1.Handler{
		"orders.created.v1": f1.HandlerFunc(handleOrder),
	},
})
```

The effective attempt limit is the smaller of the event's
[`WithMaxAttempts`](../../publisher.go) value and the subscription policy.
An event can lower the policy ceiling, but cannot raise it. `MaxAttempts` is
the total delivery-attempt cap, including the first delivery; the event's
`Attempt()` value is one-based. Once the current attempt reaches that cap, an
ordinary or `RetryAfter` error becomes `ReasonMaxAttempts` and follows the
dead-letter path.

When no explicit retry tiers are configured, F1 derives them from
`InitialInterval`, `Multiplier`, `MaxInterval`, and `Jitter`. Explicit
`Tiers` can define the delays directly. `RetryAfter` selects the delay for the
current retry but does not bypass the configured attempt cap. The validation
and delay calculation live in [`config.go`](../../config.go); keep service
policy in configuration rather than implementing a second retry loop in a
handler or middleware.

## Dead-letter handling

F1 publishes a dead-letter successor when a message cannot or should not be
retried. The dead-letter copy carries the original body and envelope metadata,
plus a death reason and the last error. Current automatic reasons include:

- `terminal` — the handler returned `f1.Terminal`;
- `max_attempts` — the retry ladder was exhausted;
- `panic` — the handler or middleware panicked;
- `decode` — the envelope, codec, or body could not be decoded;
- `expired` — the event expired before handling;
- `unmatched` — no handler matched and the subscription selected
  `f1.DeadLetter`; and
- `poison` — the retry metadata exceeded the runtime's sanity limit.

The reason is available in the dead-letter envelope and in the
[`DeadLettered`](../../subscription.go) callback payload:

```go
subscription := f1.Subscription{
	Name:            "orders-worker",
	Topics:          []string{"orders.created"},
	UnmatchedPolicy: f1.DeadLetter,
	OnDeadLetter: func(ctx context.Context, dead f1.DeadLettered) {
		log.ErrorContext(ctx, "event dead-lettered",
			"id", dead.Envelope.ID,
			"reason", dead.Reason,
			"attempt", dead.Attempt,
			"destination", dead.Destination,
			"error", dead.LastErr,
		)
	},
}
```

`OnDeadLetter` is a notification surface. Its `Body` and `Envelope` are
independent copies, but the callback must not be the only place where a
correctness-critical action happens. F1 invokes the callback asynchronously
with a bounded notification context; a slow or panicking callback must not
stall delivery or shutdown.

The dead-letter publication is confirmed before F1 acknowledges the original
delivery. If the successor cannot be published within the bounded successor
handoff budget, F1 reports the runtime error and releases the consumer without
acknowledging the original delivery, allowing the driver to redeliver it. This
settle-last ordering prevents a failed dead-letter handoff from becoming
silent loss. The full settlement sequence is described in
[Lifecycle and shutdown](lifecycle-and-shutdown.md).

## Drop and unmatched events

Use `f1.Drop` only when the event is intentionally handled without applying an
effect and should not be retained for later inspection. F1 acknowledges it and
notifies `Subscription.OnDiscarded` with `DiscardDropped`:

```go
if alreadyCancelled(order) {
	return f1.Drop(errors.New("order was cancelled before processing"))
}
```

An event with no matching handler is a separate case. The default
`UnmatchedPolicy` is `f1.Ignore`: F1 acknowledges the event and reports
`DiscardUnmatched` through `OnDiscarded`. Set `UnmatchedPolicy: f1.DeadLetter`
when an unmatched event must be retained instead of silently discarded.

Do not use `Drop` for a transient dependency failure. It removes the event
from the delivery path just as surely as a successful acknowledgement.

## Idempotency and duplicate delivery

Retry and dead-letter handoff are successor operations. F1 publishes the
successor first and acknowledges the current delivery second, but a process can
still stop after the handler's side effect and before either settlement step
completes. The original or successor may therefore be observed more than once.

Use [`Event.IdempotencyKey()`](../basics/message.md#delivery-identity-and-redelivery)
as the input to application-owned deduplication when an effect must be safe to
repeat. A producer can set the key with
[`WithIdempotencyKey`](../../publisher.go); otherwise F1 falls back to the
event ID. The key is an input to the application's idempotency store, not a
deduplication database managed by F1.

`Event.Attempt()` is useful for diagnostics and policy decisions, but it is
not an identity. Never use the attempt number as a deduplication key.

## Runtime error visibility

There are two different kinds of failure notification:

| Surface | Receives | Does not receive |
| --- | --- | --- |
| `Subscription.OnDeadLetter` | A confirmed dead-letter outcome and its reason | A failed successor handoff that was not confirmed |
| `Subscription.OnDiscarded` | An unmatched or explicitly dropped event | Retried or dead-lettered handler errors |
| `f1.WithErrorHandler` | Driver-level asynchronous errors and retry/dead-letter successor-publish failures | Ordinary errors returned by a handler |

Configure [`WithErrorHandler`](../../options.go) for runtime failures that the
handler result cannot represent. The event argument identifies the affected
delivery when there is one; it is `nil` for a connection-level error. The
callback runs asynchronously with a bounded context and must remain safe to
drop or repeat. Ordinary handler errors are already represented by the retry
or dead-letter policy and are not sent to this callback a second time.

## Test the policy, not a driver

Failure behavior is easiest to verify with [`f1test`](../../f1test/f1test.go),
which uses the in-memory driver and a manually advanced clock:

```go
func TestOrderFailurePolicy(t *testing.T) {
	client := f1test.NewClient(t)
	client.Deliver(t, "orders.created.v1", Order{ID: "order-123"})

	// Let the handler run, then move fake time across the configured retry tier.
	client.Advance(time.Minute)

	deadLetters := client.DLQ()
	if len(deadLetters) != 1 {
		t.Fatalf("dead letters = %d, want 1", len(deadLetters))
	}
}
```

The example shows the test surface; a real test should register a subscription
whose handler deliberately returns the failure being exercised and then assert
the complete policy: attempt count, delay, dead-letter reason, body, and
whether `OnDiscarded` or `OnDeadLetter` ran. Use `Advance` instead of sleeping.
Cover at least:

1. an ordinary error that retries and then reaches `ReasonMaxAttempts`;
2. `RetryAfter` choosing an explicit delay;
3. `Terminal` bypassing the retry ladder;
4. `Drop` acknowledging without a dead-letter copy;
5. malformed or expired input becoming a dead letter; and
6. duplicate-safe business effects using the idempotency key.

The in-memory driver and test helper are useful for policy tests, but driver
compatibility still belongs to the driver's conformance suite. See
[`f1test`](../../f1test/f1test.go) and the [driver conformance package](../../driver/conformance/)
for the two boundaries.

## Common mistakes

- Returning `nil` after logging a failure, which acknowledges the event.
- Retrying a deterministic decode or validation failure instead of returning
  `f1.Terminal`.
- Returning a wrapped classification without `%w`, which hides the original
  error from F1.
- Assuming `WithDetails` changes retry routing; it only adds terminal or
  max-attempts dead-letter diagnostics.
- Using `Attempt()` as a business identity.
- Performing manual driver acknowledgement or retry logic from a handler,
  middleware, or notification callback.
- Treating `OnDeadLetter`, `OnDiscarded`, or `WithErrorHandler` as a durable
  workflow step instead of an observation surface.

## Continue from here

- [Message](../basics/message.md) — envelope metadata, identity, and
  at-least-once handler design;
- [Publisher and subscriber](../basics/pubsub.md) — the publish/consume
  boundary and handler lifecycle;
- [Middleware](../basics/middleware.md) — where cross-cutting handler logic
  runs and where classification remains outside the chain;
- [Lifecycle and shutdown](lifecycle-and-shutdown.md) — successor
  handoff, acknowledgement, release, and drain behavior; and
- [`errors.go`](../../errors.go), [`worker.go`](../../worker.go), and
  [`subscription.go`](../../subscription.go) — executable failure contracts.
