# Middleware

F1 middleware wraps an [`f1.Handler`](../../handler.go) with behavior that is
useful across handlers but should not be part of one handler's business logic.
Typical examples are structured logging, timing, tracing, and application-level
metrics.

F1 currently exposes one middleware extension point. It does not ship a
catalog of retry, timeout, deduplication, or transport middleware. Retry and
delivery settlement remain core runtime responsibilities.

## Where middleware runs

The middleware boundary is part of the handler path:

1. `f1.New` records middleware supplied through `f1.WithMiddleware`.
2. `Client.Subscribe` wraps each handler in the subscription with that chain.
3. `Runner` invokes the wrapped handler with the delivery context and event.
4. F1 classifies the returned error and settles the delivery outside the
   middleware chain.

The owning implementation is [`buildHandlerChain`](../../handler.go), and
subscription registration is wired by [`wrapHandlers`](../../subscription.go).
Middleware does not see driver messages or broker settlement references.

## Define middleware

Middleware has the shape `func(f1.Handler) f1.Handler`. It can run code before
and after the next handler while preserving the handler's context, event, and
error result:

```go
func withLogging(next f1.Handler) f1.Handler {
	return f1.HandlerFunc(func(ctx context.Context, event *f1.Event) error {
		started := time.Now()
		err := next.Handle(ctx, event)

		log.Printf("handled event type=%s id=%s elapsed=%s error=%v",
			event.Type(), event.ID(), time.Since(started), err)
		return err
	})
}
```

The middleware should normally return the downstream error unchanged. If it
adds context, wrap the error with `%w` so F1 can still recognize classified
errors such as `f1.Terminal`, `f1.Drop`, and `f1.RetryAfter`.

## Register middleware

Pass middleware as client options when the client is constructed:

```go
client, err := f1.New(
	ctx,
	cfg,
	f1.WithDriver(selectedDriver),
	f1.WithMiddleware(withLogging),
)
if err != nil {
	return fmt.Errorf("create F1 client: %w", err)
}
```

`WithMiddleware` is client-wide. It wraps every non-nil handler registered
through that client, including handlers in different subscriptions. F1 rejects
nil middleware during client construction.

There is no separate F1 option for registering middleware on one handler. If a
cross-cutting behavior belongs to only one handler, wrap that handler directly
when building the subscription:

```go
runner, err := client.Subscribe(ctx, f1.Subscription{
	Name:   "orders-worker",
	Topics: []string{"orders.created"},
	Handlers: map[string]f1.Handler{
		"orders.created.v1": withLogging(f1.HandlerFunc(handleOrderCreated)),
	},
})
```

Use client-wide `WithMiddleware` for a service-wide invariant. Use direct
wrapping for a deliberately local behavior. Keep the choice visible at the
composition boundary.

## Ordering and panic recovery

Middleware is executed in the order supplied to `WithMiddleware`:

```go
f1.WithMiddleware(first, second)
```

The effective call sequence is:

```text
first before
  second before
    handler
  second after
first after
```

F1's internal panic-recovery wrapper is outside the user chain. A panic from a
handler or middleware is therefore converted into an error that the runtime can
classify instead of escaping the worker goroutine. Do not add a recovery layer
just to make normal F1 usage safe; add one only when the application needs
additional logging or panic policy around its own boundary.

Middleware order is part of application behavior. Put outer concerns such as
request logging or tracing first, and put concerns that should measure only the
inner handler nearer to the end of the chain.

## Preserve context and event ownership

The handler context carries cancellation and the configured handler deadline.
Pass it to downstream calls:

```go
func withDependencyMetrics(next f1.Handler) f1.Handler {
	return f1.HandlerFunc(func(ctx context.Context, event *f1.Event) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		log.Printf("starting downstream work for event type=%s", event.Type())
		return next.Handle(ctx, event)
	})
}
```

Do not replace the context with `context.Background()` and do not retain it for
work that outlives the delivery. During drain, F1 cancels handler work and waits
for in-flight delivery settlement within the configured lifecycle budgets.

`Event` is the handler-facing view of a delivery. Middleware may inspect its
accessors and call `Decode`, but it should not make the handler depend on a
driver-specific message or settlement object. See [Message](message.md) for
payload, envelope, header, and identity guidance.

## Error propagation and settlement

Middleware participates in handler execution; it does not settle deliveries.
The final error returned by the chain is interpreted by the F1 runtime:

| Chain result | Runtime outcome |
| --- | --- |
| `nil` | The delivery is acknowledged as handled. |
| Ordinary error | F1 applies the subscription retry policy. |
| `f1.RetryAfter(err, delay)` | F1 retries with the requested delay. |
| `f1.Terminal(err)` | F1 stops retrying and dead-letters the event. |
| `f1.Drop(err)` | F1 acknowledges the event without applying its effect or retaining a copy. |

The dispatch implementation keeps this classification and settlement outside
the middleware chain; see [`dispatchMessage`](../../worker.go). A middleware
that logs an error must still return it. Swallowing the error by returning `nil`
changes the delivery outcome to success.

If middleware needs to add context to a failure, use wrapping:

```go
return fmt.Errorf("record handler metrics: %w", err)
```

Do not implement a second retry or dead-letter loop by calling the handler
again from middleware. Configure retry on the [`Subscription`](../../subscription.go)
and use the classification helpers in [`errors.go`](../../errors.go).

## Middleware is not an error callback

F1 has callback surfaces for events that are not part of the handler pipeline:

| Surface | Scope | Use it for |
| --- | --- | --- |
| `WithMiddleware` | Every handler on a client | Cross-cutting behavior around handler execution |
| `WithErrorHandler` | Client-level asynchronous runtime errors | Driver error-stream failures and bounded retry/dead-letter successor-publish failures |
| `Subscription.OnDeadLetter` | One subscription | Notification when an event reaches the dead-letter path |
| `Subscription.OnDiscarded` | One subscription | Notification for unmatched or explicitly dropped events |
| `WithLogger` | Client runtime | Structured SDK logs |

`WithErrorHandler` does not receive ordinary errors returned by a handler;
those errors already flow through retry and dead-letter classification. Its
callback may receive a nil event for a connection-level error, and it must not
block delivery or shutdown. The option's contract is documented in
[`options.go`](../../options.go).

Keep dead-letter and discarded callbacks short. They are notification surfaces,
not replacement handler pipelines. Use the subscription's
[`DeadLettered`](../../subscription.go) and [`Discarded`](../../subscription.go)
payloads to inspect the event and failure reason.

## Testing middleware

Test middleware at two levels:

1. Unit-test the wrapper with a small fake handler to prove ordering, context
   propagation, and error preservation.
2. Use [`f1test`](../../f1test/f1test.go) to verify the wrapped handler's
   observable behavior through a real `Client`, `Subscription`, and `Runner`.

Include cases for success, a wrapped retryable error, a wrapped terminal error,
and a panic if the middleware changes panic visibility or logging. The expected
settlement belongs to the handler/runtime contract, not to a mock Ack/Nack
implementation.

## Continue from here

- [Message](message.md) — event data, metadata, context, and identity;
- [Publisher and subscriber](pubsub.md) — publish, subscribe, runner, and
  lifecycle boundaries;
- [`Terminal`, `Drop`, and `RetryAfter`](../../errors.go) — failure
  classification helpers;
- [Consume flow](../development/consume-flow.md) — dispatch, classification, and
  settlement internals; and
- [`Middleware`](../../handler.go) — the executable type and composition owner.
