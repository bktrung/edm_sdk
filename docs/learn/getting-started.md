# Getting started

F1 is a generic event-messaging SDK for Go. It gives an application a stable
way to publish and consume events while a driver adapts that application to the
messaging system selected at deployment time.

This guide walks through the smallest useful F1 flow:

1. create a client with an application-selected driver;
2. publish a versioned event;
3. subscribe to its topic and decode it in a handler; and
4. drain the subscription and close the client safely.

The examples deliberately do not choose a broker. F1's application-facing code
should not need to change when the driver changes. Select and configure a
concrete driver in your application's composition root, then pass it to F1 as
an implementation of [`driver.Driver`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/driver/driver.go).

## The mental model

An event has two identities:

- Its **event type** describes the contract being handled, such as
  `orders.placed.v1`.
- Its **topic** is the logical destination that carries related events. By
  default, F1 derives `orders.placed` from `orders.placed.v1` by removing the
  trailing version segment.

The publisher sends an encoded payload with an envelope containing the event
type, ID, subject, routing metadata, and delivery metadata. A subscription
consumes one or more topics and routes each event to the handler registered for
its event type.

Delivery is at least once. A handler can see the same event again after a
redelivery, so the side effect performed by a handler should be idempotent.
`Event.IdempotencyKey()` gives the stable application key when the publisher
provided one, and otherwise falls back to the event ID.

## Install F1

The module is hosted on the project's private Go module host. Set `GOPRIVATE`
before downloading it so the Go tool does not query the public proxy or
checksum database, which cannot see that host, then add F1 to the application:

```sh
go env -w GOPRIVATE=fgit.zapps.vn
go get fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk
```

Use the Go version declared in [`go.mod`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/go.mod). Keep connection
endpoints, credentials, and driver-specific settings in your application's
configuration; do not put them in handler code.

## Choose a driver

The application selects the concrete driver at the composition boundary. The
service-facing code uses the root `f1` package; it does not use a broker client
directly. The repository currently includes:

- `drivers/kafka` for Kafka classic consumer groups and partition-bound scaling.
- `drivers/rabbitmq` for RabbitMQ.
- `drivers/inmem` for deterministic tests and local in-process use.

Every broker-specific setting a driver accepts, what it does, and what it
defaults to is listed in [Driver options](/user-guide/driver-options). The local
Kafka fixture is defined in [`docker/docker-compose.yml`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/docker/docker-compose.yml), and
`make test-kafka` / `make test-kafka-conformance` own its broker-backed verification.

Start the configuration from [`examples/config.yaml`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/examples/config.yaml), then provide the
service's broker endpoint, topology policy, and subscription settings.

## Connect the client

F1 requires two pieces at startup:

- a resolved [`f1.Config`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/config.go), normally loaded with
  `f1.LoadConfig`; and
- the concrete `driver.Driver` selected by the application.

Keeping driver selection outside the service logic is the important portability
boundary. The following helper is intentionally generic: `selectedDriver` is
created by your application's adapter/configuration layer.

```go
package orders

import (
	"context"
	"errors"
	"fmt"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func connect(ctx context.Context, configPath string, selectedDriver driver.Driver) (*f1.Client, error) {
	cfg, err := f1.LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("load F1 config: %w", err)
	}

	client, err := f1.New(ctx, cfg, f1.WithDriver(selectedDriver))
	if err != nil {
		return nil, fmt.Errorf("connect F1: %w", err)
	}
	return client, nil
}
```

`f1.New` opens the supplied driver before it returns. Startup failures are
therefore returned at the connection boundary, before the application begins
publishing or consuming. Keep the `broker.driver` value in the loaded config
aligned with the selected driver's `Name()`; F1 logs a driver-identity mismatch
so an accidental configuration split is visible.

Pass `f1.WithPublishTopics(...)` as well when F1 should ensure the publisher's
topology at startup. Production topology should normally be provisioned
separately and verified rather than auto-created. See [`config.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/config.go) and
[`options.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/options.go) for the owning definitions.

The exact configuration belongs to the selected driver and deployment. This
guide leaves that choice open on purpose; the rest of the application can use
the same F1 APIs regardless of which adapter is injected.

## Publish an event

Define a payload type that represents the event contract. Version the event
type when the contract changes:

```go
type OrderPlaced struct {
	OrderID string `json:"orderId"`
}

func publishOrder(ctx context.Context, client *f1.Client, orderID string) error {
	_, err := client.Publisher().Publish(
		ctx,
		"orders.placed.v1",
		OrderPlaced{OrderID: orderID},
		f1.WithKey(orderID),
		f1.WithSubject("order:"+orderID),
		f1.WithIdempotencyKey("order-placed:"+orderID),
	)
	if err != nil {
		return fmt.Errorf("publish order %s: %w", orderID, err)
	}
	return nil
}
```

`Publish` returns only after the selected driver reports durable broker
acknowledgement. The returned event ID is useful for logs and tracing. The
event type determines the default logical topic; use `f1.WithTopic` only when
the application needs an explicit topic that differs from that convention.

`WithKey` carries the business routing key. It is also the default key used by
the driver when no other routing key is supplied, so a stable value such as an
order ID is usually preferable to a random value.

## Subscribe and handle events

A subscription names its consumer group, declares the logical topics it reads,
and maps event types to handlers. The subscription below uses the same topic
derived from `orders.placed.v1`:

```go
func subscribeOrders(ctx context.Context, client *f1.Client) (*f1.Runner, error) {
	runner, err := client.Subscribe(ctx, f1.Subscription{
		Name:   "order-projector",
		Topics: []string{"orders.placed"},
		Handlers: map[string]f1.Handler{
			"orders.placed.v1": f1.HandlerFunc(func(ctx context.Context, event *f1.Event) error {
				var placed OrderPlaced
				if err := event.Decode(&placed); err != nil {
					return f1.Terminal(fmt.Errorf("decode %s: %w", event.Type(), err))
				}

				// Replace this with the application's idempotent side effect.
				if err := applyOrder(ctx, event.IdempotencyKey(), placed); err != nil {
					return err
				}
				return nil
			}),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create order subscription: %w", err)
	}
	return runner, nil
}
```

The handler result controls delivery:

- return `nil` after the side effect succeeds; F1 settles the event as handled;
- return a normal error for a transient failure; F1 applies the configured
  retry policy; and
- wrap an unrecoverable error with `f1.Terminal` when retrying cannot help.

The handler should use the event's idempotency key when writing its side effect.
Do not use the delivery attempt as a deduplication key: attempts can change
when the same event is redelivered.

The `applyOrder` call in the example stands for the application's own side
effect. It should record or update state using the idempotency key so a
redelivery cannot apply the same business operation twice.

If a handler is not registered for an event type, the subscription's
`UnmatchedPolicy` determines whether F1 ignores or dead-letters that event. Set
the policy explicitly when that distinction matters to the application.

## Run and shut down

`Subscribe` validates the subscription and returns a runner. `Run` owns the
delivery loop; it blocks until the context is canceled or the runner stops with
an error. Shutdown is `client.Close`: it stops delivery, lets in-flight handlers
finish within the drain budget, and then releases the client. In the example
below, `ctx` is only the shutdown trigger, and the runner gets a context that
the trigger's cancel cannot reach:

```go
func runOrders(ctx context.Context, client *f1.Client, runner *f1.Runner) error {
	runDone := make(chan error, 1)
	go func() {
		// Run keeps the values ctx carries, but not its cancellation: the
		// shutdown trigger must not cut in-flight handlers off.
		runDone <- runner.Run(context.WithoutCancel(ctx))
	}()

	var runErr error
	runReturned := false
	select {
	case runErr = <-runDone:
		runReturned = true
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := client.Close(shutdownCtx); err != nil {
		return fmt.Errorf("close F1 client: %w", err)
	}

	// Close drains the runner, so Run returns even though its context was
	// never cancelled.
	if !runReturned {
		runErr = <-runDone
	}
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return fmt.Errorf("order subscription stopped: %w", runErr)
	}
	return nil
}
```

In a real service, watch the process's termination signals and shut down by
calling `client.Close` with a fresh timeout context, or `runner.Drain` for a
single subscription, instead of cancelling `Run`'s context: cancelling `Run`
skips the grace period and abandons the handler that is already in flight. The
[independent shutdown contexts](/advanced-topics/lifecycle-and-shutdown#use-independent-shutdown-contexts)
section shows the pattern and the budgets it uses. Give shutdown a separate
timeout so a stalled handler or driver cannot keep the process alive forever.
For a whole-process shutdown, `client.Close` drains every registered runner,
waits until no publish is in flight, and releases the driver's producer and
connection resources. Call `runner.Drain` directly when you need to stop one
subscription while keeping the client alive for other work.

## Run the local example

The root [README](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/README.md) contains the complete RabbitMQ fixture quickstart. It
builds separate publisher and consumer services from
[`examples/`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/examples) so you can see the composition boundary in a
runnable form.

## Design around these guarantees

Before moving beyond the first example, make these choices explicit in the
service design:

- **At-least-once handling:** handlers may receive a redelivery.
- **Idempotent effects:** deduplicate using a stable business key, not an
  attempt number.
- **Versioned contracts:** add a new event type version when the payload
  contract changes; keep old handlers until their events are retired.
- **Failure classification:** ordinary errors are retryable, terminal errors
  bypass retries, and successful handlers return `nil`.
- **Lifecycle ownership:** the process that creates the client owns its
  runners and must drain and close them.
- **Driver portability:** application composition chooses the adapter; business
  handlers should depend on F1 events, not driver packages.

## Next steps

- [Publishing events](/user-guide/publishing-events) - routing metadata and batches.
- [Consuming events](/user-guide/consuming-events) - handlers, metadata, and ordering.
- [Handling failures](/user-guide/handling-failures) - retry, terminal, drop, and dead-letter.

## Continue from the source contracts

When you need more detail, read the symbols that own the behavior:

- [`Publisher`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/publisher.go) - event construction, routing metadata, and
  durable publish behavior;
- [`Subscription`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/subscription.go) - topics, handlers, retry, and
  delivery policy;
- [`Event`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/event.go) - envelope access, decoding, and idempotency;
- [`HandlerFunc`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/handler.go) and [`Terminal`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/errors.go) - handler
  adaptation and failure classification; and
- [`driver.Driver`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/driver/driver.go) - the adapter boundary used by the
  application composition layer.
