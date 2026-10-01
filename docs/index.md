---
layout: home
hero:
  name: F1
  text: Event-driven messaging for Go services
  tagline: One SDK to publish and consume events. F1 owns delivery, retries, dead-lettering, priority and zero-loss shutdown; the broker is a driver you pick at startup.
  actions:
    - theme: brand
      text: Get started
      link: /learn/getting-started
    - theme: alt
      text: Quickstart
      link: /learn/quickstart
    - theme: alt
      text: Architecture
      link: /development/architecture
features:
  - title: At-least-once, stable identity
    details: Every retry, dead-letter and redelivery copy keeps the same idempotency key, so a handler can make its effect safe to apply twice.
    link: /advanced-topics/failure-handling
    linkText: Failure handling
  - title: Zero-loss shutdown
    details: A drain lets accepted deliveries finish within configured time limits. Unfinished deliveries remain available for broker redelivery, and Drain reports failure.
    link: /advanced-topics/lifecycle-and-shutdown
    linkText: Lifecycle and shutdown
  - title: Retry steps to a dead-letter queue
    details: Retryable failures move through growing retry delays. Exhausted or terminal failures dead-letter with a recorded death reason and error.
    link: /advanced-topics/failure-handling
    linkText: Failure handling
  - title: Starvation-free priority, ordering by key
    details: Weighted scheduling and overdue-lane promotion keep low-priority work moving without promising a maximum wait. Ordered mode serializes equal-key handlers, but delayed retries can run after later messages.
    link: /advanced-topics/ordering-and-scheduling
    linkText: Ordering and scheduling
  - title: Poison-message safety
    details: A message that cannot be decoded, or a handler that panics, is quarantined instead of retried forever or taking the process down.
    link: /advanced-topics/failure-handling
    linkText: Failure handling
  - title: Broker agnostic, limits declared
    details: In-memory, RabbitMQ and Kafka drivers sit behind one port. Client.Limits() reports what the connected driver can and cannot do.
    link: /advanced-topics/topology-and-capabilities
    linkText: Topology and capabilities
---

## At a glance

The driver is chosen once, when the client is built. Everything after that line is broker-free:
publish an event, handle it, and let F1 ack, retry, or dead-letter it.

```go
client, err := f1.New(ctx, cfg, f1.WithDriver(rabbitmq.Driver{}))
if err != nil {
	return err
}
defer client.Close(context.Background())

// Publish returns after the broker durably accepts the event. The topic must
// already be routable: topology provisioned, or the consumer below already running.
if _, err := client.Publisher().Publish(ctx, "orders.placed.v1",
	OrderPlaced{OrderID: "o-42"},
	f1.WithKey("o-42"),
); err != nil {
	return err
}

// A subscription maps event types to handlers; Run owns the delivery loop.
runner, err := client.Subscribe(ctx, f1.Subscription{
	Name:   "order-projector",
	Topics: []string{"orders.placed"},
	Handlers: map[string]f1.Handler{
		"orders.placed.v1": f1.HandlerFunc(func(ctx context.Context, e *f1.Event) error {
			var placed OrderPlaced
			if err := e.Decode(&placed); err != nil {
				return f1.Terminal(err) // retrying cannot fix a bad payload
			}
			return project(ctx, e.IdempotencyKey(), placed) // nil acks, an error retries
		}),
	},
})
if err != nil {
	return err
}
return runner.Run(ctx)
```

## Choose your path

<div class="f1-paths">
  <a class="f1-path" href="/learn/getting-started">
    <span class="f1-path__eyebrow">Build a service</span>
    <strong class="f1-path__title">Publish and consume events</strong>
    <span class="f1-path__body">Install the module, connect a driver, then follow Basics for the message and subscription model and Advanced for failures, ordering and shutdown.</span>
    <span class="f1-path__cta">Getting started -&gt;</span>
  </a>
  <a class="f1-path" href="/development/architecture">
    <span class="f1-path__eyebrow">Understand the runtime</span>
    <strong class="f1-path__title">Follow a message end to end</strong>
    <span class="f1-path__body">How a message travels from Publish to its final ack, and which package owns each step of the way.</span>
    <span class="f1-path__cta">Architecture map -&gt;</span>
  </a>
  <a class="f1-path" href="/development/driver-contract">
    <span class="f1-path__eyebrow">Write or review a driver</span>
    <strong class="f1-path__title">Implement the port</strong>
    <span class="f1-path__body">The interfaces every broker adapter implements, and the conformance suite that proves an adapter keeps F1's semantics.</span>
    <span class="f1-path__cta">Driver contract -&gt;</span>
  </a>
</div>

## How it fits together

Services talk to the `f1` package. The core talks to a standard-library-only driver port, and each
adapter translates that port into one broker. The core never imports a broker client;
`make verify-agnostic` enforces it. The [architecture map](/development/architecture) has the full
package picture.

```mermaid
flowchart TB
    APP[Your service<br/>handlers and events] --> CORE[package f1<br/>publish, consume, retry, drain]
    CORE --> PORT[driver port<br/>stdlib-only interfaces]
    PORT --> INMEM[drivers/inmem<br/>tests and local runs]
    PORT --> RABBIT[drivers/rabbitmq]
    PORT --> KAFKA[drivers/kafka]
```

## Go further

- [Getting started](/learn/getting-started) - connect a driver and run a service.
- [Message](/basics/message) - understand events, envelopes, and delivery identity.
- [Driver contract](/development/driver-contract) - implement or review a broker adapter.
