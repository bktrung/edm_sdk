# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project uses
[Semantic Versioning](https://semver.org/).

## [0.1.0] - Unreleased

### Added

#### Client and lifecycle

- Clients can be constructed with functional options and manage broker connections through an explicit lifecycle.
- The client reconnects after classified transient broker failures and reports the reconnect cause.
- Graceful shutdown drains accepted work through bounded lifecycle phases instead of dropping in-flight messages.
- Drivers can release unsettled deliveries during drain and expose capability limits to the client.
- Driver errors carry operation, kind, retryability, and the wrapped broker error for deliberate recovery decisions.

#### Publishing

- The publisher provides synchronous confirmed publication with topic validation and canonical topic handling.
- Publisher topology controls resolve publish and subscription destinations without exposing broker concepts to handlers.
- Messages use a versioned envelope wire mapping with stable identity, headers, and body data.
- Codec users can register read-side codecs while retaining the built-in JSON codec.
- Fanout capabilities and topology bindings support publish-time and consume-time fanout.
- Batch publication errors identify the first failed message index.

#### Consuming and retries

- Subscriptions validate handler-facing configuration before admitting a consumer.
- Handler middleware composes around the worker pipeline without changing the broker-facing port.
- The worker pool dispatches concurrent deliveries with settle-last lifecycle ordering.
- Terminal handler notifications include the original message body.
- Error handlers receive driver and successor-handoff failures through the configured client path.

#### Dead letters

- Retryable deliveries resolve through destination-specific retry tiers before dead-letter routing.
- Handler-attached death details are preserved on the dead-letter copy.
- Fatal consumer errors and undecodable messages are contained as poison outcomes instead of taking down the process.
- A failed successor publication is not acknowledged as successfully handed off.

#### Priorities and deadlines

- Priority scheduling promotes aged low-priority work to prevent starvation under sustained higher-priority load.
- The scheduler uses smooth weighted round robin to interleave groups while preserving configured shares. Every pick scans all groups instead of exiting at the first non-empty one, so the full scan is the cost of smoothness: the disabled-promotion weighted-pick benchmark measured 21.53 ns/op versus 17.84 ns/op at six groups, with overlapping three-run spreads of 17.01-21.58 and 17.81-17.84; at sixty groups, the medians were 118.8 ns/op versus 16.59 ns/op, with spreads of 77.69-123 and 12.4-16.65, respectively. The benefit is in the mid-weight lane: in the shipped three-lane benchmark with high 8, medium 4 and low 1, medium p95 settle latency measured 21.79-22.20ms against 26.14-26.42ms for the previous cursor-order loop over five runs each, with the selection loop as the only variable; the low lane did not move.
- Deferred deliveries can be held until their due time, with drivers declaring delay accuracy and rounding rules.
- Ordered subscriptions preserve per-key ordering while allowing independent keys to run concurrently.
- Ordered subscriptions reject a `Concurrency x Prefetch` product above 2,097,152 queued entries (64 MiB).

#### Kafka driver

- The Kafka driver opens real connections and reports capabilities derived from the broker.
- Kafka provides topology administration, confirmed production, consumer intake, maintenance, and per-message failure reporting.
- Kafka commits the contiguous acknowledged prefix for each partition and releases unsettled deliveries back to the consumer group.
- Kafka consumer groups balance ownership by lane and preserve safe ownership transitions across rebalances.
- Kafka rebalances cooperatively by default; `kafka.balancer` selects `cooperative-sticky`, `sticky`, or `range`, and any other value is refused at connection time.
- A Kafka consumer assigned fewer partitions than its subscription's slot budget logs a warning naming the destination, the partitions it holds, the budget, and which lever raises it.
- Kafka can defer fetching records until their due time.
- `lifecycle.rebalanceDrainTimeout` is validated against both `kafka.sessionTimeout` and `kafka.rebalanceTimeout`, and a refusal names the bound it exceeded and both timeout values.

#### RabbitMQ driver

- The RabbitMQ driver provides connections, per-destination consumers, topology declaration, confirmed publication, and delivery settlement.
- RabbitMQ exposes administrative inspection and conformance support for topology, queue, and consumer state.
- RabbitMQ parks delayed and fixed-delay retry messages in broker-backed delay queues.
- RabbitMQ detects queue-argument drift and supports management API configuration for topology checks.
- RabbitMQ validates secure remote endpoints and refuses unsafe TLS and plaintext combinations.
- RabbitMQ applies the same endpoint rules whether or not a vhost is present, and refuses a host-less endpoint with a message showing the expected form.
- Kafka and RabbitMQ TLS configuration accepts an explicit server name for certificate verification when an endpoint uses an IP address or alias.

#### In-memory driver and f1test

- The in-memory driver provides a shared broker implementation for deterministic end-to-end SDK behavior.
- f1test provides handler tests with a normal client surface, captured publication and dead-letter output, and an advanceable clock.
- f1test provides an observer recorder for asserting lifecycle event contracts.
- The f1test capture store keeps a pending wakeup when it is drained, so a test waiting on captured output is not stranded by a full buffer.

#### Configuration

- Config and LoadConfig validate startup settings before a client connects to a broker.
- Configuration selects topology policies and validates driver-dependent options against the driver that will run them.
- Broker endpoints, retry settings, lifecycle bounds, and production TLS requirements are validated explicitly.
- A prefetch of zero means unset on every route that can set one, so a file, environment, or explicit zero falls back to the configured default instead of being taken literally.

#### Observability

- f1.Observer receives publish, intake, process, settlement, retry, dead-letter, drain, reconnect, and driver lifecycle events.
- Observer events include backlog samples, enqueue timing, and deadline-promotion observations.
- f1otel adapts observer events to OpenTelemetry metrics and spans with application-owned providers and configured trace-context propagation.
- f1otel duration metrics carry exemplars linked to the operation span when tracing is enabled and the operation span is sampled.
- Observer events carry logical topic and priority context for publish, receive, settlement, retry, dead-letter, backlog, and deadline-promotion observations.
- Observer events report enqueue timestamps and backlog-head age when the connected driver provides them.
- Per-message log records carry the message's own context, so publish, dispatch, and settlement lines can be correlated to a single message.
- The user guide documents OpenTelemetry metrics, tracing, enqueue and backlog signals, and PromQL starting points for alerts.
- The user guide documents how the runner's lanes feed prefetch and how queue depth relates to the configured budget.
- Runnable Go examples cover client publish and consume and f1otel metric setup; the production consumer example adds readiness and signal-driven drain.
- The production consumer example completes its shutdown inside the default grace period and spends part of that budget flushing telemetry, so the example does not exceed the budget it demonstrates.

#### Tooling

- `make vulncheck` scans the root module and each tools module for known vulnerabilities, stopping on the first failing scan.
