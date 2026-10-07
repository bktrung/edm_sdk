# Changelog

F1's notable changes are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- RabbitMQ `Open` refuses a `broker.rabbitmq.vhost` that differs from any endpoint's
  parsed vhost, and SASL credentials set without a mechanism, with a fatal error before dialing.
- Documentation qualifies shutdown, scheduling, ordering, and retry timing guarantees,
  reconciles RabbitMQ publishing with shared confirm channels, and removes duplicate
  analysis and standalone JavaScript simulators.
- The RabbitMQ driver publishes on 16 shared confirm channels and waits for each message's own
  confirmation, instead of holding one of 16 channels for a whole `Publish` call. Publish
  throughput now follows the offered rate until the broker or its disk is the limit: 10,000
  msgs/s at a 5.4 ms median latency where the previous producer reached about 2,500. A cancelled
  publish no longer closes its channel.
- A RabbitMQ destination with a delay has one parking queue, `<destination>.park`, instead of a
  ladder of eight rung queues (`<destination>.park.500ms` to `<destination>.park.64s`) or a
  `<destination>.park.fixed-<N>ms` queue. Every message waits the destination's own delay.
  `EnsureTopology` refuses a delay longer than 2,147,483,647 ms.
- Zero or omitted `broker.defaultPrefetch` selects automatic sizing from the resolved lane
  capacities instead of a fixed 64. A zero or omitted subscription prefetch uses a positive
  `broker.defaultPrefetch` when one is set, and automatic sizing otherwise. A positive
  subscription prefetch overrides it, and an environment zero clears a lower-priority
  subscription value before that fallback. A total above the lane-capacity sum is capped without
  shrinking any destination's window.
- A positive subscription prefetch from 1 to 65535 is valid even below the lane count: the total
  cap limits admission without reducing destination windows.
- Consumers enforce both the subscription's total prefetch and each destination's window over
  the deliveries the SDK has admitted and not yet settled. RabbitMQ broker credit and Kafka fetch
  buffering stay separate from that admission, and RabbitMQ's `brokerPrefetch` cannot bypass
  either ceiling.
- A subscription's `fairness.weights` must be at most 65535 per priority; a larger weight fails
  validation. Weights are ratios, and the bound lets automatic prefetch size every lane exactly:
  large weights used to shrink it, for example to 128 where 512 was right.
- The Kafka driver batches offset commits: the commits that arrive while one request is in
  flight go out together in the next. A settlement still returns only after the request carrying
  its offset finished.
- Deferred conformance measures release lateness from a bracket around publication, excluding
  time spent publishing while retaining the never-early and late-delivery checks.
- Breaking, draft observer API: `FinishEvent.Results` is now `[]ObserverMessageResult`
  (`ID`, `ErrorClass`) instead of `[]MessageResult`, so observers no longer receive raw
  per-message errors. An observer that checked `Err == nil` checks `ErrorClass == ""` and
  `ID != ""` instead. `PublishBatch` results and `MessageResult.Err` returned to the application
  are unchanged.
- `f1.New` refuses an f1otel observer that is already attached to another live Client. Create
  one `f1otel.New` per Client and share the OpenTelemetry providers between them. An observer
  opts in through the new optional `f1.ObserverBinder` interface; the binding is released when
  `Close` completes terminal shutdown or when `New` fails.

### Removed

- `f1.RetryAfter` and `f1.RetryDelay`. A delayed retry always waits its retry step's delay; a
  handler cannot choose its own.
- `driver.DelayAccuracy` and `driver.Capabilities.DelayAccuracy`. The `native_delay` entry of
  `Client.Limits` now says where a delayed message waits instead of rendering a lateness bound.
- `driver.OutboundMessage.DelayUntil` and `driver.DestinationSpec.FixedDelay`. A destination's
  `Delay` applies to every message published to it.
- `conformance.DeferralModel` and `conformance.Suite.DeferralModel`.
- The driver-flip acceptance's redundant same-checkout source-tree comparison and
  business-tree manifest artifacts. Both broker runs still use the same binaries built once.

### Fixed

- A reconnect failure classified by the driver as non-transient is terminal, just like
  exhausted attempts. Publishes, `Client.Health`, and further reconnect requests return
  that error. After any terminal reconnect failure, `Client.Health` joins the reconnect
  error with stopped-subscription errors; match it with `errors.Is`, not `==`.
- Closing a spare producer created by concurrent first publishes is bounded by
  `Lifecycle.CloseTimeout`, so a stuck broker no longer hangs the publish.
- Dead letters for messages whose envelope cannot be decoded respect the header size
  limit. A message whose dead-letter headers can never fit is dropped and reported,
  and consumption continues.
- `Envelope.EncodeHeaders` no longer sheds `datacontenttype` to fit the header size
  limit. Headers that cannot fit with it return `ErrEnvelopeTooLarge`.
- RabbitMQ management uses the AMQP-parsed URI login, including defaults, or the selected
  PLAIN/AMQPLAIN SASL identity. EXTERNAL retains the URI login for management.
- RabbitMQ `Open` bounds TCP connect, TLS and AMQP handshakes by the connect timeout, so a peer
  that accepts TCP but never answers no longer hangs connection setup. A cancelled connect
  closes its socket at once instead of leaking it.
- RabbitMQ consumer `Stop` and `Release` share one teardown. A concurrent or repeated call
  returns success only after the consumer is unregistered from its connection and `Messages`
  and `Errors` are closed, so closing the connection afterwards no longer fails.
- A runner drained while waiting for a connection rebuild completes its terminal
  consumer teardown before `Run` returns, including when reconnect has already
  released the consumer.
- RabbitMQ management 401/403 responses are classified as permission errors and no longer retried.
- Reconnect retains retired resources after close errors or timeouts, preserving
  producer/consumer-before-connection order. `Client.Close` returns an error until every
  retired teardown succeeds, rejoining running attempts or retrying failed retirements
  once per caller-issued close without background retries. Each retirement wait has its
  own `Lifecycle.CloseTimeout` bound.
- Kafka drain and release fence settlement-side handoffs as well as polling, so an in-flight
  handoff cannot send after drain returns.
- The RabbitMQ consumer's `Stop` and `Release` wait for their goroutines only as long as the
  caller's context allows. A `Release` that ends on its context leaves the consumer registered,
  and the next `Release` finishes the teardown instead of the first being reported as done.
- A RabbitMQ consumer `Stop` whose drain succeeded no longer returns `ErrDrainTimeout` when its
  deadline passes just after the drain. It finishes the shutdown and closes the consumer's lanes.
- A RabbitMQ consumer `Stop` that takes over the shutdown of a failed `Release` is bounded by its
  own context, and returns a retryable `ErrDrainTimeout` if that context ends. A `Stop` racing a
  `Release` no longer waits on reader goroutines that only the `Release` could end.
- The Kafka consumer clears a delivered record's slot in its pending queue, so the delivered
  record and its fetch data can be freed sooner.
- f1otel metrics no longer carry `f1.priority=medium` when the priority is unknown: a
  `PublishBatch` of mixed priorities, or an abandoned stage. The new `FinishEvent.PriorityKnown`
  field reports whether `Priority` carries the stage's delivery lane; F1 sets it on every Finish
  that has one. A batch's topic and priority are resolved independently. Metrics and spans name
  the consumer group by the same rule: `ConsumerGroup`, else `Subscription`.
- The Kafka consumer re-arms its head-hold timer after it fires for a held retry whose destination
  is paused, so the next held retry is delivered when its own delay elapses instead of waiting for
  unrelated traffic.
- The RabbitMQ driver refuses a message whose correlation ID, message ID, type or content type is
  longer than 255 bytes as too large, before anything is written, instead of letting the AMQP client
  close the whole shared connection.
- The RabbitMQ driver refuses a message whose custom header name makes its AMQP table key longer
  than 255 bytes as too large, before anything is written, instead of letting the client close the
  whole connection.

### Upgrade notes

- RabbitMQ: the parking queues 0.1.0 declared, `<destination>.park.<rung>` and
  `<destination>.park.fixed-<N>ms`, are no longer used. Messages already in them still return to
  their destination when their delay ends. `EnsureTopology` reports those queues as orphaned,
  with their message counts. `Prune` refuses such a queue while it holds messages, but a
  destination's own prune checks only `<destination>.park`, not the old queues. Prune the old
  queues once they are empty, and only then the destination their messages return to.

## [0.1.0] - 2026-09-23

### Added

#### Client and lifecycle

- Clients can be constructed with functional options and manage broker connections through an explicit lifecycle.
- The client reconnects after classified transient broker failures and reports the reconnect cause.
- Graceful shutdown drains accepted work through bounded lifecycle phases instead of dropping in-flight messages.
- Drivers can release deliveries not yet acked during drain and expose capability limits to the client.
- A runner drain releases its consumer after any failed `Stop`, so a consumer left registered cannot keep `Client.Close` from finishing.
- A consumer whose `Release` fails is released again by the next `Client.Close`, or before a reconnect closes the connection it was opened on. `Lifecycle.CloseTimeout` bounds every `Release`, including the reconnect and lane-repair paths.
- `Runner.Drain` returns the drain failure that `Run` returns.
- A reconnect replays subscription topologies up to four at a time.
- `Client.Close` waits up to `Lifecycle.CloseTimeout` for a reconnect in progress to stop, and a reconnect bounds the close of the producer and connection it retires by the same limit.
- Driver errors carry operation, kind, retryability, and the wrapped broker error for deliberate recovery decisions.

#### Publishing

- The publisher provides synchronous confirmed publication with topic validation and canonical topic handling.
- Publisher topology controls resolve publish and subscription destinations without exposing broker concepts to handlers.
- Messages use a versioned envelope wire mapping with stable identity, headers, and body data.
- Codec users can register read-side codecs while retaining the built-in JSON codec.
- Fanout capabilities and topology bindings support publish-time and consume-time fanout.
- Batch publication errors identify the first failed message index.
- A message the driver reports as failed without a cause is still reported as failed.
- `WithCodec` rejects two codecs for one content type or name, which would publish with one and read with the other.
- A batch failure report that names an index outside the batch fails the whole batch instead of reporting it as published.
- The `f1producer` header carries `service/env/instanceID`.
- A `PublishError` whose causes are all notifications reports `KindNotification` and is not retryable.
- A publish started while the client reconnects is refused at once, so callers retrying in a loop cannot hold the reconnect back.

#### Consuming and retries

- Subscriptions validate handler-facing configuration before letting a consumer start.
- Handler middleware composes around the worker pipeline without changing the broker-facing port.
- The worker pool dispatches concurrent deliveries and acks each source message only after its handling path finishes.
- Terminal handler notifications include the original message body.
- Error handlers receive driver failures and failures to publish a retry or dead-letter copy through the configured client path.
- A failed retry or dead-letter copy reaches the error handler as F1's own error wrapping the cause, so `driver.Classify` reports the cause's kind, or no kind for an unclassified cause.
- A not-found, too-large or permission consumer error is reported without ending the generation, and a later fatal error still fails the subscription.
- `driver.ConsumerConfig.DestinationPrefetch` gives each destination's share of the subscription budget, so the Kafka and RabbitMQ adapters split it the way the core counts it.
- A delivery's retry and dead-letter destinations follow the destination it was read from, even when its headers do not decode or name another priority.
- When a retry or dead-letter copy fails to publish and the consumer then refuses to be released, the runner stops instead of fetching on that consumer.
- `driver.Classify` and `*driver.Error` methods accept a typed-nil `*driver.Error` and read it as transient.

#### Dead letters

- Retryable deliveries resolve through destination-specific retry steps before dead-letter routing.
- Handler-attached death details are preserved on the dead-letter copy.
- Fatal consumer errors and undecodable messages are contained as poison outcomes instead of taking down the process.
- When a retry or dead-letter copy fails to publish, the source message is not acked.

#### Priorities and deadlines

- Priority scheduling lets an overdue low-priority lane jump the queue to prevent starvation under sustained higher-priority load.
- The scheduler uses smooth weighted round robin to interleave groups while keeping their configured shares. Each pick scans every group, a small cost that grows with the group count, and in the three-lane benchmark it lowered the medium lane's p95 latency. See [Benchmarks](docs/development/benchmarks.md) for the measurements.
- Deferred deliveries can be held until their due time, with drivers declaring delay accuracy and rounding rules.
- Ordered subscriptions preserve per-key ordering while allowing independent keys to run concurrently.
- Ordered subscriptions reject a `Concurrency x Prefetch` product above 2,097,152 queued entries (64 MiB).
- A delivery's lane wait includes the time it waits for room in a full lane.

#### Kafka driver

- The Kafka driver opens real connections and reports capabilities derived from the broker.
- Kafka provides topology administration, confirmed production, consumer intake, maintenance, and per-message failure reporting.
- Kafka commits each partition's offset past the acked record, skipping offsets Kafka never delivered (compaction, transaction markers, retention), and releases deliveries not yet acked back to the consumer group.
- Kafka endpoints must be `host:port`; `Open` refuses an endpoint containing `://` or `@`, and credentials belong in `broker.sasl`.
- Kafka consumer groups balance ownership by lane and preserve safe ownership transitions across rebalances.
- Kafka rebalances cooperatively by default; `kafka.balancer` selects `cooperative-sticky`, `sticky`, or `range`, and any other value is refused at connection time.
- A Kafka consumer assigned fewer partitions than its subscription's prefetch logs a warning naming the destination, the partitions it holds, the prefetch, and what to scale.
- Kafka can defer fetching records until their due time.
- `lifecycle.rebalanceDrainTimeout` is validated against both `kafka.sessionTimeout` and `kafka.rebalanceTimeout`, and a refusal names the bound it exceeded and both timeout values.
- A Kafka ack or nack that races a draining consumer's partition revoke no longer reads the settler's tracker without its lock.

#### RabbitMQ driver

- The RabbitMQ driver provides connections, per-destination consumers, topology declaration, confirmed publication, and delivery acks and nacks.
- RabbitMQ exposes administrative inspection and conformance support for topology, queue, and consumer state.
- RabbitMQ parks delayed and fixed-delay retry messages in broker-backed delay queues.
- RabbitMQ logs one warning per destination when the quorum queue kind declares a non-durable destination as durable.
- Under `TopologyNone`, RabbitMQ routes retry copies to the fixed-delay parking queue the spec describes, the one an operator provisions.
- RabbitMQ refuses unreadable TLS material at `Open` as a fatal error instead of retrying until the connect timeout.
- The RabbitMQ management client works over TLS when the application replaced `http.DefaultTransport`.
- A failed RabbitMQ consumer re-attach is reported even when a burst of consumer cancels has filled the error buffer.
- RabbitMQ reports no native priority, since it declares no `x-max-priority` queue; the core's per-priority destinations carry priority.
- RabbitMQ detects queue-argument drift and supports management API configuration for topology checks.
- RabbitMQ validates secure remote endpoints and refuses unsafe TLS and plaintext combinations.
- RabbitMQ applies the same endpoint rules whether or not a vhost is present, and refuses a host-less endpoint with a message showing the expected form.
- Kafka and RabbitMQ TLS configuration accepts an explicit server name for certificate verification when an endpoint uses an IP address or alias.
- RabbitMQ verifies each endpoint's certificate against that endpoint's own host name when `TLS.ServerName` is empty, so a failed dial to one node no longer breaks failover to the next.
- A second RabbitMQ consumer `Release` waits for one still in progress instead of reporting success early.

#### In-memory driver and f1test

- The in-memory driver provides a shared broker implementation for deterministic end-to-end SDK behavior.
- f1test provides handler tests with a normal client surface, captured publication and dead-letter output, and an advanceable clock.
- f1test sorts captured messages by the destination kind F1 declared, so a topic with `dlq` in its name counts as published.
- f1test provides an observer recorder for asserting lifecycle event contracts.
- The f1test capture store keeps a pending wakeup when it is drained, so a test waiting on captured output is not stranded by a full buffer.
- `f1test.Client.Advance` reads the release hook under the lock a reconnect writes it under.
- An f1test publish waits at most one second for its destination to be set up, then returns the driver's error, so a publish to a topic nothing declared no longer waits for its context to end.

#### Configuration

- Config and LoadConfig validate startup settings before a client connects to a broker.
- Configuration selects topology policies and validates driver-dependent options against the driver that will run them.
- Broker endpoints, retry settings, lifecycle bounds, and production TLS requirements are validated explicitly.
- A prefetch of zero means unset on every route that can set one, so a file, environment, or explicit zero falls back to the configured default instead of being taken literally.
- The environment and subscription names may use only letters, digits, `-`, and `_`, so two configurations cannot name the same destination and `prod/us` cannot share production's destinations without its checks.
- A retry step longer than 2,147,483,647 ms (about 24.8 days) is rejected at startup.
- Config loading and `Subscribe` run the same subscription checks, so a subscription that loads can subscribe. Both reject two topics that name the same topic, a negative fairness budget, `retryWeightDivisor` or `prefetchFactor`, and an ordered buffer too large to allocate.

#### Observability

- f1.Observer receives events for publish, intake, processing, finishing a message, retry, dead-letter, drain, reconnect, and the driver lifecycle.
- Observer events include backlog samples, enqueue timing, and queue-jump observations.
- f1otel adapts observer events to OpenTelemetry metrics and spans with application-owned providers and configured trace-context propagation.
- f1otel duration metrics carry exemplars linked to the operation span when tracing is enabled and the operation span is sampled.
- f1otel spans carry `server.address` and `server.port` when only tracing is configured.
- Observer events carry logical topic and priority context for publish, receive, finishing a message, retry, dead-letter, backlog, and queue-jump observations.
- Observer events report enqueue timestamps and backlog-head age when the connected driver provides them.
- Per-message log records carry the message's own context, so the lines for a publish, a dispatch, and the finishing of a message can be correlated to that message.
- The user guide documents OpenTelemetry metrics, tracing, enqueue and backlog signals, and PromQL starting points for alerts.
- The user guide documents how the runner's lanes feed prefetch and how queue depth relates to the configured prefetch limit.
- Runnable Go examples cover client publish and consume and f1otel metric setup; the production consumer example adds readiness and signal-driven drain.
- The production consumer example completes its shutdown inside the default grace period and spends part of that time limit flushing telemetry, so the example does not exceed the limit it demonstrates.

#### Tooling

- `make vulncheck` scans the root module and each tools module for known vulnerabilities, stopping on the first failing scan.

### Known limitations

- Delivery is at-least-once; no exactly-once transport or effects, and no SDK deduplication.
- Kafka consumption concurrency is capped by the partition count of the destination.
- On an ordered subscription, a retried message can be delivered after later messages for the same key.
- A delayed retry waits its retry step's fixed delay, and a requested `RetryAfter` rounds to the nearest step, on both brokers.
- Priority scheduling and fairness are tuned for handlers that take roughly 100 ms or longer; at sub-millisecond handler times the scheduler's own cost is visible.
- A handler that ignores cancellation past four times `HandlerTimeout` keeps running after F1 requeues its delivery, so the redelivery can run beside it; handlers must return on context cancellation.
- A dead-letter or discarded callback that ignores its context keeps its goroutine and the event until it returns; the number of such goroutines is not capped.
- RabbitMQ `Purge` and `DescribeTopology` cover only the current parking queues; copies parked under an earlier retry step or delay shape return to the destination when their delay ends.

[0.1.0]: https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tags/v0.1.0
