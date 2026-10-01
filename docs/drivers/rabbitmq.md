# RabbitMQ driver

F1's RabbitMQ driver sends and receives messages over AMQP and reads the broker's queues and bindings through the RabbitMQ management HTTP API.

For shared configuration and security, see [Drivers and capabilities](/drivers-and-capabilities).

## How the driver works

The RabbitMQ adapter translates the port into AMQP and management operations.
The provider-specific ownership is split across these files in
[`drivers/rabbitmq`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/drivers/rabbitmq):

- `rabbitmq.go` for connection, TLS/SASL,
  and the feature list;
- `producer.go` for publishing, confirms,
  returns, and delayed messages;
- `consumer.go` for delivery lanes,
  pause/resume, drain, and lag;
- `settlement.go` for ack/nack
  serialization;
- `topology.go` for exchanges, queues,
  bindings, and the broker's own dead-letter routes; and
- `management.go` for inspection and
  pruning.

Use the RabbitMQ suite for queue, exchange, management, confirmation,
reconnect, TLS, and broker-specific flow control. Use conformance when
the behavior is part of the shared port.

### Publishing shares a few confirm channels

Application publishes, retry copies, and dead-letter copies share the producer's
confirm-mode channels. A call waits for its own per-message confirmations
without exclusively holding a channel through that wait.

[`producer.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/producer.go)
owns the channel count, bounded batch segments, ID reservations, cancellation,
and channel-failure recovery. Order is preserved within each call; later
segments can use other channels after the earlier segment's outcomes are
decided. Concurrent calls have no relative ordering guarantee.

Shared channels avoid a fixed ceiling on concurrent calls, but do not promise
a particular throughput. Use [Benchmarks](/development/benchmarks) for
measurement context and measure your own workload.
The [publish-channel rationale](/deep-dives/rabbitmq-publish-pool) explains why
returns need message IDs, why cancellation leaves a shared channel usable,
and why recovery after a channel failure must account for ordering and
duplicates.

### The management HTTP API is a deployment requirement

The adapter inspects broker-side state through the RabbitMQ management HTTP
API, not through AMQP. A passive declaration confirms a queue's name and
nothing else, so a driver that must compare the arguments a queue was declared
with against what the broker holds has no AMQP channel for that comparison.
The same applies to bindings, which AMQP cannot enumerate. The requirement
therefore reaches the subscription path:

| Topology policy | Management API needed to start a subscription |
| --- | --- |
| `TopologyDeclare` | Yes. The subscription's bindings are read before the consumer opens. |
| `TopologyVerify` | Yes. The broker's queue arguments and the bindings are both read. |
| `TopologyNone` | No. The adapter makes no management call, so the topology must already exist. |

The endpoint defaults to the AMQP host with the AMQP port plus 10000, and uses
the AMQP credentials unless `broker.sasl` overrides them. Set
`broker.rabbitmq.managementPort` when the management plugin listens elsewhere.
The endpoint's host is always the AMQP host, and only its port can be
overridden. A managed offering that serves the management API on a separate
hostname cannot be configured today: there is no host option, so a port setting
does not reach it and a deployment of that shape needs the API exposed on the
AMQP host or a proxy in front of it.

An unreachable API fails subscription start with an error naming the endpoint,
rather than skipping the check; a broker with the management plugin disabled,
firewalled, or on a non-default port is the most likely cause.

The local fixture (`make broker-up`) runs a `-management` image with the API on
15672. Managed RabbitMQ offerings differ in whether that API is exposed and on
which credentials, so check it before deploying.

### Delayed and retried messages need per-destination [parking queues](/learn/glossary#parking-queue)

RabbitMQ has no built-in delayed delivery, so the adapter builds it from a
message expiration and a dead-letter route back to the destination.

Each delayed destination, which in F1 means each [retry step](/learn/glossary#retry-tier),
has exactly one parking queue, named `<destination>.park`, for example
`orders.retry.2.park`. Every message published to the destination is routed to
that queue and carries the destination's delay, rounded up to whole
milliseconds, as its expiration. When it expires, RabbitMQ dead-letters it
through the default exchange into the destination, where the consumer reads it.

The queue carries no TTL of its own, and its name carries no delay, so changing
a step's delay needs no change to the queue: the next copy simply carries the new
delay. Under the head-of-queue expiration model, equal-delay copies avoid
holding a shorter delay behind a longer one. After a delay decrease, or while
a rolling deployment publishes both delays, that head-of-line blocking can
return within the step.

Expiration makes a copy eligible for dead-letter routing once it reaches the
queue head; it does not bound when the transfer completes, when a consumer
picks it up, or when a handler finishes. See
[Parking retries on RabbitMQ](/deep-dives/rabbitmq-delay-ladder) for the
illustrative queue sequence and failure trade-offs.

The longest delay a message expiration holds is 2,147,483,647 ms, about 24.8
days; the adapter refuses a longer one at topology time under every policy.

The name shape is reserved: a destination may not end in `.park`, because that
shape belongs to parking queues.

The parking queue is declared for every destination with a delay, and which
policy makes it exist is the same split as any other destination:

| Topology policy | Parking queue |
| --- | --- |
| `TopologyDeclare` | Declared by the adapter at subscription start. |
| `TopologyVerify` | Must exist and match its declared arguments, checked at subscription start. |
| `TopologyNone` | Provisioned by the operator. The adapter declares and checks nothing. |

The queue is durable, and its declare arguments follow the deployment's queue
type (`broker.rabbitmq.queueType`, `quorum` by default):

| Argument | Value |
| --- | --- |
| `x-queue-type` | `quorum`, or `classic` when the deployment selects it |
| `x-dead-letter-exchange` | `""`, the default exchange |
| `x-dead-letter-routing-key` | the destination name |
| `x-dead-letter-strategy` | `at-least-once`, on quorum only |
| `x-overflow` | `reject-publish`, on quorum only |

On quorum the two strategy arguments are added because all three dead-letter
arguments are required together for RabbitMQ's at-least-once dead-letter
guarantee; the parking queue is the delay mechanism itself, not a failure
path, so a message lost there is a dropped retry with no error. A classic
deployment omits them, and its delay path is at-most-once.

An application that empties a destination (`Purge`) empties its parking queue
too, and one that deletes a destination (`Prune`) is refused while any parking
queue of it still holds a message.

Under `TopologyNone` a missing parking queue is discovered by the publish that
needed it. The broker returns the message (`312 NO_ROUTE`) rather than closing
the channel, and the publish failure names the missing queue, the policy that
obliges you to create it, and its declare arguments. The producer recovers on
its own: creating the queue while the service runs is enough, with no restart.

This obligation is RabbitMQ's alone, and it is worth knowing that the same topology policy does not
mean the same thing on both drivers. [Kafka carries the delay in a record header on the
destination's own topic](/drivers/kafka#kafka-delayed-records) and the consuming side holds the record
until its due time, so a delayed or retried message needs no topology beyond the destination topic.
RabbitMQ's version needs the parking queue, so `TopologyNone` obliges an operator to provision the
destinations and their parking queues here, and only the topics there.

Consumer delivery timeouts are a destination-queue setting rather than a
parking-queue one, and they follow the same policy split. Setting
`broker.rabbitmq.consumerTimeout` reaches the broker as the queue argument
`x-consumer-timeout`, on quorum destination queues only: the broker refuses the
argument on a classic queue, so a classic deployment never declares it, and when
the key is absent nothing is declared and the broker's own default stays in
force. The argument is fixed when the queue is created, which is what decides
the upgrade path for a deployment whose destination queues already exist.

A default deployment runs `TopologyVerify`. Against an existing queue, startup
compares `x-consumer-timeout` with the requested value. It logs a warning under
`f1 topology argument drift` with `argument=x-consumer-timeout` instead of
failing, and the queue keeps the broker default.

Under `topology.autoCreate` (`TopologyDeclare`), there is no drift check. The
existing queue is found and left as it was. The adapter never actively
redeclares the argument because the broker refuses that operation with
`406 PRECONDITION_FAILED`. Delete and recreate an existing queue to gain the
setting; the drift warning is the signal to do so.

## RabbitMQ options

| Key | What it does | Accepted values | Default | Read at |
| --- | --- | --- | --- | --- |
| `broker.rabbitmq.vhost` | Vhost the management API inspects when it reads queue arguments and bindings. It does not change the AMQP connection: the endpoint URI still selects the vhost that messages are published to. | Any string, used as the vhost name. Empty falls back to the endpoint URI. | The endpoint URI's vhost as the AMQP client parses it: `/` when the URI has no path, and `orders` for `amqp://host/orders`. | Open |
| `broker.rabbitmq.queueType` | Queue type every destination and its parking queue is declared with. `classic` also clears the delivery-count and dead-letter capabilities, both of which are quorum arguments. Quorum queues are always durable, so a destination declared non-durable is declared durable and the driver logs one warning for it. | `quorum` or `classic`, case-insensitive, with surrounding whitespace ignored. Any other value fails `Open`, and `env: prod` requires the exact string `quorum`. | `quorum` | Open |
| `broker.rabbitmq.consumerTimeout` | `x-consumer-timeout` declared on quorum destination queues. The broker cancels a consumer that has held one delivery this long. | Go duration of at least `1ms`, and at least three times every subscription's `handlerTimeout`. Shorter, zero, negative, or unparsable fails `Open`. | None: nothing is declared and the broker's own default stays in force. | Open |
| `broker.rabbitmq.brokerPrefetch` | Broker transport credit per destination, separate from the [SDK admission total and destination windows](/advanced-topics/configuration#prefetch-resolution). Extra broker deliveries wait in bounded driver pending buffers; this option never bypasses either SDK ceiling. | Integer from 1 to 65535, at least every destination's core window. A non-integer, zero, negative value, or smaller value fails `Open` or consumer creation. | Unset: each destination uses its core window. | Open |
| `broker.rabbitmq.managementPort` | Port the RabbitMQ management HTTP API listens on. | Integer from 1 to 65535. Any other value fails `Open`. | The AMQP port plus 10000, so 15672 for the usual 5672. | Open |
| `broker.rabbitmq.trustBrokerTimestamp` | Trusts RabbitMQ's `timestamp_in_ms` header as the broker enqueue time. Enable this only when `message_interceptors.incoming.set_header_timestamp.overwrite = true`; without that setting the header is publisher-controlled. | Boolean, using Go's accepted boolean spellings after surrounding whitespace is trimmed. Any other value fails `Open`. | `false` | Open |

`broker.rabbitmq.vhost` deserves extra care because the management client and
the AMQP connection must read the same vhost from the endpoint. Both parse it
once through the AMQP client: `amqp://host/orders` is the vhost `orders`, and a
URI with no path is the default vhost `/`. The readings agree for the
percent-encoded `amqp://host/%2Forders` vhost `/orders`. Set this key only when
the management API must inspect a vhost other than the one the endpoint names.

Extra broker credit carries a RabbitMQ-specific risk. On a quorum queue, closing
a consumer that holds deliveries increments the broker delivery count for each
of them, so a crash loop can dead-letter messages that no handler ever saw.
Classic queues have no broker delivery count, so they do not carry that risk.
With the option set, the deliveries held in the driver's channel are bounded by
the option value per destination, so a subscription with several destinations
can hold that value times the destination count. The other costs of the option
are in [Ordering and
scheduling](/advanced-topics/ordering-and-scheduling#configure-execution-capacity).

### Delivery counts

RabbitMQ `DeliveryCount` describes broker delivery/acquisition history. It
must be interpreted separately from the F1 retry attempt, the number of
handler invocations, and the number of business side effects. A delivery held
by broker prefetch can contribute to that history when the consumer is
released even if no handler ran.

[`consumer.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/consumer.go),
at `deliveryCount`, owns the mapping from broker metadata, including the
priority of `x-acquired-count` over `x-delivery-count` and the unavailable-count
sentinel. The
[broker-prefetch integration scenario](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/broker_prefetch_integration_test.go)
is the evidence owner for held-delivery redelivery.


## RabbitMQ TLS

| YAML key | What it does |
| --- | --- |
| `broker.tls.enabled` | Set to `true` to enable TLS for RabbitMQ connections. When enabled, every endpoint must use the `amqps://` scheme. |
| `broker.tls.caFile` | Path to a PEM file containing trusted CA certificates. When set, the driver uses those certificates to verify the broker. |
| `broker.tls.certFile` | Path to the client certificate PEM file for mutual TLS. Set it together with `broker.tls.keyFile`; providing only one of the pair fails when RabbitMQ opens. |
| `broker.tls.keyFile` | Path to the private key for `broker.tls.certFile`. |
| `broker.tls.serverName` | TLS server name used to verify the broker certificate. Set it when the endpoint is an IP address or the certificate is issued for an alias. Leave it empty to derive the name from the endpoint. |
| `broker.tls.insecureSkipVerify` | Set to `true` only for a test endpoint. It disables server certificate verification and is refused for other endpoints and in `prod`. |

## RabbitMQ SASL

| YAML key | What it does |
| --- | --- |
| `broker.sasl.mechanism` | Selects `plain`, `amqplain`, or `external`, case-insensitively. Leave it empty to use no explicit SASL mechanism. |
| `broker.sasl.username` | Username passed to the `plain` or `amqplain` mechanism. Credentials are never logged. |
| `broker.sasl.password` | Password passed to the `plain` or `amqplain` mechanism. Credentials are never logged. |

An unsupported RabbitMQ mechanism makes `Open` return
`rabbitmq: unsupported SASL mechanism %q; supported mechanisms: PLAIN, AMQPLAIN, EXTERNAL, or empty`,
with the unsupported value substituted for `%q`. In `prod`, selecting SASL
also requires `broker.tls.enabled: true`.

## Go further

- [Drivers and capabilities](/drivers-and-capabilities) - shared configuration, security, and the feature list.
- [Parking retries on RabbitMQ](/deep-dives/rabbitmq-delay-ladder) - understand queued delay and its trade-offs.
- [Driver contract](/development/driver-contract) - implement the broker-neutral port.
- [Driver conformance](/development/driver-conformance) - validate portability across adapters and capability profiles.
