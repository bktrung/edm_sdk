# Drivers and capabilities
This page combines the configuration values an application sets with
provider-specific behavior and operational differences. The broker-independent
port contract is documented in [Driver contract](/development/driver-contract),
and the shared portability checks are documented in
[Driver conformance](/development/driver-conformance).

## Driver configuration

A driver accepts only the broker-specific keys listed on this page. The
configuration loader checks the list by name, so a key outside it is refused
before the driver opens. Each driver has its own list:
`broker.kafka.compression` is a Kafka option and is refused on a RabbitMQ
deployment.

Options live under the driver's name, and every value in the configuration file
is a string, whatever type the driver parses it as:

```yaml
f1:
  broker:
    driver: kafka
    endpoints:
      - localhost:19092
    kafka:
      compression: lz4
      batchLinger: 5ms
```

The **Accepted values** column says what the driver takes, not what the broker
supports. A value the driver cannot translate fails `Open`, and an `Open`
failure is fatal, so a process never starts on a value that would silently do
nothing. Some keys are also constrained earlier by configuration and
subscription validation; those checks are listed in
[Validation before the driver](#validation-before-the-driver).

The **Read at** column says when an option takes effect. *Open* means it is
resolved once when the driver opens. *Per consumer* means it is resolved again
for each consumer the driver creates. *Topology* means it is read when the
driver declares or verifies destinations rather than at connection time.

### Environment overrides

F1 applies these environment variables after loading the configuration file, so
an environment value wins over the corresponding file value:

| Variable | Field set | Details |
| --- | --- | --- |
| `F1_ENV` | `f1.env` | Replaces the environment name. |
| `F1_SERVICE` | `f1.service` | Replaces the service name. |
| `F1_INSTANCE_ID` | `f1.instanceId` | Replaces the instance identity. |
| `F1_BROKER_DRIVER` | `f1.broker.driver` | Replaces the selected broker driver. |
| `F1_BROKER_ENDPOINTS` | `f1.broker.endpoints` | Comma-separated endpoints. F1 trims surrounding blanks and drops empty items. |

Each subscription also reads its own variables when `Subscribe` resolves it.
The name is `F1_SUBSCRIPTIONS_`, then the subscription name, then the setting,
each upper-cased with a camelCase boundary or punctuation turned into `_`:

```sh
F1_SUBSCRIPTIONS_ORDERS_CONCURRENCY=8
F1_SUBSCRIPTIONS_ORDERS_HANDLER_TIMEOUT=45s
F1_SUBSCRIPTIONS_ORDERS_RETRY_MAX_ATTEMPTS=5
```

The settings covered are `topics`, `mode`, `concurrency`, `prefetch`,
`priorities`, `handlerTimeout`, `unmatchedPolicy`, the `retry` fields
(`maxAttempts`, `initialInterval`, `maxInterval`, `multiplier`, `tiers`), and
the `fairness` fields (`prefetchFactor`, `retryWeightDivisor`,
`disableDeadlinePromotion`). A variable under a subscription's prefix that
matches none of them fails `Subscribe`, so a misspelled key is an error rather
than a silent no-op.

No other setting has an environment override. Keep credentials and other
driver-specific settings in the configuration file or the application's
configuration layer.

### Security

For a normal deployment, enable TLS and provide a CA file so the client
verifies the broker certificate. Add a client certificate and key when the
broker requires mutual TLS. The `broker.tls` and `broker.sasl` settings below
are shared configuration fields, but each driver accepts its own SASL
mechanisms.

#### Kafka TLS

| YAML key | What it does |
| --- | --- |
| `broker.tls.enabled` | Set to `true` to enable TLS for Kafka connections. |
| `broker.tls.caFile` | Path to a PEM file containing trusted CA certificates. When set, the driver uses those certificates to verify the broker. |
| `broker.tls.certFile` | Path to the client certificate PEM file for mutual TLS. Set it together with `broker.tls.keyFile`; providing only one of the pair fails when Kafka opens. |
| `broker.tls.keyFile` | Path to the private key for `broker.tls.certFile`. |
| `broker.tls.serverName` | TLS server name used to verify the broker certificate. Set it when the endpoint is an IP address or the certificate is issued for an alias. Leave it empty to derive the name from the endpoint. |
| `broker.tls.insecureSkipVerify` | Set to `true` only for a test endpoint. It disables server certificate verification and is refused for other endpoints and in `prod`. |

Kafka uses `broker.tls.serverName` when it is non-empty. When it is empty,
the client library verifies the certificate against the endpoint host. If the
endpoint is an IP address and the server name is empty, the certificate must
carry that IP address.

#### Kafka SASL

| YAML key | What it does |
| --- | --- |
| `broker.sasl.mechanism` | Selects `plain`, `scram-sha-256`, or `scram-sha-512`, case-insensitively. Leave it empty to disable SASL. Every non-empty mechanism requires non-empty `broker.sasl.username` and `broker.sasl.password`. |
| `broker.sasl.username` | Username passed to the selected SASL mechanism. Required when `broker.sasl.mechanism` is non-empty. |
| `broker.sasl.password` | Password passed to the selected SASL mechanism. Credentials are never logged. |

An unsupported Kafka mechanism makes `Open` return
`kafka: unsupported SASL mechanism %q`, with the unsupported value substituted
for `%q`. In `prod`, selecting SASL also requires `broker.tls.enabled: true`.
A Kafka `prod` configuration without SASL may run without TLS on a private
network cluster; enable TLS whenever the cluster is on an untrusted network.

#### RabbitMQ TLS

| YAML key | What it does |
| --- | --- |
| `broker.tls.enabled` | Set to `true` to enable TLS for RabbitMQ connections. When enabled, every endpoint must use the `amqps://` scheme. |
| `broker.tls.caFile` | Path to a PEM file containing trusted CA certificates. When set, the driver uses those certificates to verify the broker. |
| `broker.tls.certFile` | Path to the client certificate PEM file for mutual TLS. Set it together with `broker.tls.keyFile`; providing only one of the pair fails when RabbitMQ opens. |
| `broker.tls.keyFile` | Path to the private key for `broker.tls.certFile`. |
| `broker.tls.serverName` | TLS server name used to verify the broker certificate. Set it when the endpoint is an IP address or the certificate is issued for an alias. Leave it empty to derive the name from the endpoint. |
| `broker.tls.insecureSkipVerify` | Set to `true` only for a test endpoint. It disables server certificate verification and is refused for other endpoints and in `prod`. |

#### RabbitMQ SASL

| YAML key | What it does |
| --- | --- |
| `broker.sasl.mechanism` | Selects `plain`, `amqplain`, or `external`, case-insensitively. Leave it empty to use no explicit SASL mechanism. |
| `broker.sasl.username` | Username passed to the `plain` or `amqplain` mechanism. Credentials are never logged. |
| `broker.sasl.password` | Password passed to the `plain` or `amqplain` mechanism. Credentials are never logged. |

An unsupported RabbitMQ mechanism makes `Open` return
`rabbitmq: unsupported SASL mechanism %q; supported mechanisms: PLAIN, AMQPLAIN, EXTERNAL, or empty`,
with the unsupported value substituted for `%q`. In `prod`, selecting SASL
also requires `broker.tls.enabled: true`.

Both drivers refuse `broker.tls.insecureSkipVerify: true` outside a test
endpoint, and the configuration validator always refuses it in `prod`.

### Kafka options

| Key | What it does | Accepted values | Default | Read at |
| --- | --- | --- | --- | --- |
| `broker.kafka.compression` | Codec the producer compresses record batches with. A named codec stands alone: the producer does not fall back to another codec. | `none`, `gzip`, `snappy`, `lz4`, or `zstd`, case-insensitive. Any other value fails `Open`. | `snappy`, falling back to no compression for a batch snappy does not shrink. | Open |
| `broker.kafka.batchLinger` | How long the producer waits for a batch to fill before sending it. `0` sends as soon as there is something to send. | Go duration, zero or positive. A negative duration fails `Open`. | `10ms` | Open |
| `broker.kafka.fetchMaxBytes` | Byte ceiling a consumer asks each broker for in one fetch. | Integer from 1 to 2147483647. Any other value fails `Open`. | `52428800` (50 MiB) | Open |
| `broker.kafka.fetchMaxWait` | Maximum time Kafka holds an empty fetch; bounding it prevents an exhausted destination from blocking a sibling on the same broker beyond this value. | Whole-millisecond Go duration from `1ms` to `596h31m23.647s`. Any other value fails `Open`. | `50ms` | Open, and per consumer |
| `broker.kafka.sessionTimeout` | Session timeout the member carries in its join request; the group coordinator expires the member after that much silence. | Positive Go duration, and at least as long as `lifecycle.rebalanceDrainTimeout` divided by 0.6. | `45s` | Open |
| `broker.kafka.rebalanceTimeout` | Window the coordinator allows a member to complete a rebalance before removing it from the group. It must be greater than the configured `lifecycle.rebalanceDrainTimeout`. | Positive Go duration, and above the revoke wait bound. | `60s` | Open |
| `broker.kafka.staticMembership` | Adds the configured instance ID to the join request, so a member that restarts inside its session timeout rejoins without a rebalance. It changes nothing when no instance ID is configured. | Boolean, in the spellings `1`, `t`, `T`, `TRUE`, `true`, `True` and their `0`, `f`, `F`, `FALSE`, `false`, `False` counterparts. Any other value fails `Open`. | `true` | Open, and per consumer |
| `broker.kafka.balancer` | Group balancer protocol the consumer clients join with. | `cooperative-sticky`, `sticky`, or `range`. `lane` is refused. | `cooperative-sticky` | Open |
| `broker.kafka.maxExpectedInstances` | Partition floor: a destination with fewer partitions than this is refused, and a declaration that names no partition count gets this many instead. `0` leaves both decisions to the destination. | Non-negative integer. | `0`, no floor | Topology |

`broker.kafka.maxExpectedInstances` guards against a topic that was created with
fewer partitions than the service can use. It applies to declaration and
verification, not to `TopologyNone`, where the driver declares and checks
nothing. Under `TopologyVerify` an existing topic below the floor is a startup
failure. Under `TopologyDeclare` a request for fewer partitions than the floor
is refused before a topic is created.

#### Kafka parallelism and partitions

Kafka admits at most one delivery from each partition owned by a consumer. A
subscription's effective handler parallelism is therefore the smaller of its
`Concurrency` and the number of partitions each member holds for a destination.
Fairness weights can divide the available slots, but a weight above the assigned
partition count cannot create another delivery.

When a member is assigned fewer partitions than the destination's resolved slot
budget, the driver emits one warning with the fields `assigned_partitions`,
`budget`, and `lever`. Increase the destination's partition count, or use
`broker.kafka.maxExpectedInstances` to require that floor during topology setup.
Raising a partition count re-maps keys already published, and the partition
count cannot be lowered, so treat that change as a one-way capacity decision.

#### Kafka retry timing

Kafka retry records are due at their record timestamp plus the retry tier's
delay. `RetryDelay` and other custom per-delivery delays are not honored by this
driver; the configured retry ladder supplies the destination delay. The record
timestamp is the producer's `CreateTime` when the topic uses the default
`message.timestamp.type=CreateTime`. A topic configured with
`message.timestamp.type=LogAppendTime` uses the broker append time instead.
Publisher clock skew and append lag can make a retry late, never early.

#### Kafka rebalancing

The default group protocol is cooperative-sticky. The old `lane` protocol is
refused. A cooperative group must not be changed back to an eager protocol
without planning for duplicates: a revoke waits at most
`lifecycle.rebalanceDrainTimeout` for the one delivery in flight, and the next
owner can redeliver that record after the wait.
`broker.kafka.rebalanceTimeout` must be above that drain bound; the driver
refuses a value at or below it.

#### Kafka message-size limit

When the driver opens, it reads the broker's `message.max.bytes` and derives
the producer batch cap and the maximum body size it declares through
`Client.Limits()`. A topic-level `max.message.bytes` override below the
driver's produce batch cap is a misconfiguration: the driver's size checks
assume the broker limit, so the topic can reject records that the driver
considers valid. Set the topic limit at or above the driver's cap and verify
the effective broker configuration before publishing large events.

A subscription's effective `Prefetch` is the smaller of its configured prefetch
and the sum of its lane capacities. The runner derives each destination's
deferred hold limit from its lane capacity, which is based on subscription
concurrency, fairness weights, and `PrefetchFactor`. A delayed destination
keeps its fetches while it holds fewer records waiting for a due time than
`max(limit, 2)`. Two is the floor, because one waiting record must never be
enough to hold the fetches: a record behind it can be due sooner.

At that limit the driver holds the destination's fetches, which bounds what it
spends on records it cannot deliver yet. A record behind the held ones cannot be
read until they have been delivered, so it arrives at the earliest held
record's due time rather than at its own, and a later-due record can be
delivered before a nearer one behind it. Raising a destination's lane capacity
narrows that window; lowering it cannot narrow the window past two records.
Bounded memory is what the lateness buys.

### RabbitMQ options

| Key | What it does | Accepted values | Default | Read at |
| --- | --- | --- | --- | --- |
| `broker.rabbitmq.vhost` | Vhost the management API inspects when it reads queue arguments and bindings. It does not change the AMQP connection: the endpoint URI still selects the vhost that messages are published to. | Any string, used as the vhost name. Empty falls back to the endpoint URI. | The endpoint URI's vhost as the AMQP client parses it: `/` when the URI has no path, and `orders` for `amqp://host/orders`. | Open |
| `broker.rabbitmq.queueType` | Queue type every destination and its parking queue is declared with. `classic` also clears the delivery-count and dead-letter capabilities, both of which are quorum arguments. | `quorum` or `classic`, case-insensitive, with surrounding whitespace ignored. Any other value fails `Open`, and `env: prod` requires the exact string `quorum`. | `quorum` | Open |
| `broker.rabbitmq.consumerTimeout` | `x-consumer-timeout` declared on quorum destination queues. The broker cancels a consumer that has held one delivery this long. | Go duration of at least `1ms`, and at least three times every subscription's `handlerTimeout`. Shorter, zero, negative, or unparsable fails `Open`. | None: nothing is declared and the broker's own default stays in force. | Open |
| `broker.rabbitmq.brokerPrefetch` | Broker credit per destination. It raises how many deliveries RabbitMQ can hold ahead of the core without changing the core's in-flight budget. Extra deliveries wait in the driver's messages channel. | Integer from 1 to 65535, at least every destination's core window. A non-integer, zero, negative value, or smaller value fails `Open` or consumer creation. | Unset: each destination uses its core window. | Open |
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
Classic queues have no native delivery count, so they do not carry that risk.
With the option set, the deliveries held in the driver's channel are bounded by
the option value per destination, so a subscription with several destinations
can hold that value times the destination count. The other costs of the option
are in [Ordering and
scheduling](/advanced-topics/ordering-and-scheduling#configure-execution-capacity).

### Validation before the driver

Three keys above are also constrained before any driver parses them: when the
configuration loads, and for a subscription, also when it is created. Each
refusal names the key.

- `broker.rabbitmq.queueType` must be exactly `quorum` when `env: prod` and the
  selected driver is `rabbitmq`. The comparison is on the raw string, so a
  spelling that would fold and trim into `quorum`, such as `QUORUM` or
  ` quorum `, is refused there, and leaving the key out is refused as well.
- `broker.kafka.sessionTimeout` bounds `lifecycle.rebalanceDrainTimeout`: the
  drain timeout must be at most 0.6 times the session timeout. The check reads
  this key on the `kafka` driver only and uses the 45s default when the key is
  absent.
- `broker.rabbitmq.consumerTimeout` must be at least three times every
  subscription's `handlerTimeout`. The check reads this key on the `rabbitmq`
  driver only and uses the 90s default when the key is absent. The default
  `handlerTimeout` is 30s, which that default exactly meets, so a handler
  timeout above 30s with this key unset fails startup.

For the two duration keys, a value that does not parse as a duration is refused
by these checks too, before the driver would refuse it at `Open`. The accepted
key list and its error are implemented in
[`broker_config.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/broker_config.go).


## Provider boundary

The core generates logical F1 routing and passes physical destination names,
capability selections, and topology specifications through the `driver` port.
Each adapter owns broker syntax, client objects, physical destinations,
confirmations, offsets, management APIs, and provider-specific failure
handling. The import boundary is enforced by
`make verify-agnostic` and `.golangci.yml`.

## Enqueue time and backlog

The [observability guide](/advanced-topics/observability#enqueue-time-backlog-and-oldest-age)
defines how these sources affect broker wait and oldest-age metrics:

| Driver | Enqueue time source(s) | Backlog count | Head time | Oldest-age metric |
| --- | --- | --- | --- | --- |
| In-memory | The in-memory broker clock assigns the enqueue time during dispatch; source is `broker` | Number of queued messages | Earliest non-zero queued timestamp | Reported when the head timestamp is known |
| RabbitMQ | Trusted `timestamp_in_ms` with overwrite mode is `broker`; CloudEvents time or AMQP timestamp is `producer`; otherwise `unknown` | Same value as `Lag` | Management API head for classic queues; unknown for quorum queues | Not reported: a management head is marked `producer`, and quorum has no head |
| Kafka | `CreateTime` is `producer`; `LogAppendTime` is `broker`; absent or unknown timestamp is `unknown` | Offset lag from group commits to log ends | Earliest pending record across partitions, with its timestamp source | Reported only for a `broker` head; set `message.timestamp.type=LogAppendTime` for broker time |

## In-memory driver

The in-memory adapter is the deterministic reference implementation used by
integration tests and [`f1test`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/f1test). It supports isolated or shared
state, fake-clock delayed delivery, consumer groups, per-key affinity,
delivery counters, redelivery, topology administration, and injectable test
failures.

Start with [`drivers/inmem/inmem.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/inmem/inmem.go), then read its
producer, consumer, admin, settlement, and conformance tests. It is the best
adapter to read when diagnosing a port or core semantic before introducing
broker-specific timing or management behavior.

## RabbitMQ driver

The RabbitMQ adapter translates the port into AMQP and management operations.
The provider-specific ownership is split across these files in
[`drivers/rabbitmq`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/drivers/rabbitmq):

- `rabbitmq.go` for connection, TLS/SASL,
  and capability reporting;
- `producer.go` for publishing, confirms,
  returns, and delayed messages;
- `consumer.go` for delivery lanes,
  pause/resume, drain, and lag;
- `settlement.go` for ack/nack
  serialization;
- `topology.go` for exchanges, queues,
  bindings, and backstop routes; and
- `management.go` for inspection and
  pruning.

Use the RabbitMQ suite for queue, exchange, management, confirmation,
reconnect, TLS, and broker-specific admission behavior. Use conformance when
the behavior is part of the shared port.

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

### Delayed and retried messages need per-destination parking queues

RabbitMQ has no native delayed delivery, so the adapter emulates it with
queue-level TTLs and a dead-letter route back to the destination.

A delay of at most 64s on the ladder path is parked in the queue of the smallest rung of a fixed
ladder that is at least the delay. Fixed-delay destinations are the exception: core retry tiers
park in one queue whose TTL is the tier's delay (see Fixed-delay retry tiers in the delay-ladder
deep-dive; the per-spec set lives in `parkQueueNamesFor` in `drivers/rabbitmq/topology.go`):

| Rung | 500ms | 1s | 2s | 4s | 8s | 16s | 32s | 64s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |

That queue is named `<destination>.park.<rung>`, for example `orders.park.2s`,
and it is declared with `x-message-ttl` set to its rung. The message carries no
expiration of its own: RabbitMQ expires a per-message TTL only when the message
reaches the head of its queue, so a message parked behind a later one waits for
it and the delay can overrun by the distance between the two due times. With the
TTL on the queue, every message in it expires in FIFO order, and a rung queue
cannot construct that case at all. Rounding the delay up is what keeps a message
from being released before its due time; the cost is lateness below one rung, so
on the ladder path a 5s delay is released at about 8s. A fixed-delay tier is late
only by the time its publish took.

That cost is reported rather than left to folklore. `Client.Limits()`
renders the delay feature for this driver as `late by at most the requested delay, or
500ms, whichever is larger, for a delay of at most 1m4s; no bound above that`.
Both numbers are read from the rung table rather than written down beside it:
the floor is its first rung, where every shorter delay lands, and the ceiling is
its last, above which the per-message path below takes over. Editing a rung
changes what the report says, so the declaration cannot drift from the
mechanism.

A delay above 64s parks in `<destination>.park`, the queue that carries a
per-message expiration, exactly as it always has. The deferral ceiling stays
where it was (about 24.8 days), and the limitation of that path is unchanged: a
due time beyond the top rung can still be released late by a message parked
ahead of it. It is not reachable from the SDK's retry path, whose delays are
bounded by configuration, and it is reachable by an application passing
`DelayUntil` more than a minute out.

The names are reserved: a destination name may not end in `.park`, and it may
not end in `.park.` followed by one of the rung tags above or by
`fixed-<ms>ms`. All three shapes belong to parking queues, and the adapter reads
a queue name back to find the destination it parks for (`parkQueueParts` in
`drivers/rabbitmq/topology.go`).

The rung queues and the queue above the ladder are declared from the
destination's delay, and which policy makes them exist is the same split as any
other destination:

| Topology policy | Parking queues |
| --- | --- |
| `TopologyDeclare` | Declared by the adapter at subscription start. |
| `TopologyVerify` | Must exist and match their declared arguments, checked at subscription start. |
| `TopologyNone` | Provisioned by the operator. The adapter declares and checks nothing. |

The queues are durable, and their declare arguments follow the deployment's
queue type (`broker.rabbitmq.queueType`, `quorum` by default):

| Argument | Value |
| --- | --- |
| `x-queue-type` | `quorum`, or `classic` when the deployment selects it |
| `x-dead-letter-exchange` | `""`, the default exchange |
| `x-dead-letter-routing-key` | the destination name |
| `x-dead-letter-strategy` | `at-least-once`, on quorum only |
| `x-overflow` | `reject-publish`, on quorum only |
| `x-message-ttl` | the rung, in milliseconds, on the rung queues only |

On quorum the two strategy arguments are added because all three dead-letter
arguments are required together for RabbitMQ's at-least-once dead-letter
guarantee; the parking queues are the delay mechanism itself, not a failure
path, so a message lost there is a dropped retry with no error. A classic
deployment omits them, and its delay path is at-most-once.

Upgrading from a release without the ladder needs no drain. The existing
`<destination>.park` queue keeps dead-lettering to its destination and the
messages parked in it leave on their own schedule, and the rung queues are
declared by the topology pass. A fixed-delay destination declares its fixed
queue plus `<destination>.park`; rung queues left by an earlier version report
as orphans and drain on their own TTLs. An application that empties a destination
(`Purge`) empties every parking queue of it, and one that deletes a destination
(`Prune`) is refused while any of them still holds a message.

Under `TopologyNone` a missing parking queue is discovered by the publish that
needed it. The broker returns the message (`312 NO_ROUTE`) rather than closing
the channel, and the publish failure names the missing queue, the policy that
obliges you to create it, and its declare arguments. The producer recovers on
its own: creating the queue while the service runs is enough, with no restart.

This obligation is RabbitMQ's alone, and it is worth knowing that the same
topology policy does not mean the same thing on both drivers. Kafka carries the
deferral in a record header on the destination's own topic and the consuming
side holds the record until its due time, so a delayed or retried message needs
no topology beyond the destination topic. RabbitMQ's emulation needs the parking
queue, so `TopologyNone` obliges an operator to provision the destinations and
their parking queues here, and only the topics there.

Consumer delivery timeouts are a destination-queue setting rather than a
parking-queue one, and they follow the same policy split. Setting
`broker.rabbitmq.consumerTimeout` reaches the broker as the queue argument
`x-consumer-timeout`, on quorum destination queues only: the broker refuses the
argument on a classic queue, so a classic deployment never declares it, and when
the key is absent nothing is declared and the broker's own default stays in
force. The argument is fixed when the queue is created, which is what decides
the upgrade path for a deployment whose destination queues already exist.

A default deployment runs `TopologyVerify`. Against an existing queue the drift
detector names the argument as `x-consumer-timeout Want:90000 Got:<absent>`, and
startup logs that as a warning under `f1 topology argument drift` rather than
failing; the queue keeps the broker default. Under `topology.autoCreate`
(`TopologyDeclare`) there is no drift check at all, so the existing queue is
found and left as it was, silently. The adapter never makes an active redeclare
carrying the argument either way, because the broker refuses one with
`406 PRECONDITION_FAILED`. An existing queue therefore has to be deleted and
recreated to gain the setting, and the drift warning is the only thing that says
so.

## Kafka driver

The Kafka adapter uses consumer groups and franz-go. Its connection-derived
capabilities expose partition-bound scaling and the core-emulated paths for
delay, priority, delivery count, and dead-letter behavior where Kafka has no
native equivalent.

Provider-specific behavior includes producer confirmation, partition and
offset ownership, consumer-group rebalance, deferred records, lag queries,
TLS, and Kafka error mapping. Start with
[`drivers/kafka/kafka.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/kafka.go), then read its producer,
consumer, topology, ack-tracker, rebalance, and broker-backed tests.

Inspect [`Client.Limits()`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/limits.go)
after connecting when service behavior depends on a provider capability. Each
entry is a `FeatureStatus`
with a mode and, where declared, a detail string. A service that depends on a
feature can refuse to start without it:

```go
limits := client.Limits()
for _, feature := range limits.Features {
	if feature.Feature == "lag_metrics" && feature.Mode == f1.FeatureUnavailable {
		return fmt.Errorf("driver %s reports no backlog; backlog alerts would stay silent", limits.Driver)
	}
}
```

The report lists these rows, in this order:

| Feature | Mode | Detail |
| --- | --- | --- |
| `per_message_ack` | `native` or `emulated` | `driver.Capabilities.PerMessageAck` selects the mode. Native: the broker settles each message independently, so a slow message does not hold its lane's in-flight budget. Emulated: the core settles each message itself; settlement order is the core's, so a slow message holds its lane's in-flight budget. |
| `ordered_by_key` | `native` or `unavailable` | `driver.Capabilities.OrderedByKey` selects the mode. Native means ordering is guaranteed for equal keys. Unavailable means a subscription requesting ordered mode is rejected. |
| `priority_fairness` | `emulated` | The core's scheduler substitutes weighted lanes, so fairness is per lane and not per broker. Provider-native priority, when declared through `driver.Capabilities.NativePriority`, does not replace this core scheduling contract. |
| `native_delay` | `native` or `emulated` | `driver.Capabilities.NativeDelay` selects the mode. The connected `driver.DelayAccuracy` is rendered as the lateness bound; its zero value is reported as `delay accuracy not declared by this driver`. A driver can report an emulated mode while still declaring accuracy for its own deferral path. |
| `delivery_count` | `native` or `emulated` | `driver.Capabilities.NativeDeliveryCount` selects the mode. Native: the broker supplies a redelivery count to observer events, but handler code reads the core's one-based attempt count instead. Emulated: the core counts handler attempts in the envelope; retry copies increment that count, broker redeliveries do not, and a new publish resets it to one. |
| `dlq_backstop` | `native` or `unavailable` | `driver.Capabilities.NativeDLQ` selects the mode. Native: the broker routes an exhausted message to its dead-letter destination; the core also has a successor publish path, but this row reports only broker-native dead-letter routing. Unavailable: the core's dead-letter path publishes a successor and settles the source after publication; broker-native backstop routing is absent, while the core path remains available. |
| `lag_metrics` | `native` or `unavailable` | `driver.Capabilities.LagQueryable` selects the mode. Native means the broker exposes a backlog query; unavailable means it does not. No additional detail is emitted for this status. |
| `consumer_scaling` | `native` | `driver.Capabilities.ConsumerScaling` is rendered through `driver.Scaling.String`: `partition-bound` limits consumers by broker partitions, while `free` allows consumers to scale independently of partitions. |

The scheduler row is always emulated and the scaling row is always native
because `limitsFor`
defines those reports directly. Other modes can vary with the connected
driver's live capabilities and broker configuration. Static declarations live
in `inmem.Driver.Capabilities`,
`rabbitmq.Driver.Capabilities`,
and `kafka.Driver.Capabilities`;
the connected `driver.Conn.Capabilities`
may reduce those declarations. Read the connected report rather than
maintaining a hard-coded provider matrix.

## Choosing the right guide

- Use [Driver contract](/development/driver-contract) to implement or review
  a port interface.
- Use [Driver conformance](/development/driver-conformance) to validate
  portability across adapters and capability profiles.
- Use this page for driver configuration, provider-specific behavior, and
  broker operations.
- Use [Topology and capabilities](/advanced-topics/topology-and-capabilities)
  for service-facing capability and topology decisions.
