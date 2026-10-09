# Kafka driver

The Kafka driver is for services that need partitioned logs, consumer groups, and offsets owned by Kafka itself.

For shared configuration and security, see [Drivers and capabilities](/drivers-and-capabilities).

## How the driver works

The Kafka adapter uses consumer groups and franz-go. Its feature list shows that [parallelism is limited by partition count](/learn/glossary#partition-bound-scaling), and that
[F1 does](/learn/glossary#emulated) delay, priority, delivery count, and dead-letter handling itself where Kafka has no
built-in equivalent.

Start with the [Kafka adapter](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/kafka.go)
for connection policy and the [source-reading guide](/development/source-reading-guide)
for the producer, consumer, topology, and observation boundaries.

The shared [feature list](/drivers-and-capabilities#capability-report)
shows the live feature modes and limits after a client connects.

A Kafka endpoint is `host:port`. `Open` refuses an endpoint that contains `://`
or `@`, and credentials belong in `broker.sasl.username` and
`broker.sasl.password` rather than in the endpoint.

F1's Kafka topics must be written only by F1 publishers. A record from another
service carries no F1 envelope, so F1 fails to decode it and sends it down the
dead-letter path instead of to a handler.

Missing offsets are not unfinished deliveries. F1 can advance past records Kafka
never delivered, but it must not pass an earlier record still being handled.
The [ack safety rationale](/deep-dives/kafka-ack-tracker) explains that distinction.

F1 itself never uses Kafka transactions and offers at-least-once delivery, not
exactly-once.

## Kafka options

| Key | What it does | Accepted values | Default | Read at |
| --- | --- | --- | --- | --- |
| `broker.kafka.compression` | Codec the producer compresses record batches with. A named codec stands alone: the producer does not fall back to another codec. | `none`, `gzip`, `snappy`, `lz4`, or `zstd`, case-insensitive. Any other value fails `Open`. | `snappy`, falling back to no compression for a batch snappy does not shrink. | Open |
| `broker.kafka.batchLinger` | How long the producer waits for a batch to fill before sending it. `0` sends as soon as there is something to send. | Go duration, zero or positive. A negative duration fails `Open`. | `10ms` | Open |
| `broker.kafka.fetchMaxBytes` | Byte ceiling a consumer asks each broker for in one fetch. | Integer from 1 to 2147483647. Any other value fails `Open`. | `52428800` (50 MiB) | Open |
| `broker.kafka.fetchMaxWait` | Maximum time Kafka holds an empty fetch; bounding it prevents an exhausted destination from blocking a sibling on the same broker beyond this value. | Whole-millisecond Go duration from `1ms` to `596h31m23.647s`. Any other value fails `Open`. | `50ms` | Open, and per consumer |
| `broker.kafka.sessionTimeout` | Session timeout the member carries in its join request; the group coordinator expires the member after that much silence. | Positive Go duration, and at least as long as `lifecycle.rebalanceDrainTimeout` divided by 0.6. It must also fall within the broker's `group.min.session.timeout.ms` and `group.max.session.timeout.ms`. | `45s` | Open |
| `broker.kafka.rebalanceTimeout` | Window the coordinator allows a member to complete a rebalance before removing it from the group. | Positive Go duration, and at least as long as `lifecycle.rebalanceDrainTimeout` divided by 0.6. | `60s` | Open |
| `broker.kafka.staticMembership` | Adds the configured instance ID to the join request, so a member that restarts inside its session timeout rejoins without a rebalance. It changes nothing when no instance ID is configured. | Boolean, in the spellings `1`, `t`, `T`, `TRUE`, `true`, `True` and their `0`, `f`, `F`, `FALSE`, `false`, `False` counterparts. Any other value fails `Open`. | `true` | Open, and per consumer |
| `broker.kafka.balancer` | Group balancer protocol the consumer clients join with. | `cooperative-sticky`, `sticky`, or `range`. Any other value fails `Open`. | `cooperative-sticky` | Open |
| `broker.kafka.maxExpectedInstances` | Partition floor: a destination with fewer partitions than this is refused, and a declaration that names no partition count gets this many instead. `0` leaves both decisions to the destination. | Non-negative integer. | `0`, no floor | Topology |

`broker.kafka.maxExpectedInstances` guards against a topic that was created with
fewer partitions than the service can use. It applies to declaration and
verification, not to `TopologyNone`, where the driver declares and checks
nothing. Under `TopologyVerify` an existing topic below the floor is a startup
failure. Under `TopologyDeclare` a request for fewer partitions than the floor
is refused before a topic is created. The floor applies to retry destinations
as well as main and priority destinations; it is not a guarantee that each
member receives enough partitions to fill its delivery budget.

### Kafka parallelism and partitions

Kafka lets in at most one delivery from each partition owned by a consumer. A
subscription's effective handler parallelism is therefore the smaller of its
`Concurrency` and the number of partitions each member holds for a destination.
Fairness weights can divide the available slots, but a weight above the assigned
partition count cannot create another delivery.

For example, two service instances with `Concurrency: 16` sharing four
partitions equally have only two partitions each. Sixteen worker slots do not
turn those two partitions into sixteen independent deliveries.

For assignment and rebalance details, see [Kafka partition assignment and rebalancing](/deep-dives/kafka-lane-balancer).

Partition count is a one-way capacity decision: increasing it can remap keys,
and it cannot later be lowered. Size retry and priority topics as well as the
main topic; more main-topic partitions do not supply retry capacity.

A partition-shortfall warning compares the assigned count with the resolved
destination budget. A partial cooperative assignment can warn before later
rounds fill that budget; retry destinations with a positive delay are excluded.
Check the completed assignment before expanding topics. The
[rebalance rationale](/deep-dives/kafka-lane-balancer#a-warning-can-precede-the-final-assignment)
explains the diagnostic trade-off.

### Kafka retry timing

The retry step's configured delay is added to the record timestamp by the
consumer, not enforced as a broker topic policy. The [eligibility source](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/deferral.go)
owns the timestamp precision adjustment and delay check.

The record timestamp is the producer's `CreateTime` when the topic uses the
default `message.timestamp.type=CreateTime`, and the broker append time when it
uses `message.timestamp.type=LogAppendTime`. The consumer compares that due time
with its own clock, so the guarantee is "never early by the consumer's clock".
Clock skew can make a record appear due before the intended real-time delay
has elapsed. Keep producers, brokers, and consumers synchronized for the
timestamp policy in use.

F1 promises no bound on lateness. Changing a step's delay also affects records
already stored in that retry topic; use the [backlog trade-off](/deep-dives/kafka-retry-delays#limits-and-trade-offs)
when planning a delay change.

For how the consumer holds a partition until its head record is due, see
[Kafka retry delays](/deep-dives/kafka-retry-delays).

### Kafka rebalancing

Cooperative-sticky is the default; `sticky` and `range` use eager revocation.
Every group member must advertise a compatible strategy. A protocol change
needs a deployment plan for ownership transfer and possible duplicate work.

`lifecycle.rebalanceDrainTimeout` must not exceed 0.6 times the smaller of
`broker.kafka.sessionTimeout` and `broker.kafka.rebalanceTimeout`. That leaves
room for the final commit and rejoin. The wait is shared by a revoke callback's
partitions, not renewed for each partition. See [rebalance ownership](/deep-dives/kafka-lane-balancer)
for revoke versus lost assignment and late-handler consequences.

### Kafka message-size limit

When Kafka opens, it reads the broker's `message.max.bytes`. That value sets the
producer batch cap and the size checks applied before publish. It is not a
size field in `Client.Limits()`, which reports feature capabilities instead.
A topic-level `max.message.bytes` override below the driver's produce batch cap
is a misconfiguration: the topic can reject records that the driver considers
valid.

Set the topic limit at or above the driver's cap and verify the effective broker
configuration before publishing large events.

### Kafka delayed records

A held retry head prevents later records on its partition from passing it,
even when their own delay has elapsed. See [Kafka retry delays](/deep-dives/kafka-retry-delays)
for why offset order wins over timestamp order.

Kafka fetch buffering is separate from the [SDK admission total and destination
windows](/advanced-topics/configuration#prefetch-resolution); poll results are not
evidence of admitted unsettled work. For buffer bounds, partition pauses, and
admission accounting, read the consumer in
[`drivers/kafka/consumer.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/consumer.go).

Memory planning must include client fetch buffers and driver read-ahead as well
as work already delivered to F1. Large records and many held partition heads
are deployment sizing inputs, not a memory bound implied by prefetch.

## Topic policy and maintenance

Topology verification is not a production topic-policy audit. Retention,
compaction, replication, `min.insync.replicas`, topic-level
`max.message.bytes`, and `message.timestamp.type` remain operator decisions.
Connection-time broker checks do not replace them. The
[topology owner](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/topology.go)
defines what startup verification covers.

Retention and compaction must fit the application's replay and retry needs.
F1's at-least-once contract is not an archive: do not expect it to recover or
dead-letter records the broker has already removed. Producer confirmation
also does not make a database effect atomic with a group offset commit.

Stop publishing and consuming before `Purge` or `Prune`. A consumer can attach
after a prune guard succeeds but before topic deletion, because the delete
request is not conditional on that guard. The
[maintenance owner](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/admin.go)
defines the refusal checks; they do not close this race with other clients.

## Lag and deployment evidence

Use lag as a trend signal, not an exact count of records or this member's
share. Offset gaps and group-wide sampling make those interpretations unsafe.
Backlog-head observation requires additional fetches and can be unknown;
unknown is not evidence of an empty backlog. The
[backlog owner](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/backlog.go)
defines the offset snapshot and head-probe boundary.

The [local fixture](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/docker/docker-compose.yml)
has one Kafka broker. Passing fixture tests does not establish multi-replica
failure behavior, physical fsync, or exactly-once effects. Use the
[testing guide](/development/testing) to select repository evidence.

Production throughput, commit latency, failure-driven duplicate frequency,
rebalance timing under load, memory use with large fetches, and effective
TLS, SASL, ACL, and topic policies require deployment-specific evidence before
they enter a capacity or failure plan. These are measurement requirements,
not findings from the local fixture.

## Kafka TLS

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

## Kafka SASL

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

## Go further

- [Drivers and capabilities](/drivers-and-capabilities) - shared configuration, security, and the feature list.
- [Kafka partition assignment and rebalancing](/deep-dives/kafka-lane-balancer) - understand partition ownership changes.
- [Kafka ack tracker](/deep-dives/kafka-ack-tracker) - understand how offsets are committed while partitions move.
- [Driver contract](/development/driver-contract) - implement the broker-neutral port.
- [Driver conformance](/development/driver-conformance) - validate portability across adapters and capability profiles.
