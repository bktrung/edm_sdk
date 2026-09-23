# Kafka driver

The Kafka driver is for services that need partitioned logs, consumer groups, and offsets owned by Kafka itself.

For shared configuration and security, see [Drivers and capabilities](/drivers-and-capabilities).

## How the driver works

The Kafka adapter uses consumer groups and franz-go. Its feature list shows that [parallelism is limited by partition count](/learn/glossary#partition-bound-scaling), and that
[F1 does](/learn/glossary#emulated) delay, priority, delivery count, and dead-letter handling itself where Kafka has no
built-in equivalent.

Provider-specific behavior includes producer confirmation, partition and
offset ownership, consumer-group rebalance, deferred records, lag queries,
TLS, and Kafka error mapping. Start with
[`drivers/kafka/kafka.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/kafka.go), then read its producer,
consumer, topology, ack-tracker, rebalance, and broker-backed tests.

The shared [feature list](/drivers-and-capabilities#capability-report)
shows the live feature modes and limits after a client connects.

A Kafka endpoint is `host:port`. `Open` refuses an endpoint that contains `://`
or `@`, and credentials belong in `broker.sasl.username` and
`broker.sasl.password` rather than in the endpoint.

F1's Kafka topics must be written only by F1 publishers. A record from another
service carries no F1 envelope, so F1 fails to decode it and sends it down the
dead-letter path instead of to a handler.

A gap in a partition's offsets does not stall the partition. Compaction, a
transaction marker, and retention deleting records ahead of a slow consumer all
leave offsets that Kafka never delivered, and F1 commits past them: the tracker
that let a record in commits the offset after it, not only the next offset in
sequence.

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
| `broker.kafka.rebalanceTimeout` | Window the coordinator allows a member to complete a rebalance before removing it from the group. It must be greater than the configured `lifecycle.rebalanceDrainTimeout`. | Positive Go duration, and above the revoke wait bound. | `60s` | Open |
| `broker.kafka.staticMembership` | Adds the configured instance ID to the join request, so a member that restarts inside its session timeout rejoins without a rebalance. It changes nothing when no instance ID is configured. | Boolean, in the spellings `1`, `t`, `T`, `TRUE`, `true`, `True` and their `0`, `f`, `F`, `FALSE`, `false`, `False` counterparts. Any other value fails `Open`. | `true` | Open, and per consumer |
| `broker.kafka.balancer` | Group balancer protocol the consumer clients join with. | `cooperative-sticky`, `sticky`, or `range`. Any other value fails `Open`. | `cooperative-sticky` | Open |
| `broker.kafka.maxExpectedInstances` | Partition floor: a destination with fewer partitions than this is refused, and a declaration that names no partition count gets this many instead. `0` leaves both decisions to the destination. | Non-negative integer. | `0`, no floor | Topology |

`broker.kafka.maxExpectedInstances` guards against a topic that was created with
fewer partitions than the service can use. It applies to declaration and
verification, not to `TopologyNone`, where the driver declares and checks
nothing. Under `TopologyVerify` an existing topic below the floor is a startup
failure. Under `TopologyDeclare` a request for fewer partitions than the floor
is refused before a topic is created.

### Kafka parallelism and partitions

Kafka lets in at most one delivery from each partition owned by a consumer. A
subscription's effective handler parallelism is therefore the smaller of its
`Concurrency` and the number of partitions each member holds for a destination.
Fairness weights can divide the available slots, but a weight above the assigned
partition count cannot create another delivery.

For assignment and rebalance details, see [Kafka partition assignment and rebalancing](/deep-dives/kafka-lane-balancer).

When a member is assigned fewer partitions than the destination's delivery
slots, the driver emits one warning with the fields `assigned_partitions`,
`budget`, and `lever`. Increase the destination's partition count, or use
`broker.kafka.maxExpectedInstances` to require that floor during topology setup.
Raising a partition count re-maps keys already published, and the partition
count cannot be lowered, so treat that change as a one-way capacity decision.

### Kafka retry timing

Kafka retry records are due at their record timestamp plus the [retry step's](/learn/glossary#retry-tier)
delay. `RetryDelay` and other custom per-delivery delays are not honored by this
driver; the configured list of retry delays supplies the destination delay.

The record timestamp is the producer's `CreateTime` when the topic uses the
default `message.timestamp.type=CreateTime`, and the broker append time when it
uses `message.timestamp.type=LogAppendTime`. The consumer compares that due time
with its own clock, so the guarantee is "never early by the consumer's clock".
If the consumer's clock runs ahead of the producer's, a record can look due
early; keep hosts on NTP.

Kafka declares no bound on lateness: a record can wait past its due time while
its partition's fetches are paused or the broker is unreachable.

### Kafka rebalancing

The default group protocol is cooperative-sticky. A cooperative group must not
be changed back to an eager protocol without planning for duplicates: a revoke
waits at most `lifecycle.rebalanceDrainTimeout` for the one delivery in flight,
and the next owner can redeliver that record after the wait.
`broker.kafka.rebalanceTimeout` must be above that drain bound; the driver
refuses a value at or below it.

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

Kafka holds a record that is not yet due at the head of its partition and pauses
fetching that partition until the record's due time.

Records behind a held head wait on the same partition, so one can arrive after
its own due time, never before it. The driver wakes the poll loop when the held
record becomes due and then resumes fetching.

Each partition buffers a bounded number of fetched records, at least the destination's prefetch.
At that count the driver pauses that partition's fetches until the buffer drains.
This is separate from the in-flight limit: the driver also pauses a whole topic
while its records not yet acked reach the lane capacity
([how the two drivers stop the flow](/deep-dives/scheduler#why-lane-capacity-stays-small)).

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
