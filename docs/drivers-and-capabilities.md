# Drivers and provider notes

This page retains provider-specific behavior and operational differences. The
broker-independent port contract is documented in
[Driver contract](/development/driver-contract), and the shared portability
checks are documented in [Driver conformance](/development/driver-conformance).

## Provider boundary

The core generates logical F1 routing and passes physical destination names,
capability selections, and topology specifications through the `driver` port.
Each adapter owns broker syntax, client objects, physical destinations,
confirmations, offsets, management APIs, and provider-specific failure
handling. The import boundary is enforced by
[`make verify-agnostic`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/Makefile) and [`.golangci.yml`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/.golangci.yml).

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
The provider-specific ownership is split across:

- [`rabbitmq.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/rabbitmq.go) for connection, TLS/SASL,
  and capability reporting;
- [`producer.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/producer.go) for publishing, confirms,
  returns, and delayed messages;
- [`consumer.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/consumer.go) for delivery lanes,
  pause/resume, drain, and lag;
- [`settlement.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/settlement.go) for ack/nack
  serialization;
- [`topology.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/topology.go) for exchanges, queues,
  bindings, and backstop routes; and
- [`management.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/management.go) for inspection and
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

A delay of at most 64s is parked in the queue of the smallest rung of a fixed
ladder that is at least the delay:

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
a 5s delay is released at about 8s.

A delay above 64s parks in `<destination>.park`, the queue that carries a
per-message expiration, exactly as it always has. The deferral ceiling stays
where it was (about 24.8 days), and the limitation of that path is unchanged: a
due time beyond the top rung can still be released late by a message parked
ahead of it. It is not reachable from the SDK's retry path, whose delays are
bounded by configuration, and it is reachable by an application passing
`DelayUntil` more than a minute out.

The names are reserved: a destination name may not end in `.park`, and it may
not end in `.park.` followed by one of the rung tags above. Both shapes belong
to parking queues, and the adapter reads a queue name back to find the
destination it parks for.

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
declared by the topology pass. An application that empties a destination
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

The Kafka adapter uses classic consumer groups and franz-go. Share-group mode
is not implemented. Its connection-derived capabilities expose partition-bound
scaling and the core-emulated paths for delay, priority, delivery count, and
dead-letter behavior where Kafka has no native equivalent.

Provider-specific behavior includes producer confirmation, partition and
offset ownership, consumer-group rebalance, deferred records, lag queries,
TLS, and Kafka error mapping. Start with
[`drivers/kafka/kafka.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/kafka.go), then read its producer,
consumer, topology, ack-tracker, rebalance, and broker-backed tests.

Inspect `Client.Limits()` after connecting when service behavior depends on a
provider capability. A capability is an optimization or physical limit, not a
license for an adapter to change F1 semantics.

## Choosing the right guide

- Use [Driver contract](/development/driver-contract) to implement or review
  a port interface.
- Use [Driver conformance](/development/driver-conformance) to validate
  portability across adapters and capability profiles.
- Use this page for provider-specific behavior and broker operations.
- Use [Topology and capabilities](/advanced-topics/topology-and-capabilities)
  for service-facing capability and topology decisions.
