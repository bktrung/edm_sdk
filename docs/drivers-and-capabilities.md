# Drivers and provider notes

This page retains provider-specific behavior and operational differences. The
broker-independent port contract is documented in
[Driver contract](development/driver-contract.md), and the shared portability
checks are documented in [Driver conformance](development/driver-conformance.md).

## Provider boundary

The core generates logical F1 routing and passes physical destination names,
capability selections, and topology specifications through the `driver` port.
Each adapter owns broker syntax, client objects, physical destinations,
confirmations, offsets, management APIs, and provider-specific failure
handling. The import boundary is enforced by
[`make verify-agnostic`](../Makefile) and [`.golangci.yml`](../.golangci.yml).

## In-memory driver

The in-memory adapter is the deterministic reference implementation used by
integration tests and [`f1test`](../f1test/). It supports isolated or shared
state, fake-clock delayed delivery, consumer groups, per-key affinity,
delivery counters, redelivery, topology administration, and injectable test
failures.

Start with [`drivers/inmem/inmem.go`](../drivers/inmem/inmem.go), then read its
producer, consumer, admin, settlement, and conformance tests. It is the best
adapter to read when diagnosing a port or core semantic before introducing
broker-specific timing or management behavior.

## RabbitMQ driver

The RabbitMQ adapter translates the port into AMQP and management operations.
The provider-specific ownership is split across:

- [`rabbitmq.go`](../drivers/rabbitmq/rabbitmq.go) for connection, TLS/SASL,
  and capability reporting;
- [`producer.go`](../drivers/rabbitmq/producer.go) for publishing, confirms,
  returns, and delayed messages;
- [`consumer.go`](../drivers/rabbitmq/consumer.go) for delivery lanes,
  pause/resume, drain, and lag;
- [`settlement.go`](../drivers/rabbitmq/settlement.go) for ack/nack
  serialization;
- [`topology.go`](../drivers/rabbitmq/topology.go) for exchanges, queues,
  bindings, and backstop routes; and
- [`management.go`](../drivers/rabbitmq/management.go) for inspection and
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
An unreachable API fails subscription start with an error naming the endpoint,
rather than skipping the check; a broker with the management plugin disabled,
firewalled, or on a non-default port is the most likely cause.

The local fixture (`make broker-up`) runs a `-management` image with the API on
15672. Managed RabbitMQ offerings differ in whether that API is exposed and on
which credentials, so check it before deploying.

### Delayed and retried messages need a per-destination parking queue

RabbitMQ has no native delayed delivery, so the adapter emulates it with one
parking queue per destination. A delayed or retried message is published with a
per-message TTL to `<destination>.park`, and the broker dead-letters it back to
the destination when the TTL expires. The name is always the destination name
with `.park` appended, so a destination name ending in `.park` is reserved.

The parking queue is declared from the destination's delay, and which policy
makes it exist is the same split as any other destination:

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

On quorum the last two arguments are added because all three dead-letter
arguments are required together for RabbitMQ's at-least-once dead-letter
guarantee; the parking queue is the delay mechanism itself, not a failure path,
so a message lost there is a dropped retry with no error. A classic deployment
omits them, and its delay path is at-most-once.

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

## Kafka driver

The Kafka adapter uses classic consumer groups and franz-go. Share-group mode
is not implemented. Its connection-derived capabilities expose partition-bound
scaling and the core-emulated paths for delay, priority, delivery count, and
dead-letter behavior where Kafka has no native equivalent.

Provider-specific behavior includes producer confirmation, partition and
offset ownership, consumer-group rebalance, deferred records, lag queries,
TLS, and Kafka error mapping. Start with
[`drivers/kafka/kafka.go`](../drivers/kafka/kafka.go), then read its producer,
consumer, topology, ack-tracker, rebalance, and broker-backed tests.

Inspect `Client.Limits()` after connecting when service behavior depends on a
provider capability. A capability is an optimization or physical limit, not a
license for an adapter to change F1 semantics.

## Choosing the right guide

- Use [Driver contract](development/driver-contract.md) to implement or review
  a port interface.
- Use [Driver conformance](development/driver-conformance.md) to validate
  portability across adapters and capability profiles.
- Use this page for provider-specific behavior and broker operations.
- Use [Topology and capabilities](advanced-topics/topology-and-capabilities.md)
  for service-facing capability and topology decisions.
