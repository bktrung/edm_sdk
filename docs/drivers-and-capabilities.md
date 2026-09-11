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
