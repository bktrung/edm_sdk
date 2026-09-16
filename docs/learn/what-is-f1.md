# What is F1

F1 is the Go SDK every service in the estate uses to publish and consume events. A service author
writes handlers and event structs. F1 owns envelope construction, delivery guarantees,
acknowledgement, retry ladders, dead-letter routing, poison-message containment, priority
scheduling, zero-loss shutdown, and observability.

The message broker is a pluggable driver chosen by the application at its composition root.
Business code talks to F1, never to a broker client, so moving between brokers changes
configuration rather than handlers.

## Guarantees

Each guarantee is testable, and the repository gates exercise them.

<!--@include: ../../README.md#guarantees-->

## Non-goals for v1

<!--@include: ../../README.md#non-goals-->

## Current status

The repository contains the core SDK, the public codec and driver ports, the deterministic
in-memory driver, the RabbitMQ driver, and a connected Kafka driver. Kafka consumes with consumer
groups. Each driver reports its transport limits through `Client.Limits()`, and the core emulates
F1 retry, delay, and dead-letter behavior where a broker has no native equivalent.

## Where to go next

- [Getting started](/learn/getting-started) - install the module, connect a driver, publish, and
  consume.
- [Publisher and subscriber](/basics/pubsub) - the concepts behind the API.
- [Architecture](/development/architecture) - package boundaries and the invariants that hold
  them.
