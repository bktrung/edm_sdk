# F1 - Event-Driven Messaging SDK

F1 is a single Go SDK that every service in the estate uses to publish and consume events. A
service author writes handlers and event structs; F1 owns envelope construction, delivery
guarantees, acknowledgement, retry ladders, dead-letter routing, poison-message containment,
priority scheduling, zero-loss shutdown, and observability. The message broker is a pluggable
driver, swapped by configuration.

This file states what F1 must be true of. It is not a tour of the code, and it does not duplicate
the design - see [Where the design lives](#where-the-design-lives) below for that.

## Guarantees

Each of these is testable, and each is enforced by CI, not documentation:

- **At-least-once delivery, plus idempotent effects.** Transport redelivers during rolling
  restarts - that is the guarantee, not a bug. A dedupe store keyed on `idempotency_key` makes the
  *effect* apply exactly once. Measured as `f1_messages_lost_total == 0` and
  `f1_duplicate_applied_total == 0`.
- **Zero-loss graceful shutdown.** A drain protocol settles every in-flight message before the
  process exits; nothing accepted is dropped on a rolling restart or `SIGTERM`.
- **Automatic retry to a dead-letter queue.** Retryable failures move through a tiered backoff
  ladder; exhausted or terminal failures dead-letter with a recorded reason - every message in a
  DLQ has one.
- **Starvation-free priority handling.** Low-priority messages have a bounded maximum wait under
  sustained high-priority load; that bound is measured, not assumed.
- **Poison-message safety.** A message that cannot be decoded, or a handler that panics, is
  quarantined rather than retried forever or taken down with the process.
- **Ordering, where declared.** Per-key ordering is available and preserved end to end when a
  subscription asks for it. F1 does not offer or imply global ordering across partitions or queues.
- **Non-leaking abstraction.** No broker-specific concept is visible in the handler-facing API.
  Broker differences may appear in configuration; they never appear in business code.
- **Capability declaration, no silent degradation.** F1 declares its limits under the connected
  driver at runtime (`Client.Limits()`) and fails or degrades explicitly and observably when a
  requested feature is unavailable under that driver.
- **Broker agnostic.** Swapping the driver is a configuration and topology change with zero
  business-code modification, demonstrated by a driver-flip test (`make swap-report`) and enforced
  by an import-boundary lint (`make verify-agnostic`) that fails the build on a violation.

## Non-goals for v1

- Exactly-once *transport* (Kafka transactions). At-least-once transport plus idempotent effects is
  the guarantee; see ADR-0003 in the design repository.
- Global ordering across partitions/queues. Per-key ordering only.
- Cross-region replication and DR topology. Owned by the platform team.
- Request/reply over messaging. Use gRPC.
- Schema registry integration. v1 uses JSON plus a versioned envelope; the codec port has hooks for
  Avro/Protobuf, but no registry client ships in v1.
- NATS JetStream / Pulsar drivers. The port is designed to accommodate them; neither is scheduled.
- Multiple broker versions per broker. v1 targets one pinned, stable version of Kafka and one of
  RabbitMQ; see ADR-0009.

## Install

```
go get fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk
```

The module path is the real repository path, not `github.com/za/f1` (see ADR-0014). The root
package is `package f1`; since the module's last path element does not match the package name, the
house style is an explicit import alias everywhere the root package is imported:

```go
import f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
```

Set `GOPRIVATE=fgit.zapps.vn` before the first `go get` of this module - it lives on an internal
host, and without this the Go toolchain will try the public checksum database and proxy, which
cannot see it.

## Where the design lives

The requirements above are sourced from `REQUIREMENTS.md` and doc 00 in the design repository,
which is where the actual design work happens - this repository is the implementation of it, not
the other way around. Two documents are the exception and are duplicated here deliberately, because
someone reading this code should not have to leave it to orient themselves:

- [`ARCHITECTURE.md`](ARCHITECTURE.md) - package layout, data flow, the import boundaries
  `make verify-agnostic` enforces.
- This README.

Everything else - the sixteen design documents, the ADRs, the capability matrix, and the build plan
that orders the work in this repository task by task - lives in the design repository. Ask your
lead for its remote if it is not already in your git remotes.
