# F1 - Event-Driven Messaging SDK

F1 is a single Go SDK that every service in the estate uses to publish and consume events. A
service author writes handlers and event structs; F1 owns envelope construction, delivery
guarantees, acknowledgement, retry ladders, dead-letter routing, poison-message containment,
priority scheduling, zero-loss shutdown, and observability. The message broker is a pluggable
driver, swapped by configuration.

This file states what must be true of F1: the guarantees it owes a service author, and the things it
deliberately does not do. For the code tour, start with [`docs/`](docs/index.md).

## Guarantees

Each of these is testable. The default CI pipeline runs the build, unit, lint, API-surface,
API-diff, import-boundary, and self-contained gates listed in [`.gitlab-ci.yml`](.gitlab-ci.yml).
Broker-backed jobs remain available there and run when `RUN_BROKER_TESTS=1` is set; the
[Makefile](Makefile) owns the corresponding local command set.

<!-- #region guarantees -->
- **At-least-once delivery, with a stable message identity.** Transport redelivers during failures
  and rolling restarts - that is the guarantee, not a bug. F1 generates an event id, uses it as the
  idempotency key by default, and preserves that key across every retry, dead-letter and
  redelivery copy of the same message. F1 does not deduplicate handler effects and keeps no store of
  seen keys: a service reads `Event.IdempotencyKey()` and makes its own effect safe to apply twice.
- **Zero-loss graceful shutdown.** A drain protocol settles every in-flight message before the
  process exits; nothing accepted is dropped on a rolling restart or `SIGTERM`.
- **Automatic retry to a dead-letter queue.** Retryable failures move through a tiered backoff
  ladder; exhausted or terminal failures dead-letter with a recorded death reason and error. That
  holds for every entry F1 itself writes. The broker-side backstop queue is a separate destination,
  reached only when the broker's own delivery limit kills a message F1 never routed, and its entries
  carry no F1 death reason by construction - a non-zero depth there is the alert, not the norm.
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
- **Broker agnostic.** The core depends on the `driver` port, not a broker client. Concrete drivers
  are selected by the application and import-boundary linting is enforced by `make verify-agnostic`.
<!-- #endregion guarantees -->

## Non-goals for v1

<!-- #region non-goals -->
- Exactly-once *transport* (Kafka transactions), and exactly-once *effects*. At-least-once delivery
  is the guarantee.
- SDK-owned deduplication. F1 supplies a stable idempotency key; the application owns effect
  deduplication and the store that would have to remember a key.
- Transactional publish bound to a database transaction, and an outbox to carry it. F1 opens no
  database, so the window between a local commit and a publish is named rather than closed, and it
  is the application's to close.
- Global ordering across partitions/queues. Per-key ordering only.
- Cross-region replication and DR topology. Owned by the platform team.
- Request/reply over messaging. Use gRPC.
- Schema registry integration. v1 uses JSON plus a versioned envelope; the codec port has hooks for
  Avro/Protobuf, but no registry client ships in v1.
- NATS JetStream / Pulsar drivers. The port is designed to accommodate them; neither is present.
- Kafka share-group mode is not implemented. The Kafka adapter supports classic consumer groups;
  its connected capability report exposes partition-bound scaling and the core-emulated paths for
  delay, delivery count, and dead-letter behavior.
- Multiple broker versions per broker. The RabbitMQ adapter targets one stable broker family.
<!-- #endregion non-goals -->

## Install

```
go get fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk
```

```go
import f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
```

Set `GOPRIVATE=fgit.zapps.vn` before the first `go get` of this module - it lives on an internal
host, and without this the Go toolchain will try the public checksum database and proxy, which
cannot see it.

## Local documentation site

The Markdown under `docs/` is also available as a local VitePress site. Use Node.js 18 or newer,
then install the pinned dependencies and choose a command:

```sh
npm ci
npm run docs:dev       # live authoring server
npm run docs:build     # static output under docs/.vitepress/dist
npm run docs:preview   # preview the last build
```

## Quickstart with RabbitMQ

This repository includes two separate services that use the public SDK API against the local
RabbitMQ fixture. The [compose fixture](docker/docker-compose.yml) also defines Kafka for driver
tests and acceptance runs. The services share `examples/config.yaml`;
change the broker endpoint there when using a real broker. The development config sets
`topology.autoCreate: true` so the publisher can declare its entry point. Production should
provision topology separately and leave auto-creation disabled.

The fixture's RabbitMQ image includes the management plugin. The consumer's subscription path
requires that plugin's HTTP API: it reads bindings, and under startup verification the broker's
queue arguments, through it, so a broker whose management plugin is disabled, firewalled, or on a
non-default port fails at subscription start. The API defaults to the AMQP host with the AMQP port
plus 10000, which is 15672 for the fixture; set `broker.rabbitmq.managementPort` when it listens
elsewhere. `TopologyNone` is the only policy that starts a subscription without the management API,
and it requires the topology to already exist.

From a clean checkout:

```sh
make broker-up
make broker-smoke
go build -o /tmp/f1-quickstart-publisher ./examples/publisher
go build -o /tmp/f1-quickstart-consumer ./examples/consumer
```

In terminal 1, start the publisher. It declares its publish entry point and waits for the consumer:

```sh
/tmp/f1-quickstart-publisher --config examples/config.yaml --wait
```

In terminal 2, start the consumer:

```sh
/tmp/f1-quickstart-consumer --config examples/config.yaml
```

When the consumer prints `consumer ready; waiting for messages`, press Enter in terminal 1. The
publisher prints its message ID and the consumer prints `handled message: hello from publisher`.
Press Ctrl-C in terminal 2, then stop the broker:

```sh
make broker-down
```

For a fresh broker volume, use `make broker-reset` instead of `make broker-down`.

## Comment conventions

Comments should help a reader understand the code without narrating obvious statements.

- Prefer clear names, types, and structure over explanatory comments.
- Keep package and exported Go documentation immediately before the declaration. Start with the
  declared name and write a complete sentence.
- Document observable contracts: purpose, inputs, outputs, zero values, errors, panics, concurrency,
  and wire-format behavior when they are not obvious from the API.
- Use inline comments for non-obvious intent, invariants, constraints, or trade-offs. Explain why
  the code matters, not what the next line does.
- Keep comments short, local, accurate, and maintained with the code. Remove stale history,
  issue references, milestones, external design references, and commented-out code.
- Preserve compiler and tool directives such as `//go:...` and `//nolint:...`; every suppression
  must name its linter and explain its reason.
- Run `gofmt` after changing Go comments. Wrap long comments for source readability; there is no
  fixed line-length rule.

Reference guides: [Go Doc Comments](https://go.dev/doc/comment), [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments),
and the [Google Go Style Guide](https://google.github.io/styleguide/go/guide.html).

## Project documentation

Start with these local documents:

- [`docs/index.md`](docs/index.md) - documentation home: guarantees at a glance and a path for service authors, runtime readers, and driver authors.
- [`docs/learn/getting-started.md`](docs/learn/getting-started.md) - install and connect the SDK in a Go service.
- [`docs/development/architecture.md`](docs/development/architecture.md) - current package boundaries and invariants.
- This README - product guarantees, non-goals, installation, and quickstart.
