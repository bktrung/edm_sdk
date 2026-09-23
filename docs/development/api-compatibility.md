# API compatibility

## Public API

F1's compatibility promise covers the exported API in these packages:

- `f1`
- `driver`
- `codec`
- `f1test`
- `f1otel`

The driver-specific configuration keys are also public compatibility surface:

- `drivers/inmem`: no driver-specific keys.
- `drivers/kafka`: `broker.kafka.compression`, `broker.kafka.batchLinger`, `broker.kafka.fetchMaxBytes`, `broker.kafka.fetchMaxWait`, `broker.kafka.sessionTimeout`, `broker.kafka.rebalanceTimeout`, `broker.kafka.staticMembership`, `broker.kafka.balancer`, and `broker.kafka.maxExpectedInstances`.
- `drivers/rabbitmq`: `broker.rabbitmq.vhost`, `broker.rabbitmq.queueType`, `broker.rabbitmq.consumerTimeout`, `broker.rabbitmq.brokerPrefetch`, `broker.rabbitmq.managementPort`, and `broker.rabbitmq.trustBrokerTimestamp`.

Everything under `internal/` is private implementation detail and is not covered by this promise.

`driver/conformance`, the driver contract suite, is experimental. Its API may change in any release,
including a patch release, and it is not covered by this promise.

## v0.x promise

A minor release in v0.x may break the public API. Every breaking change is named in the root
`CHANGELOG.md`.

A patch release never breaks the public API.

The observer API, including `f1.Observer`, its event types, and `f1otel`, is draft API. It may change
within v0.x without the stability expected of the rest of the public surface.

When tracing is enabled and the operation span is sampled, `f1otel` duration metrics carry exemplars
that link each measurement to the operation span that recorded it. Point events emitted through
`Record` carry no exemplar because the observer port supplies them no context; this is a deliberate
draft-API decision.

## Enforcement

The `check-api-surface` target and the per-package
`check-api-surface-codec`, `check-api-surface-driver`,
`check-api-surface-f1test`, and `check-api-surface-f1otel` targets compare
exported symbols with committed surface fixtures. The `check-api-diff` target
compares `f1`, `driver`, `codec`, and `f1otel` with recorded API-diff baselines;
`f1test` is not included. See [API and repository boundary checks](./testing.md#api-and-repository-boundary-checks) for
the commands and their failure rules.

## Go further

- [Testing strategy](/development/testing) - repository gates and their failure rules;
- [Observer](/basics/observer) - the draft observer contract and event model.
