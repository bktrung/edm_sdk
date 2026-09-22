# API compatibility

## Public API

The public compatibility surface consists of the exported API in these packages:

- `f1`
- `driver`
- `codec`
- `f1test`
- `f1otel`

The driver-specific configuration keys are also public compatibility surface:

- `drivers/inmem`: no driver-specific keys.
- `drivers/kafka`: `broker.kafka.compression`, `broker.kafka.batchLinger`, `broker.kafka.fetchMaxBytes`, `broker.kafka.sessionTimeout`, `broker.kafka.rebalanceTimeout`, `broker.kafka.staticMembership`, `broker.kafka.balancer`, and `broker.kafka.maxExpectedInstances`.
- `drivers/rabbitmq`: `broker.rabbitmq.vhost`, `broker.rabbitmq.queueType`, `broker.rabbitmq.consumerTimeout`, `broker.rabbitmq.managementPort`, and `broker.rabbitmq.trustBrokerTimestamp`.

Everything under `internal/` is private implementation detail and is not covered by this promise.

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

The committed public API fixtures record the exported symbols that must remain intentional. The
`check-api-surface` and `check-api-diff` Makefile targets enforce those fixtures and the recorded API
diff policy. See [API and repository boundary checks](./testing.md#api-and-repository-boundary-checks) for
the commands and their failure rules.
