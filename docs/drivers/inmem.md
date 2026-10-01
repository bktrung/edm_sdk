# In-memory driver

F1's in-memory driver runs a broker inside your process, so tests and local runs need no RabbitMQ or Kafka and behave the same on every run.

For shared configuration and security, see [Drivers and capabilities](/drivers-and-capabilities).

## How the driver works

The in-memory adapter is the deterministic reference implementation used by
integration tests and [`f1test`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/f1test). It supports isolated or shared
state, delayed delivery on a fake clock, [consumer groups](/learn/glossary#consumer-group),
delivery counters, redelivery, topology administration, and injectable test
failures. Messages with the same key go to the same consumer within a group.

Do not use the in-memory adapter to model broker delay lateness or durable
retention. Its fake-clock delays are deterministic, and its replay history is
bounded, evicting the oldest entries at the cap. The last consumer detaching
from a destination clears that destination's history, so a later earliest-start
group cannot rely on replaying messages from before it went idle.

[`inmem.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/inmem/inmem.go),
at `maxDestinationHistory`, `recordHistoryLocked`, and `replayHistoryLocked`,
owns the replay cap and selection.
[`consumer.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/inmem/consumer.go),
at `detachLocked`, owns idle-history clearing. These rules apply to replay
history; they are not a promise to purge pending deliveries when a consumer
leaves.

Start with [`drivers/inmem/inmem.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/inmem/inmem.go), then read its
producer, consumer, admin, ack, and conformance tests. It is the best
adapter to read when diagnosing a port or core semantic before introducing
broker-specific timing or management behavior.

## Go further

- [Drivers and capabilities](/drivers-and-capabilities) - shared configuration, security, and the feature list.
- [Driver contract](/development/driver-contract) - implement the broker-neutral port.
- [Driver conformance](/development/driver-conformance) - validate portability across adapters and capability profiles.
