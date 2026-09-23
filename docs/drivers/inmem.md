# In-memory driver

F1's in-memory driver runs a broker inside your process, so tests and local runs need no RabbitMQ or Kafka and behave the same on every run.

For shared configuration and security, see [Drivers and capabilities](/drivers-and-capabilities).

## How the driver works

The in-memory adapter is the deterministic reference implementation used by
integration tests and [`f1test`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/f1test). It supports isolated or shared
state, delayed delivery on a fake clock, [consumer groups](/learn/glossary#consumer-group),
delivery counters, redelivery, topology administration, and injectable test
failures. Messages with the same key go to the same consumer within a group.

Two limits matter when you pick it for tests. Delays have zero lateness: a
delayed message is released in the same step that the driver's clock reaches its
due time. A consumer group that attaches from the earliest message after every
other consumer has left can replay only the newest 10,000 messages of each
destination; older ones are dropped, as a real broker with limited retention
would.

Start with [`drivers/inmem/inmem.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/inmem/inmem.go), then read its
producer, consumer, admin, ack, and conformance tests. It is the best
adapter to read when diagnosing a port or core semantic before introducing
broker-specific timing or management behavior.

## Go further

- [Drivers and capabilities](/drivers-and-capabilities) - shared configuration, security, and the feature list.
- [Driver contract](/development/driver-contract) - implement the broker-neutral port.
- [Driver conformance](/development/driver-conformance) - validate portability across adapters and capability profiles.
