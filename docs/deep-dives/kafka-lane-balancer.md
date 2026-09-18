# Kafka partition assignment and rebalancing

*By F1 maintainers, September 2026.*

Kafka assigns partitions to members of a consumer group. F1 maps each logical
destination, priority, and retry tier to a Kafka topic, then joins the topics
for one subscription in one group. The assignment determines which service
instance can receive each destination's records and how much parallel work that
instance can admit.

This page describes the stock balancer protocols selected by the Kafka driver.
The former custom lane protocol is gone and is refused when configured.

## Group assignment

A Kafka consumer group is a set of members reading the same topics, with each
partition read by one member at a time. A member is one consumer in that group,
usually one running service instance. A partition is one slice of a Kafka topic
that one member reads at a time.

When members join or leave, Kafka starts a rebalance and assigns the group's
partitions again. The group leader performs the assignment using the protocol
all members advertised. The driver uses franz-go's stock assignors and does not
maintain a custom per-topic plan.

The option is `broker.kafka.balancer`:

- `cooperative-sticky` is the default. It tries to keep each member's current
  partitions and transfers only what the new membership requires.
- `sticky` also prefers to keep current ownership, but uses Kafka's eager
  protocol. Members revoke their assignments before receiving the new ones.
- `range` assigns contiguous ranges for each topic and also uses the eager
  protocol.
- `lane` is refused. It was a custom eager protocol and is not an alias for
  any stock assignor.

Every member of a group must advertise a compatible protocol. Change the value
on all members as one deployment. Moving a group from cooperative-sticky to an
eager protocol causes a full revoke and re-consumption, so records can be
delivered again during that change.

## Parallelism is partition-bound

The Kafka driver admits one delivery per partition at a time. For a destination,
the member's transport ceiling is therefore the number of that destination's
partitions assigned to it. The handler pool adds a second ceiling: effective
handler parallelism is the smaller of `Concurrency` and the assigned partition
count.

F1's priority and retry lanes still compete through the core scheduler, but a
weight cannot create a partition. If a member owns fewer partitions than the
resolved destination slot budget, the driver emits one warning with
`assigned_partitions`, `budget`, and `lever`. The `lever` is the destination
partition count by default, or `broker.kafka.maxExpectedInstances` when that
floor is configured.

The operator response is to provision more partitions or require a partition
floor with `broker.kafka.maxExpectedInstances`. Raising a partition count
re-maps keys already published to the topic, and Kafka cannot lower a topic's
partition count. Treat an increase as a one-way capacity decision, and choose a
keying scheme that remains valid after the remap.

A warning can be produced by the first callback of a cooperative rebalance
when the callback carries only part of the member's eventual assignment. The
driver records the warning once rather than retracting it if a later callback
fills the budget. The warning is therefore an operator signal about a possible
partition shortfall, not a promise that the final callback has already arrived.

## Cooperative rebalancing

Cooperative assignment lets a member retain partitions that do not need to
move. A revoke callback waits for the one delivery in flight on each revoked
partition, bounded by `lifecycle.rebalanceDrainTimeout`. If the delivery has
not settled when that bound expires, the member gives up the assignment and the
next owner may read the record from its committed offset. That is an
at-least-once redelivery, not a loss.

`broker.kafka.rebalanceTimeout` is the broker's window for completing the
rebalance. It must be greater than `lifecycle.rebalanceDrainTimeout`; the driver
refuses a value at or below that bound. The larger window is required because
the revoke callback holds the rebalance while it waits for the in-flight
settlement.

The cooperative protocol is not an eager protocol with a different name. A
mixed group that changes protocol must be rolled out consistently, and moving
back to `sticky` or `range` accepts a full revoke and possible duplicates.

## Partition floors and topology

A destination with fewer partitions than its members cannot give every member
work for that destination. `broker.kafka.maxExpectedInstances` makes this
capacity requirement explicit: topology declaration fills an unspecified count
with the floor, and topology verification refuses an existing destination
below it. Its default `0` leaves the broker's partition count in charge.

The floor does not change an existing topic. Provision the topic with the
needed count before starting consumers, and remember that increasing the count
changes key-to-partition mapping. `TopologyNone` skips this check because the
driver neither declares nor verifies the destination there.

## Retry topics and delayed work

Retry tiers are separate Kafka topics, and their records are due at the record
timestamp plus the tier delay. The driver uses the timestamp Kafka exposes: a
producer `CreateTime` by default, or broker append time when the topic uses
`message.timestamp.type=LogAppendTime`. Publisher clock skew and append lag may
make a retry late, never early. A handler's `RetryDelay` request does not become
an arbitrary Kafka due time; the configured tier delay is the value the driver
uses.

Retry topics are still subject to the same one-delivery-per-partition rule. A
retry topic with too few partitions can leave assigned handler capacity idle,
but retry traffic is not a reason to raise the partition count casually: the
same key-remapping and irreversible partition-count rules apply.

## Read the code

- [`resolveBalancer`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/balancer.go) - accepted protocols and the cooperative default.
- [`consumerClientOpts`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/consumer.go) - group options, callbacks, and the one-delivery admission path.
- [`resolveRebalanceTimeout`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/consumer.go) - the timeout refusal that protects the revoke wait.
- [`assignmentWarningsLocked`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/consumer.go) - the assigned partition and budget warning.
- [Driver options](/user-guide/driver-options) - Kafka configuration values and operator limits.
