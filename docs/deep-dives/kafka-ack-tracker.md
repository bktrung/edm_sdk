# One in-flight record, one committed cursor: the Kafka ack tracker

*By F1 maintainers, September 2026.*

Kafka stores one committed offset per partition. F1's Kafka driver admits one
delivery per partition at a time, so the record being settled is the next record
at that partition's cursor. The ack tracker turns that settlement into the next
Kafka offset and refuses a settlement through a tracker after its assignment is
revoked; a renewed assignment gets a fresh tracker seeded from owed records.

## Background

A Kafka topic is split into partitions. Each partition is an append-only log
with offsets 0, 1, 2, and so on. A consumer group stores one committed offset
per partition. That offset means "the next offset to read"; after a restart or
a rebalance, reading resumes there.

Kafka has no per-message acknowledgement. Committing offset N tells Kafka that
every record below N is done, so a commit always represents a contiguous prefix
of the partition log. F1 calls finishing a delivery settlement: an ack commits
it, a discard also commits it, and a requeue leaves it unsettled for redelivery.

## The cursor

The driver gives each owned partition one tracker. Its state is deliberately
small:

- `base` is the next offset not committed by the current ownership generation.
- `revoked` records that the partition left this ownership, so a late settlement
  through this tracker returns `ErrRevoked`.
- `commitMu` serializes the broker commit and any rollback for that partition.

The hold rule in the consumer admits at most one delivery from a partition until
that delivery settles. As a result, the tracker does not need an out-of-order
ack set or an acknowledgement-gap limit. The next valid settlement is the
record at `base`.

When `ackTracker.Ack` receives that offset, it advances the in-memory cursor to
`offset + 1`, then commits that next offset to Kafka. The cursor is not advanced
for an older or non-current offset; the driver reports it as already settled.
The commit point is the offset Kafka will resume from after a restart or a
rebalance.

The cursor sequence is pinned by [`TestAckTrackerInOrderAcks`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/acktracker_test.go):

| Settled offset | Cursor before | Cursor after |
| --- | ---: | ---: |
| 0 | 0 | 1 |
| 1 | 1 | 2 |
| 2 | 2 | 3 |

There is no separate higher-offset acknowledgement to hold above a slow base:
the next record on the partition cannot be admitted until the current one is
settled. A crash can still redeliver the current record when its settlement did
not reach Kafka, which is the at-least-once guarantee and why handler effects
must be idempotent.

## Commits stay ordered

`mu` protects the cursor and revocation state. `commitMu` covers the full
advance-and-commit sequence, including the broker callback. The two locks have
different jobs: ordinary state inspection does not wait on broker I/O, while two
settlements cannot send commits out of order.

Without `commitMu`, the following interleaving could regress the broker's
committed offset:

1. Goroutine A advances the cursor to 1, releases `mu`, and starts `commit(1)`.
2. Goroutine B advances it to 2, releases `mu`, and starts `commit(2)`.
3. Kafka receives `commit(2)` first and then `commit(1)`.
4. The committed offset moves backwards to 1.

With `commitMu`, B cannot start its advance until A's commit returns, so Kafka
sees 1 and then 2.

If a commit fails after the in-memory advance, `Ack` restores the old cursor
unless the partition was revoked during the broker call. The next attempt can
then settle the same record. If the commit fails after `Drop` revokes the
tracker, `Ack` returns `ErrRevoked`; if the commit succeeds after `Drop`, `Ack`
returns nil because Kafka already owns that offset, and the commit is not undone.

`Drop` takes only `mu` and does not wait for an in-flight commit. The revoke path
can therefore detach the tracker while broker I/O is outstanding. If the
partition moves to another member, a late ack or discard through the old
tracker is rejected; a requeue is completed without committing so the next
owner can redeliver it. If the partition returns to this consumer, the
settlement path uses the new tracker, whose base includes records still owed.

## Requeue means local redelivery

A handler error normally publishes a retry copy and acknowledges the original.
A requeue is reserved for work the driver hands back without creating a
successor. `settler.Nack` marks the current delivery for redelivery and keeps
its partition charge. The current consumer's next read takes the local pending
copy before the partition's other records, so it does not admit a second
independent delivery first.

`requeueLocked` prepends the record to this consumer's pending queue. The
committed cursor does not move because the record is still unsettled, and the
poll loop delivers that local copy before the partition's other pending records.
If the consumer leaves before the redelivery settles, the next owner reads from
the unchanged committed cursor and Kafka redelivers it.

The consumer uses the partition cursor and current ownership to decide what
happens to records it encounters after a requeue:

- the requeued offset is delivered from the local pending queue;
- an offset below the cursor is already committed and is skipped;
- an offset still represented by the current delivery is not duplicated; and
- an offset the tracker has not seen is admitted as the next delivery.

A nack without requeue is a discard. It uses the same cursor advance as an ack
and logs the topic, partition, and offset. Classic Kafka groups do not expose a
per-message delivery count, so F1's `CountAsFailure` option has no Kafka-native
counter to update.

## When the partition moves

A rebalance changes which member owns a partition. The revoke callback waits up
to `lifecycle.rebalanceDrainTimeout` for the one in-flight delivery. When the
partition leaves, `Drop` marks its tracker revoked and the consumer detaches it.
A late settlement or requeue from that ownership cannot settle through the
new member.

If the partition returns, the consumer creates a tracker for the new generation.
It does not reuse the revoked tracker because settlers from the old generation
still point at it. The new cursor starts at the lowest offset this consumer
still owes after its assignment and any records already in hand. Starting at a
later delivered offset would let a held record look committed and lose it.

The next owner reads from Kafka's committed cursor. If the old owner did not
commit the record before revoke, redelivery is expected. If it did commit, the
new owner starts after it. This is why cooperative rebalancing reduces moved
partitions without promising duplicate-free handoff.

## Limits and trade-offs

- One slow record pauses that partition, because the next record cannot pass its
  cursor.
- A crash or revoke before a successful commit can redeliver the current record.
- A requeue keeps the cursor unchanged and prepends a local redelivery. If
  ownership leaves before it settles, the next owner redelivers from Kafka's
  committed cursor.
- A late settlement from a revoked ownership fails with `ErrRevoked` instead of
  committing through the old assignment.
- The partition count, not an acknowledgement-gap setting, is the Kafka
  parallelism lever. Increasing it can remap keys and cannot be undone.

## Read the code

- [`ackTracker.Ack`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/acktracker.go) - cursor advance, commit ordering, and rollback.
- [`acktracker_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/acktracker_test.go) - cursor, commit, revoke, and timing behavior.
- [`settler.Ack` and `settler.Nack`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/consumer.go) - settlement, discard, and requeue paths.
- [`trackerForLocked` and `trackerBaseLocked`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/consumer.go) - generation ownership and the starting cursor.
- [`driver-options`](/user-guide/driver-options) - Kafka options and partition-bound limits.
