# Out-of-order handlers, one committed offset: the Kafka ack tracker

*By trungbk, September 2026.*

A Kafka consumer reads partition 0 of `orders.placed` while sixteen handlers work at once. Offset 7 finishes before offset 5 does. Kafka remembers one number per partition, so the driver must turn those out-of-order finishes into one offset that never skips an unfinished record and never moves backwards. That is the ack tracker's job.

## Background

A Kafka topic is split into partitions. Each partition is an append-only log with offsets 0, 1, 2, and so on. A consumer group is the set of consumers that share work, and the group stores one committed offset per partition. That offset means "the next offset to read"; after a restart, or after a rebalance that moves a partition to another consumer, reading resumes there.

Kafka has no per-message acknowledgement. Committing offset N tells Kafka that every record below N is done, so the commit represents a whole prefix of the partition log. The Kafka driver is the part of F1 that connects the SDK's consumer to Kafka.

F1 calls finishing a delivery settlement. An ack settles it as successfully handled; a nack tells the driver to either discard it or requeue it for another delivery. The driver later commits or rewinds Kafka according to that choice.

The order of settlement is independent of log order. F1 sends records to a pool of concurrent handlers, so offset 7 can finish before offset 5. Deferred records add another ordering: delivery follows due times, so a record published earlier with a later due time can be delivered after records behind it. The lowest offset the consumer holds can therefore be the last one it delivered.

## How it works

We give each partition owned by this consumer one tracker. The tracker stores five pieces of state:

- `base`: the lowest offset that is still unacked, and the next possible commit point.
- `acked`: offsets above `base` that have already been acknowledged.
- `outstanding`: how many live delivery copies for each offset are currently unsettled.
- `requeued`: offsets waiting for redelivery.
- `generation`: the assignment generation to which the tracker belongs.

The tracker also keeps a revoked tombstone and two mutexes. The tombstone is used when a partition leaves this consumer. The mutexes matter later because bookkeeping and broker I/O have different waiting rules.

When [`ackTracker.Ack`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/acktracker.go) receives an offset, it first records that offset in `acked`. It then walks `base` upward while the current base is in `acked`, removing each passed offset. If the base moved, the new base is committed. That new base is Kafka's "next offset to read", so every offset below it is done.

Here is the exact sequence pinned by [`TestAckTrackerOutOfOrderAcks`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/acktracker_test.go). For one partition, we start with base 0, track offsets 0 through 3, then ack them in the order 3, 1, 0, 2:

| Ack | acked set after | base (commit point) | gap |
| --- | --- | --- | --- |
| 3 | {3} | 0 | 3 |
| 1 | {1, 3} | 0 | 3 |
| 0 | {3} | 2 | 1 |
| 2 | {} | 4 | 0 |

Acking 3 records the work that finished, yet base stays at 0 because offset 0 is unfinished. Acking 1 also leaves the base at 0. Acking 0 lets the tracker pass offsets 0 and 1, so it commits 2. Acking 2 then closes the remaining prefix and commits 4. The gap is the highest acked offset minus base, with zero returned when there is no higher acked offset.

An ack below base is already covered by an earlier commit. An ack for an offset already in `acked` is also a duplicate. Both cases return "already settled", which the driver exposes as [`driver.ErrAlreadySettled`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/driver/errors.go).

This ordering has a crash cost. If the process dies while base is below some offsets that were already acked, the group resumes at base and Kafka delivers those records again. F1 gives at-least-once delivery, so handlers need to be idempotent. The tracker accepts that redelivery because it keeps unfinished records from being lost.

Committing each record as soon as it finished would lose messages: if record 8 finished first and the settler committed offset 9, then the process died before records 5, 6, and 7 finished, the group would resume at 9 and never deliver 5 through 7 again. That is the defect the prefix walk replaced, in commit `d4c43b1`.

## Measured

The gap belongs to a partition. It is the highest acked offset minus base. One slow record at base holds the commit back for the whole partition, while later records continue to be acknowledged above it. That same distance describes how much work can be redelivered after a crash.

The Kafka driver bounds this distance with `maxAckGap`. If any partition of a destination has a gap above that limit, the driver pauses fetching that destination until the gap shrinks. The default is 10000, and the option is documented in [Driver options](/user-guide/driver-options). The check runs when a delivery completes settlement, after a failed ack or nack, and when a partition tracker is dropped or handed over. The source for this check is [`refreshAckGapLocked`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/consumer.go).

The pause keeps the number of acked-but-uncommitted records per partition near `maxAckGap`, rather than letting it grow without limit. "Near" matters here: records already fetched before the pause can still arrive. The limit therefore controls new fetching while the tracker drains its existing gap.

## Commits must not go backwards

The tracker protects two different operations with two different locks. The bookkeeping lock, `mu`, protects the maps and `base`. `Ack` does that work under `mu`, releases it, and then calls the broker commit. The second lock, `commitMu`, covers the whole `Ack`, including the commit callback and any rollback. This keeps two commits for one partition in order without making ordinary tracking wait on the network.

Without `commitMu`, two acks could interleave like this:

1. Offsets 0 and 1 are in flight. Goroutine A acks 0, moves base to 1 under `mu`, releases `mu`, and starts the network call `commit(1)`.
2. Before that call reaches the broker, goroutine B acks 1, moves base to 2 under `mu`, releases it, and starts `commit(2)`.
3. The broker receives `commit(2)` first and then `commit(1)`.
4. The committed offset goes from 2 back to 1.

With `commitMu`, goroutine B cannot enter its ack until goroutine A's commit has returned. The broker sees 1 and then 2. The [`TestAckTrackerCommitsNeverRegress`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/acktracker_test.go) test holds the older commit open, checks that the newer callback does not start early, then verifies the observed commit points never decrease.

A broker commit can fail after the tracker has advanced its in-memory base. In that case `Ack` rolls the state back: base returns to its old value, the offsets it passed are put back into `acked`, the just-acked offset is removed, and its outstanding count is restored. A later retry of the same ack can then succeed. If that failed commit raced a revocation, `Ack` returns `ErrRevoked`. A commit that succeeds after the partition was dropped still returns nil, because the broker already holds that offset.

The first version of the tracker also made `Track`, which admits a new record, and `Drop`, which revokes a partition, take `commitMu`. That lock stays held through the broker round trip, so admitting the next record and revoking the partition queued behind network I/O. The revoke path called `Drop` while holding the consumer's assignment lock and its main lock, so a commit on the network could make consumer bookkeeping wait on that network call. That was the defect fixed in commit `70e4376`: `Track` and `Drop` now take only `mu`, while `commitMu` still serializes a commit and its rollback. [`TestAckTrackerDropDoesNotWaitOnCommit`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/acktracker_test.go) holds a commit callback open, calls `Drop` in another goroutine, and requires the drop to finish before the callback is released; [`TestAckTrackerTrackDoesNotWaitOnCommit`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/acktracker_test.go) checks the same boundary for admission.

## Requeue means rewind

A handler never nacks. When a handler returns an error, F1 publishes a retry copy and acks the original. F1 nacks with requeue only for work it hands back to the broker, such as a stuck handler it stopped waiting for, or a delivery cancelled while the dispatch channel was full. In that case [`settler.Nack`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/consumer.go) calls `Release`. If the offset is still outstanding, the tracker marks it as requeued. The consumer then finds the lowest requeued offset and moves that partition's fetch position back to it with franz-go `SetOffsets`. Requeue does not move base.

The rewind makes Kafka send every record from that offset again. That can include records already acked and records that are still with a handler. The driver asks `TrackRedelivery` what to do with each one: a requeued offset is delivered again; an offset below base, an already acked offset, or an offset still outstanding is skipped; an offset the tracker has never seen is tracked and delivered.

A nack without requeue is a discard. It follows the same `Ack` path, so the offset is committed past, and the driver logs a warning containing the topic, partition, and offset. Classic Kafka groups keep no per-message delivery count, so F1's `CountAsFailure` option is a no-op in [`settler.Nack`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/consumer.go).

## When the partition moves

A rebalance is the Kafka group event in which partition ownership changes. When a partition is revoked, its tracker becomes a tombstone: `Drop` marks it revoked, and later `Track`, `Ack`, and `Release` calls return `ErrRevoked`. The settler reports that as a fatal settlement error, and this consumer does not commit the record.

If the partition returns, the consumer creates a tracker for the new generation. It never reuses the revoked tracker because settlers from the old generation still point at it. [`trackerForLocked`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/consumer.go) compares the tracker generation with the active generation before reusing anything.

The new tracker's base is the lowest offset this consumer still owes a delivery for. That includes records already fetched and waiting to be delivered, along with reserved handoffs. It is not always the first record delivered: deferred delivery follows due times, so an earlier log record can still be held when a later record is delivered. Starting at the delivered record's offset would make the held record look settled and drop it. [`trackerBaseLocked`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/consumer.go) scans those owed records when it creates the tracker.

## Limits and trade-offs

- A crash can redeliver records that were already acked but not included in a commit; `maxAckGap` keeps that redelivery window roughly bounded.
- One slow record can hold back a whole partition and can pause fetching for its destination when the gap exceeds `maxAckGap`.
- A rewind re-fetches records from the lowest requeued offset, including records the tracker later skips because they are already settled or still outstanding.
- A late ack for a revoked partition fails with `ErrRevoked` instead of committing through the old consumer assignment.

## Read the code

- [`ackTracker.Ack`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/acktracker.go) - prefix advancement, rollback, and the two locks.
- [`acktracker_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/acktracker_test.go) - out-of-order acks, commit ordering, and drop timing.
- [`settler.Ack` and `settler.Nack`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/consumer.go) - settlement, discard, and requeue paths.
- [`emit`, `trackerBaseLocked`, `refreshAckGapLocked`, and `resetOffset`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/consumer.go) - admission, generation state, gap pausing, and rewinding.
- [Driver options](/user-guide/driver-options) - the `maxAckGap` setting.
