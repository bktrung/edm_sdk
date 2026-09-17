# Every instance reads every lane: balancing Kafka partitions per topic

*By trungbk, September 2026.*

One F1 subscription, a named consumer with its own handlers, reads several Kafka topics at once. Each priority it handles gets its own topic, and so does each retry tier, the holding place for messages that will be tried again after a delay. Every running instance of the service should receive a share of each of those topics.

Kafka's usual balancers can spread the total number of partitions across instances without giving every instance a partition from every topic. That difference matters when a topic represents a priority or a retry tier. We built the lane balancer so each topic gets its own plan.

## Background

A Kafka consumer group is a set of members reading the same topics, with each partition read by one member at a time. A member is one consumer in that group, usually one running service instance. A partition is one slice of a Kafka topic that a single member reads at a time.

When members join or leave, Kafka starts a rebalance, which is the process of assigning the group's partitions again. The group leader is the member Kafka chooses to make that assignment. A balancer, also called an assignor, is the code that decides which member gets each partition. Each completed rebalance has a generation number, and that number goes up by one each time.

The Kafka driver uses franz-go, which gives the balancer members sorted by instance ID when one is set, then by member ID. We use that stable order in the examples below.

The driver maps F1's model onto this group. Each F1 destination becomes its own Kafka topic. [`subscriptionDestinations`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/worker.go) builds one destination for each priority and retry tier, and [`openRunnerConsumerWith`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/worker.go) joins them as one subscription's consumer group. With the default of three priorities, one subscription therefore joins several Kafka topics. Every running service instance is a member of that group, and the group name comes from the subscription.

The balancer calls each of those topics a lane. It gets the list of lanes from the consumer group and plans each one separately. The Kafka driver selects the implementation with the `balancer` option: `lane` is the default, and the other choices are `cooperative-sticky`, `sticky`, and `range`. The option is documented in [Driver options](/user-guide/driver-options), and the selection happens in [`resolveBalancer`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/balancer.go).

## How it works

The lane balancer promises two things when a lane has at least as many partitions as there are members. Every member gets at least one partition from that lane, and the member counts differ by at most one. The coverage assertion is in [`TestLaneBalancerCoverage`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/balancer_test.go).

That promise determines who can see the work. A lane is served only by the members that hold its partitions. If one instance holds no partition from the high-priority lane, its handlers never see high-priority work, even if that instance is idle.

A balancer that aims to even out each member's total number of partitions across all topics, such as the sticky kind, has no rule that each member holds part of every topic. A range balancer does split each topic on its own, yet it gives every topic's leftover partitions to the first members in order.

With four lanes of four partitions and three members, range gives each lane's two leftover partitions to the first member, so that member ends with eight partitions while the others end with four each. The lane plan rotates where each lane's leftover starts instead. With the lane names from [`TestLaneBalancerCoverage`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/balancer_test.go), the totals stay within one of each other. The lane names there are `lane-a`, `lane-b`, `lane-c`, and `low`, and the test asserts per-lane coverage plus cross-lane totals within one.

The starting member for a lane's remainder comes from an FNV hash of the lane name. Every member computes that same starting point from the lane name alone, so the result does not depend on which member is leader or on state kept in memory. The calculation lives in [`laneRemainderOffset`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/balancer.go).

A lane's assignment depends only on that lane. Balancing a lane alone gives the same result as balancing it alongside other lanes, with or without prior ownership claims. The same test checks both cases.

Coverage has a limit. If a lane has fewer partitions than members, some member receives none of that lane. The `maxExpectedInstances` option is the guard for this topology mistake: setup refuses a destination with fewer partitions than the configured number, and a destination declared without a partition count receives that number. Its default is `0`, which turns the check off. [`TestLaneBalancerPartitionFloor`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/balancer_test.go) and [Driver options](/user-guide/driver-options) show this boundary.

## Measured

Planning every lane from scratch on every rebalance ignores the partitions a member already holds. Its join metadata delegated to franz-go's round-robin balancer, and the comment in [`JoinGroupMetadata`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/balancer.go) describes the consequence: that path discards `currentAssignment`, along with every claim a member could make. The original implementation is visible at commit `a773eb0`.

Here is the case that exposed the cost. One lane has twelve partitions. Before the rebalance, the first member holds 0-3, the second holds 4-7, and the third holds 8-11. The third leaves. The first version splits the lane from scratch, so partitions 4 and 5 move from the second member to the first even though it stays in the group, and partitions 8-11 move from the departed member. Six partitions change owner.

A move costs something after the balancer has made its choice. The new owner resumes at the partition's committed offset. Records the old owner had acknowledged above that point are delivered again, and records the old owner was still handling are handled again. The [Kafka ack tracker](/deep-dives/kafka-ack-tracker) explains why acknowledged records can sit above the committed offset.

We changed the join metadata and the planning order in commit `ab79087`. Each member now advertises the partitions it held and the generation in which it held them. Consumer metadata version 3 carries both the owned partitions and that generation.

[`planLane`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/balancer.go) works in two steps. First, it decides how many partitions each member gets: the base share plus the rotated remainder. Second, it lets each member keep partitions it already owns, up to that count. It hands whatever remains to members in order. Stickiness therefore decides which partitions a member gets, never how many a lane gives it.

The same twelve-partition case now leaves the survivors' partitions where they are, and only the departed member's four partitions move. This is the behavior checked by [`TestLaneBalancerRebalanceMovesOnlyDepartedPartitions`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/balancer_test.go).

The count still comes first when a member joins. A new fourth member joining the twelve-partition lane takes the share down to three partitions per member. Each existing member gives up exactly one partition, the one above its new share. A member gives up a partition only when it holds more than the lane now gives it. The test for this case is [`TestLaneBalancerJoinedMemberTakesFromMembersOverTheirQuota`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/balancer_test.go).

With no membership change, nothing moves. Every member already holds what the plan would give it, so its claims fill the same targets.

## Claims we do not trust

The join metadata can contain a claim from a member that missed a rebalance. That member may rejoin advertising the partitions it held earlier, while another member is already reading those partitions. We take the highest generation any member advertises as the current one and ignore every claim from an older generation.

The stale-claim test uses a four-partition lane. At generation 5, the second member claims partitions 0 and 1. The first also claims 0 and 1, yet its claim comes from generation 3. The second keeps 0 and 1, and the first gets neither. [`laneCurrentGeneration`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/balancer.go) finds the current generation, while [`laneOwnedPartitions`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/balancer.go) ignores claims from older ones. [`TestLaneBalancerIgnoresStaleGenerationClaim`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/balancer_test.go) pins the result.

We also place each partition once. Two members can claim the same partition at the same generation after a rebalance fails between its join and sync steps. The member processed later treats an already-placed partition like any other partition it cannot keep. The `placed` check in [`planLane`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/balancer.go) makes that rule explicit.

## Limits and trade-offs

- Coverage needs at least as many partitions in each lane as there are members. A smaller lane leaves some member without that lane.
- The lane balancer reports itself as non-cooperative, so the group uses Kafka's original eager protocol. At every rebalance, each member gives up all its partitions and then receives its new assignment. Stickiness usually gives it the same partitions back.
- A moved partition can redeliver records that the old owner acknowledged above the committed offset.
- Stickiness never overrides the counts. A member can lose a partition it would prefer to keep when the lane gives it a smaller share.
- Static membership is controlled by the `staticMembership` option, which defaults to `true`. When an instance ID is configured, the driver adds it to the join request, so a member that restarts inside its session timeout can rejoin without a rebalance. With no instance ID configured, the option changes nothing. See [Driver options](/user-guide/driver-options).

## Read the code

- [`Balance`, `planLane`, `laneOwnedPartitions`, `laneCurrentGeneration`, `laneRemainderOffset`, and `JoinGroupMetadata`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/balancer.go) - the lane plan, ownership claims, generation filter, remainder rotation, and join metadata.
- [`balancer_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/balancer_test.go) - coverage, rebalance, join, stale-claim, and partition-floor scenarios.
- [Driver options](/user-guide/driver-options) - the `balancer`, `staticMembership`, and `maxExpectedInstances` settings.
