# Ordering and scheduling

F1 has two separate delivery decisions:

- **Ordering** answers whether deliveries with the same message key may run at
  the same time.
- **Scheduling** answers which ready delivery receives the next available
  handler slot when topics, priorities, or retry work compete.

Do not use one setting to solve the other. `OrderedByKey` protects a per-key
business invariant; `FairnessConfig` protects service capacity across work
classes. Both operate within F1's at-least-once delivery model, so ordering
does not remove the need for idempotent effects. See [Message](/basics/message)
and [Publisher and subscriber](/basics/pubsub) for the application-facing
message and subscription model.

## Choose the ordering guarantee

The default subscription mode is `f1.Unordered`. It allows the configured
handler concurrency to process deliveries in parallel and makes no ordering
promise between deliveries.

Use `f1.OrderedByKey` when a business entity must be processed serially:

```go
runner, err := client.Subscribe(ctx, f1.Subscription{
	Name:        "order-projector",
	Topics:      []string{"orders.placed"},
	Mode:        f1.OrderedByKey,
	Concurrency: 4,
	Handlers: map[string]f1.Handler{
		"orders.placed.v1": f1.HandlerFunc(handleOrderPlaced),
	},
})
```

In ordered mode:

- deliveries with equal message keys are assigned to the same worker and do
  not overlap;
- different keys may run concurrently on different workers; and
- F1 does not provide one global order across all topics, priorities, keys, or
  consumer instances.

This guarantee ends at a retry. A delivery that fails and is retried is
acknowledged as soon as its retry copy is stored, which releases the key, so
the next message with that key is handled before the retry comes back. When a
key must not be handled out of order at all, either set
`RetryConfig{MaxAttempts: 1}` for the subscription so a failure goes straight
to the dead-letter destination, or make the handler tolerate the reorder.

The connected driver must advertise the `ordered_by_key` capability. F1
rejects an ordered subscription when that capability is unavailable. Check
`client.Limits()` during startup when deployment portability matters; the
capability report is the authority for the connected driver. The capability
contract is defined by [`driver.Capabilities`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/driver/capability.go) and
the subscription check is in [`Subscribe`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/subscription.go).

### Make the key stable

Publish the same logical key for every event whose state must be serialized:

```go
_, err := client.Publisher().Publish(
	ctx,
	"orders.placed.v1",
	OrderPlaced{OrderID: orderID},
	f1.WithKey(orderID),
)
```

`WithKey` is the explicit partition/routing key. If it is omitted, F1 derives
the transport key from the subject and then the generated event ID. That is
safe as a default, but generated IDs do not create a useful per-order
ordering relationship. `WithSubject` can provide a stable default when the
subject is the business entity. See [`WithKey`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/publisher.go) and
[Message metadata](/basics/message#metadata-and-the-envelope).

Ordering follows the message key. `WithKey` sets it explicitly; `WithSubject`
supplies it when no explicit key is present; and when neither is provided F1
falls back to the generated event ID. Ordering does not follow the event type
or idempotency key unless the application deliberately uses that value as a
key.

### Understand the trade-off

Ordered mode preserves serial execution for a key across the handler and its
settlement path, but a single hot key remains a single serial bottleneck. If
all messages use one key, increasing `Concurrency` cannot make that key run in
parallel. If the service needs more throughput, use a key that matches the
business serialization boundary rather than enabling global ordering.

An ordered subscription also needs enough distinct keys to use its workers.
Measure the active key distribution before increasing concurrency; more worker
slots do not help when most work hashes to one key.

## Configure execution capacity

`Concurrency` and `Prefetch` control different parts of the pipeline:

| Setting | Controls | Main trade-off |
| --- | --- | --- |
| `Concurrency` | Number of handler workers available to a subscription | More parallelism requires thread-safe, idempotent handler effects and more downstream capacity. |
| `Prefetch` | How many deliveries the consumer may hold ahead of settlement; in ordered mode it is also the dispatch queue budget. A partition-bound driver admits one delivery per partition whatever this is set to, so there the ceiling is the number of partitions assigned to the consumer | More buffering can improve utilization but increases in-flight work, memory, and shutdown backlog. Raising it above the partitions assigned to the consumer does not raise the ceiling, because each of those partitions is already carrying one delivery. |

F1 keeps admission and scheduling bounded. A delivery passes through the
driver's prefetch budget, the fetch-to-dispatch boundary, bounded scheduler
lanes, and the dispatch pool before the handler runs. When a downstream stage
is full, the pipeline waits for capacity instead of growing an unbounded
in-memory queue. The executable pipeline is owned by
[`runDispatchPipeline`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/worker.go); the internal rationale is in
[Consume flow](/development/consume-flow).

Start with `Concurrency` matched to the safe parallelism of the handler and
its dependencies. Set `Prefetch` high enough to keep those workers supplied,
but not so high that a slow dependency creates an unnecessarily large
in-flight backlog. Tune one setting at a time while observing handler
latency, downstream saturation, redelivery, and drain time.

A partition-bound driver, which is Kafka, admits one delivery per partition
owned by a member. The effective handler parallelism for a destination is the
smaller of `Concurrency` and that member's assigned partition count. Priority
weights above the assigned partition count do nothing, and `Prefetch` cannot
raise the ceiling.

When the assigned count is below the destination's resolved slot budget, Kafka
logs one warning with `assigned_partitions`, `budget`, and `lever`. Increase
the destination's partition count, or set `broker.kafka.maxExpectedInstances`
to require a floor during topology setup. Raising the partition count re-maps
keys already published to the destination, and Kafka cannot lower the count, so
the change is a one-way capacity decision. See [Consuming events](/user-guide/consuming-events)
for the operator-facing sizing rule.

`Prefetch` is not a substitute for capacity planning. A larger value cannot
make a hot ordered key concurrent, and it cannot make a handler that is
blocked on a dependency complete faster. It also cannot raise a
partition-bound driver's ceiling: that driver admits one delivery per
partition, so on Kafka the effective ceiling is the number of partitions
assigned to the consumer, and raising `Prefetch` past that does nothing.

## Priorities are fair scheduling lanes

`Priority` selects a delivery lane. The values `high`, `medium`, and `low` are
lane identifiers; their numeric values do not define a strict execution order.
Publish priority explicitly when it is part of the event's service policy:

```go
_, err := client.Publisher().Publish(
	ctx,
	"orders.placed.v1",
	OrderPlaced{OrderID: orderID},
	f1.WithKey(orderID),
	f1.WithPriority(f1.PriorityHigh),
)
```

A subscription chooses which priority lanes it consumes with
`Subscription.Priorities`. The scheduler then applies the subscription's
`FairnessConfig` across its configured topics, priorities, and retry tiers.
Weights express relative opportunity, not a promise that one lane always wins.
This is intentional: strict priority could starve medium work during a
sustained high-priority load.

The default fairness policy is defined by `defaultSubscription` in
[`config.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/config.go). Override it when the service has a measured
capacity policy rather than copying defaults into application code:

```go
	fairness := f1.FairnessConfig{
		Weights: map[f1.Priority]int{
			f1.PriorityHigh:   8,
			f1.PriorityMedium: 4,
			f1.PriorityLow:    1,
		},
		Budgets: map[f1.Priority]time.Duration{
			f1.PriorityHigh:   5 * time.Second,
			f1.PriorityMedium: 30 * time.Second,
			f1.PriorityLow:    2 * time.Minute,
	},
	RetryWeightDivisor: 2,
	PrefetchFactor:     2,
}
```

The fields have distinct jobs:

- `Weights` controls the relative share of ready lanes;
- `Budgets` defines how long a lane may wait before deadline promotion can promote it;
- `RetryWeightDivisor` reduces retry pressure relative to fresh work;
- `PrefetchFactor` scales each scheduler lane's bounded capacity; and
- `DisableDeadlinePromotion` turns off deadline promotion for lanes that exceed their
  budget. Its zero value leaves promotion on, which is the default.

The scheduler uses weighted round-robin when no lane has exceeded its
budget. When deadline promotion is enabled, the lane with the greatest budget overrun may
be selected first. This gives latency-sensitive work a way to recover from
temporary contention without turning the whole policy into strict priority.
The implementation is [`internal/sched`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/internal/sched), and its lane
construction is [`newRunnerScheduler`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/worker.go).

## Keep retry work from taking all capacity

Retry timing and ready-work scheduling are separate:

1. `RetryConfig` decides when a failed event becomes eligible for another
   attempt.
2. `FairnessConfig` decides how that ready retry competes with fresh work and
   other retry tiers.

F1 gives retry lanes their own scheduler groups and normally reduces their
weight with `RetryWeightDivisor`. This prevents a retry storm from consuming
all handler capacity and starving new events. Deadline promotion can still promote a retry
lane that has waited beyond its configured budget.

Returning `f1.RetryAfter` changes the event's next eligible time; it does not
give that retry priority over fresh work once it is ready. Configure retry
classification and delay in [Failure handling](/advanced-topics/failure-handling), then use
fairness settings for the service-level capacity decision.

## A practical tuning sequence

When a subscription is slow or unfair, identify which guarantee is actually
needed before changing configuration:

1. **Need serial state transitions for one entity?** Use `OrderedByKey` and a
   stable `WithKey`. Do not use `Concurrency: 1` unless the whole subscription
   truly needs global serialization.
2. **Handlers are idle while work is available?** Check `Prefetch`, lane
   capacity, the number of distinct keys, and the partitions assigned to the
   consumer before increasing concurrency. On a partition-bound driver a
   `Prefetch` above that partition count is not what is holding the workers
   back.
3. **Fresh work is delayed by retries?** Keep retry lanes available but lower
   their relative weight with `RetryWeightDivisor`; do not discard retries that
   represent a real transient failure.
4. **A lane waits too long?** Enable deadline promotion or adjust its budget after checking
   the weight and downstream capacity. A budget is a scheduling escape hatch,
   not a latency guarantee.
5. **A high-priority lane dominates?** Reduce its weight or split the workload
   into subscriptions with independent capacity. Do not assume the priority
   label itself is strict.

Every handler must remain safe under redelivery. Increasing concurrency,
changing key distribution, or allowing retries to run later can expose races
and duplicate effects that were already possible under at-least-once delivery.

## Test ordering and fairness

Use [`f1test`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/f1test/f1test.go) for deterministic service-policy tests:

- configure `Mode: f1.OrderedByKey` and verify equal keys never overlap;
- publish different keys and verify the subscription can use its configured
  concurrency;
- publish explicit priorities and verify the selected lanes receive service;
- fill fresh and retry lanes and verify retry pressure does not starve fresh
  work; and
- advance the fake clock when testing retry eligibility or deadline promotion instead of
  sleeping in the test.

The repository's [`ordered_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/f1test/ordered_test.go) covers equal
key serialization and different-key concurrency. The scheduler unit tests in
[`internal/sched/scheduler_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/internal/sched/scheduler_test.go)
cover weighted selection, bounded lanes, and deadline promotion; the retry-storm test in
[`retry_storm_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/retry_storm_test.go) guards fresh-work share under
retry pressure. A driver implementation must also preserve the capability
contract validated by the [driver conformance package](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/driver/conformance).

## Common mistakes

- Treating `OrderedByKey` as global ordering across keys, topics, or consumer
  instances.
- Publishing without a stable business key and expecting event IDs to preserve
  entity order.
- Using `Concurrency: 1` to compensate for a missing key or an incorrect
  downstream idempotency design.
- Assuming `PriorityHigh` is strict priority and that medium or low work will
  stop while high work exists.
- Increasing `Prefetch` to solve a slow dependency, a hot ordered key, or a
  partition-bound driver's per-partition ceiling.
- Letting retry volume consume all capacity by omitting retry fairness from
  load testing.
- Assuming changing `Concurrency` is enough to scale an ordered subscription;
  the key distribution and connected driver's capability model also matter.

## Continue from here

- [Failure handling](/advanced-topics/failure-handling) - retry classification, dead letters,
  and idempotent effects;
- [Message](/basics/message) - keys, priority metadata, and delivery
  identity;
- [Publisher and subscriber](/basics/pubsub) - publish and subscription
  boundaries;
- [Driver and capabilities](/drivers-and-capabilities) - capability
  discovery and portability; and
- [Consume flow](/development/consume-flow) - implementation details for
  lanes, pool routing, and backpressure.
