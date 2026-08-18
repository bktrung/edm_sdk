# Dispatch and scheduling

The runtime separates admission, lane selection, and handler execution.

```mermaid
flowchart LR
    D[driver.Messages] --> F[fetchRunner]
    F --> Q[dispatch channel\ncapacity = concurrency]
    Q --> S[Scheduler\nbounded lanes]
    S --> P[Dispatch pool]
    P --> H[handler + settlement]
```

## Scheduler

Implementation: [`internal/sched`](../internal/sched)

Each configured topic, priority, and retry tier becomes a lane. A lane has:

- an ID
- a bounded capacity
- a weight
- an optional aging budget
- an optional retry group

The scheduler uses deficit weighted round-robin. Retry lanes normally receive reduced weight so a
retry storm cannot consume all handler capacity. Aging promotes a lane whose oldest item exceeded
its budget.

```mermaid
flowchart TD
    N[Next] --> AGING{Aging enabled?}
    AGING -->|urgent lane| PROMOTE[Pop largest budget overrun]
    AGING -->|none| DWRR[Deficit weighted round-robin]
    DWRR --> GROUP[Select group by weight]
    GROUP --> LANE[Select lane within group]
    LANE --> ITEM[Return one item]
    PROMOTE --> ITEM
```

## Dispatch pool

Implementation: [`internal/dispatch/pool.go`](../internal/dispatch/pool.go)

```mermaid
flowchart LR
    W[Work item] --> MODE{OrderedByKey?}
    MODE -->|no| SHARED[Shared queue]
    MODE -->|yes| HASH["FNV(key) % workers"]
    HASH --> Q1[Worker queue]
    SHARED --> Q2[Worker queue]
    Q1 --> G[Worker goroutine]
    Q2 --> G
    G --> RUN[Run handler and complete settlement]
```

Ordered mode hashes the message key to one worker. Therefore equal keys cannot overlap. Different
keys may execute concurrently when they map to different workers.

## Backpressure

Backpressure exists at multiple points:

1. driver prefetch limits broker deliveries
2. fetch-to-dispatch channel is bounded
3. scheduler lanes are bounded
4. dispatch pool queues are bounded
5. handler concurrency is capped

When the scheduler cannot accept a delivery, `runDispatchPipeline` waits for a free worker rather
than growing memory without limit.

## Relevant tests

- [`internal/sched/scheduler_test.go`](../internal/sched/scheduler_test.go) — lane selection and aging.
- [`internal/dispatch/pool_test.go`](../internal/dispatch/pool_test.go) — pool behavior.
- [`f1test/ordered_test.go`](../f1test/ordered_test.go) — equal-key serialization and different-key concurrency.
- [`retry_storm_test.go`](../retry_storm_test.go) — retry pressure and fairness behavior.
