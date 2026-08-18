# Consume flow

Entry points:

- [`Client.Subscribe`](../subscription.go)
- [`Runner.Run`](../worker.go)
- [`fetchRunner`](../worker.go)
- [`runDispatchPipeline`](../worker.go)
- [`dispatchMessage`](../worker.go)
- [`invokeHandler`](../worker.go)

```mermaid
flowchart TB
    SUB[Subscribe] --> RES[Resolve config precedence]
    RES --> VAL[Validate subscription]
    VAL --> RUNNER[Create Runner]
    RUNNER --> RUN[Runner.Run]
    RUN --> TOPO[Ensure subscription topology]
    TOPO --> CONSUMER[Create driver.Consumer]
    CONSUMER --> FETCH[Fetcher reads Messages]
    FETCH --> QUEUE[Bounded dispatch channel]
    QUEUE --> SCHED[Scheduler selects lane]
    SCHED --> POOL[Dispatch pool]
    POOL --> PROC[processDelivery]
    PROC --> DEC[Decode envelope]
    DEC --> DECISION{Delivery decision}
    DECISION --> HANDLER[Match and invoke handler]
    DECISION --> DLQ[Dead-letter malformed/invalid event]
    HANDLER --> RESULT{Handler result}
    RESULT --> ACK[Ack]
    RESULT --> RETRY[Publish retry, then ack]
    RESULT --> DLQ2[Publish DLQ, then ack]
    RESULT --> REQUEUE[Nack with requeue]
```

## Runner startup

`Subscribe` only validates and registers a runner. `Run` performs the runtime work:

1. Create lifecycle, settlement, metrics, and cancellation state.
2. Build destination names from topics, priorities, retry tiers, and capability profile.
3. Ensure or verify topology according to client policy.
4. Create one driver consumer with a per-destination prefetch allocation.
5. Start fetch, dispatch, and driver-error goroutines.

## File-level worker map

| Function | Responsibility |
| --- | --- |
| `Run` | Own runner startup, goroutines, and final drain |
| `openRunnerConsumer` | Resolve destinations, topology, and consumer config |
| `fetchRunner` | Read driver messages and register accepted deliveries |
| `fetchRunnerAfterCancel` | Stop intake, drain the consumer, and forward already-delivered messages |
| `runDispatchPipeline` | Feed the scheduler and dispatch pool |
| `processDelivery` | Guarantee final settlement/accounting even after panic or abandonment |
| `dispatchMessage` | Decode, validate, match handler, classify outcome |
| `invokeHandler` | Apply timeout, panic recovery, and stuck-worker detection |
| `retryAndSettle` | Build and durably publish the next retry copy, then ack |
| `deadLetterAndSettle` | Build and durably publish the DLQ copy, then ack |
| `ackDelivery` / `nackDelivery` | Call the driver settler and update state |
| destination helpers | Generate main, retry, DLQ, and backstop names |

## Handler decision tree

```mermaid
flowchart TD
    A[InboundMessage] --> B{Headers decode?}
    B -->|no| DLQ[DLQ: decode]
    B -->|yes| C{Retry counter sane?}
    C -->|no| POISON[DLQ: poison]
    C -->|yes| D{Body size and expiry valid?}
    D -->|no| DLQ2[DLQ: decode or expired]
    D -->|yes| E{Handler matches?}
    E -->|no + ignore| DISCARD[Notify discarded + ack]
    E -->|no + deadletter| DLQ3[DLQ: unmatched]
    E -->|yes| H[Invoke handler]
    H --> I{Result}
    I -->|nil| OK[Ack]
    I -->|Drop| DROP[Notify discarded + ack]
    I -->|Terminal or panic| T[Publish DLQ + ack]
    I -->|retryable| R{Attempts remain?}
    R -->|no| MAX[Publish DLQ + ack]
    R -->|yes| RETRY[Publish retry + ack]
```

If successor publishing or acknowledgement fails, the original remains eligible for broker
redelivery. The `processDelivery` defer block retries the relevant settlement operation and records
unknown or abandoned outcomes when success cannot be proven.
