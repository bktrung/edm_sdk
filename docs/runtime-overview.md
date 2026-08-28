# Runtime overview

F1 separates application-facing behavior from broker mechanics.

```mermaid
flowchart TB
    APP[Application service]
    API[package f1\nClient · Publisher · Subscription · Runner · Event]
    CORE[Core runtime\nconfig · envelope · worker · retry · dispatch · lifecycle]
    PORT[driver port\ninterfaces · errors · capabilities · topology]
    INMEM[drivers/inmem]
    RMQ[drivers/rabbitmq]
    KAFKA[drivers/kafka\nscaffold]
    BROKER[(RabbitMQ or Kafka)]

    APP --> API --> CORE --> PORT
    PORT --> INMEM
    PORT --> RMQ --> BROKER
    PORT --> KAFKA
    KAFKA -. Open unsupported .-> BROKER
    INMEM --> MEM[(In-memory broker state)]
```

## Object ownership

| Object | Owns |
| --- | --- |
| `Client` | one live connection, shared producer, runners, lifecycle admission |
| `Publisher` | application publish calls; delegates state to `Client` |
| `Subscription` | handler-facing policy and callbacks |
| `Runner` | one subscription’s consumer, fetcher, scheduler, workers, and drain |
| `driver.Conn` | live broker resources |
| `driver.Producer` | durable publish acknowledgement |
| `driver.Consumer` | broker delivery and transport lifecycle |
| `driver.Settler` | broker-specific ack/nack operation for one delivery |

## Publish path

```mermaid
sequenceDiagram
    participant App
    participant P as f1.Publisher
    participant C as codec.Codec
    participant E as Envelope
    participant D as driver.Producer
    participant B as Broker

    App->>P: Publish(eventType, payload, options)
    P->>C: Encode(payload)
    P->>E: Build ID, routing, metadata
    E-->>P: Canonical headers
    P->>D: Publish(OutboundMessage)
    D->>B: Broker write + durable confirmation
    B-->>D: Confirm or failure
    D-->>P: Result
    P-->>App: Event ID or error
```

Details: [publish flow](publish-flow.md).

## Consume path

```mermaid
sequenceDiagram
    participant B as Broker
    participant D as driver.Consumer
    participant R as Runner
    participant S as Scheduler
    participant W as Dispatch pool
    participant H as Handler
    participant P as Shared producer

    B->>D: Delivery
    D->>R: InboundMessage + Settler
    R->>S: Enqueue bounded lane
    S->>W: Select next delivery
    W->>H: Decode Event and invoke handler
    alt success or Drop
        W->>D: Ack original
    else retryable failure
        W->>P: Publish retry successor durably
        W->>D: Ack original
    else terminal failure, panic, decode, expiry, or unmatched DLQ policy
        W->>P: Publish DLQ successor durably
        W->>D: Ack original
    end
```

Details: [consume flow](consume-flow.md), [settlement and shutdown](settlement-and-shutdown.md).

## Non-negotiable boundaries

- Core code does not import broker clients or concrete drivers.
- `driver` imports only the standard library.
- A driver receives physical destinations; it does not construct F1 names.
- A successor retry/DLQ copy is confirmed before the original is acknowledged.
- At-least-once delivery means duplicate handler effects are possible.
- The application owns idempotency using `Event.IdempotencyKey()`.

The import rules are enforced by [`make verify-agnostic`](../Makefile) and `.golangci.yml`.
