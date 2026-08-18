# Publish flow

Entry points:

- [`Publisher.Publish`](../publisher.go)
- [`Publisher.PublishBatch`](../publisher.go)
- [`buildOutbound`](../publisher.go)
- [`Client.ensurePublisherTopology`](../client.go)

```mermaid
flowchart LR
    A[Publish call] --> V[Validate options]
    V --> ID[Generate event ID]
    ID --> ENC[Codec.Encode payload]
    ENC --> ENV[Build Envelope]
    ENV --> HDR[EncodeHeaders]
    HDR --> DEST[Resolve physical entry point]
    DEST --> PROD[Shared driver.Producer]
    PROD --> ACK{Durable result}
    ACK -->|success| OUT[Return event ID]
    ACK -->|partial PublishError| PART[Per-message results]
    ACK -->|transport error| ERR[Return error]
```

## What `buildOutbound` decides

| Decision | Source |
| --- | --- |
| event ID | `newEventID` |
| payload bytes | configured `codec.Codec` |
| logical topic | explicit `WithTopic`, otherwise `topicFor(eventType)` |
| partition key | explicit key, subject, then event ID |
| idempotency key | explicit key, otherwise event ID |
| correlation/causation | explicit values or `WithCausedBy` inheritance |
| broker entry point | `publishEntryPoint` and priority |
| physical headers | `Envelope.EncodeHeaders` |

The event type version suffix is used for handler compatibility while the physical topic is based
on the unversioned logical topic. The implementation of that rule is `topicFor`.

## Topology timing

```mermaid
sequenceDiagram
    participant App
    participant Client
    participant Admin as driver.Admin
    participant Broker

    App->>Client: New(... WithPublishTopics(...))
    Client->>Client: Resolve topology policy
    alt declare or verify
        Client->>Admin: EnsureTopology(publisher spec)
        Admin->>Broker: Create or inspect entry points
        Broker-->>Admin: Topology result
        Admin-->>Client: TopologyDiff or error
    else none
        Client->>Client: Skip topology round trip
    end
    Client-->>App: Connected client or startup error
```

`TopologyNone` assumes topology already exists. `TopologyDeclare` is useful for development;
production configuration rejects auto-creation in `validateConfig`.

## Batch semantics

`PublishBatch` is synchronous but not atomic. A `driver.PublishError` identifies failed indexes; the
successful messages keep their IDs and failed messages receive individual errors. A non-partial
transport error is returned as the method error.

## Close interaction

`Client.Close` marks the client as closing before waiting for active publishes. New application
publishes are rejected, while core-generated retry/DLQ publishes may use the internal
`allowClosing` path during runner drain.
