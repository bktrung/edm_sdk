# Observer event reference

This page is generated from F1's observer event vocabulary in [`observer.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/observer.go). Run `make check-observer-events` to verify it. Edit `observer.go`; do not edit the generated tables.

<!-- BEGIN GENERATED CONTENT -->
## ObserverKind

| Constant | Value | Shape | Description |
| --- | --- | --- | --- |
| `ObserverPublish` | `publish` | paired | ObserverPublish is a paired kind. The publisher emits Start at admission and Finish after the producer returns for a primary PublishBatch call. The worker emits another pair for each retry or dead-letter successor publish. Route is PublishRoutePrimary for the first path and PublishRouteRetry or PublishRouteDeadLetter for successor paths. |
| `ObserverMessageBuilt` | `message_built` | paired | ObserverMessageBuilt is a paired kind around building one outbound message in a PublishBatch call. |
| `ObserverProcess` | `process` | paired | ObserverProcess is a paired kind around one handler invocation. |
| `ObserverSettle` | `settle` | paired | ObserverSettle is a paired kind around one acknowledgement or negative acknowledgement. |
| `ObserverDrain` | `drain` | paired | ObserverDrain is a paired kind around one Runner.Drain call. |
| `ObserverDeliveryReceived` | `delivery_received` | point | ObserverDeliveryReceived is a point kind emitted once for each delivery admitted to dispatch. |
| `ObserverRetryScheduled` | `retry_scheduled` | point | ObserverRetryScheduled is a point kind emitted when a retry successor is confirmed. |
| `ObserverDeadLetterDecided` | `dead_letter_decided` | point | ObserverDeadLetterDecided is a point kind emitted when a dead-letter decision is made, before the copy is confirmed. |
| `ObserverDeadLetterPublished` | `dead_letter_published` | point | ObserverDeadLetterPublished is a point kind emitted when a dead-letter copy is confirmed. |
| `ObserverDeadLetterFailed` | `dead_letter_failed` | point | ObserverDeadLetterFailed is a point kind emitted when publishing a dead-letter copy fails. |
| `ObserverPoisonRejected` | `poison_rejected` | point | ObserverPoisonRejected is a point kind emitted after a dead-letter publish fails and the delivery is dropped because its reason is poison with no dead-letter route, or because the copy cannot be published due to broker size limits or an encoding error. A confirmed copy emits ObserverDeadLetterPublished. |
| `ObserverBacklogSampled` | `backlog_sampled` | point | ObserverBacklogSampled is a point kind emitted once for each sampled destination. |
| `ObserverConnectionLost` | `connection_lost` | point | ObserverConnectionLost is a point kind emitted when F1 observes a lost connection. |
| `ObserverConnectionRestored` | `connection_restored` | point | ObserverConnectionRestored is a point kind emitted when F1 installs a replacement connection. |
| `ObserverDriverSelected` | `driver_selected` | point | ObserverDriverSelected is a point kind emitted once after New opens the driver. |
| `ObserverDeadlinePromoted` | `deadline_promoted` | point | ObserverDeadlinePromoted is a point kind emitted when the scheduler promotes a lane head after its deadline. Events are rate-limited per lane; Suppressed counts promotions omitted since the previous event for that lane. |

## ObserverOutcome

| Constant | Value | Description |
| --- | --- | --- |
| `ObserverOutcomeOK` | `ok` | ObserverOutcomeOK means the stage completed normally. |
| `ObserverOutcomeError` | `error` | ObserverOutcomeError means the stage returned an error. |
| `ObserverOutcomeAbandoned` | `abandoned` | ObserverOutcomeAbandoned means the stage exited abnormally or was abandoned. |

## ErrorClass

| Constant | Value | Description |
| --- | --- | --- |
| `ErrorClassRetryable` | `f1_retryable` | ErrorClassRetryable means a handler error that is neither terminal nor dropped. |
| `ErrorClassTerminal` | `f1_terminal` | ErrorClassTerminal means the error was classified terminal. |
| `ErrorClassDropped` | `f1_dropped` | ErrorClassDropped means the error was classified dropped. |
| `ErrorClassDecode` | `f1_decode` | ErrorClassDecode means the payload could not be decoded. |
| `ErrorClassPanic` | `f1_panic` | ErrorClassPanic means the handler panicked. |
| `ErrorClassExpired` | `f1_expired` | ErrorClassExpired means the event expired before handling. |
| `ErrorClassUnmatched` | `f1_unmatched` | ErrorClassUnmatched means no handler matched the event type. |
| `ErrorClassMaxAttempts` | `f1_max_attempts` | ErrorClassMaxAttempts means the retry limit was exhausted. |
| `ErrorClassPoison` | `f1_poison` | ErrorClassPoison means the envelope attempt count exceeded configured maxAttempts by more than 10 and is treated as retry-counter runaway. |
| `ErrorClassDriverTransient` | `driver_transient` | ErrorClassDriverTransient means the driver reported a transient error. |
| `ErrorClassDriverFatal` | `driver_fatal` | ErrorClassDriverFatal means the driver reported a fatal error. |
| `ErrorClassDriverNotFound` | `driver_not_found` | ErrorClassDriverNotFound means the driver reported a missing resource. |
| `ErrorClassDriverTooLarge` | `driver_too_large` | ErrorClassDriverTooLarge means the driver rejected an oversized message. |
| `ErrorClassDriverPermission` | `driver_permission` | ErrorClassDriverPermission means the driver reported a permission error. |
| `ErrorClassDriverNotification` | `driver_notification` | ErrorClassDriverNotification means the driver reported a notification error. |
| `ErrorClassOther` | `_OTHER` | ErrorClassOther is the fallback for anything else. |

## PublishRoute

| Constant | Value | Description |
| --- | --- | --- |
| `PublishRoutePrimary` | `primary` | PublishRoutePrimary identifies an application-originated publish. |
| `PublishRouteRetry` | `retry` | PublishRouteRetry identifies a retry successor publish. |
| `PublishRouteDeadLetter` | `dead_letter` | PublishRouteDeadLetter identifies a dead-letter successor publish. |

## SettleOperation

| Constant | Value | Description |
| --- | --- | --- |
| `SettleAck` | `ack` | SettleAck means the delivery was acknowledged. |
| `SettleNack` | `nack` | SettleNack means the delivery was negatively acknowledged. |

## EnqueuedAtSource

| Constant | Value | Description |
| --- | --- | --- |
| `EnqueuedAtUnknown` | `` | EnqueuedAtUnknown means the enqueue time source is not known. |
| `EnqueuedAtProducer` | `producer` | EnqueuedAtProducer means the timestamp was stamped by the producer. |
| `EnqueuedAtBroker` | `broker` | EnqueuedAtBroker means the timestamp was stamped by the broker. |

## StartEvent fields

| Field | Type | Description |
| --- | --- | --- |
| `Kind` | `ObserverKind` | Kind identifies the stage. F1 sets it for every Start kind. |
| `At` | `time.Time` | At is the client clock time. F1 sets it for every Start kind. |
| `Topic` | `string` | Topic is the logical topic. F1 sets it for message-built, process, settle and successor-publish Starts. Primary-publish and drain Starts leave it zero. |
| `Subscription` | `string` | Subscription names the subscription. F1 sets it for process and settle Starts; it is zero for publish, message-built and drain Starts. |
| `ConsumerGroup` | `string` | ConsumerGroup names the consumer group. F1 sets it for process and settle Starts; it is zero otherwise. |
| `EventType` | `string` | EventType is the event type. F1 sets it for message-built, process and successor-publish Starts; it is zero for primary-publish, settle and drain. |
| `Priority` | `Priority` | Priority is the delivery lane. F1 sets it for message-built, process, settle and successor-publish Starts; primary-publish and drain Starts leave it zero. |
| `Attempt` | `int` | Attempt is the one-based attempt. F1 sets it for message-built, process and successor-publish Starts; it is zero for primary-publish, settle and drain. |
| `Destination` | `string` | Destination is the physical destination. F1 sets it for message-built, process, settle and successor-publish Starts; it is zero for primary-publish and drain. |
| `MessageID` | `string` | MessageID is the envelope ID. F1 sets it for message-built, process and successor-publish Starts; it is zero for primary-publish, settle and drain. |
| `CorrelationID` | `string` | CorrelationID is the workflow correlation ID. F1 sets it for message-built, process and successor-publish Starts; it is zero for primary-publish, settle and drain. |
| `Route` | `PublishRoute` | Route identifies the publish path. F1 sets it for primary and successor publish Starts; it is zero otherwise. |
| `BatchSize` | `int` | BatchSize counts messages in the call. F1 sets it for primary and successor publish Starts; it is zero otherwise. |
| `Operation` | `SettleOperation` | Operation identifies the settlement. F1 sets it for settle Starts; it is zero otherwise. |
| `DeliveryCount` | `int` | DeliveryCount counts broker deliveries. F1 sets it for process Starts; it is zero otherwise. |
| `EnqueuedAt` | `time.Time` | EnqueuedAt is the broker enqueue time. F1 sets it for process Starts when known; it is zero otherwise. |
| `EnqueuedAtSource` | `EnqueuedAtSource` | EnqueuedAtSource names the EnqueuedAt source. F1 sets it for process Starts when EnqueuedAt is set; it is unknown otherwise. |
| `TraceParent` | `string` | TraceParent carries the inbound traceparent for extraction. F1 sets it for process Starts when the inbound headers carry it; it is empty otherwise. |
| `TraceState` | `string` | TraceState carries the inbound tracestate for extraction. F1 sets it for process Starts when the inbound headers carry it; it is empty otherwise. |
| `Drain` | `DrainCounts` | Drain carries drain progress. F1 sets it for drain Starts; it is zero otherwise. |

## FinishEvent fields

| Field | Type | Description |
| --- | --- | --- |
| `Kind` | `ObserverKind` | Kind identifies the stage. F1 sets it for every Finish kind. |
| `At` | `time.Time` | At is the client clock time. F1 sets it for every Finish kind. |
| `Topic` | `string` | Topic is the logical topic. F1 sets it for message-built, process, settle and successor-publish Finishes. A primary-publish Finish gets it only when every message resolves to one topic; mixed or early-failure and drain Finishes leave it zero. |
| `Subscription` | `string` | Subscription names the subscription. F1 sets it for process and settle Finishes; it is zero otherwise. |
| `ConsumerGroup` | `string` | ConsumerGroup names the consumer group. F1 sets it for process and settle Finishes; it is zero otherwise. |
| `EventType` | `string` | EventType is the event type. F1 sets it for message-built, process and successor-publish Finishes; it is zero for primary-publish, settle and drain. |
| `Priority` | `Priority` | Priority is the delivery lane. F1 sets it for message-built, process, settle and successor-publish Finishes. A primary-publish Finish gets it only when every message resolves to one priority; mixed and drain Finishes use the zero value, PriorityMedium. |
| `Attempt` | `int` | Attempt is the one-based attempt. F1 sets it for message-built, process and successor-publish Finishes; it is zero otherwise. |
| `Destination` | `string` | Destination is the physical destination. F1 sets it for message-built, process, settle and successor-publish Finishes; it is zero otherwise. |
| `MessageID` | `string` | MessageID is the envelope ID. F1 sets it for message-built, process and successor-publish Finishes; it is zero for primary-publish, settle and drain. |
| `CorrelationID` | `string` | CorrelationID is the workflow correlation ID. F1 sets it for message-built, process and successor-publish Finishes; it is zero for primary-publish, settle and drain. |
| `Outcome` | `ObserverOutcome` | Outcome identifies how the stage ended. F1 sets it for every Finish kind. |
| `ErrorClass` | `ErrorClass` | ErrorClass classifies the failure. F1 sets it when Outcome is error; it is empty otherwise. |
| `Terminal` | `bool` | Terminal reports whether processing ended the delivery. F1 sets it for process Finishes; it is false otherwise. |
| `Results` | `[]MessageResult` | Results carries per-index primary-publish outcomes. F1 sets it for primary publish Finishes; it is nil for successor-publish and other Finishes. The implementation must not retain or mutate it. |
| `Drain` | `DrainCounts` | Drain carries drain progress. F1 sets it for drain Finishes; it is zero otherwise. |

## PointEvent fields

| Field | Type | Description |
| --- | --- | --- |
| `Kind` | `ObserverKind` | Kind identifies the point. Set for every point kind. |
| `At` | `time.Time` | At is the client clock time. Set for every point kind. |
| `Topic` | `string` | Topic is the logical topic. Set for delivery received when its destination is known to the consumer generation; set for retry scheduled, dead-letter decided, dead-letter published, dead-letter failed, poison rejected, backlog sampled and deadline promoted; zero for unknown destinations, connection lost, connection restored and driver selected. |
| `Subscription` | `string` | Subscription names the subscription. Set for delivery received, retry scheduled, dead-letter kinds, poison rejected, backlog sampled and deadline promoted; zero otherwise. |
| `ConsumerGroup` | `string` | ConsumerGroup names the consumer group. Set for delivery received where a group applies; zero otherwise. |
| `EventType` | `string` | EventType is the event type. Set for delivery received, retry scheduled, dead-letter kinds and poison rejected; zero otherwise. |
| `Priority` | `Priority` | Priority is the delivery lane. Set for delivery received when its destination maps to one priority; set for retry scheduled, dead-letter kinds, poison rejected, backlog sampled and deadline promoted; the zero value, PriorityMedium, for unknown destinations, connection lost, connection restored and driver selected. |
| `Attempt` | `int` | Attempt is the one-based attempt. Set for delivery received, retry scheduled, dead-letter kinds and poison rejected; zero otherwise. |
| `Destination` | `string` | Destination is the physical destination. Set for delivery received, retry scheduled, dead-letter kinds and poison rejected; zero otherwise. |
| `MessageID` | `string` | MessageID is the envelope ID. Set for delivery received, retry scheduled, dead-letter kinds and poison rejected; zero otherwise. |
| `CorrelationID` | `string` | CorrelationID is the workflow correlation ID. Set for delivery received, retry scheduled, dead-letter kinds and poison rejected; zero otherwise. |
| `ErrorClass` | `ErrorClass` | ErrorClass classifies the failure. Set for retry scheduled, dead-letter failed, poison rejected and connection lost; empty otherwise. |
| `DeliveryCount` | `int` | DeliveryCount counts broker deliveries. Set for delivery received, retry scheduled, dead-letter kinds and poison rejected; zero otherwise. |
| `EnqueuedAt` | `time.Time` | EnqueuedAt is the broker enqueue time. Set for delivery received, retry scheduled, dead-letter kinds and poison rejected when known; zero otherwise. |
| `EnqueuedAtSource` | `EnqueuedAtSource` | EnqueuedAtSource names the EnqueuedAt source. Set for delivery received, retry scheduled, dead-letter kinds and poison rejected when EnqueuedAt is known; for backlog sampled, it names the head source when HeadAgeKnown. Unknown is reported otherwise. |
| `NextAttempt` | `int` | NextAttempt is the scheduled attempt. Set for retry scheduled; zero otherwise. |
| `MaxAttempts` | `int` | MaxAttempts caps the retry ladder. Set for retry scheduled; zero otherwise. |
| `Backoff` | `time.Duration` | Backoff delays the next attempt. Set for retry scheduled; zero otherwise. |
| `Reason` | `DeathReason` | Reason identifies why a message was dead-lettered. Set for dead-letter decided, dead-letter published, dead-letter failed and poison rejected; empty otherwise. |
| `DeadLetterDestination` | `string` | DeadLetterDestination names the dead-letter destination. Set for dead-letter decided, dead-letter published and dead-letter failed; empty otherwise. |
| `Backlog` | `int64` | Backlog counts lagged messages. Set for backlog sampled; zero otherwise. |
| `HeadAge` | `time.Duration` | HeadAge is the oldest waiting age. Set for backlog sampled when known; zero otherwise. See HeadAgeKnown. |
| `HeadAgeKnown` | `bool` | HeadAgeKnown distinguishes a known zero age from an unknown age. Set for backlog sampled; false otherwise. |
| `Downtime` | `time.Duration` | Downtime is the elapsed outage. Set for connection restored; zero otherwise. |
| `DriverName` | `string` | DriverName names the driver. Set for driver selected; empty otherwise. |
| `SDKVersion` | `string` | SDKVersion is the SDK release in use, from the build information, or "(devel)" when the build records none. Set for driver selected; empty otherwise. |
| `ServerAddress` | `string` | ServerAddress names the broker host. Set for connection lost, connection restored and driver selected when known; empty otherwise. |
| `ServerPort` | `int` | ServerPort numbers the broker port. Set for connection lost, connection restored and driver selected when known; zero otherwise. |
| `LaneWait` | `time.Duration` | LaneWait measures scheduler wait for the promoted head. Set for deadline promoted; zero otherwise. |
| `LaneDepth` | `int` | LaneDepth counts the lane depth at the pick. Set for deadline promoted; zero otherwise. |
| `Suppressed` | `int` | Suppressed counts deadline promotions the per-lane rate limit dropped since the previous event for that lane. Set for deadline promoted; zero otherwise. |

## Token fields

| Field | Type | Description |
| --- | --- | --- |
| `Kind` | `ObserverKind` | Kind identifies the stage matched by this token. |
| `Start` | `time.Time` | Start records the client-clock time at which the stage started. |
| `Handle` | `uint64` | Handle identifies the adapter-owned stage state; zero means no state. |

## DrainCounts fields

| Field | Type | Description |
| --- | --- | --- |
| `InFlight` | `int` | InFlight counts deliveries in flight. F1 sets it on drain starts and finishes. |
| `Drained` | `int` | Drained counts deliveries drained. F1 sets it on drain finishes. |
| `Remaining` | `int` | Remaining counts deliveries still held after the drain. F1 sets it on drain finishes. |
| `Timeout` | `time.Duration` | Timeout bounds the drain. F1 sets it on drain starts and finishes when a bound applies. |

<!-- END GENERATED CONTENT -->
