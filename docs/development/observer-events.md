# Observer event reference

This page is generated from [`observer.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/observer.go). Run `make check-observer-events` to verify it. Edit `observer.go`; do not edit the generated tables.

<!-- BEGIN GENERATED CONTENT -->
## ObserverKind

| Constant | Value | Shape | Description |
| --- | --- | --- | --- |
| `ObserverPublish` | `publish` | paired | ObserverPublish is a paired kind. The publisher emits Start and Finish around one PublishBatch call at admission and after the producer returns. |
| `ObserverMessageBuilt` | `message_built` | paired | ObserverMessageBuilt is a paired kind. The publisher emits Start and Finish around one outbound message build inside PublishBatch. |
| `ObserverProcess` | `process` | paired | ObserverProcess is a paired kind. The worker emits Start and Finish around one handler invocation on a pool worker. |
| `ObserverSettle` | `settle` | paired | ObserverSettle is a paired kind. The worker emits Start and Finish around one acknowledgement or negative acknowledgement. |
| `ObserverDrain` | `drain` | paired | ObserverDrain is a paired kind. The runner emits Start and Finish around one Runner Drain. |
| `ObserverDeliveryReceived` | `delivery_received` | point | ObserverDeliveryReceived is a point kind. The worker emits it once per delivery admitted to dispatch. |
| `ObserverRetryScheduled` | `retry_scheduled` | point | ObserverRetryScheduled is a point kind. The worker emits it when a retry successor is confirmed. |
| `ObserverDeadLetterDecided` | `dead_letter_decided` | point | ObserverDeadLetterDecided is a point kind. The worker emits it when the dead-letter decision is taken, before the confirm. |
| `ObserverDeadLetterPublished` | `dead_letter_published` | point | ObserverDeadLetterPublished is a point kind. The worker emits it when the dead-letter publish is confirmed by the driver. |
| `ObserverDeadLetterFailed` | `dead_letter_failed` | point | ObserverDeadLetterFailed is a point kind. The worker emits it when the dead-letter publish fails. |
| `ObserverPoisonRejected` | `poison_rejected` | point | ObserverPoisonRejected is a point kind. The worker emits it when a decode, poison, expired or unmatched delivery is dropped. |
| `ObserverBacklogSampled` | `backlog_sampled` | point | ObserverBacklogSampled is a point kind. The backlog poller emits it from the backlog poll loop once per sampled destination. |
| `ObserverConnectionLost` | `connection_lost` | point | ObserverConnectionLost is a point kind. The reconnect supervisor emits it when it observes a lost connection. |
| `ObserverConnectionRestored` | `connection_restored` | point | ObserverConnectionRestored is a point kind. The reconnect supervisor emits it when it swaps in a new connection. |
| `ObserverDriverSelected` | `driver_selected` | point | ObserverDriverSelected is a point kind. The client emits it once per client in New after the driver is opened. |
| `ObserverDeadlinePromoted` | `deadline_promoted` | point | ObserverDeadlinePromoted is a point kind. The worker emits it after the scheduler promotes a lane head past its deadline, rate limited to one event per lane per window. Suppressed carries the promotions the limit dropped since the last recorded event. |

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
| `ErrorClassPoison` | `f1_poison` | ErrorClassPoison means the message repeatedly crashed its worker. |
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
| `PublishRoutePrimary` | `primary` | PublishRoutePrimary means the application primary publish path. |
| `PublishRouteRetry` | `retry` | PublishRouteRetry means the internal retry successor publish path. |
| `PublishRouteDeadLetter` | `dead_letter` | PublishRouteDeadLetter means the internal dead-letter publish path. |

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
| `Kind` | `ObserverKind` | Kind identifies the stage. Set for every Start kind. |
| `At` | `time.Time` | At is the client clock time. Set for every Start kind. |
| `Topic` | `string` | Topic is the logical topic. Set for message built, process and settle. Primary publish Start is emitted before message resolution and leaves this zero; drain and starts without a message also leave it zero. |
| `Subscription` | `string` | Subscription names the subscription. Set for process and settle; zero for publish, message built and drain. |
| `ConsumerGroup` | `string` | ConsumerGroup names the consumer group. Set for process and settle; zero otherwise. |
| `EventType` | `string` | EventType is the event type. Set for publish, message built, process and settle; zero for drain. |
| `Priority` | `Priority` | Priority is the delivery lane. Set for message built, process and settle. Primary publish Start is emitted before message resolution and leaves this zero; drain and starts without a message also leave it zero. |
| `Attempt` | `int` | Attempt is the one-based attempt. Set for process and settle; zero for publish, message built and drain. |
| `Destination` | `string` | Destination is the physical destination. Set for publish, message built, process and settle; zero for drain. |
| `MessageID` | `string` | MessageID is the envelope ID. Set for publish, message built, process and settle; zero for drain. |
| `CorrelationID` | `string` | CorrelationID is the workflow correlation ID. Set for publish, message built, process and settle; zero for drain. |
| `Route` | `PublishRoute` | Route identifies the publish path. Set for publish; zero otherwise. |
| `BatchSize` | `int` | BatchSize counts messages in the call. Set for publish; zero otherwise. |
| `Operation` | `SettleOperation` | Operation identifies the settlement. Set for settle; zero otherwise. |
| `DeliveryCount` | `int` | DeliveryCount counts broker deliveries. Set for process and settle; zero otherwise. |
| `EnqueuedAt` | `time.Time` | EnqueuedAt is the broker enqueue time. Set for process when known; zero otherwise. |
| `EnqueuedAtSource` | `EnqueuedAtSource` | EnqueuedAtSource names the EnqueuedAt source. Set for process when EnqueuedAt is set; unknown otherwise. |
| `TraceParent` | `string` | TraceParent carries the inbound traceparent for extraction. Set for process when the inbound headers carry it; empty otherwise. |
| `TraceState` | `string` | TraceState carries the inbound tracestate for extraction. Set for process when the inbound headers carry it; empty otherwise. |
| `Drain` | `DrainCounts` | Drain carries drain progress. Set for drain; zero otherwise. |

## FinishEvent fields

| Field | Type | Description |
| --- | --- | --- |
| `Kind` | `ObserverKind` | Kind identifies the stage. Set for every Finish kind. |
| `At` | `time.Time` | At is the client clock time. Set for every Finish kind. |
| `Topic` | `string` | Topic is the logical topic. Set for message built, process and settle; set for publish when every message in the call resolves to the same topic; zero for mixed calls, failures before all messages resolve and drain. |
| `Subscription` | `string` | Subscription names the subscription. Set for process and settle; zero otherwise. |
| `ConsumerGroup` | `string` | ConsumerGroup names the consumer group. Set for process and settle; zero otherwise. |
| `EventType` | `string` | EventType is the event type. Set for publish, message built, process and settle; zero for drain. |
| `Priority` | `Priority` | Priority is the delivery lane. Set for message built, process and settle; set for publish when every message in the call resolves to the same priority; the zero value, PriorityMedium, for mixed calls, failures before all messages resolve and drain. |
| `Attempt` | `int` | Attempt is the one-based attempt. Set for process and settle; zero otherwise. |
| `Destination` | `string` | Destination is the physical destination. Set for publish, message built, process and settle; zero for drain. |
| `MessageID` | `string` | MessageID is the envelope ID. Set for publish, message built, process and settle; zero for drain. |
| `CorrelationID` | `string` | CorrelationID is the workflow correlation ID. Set for publish, message built, process and settle; zero for drain. |
| `Outcome` | `ObserverOutcome` | Outcome identifies how the stage ended. Set for every Finish kind. |
| `ErrorClass` | `ErrorClass` | ErrorClass classifies the failure. Set when Outcome is error; empty otherwise. |
| `Terminal` | `bool` | Terminal reports the settlement ends the delivery. Set for process; false otherwise. |
| `Results` | `[]MessageResult` | Results carries per-index publish outcomes. Set for publish; nil otherwise. The implementation must not retain or mutate it. |
| `Drain` | `DrainCounts` | Drain carries drain progress. Set for drain; zero otherwise. |

## PointEvent fields

| Field | Type | Description |
| --- | --- | --- |
| `Kind` | `ObserverKind` | Kind identifies the point. Set for every point kind. |
| `At` | `time.Time` | At is the client clock time. Set for every point kind. |
| `Topic` | `string` | Topic is the logical topic. Set for delivery received when its destination is known to the consumer generation, including entries with ambiguous priority; set for retry scheduled, dead-letter decided, dead-letter published, dead-letter failed, poison rejected, backlog sampled and deadline promoted; zero for unknown destinations, connection lost, connection restored and driver selected. |
| `Subscription` | `string` | Subscription names the subscription. Set for delivery received, retry scheduled, dead-letter kinds, poison rejected, backlog sampled and deadline promoted; zero otherwise. |
| `ConsumerGroup` | `string` | ConsumerGroup names the consumer group. Set for delivery received where a group applies; zero otherwise. |
| `EventType` | `string` | EventType is the event type. Set for delivery received, retry scheduled, dead-letter kinds and poison rejected; zero otherwise. |
| `Priority` | `Priority` | Priority is the delivery lane. Set for delivery received when its destination maps to one priority; set for retry scheduled, dead-letter kinds, poison rejected, backlog sampled and deadline promoted; the zero value, PriorityMedium, for ambiguous or unknown destinations, connection lost, connection restored and driver selected. |
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
| `DriverVersion` | `string` | DriverVersion versions the driver. Set for driver selected; empty otherwise. |
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
| `InFlight` | `int` | InFlight counts deliveries in flight. Set for drain starts and finishes. |
| `Queued` | `int` | Queued counts deliveries queued behind the drain. Set for drain starts and finishes. |
| `Drained` | `int` | Drained counts deliveries drained. Set for drain finishes. |
| `Remaining` | `int` | Remaining counts deliveries still held after the drain. Set for drain finishes. |
| `Timeout` | `time.Duration` | Timeout bounds the drain. Set for drain starts and finishes when a bound applies. |

<!-- END GENERATED CONTENT -->
