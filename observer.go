package f1

import (
	"context"
	"time"
)

// Observer receives lifecycle and point events from a Client.
//
// F1 calls observer methods synchronously and may call them concurrently across
// publishers and subscriptions. Implementations must be safe for concurrent
// use, must not depend on goroutine-local state, and should return promptly.
// Start precedes its matching Finish; no order is guaranteed between different
// stages. A stage may have no Start if a message never reaches it.
//
// A panic is recovered and logged once per ObserverKind without changing the
// message outcome. F1 calls no observer method while holding Client or Runner
// locks, so implementations may call back into the Client.
//
// Start's returned context reaches the handler only for ObserverProcess. Other
// returned contexts may supply values for trace injection and context-aware
// logging, but do not control broker operations, including primary publication,
// successor publication, and settlement. Event values are passed by value.
// Do not retain or mutate FinishEvent.Results after Finish returns. Timestamps
// use the Client clock; derive durations from event timestamps. Errors are
// represented by bounded ErrorClass values, not raw error strings or Go type
// names.
//
// For metric dimensions, use logical topics, subscriptions, and consumer groups;
// avoid physical destinations and message identities.
//
// F1 calls Finish once for each Start, with outcome ObserverOutcomeAbandoned if
// a stage exits abnormally. A nil Observer disables callbacks. Ignore unknown
// kinds and fields so additions remain compatible.
type Observer interface {
	// Start begins an observed stage and returns its context and observer-owned
	// token. For ObserverProcess, F1 passes the returned context to the handler; a
	// nil context falls back to the caller's context.
	Start(context.Context, StartEvent) (context.Context, Token)
	// Finish ends the stage identified by token. F1 calls it once for each Start.
	Finish(Token, FinishEvent)
	// Record receives a point event that has no matching Finish.
	Record(PointEvent)
}

// TraceInjector optionally supplies trace context for outbound messages
// published through an observed Client. Inbound trace values are available in
// StartEvent; no separate extraction method is called. Implementations must be
// safe for concurrent calls.
type TraceInjector interface {
	// InjectTrace returns the traceparent and tracestate values to attach to an
	// outbound message built with ctx.
	InjectTrace(ctx context.Context) (traceParent, traceState string)
}

// ObserverBinder optionally binds an Observer to the Client that New is
// building. New calls BindClient once, after configuration and codec
// validation and before it opens the driver or emits any observer event. If
// BindClient returns an error, no binding was acquired and New returns that
// error wrapped, without opening the driver. Implementations must be safe for
// concurrent calls and should return promptly.
//
// After a successful BindClient, F1 calls a non-nil unbind exactly once: when
// New fails later, or when Close completes terminal shutdown of the Client. A
// Close that fails part way and leaves the Client retryable keeps the binding.
// A nil unbind needs no cleanup. Unbind is called without holding Client
// locks, must return promptly, and must not wait for observer calls to stop:
// binding controls which Client is attached, not delivery of events already in
// flight.
//
// A wrapper Observer must forward BindClient to keep the wrapped Observer's
// binding enforced, returning the wrapped callback and error unchanged.
// Observers that do not implement ObserverBinder have no binding restriction.
type ObserverBinder interface {
	// BindClient acquires this Observer's attachment to one Client and returns
	// the callback that releases it, or an error without acquiring anything.
	BindClient() (unbind func(), err error)
}

// ObserverKind identifies one lifecycle stage or point event.
type ObserverKind string

const (
	// ObserverPublish is a paired kind. The publisher emits Start at admission and
	// Finish after the producer returns for a primary PublishBatch call. The worker
	// emits another pair for each retry or dead-letter successor publish. Route is
	// PublishRoutePrimary for the first path and PublishRouteRetry or
	// PublishRouteDeadLetter for successor paths.
	ObserverPublish ObserverKind = "publish"
	// ObserverMessageBuilt is a paired kind around building one outbound message
	// in a PublishBatch call.
	ObserverMessageBuilt ObserverKind = "message_built"
	// ObserverProcess is a paired kind around one handler invocation.
	ObserverProcess ObserverKind = "process"
	// ObserverSettle is a paired kind around one acknowledgement or negative
	// acknowledgement.
	ObserverSettle ObserverKind = "settle"
	// ObserverDrain is a paired kind around one Runner.Drain call.
	ObserverDrain ObserverKind = "drain"
	// ObserverDeliveryReceived is a point kind emitted once for each delivery
	// admitted to dispatch.
	ObserverDeliveryReceived ObserverKind = "delivery_received"
	// ObserverRetryScheduled is a point kind emitted when a retry successor is
	// confirmed.
	ObserverRetryScheduled ObserverKind = "retry_scheduled"
	// ObserverDeadLetterDecided is a point kind emitted when a dead-letter
	// decision is made, before the copy is confirmed.
	ObserverDeadLetterDecided ObserverKind = "dead_letter_decided"
	// ObserverDeadLetterPublished is a point kind emitted when a dead-letter
	// copy is confirmed.
	ObserverDeadLetterPublished ObserverKind = "dead_letter_published"
	// ObserverDeadLetterFailed is a point kind emitted when publishing a
	// dead-letter copy fails.
	ObserverDeadLetterFailed ObserverKind = "dead_letter_failed"
	// ObserverPoisonRejected is a point kind emitted after a dead-letter publish
	// fails and the delivery is dropped because its reason is poison with no
	// dead-letter route, or because the copy cannot be published due to broker
	// size limits or an encoding error. A confirmed copy emits
	// ObserverDeadLetterPublished.
	ObserverPoisonRejected ObserverKind = "poison_rejected"
	// ObserverBacklogSampled is a point kind emitted once for each sampled
	// destination.
	ObserverBacklogSampled ObserverKind = "backlog_sampled"
	// ObserverConnectionLost is a point kind emitted when F1 observes a lost
	// connection.
	ObserverConnectionLost ObserverKind = "connection_lost"
	// ObserverConnectionRestored is a point kind emitted when F1 installs a
	// replacement connection.
	ObserverConnectionRestored ObserverKind = "connection_restored"
	// ObserverDriverSelected is a point kind emitted once after New opens the
	// driver.
	ObserverDriverSelected ObserverKind = "driver_selected"
	// ObserverDeadlinePromoted is a point kind emitted when the scheduler
	// promotes a lane head after its deadline. Events are rate-limited per lane;
	// Suppressed counts promotions omitted since the previous event for that
	// lane.
	ObserverDeadlinePromoted ObserverKind = "deadline_promoted"
)

// ObserverOutcome identifies how a started observer stage ended.
type ObserverOutcome string

const (
	// ObserverOutcomeOK means the stage completed normally.
	ObserverOutcomeOK ObserverOutcome = "ok"
	// ObserverOutcomeError means the stage returned an error.
	ObserverOutcomeError ObserverOutcome = "error"
	// ObserverOutcomeAbandoned means the stage exited abnormally or was abandoned.
	ObserverOutcomeAbandoned ObserverOutcome = "abandoned"
)

// ErrorClass is the bounded error vocabulary carried on events. The empty
// value means no error.
type ErrorClass string

const (
	// ErrorClassRetryable means a handler error that is neither terminal nor dropped.
	ErrorClassRetryable ErrorClass = "f1_retryable"
	// ErrorClassTerminal means the error was classified terminal.
	ErrorClassTerminal ErrorClass = "f1_terminal"
	// ErrorClassDropped means the error was classified dropped.
	ErrorClassDropped ErrorClass = "f1_dropped"
	// ErrorClassDecode means the payload could not be decoded.
	ErrorClassDecode ErrorClass = "f1_decode"
	// ErrorClassPanic means the handler panicked.
	ErrorClassPanic ErrorClass = "f1_panic"
	// ErrorClassExpired means the event expired before handling.
	ErrorClassExpired ErrorClass = "f1_expired"
	// ErrorClassUnmatched means no handler matched the event type.
	ErrorClassUnmatched ErrorClass = "f1_unmatched"
	// ErrorClassMaxAttempts means the retry limit was exhausted.
	ErrorClassMaxAttempts ErrorClass = "f1_max_attempts"
	// ErrorClassPoison means the envelope attempt count exceeded configured
	// maxAttempts by more than 10 and is treated as retry-counter runaway.
	ErrorClassPoison ErrorClass = "f1_poison"
	// ErrorClassDriverTransient means the driver reported a transient error.
	ErrorClassDriverTransient ErrorClass = "driver_transient"
	// ErrorClassDriverFatal means the driver reported a fatal error.
	ErrorClassDriverFatal ErrorClass = "driver_fatal"
	// ErrorClassDriverNotFound means the driver reported a missing resource.
	ErrorClassDriverNotFound ErrorClass = "driver_not_found"
	// ErrorClassDriverTooLarge means the driver rejected an oversized message.
	ErrorClassDriverTooLarge ErrorClass = "driver_too_large"
	// ErrorClassDriverPermission means the driver reported a permission error.
	ErrorClassDriverPermission ErrorClass = "driver_permission"
	// ErrorClassDriverNotification means the driver reported a notification error.
	ErrorClassDriverNotification ErrorClass = "driver_notification"
	// ErrorClassOther is the fallback for anything else.
	ErrorClassOther ErrorClass = "_OTHER"
)

// PublishRoute identifies the path used for a publish. Its zero value is empty
// and means no route was recorded.
type PublishRoute string

const (
	// PublishRoutePrimary identifies an application-originated publish.
	PublishRoutePrimary PublishRoute = "primary"
	// PublishRouteRetry identifies a retry successor publish.
	PublishRouteRetry PublishRoute = "retry"
	// PublishRouteDeadLetter identifies a dead-letter successor publish.
	PublishRouteDeadLetter PublishRoute = "dead_letter"
)

// SettleOperation identifies the settlement operation.
type SettleOperation string

const (
	// SettleAck means the delivery was acknowledged.
	SettleAck SettleOperation = "ack"
	// SettleNack means the delivery was negatively acknowledged.
	SettleNack SettleOperation = "nack"
)

// EnqueuedAtSource identifies where an enqueue timestamp came from. The zero
// value is unknown.
type EnqueuedAtSource string

const (
	// EnqueuedAtUnknown means the enqueue time source is not known.
	EnqueuedAtUnknown EnqueuedAtSource = ""
	// EnqueuedAtProducer means the timestamp was stamped by the producer.
	EnqueuedAtProducer EnqueuedAtSource = "producer"
	// EnqueuedAtBroker means the timestamp was stamped by the broker.
	EnqueuedAtBroker EnqueuedAtSource = "broker"
)

// Token carries the value an observer needs between Start and Finish. Its zero
// value carries no started stage and is ignored by adapters that track stages.
type Token struct {
	// Kind identifies the stage matched by this token.
	Kind ObserverKind
	// Start records the client-clock time at which the stage started.
	Start time.Time
	// Handle identifies the adapter-owned stage state; zero means no state.
	Handle uint64
}

// DrainCounts carries drain progress. Zero value means the kind does not
// report drain progress.
type DrainCounts struct {
	// InFlight counts deliveries in flight. F1 sets it on drain starts and finishes.
	InFlight int
	// Drained counts deliveries drained. F1 sets it on drain finishes.
	Drained int
	// Remaining counts deliveries still held after the drain. F1 sets it on drain
	// finishes.
	Remaining int
	// Timeout bounds the drain. F1 sets it on drain starts and finishes when a
	// bound applies.
	Timeout time.Duration
}

// StartEvent describes the beginning of an observer stage. All structs pass by
// value with no pointers and no maps. F1 sets fields according to the stage;
// a field a kind does not set holds its zero value.
type StartEvent struct {
	// Kind identifies the stage. F1 sets it for every Start kind.
	Kind ObserverKind
	// At is the client clock time. F1 sets it for every Start kind.
	At time.Time
	// Topic is the logical topic. F1 sets it for message-built, process, settle
	// and successor-publish Starts. Primary-publish and drain Starts leave it zero.
	Topic string
	// Subscription names the subscription. F1 sets it for process and settle
	// Starts; it is zero for publish, message-built and drain Starts.
	Subscription string
	// ConsumerGroup names the consumer group. F1 sets it for process and settle
	// Starts; it is zero otherwise.
	ConsumerGroup string
	// EventType is the event type. F1 sets it for message-built, process and
	// successor-publish Starts; it is zero for primary-publish, settle and drain.
	EventType string
	// Priority is the delivery lane. F1 sets it for message-built, process, settle
	// and successor-publish Starts; primary-publish and drain Starts leave it zero.
	Priority Priority
	// Attempt is the one-based attempt. F1 sets it for message-built, process and
	// successor-publish Starts; it is zero for primary-publish, settle and drain.
	Attempt int
	// Destination is the physical destination. F1 sets it for message-built,
	// process, settle and successor-publish Starts; it is zero for primary-publish
	// and drain.
	Destination string
	// MessageID is the envelope ID. F1 sets it for message-built, process and
	// successor-publish Starts; it is zero for primary-publish, settle and drain.
	MessageID string
	// CorrelationID is the workflow correlation ID. F1 sets it for message-built,
	// process and successor-publish Starts; it is zero for primary-publish, settle
	// and drain.
	CorrelationID string
	// Route identifies the publish path. F1 sets it for primary and successor
	// publish Starts; it is zero otherwise.
	Route PublishRoute
	// BatchSize counts messages in the call. F1 sets it for primary and successor
	// publish Starts; it is zero otherwise.
	BatchSize int
	// Operation identifies the settlement. F1 sets it for settle Starts; it is zero
	// otherwise.
	Operation SettleOperation
	// DeliveryCount counts broker deliveries. F1 sets it for process Starts; it is
	// zero otherwise.
	DeliveryCount int
	// EnqueuedAt is the broker enqueue time. F1 sets it for process Starts when
	// known; it is zero otherwise.
	EnqueuedAt time.Time
	// EnqueuedAtSource names the EnqueuedAt source. F1 sets it for process Starts
	// when EnqueuedAt is set; it is unknown otherwise.
	EnqueuedAtSource EnqueuedAtSource
	// TraceParent carries the inbound traceparent for extraction. F1 sets it for
	// process Starts when the inbound headers carry it; it is empty otherwise.
	TraceParent string
	// TraceState carries the inbound tracestate for extraction. F1 sets it for
	// process Starts when the inbound headers carry it; it is empty otherwise.
	TraceState string
	// Drain carries drain progress. F1 sets it for drain Starts; it is zero
	// otherwise.
	Drain DrainCounts
}

// ObserverMessageResult describes one primary-publish outcome without exposing
// its error. Results follow input order. The zero value means no published ID
// and no per-index error, as on an unattempted publish.
type ObserverMessageResult struct {
	// ID is the published envelope ID. It is empty for failed or unattempted
	// messages; a successful result has a non-empty ID and an empty ErrorClass.
	ID string
	// ErrorClass is the bounded classification of the per-index publish error.
	// It is empty when that error is nil, including unattempted messages.
	ErrorClass ErrorClass
}

// FinishEvent describes the end of an observer stage. All structs pass by
// value with no maps and no pointers except Results. F1 sets fields according
// to the stage; a field a kind does not set holds its zero value.
type FinishEvent struct {
	// Kind identifies the stage. F1 sets it for every Finish kind.
	Kind ObserverKind
	// At is the client clock time. F1 sets it for every Finish kind.
	At time.Time
	// Topic is the logical topic. F1 sets it for message-built, process, settle
	// and successor-publish Finishes. A primary-publish Finish gets it only when
	// every message resolves to one topic; mixed or early-failure and drain
	// Finishes leave it zero.
	Topic string
	// Subscription names the subscription. F1 sets it for process and settle
	// Finishes; it is zero otherwise.
	Subscription string
	// ConsumerGroup names the consumer group. F1 sets it for process and settle
	// Finishes; it is zero otherwise.
	ConsumerGroup string
	// EventType is the event type. F1 sets it for message-built, process and
	// successor-publish Finishes; it is zero for primary-publish, settle and drain.
	EventType string
	// Priority is the delivery lane. It is meaningful only when PriorityKnown is
	// true; otherwise it holds the zero value, PriorityMedium.
	Priority Priority
	// PriorityKnown reports whether Priority carries the stage's delivery lane.
	// F1 sets it for message-built, process, settle and successor-publish
	// Finishes, and for a primary-publish Finish whose messages all resolve to
	// one priority. It is false for a mixed or unresolved primary publish, a
	// drain Finish, and an abandoned Finish.
	PriorityKnown bool
	// Attempt is the one-based attempt. F1 sets it for message-built, process and
	// successor-publish Finishes; it is zero otherwise.
	Attempt int
	// Destination is the physical destination. F1 sets it for message-built,
	// process, settle and successor-publish Finishes; it is zero otherwise.
	Destination string
	// MessageID is the envelope ID. F1 sets it for message-built, process and
	// successor-publish Finishes; it is zero for primary-publish, settle and drain.
	MessageID string
	// CorrelationID is the workflow correlation ID. F1 sets it for message-built,
	// process and successor-publish Finishes; it is zero for primary-publish,
	// settle and drain.
	CorrelationID string
	// Outcome identifies how the stage ended. F1 sets it for every Finish kind.
	Outcome ObserverOutcome
	// ErrorClass classifies the failure. F1 sets it when Outcome is error; it is
	// empty otherwise.
	ErrorClass ErrorClass
	// Terminal reports whether processing ended the delivery. F1 sets it for
	// process Finishes; it is false otherwise.
	Terminal bool
	// Results carries bounded per-index primary-publish outcomes in input order.
	// F1 sets it for primary-publish Finishes; it is nil for successor-publish
	// and other Finishes. Observers must not retain or mutate it. An empty ID and
	// empty ErrorClass means no published result and no per-index error.
	Results []ObserverMessageResult
	// Drain carries drain progress. F1 sets it for drain Finishes; it is zero
	// otherwise.
	Drain DrainCounts
}

// PointEvent describes an instantaneous observer point. All structs pass by
// value with no pointers and no maps. A field a kind does not set holds
// its zero value.
type PointEvent struct {
	// Kind identifies the point. Set for every point kind.
	Kind ObserverKind
	// At is the client clock time. Set for every point kind.
	At time.Time
	// Topic is the logical topic. Set for delivery received when its
	// destination is known to the consumer generation; set for retry
	// scheduled, dead-letter decided, dead-letter published, dead-letter
	// failed, poison rejected, backlog sampled and deadline promoted; zero for
	// unknown destinations, connection lost, connection restored and driver
	// selected.
	Topic string
	// Subscription names the subscription. Set for delivery received, retry
	// scheduled, dead-letter kinds, poison rejected, backlog sampled and
	// deadline promoted; zero otherwise.
	Subscription string
	// ConsumerGroup names the consumer group. Set for delivery received where
	// a group applies; zero otherwise.
	ConsumerGroup string
	// EventType is the event type. Set for delivery received, retry
	// scheduled, dead-letter kinds and poison rejected; zero otherwise.
	EventType string
	// Priority is the delivery lane. Set for delivery received when its
	// destination maps to one priority; set for retry scheduled, dead-letter
	// kinds, poison rejected, backlog sampled and deadline promoted; the zero
	// value, PriorityMedium, for unknown destinations, connection lost,
	// connection restored and driver selected.
	Priority Priority
	// Attempt is the one-based attempt. Set for delivery received, retry
	// scheduled, dead-letter kinds and poison rejected; zero otherwise.
	Attempt int
	// Destination is the physical destination. Set for delivery received,
	// retry scheduled, dead-letter kinds and poison rejected; zero otherwise.
	Destination string
	// MessageID is the envelope ID. Set for delivery received, retry
	// scheduled, dead-letter kinds and poison rejected; zero otherwise.
	MessageID string
	// CorrelationID is the workflow correlation ID. Set for delivery
	// received, retry scheduled, dead-letter kinds and poison rejected; zero
	// otherwise.
	CorrelationID string
	// ErrorClass classifies the failure. Set for retry scheduled,
	// dead-letter failed, poison rejected and connection lost; empty
	// otherwise.
	ErrorClass ErrorClass
	// DeliveryCount counts broker deliveries. Set for delivery received,
	// retry scheduled, dead-letter kinds and poison rejected; zero otherwise.
	DeliveryCount int
	// EnqueuedAt is the broker enqueue time. Set for delivery received, retry
	// scheduled, dead-letter kinds and poison rejected when known; zero
	// otherwise.
	EnqueuedAt time.Time
	// EnqueuedAtSource names the EnqueuedAt source. Set for delivery received,
	// retry scheduled, dead-letter kinds and poison rejected when EnqueuedAt is
	// known; for backlog sampled, it names the head source when HeadAgeKnown.
	// Unknown is reported otherwise.
	EnqueuedAtSource EnqueuedAtSource
	// NextAttempt is the scheduled attempt. Set for retry scheduled; zero
	// otherwise.
	NextAttempt int
	// MaxAttempts caps the retry ladder. Set for retry scheduled; zero
	// otherwise.
	MaxAttempts int
	// Backoff delays the next attempt. Set for retry scheduled; zero
	// otherwise.
	Backoff time.Duration
	// Reason identifies why a message was dead-lettered. Set for dead-letter
	// decided, dead-letter published, dead-letter failed and poison rejected;
	// empty otherwise.
	Reason DeathReason
	// DeadLetterDestination names the dead-letter destination. Set for
	// dead-letter decided, dead-letter published and dead-letter failed; empty
	// otherwise.
	DeadLetterDestination string
	// Backlog counts lagged messages. Set for backlog sampled; zero otherwise.
	Backlog int64
	// HeadAge is the oldest waiting age. Set for backlog sampled when known;
	// zero otherwise. See HeadAgeKnown.
	HeadAge time.Duration
	// HeadAgeKnown distinguishes a known zero age from an unknown age. Set
	// for backlog sampled; false otherwise.
	HeadAgeKnown bool
	// Downtime is the elapsed outage. Set for connection restored; zero
	// otherwise.
	Downtime time.Duration
	// DriverName names the driver. Set for driver selected; empty otherwise.
	DriverName string
	// SDKVersion is the SDK release in use, from the build information, or
	// "(devel)" when the build records none. Set for driver selected; empty
	// otherwise.
	SDKVersion string
	// ServerAddress names the broker host. Set for connection lost, connection
	// restored and driver selected when known; empty otherwise.
	ServerAddress string
	// ServerPort numbers the broker port. Set for connection lost, connection
	// restored and driver selected when known; zero otherwise.
	ServerPort int
	// LaneWait measures scheduler wait for the promoted head. Set for
	// deadline promoted; zero otherwise.
	LaneWait time.Duration
	// LaneDepth counts the lane depth at the pick. Set for deadline promoted;
	// zero otherwise.
	LaneDepth int
	// Suppressed counts deadline promotions the per-lane rate limit dropped
	// since the previous event for that lane. Set for deadline promoted; zero
	// otherwise.
	Suppressed int
}
