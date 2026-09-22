package f1

import (
	"context"
	"time"
)

// Observer receives typed lifecycle events from a Client. This is a draft
// API: it lands with its fixture and baseline recorded but without owner
// approval, so nothing about it is final.
//
// Concurrency: every method may be called concurrently from any goroutine,
// for different messages, across all subscriptions of one client and from
// concurrent publishers. Implementations must be safe for concurrent use and
// must not use goroutine-local state.
//
// Ordering: for one message a Start call happens before its own Finish and
// nothing else is ordered. A Finish for one message may run before or
// concurrently with a Start for another, and a Start and its Finish may run
// on different goroutines.
//
// Non-blocking: F1 calls the observer synchronously on the message path, so
// a method must not block. A slow method stalls intake, dispatch,
// settlement, successor publication, drain or the dispatch loop depending on
// the event, and a stall on the settlement path spends the broker consumer
// liveness window. F1 applies no timeout because a watchdog would cost a
// goroutine or timer per message.
//
// Panic containment: a panic in any method is recovered, logged once per
// kind through the client logger, and the message outcome is unchanged. A
// recovered panic is an observability defect, not a delivery failure.
//
// Context: the context returned by Start of ObserverProcess reaches the
// handler. Nothing else is promised: settlement and successor publication
// may run on a drain-time window with a different context, so cross-hop
// propagation travels on the message rather than on the context.
//
// Locking: no method is called while F1 holds a client or runner lock, so an
// implementation may call back into the client. Calling the observer while
// holding a core lock would be a defect in the core.
//
// Nil observer: a nil observer means zero cost. Every call site checks for
// nil before constructing an event, so nothing is emitted, nothing can
// panic, and no event struct is built.
//
// Payload lifetime: events pass by value. An implementation must not retain
// or mutate Results after the call returns; the core may reuse the storage.
//
// Timestamps: every timestamp on an event comes from the client clock. An
// implementation must never call time.Now and must derive durations from the
// event timestamps so a fake-clock test observes exact values.
//
// Cardinality: an implementation must not use a physical destination or a
// message identity as a metric dimension. The logical topic, subscription
// and consumer group are the low-cardinality dimensions.
//
// Error classes: the event carries a bounded ErrorClass value, never a raw
// error string or Go type name. Adapters map it to their own catalogue.
//
// Finish guarantee: every Start gets exactly one Finish. On abnormal exit
// the Finish carries ObserverOutcomeAbandoned: a panic past recovery,
// shutdown with work in flight, or a lost delivery. A stage may also have
// no Start at all when the message never reaches it.
//
// Unknown kinds and unknown fields are ignored: an implementation must not
// fail on a kind it does not know or a field it does not read.
type Observer interface {
	Start(context.Context, StartEvent) (context.Context, Token)
	Finish(Token, FinishEvent)
	Record(PointEvent)
}

// TraceInjector is an optional second interface an Observer may implement.
// New checks for it once by type assertion and stores the result on the
// Client next to the observer. After Start of ObserverMessageBuilt and of a
// republish ObserverPublish, the core calls InjectTrace with the returned
// context and writes the two strings into the envelope. Extraction needs no
// hook: the inbound values ride on StartEvent. Strings return by value so an
// observer that does not implement this interface costs nothing.
type TraceInjector interface {
	InjectTrace(ctx context.Context) (traceParent, traceState string)
}

// ObserverKind identifies one lifecycle stage or point event.
type ObserverKind string

const (
	// ObserverPublish is a paired kind. The publisher emits Start and Finish around
	// one PublishBatch call at admission and after the producer returns.
	ObserverPublish ObserverKind = "publish"
	// ObserverMessageBuilt is a paired kind. The publisher emits Start and Finish
	// around one outbound message build inside PublishBatch.
	ObserverMessageBuilt ObserverKind = "message_built"
	// ObserverProcess is a paired kind. The worker emits Start and Finish around
	// one handler invocation on a pool worker.
	ObserverProcess ObserverKind = "process"
	// ObserverSettle is a paired kind. The worker emits Start and Finish around
	// one acknowledgement or negative acknowledgement.
	ObserverSettle ObserverKind = "settle"
	// ObserverDrain is a paired kind. The runner emits Start and Finish around
	// one Runner Drain.
	ObserverDrain ObserverKind = "drain"
	// ObserverDeliveryReceived is a point kind. The worker emits it once per
	// delivery admitted to dispatch.
	ObserverDeliveryReceived ObserverKind = "delivery_received"
	// ObserverRetryScheduled is a point kind. The worker emits it when a retry
	// successor is confirmed.
	ObserverRetryScheduled ObserverKind = "retry_scheduled"
	// ObserverDeadLetterDecided is a point kind. The worker emits it when the
	// dead-letter decision is taken, before the confirm.
	ObserverDeadLetterDecided ObserverKind = "dead_letter_decided"
	// ObserverDeadLetterPublished is a point kind. The worker emits it when the
	// dead-letter publish is confirmed by the driver.
	ObserverDeadLetterPublished ObserverKind = "dead_letter_published"
	// ObserverDeadLetterFailed is a point kind. The worker emits it when the
	// dead-letter publish fails.
	ObserverDeadLetterFailed ObserverKind = "dead_letter_failed"
	// ObserverPoisonRejected is a point kind. The worker emits it when a decode,
	// poison, expired or unmatched delivery is dropped.
	ObserverPoisonRejected ObserverKind = "poison_rejected"
	// ObserverBacklogSampled is a point kind. The backlog poller emits it from the
	// backlog poll loop once per sampled destination.
	ObserverBacklogSampled ObserverKind = "backlog_sampled"
	// ObserverConnectionLost is a point kind. The reconnect supervisor emits it
	// when it observes a lost connection.
	ObserverConnectionLost ObserverKind = "connection_lost"
	// ObserverConnectionRestored is a point kind. The reconnect supervisor emits
	// it when it swaps in a new connection.
	ObserverConnectionRestored ObserverKind = "connection_restored"
	// ObserverDriverSelected is a point kind. The client emits it once per
	// client in New after the driver is opened.
	ObserverDriverSelected ObserverKind = "driver_selected"
	// ObserverDeadlinePromoted is a point kind. The worker emits it after the
	// scheduler promotes a lane head past its deadline, rate limited to one
	// event per lane per window. Suppressed carries the promotions the limit
	// dropped since the last recorded event.
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
	// ErrorClassPoison means the message repeatedly crashed its worker.
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

// PublishRoute identifies which route a publish takes.
type PublishRoute string

const (
	// PublishRoutePrimary means the application primary publish path.
	PublishRoutePrimary PublishRoute = "primary"
	// PublishRouteRetry means the internal retry successor publish path.
	PublishRouteRetry PublishRoute = "retry"
	// PublishRouteDeadLetter means the internal dead-letter publish path.
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
	// InFlight counts deliveries in flight. Set for drain starts and finishes.
	InFlight int
	// Queued counts deliveries queued behind the drain. Set for drain starts and finishes.
	Queued int
	// Drained counts deliveries drained. Set for drain finishes.
	Drained int
	// Remaining counts deliveries still held after the drain. Set for drain finishes.
	Remaining int
	// Timeout bounds the drain. Set for drain starts and finishes when a bound applies.
	Timeout time.Duration
}

// StartEvent describes the beginning of an observer stage. All structs pass
// by value with no pointers and no maps. A field a kind does not set holds
// its zero value.
type StartEvent struct {
	// Kind identifies the stage. Set for every Start kind.
	Kind ObserverKind
	// At is the client clock time. Set for every Start kind.
	At time.Time
	// Topic is the logical topic. Set for message built, process and settle.
	// Primary publish Start is emitted before message resolution and leaves
	// this zero; drain and starts without a message also leave it zero.
	Topic string
	// Subscription names the subscription. Set for process and settle; zero
	// for publish, message built and drain.
	Subscription string
	// ConsumerGroup names the consumer group. Set for process and settle;
	// zero otherwise.
	ConsumerGroup string
	// EventType is the event type. Set for publish, message built, process
	// and settle; zero for drain.
	EventType string
	// Priority is the delivery lane. Set for message built, process and settle.
	// Primary publish Start is emitted before message resolution and leaves
	// this zero; drain and starts without a message also leave it zero.
	Priority Priority
	// Attempt is the one-based attempt. Set for process and settle; zero for
	// publish, message built and drain.
	Attempt int
	// Destination is the physical destination. Set for publish, message
	// built, process and settle; zero for drain.
	Destination string
	// MessageID is the envelope ID. Set for publish, message built, process
	// and settle; zero for drain.
	MessageID string
	// CorrelationID is the workflow correlation ID. Set for publish, message
	// built, process and settle; zero for drain.
	CorrelationID string
	// Route identifies the publish path. Set for publish; zero otherwise.
	Route PublishRoute
	// BatchSize counts messages in the call. Set for publish; zero otherwise.
	BatchSize int
	// Operation identifies the settlement. Set for settle; zero otherwise.
	Operation SettleOperation
	// DeliveryCount counts broker deliveries. Set for process and settle;
	// zero otherwise.
	DeliveryCount int
	// EnqueuedAt is the broker enqueue time. Set for process when known;
	// zero otherwise.
	EnqueuedAt time.Time
	// EnqueuedAtSource names the EnqueuedAt source. Set for process when
	// EnqueuedAt is set; unknown otherwise.
	EnqueuedAtSource EnqueuedAtSource
	// TraceParent carries the inbound traceparent for extraction. Set for
	// process when the inbound headers carry it; empty otherwise.
	TraceParent string
	// TraceState carries the inbound tracestate for extraction. Set for
	// process when the inbound headers carry it; empty otherwise.
	TraceState string
	// Drain carries drain progress. Set for drain; zero otherwise.
	Drain DrainCounts
}

// FinishEvent describes the end of an observer stage. All structs pass by
// value with no maps and no pointers except Results. A field a kind does not
// set holds its zero value.
type FinishEvent struct {
	// Kind identifies the stage. Set for every Finish kind.
	Kind ObserverKind
	// At is the client clock time. Set for every Finish kind.
	At time.Time
	// Topic is the logical topic. Set for message built, process and settle;
	// set for publish when every message in the call resolves to the same topic;
	// zero for mixed calls, failures before all messages resolve and drain.
	Topic string
	// Subscription names the subscription. Set for process and settle; zero
	// otherwise.
	Subscription string
	// ConsumerGroup names the consumer group. Set for process and settle;
	// zero otherwise.
	ConsumerGroup string
	// EventType is the event type. Set for publish, message built, process
	// and settle; zero for drain.
	EventType string
	// Priority is the delivery lane. Set for message built, process and settle;
	// set for publish when every message in the call resolves to the same
	// priority; the zero value, PriorityMedium, for mixed calls, failures
	// before all messages resolve and drain.
	Priority Priority
	// Attempt is the one-based attempt. Set for process and settle; zero
	// otherwise.
	Attempt int
	// Destination is the physical destination. Set for publish, message
	// built, process and settle; zero for drain.
	Destination string
	// MessageID is the envelope ID. Set for publish, message built, process
	// and settle; zero for drain.
	MessageID string
	// CorrelationID is the workflow correlation ID. Set for publish, message
	// built, process and settle; zero for drain.
	CorrelationID string
	// Outcome identifies how the stage ended. Set for every Finish kind.
	Outcome ObserverOutcome
	// ErrorClass classifies the failure. Set when Outcome is error; empty
	// otherwise.
	ErrorClass ErrorClass
	// Terminal reports the settlement ends the delivery. Set for process;
	// false otherwise.
	Terminal bool
	// Results carries per-index publish outcomes. Set for publish; nil
	// otherwise. The implementation must not retain or mutate it.
	Results []MessageResult
	// Drain carries drain progress. Set for drain; zero otherwise.
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
	// destination is known to the consumer generation, including entries with
	// ambiguous priority; set for retry scheduled, dead-letter decided,
	// dead-letter published, dead-letter failed, poison rejected, backlog
	// sampled and deadline promoted; zero for unknown destinations, connection
	// lost, connection restored and driver selected.
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
	// value, PriorityMedium, for ambiguous or unknown destinations, connection
	// lost, connection restored and driver selected.
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
	// DriverVersion versions the driver. Set for driver selected; empty
	// otherwise.
	DriverVersion string
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
