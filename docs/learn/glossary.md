# F1 glossary

F1 gives familiar messaging words precise meanings at the application boundary.

[A](#at-least-once-delivery) [B](#backlog-lag) [C](#capability-report) [D](#deadline-promotion) [E](#emulated) [F](#fairness-weight) [H](#handler-result) [I](#idempotency-key) [L](#lane) [N](#native) [O](#ordered-by-key-mode) [P](#parking-queue) [R](#rebalance-timeout) [S](#settlement) [T](#terminal-error) [U](#unavailable)

### At-least-once delivery

At-least-once delivery means F1 may deliver the same message again; unlike a retry, a redelivery can return the original without F1 publishing a copy.

See [Delivery identity and redelivery](/basics/message#delivery-identity-and-redelivery).

### Backlog lag

Backlog lag is the queued message count reported by a driver. It does not require a broker enqueue timestamp.

See [Enqueue-time backlog and oldest age](/advanced-topics/observability#use-enqueue-timestamps-correctly).

### Backstop

A backstop is the broker's own dead-letter queue, where the broker sends a message after its own delivery limit is reached without F1 deciding its outcome; unlike F1's normal dead-letter path, a message there is an operational alert.

See [RabbitMQ's own dead-letter queue](/advanced-topics/running-in-production#rabbitmq-s-own-dead-letter-queue).

### Broker enqueue time

Broker enqueue time is the trusted timestamp used to measure broker wait. It is unavailable when the broker does not supply it.

See [Use enqueue timestamps correctly](/advanced-topics/observability#use-enqueue-timestamps-correctly).

### Capability report

The capability report is the list of features the connected broker supports, returned by `Client.Limits()`; for each feature it says whether the broker does it, F1 does it, or it is unavailable.

See [Read the connected feature list](/advanced-topics/topology-and-capabilities#read-the-connected-feature-list).

### Close

`Client.Close` drains registered runners and accepted publishing work, then closes the producer and driver connection; unlike a drain, close releases client resources and is the process-boundary operation.

See [Close in ack-last order](/advanced-topics/lifecycle-and-shutdown#close-in-ack-last-order).

### Codec

A codec encodes outbound payloads and decodes inbound payloads selected by content type.

See [Publish a message](/basics/message#publish-a-message).

### Concurrency

Concurrency is the number of handler workers. Raising it cannot bypass a lower broker or partition limit.

See [Configure execution capacity](/advanced-topics/ordering-and-scheduling#configure-execution-capacity).

### Connection epoch

A connection epoch is the number of the client's current broker connection, 1 for the first and one higher after each reconnect; work records the epoch it started on and is refused once the number moves, so nothing acts on a replaced connection. Unlike Kafka's leader epoch, it counts F1's own connections, not partition leadership.

See [Connection numbers reject stale work](/deep-dives/reconnect-and-generations#connection-numbers-reject-stale-work).

### Consumer group

A consumer group is the driver ownership scope identified by a subscription name.

See [Declare and run a subscription](/basics/pubsub#declare-and-run-a-subscription).

### Consumer timeout

A consumer timeout is RabbitMQ's queue setting that cancels a consumer holding one delivery too long. Its argument is fixed when the queue is created.

See [RabbitMQ options](/drivers/rabbitmq#rabbitmq-options).

### Deadline promotion

Deadline promotion lets a lane jump the queue once its oldest message has waited at least the lane's wait limit; `DisableDeadlinePromotion` turns it off, and its zero value leaves it on.

See [Jumping the queue when overdue](/deep-dives/scheduler#jumping-the-queue-when-overdue).

### Dead-letter destination

A dead-letter destination stores a confirmed copy of a message that F1 cannot or should not retry; unlike a live destination, the copy records a death reason and the last error.

See [Dead letters and discarded events](/advanced-topics/failure-handling#dead-letters-and-discarded-events).

### Dead-letter successor

A dead-letter successor is the dead-letter copy F1 publishes, and waits to be confirmed, before it acks the original delivery for the `max_attempts`, `terminal`, `panic`, `decode`, `expired`, `poison`, or `unmatched` death reason; if that publish fails, the original remains available for the failure path.

See [Follow a failed delivery](/advanced-topics/failure-handling#follow-a-failed-delivery).

### Delivery attempt

A delivery attempt is the one-based attempt number carried in the envelope. Retry copies increment it; broker redeliveries do not. The configured maximum includes the first attempt.

See [Delivery identity and redelivery](/basics/message#delivery-identity-and-redelivery).

### Delivery count

Delivery count is broker delivery history, or `-1` when unavailable. RabbitMQ acquisition history can include messages fetched but never handled; it is not a handler-invocation or business-effect count.

See [RabbitMQ delivery counts](/drivers/rabbitmq#delivery-counts).

### Discarded event

A discarded event is acknowledged without applying its effect or keeping a copy in the broker: a handler returned `Drop`, or no handler matched and the unmatched policy is `Ignore`. F1 passes a copy of the envelope and body, with the reason `dropped` or `unmatched`, to the subscription's `OnDiscarded` callback; unlike a dead-lettered message, nothing is published to a dead-letter destination.

See [Dead letters and discarded events](/advanced-topics/failure-handling#dead-letters-and-discarded-events).

### Drain

A drain stops new intake and lets accepted deliveries finish and be acked within the lifecycle time limits; unlike close, `Runner.Drain` leaves the client and its driver resources available.

See [Run and drain a subscription](/advanced-topics/lifecycle-and-shutdown#run-and-drain-a-subscription).

### Drop

`Drop` marks a handler error as handled without another attempt and without retaining a copy; unlike a terminal error, it does not send the delivery through the dead-letter path.

See [Choose the outcome](/advanced-topics/failure-handling#choose-the-outcome).

### Emulated

An emulated capability is a feature F1 provides without requiring the broker to implement it.

See [Read the connected feature list](/advanced-topics/topology-and-capabilities#read-the-connected-feature-list).

### Envelope

An envelope is F1's standard message metadata, sent as headers separately from the payload body.

See [Metadata and the envelope](/basics/message#metadata-and-the-envelope).

### Event ID

An event ID identifies one published event instance; unlike an idempotency key, it does not identify a business effect across deliveries.

See [Publish a message](/basics/message#publish-a-message).

### Fairness weight

A fairness weight gives a lane group a relative share of scheduling opportunities. It does not impose strict priority.

See [Use priorities as fair lanes](/advanced-topics/ordering-and-scheduling#use-priorities-as-fair-lanes).

### Free scaling

Free scaling means the driver's parallelism is not limited by assigned partitions.

See [Kafka parallelism and partitions](/drivers/kafka#kafka-parallelism-and-partitions).

### Handler result

A handler result is the error or nil the handler returns, which F1 turns into the delivery's outcome; unlike [settlement](#settlement), it is not itself the ack or nack of the original delivery.

See [Handler results decide the ack](/basics/pubsub#handler-results-decide-the-ack).

### Header size cap

A header size cap bounds encoded envelope metadata. Optional extensions are shed before mandatory fields; encoding fails if the remainder is still too large.

See [Metadata and the envelope](/basics/message#metadata-and-the-envelope).

### Idempotency key

An idempotency key identifies the business effect that may be deduplicated across deliveries. The producer can supply it; otherwise it defaults to the event ID.

See [Publish a message](/basics/message#publish-a-message).

### Lane

A lane is a bounded queue inside F1, chosen by topic, priority, and retry step.

See [Use priorities as fair lanes](/advanced-topics/ordering-and-scheduling#use-priorities-as-fair-lanes).

### Lane budget

A lane budget is the time its oldest message waits inside F1 before the lane counts as overdue. It triggers promotion eligibility, not a maximum-wait guarantee.

See [Configure execution capacity](/advanced-topics/ordering-and-scheduling#configure-execution-capacity).

### Logical topic

A logical topic is the topic name your code uses, derived from the event type or set explicitly; unlike a physical destination, it is not the broker's queue or topic name.

See [Understand generated resources](/advanced-topics/topology-and-capabilities#understand-generated-resources).

### Native

A native capability is a feature the connected broker implements.

See [Read the connected feature list](/advanced-topics/topology-and-capabilities#read-the-connected-feature-list).

### Ordered-by-key mode

Ordered-by-key mode handles equal keys one at a time while allowing different keys to run concurrently; unlike global ordering, it does not serialize unrelated keys.

See [Choose the ordering guarantee](/advanced-topics/ordering-and-scheduling#choose-the-ordering-guarantee).

### Parking queue

A parking queue is a RabbitMQ delay queue whose messages each expire after their destination's delay and dead-letter back to the destination; F1 uses it because RabbitMQ has no built-in delayed delivery.

See [RabbitMQ retry parking](/deep-dives/rabbitmq-delay-ladder).

### Partition-bound scaling

Partition-bound scaling limits a member's parallelism by its assigned partitions; unlike free scaling, raising concurrency or prefetch cannot create another delivery without another partition.

See [Kafka parallelism and partitions](/drivers/kafka#kafka-parallelism-and-partitions).

### Partition key

A partition key keeps related events together for driver routing and per-key ordering. F1 carries it separately from free-form headers.

See [Publish a stable key](/advanced-topics/ordering-and-scheduling#publish-a-stable-key).

### Physical destination

A physical destination is the broker queue or topic name F1 builds from the topic, priority, retry policy, and subscription. Applications should not build this name themselves.

See [Understand generated resources](/advanced-topics/topology-and-capabilities#understand-generated-resources).

### Poison message

A poison message has an attempt count more than 10 above the effective maximum, so F1 gives it the death reason `poison`.

An undecodable body is assigned death reason `decode`, not `poison`.

See [Dead letters and discarded events](/advanced-topics/failure-handling#dead-letters-and-discarded-events).

### Prefetch

Prefetch is the total SDK-admitted unsettled window across a subscription's destinations, including work waiting for or running in a handler; unlike transport buffering, it is bounded by both the subscription total and destination windows.

See [Configure execution capacity](/advanced-topics/ordering-and-scheduling#configure-execution-capacity).

### Prefetch factor

The prefetch factor scales the bounded capacity assigned to each scheduling lane from its weighted share of concurrency. It cannot bypass worker, broker, or partition limits.

See [Use priorities as fair lanes](/advanced-topics/ordering-and-scheduling#use-priorities-as-fair-lanes).

### Priority lane

A priority lane is the high, medium, or low scheduling lane selected by publish metadata. Its weight controls a fair share of scheduling opportunities.

See [Use priorities as fair lanes](/advanced-topics/ordering-and-scheduling#use-priorities-as-fair-lanes).

### Rebalance timeout

A rebalance timeout is Kafka's coordinator window for completing partition revocation and reassignment.

See [Kafka rebalancing](/drivers/kafka#kafka-rebalancing).

### Reconnect generation

A reconnect generation is a runner's current consumer, with its fetch, dispatch, and error-reading goroutines, tied to one [connection epoch](/learn/glossary#connection-epoch). Reconnect replaces it on the existing runner.

See [Reconnect behavior](/development/consume-flow#reconnect-behavior).

### Redelivery

A redelivery returns the original message from the broker; unlike a retry, F1 does not publish a new copy.

See [Delivery identity and redelivery](/basics/message#delivery-identity-and-redelivery).

### Retry

A retry publishes a copy of the message to a retry destination, waits for the broker to confirm it, and then acks the original; unlike a redelivery, the copy is a new message published by F1.

See [Follow a failed delivery](/advanced-topics/failure-handling#follow-a-failed-delivery).

### Retry ladder

A retry ladder is the list of retry delays: the subscription policy that combines an attempt limit with the delays between handler attempts.

See [Configure the retry delays](/advanced-topics/failure-handling#configure-the-retry-delays).

### Retry tier

A retry tier is one retry step: one delay in the list of retry delays, with its own retry destination; RabbitMQ parks messages for that destination in its parking queue.

See [Kafka retry timing](/drivers/kafka#kafka-retry-timing).

### Settlement

Settlement is finishing a message: the final ack or nack of the original delivery; unlike a handler result, it happens after F1 has classified that result and any retry or dead-letter copy is confirmed.

See [Handler results decide the ack](/basics/pubsub#handler-results-decide-the-ack).

### Successor publish

A successor publish is publishing the retry or dead-letter copy of the current delivery; F1 waits for the copy to be confirmed before it acks the original, so a failed publish leaves the original to be delivered again.

See [Follow a failed delivery](/advanced-topics/failure-handling#follow-a-failed-delivery).

### Terminal error

A terminal error marks a permanent failure and skips retries; F1 sends the delivery through the dead-letter path when it can keep a copy.

See [Choose the outcome](/advanced-topics/failure-handling#choose-the-outcome).

### Topology drift

Topology drift means a queue or topic on the broker has different settings from what F1 expects, found when F1 verifies it; F1 reports the mismatch instead of silently treating the resource as correct.

See [Understand generated resources](/advanced-topics/topology-and-capabilities#understand-generated-resources).

### Topology policy

A topology policy selects whether F1 declares missing resources, verifies existing resources, or assumes resources already exist; the three policies are `TopologyDeclare`, `TopologyVerify`, and `TopologyNone`.

See [Select a topology policy](/advanced-topics/topology-and-capabilities#select-a-topology-policy).

### Unavailable

An unavailable capability means the connected client cannot provide the requested behavior. An operation that depends on it can be rejected instead of silently weakening its contract.

See [Read the connected feature list](/advanced-topics/topology-and-capabilities#read-the-connected-feature-list).

### Unmatched policy

An unmatched policy controls an event with no matching handler; unlike `DeadLetter`, `Ignore` acknowledges it without a retained copy.

See [Dead letters and discarded events](/advanced-topics/failure-handling#dead-letters-and-discarded-events).

## Go further

- [Message](/basics/message) - envelope, identity, and payload behavior.
- [Failure handling](/advanced-topics/failure-handling) - how handler results become retries and dead letters.
- [Ordering and scheduling](/advanced-topics/ordering-and-scheduling) - lanes, fairness, and capacity.
- [Topology and capabilities](/advanced-topics/topology-and-capabilities) - resource policy and connected limits.
