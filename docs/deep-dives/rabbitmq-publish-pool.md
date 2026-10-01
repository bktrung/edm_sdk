# Shared confirm channels let RabbitMQ publishes overlap

*By trungbk.*

Retry and dead-letter copies need confirmation before F1 acks the original.
Acking first would leave a loss window if the copy never reached the broker.
A crash between confirmation and ack can leave both copies available,
which is the intentional at-least-once trade-off. [Consume flow](/development/consume-flow)
maps the code that creates those copies and finishes the original.

Waiting for that confirmation should not prevent unrelated calls from
publishing on the same channel. Application publishes and retry or dead-letter
copies share the producer, so exclusive channel ownership through a confirm
wait would make them compete for a fixed number of in-flight calls.

## Confirmations belong to messages

Per-message confirmation handles let each caller wait for its own broker
answer while other calls use the same channel. Holding a channel only to
wait would turn broker confirmation latency into a concurrency ceiling,
even while the channel could accept more writes.

Order within a call still matters. Waiting for one batch segment's outcomes
before sending the next prevents a later segment from overtaking an
undecided earlier one, even when the next uses another channel. Bounded
segments also avoid encoding an entire large batch at once. Concurrent calls
have no relative ordering guarantee.

The [producer](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/producer.go)
is the executable owner for channel selection, segment bounds, confirmation
decisions, and recovery. These bounds are implementation choices, not
deployment settings.

## Returns need message IDs

A mandatory publish that cannot be routed comes back as `basic.return`.
Unlike a confirmation, the return has no delivery tag. Its message ID is the
correlation key, so two unresolved messages with the same ID cannot share a
channel. Retry and dead-letter copies preserve identity, so concurrent
redeliveries can publish copies with the same ID. An ID reservation keeps
each return associated with one unresolved publish on that channel.

The [return watcher](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/publish_watcher.go)
owns matching and reservations. The design relies on the broker/client
return-before-confirm contract; this is a design basis, not a live measurement
reported here. That ordering alone is not enough: a return can still be
buffered when the confirmation wakes the caller. Reading the queued return
before answering the caller prevents an unroutable message from being
reported as published.

## Cancellation leaves the channel usable

Cancelling one call does not make another call's confirmations ambiguous:
each has its own confirmation handles. Closing the shared channel on
cancellation would disrupt unrelated callers.

Suppose call A publishes message X, then its context ends before confirmation.
If cancellation released X's ID immediately, call B could reserve that ID
and publish its own copy. A late return for A's X would then be assigned to B,
making a routed copy appear unroutable.

The reservation therefore outlives the cancelled call until its confirmation
resolves, either through a broker answer or channel closure. The
return-before-confirm contract and buffered-return handling make that the
safe release boundary. A caller needing the same ID must wait or use another
channel; unrelated callers can continue on the shared channel.

## Channel failures can affect neighbours

Sharing a channel also shares its failure boundary. A size refusal or a
publish to a missing exchange can close the channel while other callers
still await answers. The close reason can distinguish a refused message
from an unrelated message whose confirmation was interrupted.

Recovery must preserve order. If a call writes A then B, loses A's
confirmation, but receives B's confirmation before the close, republishing A
would place it behind B. A missing confirmation is not permission to retry
every message on the channel.

Automatic recovery is safe only when the close identifies a refusal unrelated
to the call's messages and its undecided messages form a trailing group,
with no later confirmed message ahead of a republished one. A close that
does not identify the cause cannot support that distinction. Recovery is
bounded, and it can still duplicate a message the broker accepted before the
close; at-least-once delivery permits that uncertainty, not reordering.

Connection-wide publisher alarms affect every channel. Waiting for an alarm
to clear avoids turning a temporary block into a retry storm. The producer
owns these waits and their cancellation boundaries.

## Go further

- [RabbitMQ driver](/drivers/rabbitmq#publishing-shares-a-few-confirm-channels) - deployment guidance and publish ownership.
- [Benchmarks](/development/benchmarks) - workload-specific measurements.
- [Publish flow](/development/publish-flow) - core publish ownership.
- [Retries and dead letters](/deep-dives/retries-and-dead-letters) - why copies must be confirmed before the original is acked.
