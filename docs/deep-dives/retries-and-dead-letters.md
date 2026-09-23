# A retry copy travels before the original is acked

*By trungbk.*

A handler returns an ordinary error while processing `orders.created` attempt 1. A broker requeue would bring the message straight back, with no delay and no record that this was attempt 1. So I do not requeue it: F1 builds a new message that carries the next attempt and a due time, waits for the broker to store that copy, and only then [acks](/learn/glossary#settlement) the message that called the handler.

## Background

I treat a handler result as one of three requests. A handler that wraps its error with `f1.Terminal` returns a [terminal error](/learn/glossary#terminal-error): it goes to a dead-letter copy without another attempt. A handler that wraps it with `f1.Drop` [drops](/learn/glossary#drop) the message: F1 acks it without a copy and passes it to the subscription's `OnDiscarded` callback, if one is set. A panic goes to a dead-letter copy at once, like a terminal error.

Every other error asks for another attempt until the effective maximum is reached. The effective maximum is the smaller of the event's own attempt limit and the subscription's `Retry.MaxAttempts`, or the subscription's alone when the event sets none. The publisher sets the event's limit with the publish option `WithMaxAttempts`; an event published with `WithMaxAttempts(2)` to a subscription that allows four gets two attempts.

Attempts start at one and include the original delivery. A subscription that sets no retry policy allows four attempts, with nominal delays of 1 second, 5 seconds, and 25 seconds.

An explicit list of [retry steps](/learn/glossary#retry-tier) replaces those calculated delays. A handler can ask for a specific delay with [`RetryAfter`](/learn/glossary#retry-after), but F1 maps that duration to the nearest configured step instead of creating an arbitrary destination. A request halfway between two steps takes the shorter one, and a request outside the range takes the first or last step. The retry copy carries the next attempt, a due time, the original key, and the original body.

## Copy first, ack second

What matters is the order: publish the copy, then ack the source.

```mermaid
flowchart TB
    B[(broker)] --> I[deliver]
    I --> H([handler])
    H --> D{outcome}
    D -->|retry| R[retry copy]
    D -->|terminal or cap| L[dead letter]
    D -->|drop| A
    R --> P[publish copy]
    L --> P
    P --> B
    P --> C{confirmed}
    C -->|yes| A[ack original]
    A --> B
    C -->|no| X[release consumer]
    X -.-> B
```

F1 copies the envelope metadata, clears old death metadata, increments the attempt, chooses a retry step, and publishes with the computed due time. The body and message key are copied without applying `codec.maxBodyBytes`, the body limit for application publishes, again. The broker's own size limit still applies, and a copy it rejects as too large takes the dead-letter path below. The broker has already accepted those bytes once; rejecting them while making the copy would create a new way to lose a message.

A confirmed retry copy is followed by an ack of the source. A confirmed dead-letter copy follows the same order. If the process stops after confirmation but before the ack, the broker can redeliver the source while the copy also exists. That is an intentional at-least-once duplicate, so the handler still needs an idempotent effect. The [life of a delivery](/deep-dives/life-of-a-delivery) page shows the same boundary in the wider delivery lifecycle.

## One message through the retry steps

This trace uses source `/prod/orders`, topic family `orders.created`, subscription `orders`, and medium priority. The handler returns an ordinary retryable error on every attempt.

| Current attempt | Handler result | Retry step and delay | Copy and destination | Original |
| ---: | --- | --- | --- | --- |
| 1 | ordinary error | 1, 1 s | attempt 2, `f1.prod.orders.created.orders.medium.retry.1` | Ack original |
| 2 | ordinary error | 2, 5 s | attempt 3, `f1.prod.orders.created.orders.medium.retry.2` | Ack original |
| 3 | ordinary error | 3, 25 s | attempt 4, `f1.prod.orders.created.orders.medium.retry.3` | Ack original |
| 4 | ordinary error | cap reached | DLQ copy keeps attempt 4, `f1.prod.orders.created.dlq.orders` | Ack original |

The retry step is picked from the current attempt, while the copy gets the next attempt number. At attempt 3, for example, the current message picks step 3 and the copy is labeled attempt 4. The fourth delivery has no retry step left, so the same ordinary error becomes `max_attempts` and takes the dead-letter path.

A smaller event-level maximum shortens this trace. With `MaxAttempts` set to 2, attempt 1 uses step 1 and attempt 2 goes straight to the dead-letter destination. A policy with no retry step also dead-letters immediately. The policy validator limits the configured maximum to 1 through 20 attempts.

## When the copy cannot be stored

F1 gives publishing the copy a few quick tries, and all of them must fit within `Lifecycle.DrainTimeout`, the time allowed for acking a message; when that time runs out, F1 stops trying. A destination that is briefly unavailable is not treated as a handler failure. When the tries run out, F1 reports the error, releases the consumer, and leaves the original unacked so the broker can deliver it again. Releasing is not a nack and does not create another retry copy.

An intrinsically bad copy follows a different path. If the copy cannot be encoded, or the broker rejects it as too large, trying again cannot make that same copy acceptable. F1 moves into dead-letter handling instead of stopping the subscription.

If the dead-letter copy can never be published either, F1 reports the drop and then acks the original. A poison message whose dead-letter route is missing is contained the same way. The report goes to the observer as a dead-letter failure event and to the `WithErrorHandler` callback with the event and the publish error; without an error handler, the client logger records it.

A retry-counter runaway is another poison case. When an envelope's attempt is greater than the effective maximum plus ten, F1 assigns the `poison` reason and dead-letters it. With a maximum of four, attempt 15 is the first that counts as poison. This protects the worker from a corrupted or malicious attempt counter without confusing the case with a handler that simply failed many times.

## Naming and death reasons

The topic in retry and dead-letter destination names is the topic that owns the destination the message arrived on, when F1 can match it to a declared destination. If no declared destination matches, F1 falls back to the event type, then to `unknown` for a malformed message. The priority in a retry name is the message's own priority.

A dead-letter destination has the shape `f1.<environment>.<topic>.dlq.<subscription>`. A retry destination has the shape `f1.<environment>.<topic>.<subscription>.<priority>.retry.<step>`. In the trace below, environment `prod` comes from the source `/prod/orders`, the topic is `orders.created`, the subscription is `orders`, and the priority is `medium`, so the first retry step is `f1.prod.orders.created.orders.medium.retry.1`.

Each dead-letter copy records one reason:

| Reason | When |
| --- | --- |
| `max_attempts` | The handler failed on the last allowed attempt. |
| `terminal` | The handler returned an error wrapped with `f1.Terminal`. |
| `panic` | The handler panicked. |
| `decode` | The envelope or body could not be decoded. |
| `expired` | The event's expiry passed before a handler ran. |
| `poison` | The attempt counter ran away, as above. |
| `unmatched` | No handler matched the event type and the subscription dead-letters unmatched events. |
 A normal dead-letter copy carries the reason, the error text cut to a fixed size, and the time it was dead-lettered. Terminal and max-attempt copies also carry selected structured error details. A message whose envelope cannot be decoded uses the raw headers and body so the dead-letter path can still preserve what arrived.

## Limits and trade-offs

- The retry delay is the chosen step's delay, not a promise of exact delivery time. Broker and driver delay behavior still controls when the copy returns.
- A publish confirmation followed by a process crash can leave both the source and its copy to be handled.
- A copy that temporarily fails to publish gives the original back to the broker; an original whose copy can never be published is deliberately acked after a visible drop.
- The dead-letter destination is derived from the consuming family, so a topic override is not silently confused with the event type.

## Go further

- [Failure handling](/advanced-topics/failure-handling) - public retry and dead-letter policy.
- [Life of a delivery](/deep-dives/life-of-a-delivery) - the whole path from delivery to ack.
- [RabbitMQ retry parking](/deep-dives/rabbitmq-delay-ladder) - how the RabbitMQ driver parks retry copies.
- [Consume flow](/development/consume-flow) - the maintainer trace for dispatch and acking.
- [Source-reading guide](/development/source-reading-guide#code-behind-the-deep-dives) - where this lives in the code.
