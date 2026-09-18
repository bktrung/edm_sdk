# Parking retries without head-of-line delay: the RabbitMQ ladder

*By trungbk, September 2026.*

A retry in F1 is a new copy of a failed message with a due time, the moment before which no handler may see it. On RabbitMQ that copy has to wait somewhere until then, and RabbitMQ has no delayed delivery of its own. The RabbitMQ driver, F1's adapter for that broker, builds the wait out of ordinary queues.

A single parking queue with a per-message expiry created unbounded head-of-line delay: a message due soon could wait behind one due later. The current design uses a ladder of queues, one per fixed delay, and trades that defect for a bounded amount of lateness. It does not guarantee absolute due-time order for messages published at different times.

## Background

The driver reports no native delay (`NativeDelay: false` in [`Capabilities`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/rabbitmq.go)), so it builds one from two RabbitMQ features. A delayed message is published to a parking queue that sits beside its destination, the queue F1 would otherwise publish it to. The message waits there until its time to live (TTL) runs out. RabbitMQ then dead-letters it: it republishes the expired message to the exchange the parking queue names for that purpose. An exchange is the routing step every RabbitMQ publish passes through on its way to a queue.

[`parkingArguments`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/topology.go) points each parking queue at the default exchange, the one with the empty name, which delivers a message to the queue named by its routing key. The dead-letter routing key is the destination's name, so an expired message lands in the destination queue, where the subscription's consumers pick it up like any other.

```mermaid
flowchart LR
  Producer["Producer"] -->|"publish"| Park["Parking queue<br/>orders.park.2s"]
  Park -->|"TTL expires<br/>dead-letter"| DefaultEx["Default exchange<br/>(nameless)"]
  DefaultEx -->|"routing key<br/>orders"| Dest["Destination queue<br/>orders"]
```

In F1 today, every delay comes from the retry path: when a handler fails, F1 publishes a retry copy with a due time. The driver port accepts a due time on any outbound message; F1's public `Publish` never sets one. At publish time, [`target`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/producer.go) subtracts now from the due time and picks a parking queue from what remains. A message sent without a due time to a deferred destination, one declared with a fixed delay, gets that destination's delay.

Every publish from the driver is mandatory and uses publisher confirms. Mandatory means RabbitMQ hands back a message it cannot route to any queue instead of dropping it. Publisher confirms means RabbitMQ acknowledges each publish once it has taken responsibility for it. An unroutable message comes back first as a return, then as a confirm, and that return matters later, when a parking queue is missing.

The full reference for parking queues and their arguments is in [Drivers and capabilities](/drivers-and-capabilities).

## How it works

The first version, commit `2546ff5`, had one parking queue per destination. For `orders` that was `orders.park`. Every delayed message for `orders` went there with a per-message expiration, a TTL set on the message itself, equal to its remaining delay.

The defect is in how RabbitMQ handles that kind of expiry. It expires a per-message TTL only when the message reaches the head of its queue. A message with a short TTL that sits behind one with a long TTL cannot leave until the one in front of it has.

Commit `8e080c1` replaced the single queue with a ladder. A deferred destination on the ladder path has eight parking queues, called rungs, one per fixed delay:

| Rung | 500ms | 1s | 2s | 4s | 8s | 16s | 32s | 64s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |

For `orders` they are `orders.park.500ms`, `orders.park.1s`, and so on up to `orders.park.64s`. [`rungParkingArguments`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/topology.go) declares each rung queue with a queue-level TTL, `x-message-ttl`, equal to its rung. Messages parked there carry no expiration of their own.

That removes the unbounded head-of-line defect. Every message in a rung queue has the same TTL, so the queue expires messages in enqueue order instead of comparing different per-message TTLs. The ladder still does not guarantee absolute due-time order across messages published at different times: each message is assigned a rung from its remaining delay when it is published.

[`parkRung`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/topology.go) rounds the remaining delay up to the smallest rung at least as large. Rounding up ensures a message is never released early, because its rung is at least its remaining delay at publish time.

Back to the running example. "far", 3 s away, goes to `orders.park.4s`, and "near", 0.5 s away, goes to `orders.park.500ms`. They are in different queues now, so "near" leaves at about 0.5 s and "far" at about 4 s.

The cost on the ladder path is lateness of less than one rung. A 5 s delay parks in the 8 s rung and comes out at about 8 s, 3 s late. A 25 s delay parks in the 32 s rung and comes out about 7 s late. Fixed-delay retry tiers skip this cost; see Fixed-delay retry tiers below.

The ladder covers arbitrary-delay destinations up to 64 s with a rung to spare. Core retry tiers take the fixed-queue path below instead.

## Measured

[`TestDeferredDueOrderSurvivesReversedPublishOrder`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/deferred_integration_test.go) pins the fix. It publishes "far" due in 2 s and then "near" due in 500 ms through the driver, and reads arrivals straight from the destination queue. Neither message may arrive before its due time or more than 700 ms after it, and "near" must arrive first. The test runs only against a real broker, under the repository's `integration` build tag.

Reproduce it with:

```sh
make broker-up
F1_RABBITMQ_ENDPOINT=amqp://guest:guest@localhost:5672/ go test -count=1 -tags integration -run TestDeferredDueOrderSurvivesReversedPublishOrder ./drivers/rabbitmq/
```

Commit `4dd4f7e` made the driver declare that cost. It reports a `DelayAccuracy` that [`delayAccuracyForLadder`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/rabbitmq.go) computes from the rung table itself. The floor is the first rung, 500 ms, and the ceiling is the last, 64 s. The relative bound is the largest step between two neighbouring rungs as a fraction of the smaller one. Every rung doubles the one before, so `(rungs[i] - rungs[i-1]) / rungs[i-1]` is 1 across the whole ladder. Editing a rung changes the declaration with it, so the two cannot drift.

`Client.Limits()` renders the declaration for this driver as:

> `late by at most the requested delay, or 500ms, whichever is larger, for a delay of at most 1m4s; no bound above that`

Read plainly: a delay under 500 ms can be up to 500 ms late, a delay up to 64 s can be late by as much as the delay itself, and above 64 s nothing bounds the lateness.

## Above the ladder

A delay above 64 s still goes to `orders.park` with a per-message expiration. The comment in `target` gives the reason: rounding such a delay down into the top rung would release it early. The single-queue head-of-line defect therefore still applies above the ladder.

At the shipped defaults the retry path never gets there. Two things can. The configuration puts no upper limit on `MaxInterval`, so a service that raises it far enough produces retry delays above 64 s. And code that uses the driver directly can publish with a due time more than a minute out.

The longest per-message expiration [`expirationMillis`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/producer.go) sets is 2147483647 ms, about 24.8 days. A longer delay is cut to that.

## Names that parse one way

A rung queue's name is the destination, then `.park`, then a tag from a closed set: `500ms`, `1s`, `2s`, `4s`, `8s`, `16s`, `32s`, `64s`. The topology path that declares the queues and the publish path that routes to them build the names from the same helpers.

Reading a name back, in [`parkQueueParts`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/topology.go), is a lookup in that set. A name that merely has something after `.park.` is not a parking queue, so `orders.park.eligible` stays an ordinary destination. The conformance suite declares a destination called `topology.prune.park.eligible`, and a parser that split on `.park.` would take it for a rung queue of `topology.prune`.

F1 reserves destination names ending in `.park`, in `.park.<rung tag>`, or in `.park.fixed-<ms>ms`. The exact shapes live in [`parkQueueParts`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/topology.go).

## Losing a retry quietly

Parking queues take the connection's queue type, classic or quorum. Since commit `fb3b351`, [`parkingArguments`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/topology.go) declares a quorum parking queue with these arguments:

- `x-queue-type=quorum`
- `x-dead-letter-exchange=""`, the default exchange
- `x-dead-letter-routing-key=<destination>`
- `x-dead-letter-strategy=at-least-once`
- `x-overflow=reject-publish`

The code comment records why the last two are there. RabbitMQ dead-letters at least once only when the strategy, the overflow setting and the dead-letter exchange are all set. Remove any one of them and it downgrades to at-most-once, with no error from the broker. The parking queue is the delay mechanism itself, so a message lost on its way out is a retry dropped without a trace.

Classic queues do not support at-least-once dead-lettering. On a classic deployment the delay path stays at-most-once.

## When the parking queue is missing

A publish can reach the broker before its parking queue exists. Because every publish is mandatory, RabbitMQ returns the message with `312 NO_ROUTE`, a reply that does not say a queue is missing or how it should have been created.

Commit `d691990` made that failure readable. The driver matches each return to its message by message ID. When the message was bound for a parking queue, [`parkingFailure`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/producer.go) rewrites the error:

```text
rabbitmq: parking destination "orders.park.2s" is missing: a delayed or retried message is parked there until its delay expires, so the adapter declares the queue durable under TopologyDeclare and requires it under TopologyVerify, and under TopologyNone the operator provisions it with the declare arguments ...
```

The new message names the queue, says who creates it under each topology policy, and lists its declare arguments. Those arguments are rendered by [`parkArguments`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/topology.go), the same function that builds them for the declaration, so the text cannot drift from what the driver declares. The rewritten error keeps its original classification.

Topology policy decides who creates the parking queues. Under `TopologyDeclare` the driver declares them when the subscription starts. Under `TopologyVerify` they must already exist with matching arguments, checked at subscription start. Under `TopologyNone` an operator provisions them and the driver checks nothing.

## Fixed-delay retry tiers

Core retry tiers are fixed-delay destinations: every message published to one is due exactly its delay after publish. The driver parks such a destination in one queue whose TTL is the tier's delay, named `.park.fixed-<ms>ms`, plus the per-message queue above the ladder. So a 5s tier fires at 5s, not 8s. The ladder remains for destinations that take arbitrary due times. Old rung queues left from an earlier version show up as orphans and empty themselves within 64s.

## Limits and trade-offs

- A message on the ladder can be late by up to its own delay, or by up to 500 ms when the delay is shorter than that; a fixed-delay tier is late only by the time its publish took.
- Messages published at different times can arrive out of absolute due-time order, even though the ladder prevents the unbounded head-of-line delay of the single per-message-TTL queue.
- A deferred destination on the ladder path needs nine parking queues: eight rungs and one for delays above the ladder. A fixed-delay tier needs two parking queues (its fixed queue plus the above-ladder queue), so three queues with its tier queue. The per-spec set lives in `parkQueueNamesFor` in `drivers/rabbitmq/topology.go`.
- Above 64 s, a message can still wait behind one due later.
- On classic queues a dead-lettered message can be lost, so the delay path is at-most-once.

## Read the code

- [`drivers/rabbitmq/topology.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/topology.go) - `parkRungs` and its comment on why the ladder exists, then `parkRung`, `parkQueueParts`, `parkQueueNamesFor`, `parkingOf`, `existingParkQueues`, `parkingArguments`, and `rungParkingArguments`.
- [`drivers/rabbitmq/producer.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/producer.go) - `target` routes by remaining delay; `parkingFailure` rewrites the missing-queue error.
- [`drivers/rabbitmq/rabbitmq.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/rabbitmq.go) - `delayAccuracyForLadder` turns the rung table into the declared bound.
- [`drivers/rabbitmq/deferred_integration_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/deferred_integration_test.go) - the reversed-publish-order regression test.
- [Drivers and capabilities](/drivers-and-capabilities) - the reference for parking queues, their arguments, and topology policies.
