# Driver options

A driver accepts only the broker-specific keys listed on this page. The list is
checked when the configuration loads, and a key outside it is refused there by
name. Each driver has its own list: `broker.kafka.compression` is a Kafka
option and is refused on a RabbitMQ deployment.

Options live under the driver's name, and every value is a string in the
configuration file, whatever type the driver parses it as:

```yaml
f1:
  broker:
    driver: kafka
    endpoints:
      - localhost:19092
    kafka:
      compression: lz4
      batchLinger: 5ms
```

Two of the columns below need a word of explanation.

- **Accepted values** is what the driver takes, not what the broker supports. A
  value the driver cannot translate fails the driver's `Open`, and an `Open`
  failure is fatal, so a process never starts on a value that would silently do
  nothing. Three of the keys are also constrained earlier, by the checks that
  run when the configuration loads and when a subscription is created; those
  are in [Validation before the driver](#validation-before-the-driver).
- **Read at** says when the option takes effect. *Open* means it is resolved
  once, when the driver opens. *Per consumer* means it is resolved again for
  each consumer the driver creates. *Topology* means it is read on the path that
  declares or verifies destinations rather than at connection time.

## Kafka options

| Key | What it does | Accepted values | Default | Read at |
| --- | --- | --- | --- | --- |
| `broker.kafka.compression` | Codec the producer compresses record batches with. A named codec stands alone: the producer does not fall back to another codec. | `none`, `gzip`, `snappy`, `lz4`, or `zstd`, case-insensitive. Any other value fails `Open`. | `snappy`, falling back to no compression for a batch snappy does not shrink. | Open |
| `broker.kafka.batchLinger` | How long the producer waits for a batch to fill before sending it. `0` sends as soon as there is something to send. | Go duration, zero or positive. A negative duration fails `Open`. | `10ms` | Open |
| `broker.kafka.fetchMaxBytes` | Byte ceiling a consumer asks each broker for in one fetch. | Integer from 1 to 2147483647. Any other value fails `Open`. | `52428800` (50 MiB) | Open |
| `broker.kafka.sessionTimeout` | Session timeout the member carries in its join request; the group coordinator expires the member after that much silence. | Positive Go duration, and at least as long as `lifecycle.rebalanceDrainTimeout` divided by 0.6. | `45s` | Open |
| `broker.kafka.rebalanceTimeout` | Window the coordinator allows a member to complete a rebalance before removing it from the group. | Positive Go duration. | `60s` | Open |
| `broker.kafka.useShareGroups` | Selects the consume mode. Share-group mode is not implemented, so `auto` and `never` both consume with classic groups, and the only observable effect of this key is that `always` is refused. | `auto`, `always`, or `never`. | `auto` | Open |
| `broker.kafka.staticMembership` | Adds the configured instance ID to the join request, so a member that restarts inside its session timeout rejoins without a rebalance. It changes nothing when no instance ID is configured. | Boolean, in the spellings `1`, `t`, `T`, `TRUE`, `true`, `True` and their `0`, `f`, `F`, `FALSE`, `false`, `False` counterparts. Any other value fails `Open`. | `true` | Open, and per consumer |
| `broker.kafka.maxAckGap` | How far the committed offset may fall behind the highest acknowledged offset of one destination before the driver pauses that destination's partitions to let the commit catch up. | Positive integer. | `10000` | Per consumer |
| `broker.kafka.balancer` | Group balancer protocol the consumer clients join with. | `lane`, `cooperative-sticky`, `sticky`, or `range`. | `lane` | Open |
| `broker.kafka.maxExpectedInstances` | Partition floor: a destination with fewer partitions than this is refused, and a declaration that names no partition count gets this many instead. `0` leaves both decisions to the destination. | Non-negative integer. | `0`, no floor | Topology |

`broker.kafka.maxExpectedInstances` is a guard against a topic that was created
with fewer partitions than the service can use. It applies to the paths that
declare or verify destinations, and not to `TopologyNone`, where the driver
declares and checks nothing. Under `TopologyVerify` an existing topic below the
floor is a startup failure rather than a deployment that silently scales less
than expected, and under `TopologyDeclare` a request for fewer partitions than
the floor is refused before a topic is created.

A subscription's `Prefetch` is also the deferred hold limit for its
destinations, so it is the knob behind the ordering guarantee for a delayed
destination. The subscription's destinations share that budget, and a
destination's share is its limit: the destination keeps its fetches while it
holds fewer records waiting for a due time than `max(share, 2)`, which is what
lets a record behind a waiting one be read and delivered at its own due time.
Two is the floor, because one waiting record must never be enough to hold the
fetches: the record behind it can be due sooner, and holding there would make it
wait for a due time that is not its own.

At that limit the driver holds the destination's fetches, which bounds what it
spends on records it cannot deliver yet. A record behind the held ones cannot be
read until they have been delivered, so it arrives at the earliest held record's
due time rather than at its own, and a later-due record can be delivered before
a nearer one behind it. Raising a destination's share raises the limit and
narrows that window, and lowering it cannot narrow the window past two records,
which is the floor. Bounded memory is what the lateness buys.

## RabbitMQ options

| Key | What it does | Accepted values | Default | Read at |
| --- | --- | --- | --- | --- |
| `broker.rabbitmq.vhost` | Vhost the management API inspects when it reads queue arguments and bindings. It does not change the AMQP connection: the endpoint URI still selects the vhost that messages are published to. | Any string, used as the vhost name. Empty falls back to the endpoint URI. | The endpoint URI's path with a leading `/`: `/` when the URI has no path, and `/orders` for `amqp://host/orders`. | Open |
| `broker.rabbitmq.queueType` | Queue type every destination and its parking queue is declared with. `classic` also clears the delivery-count and dead-letter capabilities, both of which are quorum arguments. | `quorum` or `classic`, case-insensitive, with surrounding whitespace ignored. Any other value fails `Open`, and `env: prod` requires the exact string `quorum`. | `quorum` | Open |
| `broker.rabbitmq.consumerTimeout` | `x-consumer-timeout` declared on quorum destination queues. The broker cancels a consumer that has held one delivery this long. | Go duration of at least `1ms`, and at least three times every subscription's `handlerTimeout`. Shorter, zero, negative, or unparsable fails `Open`. | None: nothing is declared and the broker's own default stays in force. | Open |
| `broker.rabbitmq.managementPort` | Port the management HTTP API listens on. | Integer from 1 to 65535. Any other value fails `Open`. | The AMQP port plus 10000, so 15672 for the usual 5672. | Open |

`broker.rabbitmq.vhost` deserves the extra sentences, because the derived
default is not the vhost the AMQP connection uses once the endpoint names a
vhost other than the default. The AMQP client reads `amqp://host/orders` as the
vhost `orders`, while the management client reads the same URI as `/orders`: it
takes the URI's path with a leading slash. Management calls then address a vhost
of a different name, which usually exists nowhere, so the queues and bindings
the driver inspects belong to no visible vhost. The two readings agree when the
URI has no path, where both are `/`, and when the URI carries the vhost
percent-encoded with its own slash, `amqp://host/%2Forders` for the vhost
`/orders`. For any other named vhost, set this key to the vhost the endpoint
names, which is what the key is for.

## Validation before the driver

Three of the keys above are also constrained by the checks that run before any
driver parses them: when the configuration loads, and for a subscription, also
when it is created. Each refusal names the key.

- `broker.rabbitmq.queueType` must be exactly `quorum` when `env: prod` and the
  selected driver is `rabbitmq`. The comparison is on the raw string, so a
  spelling the driver would fold and trim into `quorum`, such as `QUORUM` or
  ` quorum `, is refused there, and leaving the key out is refused as well.
- `broker.kafka.sessionTimeout` bounds `lifecycle.rebalanceDrainTimeout`: the
  drain timeout must be at most 0.6 times the session timeout. The check reads
  this key on the `kafka` driver only, and uses the 45s default when the key is
  absent.
- `broker.rabbitmq.consumerTimeout` must be at least three times every
  subscription's `handlerTimeout`. The check reads this key on the `rabbitmq`
  driver only, and uses the 90s default when the key is absent. The default
  `handlerTimeout` is 30s, which that default exactly meets, so a handler
  timeout above 30s with this key unset fails startup.

For the two duration keys, a value that does not parse as a duration is refused
by these checks too, before the driver would refuse it at `Open`.

## Continue from the source

- [`broker_config.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/broker_config.go) - the accepted key list and the error a key outside it produces.
- [`drivers/kafka/consumer.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/consumer.go) and [`drivers/kafka/producer.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/producer.go) - the Kafka resolvers and the defaults they apply.
- [`drivers/rabbitmq/management.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/management.go) and [`drivers/rabbitmq/topology.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/topology.go) - the management endpoint and the queue arguments.
