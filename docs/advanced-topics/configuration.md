# Configuration reference

Every setting F1 reads, in one place: the YAML key, its default, the values it
accepts, and the page that explains it. Settings that live only in Go (handlers,
callbacks, client options) are listed at the end.

## Where a value comes from

`f1.LoadConfig(path)` builds a `Config` in this order, and a later step wins:

```mermaid
flowchart LR
    D[package defaults] --> Y[YAML file]
    Y --> E[F1_* environment variables]
    E --> S[non-zero fields on the<br/>Subscription passed to Subscribe]
```

- An empty `path` skips the file and uses defaults and environment variables.
- Unknown keys are refused, so a typo fails at startup instead of being ignored.
- A `Config` built in Go gets the same defaults and checks when `f1.New` runs.
- A subscription in the file is matched to `Client.Subscribe` by name. Non-zero
  fields on the Go `Subscription` override the file.

Durations use Go syntax: `500ms`, `30s`, `2m`, `1h`.

## Full example

Every key below is shown with its default, except where a comment says
otherwise. All keys sit under the root `f1:` key.

```yaml
f1:
  env: dev                      # required
  service: orders               # required
  instanceId: orders-7f9c       # optional; used for the f1producer header and Kafka static membership

  broker:
    driver: rabbitmq            # required: kafka, rabbitmq, or inmem
    endpoints:                  # required for kafka and rabbitmq
      - amqp://guest:guest@localhost:5672/
    connectTimeout: 30s
    maxReconnectAttempts: 0     # 0 = no limit
    defaultPrefetch: 0          # automatic lane-derived sizing
    tls:
      enabled: false
      caFile: ""
      certFile: ""
      keyFile: ""
      serverName: ""
      insecureSkipVerify: false
    sasl:
      mechanism: ""
      username: ""
      password: ""
    rabbitmq:                   # only for driver: rabbitmq; values are strings
      queueType: quorum
      # vhost: orders           # management API vhost; defaults to the endpoint's vhost
    # kafka:                    # only for driver: kafka
    #   balancer: cooperative-sticky

  topology:
    autoCreate: false
    verifyOnStart: true
    priorities: [high, medium, low]

  codec:
    default: json
    maxHeaderBytes: 8192
    maxBodyBytes: 1048576

  lifecycle:
    drainTimeout: 1m
    handlerGrace: 5s
    consumerDrainTimeout: 0s    # 0 = only the Close context bounds it
    closeTimeout: 10s
    rebalanceDrainTimeout: 25s

  subscriptions:
    orders:                     # the name passed to Client.Subscribe
      topics: [orders.created]  # required
      mode: unordered
      concurrency: 16
      # prefetch: 0             # positive broker fallback, otherwise automatic
      priorities: [high, medium, low]
      handlerTimeout: 30s
      unmatchedPolicy: ignore
      retry:
        maxAttempts: 4
        initialInterval: 1s
        multiplier: 5
        maxInterval: 30s
        # tiers: [5s, 30s, 2m]  # replaces the exponential delays when set
      fairness:
        weights: {high: 8, medium: 4, low: 1}
        budgets: {high: 5s, medium: 30s, low: 2m}
        retryWeightDivisor: 2
        prefetchFactor: 2
        disableDeadlinePromotion: false
```

## Identity

| Key | Default | Accepts | Notes |
| --- | --- | --- | --- |
| `env` | none, required | Letters, digits, `-`, `_` | `prod` turns on the [production checks](/advanced-topics/running-in-production#set-production-configuration). `production`, `prd` and `PROD` are refused. |
| `service` | none, required | Any non-empty string | Part of the `f1producer` header. |
| `instanceId` | empty | Any string | Part of the `f1producer` header. Kafka uses it for static membership. |

## Broker

| Key | Default | Accepts | Notes |
| --- | --- | --- | --- |
| `broker.driver` | none, required | `kafka`, `rabbitmq`, `inmem` | `inmem` is refused in `prod`. |
| `broker.endpoints` | none | List of endpoints | Required for `kafka` (`host:port`) and `rabbitmq` (`amqp://` or `amqps://`). |
| `broker.connectTimeout` | `30s` | Duration | Bounds each `Open`, including reconnects. |
| `broker.maxReconnectAttempts` | `0` | Integer, 0 or more | `0` means no limit. See [reconnects](/deep-dives/reconnect-and-generations). |
| `broker.defaultPrefetch` | `0` | Integer, 0 to 65535 | Positive subscription fallback; `0` or omission selects automatic sizing. See [prefetch resolution](#prefetch-resolution). |
| `broker.tls.*` | TLS off | See the driver page | `enabled`, `caFile`, `certFile`, `keyFile`, `serverName`, `insecureSkipVerify`. [Kafka TLS](/drivers/kafka#kafka-tls), [RabbitMQ TLS](/drivers/rabbitmq#rabbitmq-tls). |
| `broker.sasl.*` | SASL off | See the driver page | `mechanism`, `username`, `password`. In `prod`, SASL needs TLS. [Kafka SASL](/drivers/kafka#kafka-sasl), [RabbitMQ SASL](/drivers/rabbitmq#rabbitmq-sasl). |

### Driver options

Driver options sit under `broker.kafka` or `broker.rabbitmq`, and their values
are strings. A key for the other driver, or one the driver does not read, is
refused. The driver pages hold the full rules.

| Key | Default | Details |
| --- | --- | --- |
| `broker.kafka.compression` | `snappy` | [Kafka options](/drivers/kafka#kafka-options) |
| `broker.kafka.batchLinger` | `10ms` | same |
| `broker.kafka.fetchMaxBytes` | `52428800` | same |
| `broker.kafka.fetchMaxWait` | `50ms` | same |
| `broker.kafka.sessionTimeout` | `45s` | same |
| `broker.kafka.rebalanceTimeout` | `60s` | same |
| `broker.kafka.staticMembership` | `true` | same |
| `broker.kafka.balancer` | `cooperative-sticky` | same |
| `broker.kafka.maxExpectedInstances` | `0` | same |
| `broker.rabbitmq.vhost` | the endpoint's vhost | [RabbitMQ options](/drivers/rabbitmq#rabbitmq-options) |
| `broker.rabbitmq.queueType` | `quorum` | same; must be `quorum` in `prod` |
| `broker.rabbitmq.consumerTimeout` | not declared | same; at least 3 x every `handlerTimeout`. When unset, that check uses `90s`, so a `handlerTimeout` above `30s` needs this key. |
| `broker.rabbitmq.brokerPrefetch` | each destination's core window | same |
| `broker.rabbitmq.managementPort` | AMQP port + 10000 | same |
| `broker.rabbitmq.trustBrokerTimestamp` | `false` | same |

## Topology

| Key | Default | Accepts | Notes |
| --- | --- | --- | --- |
| `topology.autoCreate` | `false` | Boolean | Creates missing destinations. Refused in `prod`. |
| `topology.verifyOnStart` | `true` | Boolean | Checks destinations at startup without creating them. |
| `topology.priorities` | `[high, medium, low]` | `high`, `medium`, `low`, no duplicates | The priorities publisher entry points are declared for. |

`autoCreate` wins when both are true, and neither means no topology work. The
`WithTopology` option overrides both. See [Topology and
capabilities](/advanced-topics/topology-and-capabilities).

## Codec

| Key | Default | Accepts | Notes |
| --- | --- | --- | --- |
| `codec.default` | `json` | A registered codec name | Reads a message that carries no content type. Publishing uses the first `WithCodec` codec, or JSON. |
| `codec.maxHeaderBytes` | `8192` | Positive integer | Capped at 8192 and at the driver's own limit. |
| `codec.maxBodyBytes` | `1048576` (1 MiB) | Positive integer | |

## Lifecycle

| Key | Default | Accepts | Notes |
| --- | --- | --- | --- |
| `lifecycle.drainTimeout` | `1m` | Positive duration | Must be longer than every subscription's `handlerTimeout`. |
| `lifecycle.handlerGrace` | `5s` | Duration, 0 or more | How long handlers get after their context is canceled near the end of a drain. A value at or above `drainTimeout` is treated as `0`. |
| `lifecycle.consumerDrainTimeout` | `0` | Duration, 0 or more | Bounds `Close`'s wait for all runner drains. `0` leaves only the `Close` context. |
| `lifecycle.closeTimeout` | `10s` | Duration, 0 or more | Bounds every consumer `Stop` and `Release`, producer and connection close, and the wait for a reconnect to stop. |
| `lifecycle.rebalanceDrainTimeout` | `25s` | Duration, 0 or more | Kafka only: at most 0.6 x `sessionTimeout` and 0.6 x `rebalanceTimeout`. |

A zero duration here selects the default, except `consumerDrainTimeout`. See
[Lifecycle and shutdown](/advanced-topics/lifecycle-and-shutdown#configure-shutdown-time-limits).

## Subscriptions

Each entry under `subscriptions` is keyed by the subscription name, which may
use only letters, digits, `-` and `_`.

| Key | Default | Accepts | Notes |
| --- | --- | --- | --- |
| `topics` | none, required | Non-empty topic names | Two entries naming the same topic are refused. |
| `mode` | `unordered` | `unordered`, `orderedByKey` | [Ordering guarantee](/advanced-topics/ordering-and-scheduling#choose-the-ordering-guarantee). In ordered mode, `concurrency` x `prefetch` is at most 2,097,152. |
| `concurrency` | `16` | 1 to 1024 | Handlers running at once. On Kafka, also capped by the partitions the member holds. |
| `prefetch` | `0` | Integer, 0 to 65535 | `0` or omission uses a positive broker fallback, otherwise automatic sizing. See [prefetch resolution](#prefetch-resolution). |
| `priorities` | `[high, medium, low]` | `high`, `medium`, `low`, no duplicates | The priority lanes this subscription reads. |
| `handlerTimeout` | `30s` | Positive duration | Shorter than `lifecycle.drainTimeout`. |
| `unmatchedPolicy` | `ignore` | `ignore`, `deadletter` | What happens to an event no handler matches. |

### Prefetch resolution

Automatic sizing uses the sum of resolved lane capacities, not a fixed budget.
A positive subscription value overrides a positive `broker.defaultPrefetch`;
subscription zero or omission uses that fallback when set, otherwise automatic
sizing. An environment prefetch of `0` clears the lower-priority subscription
value before fallback resolution; a positive Go `Subscription.Prefetch` still
has higher priority.
Positive totals from `1` to `65535` are valid even below the lane count; a small
total limits concurrent SDK admission without shrinking destination windows.

The effective total bounds SDK-admitted unsettled deliveries across all
destinations, including work waiting for or running in a handler. Broker credit,
poll results, and client transport buffers are separate. Each destination keeps
its full lane window, but every adapter must enforce both that window and the
total. A positive total above the lane-capacity sum is effectively lower; it
does not enlarge lanes or let a hot lane borrow another lane's capacity.

The executable owners are `resolvePrefetch` and subscription validation in
[`config.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/config.go),
`resolveSubscription` in
[`subscription.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/subscription.go),
and `runnerLanePlan` / `runnerConsumerPrefetch` in
[`worker.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/worker.go).
Resolved automatic sizing remains subject to prefetch and ordered-buffer
validation. This admission contract does not change acknowledgement timing or
ordered-key behavior.

### Retry

| Key | Default | Accepts | Notes |
| --- | --- | --- | --- |
| `retry.maxAttempts` | `4` | 1 to 20 | Counts the first delivery. |
| `retry.initialInterval` | `1s` | Duration, 0 or more | First retry delay. |
| `retry.multiplier` | `5` | Finite number, 0 or more | Growth per retry. |
| `retry.maxInterval` | `30s` | Duration, 0 or more | Caps each delay. |
| `retry.tiers` | none | Positive durations | Explicit delays; replaces the three fields above. The last one repeats. |

No delay may exceed 2,147,483,647 ms (about 24.8 days). See [Failure
handling](/advanced-topics/failure-handling#configure-the-retry-delays).

### Fairness

| Key | Default | Accepts | Notes |
| --- | --- | --- | --- |
| `fairness.weights` | `{high: 8, medium: 4, low: 1}` | Integer from 1 to 65535, per priority | Relative share of handler slots. |
| `fairness.budgets` | `{high: 5s, medium: 30s, low: 2m}` | Duration, 0 or more, per priority | How long a lane's oldest message may wait before it jumps the queue. |
| `fairness.retryWeightDivisor` | `2` | Integer, 0 or more | Divides a retry lane's weight. `0` keeps the default. |
| `fairness.prefetchFactor` | `2` | Integer, 0 or more | Scales each lane's capacity. `0` keeps the default. |
| `fairness.disableDeadlinePromotion` | `false` | Boolean | Turns queue jumping off. |

See [Ordering and scheduling](/advanced-topics/ordering-and-scheduling) and
[how F1 picks the next message](/deep-dives/scheduler).

## Environment variables

These override the file. See [Environment
overrides](/drivers-and-capabilities#environment-overrides).

| Variable | Overrides |
| --- | --- |
| `F1_ENV` | `env` |
| `F1_SERVICE` | `service` |
| `F1_INSTANCE_ID` | `instanceId` |
| `F1_BROKER_DRIVER` | `broker.driver` |
| `F1_BROKER_ENDPOINTS` | `broker.endpoints`, comma-separated |

## Go-only settings

Some settings have no YAML key.

**Client options** are passed to `f1.New`:

| Option | What it sets |
| --- | --- |
| `WithDriver` | The broker driver to open. |
| `WithCodec` | Extra codecs. The first one is used to publish. |
| `WithLogger` | The `slog` logger. |
| `WithPublishTopics` | Topics this client may publish. They are also set up at startup. |
| `WithTopology` | Topology policy, overriding `topology.autoCreate` and `topology.verifyOnStart`. |
| `WithStrictPortability` | Use only behavior every driver supports. |
| `WithMiddleware` | Handler middleware, outermost first. |
| `WithErrorHandler` | Callback for driver errors and failed retry or dead-letter publishes. |
| `WithObserver` | Observer for lifecycle and message events. |
| `WithBacklogPollInterval` | Backlog sampling interval. Default `15s`, negative turns it off, values under `1s` are refused. |

**Subscription fields** `Handlers`, `OnDeadLetter` and `OnDiscarded` are set
on the `f1.Subscription` passed to `Client.Subscribe`. See
[Publisher and subscriber](/basics/pubsub).

**Publish options** such as `WithKey`, `WithPriority` and `WithMaxAttempts`
apply to one publish. See [Message](/basics/message).
