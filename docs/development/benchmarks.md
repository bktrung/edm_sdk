# Benchmarks

F1's benchmark harness answers three questions before you adopt it: how fast F1 publishes, how fast
it consumes, and how to size a subscription for your handler. The numbers come from the harness under
`examples/bench/`, run against the local Docker fixtures on one laptop. They are one machine's
measurement, not a target or a threshold. Use them to compare shapes and to find which limit binds
you, then measure your own workload.

## What to expect

Measured on 2026-09-23 at commit `62b3942`: Intel Core Ultra 5 235U (14 threads), 15 GB RAM,
Linux 7.0, Go 1.26.6, RabbitMQ 4.3.4 with a quorum queue, and Kafka 4.2.1 with 3 partitions. Broker
and client ran on the same machine. Each cell is the median of three runs of 1,000 messages. No run
delivered a duplicate.

### Publish

`Publish` returns after the broker confirms the message, so one publisher sends one message per
confirm round trip. Concurrent publishers on one client share the connection and scale:

| Publishers | Kafka msgs/s | RabbitMQ msgs/s |
| ---: | ---: | ---: |
| 1 | 2,204 | 250 |
| 4 | 7,217 | 694 |
| 16 | 17,710 | 2,915 |

A single RabbitMQ publisher waits about 4 ms per message for the quorum queue's confirm. If one
publisher is your shape, publish from several goroutines or use `PublishBatch`.

`PublishBatch` waits for one confirm per batch instead of one per message. On RabbitMQ, with no
consumer attached, batches of 64 raised the rate 17 to 26 times and cut the client CPU per message
by more than half:

| Publishers | `Publish` msgs/s | `PublishBatch` of 64, msgs/s | CPU per 1,000 msgs, single / batch |
| ---: | ---: | ---: | ---: |
| 16 | 1,960 | 50,691 | 0.07 s / 0.03 s |
| 32 | 2,874 | 47,737 | 0.07 s / 0.03 s |
| 64 | 2,005 | 47,830 | 0.08 s / 0.03 s |

Past 16 publishers neither path gains: the broker, not the client, is the limit. This run is from
`BenchmarkRabbitMQPublishLoad` at commit `1d0f146`, the median of three runs of 30,000 single
messages or 300,000 batched ones. Its single-message rate varied between runs by up to half,
so read it as the same range as the 16-publisher cell above, not as a new figure.

### Latency

The time from the publish call to the handler's ack, one message at a time, with a handler that
returns at once:

| Driver | p50 | p99 |
| --- | ---: | ---: |
| Kafka | 0.42 ms | 0.82 ms |
| RabbitMQ | 5.5 ms | 10.1 ms |

Most of RabbitMQ's figure is the publish confirm. One of the three RabbitMQ runs had a p99 of
193 ms; the table shows the median run.

### Consume

Each consume cell is fed by 16 publishers, so a cell can never beat the 16-publisher publish rate
above. The default concurrency, used when `Concurrency` is not set, is 16.

| Handler | Concurrency | Kafka msgs/s | RabbitMQ msgs/s |
| --- | ---: | ---: | ---: |
| returns at once | 1 | 3,292 | 1,087 |
| returns at once | 16 (default) | 4,415 | 2,790 |
| holds 5 ms | 1 | 177 | 176 |
| holds 5 ms | 3 | 471 | 544 |
| holds 5 ms | 16 (default) | 466 | 1,982 |
| holds 5 ms, `OrderedByKey` | 16 (default) | 360 | 1,738 |

Read the table with the limits in the next section:

- **Handler time binds first.** At 5 ms one handler cannot exceed 200 messages/s, and both drivers
  reach about 88 percent of it.
- **Kafka stops at its partition count.** Kafka admits one delivery per partition, so with 3
  partitions the default concurrency of 16 runs no faster than concurrency 3.
- **RabbitMQ scales with concurrency.** At 16 handlers it reaches 1,982 of a 3,200 ceiling.
- **Ordering costs throughput.** Equal keys wait for each other, so the ordered cell is 12 percent
  below unordered on RabbitMQ and 23 percent below on Kafka.
- **The fast RabbitMQ cell measures its load.** 2,790 is level with the 2,915 the publishers
  offer, so it says nothing about the consumer's own ceiling.

Heap stayed between 1.7 MB and 4.0 MB in every consume cell.

### What a retry costs

A retry does not hold a worker while it waits out its delay: the copy waits in the broker. What it
costs is worker time. The handler runs a second time, and before acking the original the worker
publishes the retry copy and waits for the broker to confirm it.

Measured at concurrency 1 with a handler that fails every message once, as worker time per message:

| Driver | No retry | With one retry | Extra per retry |
| --- | ---: | ---: | ---: |
| Kafka | 0.30 ms | 0.61 ms | about 0.3 ms |
| RabbitMQ | 0.92 ms | 6.25 ms | about 5 ms |

On RabbitMQ most of the extra time is the publish confirm, about 4 ms. If 10 percent of your
messages fail once, a single worker loses about 0.5 ms per message on average. More concurrency
spreads the cost, because other workers keep going while one waits for a confirm.

The "no retry" column is the concurrency-1 consume cell above, fed by 16 publishers instead of 4,
so the comparison is approximate. The retry delay (10 ms on Kafka, 500 ms on RabbitMQ) is paid
once for the whole run, because the copies wait in the broker at the same time; on RabbitMQ it adds
about 0.5 ms to the per-message figure.

## Sizing a subscription

A consume rate is the smallest of three limits. Find which one binds before you change anything.

| Limit | Rate ceiling | Raise it with |
| --- | --- | --- |
| Handler time | `Concurrency` / handler duration | More `Concurrency`, if the downstream can take it |
| Kafka partitions | assigned partitions / handler duration | More partitions, which re-maps keys and cannot be undone |
| Fetch from the broker | Lane capacity per destination, refilled once per broker round trip | `broker.rabbitmq.brokerPrefetch` on RabbitMQ |

Most real handlers are bound by the first row. A handler that takes 5 ms at the default concurrency
of 16 cannot exceed 16 / 0.005 s = 3,200 messages/s, whatever the broker does. A slow downstream
call lowers it further: at 50 ms the same subscription tops out at 320 messages/s, and the fix is
more concurrency or a faster dependency, not a bigger buffer.

Kafka admits one delivery per partition at a time, so a subscription with 3 assigned partitions
runs at most 3 handlers, even at concurrency 16. Plan the partition count before the first deploy.
See [Kafka parallelism and partitions](/drivers/kafka#kafka-parallelism-and-partitions).

Only a fast handler reaches the fetch limit. On RabbitMQ each destination may have only its lane's
capacity unacknowledged, so a handler that finishes in microseconds waits for the broker to send
the next batch. `broker.rabbitmq.brokerPrefetch` lets the broker send more ahead. It pays only above
the lane capacity and only while the handler is fast, and it costs:

- more memory, because every extra delivery is held in the process;
- a larger redelivery burst after a crash, reconnect, or close;
- a longer wait toward RabbitMQ's `consumer_timeout` for the deliveries it holds;
- a stall across priorities: when one lane fills, intake stops for every lane of the subscription
  until that lane has room ([why](/deep-dives/scheduler#why-lane-capacity-stays-small)).

Raise it when handlers are fast and priorities are roughly balanced. Leave it unset when one
priority can back up while the others must keep flowing. The
[prefetch sweep](#rabbitmq-consume-rate-and-broker-prefetch) below shows the gain.

## Detailed results

### RabbitMQ consume rate and broker prefetch

This sweep was measured on 2026-09-21, on an earlier commit and without the machine details above,
so compare its cells with each other rather than with the tables above. The shape is a fast handler
on a quorum queue reading a pre-published backlog. Raising the broker window from its default (unset)
to 128 took concurrency 4 from a median 820 to 17,740 settled messages/s, and concurrency 16 from
2,963 to 18,802. The core in-flight budget did not change.

| Broker prefetch | c=4 msgs/s | c=4 heap max | c=16 msgs/s | c=16 heap max |
| --- | ---: | ---: | ---: | ---: |
| unset (default) | 820 | 5.2 MB | 2,963 | 11.9 MB |
| 16 | 3,008 | 10.0 MB | 2,606 | 9.9 MB |
| 32 | 5,071 | 14.7 MB | 4,299 | 18.4 MB |
| 64 | 8,596 | 25.6 MB | 10,419 | 25.5 MB |
| 128 | 17,740 | 46.4 MB | 18,802 | 47.3 MB |
| 128, handler 5 ms | 738 | 5.6 MB | - | - |

The option pays only above the lane capacity: at c=16 a value of 16 is no better than unset,
because the lanes already allow that many. Heap grows with the window. With a 5 ms handler, the
c=4 ceiling is 4 / 0.005 s = 800 messages/s, and even a window of 128 reaches only 738: a handler
that takes real time is the limit, and no broker window lifts it. The c=16, prefetch-128 cell rests
on only 3 of 9 clean runs, so it carries less weight than the others.

The option, its default, and what it does to the core budget are in
[RabbitMQ options](/drivers/rabbitmq#rabbitmq-options).

### Scheduler fairness by priority

The shape is one subscription over the three default priority lanes on the in-memory driver,
3,000 messages from four publishers, and a handler that holds each delivery for 1 ms. It runs one
handler slot on purpose, so what a delivery waits is the scheduler's ordering rather than spare
handler capacity. Measured on 2026-09-23 at commit `1d0f146`. The time is from the harness handing
a delivery to F1 until F1 acks it, as the median of five runs with the range in brackets. Time
spent at the broker while a lane was full is not included.

| Priority | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| high | 10.92 ms (10.84-11.13) | 11.36 ms (11.04-11.57) | 11.86 ms (11.14-13.88) |
| medium | 12.88 ms (11.97-15.70) | 22.34 ms (21.92-22.73) | 22.75 ms (22.06-23.74) |
| low | 6.54 ms (6.53-6.71) | 86.42 ms (85.25-87.90) | 88.00 ms (85.64-90.16) |

High is served eight picks in thirteen, medium four, and low one. High keeps the narrowest spread
under a full backlog, which is the weight doing its job. Low has the longest tail: it is served
once in thirteen picks, so a low delivery behind a few other low messages waits several rounds. Low
also has the shortest median; this run does not isolate why. The result says nothing about higher
concurrency, where a delivery's wait mixes scheduling and handler capacity.

### Observer overhead

An observer is called on every publish and every settled delivery. On the in-memory driver, as
the median of five runs per shape, with no observer, an observer that does nothing, and one that
counts every call under a mutex, the time per message was:

| Shape | No observer | No-op observer | Counting observer |
| --- | ---: | ---: | ---: |
| Consume, concurrency 1 | 65.6 us | 76.1 us | 68.3 us |
| Consume, concurrency 8 | 69.4 us | 75.3 us | 73.1 us |
| `Publish`, one message | 185.9 us | 145.5 us | 150.2 us |
| `PublishBatch` of 100 | 5.96 ms | 5.70 ms | 5.50 ms |

The steadiest shape, consume at concurrency 8, was 5 to 8 percent slower with an observer
installed, a few microseconds per message. In the other shapes one column's runs spread wider than
the gap between columns, and publishing with no observer was even the slowest, so the difference
there is below what this benchmark resolves. An installed observer adds two allocations per
consumed message and none per publish. What your own
observer does per call, such as exporting a span, is its own cost and is not measured here.
Measured on 2026-09-23 at commit `1d0f146`.

## Method and reproduce

The broker and scheduler benchmarks live in `examples/bench/`; the observer benchmarks live in the
root package. The broker ones need the fixtures:

```sh
make broker-up kafka-up
go test -tags integration -run '^$' -benchmem -benchtime=1000x -count=3 -timeout 30m \
  -bench '^Benchmark(Kafka|RabbitMQ)(Publish|Consume|Latency|Retry)$' ./examples/bench/
```

The harness reads `F1_RABBITMQ_ENDPOINT`, `F1_KAFKA_ENDPOINT`, and `F1_KAFKA_PARTITIONS`, and falls
back to the local fixtures. `make bench` starts both fixtures and runs every benchmark in the
package, including the sweeps, which take much longer.

| Benchmark | What it measures |
| --- | --- |
| `Benchmark{Kafka,RabbitMQ}Publish` | Publish rate at 1, 4, and 16 concurrent publishers |
| `Benchmark{Kafka,RabbitMQ}Consume` | Consume-and-ack rate by concurrency and handler hold, with heap and lag |
| `Benchmark{Kafka,RabbitMQ}Latency` | Publish-to-ack p50 and p99, one message at a time |
| `Benchmark{Kafka,RabbitMQ}Retry` | Rate through one retry, with the tier reported beside it |
| `BenchmarkRabbitMQBrokerPrefetchSweep` | The prefetch sweep above |
| `BenchmarkRabbitMQConsumeSweep`, `BenchmarkRabbitMQIntakeSplit` | Consume by concurrency, prefetch, queue type, and destination count |
| `BenchmarkRabbitMQPublishLoad` | Single-message against batched publish, with no consumer |
| `BenchmarkSchedulerFairness` | The per-priority waits above, on the in-memory driver |
| `BenchmarkObserverConsume`, `BenchmarkObserverPublish` | The observer overhead above, in the root package on the in-memory driver |

```sh
go test ./examples/bench -run '^$' -bench BenchmarkSchedulerFairness -count 5 -v
go test -run '^$' -bench BenchmarkObserver -benchmem -count 5 .
```

A sweep cell is measured `sweepRepeats` times and reported as a median with its range, and
`-count=3` runs the whole sweep three times, which is where nine-run medians come from. A cell whose
drain window came in under the floor, or which ran above the machine load limit, is logged as `VOID`
and not read as a number. Both thresholds and the corpus sizing are constants in
`examples/bench/consume_sweep_integration_test.go`.

## Go further

- [Testing](/development/testing) - the `make bench` target and what each cell reports.
- [How F1 picks the next message](/deep-dives/scheduler) - why lane capacity stays small.
- [Kafka partitions and rebalances](/deep-dives/kafka-lane-balancer) - why partition count sets Kafka parallelism.
