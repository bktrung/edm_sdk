# Benchmarks

The same workload runs through F1 and through the broker's own Go client (franz-go for Kafka,
amqp091-go for RabbitMQ), with the same delivery guarantees, on the same machine. Every cell reads
"F1 / raw".

## Setup

| | |
| --- | --- |
| Machine | Intel Core Ultra 5 235U laptop, 15 GB RAM, Linux 7.0, Go 1.26.6 |
| Client | pinned to 4 cores, `GOMAXPROCS=4` |
| Broker | Docker, pinned to 4 other cores, fsync on |
| Kafka | 4.2.1, one broker |
| RabbitMQ | 4.3.4, quorum queue |
| Message | 1 KiB body |
| Runs | median of 3 per cell |

CPU is the client process's CPU in percent of one core. Memory is the client process's peak RSS.

## Publish

Messages are sent at a fixed rate, and each is timed from the moment it was scheduled to the
broker's ack. No consumer is attached.

### Kafka

| Offered msgs/s | p50 ms | p99 ms | CPU % | Memory MB |
| ---: | ---: | ---: | ---: | ---: |
| 500 | 0.71 / 0.71 | 2.0 / 1.6 | 7 / 6 | 22 / 20 |
| 1,000 | 0.66 / 0.68 | 1.9 / 2.5 | 11 / 11 | 25 / 23 |
| 2,000 | 0.60 / 0.59 | 3.2 / 2.3 | 15 / 12 | 32 / 29 |
| 5,000 | 0.63 / 0.62 | 4.2 / 4.3 | 30 / 23 | 50 / 45 |
| 10,000 | 0.15 / 0.25 | 4.5 / 4.6 | 70 / 45 | 82 / 72 |
| 20,000 | 0.15 / 0.14 | 6.0 / 6.2 | 112 / 87 | 144 / 125 |
| 50,000 | 0.33 / 0.28 | 8.9 / 6.8 | 151 / 112 | 286 / 267 |
| 100,000 | 1.3 / 0.73 | 31 / 9.0 | 201 / 117 | 523 / 497 |
| 200,000 | 14 / 2.4 | 220 / 13 | 322 / 174 | 1,048 / 974 |

### RabbitMQ

RabbitMQ's p99 follows the disk's sync time, so only p50 is shown.

| Offered msgs/s | p50 ms | CPU % | Memory MB |
| ---: | ---: | ---: | ---: |
| 1,000 | 4.5 / 4.9 | 8 / 4 | 22 / 18 |
| 5,000 | 4.9 / 4.8 | 29 / 15 | 42 / 34 |
| 10,000 | 6.0 / 5.3 | 49 / 25 | 94 / 75 |
| 20,000 | 7.9 / 6.6 | 96 / 40 | 124 / 90 |

Highest rate reached with 100,000 msgs/s offered: 45,600 / 64,200 msgs/s, at 171 / 92 percent
CPU and 225 / 183 MB.

## Consume

A pre-loaded backlog is drained by a handler that holds each message for 20 ms. Ideal is workers
divided by 20 ms.

### Kafka, 64 partitions

| Workers | Ideal msgs/s | msgs/s | CPU % | Memory MB |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 50 | 47 / 49 | 2 / 1 | 22 / 20 |
| 4 | 200 | 188 / 196 | 4 / 1 | 29 / 23 |
| 16 | 800 | 746 / 783 | 6 / 1 | 59 / 36 |
| 64 | 3,200 | 3,052 / 3,110 | 13 / 2 | 182 / 90 |
| 128 | 6,400 | 3,048 / 3,106 | 14 / 2 | 175 / 91 |
| 256 | 12,800 | 3,057 / 3,103 | 14 / 2 | 188 / 91 |

Kafka delivers one message per partition at a time, so 64 partitions cap both clients at
3,200 msgs/s.

### RabbitMQ

| Workers | Ideal msgs/s | msgs/s | CPU % | Memory MB |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 50 | 49 / 49 | 1 / 1 | 19 / 17 |
| 4 | 200 | 194 / 194 | 2 / 2 | 20 / 19 |
| 16 | 800 | 777 / 777 | 7 / 4 | 21 / 19 |
| 64 | 3,200 | 3,104 / 3,088 | 21 / 10 | 29 / 24 |
| 128 | 6,400 | 6,180 / 6,202 | 36 / 16 | 41 / 33 |
| 256 | 12,800 | 11,923 / 12,244 | 64 / 28 | 62 / 48 |

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
priority can back up while the others must keep flowing.

## Reproduce

```sh
make bench-slides
```

The target starts both brokers pinned to their cores and runs every cell above for F1 and the raw
client.

## Go further

- [Testing](/development/testing) - the `make bench` target and what each cell reports.
- [How F1 picks the next message](/deep-dives/scheduler) - why lane capacity stays small.
- [Kafka partitions and rebalances](/deep-dives/kafka-lane-balancer) - why partition count sets Kafka parallelism.
