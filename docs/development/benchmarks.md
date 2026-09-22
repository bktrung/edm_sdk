# Benchmarks

F1's throughput numbers come from the harness under `examples/bench/`, which drives the adapters
against the local fixtures and reports what a shape actually reaches. A number on this page is one
machine's measurement, not a target and not a threshold, and the run's CPU model, core count, and Go
version are not recorded beside it, so read the table as a comparison between cells measured
together rather than as an absolute rate.

## RabbitMQ consume rate and broker prefetch

The shape under test is the normal case: a fast handler on a quorum queue reading a pre-published
backlog. On one machine, raising the broker window from the shipped value to 128 took concurrency 4
from a clean median 820 to 17,740 settled messages/s, and concurrency 16 from 2,963 to 18,802. The
core in-flight budget did not change.

The rates below exclude every result block carrying a `VOID` marker. The clean counts, in table
order, are c=4: 8/9, 9/9, 8/9, 8/9, 5/9, and 7/9; c=16: 7/9, 8/9, 9/9, 5/9, and 3/9. Heap max values
are unchanged nine-run medians:

| Broker prefetch | c=4 msgs/s | c=4 heap max | c=16 msgs/s | c=16 heap max |
| --- | ---: | ---: | ---: | ---: |
| unset (shipped) | 820 | 5.2 MB | 2,963 | 11.9 MB |
| 16 | 3,008 | 10.0 MB | 2,606 | 9.9 MB |
| 32 | 5,071 | 14.7 MB | 4,299 | 18.4 MB |
| 64 | 8,596 | 25.6 MB | 10,419 | 25.5 MB |
| 128 | 17,740 | 46.4 MB | 18,802 | 47.3 MB |
| 128, handler 5ms | 738 | 5.6 MB | - | - |

The c=4, broker-prefetch=32 cell is the normal useful step: its clean median is about 6.2 times the
shipped rate. The option pays only when it is above the core window. At c=16, a value of 16 is no
better than unset, because the core window is already that wide. The c=16, broker-prefetch=128
result rests on only 3/9 clean observations, so it does not carry the same weight as the shipped
baseline. These figures are one machine's local-disk quorum queue, not a guarantee or a threshold.

With a handler that takes real time, the rate is bounded by concurrency divided by handler duration.
At c=4 and 5 ms, that arithmetic ceiling is about 800 messages/s, and the measured
broker-prefetch=128 cell reaches 738 messages/s. The broker window matters only while the handler is
fast enough for fetching to be the limit; it cannot improve this handler-bottlenecked cell. No unset
5 ms cell was measured.

The option itself, its default, and what it does to the core budget are in [Drivers and
capabilities](/drivers-and-capabilities#rabbitmq-options).

## Scheduler fairness by priority

The shape under test is one subscription over the three default priority lanes on the in-memory
driver, 3,000 messages published by four publishers, and a handler that holds each delivery for
1 ms. The subscription runs a single handler slot on purpose: with one worker, what a delivery waits
is the scheduler's ordering rather than spare handler capacity. The measured quantity is the time
from the harness handing a delivery to F1 to the ack F1 made for it, reported per priority as the
median of five runs, with the range across those runs in brackets. The clock starts at the hand-over,
not at publish, so time a delivery spent at the broker while its lane was full is not in these
numbers:

| Priority | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| high | 11.06 ms (10.97-11.09) | 11.58 ms (11.31-12.06) | 12.33 ms (12.23-14.64) |
| medium | 12.71 ms (12.33-15.33) | 22.51 ms (22.35-23.31) | 24.03 ms (22.95-25.83) |
| low | 6.69 ms (6.68-6.86) | 86.91 ms (86.40-87.68) | 88.05 ms (87.76-93.56) |

High is served eight picks in thirteen, medium four and low one. High's spread is the narrowest of
the three, which is the weight doing its job under a full backlog. Medium's median is higher and its
tail longer. Low has by far the longest tail: a low lane is
served once in thirteen picks, so a low delivery that arrives behind a few other low messages waits
several rounds. A lane holds only a bounded handful of messages, so a short backlog is already a long
wait measured in rounds. Low also has the shortest median; this run does not isolate why.

This says nothing about higher concurrency, where several workers run at once and a delivery's wait
is a mix of scheduling and handler capacity. It is also not a comparison with any other pick rule:
no other rule was measured here.

```sh
go test ./examples/bench -run '^$' -bench BenchmarkSchedulerFairness -count 5 -v
```

## Reproduce a run

Start the RabbitMQ fixture, then run the sweep that produced the table:

```sh
make broker-up
go test -tags integration -run '^$' -bench BenchmarkRabbitMQBrokerPrefetchSweep -benchmem \
  -count=3 ./examples/bench/
```

`make bench` starts both fixtures, points both endpoint variables at them, and runs every benchmark
in `examples/bench/` with `-benchmem -benchtime=1000x -count=3`. The harness reads the RabbitMQ
endpoint from `F1_RABBITMQ_ENDPOINT` and falls back to the local fixture, so a sweep needs the
fixture targets described in [Testing strategy](/development/testing).

A cell is measured `sweepRepeats` times and reported as a median with its range, and `-count=3` runs
the whole sweep three times, which is where the nine-run medians and the "/9" counts come from. A
cell whose drain window came in under the floor, or which was measured above the machine load
limit, is logged as `VOID` instead of read as a number about its shape. Both thresholds, and the
corpus sizing that gives each cell a long enough window, are constants in
`examples/bench/consume_sweep_integration_test.go`.
