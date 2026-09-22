# Alerts

This runbook uses the OpenTelemetry-to-Prometheus naming rule: dots become
underscores, instrument units add suffixes such as `_seconds`, and counters
add `_total`. The PromQL below is a starting point. Tune windows, labels, and
thresholds to the traffic and service-level objectives of each deployment.

## Dead letters appearing

**What it means:** A non-zero rate means F1 confirmed a dead-letter publication
or routing. Grouping shows which bounded error class and death reason produced
it.

**PromQL:**

```text
sum by (messaging_system, messaging_destination_name, messaging_consumer_group_name, f1_priority, error_type, reason) (
  rate(f1_messaging_dead_letters_total[5m])
) > 0
```

**Starting threshold:** Start with any rate above zero over a 5-minute window;
add an alert hold if isolated dead letters are normal for the service.

**First three things to check:**

1. Group the result by `error_type`, `reason`, destination, and consumer group.
2. Check handler, decode, and poison-message logs for the matching class.
3. Check the dead-letter destination and the error handler for publication or
   notification failures.

## Retry rate high relative to consumed messages

**What it means:** Retries are consuming a material share of deliveries rather
than being occasional recovery. The ratio is grouped by messaging system and
consumer group because delivery-receipt metrics can lack topic and priority;
use the retry labels for destination and priority drilldown.

**PromQL:**

```text
(
  sum by (messaging_system, messaging_consumer_group_name) (
    rate(f1_messaging_retries_total[5m])
  )
  /
  clamp_min(
    sum by (messaging_system, messaging_consumer_group_name) (
      rate(messaging_client_consumed_messages_total[5m])
    ),
    0.001
  )
) > 0.1
```

**Starting threshold:** Start at 10% retries per consumed message over 5
minutes. Raise or lower it after observing the normal retry mix.

**First three things to check:**

1. Break retries down by `error_type` and inspect the retry ladder and attempt
   limits.
2. Confirm consumed traffic is present and that the query filters the intended
   consumer group; use retry labels for destination and priority drilldown.
3. Check handler errors, broker redeliveries, and dependency health for the
   dominant failure class.

## Backlog growing

**What it means:** The backlog is increasing over 15 minutes and is above a
small floor, which avoids paging on a few transient messages.

**PromQL:**

```text
deriv(f1_messaging_backlog_messages[15m]) > 0
and
f1_messaging_backlog_messages > 10
```

**Starting threshold:** Start with a floor of 10 messages and the positive
15-minute derivative; choose a floor that reflects the service's normal burst
size.

**First three things to check:**

1. Check consumer membership, assigned work, concurrency, and paused
   destinations.
2. Check handler latency and retry or dead-letter rates for the same series.
3. Check broker queue or partition lag and whether producers exceed consumer
   capacity.

## Oldest message age

**What it means:** A Kafka backlog has an old broker append timestamp. This
signal is meaningful only for Kafka destinations configured with
`message.timestamp.type=LogAppendTime`; RabbitMQ does not report this metric.

**PromQL:**

```text
f1_messaging_backlog_oldest_age_seconds{messaging_system="f1.kafka"} > 300
```

**Starting threshold:** Start at 5 minutes and tune it to the destination's
latency objective. Keep this alert scoped to Kafka topics using `LogAppendTime`.

**First three things to check:**

1. Verify the affected Kafka topics use `message.timestamp.type=LogAppendTime`.
2. Check backlog by destination, consumer group, and priority.
3. Check partition assignment, consumer health, and handler latency.

## Handler latency near `handlerTimeout`

**What it means:** The p99 process duration is approaching the configured
handler timeout, leaving little room for settlement and broker liveness work.

**PromQL:**

```text
histogram_quantile(
  0.99,
  sum by (le, messaging_system, messaging_destination_name, messaging_consumer_group_name, f1_priority) (
    rate(messaging_process_duration_seconds_bucket[5m])
  )
) > 24
```

**Starting threshold:** Start at 80% of the configured `handlerTimeout`. The
query uses 24 seconds as an example for a 30-second timeout; replace it with
the value for the deployment.

**First three things to check:**

1. Compare p99 with the actual `handlerTimeout` and the broker consumer timeout
   where one is configured.
2. Split process observations by `error_type` and inspect the slow handler path.
3. Check downstream dependencies, broker wait, CPU, and concurrency saturation.

## Broker wait high

**What it means:** The p99 time from a broker enqueue timestamp to delivery is
high. The metric exists only when F1 has a trusted broker enqueue timestamp.

**PromQL:**

```text
histogram_quantile(
  0.99,
  sum by (le, messaging_system, messaging_consumer_group_name) (
    rate(f1_messaging_broker_wait_duration_seconds_bucket[5m])
  )
) > 1
```

**Starting threshold:** Start at 1 second over a 5-minute window, then set the
value from normal broker and network latency.

**First three things to check:**

1. Verify the timestamp source: Kafka needs `LogAppendTime`; RabbitMQ needs a
   trusted `timestamp_in_ms` configuration.
2. Check broker queue or partition lag, fetch capacity, and consumer
   assignment.
3. Check broker health, network latency, and connection or channel errors.

## No consumption while backlog is positive

**What it means:** F1 sees backlog but no received-message rate for the same
messaging system and consumer group. The backlog is aggregated across
destinations and priorities because delivery-receipt metrics can omit them.

**PromQL:**

```text
(
  (
    sum by (messaging_system, messaging_consumer_group_name) (
      rate(messaging_client_consumed_messages_total[5m])
    )
    or on (messaging_system, messaging_consumer_group_name)
    (
      0 * sum by (messaging_system, messaging_consumer_group_name) (
        f1_messaging_backlog_messages
      )
    )
  ) == 0
)
and on (messaging_system, messaging_consumer_group_name)
(
  sum by (messaging_system, messaging_consumer_group_name) (
    f1_messaging_backlog_messages
  ) > 0
)
```

**Starting threshold:** Start after 5 minutes of zero consumption while the
backlog remains above zero.

**First three things to check:**

1. Check consumer process health, group membership, assignment, and connection
   errors.
2. Check the broker backlog and whether the destination is paused or blocked by
   topology or permissions.
3. Check the configured error handler and logs for connection or consumer
   failures.

F1 does not emit a metric for connection lost or connection restored. Use the
error handler and logs for those signals instead of adding an alert for a
nonexistent series.
