# Kafka retry timing and head-of-line waiting

*By trungbk.*

A retry delay gives a failed dependency time to recover. RabbitMQ can [park a retry copy in a queue](/deep-dives/rabbitmq-delay-ladder); F1's Kafka driver instead checks eligibility at the consumer. That puts clock authority and partition order inside the retry-delay contract.

## Background

Kafka partitions preserve offset order, not timestamp order. Keeping a delayed retry at the head preserves the order in which F1 [finishes records](/learn/glossary#settlement), at the cost of holding records behind it. Other partitions can continue independently. The [consumer-side delay source](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/kafka/deferral.go) owns eligibility and the independent reasons a partition stays paused.

## Eligibility and ordering

A step's delay is intended as the earliest eligibility time, not a promise that the handler starts exactly then. Kafka evaluates eligibility against the consumer's clock and the record timestamp. The millisecond precision adjustment protects against timestamp truncation; it cannot compensate for clock skew.

Offset order does not guarantee increasing due times. Two F1 producers with different clocks can put an earlier-due record behind a later-due head. F1 keeps offset order, so that record waits for the head even if its own delay has already elapsed.

Eligibility also does not reserve a worker. Broker availability, partition pauses, earlier records, and scheduler or handler backlog can all add waiting time. F1 promises no upper bound on retry lateness.

Requeue and retry have different purposes: a requeue redelivers the original record, while a retry creates a copy for another attempt. Applying a fresh retry delay to an already delivered requeue would confuse redelivery with another attempt. See [Consume flow](/development/consume-flow) for that boundary.

## Limits and trade-offs

Changing a step's delay changes the eligibility calculation for records already stored on that step's topic. Shortening it can make a backlog eligible together; lengthening it can hold records that would otherwise be due. Treat the change as a backlog decision, not just a setting for future retries.

The consumer's clock decides eligibility, but the record's timestamp supplies the starting instant. With `CreateTime`, F1's producer stamps the record from its own clock. With `LogAppendTime`, the broker supplies that instant. Neither choice fixes clock skew with the consumer, and the precision adjustment cannot do so. Keep the relevant hosts synchronized; [Kafka retry timing](/drivers/kafka#kafka-retry-timing) owns the timestamp setting.

A held head makes delay and backlog interact. Adding workers cannot let later offsets pass it; partition capacity must account for waiting records as well as active handlers. This is a sizing constraint, not evidence of a particular throughput or lateness distribution.

## Go further

- [Kafka driver](/drivers/kafka#kafka-retry-timing) - retry timing, timestamps, and clock requirements.
- [RabbitMQ retry parking](/deep-dives/rabbitmq-delay-ladder) - the same retry steps on a broker that can hold a message.
- [Kafka ack tracker](/deep-dives/kafka-ack-tracker) - why a partition lets in one delivery at a time.
- [Retries and dead letters](/deep-dives/retries-and-dead-letters) - how F1 chooses the retry step.
- [Source-reading guide](/development/source-reading-guide#code-behind-the-deep-dives) - where this lives in the code.
