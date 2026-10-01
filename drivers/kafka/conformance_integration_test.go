//go:build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver/conformance"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestConformance(t *testing.T) {
	requireBroker(t)
	conformance.Run(t, conformance.Suite{
		Driver: Driver{},
		Config: driver.Config{
			Endpoints: []string{kafkaEndpoint},
			ClientID:  "f1-kafka-conformance",
			// A leave requested just after an assignment can cancel the group
			// session while the session's own offset fetch is in flight. The
			// client then closes that broker connection and retries the leave on
			// a new one after its first retry backoff, measured at 207ms to
			// 302ms. The budget has to clear that floor, with room for a second
			// retry.
			RebalanceDrainTimeout: time.Second,
		},
		NewInspector:       kafkaInspector,
		NewFaultInjector:   kafkaFaultInjector,
		NewDeadlineFixture: newDeadlineFixture,
	})
}

func kafkaInspector(raw driver.Conn) (conformance.Inspect, error) {
	connection, ok := raw.(*conn)
	if !ok {
		return nil, errors.New("Kafka inspector received a different connection")
	}
	admin := kadm.NewClient(connection.client)
	return func(ctx context.Context, destination string) (conformance.BrokerView, error) {
		if err := ctx.Err(); err != nil {
			return conformance.BrokerView{}, err
		}

		var active []*consumer
		connection.mu.RLock()
		for consumer := range connection.consumers {
			active = append(active, consumer)
		}
		connection.mu.RUnlock()

		var (
			sharedGroup string
			unsettled   int64
		)
		matches := 0
		for _, consumer := range active {
			consumer.mu.Lock()
			_, subscribed := consumer.budgets[destination]
			if subscribed && !consumer.stopped {
				if matches == 0 {
					sharedGroup = consumer.group
				} else if consumer.group == "" || consumer.group != sharedGroup {
					consumer.mu.Unlock()
					return conformance.BrokerView{}, fmt.Errorf("kafka inspector: destination %q has multiple live consumers with different groups (%q vs %q)", destination, sharedGroup, consumer.group)
				}
				matches++
				unsettled += int64(consumer.unsettled[destination])
			}
			consumer.mu.Unlock()
		}
		starts, err := listKafkaOffsets(ctx, func(listCtx context.Context) (kadm.ListedOffsets, error) {
			return admin.ListStartOffsets(listCtx, destination)
		})
		if err != nil {
			return conformance.BrokerView{}, classifyKafkaOffsetError("inspect", err)
		}
		ends, err := listKafkaOffsets(ctx, func(listCtx context.Context) (kadm.ListedOffsets, error) {
			return admin.ListEndOffsets(listCtx, destination)
		})
		if err != nil {
			return conformance.BrokerView{}, classifyKafkaOffsetError("inspect", err)
		}
		endPartitions, ok := ends[destination]
		if !ok {
			return conformance.BrokerView{}, classify("inspect", driver.KindNotFound, fmt.Errorf("destination %q is missing: %w", destination, driver.ErrDestinationMissing))
		}

		var committed kadm.OffsetResponses
		if matches > 0 && sharedGroup != "" {
			committed, err = admin.FetchOffsets(ctx, sharedGroup)
			if err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
				return conformance.BrokerView{}, classifyKafkaOffsetError("inspect", err)
			}
		}

		var ready int64
		committedOffsets := make(map[int32]int64)
		for partition, end := range endPartitions {
			start, ok := starts.Lookup(destination, partition)
			if !ok {
				return conformance.BrokerView{}, fmt.Errorf("kafka inspector: destination %q partition %d has no start offset", destination, partition)
			}
			committedOffset := start.Offset
			if matches > 0 && sharedGroup != "" {
				if response, exists := committed.Lookup(destination, partition); exists {
					if response.Err != nil {
						return conformance.BrokerView{}, classifyKafkaOffsetError("inspect", response.Err)
					}
					if response.At >= 0 {
						committedOffset = response.At
					}
				}
			}
			if end.Offset > committedOffset {
				ready += end.Offset - committedOffset
				committedOffsets[partition] = committedOffset
			}
		}
		auxiliary, err := countDeferredRecords(ctx, connection, destination, committedOffsets)
		if err != nil {
			return conformance.BrokerView{}, err
		}
		// Kafka reports committed and retained offsets, not deliveries already
		// handed to Messages. Keep those driver-held deliveries in Unsettled and
		// subtract the not-yet-due records counted as Auxiliary, so Ready is the
		// count a caller can still ask for.
		ready -= unsettled
		ready -= auxiliary
		if ready < 0 {
			ready = 0
		}
		return conformance.BrokerView{Ready: ready, Unsettled: unsettled, Auxiliary: auxiliary}, nil
	}, nil
}

// inspectorConsumerDelay returns the delay of a live consumer that subscribed to
// the destination, for a record the connection has no declaration for at all.
// The topology that declares a destination declares its delay with it, so this
// is the exception rather than the rule.
func inspectorConsumerDelay(connection *conn, destination string) (time.Duration, bool) {
	connection.mu.RLock()
	active := make([]*consumer, 0, len(connection.consumers))
	for consumer := range connection.consumers {
		active = append(active, consumer)
	}
	connection.mu.RUnlock()
	for _, consumer := range active {
		consumer.mu.Lock()
		delay, known := consumer.cfg.Delays[destination]
		subscribed := !consumer.stopped
		if _, ok := consumer.budgets[destination]; !ok {
			subscribed = false
		}
		consumer.mu.Unlock()
		if subscribed && known {
			return delay, true
		}
	}
	return 0, false
}

func countDeferredRecords(ctx context.Context, connection *conn, destination string, offsets map[int32]int64) (int64, error) {
	if len(offsets) == 0 {
		return 0, nil
	}
	fallback, hasFallback := inspectorConsumerDelay(connection, destination)
	partitions := map[string]map[int32]kgo.Offset{destination: {}}
	var expected int64
	for partition, offset := range offsets {
		partitions[destination][partition] = kgo.NewOffset().At(offset)
	}
	ends, err := kadm.NewClient(connection.client).ListEndOffsets(ctx, destination)
	if err != nil {
		return 0, classifyKafkaOffsetError("inspect", err)
	}
	for partition, end := range ends[destination] {
		if offset, ok := offsets[partition]; ok && end.Offset > offset {
			expected += end.Offset - offset
		}
	}
	if expected == 0 {
		return 0, nil
	}

	opts := append([]kgo.Opt(nil), connection.clientOpts...)
	opts = append(opts,
		kgo.ConsumePartitions(partitions),
		kgo.FetchMaxWait(50*time.Millisecond),
	)
	probe, err := kgo.NewClient(opts...)
	if err != nil {
		return 0, classify("inspect", driver.KindFatal, err)
	}
	defer probe.Close()

	var (
		fetched   int64
		auxiliary int64
	)
	for fetched < expected {
		fetches := probe.PollRecords(ctx, int(expected-fetched))
		for _, fetchErr := range fetches.Errors() {
			if errors.Is(fetchErr.Err, context.Canceled) || errors.Is(fetchErr.Err, context.DeadlineExceeded) {
				if ctx.Err() != nil {
					return 0, ctx.Err()
				}
				continue
			}
			return 0, classifyKafkaOffsetError("inspect", fetchErr.Err)
		}
		records := fetches.Records()
		if len(records) == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			continue
		}
		for _, record := range records {
			// A record's own publish instant decides which delay deferred it:
			// a destination defers a record by the delay it declares, and a
			// destination that declared none until after this record was
			// published did not defer it. A record the connection has no
			// declaration for falls back to a live consumer's config, and one
			// with neither is ready as it stands.
			delay, known := connection.destinationDelayAt(destination, record.Timestamp)
			if !known {
				delay, known = fallback, hasFallback
			}
			if !known || delay <= 0 || record.Timestamp.IsZero() {
				continue
			}
			// The same due time the consumer derives, including the rounding to
			// the resolution the broker stores a timestamp at: a record this
			// count calls ready is one the consumer would admit now.
			due := record.Timestamp.Truncate(kafkaTimestampPrecision).Add(kafkaTimestampPrecision).Add(delay)
			if due.After(clock.NewReal().Now()) {
				auxiliary++
			}
		}
		fetched += int64(len(records))
	}
	return auxiliary, nil
}

// destinationDelayAt reports the delay the destination had declared for a
// record published at instant at, and whether it had declared one at all. A
// record published before the destination's first declaration was not deferred
// by it, which a caller reads as a zero delay and reports as ready.
//
// A record's timestamp is the publish instant rounded down to the millisecond
// the broker stores, so a declaration anywhere inside that millisecond is in
// force for the record. The delay history is otherwise read by no production
// path, and this method exists for the tests that check a record's due time
// against what EnsureTopology recorded.
func (c *conn) destinationDelayAt(destination string, at time.Time) (time.Duration, bool) {
	latest := at.Truncate(kafkaTimestampPrecision).Add(kafkaTimestampPrecision)
	c.mu.RLock()
	defer c.mu.RUnlock()
	var (
		delay time.Duration
		known bool
	)
	for _, declared := range c.delays[destination] {
		if declared.at.After(latest) {
			break
		}
		delay, known = declared.delay, true
	}
	return delay, known
}
