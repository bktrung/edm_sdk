package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver/conformance"
)

func TestConformance(t *testing.T) {
	if os.Getenv("F1_KAFKA_CONFORMANCE") == "" {
		t.Skip("Kafka conformance is gated until consume and settle are complete; the settler is provisional")
	}
	requireBroker(t)
	conformance.Run(t, conformance.Suite{
		Driver:       Driver{},
		Config:       driver.Config{Endpoints: []string{kafkaEndpoint}, ClientID: "f1-kafka-conformance"},
		NewInspector: kafkaInspector,
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
			group     string
			unsettled int64
			discarded int64
		)
		matches := 0
		for _, consumer := range active {
			consumer.mu.Lock()
			_, subscribed := consumer.budgets[destination]
			if subscribed && !consumer.stopped {
				matches++
				group = consumer.group
				unsettled = int64(consumer.unsettled[destination])
				for key, offsets := range consumer.discarded {
					if key.destination == destination {
						discarded += int64(len(offsets))
					}
				}
			}
			consumer.mu.Unlock()
		}
		if matches > 1 {
			return conformance.BrokerView{}, fmt.Errorf("kafka inspector: destination %q has multiple live consumers", destination)
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
		if matches == 1 {
			committed, err = admin.FetchOffsets(ctx, group)
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
			if matches == 1 {
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
		// handed to Messages. Keep those driver-held deliveries in Unsettled,
		// subtract discarded holes that wait behind a requeued offset, and
		// subtract not-yet-due records counted as Auxiliary.
		ready -= unsettled
		ready -= discarded
		ready -= auxiliary
		if ready < 0 {
			ready = 0
		}
		return conformance.BrokerView{Ready: ready, Unsettled: unsettled, Auxiliary: auxiliary}, nil
	}, nil
}

func countDeferredRecords(ctx context.Context, connection *conn, destination string, offsets map[int32]int64) (int64, error) {
	delay, known := connection.destinationDelay(destination)
	if !known || delay <= 0 || len(offsets) == 0 {
		return 0, nil
	}
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
			if evaluateDeferral(record, delay, true, kafkaNow()).wait {
				auxiliary++
			}
		}
		fetched += int64(len(records))
	}
	return auxiliary, nil
}
