package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

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
		)
		matches := 0
		for _, consumer := range active {
			consumer.mu.Lock()
			_, subscribed := consumer.budgets[destination]
			if subscribed && !consumer.stopped {
				matches++
				group = consumer.group
				unsettled = int64(consumer.unsettled[destination])
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
			}
		}
		// Kafka reports committed and retained offsets, not deliveries already
		// handed to Messages. Keep those driver-held deliveries in Unsettled so
		// Ready remains the broker backlog after the inspector's subtraction.
		ready -= unsettled
		if ready < 0 {
			ready = 0
		}
		return conformance.BrokerView{Ready: ready, Unsettled: unsettled, Auxiliary: 0}, nil
	}, nil
}
