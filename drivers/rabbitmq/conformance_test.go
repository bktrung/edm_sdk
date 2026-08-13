package rabbitmq

import (
	"context"
	"errors"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver/conformance"
)

func TestConformance(t *testing.T) {
	requireBroker(t)
	conformance.Run(t, conformance.Suite{
		Driver:       Driver{},
		Config:       driver.Config{},
		NewInspector: rabbitInspector,
	})
}

func rabbitInspector(raw driver.Conn) (conformance.Inspect, error) {
	conn, ok := raw.(*conn)
	if !ok {
		return nil, errors.New("RabbitMQ inspector received a different connection")
	}
	return func(ctx context.Context, destination string) (conformance.BrokerView, error) {
		if err := ctx.Err(); err != nil {
			return conformance.BrokerView{}, err
		}
		channel, err := conn.amqp.Channel()
		if err != nil {
			return conformance.BrokerView{}, err
		}
		queue, err := channel.QueueInspect(destination)
		if err != nil {
			_ = channel.Close()
			return conformance.BrokerView{}, errors.Join(driver.ErrDestinationMissing, err)
		}
		parked := 0
		conn.mu.RLock()
		_, deferred := conn.deferred[destination]
		active := make([]*consumer, 0, len(conn.active))
		for item := range conn.active {
			active = append(active, item)
		}
		conn.mu.RUnlock()
		if deferred {
			park, parkErr := channel.QueueInspect(destination + ".park")
			if parkErr == nil {
				parked = park.Messages
			} else if !isNotFound(parkErr) {
				_ = channel.Close()
				return conformance.BrokerView{}, parkErr
			}
		}
		_ = channel.Close()
		var unsettled int64
		for _, item := range active {
			item.mu.Lock()
			for settler := range item.settlers {
				if settler.destination == destination {
					unsettled++
				}
			}
			for _, lane := range item.lanes {
				if lane.destination == destination {
					unsettled += int64(len(lane.pending) + len(lane.deliveries))
				}
			}
			item.mu.Unlock()
		}
		return conformance.BrokerView{
			Ready:     int64(queue.Messages),
			Unsettled: unsettled,
			Auxiliary: int64(parked),
		}, nil
	}, nil
}
