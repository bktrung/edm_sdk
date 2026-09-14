//go:build integration

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
		Driver:           Driver{},
		Config:           driver.Config{Endpoints: []string{defaultEndpoint}},
		NewInspector:     rabbitInspector,
		NewFaultInjector: rabbitFaultInjector,
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
		admin := &adminOperations{conn: conn}
		ready, err := admin.inspectQueue(ctx, destination)
		if err != nil {
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
			for _, parkName := range parkQueueNames(destination) {
				park, parkErr := admin.inspectQueue(ctx, parkName)
				if parkErr == nil {
					parked += int(park)
					continue
				}
				if !isNotFound(parkErr) {
					return conformance.BrokerView{}, parkErr
				}
			}
		}
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
					lane.mu.Lock()
					deliveries, _ := lane.deliveriesState()
					unsettled += int64(len(lane.pending) + len(deliveries) + lane.emitting)
					lane.mu.Unlock()
				}
			}
			item.mu.Unlock()
		}
		return conformance.BrokerView{
			Ready:     ready,
			Unsettled: unsettled,
			Auxiliary: int64(parked),
		}, nil
	}, nil
}
