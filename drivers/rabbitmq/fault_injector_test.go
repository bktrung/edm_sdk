package rabbitmq

import (
	"context"
	"errors"
	"fmt"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver/conformance"
)

func rabbitFaultInjector(raw driver.Conn) (conformance.FaultInjector, error) {
	conn, ok := raw.(*conn)
	if !ok {
		return nil, errors.New("RabbitMQ fault injector received a different connection")
	}
	return func(ctx context.Context, kind conformance.FaultKind) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		switch kind {
		case conformance.FaultPublishFailure:
			conn.publishFault.Store(int32(driver.KindTransient) + 1)
			return nil
		case conformance.FaultFatalPublish:
			conn.publishFault.Store(int32(driver.KindFatal) + 1)
			return nil
		case conformance.FaultConnectionDrop:
			return rabbitRequeueActive(ctx, conn, kind)
		case conformance.FaultDeliveryFailure:
			return rabbitRequeueActive(ctx, conn, kind)
		default:
			return fmt.Errorf("unsupported conformance fault %q", kind)
		}
	}, nil
}

func rabbitRequeueActive(ctx context.Context, conn *conn, kind conformance.FaultKind) error {
	conn.mu.RLock()
	consumers := make([]*consumer, 0, len(conn.active))
	for item := range conn.active {
		consumers = append(consumers, item)
	}
	conn.mu.RUnlock()

	for _, item := range consumers {
		item.mu.Lock()
		settlers := make([]*settler, 0, len(item.settlers))
		for settler := range item.settlers {
			settlers = append(settlers, settler)
		}
		item.mu.Unlock()
		for _, settler := range settlers {
			if err := settler.Nack(ctx, driver.NackOptions{Requeue: true}); err != nil && !errors.Is(err, driver.ErrAlreadySettled) {
				return err
			}
		}
		item.sendError(classify("consumer", driver.KindTransient, fmt.Errorf("injected %s fault", kind)))
	}
	return nil
}
