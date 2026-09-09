package kafka

import (
	"context"
	"errors"
	"fmt"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver/conformance"
)

func kafkaFaultInjector(raw driver.Conn) (conformance.FaultInjector, error) {
	conn, ok := raw.(*conn)
	if !ok {
		return nil, errors.New("Kafka fault injector received a different connection")
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
		case conformance.FaultCloseFailure:
			conn.closeFault.Store(true)
			return nil
		case conformance.FaultConnectionDrop, conformance.FaultDeliveryFailure:
			return kafkaRequeueActive(ctx, conn, kind)
		case conformance.FaultLaneChannelClose:
			return kafkaCloseLane(ctx, conn)
		default:
			return fmt.Errorf("unsupported conformance fault %q", kind)
		}
	}, nil
}

func kafkaRequeueActive(ctx context.Context, conn *conn, kind conformance.FaultKind) error {
	conn.mu.RLock()
	consumers := make([]*consumer, 0, len(conn.consumers))
	for item := range conn.consumers {
		consumers = append(consumers, item)
	}
	conn.mu.RUnlock()

	for _, item := range consumers {
		item.mu.Lock()
		if item.stopped {
			item.mu.Unlock()
			continue
		}
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

func kafkaCloseLane(ctx context.Context, conn *conn) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	conn.mu.RLock()
	consumers := make([]*consumer, 0, len(conn.consumers))
	for item := range conn.consumers {
		consumers = append(consumers, item)
	}
	conn.mu.RUnlock()

	candidates := make([]*consumer, 0, len(consumers))
	for _, item := range consumers {
		item.mu.Lock()
		if !item.stopped && !item.draining && len(item.destinations) >= 2 && item.laneFaultDestination == "" {
			candidates = append(candidates, item)
		}
		item.mu.Unlock()
	}
	if len(candidates) == 0 {
		return errors.New("lane channel close has no live consumer lane")
	}

	consumer := candidates[0]
	consumer.mu.Lock()
	if consumer.stopped || consumer.draining || len(consumer.destinations) < 2 || consumer.laneFaultDestination != "" {
		consumer.mu.Unlock()
		return errors.New("lane channel close has no live consumer lane")
	}
	destination := consumer.destinations[0]
	consumer.laneFaultDestination = destination
	consumer.client.PauseFetchTopics(destination)
	consumer.mu.Unlock()
	consumer.errorsMu.Lock()
	for {
		if consumer.errorsClosed {
			consumer.errorsMu.Unlock()
			return errors.New("lane channel close consumer stopped")
		}
		select {
		case _, ok := <-consumer.errors:
			if !ok {
				consumer.errorsMu.Unlock()
				return errors.New("lane channel close consumer stopped")
			}
		default:
			consumer.errorsMu.Unlock()
			consumer.sendError(classify("consumer", driver.KindTransient, fmt.Errorf("injected %s fault for %s", conformance.FaultLaneChannelClose, destination)))
			return nil
		}
	}
}
