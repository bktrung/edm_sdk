package rabbitmq

import (
	"context"
	"sync"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	amqp "github.com/rabbitmq/amqp091-go"
)

type settler struct {
	mu       sync.Mutex
	owner    *consumer
	delivery amqp.Delivery
	settled  bool
}

var _ driver.Settler = (*settler)(nil)

func (s *settler) Ack(ctx context.Context) error {
	return s.settle(ctx, driver.NackOptions{}, false)
}

func (s *settler) Nack(ctx context.Context, options driver.NackOptions) error {
	return s.settle(ctx, options, true)
}

func (s *settler) settle(ctx context.Context, options driver.NackOptions, nack bool) error {
	if err := ctx.Err(); err != nil {
		return classify("settle", driver.KindTransient, err)
	}
	s.mu.Lock()
	if s.settled {
		s.mu.Unlock()
		return classify("settle", driver.KindFatal, driver.ErrAlreadySettled)
	}
	s.mu.Unlock()

	var err error
	if nack {
		err = s.delivery.Nack(false, options.Requeue)
	} else {
		err = s.delivery.Ack(false)
	}
	if err != nil {
		return classifyAMQP("settle", driver.KindTransient, err)
	}

	s.mu.Lock()
	if s.settled {
		s.mu.Unlock()
		return classify("settle", driver.KindFatal, driver.ErrAlreadySettled)
	}
	s.settled = true
	s.mu.Unlock()
	s.owner.release(s)
	return nil
}
