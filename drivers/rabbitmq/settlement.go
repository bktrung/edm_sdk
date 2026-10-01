package rabbitmq

import (
	"context"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type settler struct {
	mu          sync.Mutex
	owner       *consumer
	destination string
	delivery    amqp.Delivery
	settling    bool
	settled     bool
	// generation is the lane generation of the consumer that received the
	// delivery. counted reports whether it holds a place in the consumer's
	// admission counts, which ends when it settles or when the broker cancels
	// that consumer. Both are guarded by owner.mu.
	generation uint64
	counted    bool
}

var _ driver.Settler = (*settler)(nil)

// Ack acknowledges the delivery.
func (s *settler) Ack(ctx context.Context) error {
	return s.settle(ctx, driver.NackOptions{}, false)
}

// Nack negatively acknowledges the delivery. When Requeue is true, RabbitMQ can redeliver the message.
func (s *settler) Nack(ctx context.Context, options driver.NackOptions) error {
	return s.settle(ctx, options, true)
}

// settle performs the one Ack or Nack this settler allows. The context is
// checked once, before the broker call, and never during it: amqp091-go v1.13.0
// Channel.Ack and Channel.Nack take no context, hold the channel mutex and write
// a single frame to the socket with no write deadline, so a settlement already
// in flight cannot be cancelled - only closing the connection ends it. Checking
// the context again after the call could not undo the settlement either: it
// would report a durably settled delivery as unsettled while a retry answers
// ErrAlreadySettled, so a cancelled context refuses the settlement up front and
// leaves the delivery for broker redelivery.
func (s *settler) settle(ctx context.Context, options driver.NackOptions, nack bool) error {
	if err := ctx.Err(); err != nil {
		return classify("settle", driver.KindTransient, err)
	}
	s.mu.Lock()
	if s.settled || s.settling {
		s.mu.Unlock()
		return classify("settle", driver.KindFatal, driver.ErrAlreadySettled)
	}
	s.settling = true
	s.mu.Unlock()

	var err error
	if nack {
		err = s.delivery.Nack(false, options.Requeue)
	} else {
		err = s.delivery.Ack(false)
	}
	if err != nil {
		s.mu.Lock()
		s.settling = false
		s.mu.Unlock()
		return classifyAMQP("settle", driver.KindTransient, err)
	}

	s.mu.Lock()
	s.settling = false
	s.settled = true
	s.mu.Unlock()
	s.owner.release(s)
	return nil
}
