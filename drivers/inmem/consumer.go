package inmem

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type consumer struct {
	conn         *conn
	cfg          driver.ConsumerConfig
	destinations []string
	messages     chan driver.InboundMessage
	errs         chan error
	paused       map[string]bool
	draining     bool
	stopped      bool
	startAfter   map[string]uint64
	outstanding  int
	unsettled    map[string]int
	unsettledKey map[deliveryKey]int
	// ackDeadline is the interval after which an unsettled delivery is
	// requeued, the in-memory stand-in for the broker's consumer timeout. It is
	// set only by the conformance deadline fixture, which needs a controllable
	// ack deadline to prove that a parked delivery never consumes one; a
	// consumer created through Consumer takes no deadline.
	ackDeadline             time.Duration
	inflight                map[*settler]struct{}
	stopOutstandingFailures uint64
}

type deliveryKey struct {
	destination string
	key         string
}

var _ driver.Consumer = (*consumer)(nil)

// Messages returns the channel of delivered messages. The channel closes when Stop or Release completes.
func (c *consumer) Messages() <-chan driver.InboundMessage { return c.messages }

// Errors returns asynchronous consumer errors. The channel closes when Stop or Release completes.
func (c *consumer) Errors() <-chan error { return c.errs }

// Pause pauses delivery from the listed destinations. With no destinations, it pauses all destinations assigned to this consumer.
func (c *consumer) Pause(destinations ...string) error {
	return c.setPaused(destinations, true)
}

// Resume resumes delivery from the listed destinations. With no destinations, it resumes all destinations assigned to this consumer.
func (c *consumer) Resume(destinations ...string) error {
	return c.setPaused(destinations, false)
}

func (c *consumer) setPaused(destinations []string, paused bool) error {
	c.conn.mu.Lock()
	defer c.conn.mu.Unlock()
	if c.stopped {
		return classify("consumer", driver.KindFatal, errors.New("consumer stopped"))
	}
	if len(destinations) == 0 {
		destinations = c.destinations
	}
	for _, name := range destinations {
		if !contains(c.destinations, name) {
			return classify("consumer", driver.KindNotFound, driver.ErrDestinationMissing)
		}
		c.paused[name] = paused
	}
	c.conn.dispatchLocked()
	c.conn.signalWake()
	return nil
}

// Drain stops new deliveries while leaving outstanding messages available for settlement.
func (c *consumer) Drain(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("drain", driver.KindTransient, err)
	}
	c.conn.mu.Lock()
	defer c.conn.mu.Unlock()
	c.draining = true
	c.conn.signalWake()
	return nil
}

// Stop ends the consumer and closes its message and error channels. It returns an error while messages remain unsettled.
func (c *consumer) Stop(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return classify("stop", driver.KindTransient, fmt.Errorf("%w: %w", driver.ErrDrainTimeout, err))
		}
		return classify("stop", driver.KindTransient, err)
	}
	c.conn.mu.Lock()
	defer c.conn.mu.Unlock()
	if c.stopped {
		return nil
	}
	if c.outstanding != 0 {
		c.stopOutstandingFailures++
		return classify("stop", driver.KindFatal, fmt.Errorf("%w: cannot stop with %d outstanding messages", driver.ErrResourcesOutstanding, c.outstanding))
	}
	c.stopped = true
	c.detachLocked()
	close(c.messages)
	close(c.errs)
	c.conn.dispatchLocked()
	c.conn.signalWake()
	return nil
}

// Release ends the consumer, requeues its unsettled messages, and closes its message and error channels.
func (c *consumer) Release(ctx context.Context) error {
	c.conn.mu.Lock()
	defer c.conn.mu.Unlock()
	if c.stopped {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return classify("release", driver.KindTransient, err)
	}
	for delivery := range c.inflight {
		c.conn.requeueDeliveryLocked(delivery, c.conn.clock.Now())
	}
	c.stopped = true
	c.detachLocked()
	close(c.messages)
	close(c.errs)
	c.conn.dispatchLocked()
	c.conn.signalWake()
	return nil
}

// detachLocked removes the consumer from the connection and from every
// destination it is attached to, and clears a destination's replay history
// once its last consumer leaves. History is only a replay source for a group
// that attaches after every prior consumer has gone, so keeping it past the
// last detach would hand a later group entries from before the destination
// went idle. The caller must have marked the consumer stopped and must hold
// c.conn.mu.
func (c *consumer) detachLocked() {
	for _, name := range c.destinations {
		destination := c.conn.destinations[name]
		delete(destination.consumers, c)
		order := destination.order
		for i, current := range order {
			if current == c {
				destination.order = append(order[:i], order[i+1:]...)
				break
			}
		}
		if len(destination.consumers) == 0 {
			c.conn.history[name] = nil
		}
	}
	delete(c.conn.consumers, c)
}

// Lag returns a backlog count for each destination assigned to this consumer.
func (c *consumer) Lag(ctx context.Context) (map[string]int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("lag", driver.KindTransient, err)
	}
	if !c.cfg.Effective.LagQueryable {
		return nil, classify("lag", driver.KindFatal, driver.ErrUnsupported)
	}
	c.conn.mu.Lock()
	defer c.conn.mu.Unlock()
	c.conn.dispatchLocked()
	out := make(map[string]int64, len(c.destinations))
	for _, name := range c.destinations {
		out[name] = int64(len(c.conn.destinations[name].messages))
	}
	return out, nil
}

func contains(values []string, want string) bool {
	return slices.Contains(values, want)
}

type settler struct {
	mu          sync.Mutex
	conn        *conn
	consumer    *consumer
	message     *queuedMessage
	settled     bool
	deliveredAt time.Time
}

var _ driver.Settler = (*settler)(nil)

// Ack acknowledges the delivery and releases its outstanding slot.
func (s *settler) Ack(ctx context.Context) error { return s.settle(ctx, driver.NackOptions{}, false) }

// Nack settles the delivery negatively. When Requeue is true, the message is returned for redelivery.
func (s *settler) Nack(ctx context.Context, opt driver.NackOptions) error {
	return s.settle(ctx, opt, true)
}

func (s *settler) settle(ctx context.Context, opt driver.NackOptions, nack bool) error {
	if err := ctx.Err(); err != nil {
		return classify("settle", driver.KindTransient, err)
	}
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.settled {
		return classify("settle", driver.KindFatal, driver.ErrAlreadySettled)
	}
	if nack && s.conn.failNextNack {
		s.conn.failNextNack = false
		s.conn.nackFailures++
		return classify("nack", driver.KindTransient, errors.New("injected nack failure"))
	}
	if !nack && s.conn.failNextAck {
		s.conn.failNextAck = false
		s.conn.ackFailures++
		return classify("ack", driver.KindTransient, errors.New("injected ack failure"))
	}
	s.retireLocked()
	if nack && opt.Requeue {
		s.message.deliveryCount++
		s.message.due = s.conn.clock.Now()
		if s.message.deliveredGroups != nil {
			delete(s.message.deliveredGroups, s.consumer.cfg.Group)
		}
		if dest, ok := s.conn.destinations[s.message.message.Destination]; ok {
			dest.requeueLocked(s.message)
		}
	}
	s.conn.dispatchLocked()
	s.conn.signalWake()
	return nil
}

// retireLocked releases a delivery's credit and key affinity after admission.
// The caller must hold s.conn.mu and s.mu and reject already-settled deliveries.
func (s *settler) retireLocked() {
	s.settled = true
	delete(s.consumer.inflight, s)
	if s.consumer.outstanding > 0 {
		s.consumer.outstanding--
	}
	name := s.message.message.Destination
	if s.consumer.unsettled[name] > 0 {
		s.consumer.unsettled[name]--
	}
	key := string(s.message.message.Key)
	if key != "" {
		keyID := deliveryKey{destination: name, key: key}
		if s.consumer.unsettledKey[keyID] > 1 {
			s.consumer.unsettledKey[keyID]--
		} else {
			delete(s.consumer.unsettledKey, keyID)
			affinity := affinityKey{group: s.consumer.cfg.Group, key: key}
			if dest, ok := s.conn.destinations[name]; ok && dest.affinity[affinity] == s.consumer {
				delete(dest.affinity, affinity)
			}
		}
	}
}
