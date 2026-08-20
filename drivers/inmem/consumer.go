package inmem

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type consumer struct {
	conn                    *conn
	cfg                     driver.ConsumerConfig
	destinations            []string
	messages                chan driver.InboundMessage
	errs                    chan error
	paused                  map[string]bool
	draining                bool
	stopped                 bool
	group                   *groupState
	startAfter              map[string]uint64
	outstanding             int
	unsettled               map[string]int
	unsettledKey            map[deliveryKey]int
	ackDeadline             time.Duration
	inflight                map[*settler]struct{}
	stopOutstandingFailures uint64
}

type deliveryKey struct {
	destination string
	key         string
}

var _ driver.Consumer = (*consumer)(nil)

func (c *consumer) Messages() <-chan driver.InboundMessage { return c.messages }
func (c *consumer) Errors() <-chan error                   { return c.errs }

func (c *consumer) Pause(destinations ...string) error {
	return c.setPaused(destinations, true)
}

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
		return classify("stop", driver.KindFatal, fmt.Errorf("cannot stop with %d outstanding messages", c.outstanding))
	}
	c.stopped = true
	for _, name := range c.destinations {
		delete(c.conn.destinations[name].consumers, c)
		order := c.conn.destinations[name].order
		for i, current := range order {
			if current == c {
				c.conn.destinations[name].order = append(order[:i], order[i+1:]...)
				break
			}
		}
		if len(c.conn.destinations[name].consumers) == 0 {
			c.conn.history[name] = nil
		}
	}
	delete(c.conn.consumers, c)
	close(c.messages)
	close(c.errs)
	c.conn.dispatchLocked()
	c.conn.signalWake()
	return nil
}

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
	for _, name := range c.destinations {
		delete(c.conn.destinations[name].consumers, c)
		order := c.conn.destinations[name].order
		for i, current := range order {
			if current == c {
				c.conn.destinations[name].order = append(order[:i], order[i+1:]...)
				break
			}
		}
		if len(c.conn.destinations[name].consumers) == 0 {
			c.conn.history[name] = nil
		}
	}
	delete(c.conn.consumers, c)
	close(c.messages)
	close(c.errs)
	c.conn.dispatchLocked()
	c.conn.signalWake()
	return nil
}

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
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
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

func (s *settler) Ack(ctx context.Context) error { return s.settle(ctx, driver.NackOptions{}, false) }

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
		deliveryKey := deliveryKey{destination: name, key: key}
		if s.consumer.unsettledKey[deliveryKey] > 1 {
			s.consumer.unsettledKey[deliveryKey]--
		} else {
			delete(s.consumer.unsettledKey, deliveryKey)
			if dest, ok := s.conn.destinations[name]; ok && dest.affinity[key] == s.consumer {
				delete(dest.affinity, key)
			}
		}
	}
	if nack && opt.Requeue {
		s.message.deliveryCount++
		s.message.due = s.conn.clock.Now()
		if s.message.deliveredGroups != nil {
			delete(s.message.deliveredGroups, s.consumer.cfg.Group)
		}
		if dest, ok := s.conn.destinations[s.message.message.Destination]; ok {
			dest.messages = append([]*queuedMessage{s.message}, dest.messages...)
		}
	}
	if s.consumer.group != nil && (!nack || !opt.Requeue) {
		if s.message.sequence > s.consumer.group.positions[name] {
			s.consumer.group.positions[name] = s.message.sequence
		}
	}
	s.conn.dispatchLocked()
	s.conn.signalWake()
	return nil
}
