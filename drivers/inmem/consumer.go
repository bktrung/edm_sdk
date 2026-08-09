package inmem

import (
	"context"
	"errors"
	"fmt"
	"sync"

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
	outstanding  int
}

var _ driver.Consumer = (*consumer)(nil)

func (c *consumer) Messages() <-chan driver.InboundMessage { return c.messages }
func (c *consumer) Errors() <-chan error                   { return c.errs }

func (c *consumer) Pause(destinations ...string) error {
	return c.setPaused(destinations, true)
}

// Quiesce has the same visible state as Pause in this driver.
func (c *consumer) Quiesce(destinations ...string) error {
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

func (c *consumer) Drain(context.Context) error {
	c.conn.mu.Lock()
	defer c.conn.mu.Unlock()
	c.draining = true
	c.conn.signalWake()
	return nil
}

func (c *consumer) Stop(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("stop", driver.KindTransient, err)
	}
	c.conn.mu.Lock()
	defer c.conn.mu.Unlock()
	if c.stopped {
		return nil
	}
	if c.outstanding != 0 {
		return classify("stop", driver.KindFatal, fmt.Errorf("cannot stop with %d outstanding messages", c.outstanding))
	}
	c.stopped = true
	for _, name := range c.destinations {
		delete(c.conn.destinations[name].consumers, c)
		for key, owner := range c.conn.destinations[name].affinity {
			if owner == c {
				delete(c.conn.destinations[name].affinity, key)
			}
		}
		order := c.conn.destinations[name].order
		for i, current := range order {
			if current == c {
				c.conn.destinations[name].order = append(order[:i], order[i+1:]...)
				break
			}
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
	mu       sync.Mutex
	conn     *conn
	consumer *consumer
	message  *queuedMessage
	settled  bool
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.settled {
		return classify("settle", driver.KindFatal, driver.ErrAlreadySettled)
	}
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	s.settled = true
	if s.consumer.outstanding > 0 {
		s.consumer.outstanding--
	}
	if nack && opt.Requeue {
		s.message.deliveryCount++
		s.message.due = s.conn.clock.Now()
		if dest, ok := s.conn.destinations[s.message.message.Destination]; ok {
			dest.messages = append(dest.messages, s.message)
		}
	}
	s.conn.dispatchLocked()
	s.conn.signalWake()
	return nil
}
