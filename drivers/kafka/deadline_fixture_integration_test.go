//go:build integration

package kafka

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver/conformance"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// deadlineFixture models a Kafka consumer that dies before committing. Kafka has
// no per-message ack deadline, so Advance returns delivered, unsettled records
// by releasing and reopening the consumer on the same group.
type deadlineFixture struct {
	conn        *conn
	mu          sync.Mutex
	deadlineNow time.Time
	consumers   map[*deadlineConsumer]struct{}
}

func newDeadlineFixture(raw driver.Conn) (conformance.DeadlineFixture, error) {
	connection, ok := raw.(*conn)
	if !ok {
		return nil, errors.New("kafka conformance deadline fixture received a different connection")
	}
	return &deadlineFixture{
		conn:        connection,
		deadlineNow: clock.NewReal().Now(),
		consumers:   make(map[*deadlineConsumer]struct{}),
	}, nil
}

func (f *deadlineFixture) Consumer(ctx context.Context, deadline time.Duration, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.Group == "" {
		group, err := newConsumerGroup()
		if err != nil {
			return nil, err
		}
		cfg.Group = group
	}
	consumer, err := f.conn.Consumer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	managed := newDeadlineConsumer(f, ctx, deadline, cfg, consumer)
	f.mu.Lock()
	f.consumers[managed] = struct{}{}
	f.mu.Unlock()
	return managed, nil
}

func (f *deadlineFixture) Now() time.Time {
	return clock.NewReal().Now()
}

func (f *deadlineFixture) syntheticNow() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deadlineNow
}

func (f *deadlineFixture) Advance(duration time.Duration) {
	f.mu.Lock()
	f.deadlineNow = f.deadlineNow.Add(duration)
	now := f.deadlineNow
	consumers := make([]*deadlineConsumer, 0, len(f.consumers))
	for consumer := range f.consumers {
		consumers = append(consumers, consumer)
	}
	f.mu.Unlock()

	expired := make([]*deadlineConsumer, 0, len(consumers))
	for _, consumer := range consumers {
		if consumer.expired(now) {
			expired = append(expired, consumer)
		}
	}
	for _, consumer := range expired {
		consumer.expire(now)
	}
}

func (f *deadlineFixture) remove(consumer *deadlineConsumer) {
	f.mu.Lock()
	delete(f.consumers, consumer)
	f.mu.Unlock()
}

// deadlineConsumer presents one stable consumer to the conformance harness while
// replacing its Kafka consumer after an expired synthetic deadline.
type deadlineConsumer struct {
	fixture  *deadlineFixture
	conn     *conn
	ctx      context.Context
	deadline time.Duration
	cfg      driver.ConsumerConfig

	mu          sync.Mutex
	current     driver.Consumer
	outstanding map[*deadlineSettler]time.Time
	closed      bool
	wake        chan struct{}
	done        chan struct{}
	closeOnce   sync.Once
	opMu        sync.Mutex
	messages    chan driver.InboundMessage
	errors      chan error
}

func newDeadlineConsumer(
	fixture *deadlineFixture,
	ctx context.Context,
	deadline time.Duration,
	cfg driver.ConsumerConfig,
	consumer driver.Consumer,
) *deadlineConsumer {
	managed := &deadlineConsumer{
		fixture:     fixture,
		conn:        fixture.conn,
		ctx:         ctx,
		deadline:    deadline,
		cfg:         cfg,
		current:     consumer,
		outstanding: make(map[*deadlineSettler]time.Time),
		wake:        make(chan struct{}),
		done:        make(chan struct{}),
		messages:    make(chan driver.InboundMessage),
		errors:      make(chan error, 8),
	}
	go managed.forwardMessages()
	go managed.forwardErrors()
	return managed
}

func (c *deadlineConsumer) Messages() <-chan driver.InboundMessage { return c.messages }

func (c *deadlineConsumer) Errors() <-chan error { return c.errors }

func (c *deadlineConsumer) Pause(destinations ...string) error {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	consumer := c.current
	c.mu.Unlock()
	if consumer == nil {
		return errors.New("kafka deadline fixture consumer is unavailable")
	}
	return consumer.Pause(destinations...)
}

func (c *deadlineConsumer) Resume(destinations ...string) error {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	consumer := c.current
	c.mu.Unlock()
	if consumer == nil {
		return errors.New("kafka deadline fixture consumer is unavailable")
	}
	return consumer.Resume(destinations...)
}

func (c *deadlineConsumer) Drain(ctx context.Context) error {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	consumer := c.current
	c.mu.Unlock()
	if consumer == nil {
		return errors.New("kafka deadline fixture consumer is unavailable")
	}
	return consumer.Drain(ctx)
}

func (c *deadlineConsumer) Stop(ctx context.Context) error {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	consumer := c.current
	c.mu.Unlock()
	if consumer == nil {
		return errors.New("kafka deadline fixture consumer is unavailable")
	}
	if err := consumer.Stop(ctx); err != nil {
		return err
	}
	return c.finish(ctx)
}

func (c *deadlineConsumer) Release(ctx context.Context) error {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	consumer := c.current
	c.mu.Unlock()
	if consumer == nil {
		return errors.New("kafka deadline fixture consumer is unavailable")
	}
	if err := consumer.Release(ctx); err != nil {
		return err
	}
	return c.finish(ctx)
}

func (c *deadlineConsumer) Lag(ctx context.Context) (map[string]int64, error) {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil
	}
	consumer := c.current
	c.mu.Unlock()
	if consumer == nil {
		return nil, errors.New("kafka deadline fixture consumer is unavailable")
	}
	return consumer.Lag(ctx)
}

func (c *deadlineConsumer) expired(now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	for _, expiresAt := range c.outstanding {
		if !now.Before(expiresAt) {
			return true
		}
	}
	return false
}

func (c *deadlineConsumer) expire(now time.Time) {
	c.opMu.Lock()
	defer c.opMu.Unlock()

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	var expired bool
	for _, expiresAt := range c.outstanding {
		if !now.Before(expiresAt) {
			expired = true
			break
		}
	}
	if !expired {
		c.mu.Unlock()
		return
	}
	old := c.current
	if old == nil {
		c.mu.Unlock()
		return
	}
	c.current = nil
	c.signalWakeLocked()
	c.mu.Unlock()

	// Kafka's Release operation returns every unsettled record in the group.
	if err := old.Release(c.ctx); err != nil {
		c.mu.Lock()
		if !c.closed {
			c.current = old
			c.signalWakeLocked()
		}
		c.mu.Unlock()
		c.reportError(err)
		return
	}

	replacement, err := c.conn.Consumer(c.ctx, c.cfg)
	if err != nil {
		c.reportError(err)
		return
	}
	c.mu.Lock()
	clear(c.outstanding)
	c.current = replacement
	c.signalWakeLocked()
	c.mu.Unlock()
}

func (c *deadlineConsumer) finish(ctx context.Context) error {
	var deleteErr error
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.current = nil
		clear(c.outstanding)
		close(c.done)
		c.signalWakeLocked()
		c.mu.Unlock()
		c.fixture.remove(c)

		response, err := kadm.NewClient(c.conn.client).DeleteGroup(ctx, c.cfg.Group)
		if err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
			deleteErr = err
		} else if response.Err != nil && !errors.Is(response.Err, kerr.GroupIDNotFound) {
			deleteErr = response.Err
		}
	})
	return deleteErr
}

func (c *deadlineConsumer) signalWakeLocked() {
	close(c.wake)
	c.wake = make(chan struct{})
}

func (c *deadlineConsumer) reportError(err error) {
	select {
	case c.errors <- err:
	default:
	}
}

func (c *deadlineConsumer) forwardMessages() {
	defer close(c.messages)
	for {
		c.mu.Lock()
		consumer := c.current
		wake := c.wake
		c.mu.Unlock()
		if consumer == nil {
			select {
			case <-c.done:
				return
			case <-wake:
				continue
			}
		}
		input := consumer.Messages()
		for {
			select {
			case message, ok := <-input:
				if !ok {
					select {
					case <-c.done:
						return
					case <-wake:
						break
					}
					goto nextConsumer
				}
				if !c.forwardMessage(message) {
					return
				}
			case <-c.done:
				return
			case <-wake:
				goto nextConsumer
			}
		}
	nextConsumer:
	}
}

func (c *deadlineConsumer) forwardMessage(message driver.InboundMessage) bool {
	var settler *deadlineSettler
	if message.Settle != nil {
		expiresAt := c.fixture.syntheticNow().Add(c.deadline)
		settler = &deadlineSettler{owner: c, inner: message.Settle}
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return false
		}
		c.outstanding[settler] = expiresAt
		c.mu.Unlock()
		message.Settle = settler
	}
	select {
	case c.messages <- message:
		return true
	case <-c.done:
		if settler != nil {
			settler.finish()
		}
		return false
	}
}

func (c *deadlineConsumer) forwardErrors() {
	defer close(c.errors)
	for {
		c.mu.Lock()
		consumer := c.current
		wake := c.wake
		c.mu.Unlock()
		if consumer == nil {
			select {
			case <-c.done:
				return
			case <-wake:
				continue
			}
		}
		input := consumer.Errors()
		for {
			select {
			case err, ok := <-input:
				if !ok {
					select {
					case <-c.done:
						return
					case <-wake:
						break
					}
					goto nextConsumer
				}
				select {
				case c.errors <- err:
				case <-c.done:
					return
				case <-wake:
					goto nextConsumer
				}
			case <-c.done:
				return
			case <-wake:
				goto nextConsumer
			}
		}
	nextConsumer:
	}
}

func (c *deadlineConsumer) forget(settler *deadlineSettler) {
	c.mu.Lock()
	delete(c.outstanding, settler)
	c.mu.Unlock()
}

type deadlineSettler struct {
	owner *deadlineConsumer
	inner driver.Settler
	once  sync.Once
}

func (s *deadlineSettler) Ack(ctx context.Context) error {
	err := s.inner.Ack(ctx)
	if err == nil {
		s.finish()
	}
	return err
}

func (s *deadlineSettler) Nack(ctx context.Context, options driver.NackOptions) error {
	err := s.inner.Nack(ctx, options)
	if err == nil {
		s.finish()
	}
	return err
}

func (s *deadlineSettler) finish() {
	s.once.Do(func() { s.owner.forget(s) })
}
