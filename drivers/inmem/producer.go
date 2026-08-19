package inmem

import (
	"context"
	"errors"
	"fmt"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type producer struct {
	conn   *conn
	cfg    driver.ProducerConfig
	closed bool
}

var _ driver.Producer = (*producer)(nil)

func (p *producer) Publish(ctx context.Context, messages ...driver.OutboundMessage) error {
	if err := ctx.Err(); err != nil {
		return classify("publish", driver.KindTransient, err)
	}
	p.conn.mu.Lock()
	defer p.conn.mu.Unlock()
	if p.closed {
		return classify("publish", driver.KindFatal, errors.New("producer closed"))
	}
	if p.conn.closed {
		return classify("publish", driver.KindFatal, errors.New("connection closed"))
	}
	if p.conn.failPublishFatal {
		p.conn.failPublishFatal = false
		return classify("publish", driver.KindFatal, errors.New("injected fatal publish failure"))
	}
	if p.conn.failPublish {
		p.conn.failPublish = false
		return classify("publish", driver.KindTransient, errors.New("injected publish failure"))
	}
	failed := make(map[int]error)
	for i, message := range messages {
		if p.cfg.Effective.MaxMessageBytes > 0 && len(message.Body) > p.cfg.Effective.MaxMessageBytes {
			failed[i] = classify("publish", driver.KindTooLarge, fmt.Errorf("message body exceeds MaxMessageBytes (%d)", p.cfg.Effective.MaxMessageBytes))
			continue
		}
		if p.cfg.Effective.MaxHeaderBytes > 0 && headerBytes(message.Headers) > p.cfg.Effective.MaxHeaderBytes {
			failed[i] = classify("publish", driver.KindTooLarge, fmt.Errorf("message headers exceed MaxHeaderBytes (%d)", p.cfg.Effective.MaxHeaderBytes))
			continue
		}
		dest, ok := p.conn.destinations[message.Destination]
		if !ok {
			failed[i] = classify("publish", driver.KindNotFound, driver.ErrDestinationMissing)
			continue
		}
		due := message.DelayUntil
		if due.IsZero() && dest.spec.Delay > 0 {
			due = p.conn.clock.Now().Add(dest.spec.Delay)
		}
		queued := &queuedMessage{
			message:  cloneOutbound(message),
			sequence: p.conn.nextMessage.Add(1),
			due:      due,
		}
		dest.messages = append(dest.messages, queued)
		p.conn.recordHistoryLocked(message.Destination, &queuedMessage{
			message:  cloneOutbound(message),
			sequence: queued.sequence,
			due:      due,
		})
	}
	p.conn.dispatchLocked()
	p.conn.signalWake()
	if len(failed) != 0 {
		return &driver.PublishError{Failed: failed}
	}
	return nil
}

func headerBytes(headers []driver.Header) int {
	total := 0
	for _, header := range headers {
		total += len(header.Key) + len(header.Value)
	}
	return total
}

func (p *producer) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("flush", driver.KindTransient, err)
	}
	return nil
}

func (p *producer) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("producer.close", driver.KindTransient, err)
	}
	p.conn.mu.Lock()
	defer p.conn.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	p.conn.producers--
	return nil
}

func cloneOutbound(message driver.OutboundMessage) driver.OutboundMessage {
	message.Key = cloneBytes(message.Key)
	message.Headers = cloneHeaders(message.Headers)
	message.Body = cloneBytes(message.Body)
	return message
}
