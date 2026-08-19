package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type producer struct {
	conn     *conn
	channel  *amqp.Channel
	confirms chan amqp.Confirmation
	returns  chan amqp.Return
	mu       sync.Mutex
	closed   bool
}

var _ driver.Producer = (*producer)(nil)

func newProducer(conn *conn, cfg driver.ProducerConfig) (*producer, error) {
	_ = cfg
	conn.mu.RLock()
	if conn.closed || conn.amqp.IsClosed() {
		conn.mu.RUnlock()
		return nil, classify("producer", driver.KindTransient, amqp.ErrClosed)
	}
	channel, err := conn.amqp.Channel()
	conn.mu.RUnlock()
	if err != nil {
		return nil, classifyAMQP("producer", driver.KindTransient, err)
	}
	if err := channel.Confirm(false); err != nil {
		_ = channel.Close()
		return nil, classifyAMQP("producer", driver.KindFatal, err)
	}
	p := &producer{
		conn:     conn,
		channel:  channel,
		confirms: make(chan amqp.Confirmation, 1),
		returns:  make(chan amqp.Return, 1),
	}
	channel.NotifyPublish(p.confirms)
	channel.NotifyReturn(p.returns)
	return p, nil
}

func (p *producer) Publish(ctx context.Context, msgs ...driver.OutboundMessage) error {
	if err := ctx.Err(); err != nil {
		return classify("publish", driver.KindTransient, err)
	}
	if len(msgs) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return classify("publish", driver.KindTransient, amqp.ErrClosed)
	}

	failed := make(map[int]error)
	for index, message := range msgs {
		if err := ctx.Err(); err != nil {
			for remaining := index; remaining < len(msgs); remaining++ {
				failed[remaining] = classify("publish", driver.KindTransient, err)
			}
			break
		}
		if err := p.publishOne(ctx, message); err != nil {
			failed[index] = err
		}
	}
	if len(failed) != 0 {
		return &driver.PublishError{Failed: failed}
	}
	return nil
}

func (p *producer) publishOne(ctx context.Context, message driver.OutboundMessage) error {
	exchange, routingKey, expiration := p.target(message)
	publishing, err := amqpPublishing(message)
	if err != nil {
		return classify("publish", driver.KindFatal, err)
	}
	publishing.Expiration = expiration
	if err := p.channel.PublishWithContext(ctx, exchange, routingKey, true, false, publishing); err != nil {
		return classifyAMQP("publish", driver.KindTransient, err)
	}
	return p.waitConfirm(ctx)
}

// target decides AMQP routing for one outbound message. Whether Destination
// names a fan-out entry point is never looked up here: the core already
// resolved that, in one read of one effective capability set, and carries
// the answer on the message itself. A driver that looked the answer up
// again in its own state could disagree with what the core declared, which
// is the defect this replaces.
//
// The deferred-destination read is kept, deliberately, against the letter
// of the exchange-routing fix above: the driver port's conformance
// requirement is that a destination declared with a native delay supplies
// the due time when DelayUntil is zero, so a publisher that never learned
// the delay can still be parked correctly. This is the one cache read this
// function keeps, and it is read-only against state EnsureTopology already
// populated - the publish path still declares nothing and still never calls
// ensureParking. Routing to the parking queue name itself always prefers
// OutboundMessage.DelayUntil when the core supplied one.
func (p *producer) target(message driver.OutboundMessage) (exchange, routingKey, expiration string) {
	destination := message.Destination
	p.conn.mu.RLock()
	delay, isDeferred := p.conn.deferred[destination]
	p.conn.mu.RUnlock()
	if isDeferred || !message.DelayUntil.IsZero() {
		now := time.Now() //nolint:forbidigo // the driver computes remaining delay at publish time
		due := message.DelayUntil
		if due.IsZero() {
			due = now.Add(delay)
		}
		if remaining := due.Sub(now); remaining > 0 {
			return "", destination + ".park", expirationMillis(remaining)
		}
	}
	if message.EntryPoint {
		return destination, "", ""
	}
	return "", destination, ""
}

const maxExpirationMillis int64 = 2147483647

func expirationMillis(remaining time.Duration) string {
	millis := remaining / time.Millisecond
	if remaining%time.Millisecond != 0 {
		millis++
	}
	if millis < 1 {
		millis = 1
	}
	if millis > time.Duration(maxExpirationMillis) {
		millis = time.Duration(maxExpirationMillis)
	}
	return strconv.FormatInt(int64(millis), 10)
}

func (p *producer) waitConfirm(ctx context.Context) error {
	var returned *amqp.Return
	for {
		select {
		case item, ok := <-p.returns:
			if !ok {
				return classify("publish", driver.KindTransient, amqp.ErrClosed)
			}
			returned = &item
		case confirmation, ok := <-p.confirms:
			if !ok {
				return classify("publish", driver.KindTransient, amqp.ErrClosed)
			}
			if !confirmation.Ack {
				return classify("publish", driver.KindTransient, errors.New("rabbitmq: publisher confirm was negative"))
			}
			if returned == nil {
				select {
				case item, ok := <-p.returns:
					if ok {
						returned = &item
					}
				default:
				}
			}
			if returned != nil {
				return returnedPublishError(*returned)
			}
			return nil
		case <-ctx.Done():
			return classify("publish", driver.KindTransient, ctx.Err())
		}
	}
}

func returnedPublishError(returned amqp.Return) error {
	reason := fmt.Errorf("rabbitmq: publish returned by broker: %d %s", returned.ReplyCode, returned.ReplyText)
	if returned.ReplyCode == 312 {
		return classify("publish", driver.KindNotFound, errors.Join(driver.ErrDestinationMissing, reason))
	}
	return classify("publish", driver.KindFatal, reason)
}

func amqpPublishing(message driver.OutboundMessage) (amqp.Publishing, error) {
	headers := make(amqp.Table, len(message.Headers)+1)
	publishing := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Headers:      headers,
		Body:         append([]byte(nil), message.Body...),
	}
	// A fanout exchange ignores the AMQP routing key, so the ordering identity
	// needs a header of its own. It is deliberately not a cloudEvents: header:
	// it is a transport detail rather than an envelope attribute, and it must
	// not surface in the portable header set on the way back.
	if len(message.Key) > 0 {
		headers[partitionKeyHeader] = string(message.Key)
	}
	for _, header := range message.Headers {
		value := string(header.Value)
		switch header.Key {
		case "id":
			publishing.MessageId = value
			headers["cloudEvents:id"] = value
		case "time":
			parsed, err := time.Parse(time.RFC3339Nano, value)
			if err != nil {
				return amqp.Publishing{}, fmt.Errorf("invalid time header: %w", err)
			}
			publishing.Timestamp = parsed
			headers["cloudEvents:time"] = value
		case "type":
			publishing.Type = value
			headers["cloudEvents:type"] = value
		case "datacontenttype":
			publishing.ContentType = value
		case "f1correlationid":
			publishing.CorrelationId = value
		default:
			headers["cloudEvents:"+header.Key] = value
		}
	}
	return publishing, nil
}

func (p *producer) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("flush", driver.KindTransient, err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return classify("flush", driver.KindTransient, amqp.ErrClosed)
	}
	return nil
}

func (p *producer) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("producer.close", driver.KindTransient, err)
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	err := p.channel.Close()
	p.mu.Unlock()
	p.conn.removeProducer(p)
	if errors.Is(err, amqp.ErrClosed) {
		return nil
	}
	if err != nil {
		return classifyAMQP("producer.close", driver.KindTransient, err)
	}
	return nil
}
