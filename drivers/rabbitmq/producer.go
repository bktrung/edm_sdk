package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
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

// publishWindowSize is the number of messages Publish publishes before it
// reads a confirmation.
//
// With a single publish in flight a client's throughput is one message per
// confirm round trip, because every message waits for the one before it to be
// durable. Publishing a window first makes W messages cost about one confirm
// latency instead of W. The window is a fixed constant rather than the size of
// the batch because Publish accepts any number of messages, so the state left
// unacknowledged when a channel stops working has to be bounded, and so does
// the number of confirmations a window that is cancelled is still owed. Both
// notify channels are buffered to the same size: the client delivers
// confirmations and returns to a listener with a send made from its own
// dispatch path, and a basic.return arrives on a different channel from the
// confirmation for the same message, so a buffer smaller than the outstanding
// window can stall dispatch. W = 1 is the one-confirm-at-a-time behaviour this
// replaces, which keeps the constant usable for bisecting a regression.
const publishWindowSize = 64

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
		confirms: make(chan amqp.Confirmation, publishWindowSize),
		returns:  make(chan amqp.Return, publishWindowSize),
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
	if raw := p.conn.publishFault.Swap(0); raw != 0 {
		kind := driver.Kind(raw - 1)
		failed := make(map[int]error, len(msgs))
		for index := range msgs {
			failed[index] = classify("publish", kind, errors.New("injected publish fault"))
		}
		return &driver.PublishError{Failed: failed}
	}

	failed := make(map[int]error)
	for base := 0; base < len(msgs); {
		consumed, err := p.publishWindow(ctx, msgs, base, failed)
		if err != nil {
			// The channel is gone. Every index the window did not decide
			// failed for that same reason, and no message after it can be
			// published on the channel either.
			for index := base; index < len(msgs); index++ {
				if _, decided := failed[index]; !decided {
					failed[index] = err
				}
			}
			break
		}
		base += consumed
	}
	if len(failed) != 0 {
		return &driver.PublishError{Failed: failed}
	}
	return nil
}

// publishWindow publishes the window of messages that starts at base and then
// reads one confirmation per published message, in order. It reports how many
// messages it consumed and a non-nil error when the channel must not be
// published on again.
//
// A nil error means every index in the window has a decided outcome: its own
// entry in failed, or a durable confirmation. An error means the channel is
// gone, and the caller reports it for every index the window did not decide.
//
// Confirmations are read in the order the client delivers them, which is
// delivery-tag order whatever order the broker acknowledged in, so the i-th
// confirmation read belongs to the i-th message published here and no
// tag-to-index map is needed.
func (p *producer) publishWindow(ctx context.Context, msgs []driver.OutboundMessage, base int, failed map[int]error) (int, error) {
	type outbound struct {
		index      int
		exchange   string
		routingKey string
		publishing amqp.Publishing
	}
	end := min(base+publishWindowSize, len(msgs))
	window := make([]outbound, 0, end-base)
	ids := make(map[string]struct{}, end-base)
	consumed := 0
	for offset, message := range msgs[base:end] {
		publishing, err := amqpPublishing(message)
		if err != nil {
			failed[base+offset] = classify("publish", driver.KindFatal, err)
			consumed = offset + 1
			continue
		}
		if _, duplicate := ids[publishing.MessageId]; duplicate {
			// A basic.return carries no delivery tag, so the MessageId the
			// broker echoes back is the only thing that identifies which
			// message it belongs to. Two messages sharing one within the same
			// window cannot be told apart, and matching a return for the later
			// one against the earlier would fail the wrong index, so the
			// window stops before the repeat. The core stamps a unique
			// envelope id on every message it publishes, which leaves an
			// id-less window to a driver-level caller.
			break
		}
		ids[publishing.MessageId] = struct{}{}
		exchange, routingKey, expiration := p.target(message)
		publishing.Expiration = expiration
		window = append(window, outbound{
			index:      base + offset,
			exchange:   exchange,
			routingKey: routingKey,
			publishing: publishing,
		})
		consumed = offset + 1
	}
	// Every message reaches the wire before the first confirmation is read, and
	// a message that failed to encode reached it not at all: filtering those
	// out here is what keeps the confirmations in step with the window, since
	// an unpublished message consumes no delivery tag.
	for offset, item := range window {
		if err := ctx.Err(); err != nil {
			if offset > 0 {
				// Messages already published here are unconfirmed, and their
				// confirmations must not be read against a later publish.
				p.invalidateLocked()
			}
			return 0, classify("publish", driver.KindTransient, err)
		}
		if err := p.channel.PublishWithContext(ctx, item.exchange, item.routingKey, true, false, item.publishing); err != nil {
			// A publish that failed on the wire leaves the channel's delivery
			// tags out of step with the messages that were not published.
			p.invalidateLocked()
			return 0, classifyAMQP("publish", driver.KindTransient, err)
		}
	}
	returned := make(map[string]error, len(window))
	for _, item := range window {
		confirmation, err := p.waitConfirm(ctx, returned)
		if err != nil {
			p.invalidateLocked()
			return 0, err
		}
		if !confirmation.Ack {
			failed[item.index] = classify("publish", driver.KindTransient, errors.New("rabbitmq: publisher confirm was negative"))
			continue
		}
		// Match a return by MessageId, never by arrival order: with a window
		// outstanding the return that has arrived next may belong to a later
		// message, and failing this one for it would blame the wrong index.
		// The window holds no duplicate id, so a match is this message.
		if returnedErr, ok := returned[item.publishing.MessageId]; ok {
			delete(returned, item.publishing.MessageId)
			failed[item.index] = parkingFailure(item.routingKey, p.conn.queueKind, returnedErr)
		}
	}
	return consumed, nil
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

// waitConfirm reads the next confirmation for the window, folding into
// returned every broker return the client has already dispatched. A
// basic.return is dispatched before the confirmation for the same message, so
// a return for the message being confirmed is already on the channel when its
// confirmation is read; it arrives on a channel of its own, though, so it is
// parked under its MessageId and matched by the caller when that message's
// confirmation is read.
//
// An error means the confirmation was abandoned on ctx.Done, or one of the two
// streams closed, so the channel must not be reused for a later publish.
func (p *producer) waitConfirm(ctx context.Context, returned map[string]error) (amqp.Confirmation, error) {
	for {
		select {
		case item, ok := <-p.returns:
			if !ok {
				return amqp.Confirmation{}, classify("publish", driver.KindTransient, amqp.ErrClosed)
			}
			returned[item.MessageId] = returnedPublishError(item)
		case confirmation, ok := <-p.confirms:
			if !ok {
				return amqp.Confirmation{}, classify("publish", driver.KindTransient, amqp.ErrClosed)
			}
			if !p.drainReturns(returned) {
				return amqp.Confirmation{}, classify("publish", driver.KindTransient, amqp.ErrClosed)
			}
			return confirmation, nil
		case <-ctx.Done():
			return amqp.Confirmation{}, classify("publish", driver.KindTransient, ctx.Err())
		}
	}
}

// drainReturns parks every return the client has already dispatched and
// reports whether the return stream is still open. It runs before a
// confirmation is acted on for two reasons: the drain is what keeps a window's
// worth of returns from filling the buffer the client's dispatch path sends
// into, and a confirmation can be selected while the return for that same
// message is still queued.
func (p *producer) drainReturns(returned map[string]error) bool {
	for {
		select {
		case item, ok := <-p.returns:
			if !ok {
				return false
			}
			returned[item.MessageId] = returnedPublishError(item)
		default:
			return true
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

// parkingSuffix turns a destination name into the name of its parking queue,
// the queue a delayed or retried message waits in until its TTL expires. The
// suffix is fixed: both the topology path that declares the queue and the
// publish path that routes to it derive the name the same way.
const parkingSuffix = ".park"

// parkingFailure rewrites a not-found publish failure for a message that was
// routed to a parking queue, so the failure names that queue and the topology
// it needs. Nothing else routes a publish to a name carrying this suffix, so a
// not-found there means the parking queue is missing, which is the one failure
// whose cause an operator cannot recover from the broker's own words: the
// broker returns 312 NO_ROUTE and never names the queue it could not route to.
//
// The queue is declared by the adapter under TopologyDeclare, checked under
// TopologyVerify, and operator-provisioned under TopologyNone, so the message
// states that split. Its declare arguments are rendered from parkingArguments
// rather than copied into the text: the two had to be edited together before,
// and an argument added there without a matching edit here would have told an
// operator to create a queue the adapter would then report as drift. Durability
// is a declare flag rather than an entry in that table, so the message states
// it in words instead: the adapter declares the parking queue durable under
// every queue kind, and an operator provisioning one under TopologyNone has
// only this text to read. kind is the connection's own queue kind, because the
// arguments differ by kind and the producer holds no other way back to it.
// Classification is deliberately the wrapped error's own: this adds context
// and changes no kind.
func parkingFailure(routingKey string, kind queueKind, err error) error {
	if !strings.HasSuffix(routingKey, parkingSuffix) || !errors.Is(err, driver.ErrDestinationMissing) {
		return err
	}
	destination := strings.TrimSuffix(routingKey, parkingSuffix)
	return fmt.Errorf(
		"rabbitmq: parking destination %q is missing: a delayed or retried message is parked there"+
			" until its delay expires, so the adapter declares the queue durable under TopologyDeclare"+
			" and requires it under TopologyVerify, and under TopologyNone the operator provisions it"+
			" with the declare arguments %s: %w",
		routingKey, declareArgumentsText(parkingArguments(destination, kind)), err,
	)
}

// declareArgumentsText renders a queue's declare arguments as key=value pairs
// in sorted key order, so the same queue configuration always produces the
// same operator-facing text. String values are quoted, which keeps the empty
// string that x-dead-letter-exchange uses for the default exchange visible.
func declareArgumentsText(args amqp.Table) string {
	keys := slices.Sorted(maps.Keys(args))
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		if text, ok := args[key].(string); ok {
			parts = append(parts, key+"="+strconv.Quote(text))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%v", key, args[key]))
	}
	return strings.Join(parts, ", ")
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
	err := p.closeLocked()
	p.mu.Unlock()
	if errors.Is(err, amqp.ErrClosed) {
		return nil
	}
	if err != nil {
		return classifyAMQP("producer.close", driver.KindTransient, err)
	}
	return nil
}

func (p *producer) invalidateLocked() {
	_ = p.closeLocked()
}

func (p *producer) closeLocked() error {
	if p.closed {
		return nil
	}
	p.closed = true
	err := p.channel.Close()
	p.conn.removeProducer(p)
	return err
}
