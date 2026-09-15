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

// publishChannelPoolSize is the number of confirm-mode channels one producer
// publishes on.
//
// One channel carried every publish on a client, and each of those publishes
// held the channel until the last confirmation of its call came back, so every
// application publish, retry and dead-letter copy queued behind one confirm
// round trip: roughly 300 messages per second whether one, four or sixteen
// publishers ran at once. A pool is what lets them overlap.
//
// 16 is the handler concurrency a default subscription runs with, which bounds
// how many successor publishes one subscription can issue at once. A publish
// takes one channel for its whole call rather than one per window, because a
// batch's order is only promised within a channel, and a batch that changed
// channel mid-call would not keep it. The size is a constant and not a
// configuration key: one channel measured about the same rate at every
// publisher count, so no measurement asks for a different number yet.
const publishChannelPoolSize = 16

// publishChannel is one confirm-mode channel and the delivery state that
// belongs to it. A channel is published on by one call at a time, which is what
// lets its confirmations be read in the order they arrive and its returns be
// matched by MessageId without the two windows reading each other's frames.
type publishChannel struct {
	channel  *amqp.Channel
	confirms chan amqp.Confirmation
	returns  chan amqp.Return
}

type producer struct {
	conn *conn
	// slots holds one token per publish this producer runs at once. A publish
	// takes a token before it takes a channel and gives it back once it has
	// given the channel back, so holding every token is holding the pool
	// quiet. It is a channel rather than a counter so the wait on it can end
	// with the caller's context.
	slots chan struct{}
	// mu guards closed, idle and open, and is never held across a broker round
	// trip: opening a channel, publishing, reading a confirmation and closing
	// a channel all happen outside it.
	mu     sync.Mutex
	closed bool
	// idle holds the channels a publish may take, and open holds every channel
	// whose close has not started. A channel taken from idle stays in open
	// until it is given back or discarded, so a Close reaches the channels a
	// publish is holding as well as the ones sitting free. A channel leaves
	// open exactly once, and whoever takes it out starts its close.
	idle []*publishChannel
	open map[*publishChannel]struct{}
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
//
// The sizing is also what a channel whose close outlived its caller depends on.
// Once the broker unblocks it still delivers acks and returns for a window
// nobody is reading any more, because the channel was discarded and the close
// that ends it is still in flight; a receiver smaller than the window would
// stall the frame reader on such a send instead of taking it.
const publishWindowSize = 64

func newProducer(ctx context.Context, conn *conn, cfg driver.ProducerConfig) (*producer, error) {
	_ = cfg
	p := &producer{
		conn:  conn,
		slots: make(chan struct{}, publishChannelPoolSize),
		open:  make(map[*publishChannel]struct{}),
	}
	for range publishChannelPoolSize {
		p.slots <- struct{}{}
	}
	// The first channel is opened here rather than by the first publish, so a
	// connection that cannot give this producer a confirm-mode channel reports
	// that to whoever asked for the producer. Every later channel is opened by
	// the publish that needs one.
	channel, err := p.openChannel(ctx)
	if err != nil {
		return nil, err
	}
	p.open[channel] = struct{}{}
	p.idle = append(p.idle, channel)
	return p, nil
}

// openChannel opens one confirm-mode channel and registers the two streams the
// publish window reads from it.
//
// A block is waited out before anything is opened: the two round trips below
// are synchronous, and a broker that is blocking publishers has stopped
// reading this connection, so neither can complete until the block clears.
// The wait is outside the connection lock because it can last as long as the
// caller's context, and holding the read lock across it would park every
// other call on the connection behind an alarm - a Close that only wanted to
// report outstanding resources included. Nothing is decided here: a
// connection that closed during the wait is caught by the check below, and
// one that closed after the channel was opened is caught by the caller's
// re-check, which closes the channel it never added to the producer.
//
// What this cannot cover is a connection the broker has just begun blocking
// for its own publishing and has not told us about yet: there is no state to
// wait on during that gap, and the channel open that follows is a round trip
// amqp091 gives no context to end.
func (p *producer) openChannel(ctx context.Context) (*publishChannel, error) {
	if err := p.conn.awaitUnblocked(ctx); err != nil {
		return nil, classify("producer", driver.KindTransient, err)
	}
	p.conn.mu.RLock()
	if p.conn.closed || p.conn.amqp.IsClosed() {
		p.conn.mu.RUnlock()
		return nil, classify("producer", driver.KindTransient, amqp.ErrClosed)
	}
	raw, err := p.conn.amqp.Channel()
	p.conn.mu.RUnlock()
	if err != nil {
		return nil, classifyAMQP("producer", driver.KindTransient, err)
	}
	if err := raw.Confirm(false); err != nil {
		_ = p.conn.awaitChannelClose(ctx, p.conn.startChannelClose(raw))
		return nil, classifyAMQP("producer", driver.KindFatal, err)
	}
	channel := &publishChannel{
		channel:  raw,
		confirms: make(chan amqp.Confirmation, publishWindowSize),
		returns:  make(chan amqp.Return, publishWindowSize),
	}
	raw.NotifyPublish(channel.confirms)
	raw.NotifyReturn(channel.returns)
	return channel, nil
}

// Publish takes one channel for the whole call and runs every window of the
// call on it, so a batch keeps the order its messages were given in and no
// caller's messages are split across channels. Concurrent calls take separate
// channels and overlap: a publish waits for a free channel, not for the
// producer.
func (p *producer) Publish(ctx context.Context, msgs ...driver.OutboundMessage) error {
	if err := ctx.Err(); err != nil {
		return classify("publish", driver.KindTransient, err)
	}
	if len(msgs) == 0 {
		return nil
	}
	// The injected fault is consumed at entry, before a slot is taken, so one
	// injection fails exactly one publish whatever else is running.
	if raw := p.conn.publishFault.Swap(0); raw != 0 {
		kind := driver.Kind(raw - 1)
		failed := make(map[int]error, len(msgs))
		for index := range msgs {
			failed[index] = classify("publish", kind, errors.New("injected publish fault"))
		}
		return &driver.PublishError{Failed: failed}
	}

	channel, err := p.acquire(ctx)
	if err != nil {
		return err
	}
	failed := make(map[int]error)
	discarded := false
	for base := 0; base < len(msgs); {
		consumed, err := p.publishWindow(channel, ctx, msgs, base, failed)
		if err != nil {
			// The channel is gone. Every index the window did not decide
			// failed for that same reason, and no message after it can be
			// published on the channel either.
			for index := base; index < len(msgs); index++ {
				if _, decided := failed[index]; !decided {
					failed[index] = err
				}
			}
			discarded = true
			break
		}
		base += consumed
	}
	if discarded {
		p.discard(ctx, channel)
	} else {
		p.release(channel)
	}
	if len(failed) != 0 {
		return &driver.PublishError{Failed: failed}
	}
	return nil
}

// acquire takes a slot and the channel the caller's publish runs on.
//
// The first check refuses a publish on a closed producer without taking a
// slot, so a publish issued while Close is shutting the pool down is refused
// at once instead of waiting for tokens Close is holding. The second check is
// the one that decides: it runs once the slot is held, because a producer that
// closed while this call waited for a slot must not hand out a channel.
func (p *producer) acquire(ctx context.Context) (*publishChannel, error) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return nil, classify("publish", driver.KindTransient, amqp.ErrClosed)
	}
	select {
	case <-p.slots:
	case <-ctx.Done():
		return nil, classify("publish", driver.KindTransient, ctx.Err())
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.slots <- struct{}{}
		return nil, classify("publish", driver.KindTransient, amqp.ErrClosed)
	}
	if count := len(p.idle); count > 0 {
		channel := p.idle[count-1]
		p.idle = p.idle[:count-1]
		p.mu.Unlock()
		return channel, nil
	}
	p.mu.Unlock()

	channel, err := p.openChannel(ctx)
	if err != nil {
		p.slots <- struct{}{}
		return nil, err
	}
	p.mu.Lock()
	if p.closed {
		// A Close that ran while this channel was opening took every channel
		// the producer knew about. This one was never in that set, so it is
		// closed here, and the caller is refused the way it would have been
		// before the open.
		p.mu.Unlock()
		_ = p.conn.awaitChannelClose(ctx, p.conn.startChannelClose(channel.channel))
		p.slots <- struct{}{}
		return nil, classify("publish", driver.KindTransient, amqp.ErrClosed)
	}
	p.open[channel] = struct{}{}
	p.mu.Unlock()
	return channel, nil
}

// release gives a working channel back to the pool and frees its slot. A
// channel a Close took out of the producer first is dropped instead of
// returned: its close is already running, and it must never be handed to a
// later publish.
func (p *producer) release(channel *publishChannel) {
	p.mu.Lock()
	if _, live := p.open[channel]; live {
		p.idle = append(p.idle, channel)
	}
	p.mu.Unlock()
	p.slots <- struct{}{}
}

// discard takes a channel whose window can no longer be trusted out of the
// producer and closes it, so the confirmations it is still owed can never be
// read against a later publish. It frees the slot as well: the next publish
// opens a fresh channel in its place.
//
// The close is started before the slot is freed, so a Close waiting on this
// slot has this channel's close registered with the connection before the
// producer is dropped from it.
//
// A channel Close took out of the producer first is dropped without a second
// close: the close already running is this channel's. ctx is the context of
// the publish that reached here, so a caller that has already run out of time
// - one of the ways a window ends up here - does not wait for the close to
// finish.
func (p *producer) discard(ctx context.Context, channel *publishChannel) {
	p.mu.Lock()
	_, live := p.open[channel]
	delete(p.open, channel)
	p.mu.Unlock()
	var done <-chan error
	if live {
		done = p.conn.startChannelClose(channel.channel)
	}
	p.slots <- struct{}{}
	if done != nil {
		_ = p.conn.awaitChannelClose(ctx, done)
	}
}

// publishWindow publishes the window of messages that starts at base and then
// reads one confirmation per published message, in order. It reports how many
// messages it consumed.
//
// A nil error means every index in the window has a decided outcome: its own
// entry in failed, or a durable confirmation. An error means the channel must
// not be published on again: the caller discards it, which is what keeps the
// confirmations it is still owed from being read against a later publish.
//
// Confirmations are read in the order the client delivers them, which is
// delivery-tag order whatever order the broker acknowledged in, so the i-th
// confirmation read belongs to the i-th message published here and no
// tag-to-index map is needed.
func (p *producer) publishWindow(channel *publishChannel, ctx context.Context, msgs []driver.OutboundMessage, base int, failed map[int]error) (int, error) {
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
	for _, item := range window {
		if err := ctx.Err(); err != nil {
			// Messages already published here are unconfirmed, and this error
			// is what tells the caller to discard the channel rather than
			// read those confirmations against a later publish.
			return 0, classify("publish", driver.KindTransient, err)
		}
		// A broker that is blocking publishers has stopped reading this
		// connection, and PublishWithContext checks ctx only on entry: the
		// socket write it makes next is the one a caller's deadline cannot
		// reach, so it is gated per message and not once per Publish. A batch
		// that starts before the alarm must not keep writing into it, and a
		// wait whose end is the caller's context keeps the semantics a caller
		// already has instead of failing every publish the moment an alarm is
		// raised, which would turn a minute of alarm into a retry storm.
		if err := p.conn.awaitUnblocked(ctx); err != nil {
			return 0, classify("publish", driver.KindTransient, err)
		}
		if err := channel.channel.PublishWithContext(ctx, item.exchange, item.routingKey, true, false, item.publishing); err != nil {
			// A publish that failed on the wire leaves the channel's delivery
			// tags out of step with the messages that were not published.
			return 0, classifyAMQP("publish", driver.KindTransient, err)
		}
	}
	returned := make(map[string]error, len(window))
	for _, item := range window {
		confirmation, err := channel.waitConfirm(ctx, returned)
		if err != nil {
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
			// On the ladder, the rung queue's own x-message-ttl does the
			// delaying and the message carries no expiration: that is what
			// makes expiry order FIFO order and keeps a message from waiting
			// for one parked ahead of it. Above the ladder the per-message
			// path stays exactly as it was, per-message expiration included,
			// because a due time up to maxExpirationMillis is still owed and
			// clamping it into the top rung would release it early.
			if rung := parkRung(remaining); rung > 0 {
				return "", parkQueueName(destination, rung), ""
			}
			return "", destination + parkingSuffix, expirationMillis(remaining)
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
// streams closed, so this channel must not be used for a later publish.
func (c *publishChannel) waitConfirm(ctx context.Context, returned map[string]error) (amqp.Confirmation, error) {
	for {
		select {
		case item, ok := <-c.returns:
			if !ok {
				return amqp.Confirmation{}, classify("publish", driver.KindTransient, amqp.ErrClosed)
			}
			returned[item.MessageId] = returnedPublishError(item)
		case confirmation, ok := <-c.confirms:
			if !ok {
				return amqp.Confirmation{}, classify("publish", driver.KindTransient, amqp.ErrClosed)
			}
			if !c.drainReturns(returned) {
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
func (c *publishChannel) drainReturns(returned map[string]error) bool {
	for {
		select {
		case item, ok := <-c.returns:
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

// parkingSuffix turns a destination name into the name of the parking queue a
// delay above the ladder waits in. A rung queue carries the same suffix
// followed by the rung's tag, and both the topology path that declares a queue
// and the publish path that routes to it derive the name from the same
// helpers, so the two shapes cannot drift apart.
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
// states that split. Its declare arguments are rendered from the argument
// builder for that queue rather than copied into the text: the two had to be
// edited together before, and an argument added there without a matching edit
// here would have told an operator to create a queue the adapter would then
// report as drift. Durability is a declare flag rather than an entry in that
// table, so the message states it in words instead: the adapter declares the
// parking queue durable under every queue kind, and an operator provisioning
// one under TopologyNone has only this text to read. kind is the connection's
// own queue kind, because the arguments differ by kind and the producer holds
// no other way back to it. Classification is deliberately the wrapped error's
// own: this adds context and changes no kind.
func parkingFailure(routingKey string, kind queueKind, err error) error {
	destination, _, isParking := parkQueueParts(routingKey)
	if !isParking || !errors.Is(err, driver.ErrDestinationMissing) {
		return err
	}
	return fmt.Errorf(
		"rabbitmq: parking destination %q is missing: a delayed or retried message is parked there"+
			" until its delay expires, so the adapter declares the queue durable under TopologyDeclare"+
			" and requires it under TopologyVerify, and under TopologyNone the operator provisions it"+
			" with the declare arguments %s: %w",
		routingKey, declareArgumentsText(parkArguments(destination, kind, routingKey)), err,
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

// Close stops the producer: new publishes are refused, every channel it holds
// is closed, and the connection stops listing it.
//
// Its order matters. The producer is marked closed first, so a publish that
// arrives while this runs is refused rather than handed a channel. Then the
// publishes in flight are waited for, so the channels closed next are all the
// producer has. Then every channel's close is started, and only then is the
// producer dropped from the connection: the wait a channel close registers is
// ordered before the connection stops listing this producer, so a conn.Close
// that saw no producers left has already registered every one of these. The
// closes are awaited last, for as long as ctx allows, so a caller that has run
// out of time is not held to the broker's pace.
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
	p.mu.Unlock()

	p.awaitPublishes(ctx)
	closes := p.startChannelCloses()
	p.conn.removeProducer(p)

	var failures []error
	for _, done := range closes {
		if err := p.conn.awaitChannelClose(ctx, done); err != nil && !errors.Is(err, amqp.ErrClosed) {
			failures = append(failures, err)
		}
	}
	if err := errors.Join(failures...); err != nil {
		return classifyAMQP("producer.close", driver.KindTransient, err)
	}
	return nil
}

// awaitPublishes waits until every publish in flight has given its channel back,
// or ctx ends, whichever comes first, and leaves the slots where it found them.
//
// A publish holds its slot until it has returned its channel, so holding every
// slot at once is holding the pool quiet, and that is the state Close needs to
// see before it closes what is left. The slots go back afterwards so a publish
// that is waiting for one is not left waiting on a producer that is closed:
// it is refused when it takes its slot, and it is the published set that has
// already been waited for. A context that ends first leaves the publishes that
// are still running to be dealt with by the closes that follow, which reach the
// channels they are holding through the producer's own set.
func (p *producer) awaitPublishes(ctx context.Context) {
	held := 0
	defer func() {
		for range held {
			p.slots <- struct{}{}
		}
	}()
	for held < publishChannelPoolSize {
		select {
		case <-p.slots:
			held++
		case <-ctx.Done():
			return
		}
	}
}

// startChannelCloses takes every channel the producer still holds out of its
// set and starts the close of each, returning the waits. A channel is taken out
// exactly once, so its close starts exactly once: a publish that reaches its
// discard path after this finds the channel gone and does not close it again,
// and the connection is not asked to close a channel twice.
func (p *producer) startChannelCloses() []<-chan error {
	p.mu.Lock()
	channels := slices.Collect(maps.Keys(p.open))
	clear(p.open)
	p.idle = nil
	p.mu.Unlock()
	closes := make([]<-chan error, 0, len(channels))
	for _, channel := range channels {
		closes = append(closes, p.conn.startChannelClose(channel.channel))
	}
	return closes
}
