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
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/wire"
)

// publishChannelCount is the number of confirm-mode channels one producer
// publishes on.
//
// A channel is shared: every call publishes on one and waits for its own
// messages' confirmations, so the channel count does not bound how many
// messages are in flight. That bound used to be the problem. A call held its
// channel until its confirmations came back, so a producer carried at most one
// call per channel and its rate was the channel count over the confirm round
// trip: about 2.6k messages per second on a disk that syncs in 2.6 ms, and a
// fraction of that whenever the disk stalled. Shared, the rate follows the
// offered load until the broker or its disk is the limit.
//
// The channel count still bounds the rate once the broker is saturated: the
// broker works through each channel's publishes in order, so fewer channels
// leave less of a quorum queue's commit pipeline in use. At 100k offered
// messages per second against one quorum queue, 16 channels published about
// 45k per second where 4 published about 32k, and amqp091-go on its own fell
// from about 64k to about 44k going from 16 channels to 4. Below saturation the
// count changes nothing: 4 and 16 measured the same at 5k and 10k.
//
// Calls start on the slots round robin and a slot opens its channel on first
// use, so a producer holds all of them after its first few calls, quiet or not.
// It is a constant and not a configuration key, because no measurement asks for
// a different number per deployment.
const publishChannelCount = 16

// publishChannel is one confirm-mode channel and the watcher of its returns and
// close.
type publishChannel struct {
	channel *amqp.Channel
	watcher *publishWatcher
}

type producer struct {
	conn *conn
	// next picks the channel a call starts on, round robin.
	next atomic.Uint64
	// opening holds one token per channel slot and serialises opening a
	// channel into that slot, so two calls that find it closed open one
	// channel between them rather than two. It is a channel rather than a
	// mutex so the wait for it can end with the caller's context.
	opening [publishChannelCount]chan struct{}
	// mu guards everything below and is never held across a broker round
	// trip.
	mu     sync.Mutex
	closed bool
	// channels is the channel of each slot, nil until a call first needs it,
	// again once a call retired it for having closed, and again once Close
	// took it.
	channels [publishChannelCount]*publishChannel
	// active counts the publishes in flight, and idle is closed when it
	// returns to zero, which is what Close waits for.
	active int
	idle   chan struct{}
}

var _ driver.Producer = (*producer)(nil)

func newProducer(ctx context.Context, conn *conn, cfg driver.ProducerConfig) (*producer, error) {
	_ = cfg
	p := &producer{conn: conn}
	for slot := range p.opening {
		p.opening[slot] = make(chan struct{}, 1)
	}
	// The first channel is opened here rather than by the first publish, so a
	// connection that cannot give this producer a confirm-mode channel reports
	// that to whoever asked for the producer. Every later channel is opened by
	// the publish that needs one.
	channel, err := p.openChannel(ctx)
	if err != nil {
		return nil, err
	}
	p.channels[0] = channel
	return p, nil
}

// openChannel opens one confirm-mode channel and starts the watcher of its
// returns and close. No confirmation listener is registered: each publish is
// told about its own message through the DeferredConfirmation the client hands
// back, and a listener nobody read would stall the client's frame reader.
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
	// The watcher is always receiving, so the returns buffer only saves the
	// client's frame reader a hand-off; any size is correct. The client sends
	// one close notification at most, so one slot is all it needs.
	returns := raw.NotifyReturn(make(chan amqp.Return, 16))
	closes := raw.NotifyClose(make(chan *amqp.Error, 1))
	return &publishChannel{channel: raw, watcher: startPublishWatcher(p.conn, returns, closes)}, nil
}

// Publish sends messages in argument order and waits for broker confirmations.
// Concurrent calls may overlap while preserving the order within each call.
func (p *producer) Publish(ctx context.Context, msgs ...driver.OutboundMessage) error {
	if err := ctx.Err(); err != nil {
		return classify("publish", driver.KindTransient, err)
	}
	if len(msgs) == 0 {
		return nil
	}
	// The injected fault is consumed at entry, before the publish is counted,
	// so one injection fails exactly one publish whatever else is running.
	if raw := p.conn.publishFault.Swap(0); raw != 0 {
		kind := driver.Kind(raw - 1)
		failed := make(map[int]error, len(msgs))
		for index := range msgs {
			failed[index] = classify("publish", kind, errors.New("injected publish fault"))
		}
		return &driver.PublishError{Failed: failed}
	}
	if !p.enter() {
		return classify("publish", driver.KindTransient, amqp.ErrClosed)
	}
	defer p.exit()

	failed := make(map[int]error)
	for base := 0; base < len(msgs); {
		consumed, err := p.publishSegment(ctx, msgs, base, failed)
		base += consumed
		if err != nil {
			// Every message the segment decided keeps the outcome it decided.
			// What fails here with the segment's own error is the tail it
			// never published, which is the part a lost channel or an ended
			// context leaves undecided.
			for index := base; index < len(msgs); index++ {
				if _, decided := failed[index]; !decided {
					failed[index] = err
				}
			}
			break
		}
	}
	if len(failed) != 0 {
		return &driver.PublishError{Failed: failed}
	}
	return nil
}

// enter counts a publish in, or refuses it on a closed producer.
func (p *producer) enter() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.active++
	if p.active == 1 {
		p.idle = make(chan struct{})
	}
	return true
}

func (p *producer) exit() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.active--
	if p.active == 0 {
		close(p.idle)
		p.idle = nil
	}
}

// outboundMessage is one message of a segment, encoded and routed.
type outboundMessage struct {
	index      int
	exchange   string
	routingKey string
	publishing amqp.Publishing
	// confirm is set once the message is on the wire.
	confirm publishConfirmation
}

// publishConfirmation is the broker's answer for one published message, as the
// client hands it back: *amqp.DeferredConfirmation.
type publishConfirmation interface {
	Done() <-chan struct{}
	Acked() bool
	WaitContext(ctx context.Context) (bool, error)
}

// maxRepublishes bounds how many times one segment publishes again after other
// calls' messages closed the channel under it. Each republish needs a fresh
// close the broker blamed on someone else's message, so the bound only matters
// while such messages keep arriving; past it the segment fails transient, the
// outcome a lost channel has always had.
const maxRepublishes = 8

// maxSegmentMessages bounds how many messages one segment encodes and has in
// flight. A segment encodes every message before it writes the first, about
// 3 KiB each with its header table, so an unbounded one would hold a whole
// batch in memory: a single call of 100,000 messages peaked at 310 MB more
// heap. A segment that stops here costs its call one confirm round trip before
// the next begins, a few milliseconds per 4,096 messages.
const maxSegmentMessages = 4096

// publishSegment publishes the segment of msgs that starts at base, waits for
// every message of it to be decided, and reports how many messages of msgs it
// consumed.
//
// A segment runs up to, and not including, a message whose MessageId repeats
// one already in it, or maxSegmentMessages messages, whichever comes first: a basic.return names a message only by that id, so two
// unresolved messages sharing one could not be told apart. The next segment
// starts once this one is decided, so a call's order holds even when the next
// segment runs on a different channel: a confirmation means the broker has the
// message, and nothing of the call's after it is on the wire yet. The core
// stamps a unique id on every message, which makes a call one segment; an
// id-less batch from a driver-level caller is one message per segment.
//
// Channels are shared, so the broker can close the one a segment runs on for a
// message of another call. When the close names that message, and none of this
// segment's, the segment's undecided messages are published again on another
// channel instead of failing: see decide.
//
// A nil error means every message consumed has an outcome in failed or was
// published. An error means the caller must fail the messages after the
// segment with it: the channel closed, or the context ended, and nothing
// behind the segment was published.
func (p *producer) publishSegment(ctx context.Context, msgs []driver.OutboundMessage, base int, failed map[int]error) (int, error) {
	pending := make([]outboundMessage, 0, len(msgs)-base)
	seen := make(map[string]struct{}, len(msgs)-base)
	consumed := 0
	for offset, message := range msgs[base:] {
		publishing, err := amqpPublishing(message)
		if err != nil {
			// amqpPublishing classifies the refusal, so a message over the
			// short-string limit fails alone as too large while the messages
			// around it still publish.
			failed[base+offset] = err
			consumed = offset + 1
			continue
		}
		if _, repeated := seen[publishing.MessageId]; repeated || len(pending) == maxSegmentMessages {
			break
		}
		seen[publishing.MessageId] = struct{}{}
		exchange, routingKey, expiration := p.target(message)
		publishing.Expiration = expiration
		pending = append(pending, outboundMessage{
			index:      base + offset,
			exchange:   exchange,
			routingKey: routingKey,
			publishing: publishing,
		})
		consumed = offset + 1
	}

	for attempt := 0; len(pending) != 0; attempt++ {
		if attempt > maxRepublishes {
			err := classify("publish", driver.KindTransient, amqp.ErrClosed)
			failAll(pending, err, failed)
			return consumed, err
		}
		channel, err := p.reserve(ctx, messageIDs(pending))
		if err != nil {
			failAll(pending, err, failed)
			return consumed, err
		}
		written, writeErr := p.write(ctx, channel, pending)
		outcome := p.decide(ctx, channel, pending[:written], pending[written:], writeErr, failed)
		if outcome.err != nil {
			return consumed, outcome.err
		}
		pending = outcome.republish
	}
	return consumed, nil
}

func messageIDs(messages []outboundMessage) []string {
	ids := make([]string, len(messages))
	for index, message := range messages {
		ids[index] = message.publishing.MessageId
	}
	return ids
}

func failAll(messages []outboundMessage, err error, failed map[int]error) {
	for _, message := range messages {
		failed[message.index] = err
	}
}

// write publishes the segment in order and returns how many of its messages
// reached the wire, with the error that stopped it when that is fewer than all.
// A message that fails to go out stops the segment rather than being skipped:
// publishing the ones behind it would put them on the broker ahead of it.
func (p *producer) write(ctx context.Context, channel *publishChannel, segment []outboundMessage) (int, error) {
	for written := range segment {
		// A broker that is blocking publishers has stopped reading this
		// connection, and PublishWithDeferredConfirmWithContext checks ctx only
		// on entry: the socket write it makes next is the one a caller's
		// deadline cannot reach, so it is gated per message and not once per
		// Publish. A batch that starts before the alarm must not keep writing
		// into it, and a wait whose end is the caller's context keeps the
		// semantics a caller already has instead of failing every publish the
		// moment an alarm is raised, which would turn a minute of alarm into a
		// retry storm.
		if err := p.conn.awaitUnblocked(ctx); err != nil {
			return written, classify("publish", driver.KindTransient, err)
		}
		item := segment[written]
		confirm, err := channel.channel.PublishWithDeferredConfirmWithContext(ctx, item.exchange, item.routingKey, true, false, item.publishing)
		if err != nil {
			return written, classifyAMQP("publish", driver.KindTransient, err)
		}
		segment[written].confirm = confirm
	}
	return len(segment), nil
}

// segmentOutcome is what decide leaves the segment to do.
type segmentOutcome struct {
	// republish holds the messages to publish again on another channel,
	// in order: the broker closed the channel for a message of another call.
	republish []outboundMessage
	// err is the error the rest of the call fails with, nil when the call
	// goes on.
	err error
}

// decide waits for the confirmation of every written message, settles the
// segment's ids with the channel's watcher, and records each message's outcome
// in failed: published, returned, negatively confirmed, or failed with the
// context's error or the channel's close. unwritten holds the messages behind
// the one write stopped at, and writeErr the reason.
//
// A confirmation that resolved without an ack on a channel that closed is the
// client giving up on it, not the broker refusing it: with automatic recovery
// off, the client resolves every confirmation it still holds as not acked when
// the channel shuts down, after every ack it had already read. Those messages
// are undecided. A genuine nack is transient too, so a nack that raced the
// close changes no outcome.
//
// What happens to undecided messages depends on whom the close blames. When it
// blames none of this segment's messages but does name its cause - another
// call's message over the size limit, or to an exchange that does not exist -
// the undecided messages and the unwritten ones behind them are handed back to
// be published again, so one caller's bad message does not fail every call
// sharing its channel. A republished message may have reached the queue before
// the close, which makes it a duplicate: at-least-once allows that, and it is
// the same copy a caller retrying the failure would have made. They are handed
// back only when they are the tail of what was written, every acked message in
// front of them; publishing an undecided message again behind one the broker
// already confirmed would reorder the call. When the close blames one of this
// segment's messages, that message fails with the close, which is where a
// refusal for size becomes too large, and the rest fail transient. When the
// close blames nothing it can name, every undecided message fails with it.
func (p *producer) decide(ctx context.Context, channel *publishChannel, written, unwritten []outboundMessage, writeErr error, failed map[int]error) segmentOutcome {
	var waitErr error
	resolved := make([]bool, len(written))
	acked := make([]bool, len(written))
	var settled, unresolvedIDs []string
	var unresolved []publishConfirmation
	for index, item := range written {
		if waitErr == nil {
			ok, err := item.confirm.WaitContext(ctx)
			if err == nil {
				resolved[index], acked[index] = true, ok
				settled = append(settled, item.publishing.MessageId)
				continue
			}
			waitErr = classify("publish", driver.KindTransient, err)
		}
		select {
		case <-item.confirm.Done():
			resolved[index], acked[index] = true, item.confirm.Acked()
			settled = append(settled, item.publishing.MessageId)
		default:
			unresolvedIDs = append(unresolvedIDs, item.publishing.MessageId)
			unresolved = append(unresolved, item.confirm)
		}
	}
	// A segment whose writing was stopped - by a publishing block the gate
	// waited on until the context ended, or by the channel - reports its
	// in-flight messages with that reason rather than the bare deadline: they
	// are stuck for the same reason, and the block's error is the one that
	// names it for the caller.
	if waitErr != nil && writeErr != nil {
		waitErr = writeErr
	}
	settled = append(settled, messageIDs(unwritten)...)
	// A write refused because the channel closed can come before the watcher
	// has heard of the close. The client marks a channel closed first and only
	// then takes the channel's lock to send the close notification, so a
	// publish that already held the lock fails with ErrClosed, releases it,
	// and only then is the notification sent. Settling in that gap would read
	// an open channel and fail the unwritten messages instead of deciding them
	// by the close. The notification always follows once the lock is free, so
	// the wait is short; the context bounds it anyway.
	if errors.Is(writeErr, amqp.ErrClosed) {
		select {
		case <-channel.watcher.shut:
		case <-ctx.Done():
		}
	}
	reply := channel.watcher.settle(settled)
	if len(unresolved) != 0 {
		channel.releaseWhenResolved(p.conn, unresolvedIDs, unresolved)
	}

	var undecided []outboundMessage
	republishable := true
	for index, item := range written {
		switch {
		case !resolved[index]:
			failed[item.index] = waitErr
		case acked[index]:
			if len(undecided) != 0 {
				republishable = false
			}
			if returnedErr, ok := reply.returned[item.publishing.MessageId]; ok {
				failed[item.index] = parkingFailure(item.routingKey, p.conn.queueKind, returnedErr)
			}
		case reply.closed:
			undecided = append(undecided, item)
		default:
			failed[item.index] = classify("publish", driver.KindTransient, errors.New("rabbitmq: publisher confirm was negative"))
		}
	}
	if waitErr != nil {
		failAll(undecided, waitErr, failed)
		failAll(unwritten, waitErr, failed)
		return segmentOutcome{err: waitErr}
	}
	if !reply.closed {
		failAll(unwritten, writeErr, failed)
		return segmentOutcome{err: writeErr}
	}

	undecided = append(undecided, unwritten...)
	if len(undecided) == 0 {
		// The channel closed after every message of the segment was decided,
		// so the close costs this call nothing: its next segment reserves a
		// channel that is open.
		return segmentOutcome{}
	}
	blamed, named := closeBlame(reply.closeErr)
	if !named {
		failAll(undecided, reply.closeErr, failed)
		return segmentOutcome{err: reply.closeErr}
	}
	culprit := slices.ContainsFunc(undecided, blamed)
	if !culprit && republishable {
		return segmentOutcome{republish: undecided}
	}
	lost := classify("publish", driver.KindTransient, amqp.ErrClosed)
	for _, item := range undecided {
		if blamed(item) {
			// The close is the failure of the message it blames: its text
			// is the broker's own reason for the refusal.
			failed[item.index] = reply.closeErr
			continue
		}
		failed[item.index] = lost
	}
	return segmentOutcome{err: lost}
}

// releaseWhenResolved gives ids back to the channel's watcher once every one of
// confirms has resolved, for a call whose context ended while they were still
// in flight.
//
// Releasing them at once would let a return meant for the abandoned call fail
// another. Walk it: call A publishes message X and its context ends before X's
// confirmation; A releases X's id and returns. Call B publishes a message with
// the same id - a retry copy keeps its original's - and the watcher lets it
// register, the id being free. The broker then returns A's X, and the watcher
// files that return under the id B now holds, so B reports a message the broker
// routed as unroutable. Holding A's ids until A's confirmations resolve closes
// that gap: the broker sends a message's return before its ack, so once X's
// confirmation has resolved its return, if any, is already with the watcher,
// and the release that follows drops it with the id. B is refused the id until
// then and waits for it or uses another channel.
//
// A confirmation resolves when the broker answers or the channel closes, so the
// goroutine ends with the channel at the latest; the connection waits for it on
// Close with the other goroutines it owns.
func (c *publishChannel) releaseWhenResolved(conn *conn, ids []string, confirms []publishConfirmation) {
	conn.detachWatch.Go(func() {
		for _, confirm := range confirms {
			<-confirm.Done()
		}
		c.watcher.release(ids)
	})
}

// reserve picks the channel a segment runs on and registers the segment's ids
// with that channel's watcher.
//
// A channel on which another call holds one of the ids is passed over for the
// next one. When every channel holds one, the segment waits for the first
// holder it met to release its ids, then tries again. A channel that turns out
// to have closed is retired so that the next look at its slot opens a fresh
// one, and a slot whose channel cannot be opened is passed over while another
// slot has a channel; the open's error is reported only when no slot has one. A
// second round in which every channel had closed means the connection is not
// keeping channels open, and the segment fails transient.
func (p *producer) reserve(ctx context.Context, ids []string) (*publishChannel, error) {
	start := p.next.Add(1)
	deadRounds := 0
	for {
		var busy <-chan struct{}
		var openErr error
		for offset := range uint64(publishChannelCount) {
			slot := int((start + offset) % publishChannelCount)
			channel, err := p.channelIn(ctx, slot)
			if err != nil {
				openErr = err
				continue
			}
			held, live := channel.watcher.register(ids)
			if !live {
				p.retire(slot, channel)
				continue
			}
			if held == nil {
				return channel, nil
			}
			if busy == nil {
				busy = held
			}
		}
		if busy == nil {
			if openErr != nil {
				return nil, openErr
			}
			deadRounds++
			if deadRounds > 1 {
				return nil, classify("publish", driver.KindTransient, amqp.ErrClosed)
			}
			continue
		}
		select {
		case <-busy:
		case <-ctx.Done():
			return nil, classify("publish", driver.KindTransient, ctx.Err())
		}
	}
}

// channelIn returns the channel of slot, opening one when the slot has none.
//
// The common case reads the slot under mu and returns. Opening is a broker
// round trip, so it runs outside mu and under the slot's opening token, and the
// slot is read again once the token is held: a call that waited for the token
// finds the channel the holder opened rather than opening another.
//
// The slot is filled under mu only if the producer is still open, and that is
// what keeps a channel from outliving Close. Walk the race: a call finds the
// slot empty and starts opening a channel; Close sets closed under mu, waits
// for publishes up to its context, and takes every channel in the slots, which
// does not yet include the one being opened; the open finishes. If the call
// filled the slot now, nothing would ever close that channel. It checks closed
// under the same mu instead, finds it set, closes the channel itself, and is
// refused as it would have been before the open. Close setting closed before it
// takes the channels is what makes the check sufficient: a slot filled before
// closed was set is one Close's take sees.
func (p *producer) channelIn(ctx context.Context, slot int) (*publishChannel, error) {
	if channel, closed := p.slotChannel(slot); closed {
		return nil, classify("publish", driver.KindTransient, amqp.ErrClosed)
	} else if channel != nil {
		return channel, nil
	}
	select {
	case p.opening[slot] <- struct{}{}:
	case <-ctx.Done():
		return nil, classify("publish", driver.KindTransient, ctx.Err())
	}
	defer func() { <-p.opening[slot] }()
	if channel, closed := p.slotChannel(slot); closed {
		return nil, classify("publish", driver.KindTransient, amqp.ErrClosed)
	} else if channel != nil {
		return channel, nil
	}
	fresh, err := p.openChannel(ctx)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = p.conn.awaitChannelClose(ctx, p.conn.startChannelClose(fresh.channel))
		return nil, classify("publish", driver.KindTransient, amqp.ErrClosed)
	}
	p.channels[slot] = fresh
	p.mu.Unlock()
	return fresh, nil
}

// slotChannel returns slot's channel, nil when the slot needs one opened, and
// whether the producer has closed. A channel that has closed stays in its slot
// until a call finds its watcher refusing registrations and retires it.
func (p *producer) slotChannel(slot int) (channel *publishChannel, closed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, true
	}
	return p.channels[slot], false
}

// retire empties slot if it still holds channel, which has closed. Nothing is
// closed here: the broker or the connection already closed the channel, and
// its watcher ends once the calls registered on it have settled.
func (p *producer) retire(slot int, channel *publishChannel) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.channels[slot] == channel {
		p.channels[slot] = nil
	}
}

// closeBlame reports which messages a channel's close is the broker refusing,
// for the closes that say.
//
// A refusal for size names the limit, and every message over it is refused
// wherever the broker reaches it, whether or not it is the one that closed this
// channel. Being over the limit is a property of the message, which is what
// lets the answer hold while a confirmation is still missing: a quorum queue
// commits a persistent message asynchronously, so the close can beat the
// confirmation of a message in front of the refused one, and calling that
// message the refused one would report a failure the broker never produced. A
// message exactly at the limit is publishable.
//
// A publish to an exchange that does not exist closes the channel with a 404
// that names the exchange, measured on RabbitMQ 4.3 as
//
//	NOT_FOUND - no exchange 'orders' in vhost '/'
//
// so every message to that exchange is refused. The default exchange always
// exists, and a message to a queue that does not is returned rather than
// refused, so no message routed by queue name is ever blamed.
//
// named is false for every other close: it names no message, so nothing can be
// told apart by it.
func closeBlame(closeErr error) (blamed func(outboundMessage) bool, named bool) {
	if limit, refused := sizeRefusalLimit(closeErr); refused {
		return func(message outboundMessage) bool { return len(message.publishing.Body) > limit }, true
	}
	var amqpErr *amqp.Error
	if errors.As(closeErr, &amqpErr) && amqpErr.Code == amqp.NotFound && strings.HasPrefix(amqpErr.Reason, missingExchangePrefix) {
		return func(message outboundMessage) bool {
			return message.exchange != "" && strings.HasPrefix(amqpErr.Reason, missingExchangePrefix+message.exchange+"' in vhost '")
		}, true
	}
	return nil, false
}

// missingExchangePrefix starts the reason of the close a publish to a missing
// exchange causes; the exchange's name follows it, in quotes.
const missingExchangePrefix = "NOT_FOUND - no exchange '"

// target decides AMQP routing for one outbound message. Whether Destination
// names a fan-out entry point is never looked up here: the core already
// resolved that, in one read of one effective capability set, and carries
// the answer on the message itself. A driver that looked the answer up
// again in its own state could disagree with what the core declared, which
// is the defect this replaces.
//
// The delayed-destination read is the one cache read this function keeps: a
// message published to a destination declared with a delay parks in that
// destination's parking queue and carries the delay as its expiration. It is
// read-only against state EnsureTopology already populated, so the publish path
// still declares nothing.
func (p *producer) target(message driver.OutboundMessage) (exchange, routingKey, expiration string) {
	p.conn.mu.RLock()
	delay, isDeferred := p.conn.deferred[message.Destination]
	p.conn.mu.RUnlock()
	if isDeferred {
		return "", parkQueueName(message.Destination), strconv.FormatInt(parkDelayMillis(delay), 10)
	}
	if message.EntryPoint {
		return message.Destination, "", ""
	}
	return "", message.Destination, ""
}

// maxParkDelayMillis is the longest expiration, in milliseconds, RabbitMQ
// accepts on a message.
const maxParkDelayMillis int64 = 2147483647

func returnedPublishError(returned amqp.Return) error {
	reason := fmt.Errorf("rabbitmq: publish returned by broker: %d %s", returned.ReplyCode, returned.ReplyText)
	if returned.ReplyCode == 312 {
		return classify("publish", driver.KindNotFound, errors.Join(driver.ErrDestinationMissing, reason))
	}
	return classify("publish", driver.KindFatal, reason)
}

// parkingSuffix starts the part of a parking queue name that follows its
// destination. Both the topology path that declares a parking queue and the
// publish path that routes to it derive the name from parkQueueName, so the two
// cannot drift apart.
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
	destination, isParking := parkQueueParts(routingKey)
	if !isParking || !errors.Is(err, driver.ErrDestinationMissing) {
		return err
	}
	return fmt.Errorf(
		"rabbitmq: parking destination %q is missing: a delayed or retried message is parked there"+
			" until its delay expires, so the adapter declares the queue durable under TopologyDeclare"+
			" and requires it under TopologyVerify, and under TopologyNone the operator provisions it"+
			" with the declare arguments %s: %w",
		routingKey, declareArgumentsText(parkArguments(destination, kind)), err,
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

// maxShortStringBytes is the longest value an AMQP short string carries: the
// length prefix of its wire form is one byte.
const maxShortStringBytes = 255

// amqpPublishing encodes one outbound message as an AMQP publishing, or returns
// a classified error for a message AMQP cannot encode. The error is classified
// here rather than at the call site because it decides the caller-visible kind:
// a message over the short-string limit is one the broker can never accept.
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
		case wire.ID:
			publishing.MessageId = value
			headers["cloudEvents:id"] = value
		case wire.Time:
			parsed, err := time.Parse(time.RFC3339Nano, value)
			if err != nil {
				return amqp.Publishing{}, classify("publish", driver.KindFatal,
					fmt.Errorf("invalid time header: %w", err))
			}
			publishing.Timestamp = parsed
			headers["cloudEvents:time"] = value
		case wire.Type:
			publishing.Type = value
			headers["cloudEvents:type"] = value
		case wire.DataContentType:
			publishing.ContentType = value
		case wire.CorrelationID:
			publishing.CorrelationId = value
		default:
			key := "cloudEvents:" + header.Key
			if len(key) > maxShortStringBytes {
				return amqp.Publishing{}, classify("publish", driver.KindTooLarge,
					fmt.Errorf("header key is %d bytes, over the AMQP short-string limit of %d",
						len(key), maxShortStringBytes))
			}
			headers[key] = value
		}
	}
	// These four are the AMQP short-string properties this encoder fills. The
	// client refuses a value over 255 bytes while serializing the header,
	// after the publish method frame is already on the wire, and shuts the
	// whole connection down for that write error (amqp091-go writeShortstr and
	// sendUnflushed): one oversized property would take every call sharing the
	// connection with it, and the plain error reads transient, so a retrying
	// caller would loop. Refusing the message here, before any channel is
	// touched, keeps the failure on the message that caused it. KindTooLarge
	// says the message can never be accepted, which is what lets the core drop
	// an unpublishable successor copy once instead of stopping the
	// subscription. The error names the property and its length and never
	// quotes the value, so a credential or payload in a property cannot reach a
	// log or an error report through it.
	for _, property := range [...]struct{ name, value string }{
		{"CorrelationId", publishing.CorrelationId},
		{"MessageId", publishing.MessageId},
		{"Type", publishing.Type},
		{"ContentType", publishing.ContentType},
	} {
		if len(property.value) > maxShortStringBytes {
			return amqp.Publishing{}, classify("publish", driver.KindTooLarge,
				fmt.Errorf("property %s is %d bytes, over the AMQP short-string limit of %d",
					property.name, len(property.value), maxShortStringBytes))
		}
	}
	return publishing, nil
}

// Close stops new publishes, waits for active work, and closes the producer's
// channels. The context bounds these waits.
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
	idle := p.idle
	p.mu.Unlock()

	// A publish in flight is waited for, not cut off, so its confirmations
	// arrive on a channel that is still open. A context that ends first
	// leaves the publishes still running to the closes that follow: a close
	// resolves every confirmation they are waiting on.
	if idle != nil {
		select {
		case <-idle:
		case <-ctx.Done():
		}
	}
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

// startChannelCloses takes every channel the producer still holds out of its
// slots and starts the close of each, returning the waits. A channel is taken
// out exactly once, so its close starts exactly once, and a slot emptied here
// is never filled again: a publish that opens a channel after Close finds the
// producer closed and closes that channel itself.
func (p *producer) startChannelCloses() []<-chan error {
	p.mu.Lock()
	var channels []*publishChannel
	for slot, channel := range p.channels {
		if channel != nil {
			channels = append(channels, channel)
			p.channels[slot] = nil
		}
	}
	p.mu.Unlock()
	closes := make([]<-chan error, 0, len(channels))
	for _, channel := range channels {
		closes = append(closes, p.conn.startChannelClose(channel.channel))
	}
	return closes
}
