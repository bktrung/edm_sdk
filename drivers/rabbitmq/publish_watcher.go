package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// publishWatcher owns the two events of a publish channel the client does not
// route to a message: a basic.return and the channel's close.
//
// Confirmations need no owner. The client numbers each publish under the
// channel's own lock and resolves the DeferredConfirmation it handed out for
// that number, so any number of calls can publish on one channel and each is
// told about its own messages. A basic.return carries no delivery tag, though:
// the MessageId the broker echoes back is the only thing that says which
// message it belongs to, so calls sharing a channel register the ids they are
// about to publish, and the watcher files each return under its id until the
// call that registered it asks.
//
// One goroutine receives the returns and answers the calls, and that is what
// makes an answer complete. The client dispatches a message's return before
// the ack for the same message, from the one goroutine that reads the
// connection, so by the time a call has seen its confirmation the return has
// been handed over: it sits in the returns buffer, or the watcher has taken it
// and recorded it, the two steps being one select case of the same goroutine.
// The watcher empties the buffer before it answers any request. Walk the
// interleaving that would otherwise go wrong: the reader sends return R for
// message M into the buffer, then acks M; the call waiting on M wakes and sends
// its settle request; the watcher's select finds both the buffered R and the
// request ready and may pick the request first. Draining first files R under M
// before the reply is built, so the call sees its return instead of reporting
// an unroutable message as published.
//
// Two unresolved registrations of one id on one channel would make a return
// ambiguous, so a registration that repeats a live id is refused and the call
// tries another channel or waits for that id to be released. The core stamps a
// unique id per envelope, but a retry or dead-letter successor copies the id of
// the delivery it came from, and two deliveries of one message can publish
// successors at the same time.
type publishWatcher struct {
	requests chan watcherRequest
	// done is closed when the watcher has exited: its channel closed and no
	// call holds a registration on it, so nothing can ask it anything again.
	done chan struct{}
	// shut is closed once the watcher has seen its channel close, reason
	// included when the broker gave one.
	shut chan struct{}
}

type watcherOp int

const (
	watchRegister watcherOp = iota
	watchSettle
	watchRelease
)

type watcherRequest struct {
	op  watcherOp
	ids []string
	// reply is buffered, so the watcher never waits on a caller.
	reply chan watcherReply
}

// watcherReply answers a register or a settle.
type watcherReply struct {
	// live answers a register: false when the channel has already closed,
	// since nothing registered there can be published.
	live bool
	// busy is closed once the registration that refused a register is
	// released. It is nil when the register succeeded.
	busy <-chan struct{}
	// returned holds the classified return of every settled id the broker
	// returned.
	returned map[string]error
	// closed reports whether the channel has closed, and closeErr is that
	// close as a classified error, the broker's reason included when it gave
	// one.
	closed   bool
	closeErr error
}

type watcherRegistration struct {
	returned error
	// released is made only when a register is refused because of this
	// registration, and closed when this one is settled or released.
	released chan struct{}
}

// startPublishWatcher starts the watcher of one channel's returns and close.
// The goroutine is one the connection owns, so the connection's Close waits for
// it; it ends once the channel has closed and every call that registered on it
// has settled or released, which a closed connection guarantees because every
// confirmation still pending then resolves.
func startPublishWatcher(c *conn, returns <-chan amqp.Return, closes <-chan *amqp.Error) *publishWatcher {
	w := &publishWatcher{
		requests: make(chan watcherRequest),
		done:     make(chan struct{}),
		shut:     make(chan struct{}),
	}
	c.detachWatch.Go(func() {
		w.run(returns, closes)
	})
	return w
}

func (w *publishWatcher) run(returns <-chan amqp.Return, closes <-chan *amqp.Error) {
	defer close(w.done)
	state := watcherState{
		registered: make(map[string]*watcherRegistration),
		returns:    returns,
		closes:     closes,
		shut:       w.shut,
	}
	for state.returns != nil || len(state.registered) != 0 {
		select {
		case item, ok := <-state.returns:
			state.takeReturn(item, ok)
		case reason, ok := <-state.closes:
			state.takeClose(reason, ok)
		case request := <-w.requests:
			state.answer(request)
		}
	}
}

// register reserves ids on this channel for a call about to publish them. It
// returns a nil busy and true when they are reserved, a busy channel and true
// when another call holds one of them, and false when the channel has closed.
func (w *publishWatcher) register(ids []string) (busy <-chan struct{}, live bool) {
	reply, served := w.ask(watchRegister, ids)
	if !served || !reply.live {
		return nil, false
	}
	return reply.busy, true
}

// settle releases ids and reports what the channel knows about them: the
// returns filed under them and whether the channel closed. A call settles only
// after every confirmation it waited for has resolved, which is what makes the
// returns complete.
func (w *publishWatcher) settle(ids []string) watcherReply {
	reply, served := w.ask(watchSettle, ids)
	if !served {
		// Unreachable while ids are registered, since the watcher does not
		// exit with registrations open; a closed channel is the honest answer
		// if it ever is.
		return watcherReply{closed: true, closeErr: classify("publish", driver.KindTransient, amqp.ErrClosed)}
	}
	return reply
}

// release gives ids back without asking about them, for a call that registered
// and then published nothing.
func (w *publishWatcher) release(ids []string) {
	select {
	case w.requests <- watcherRequest{op: watchRelease, ids: ids}:
	case <-w.done:
	}
}

func (w *publishWatcher) ask(op watcherOp, ids []string) (watcherReply, bool) {
	request := watcherRequest{op: op, ids: ids, reply: make(chan watcherReply, 1)}
	select {
	case w.requests <- request:
		return <-request.reply, true
	case <-w.done:
		return watcherReply{}, false
	}
}

type watcherState struct {
	registered map[string]*watcherRegistration
	// returns and closes are set to nil once the client closes them, so the
	// select stops waking on a closed stream.
	returns     <-chan amqp.Return
	closes      <-chan *amqp.Error
	closed      bool
	closeReason *amqp.Error
	shut        chan struct{}
}

func (s *watcherState) takeReturn(item amqp.Return, ok bool) {
	if !ok {
		s.returns = nil
		s.markClosed()
		return
	}
	// A return for an id nobody holds belongs to a call that already gave up
	// on its messages, and there is no one left to tell.
	if registration := s.registered[item.MessageId]; registration != nil && registration.returned == nil {
		registration.returned = returnedPublishError(item)
	}
}

func (s *watcherState) takeClose(reason *amqp.Error, ok bool) {
	if !ok {
		s.closes = nil
	} else if reason != nil {
		s.closeReason = reason
	}
	s.markClosed()
}

// markClosed records that the channel closed. The client sends the close
// notification before it closes the return stream, so by the time either is
// seen the reason, if any, is already buffered; shut is closed only after a
// drain has taken it.
func (s *watcherState) markClosed() {
	if s.closed {
		return
	}
	s.closed = true
	s.drain()
	if s.shut != nil {
		close(s.shut)
	}
}

// drain takes every return and close the client has already handed over,
// without waiting for more. The client sends the close notification before it
// closes the return stream, so a closed return stream seen here comes with the
// reason already buffered, and the drain reads that too.
func (s *watcherState) drain() {
	for s.returns != nil || s.closes != nil {
		select {
		case item, ok := <-s.returns:
			s.takeReturn(item, ok)
		case reason, ok := <-s.closes:
			s.takeClose(reason, ok)
		default:
			return
		}
	}
}

// answer serves a request once everything the client already handed over is
// taken, which is what makes a settle's returns and close complete.
func (s *watcherState) answer(request watcherRequest) {
	s.drain()
	s.serve(request)
}

func (s *watcherState) serve(request watcherRequest) {
	switch request.op {
	case watchRegister:
		request.reply <- s.registerIDs(request.ids)
	case watchSettle:
		reply := watcherReply{closed: s.closed}
		for _, id := range request.ids {
			if registration := s.registered[id]; registration != nil && registration.returned != nil {
				if reply.returned == nil {
					reply.returned = make(map[string]error)
				}
				reply.returned[id] = registration.returned
			}
		}
		s.unregister(request.ids)
		if s.closed {
			reply.closeErr = s.closeError()
		}
		request.reply <- reply
	case watchRelease:
		s.unregister(request.ids)
	}
}

// registerIDs reserves every id or none: a call whose ids were half reserved
// would hold ids it cannot publish while it waits for the rest.
func (s *watcherState) registerIDs(ids []string) watcherReply {
	if s.closed {
		return watcherReply{}
	}
	for _, id := range ids {
		if held := s.registered[id]; held != nil {
			if held.released == nil {
				held.released = make(chan struct{})
			}
			return watcherReply{live: true, busy: held.released}
		}
	}
	for _, id := range ids {
		s.registered[id] = &watcherRegistration{}
	}
	return watcherReply{live: true}
}

func (s *watcherState) unregister(ids []string) {
	for _, id := range ids {
		registration := s.registered[id]
		if registration == nil {
			continue
		}
		if registration.released != nil {
			close(registration.released)
		}
		delete(s.registered, id)
	}
}

// closeError classifies the channel's close, which is where a publish the
// broker refused for its size is recognised. A close that carried no reason -
// a clean shutdown of the connection, or the client closing the notification
// stream without sending on it - keeps the bare amqp.ErrClosed a closed publish
// channel has always reported.
func (s *watcherState) closeError() error {
	if s.closeReason != nil {
		return classifyPublishClose(s.closeReason)
	}
	return classify("publish", driver.KindTransient, amqp.ErrClosed)
}
