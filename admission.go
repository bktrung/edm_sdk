package f1

import (
	"errors"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
)

// workKind names one of the operations that asks the client whether it may
// proceed. Each kind exists because at least one call site decides it with a
// different combination of the client's state than its neighbours do, so the
// differences are named here rather than repeated at each site.
type workKind uint8

const (
	// workPublishEntry is the application publish's entry gate, the first thing
	// PublishBatch does with the message it was handed. It admits a Ready
	// client on a live connection only: once Close has begun, a new application
	// publish is refused even while the producer still stands.
	workPublishEntry workKind = iota
	// workPublish is any publish through the client's shared producer: the
	// application publish after its entry gate, and every core-generated
	// successor publish, which are the retry and dead-letter copies a runner
	// makes while it is settling a delivery. It admits while the producer
	// stands, so a Close that is draining work does not stop a successor copy.
	workPublish
	// workSubscribe is Subscribe, whose gates run once before the subscription
	// is resolved and again after.
	workSubscribe
	// workRun is Runner.Run's gate, which runs before a runner owns anything.
	workRun
	// workConsumerAdmission is the consumer a runner has just opened being
	// admitted to the generation that opened it. It is not workRun: Run's gate
	// runs before the runner owns a consumer and the open waits an attempt out,
	// so that gate admits a connection that is being rebuilt, while this one
	// refuses it. A consumer that appears after the reconnect supervisor has
	// abandoned the runners is one nothing else will release, and the
	// connection it was opened on is the one the swap is about to retire.
	workConsumerAdmission
	// workReconnect is Client.requestReconnect's gate, the one kind that runs
	// while the connection is already being rebuilt.
	workReconnect
	// workHealth is Client.Health, the one kind that reports what is wrong with
	// the connection when the connection and the shutdown are both wrong.
	workHealth
)

// connState is the connection axis of the client's state: which incarnation is
// current, and whether it is usable. It is derived from the connection flags
// and from nothing else.
type connState uint8

const (
	// connNone means the client holds no connection.
	connNone connState = iota
	// connFailed means the connection was given up: a reconnect decision is
	// retained as terminal and no attempt is rebuilding it.
	connFailed
	// connReconnecting means an attempt is rebuilding the connection, and a
	// caller that needs the connection either joins that attempt or is woken
	// when it ends.
	connReconnecting
	// connLive means the connection is the one the client is on and nothing has
	// reported it unusable.
	connLive
)

// lifecycleLocked reports the client's lifecycle. The caller holds c.mu.
//
// It is a read of the stored state and not a derivation: the state is moved by
// the transitions that Close makes, each in the critical section that used to
// set the flag behind it. Ready is the state before Close is entered, Draining
// is a Close attempt in flight, Aborted is one that gave up part way and may be
// retried, and Closed is the state nothing moves out of. Aborted is not
// Draining: admission stays shut in both while the difference is that Close
// itself proceeds from Aborted and is refused from Draining.
func (c *Client) lifecycleLocked() lifecycle.State {
	return c.lifecycle.State()
}

// connStateLocked reports the connection the client is on. The caller holds
// c.mu.
//
// The order of the cases is the order the publish path has always read them:
// an absent connection is reported as absent even when a reconnect decision is
// retained, because a decision about a connection the client no longer holds
// says nothing about what a caller can do next.
func (c *Client) connStateLocked() connState {
	switch {
	case c.current.conn == nil:
		return connNone
	case c.reconnectErr != nil:
		return connFailed
	case c.conn == connReconnecting:
		return connReconnecting
	default:
		return connLive
	}
}

// epochLocked returns the connection incarnation the client is on and the wake
// that is released when that incarnation changes or when a reconnect attempt
// ends. The caller holds c.mu, so the pair is one view: the epoch names the
// connection the wake belongs to. Read c.attemptErr under the same lock after
// the wake fires, and it carries the outcome of the attempt that released the
// waiter.
func (c *Client) epochLocked() (uint64, <-chan struct{}) {
	return c.current.epoch, c.wakeLocked()
}

// staleClaimLocked reports whether the connection a caller captured is no
// longer the client's. The caller holds c.mu.
//
// A zero epoch is a caller that holds no connection and asks no question about
// one, which is the value the sites that never captured a connection pass. A
// caller that captured one passes the number that came with the connection it
// read, and it is stale exactly when the client is on a later incarnation: the
// supervisor installs a connection and its number as one value, so a moved
// number means the connection the caller holds was replaced under it.
func (c *Client) staleClaimLocked(epoch uint64) bool {
	return epoch != 0 && epoch != c.current.epoch
}

// admit reports whether kind may proceed, and returns the error the caller
// returns when it may not. A nil error admits the call; every non-nil error is
// the error the site this kind names returns today, text included. The caller
// holds c.mu.
//
// The whole rule set is the table below. The client's state is three
// independent parts - its lifecycle, whether the shared producer has been torn
// down, and its connection - and no part has precedence over another: a rule
// that reads one part never reads another to break the tie. That is why the
// rules look repetitive and why a single derived state could not express them:
// a Close that timed out after the producer was torn down has the producer fact
// set with the lifecycle back in Aborted, and a reconnect that exhausted its
// budget sets the terminal connection error with the lifecycle still Ready.
//
//	kind             refuses when                              with
//	workPublishEntry lifecycle is not Ready, or no connection   "f1: client is closed"
//	workPublish      lifecycle is Closed, the producer is
//	                 torn down, or no connection               "f1: client is closed"
//	workPublish      the connection failed                     the retained reconnect error
//	workPublish      the connection is reconnecting            the reconnecting error
//	workPublish      the caller's connection was replaced      the reconnecting error
//	workSubscribe    lifecycle is Closed, or no connection     "f1: client is closed"
//	workSubscribe    any other non-Ready lifecycle             "f1: client is closing"
//	workRun          lifecycle is not Ready, or no connection  "f1: client is closing"
//	workConsumerAdmission an attempt is rebuilding the
//	                 connection, or the caller's connection
//	                 was replaced                              the reconnecting error
//	workReconnect    lifecycle is not Ready                    "f1: client is closing"
//	workReconnect    the connection failed                     the retained reconnect error
//	workHealth       lifecycle is Closed                       "f1: client is closed"
//	workHealth       the connection failed                     the retained reconnect error
//	workHealth       no connection                             "f1: client is not connected"
//	workHealth       the connection is reconnecting            "f1: client is reconnecting"
//	workHealth       any other non-Ready lifecycle             "f1: client is closing"
//
// Three consequences are worth stating, because they are the differences the
// table is here to make visible. A publish is admitted while the client is
// Draining or Aborted, as long as the producer still stands, which is what
// lets a runner settle the delivery it holds. A subscribe is refused as soon as
// shutdown begins, because it would create work nothing is draining. And a
// caller that only needs the connection, a health probe or a reconnect request,
// does not care that shutdown has begun unless the connection is gone too.
//
// epoch is the connection incarnation the caller captured, and the zero value
// means the caller holds no connection and asks no question about one. A caller
// that captured one passes the number that came with the connection it read,
// and a kind that takes a live connection refuses once that number is not the
// client's any more: the connection the caller holds was replaced under it,
// which is the condition the reconnecting error already reports.
func (c *Client) admit(kind workKind, epoch uint64) error {
	conn := c.connStateLocked()
	life := c.lifecycleLocked()
	switch kind {
	case workPublishEntry:
		if life != lifecycle.Ready || conn == connNone {
			return errors.New("f1: client is closed")
		}
	case workPublish:
		if life == lifecycle.Closed || c.producerTeardown || conn == connNone {
			return errors.New("f1: client is closed")
		}
		if conn == connFailed {
			return c.reconnectErr
		}
		if conn == connReconnecting {
			return c.reconnectingError("publish")
		}
		if c.staleClaimLocked(epoch) {
			return c.reconnectingError("publish")
		}
	case workSubscribe:
		if life == lifecycle.Closed || conn == connNone {
			return errors.New("f1: client is closed")
		}
		if life != lifecycle.Ready {
			return errors.New("f1: client is closing")
		}
	case workRun:
		if life != lifecycle.Ready || conn == connNone {
			return errors.New("f1: client is closing")
		}
	case workConsumerAdmission:
		// The stored connection axis is read here as the attempt flag it
		// replaced, rather than the derived axis. The derived axis puts a
		// retained reconnect decision before an attempt, which is the order a
		// publish needs, and the attempt still owns the client while that
		// decision is being recorded: reading the derived axis would admit a
		// consumer in that window, and abandonRunners has already run by then,
		// so nothing would release it at the swap.
		if c.conn == connReconnecting {
			return c.reconnectingError("consume")
		}
		if c.staleClaimLocked(epoch) {
			return c.reconnectingError("consume")
		}
	case workReconnect:
		if life != lifecycle.Ready {
			return errors.New("f1: client is closing")
		}
		if conn == connFailed {
			return c.reconnectErr
		}
	case workHealth:
		if life == lifecycle.Closed {
			return errors.New("f1: client is closed")
		}
		switch conn {
		case connFailed:
			return c.reconnectErr
		case connNone:
			return errors.New("f1: client is not connected")
		case connReconnecting:
			return errors.New("f1: client is reconnecting")
		}
		if life != lifecycle.Ready {
			return errors.New("f1: client is closing")
		}
	}
	return nil
}
