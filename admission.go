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

// lifecycleLocked maps the Close flags onto the lifecycle state that admission
// reads. The caller holds c.mu.
//
// The three flags are not one boolean. closed is terminal, closing lasts only
// as long as the Close attempt that set it and is cleared when that attempt
// fails, and shutdownStarted is set once when Close is entered and never
// cleared. Aborted is the combination those rules produce: shutdown has begun,
// no Close attempt is running, and Close may be retried.
func (c *Client) lifecycleLocked() lifecycle.State {
	switch {
	case c.closed:
		return lifecycle.Closed
	case c.closing:
		return lifecycle.Draining
	case c.shutdownStarted:
		return lifecycle.Aborted
	default:
		return lifecycle.Ready
	}
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
	case c.conn == nil:
		return connNone
	case c.reconnectErr != nil:
		return connFailed
	case c.reconnecting:
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
	return c.epoch, c.wakeLocked()
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
//	workSubscribe    lifecycle is Closed, or no connection     "f1: client is closed"
//	workSubscribe    any other non-Ready lifecycle             "f1: client is closing"
//	workRun          lifecycle is not Ready, or no connection  "f1: client is closing"
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
// epoch is the connection incarnation the caller believes it holds. Nothing
// reads it yet: the connection checks stay at the call sites until they move
// here, and the sites pass the zero value.
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
