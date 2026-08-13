package lifecycle

import (
	"sync/atomic"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/dispatch"
)

// Disposition is the terminal accounting category for one accepted message.
type Disposition uint8

const (
	// Handled means the original message was acknowledged as successful.
	Handled Disposition = iota
	// Requeued means the original message was returned for broker redelivery.
	Requeued
	// Retried means a successor retry or deferral copy was durably published.
	Retried
	// DeadLettered means a successor dead-letter copy was durably published.
	DeadLettered
)

// Counts is a point-in-time accounting snapshot.
type Counts struct {
	Handled      uint64
	Requeued     uint64
	Retried      uint64
	DeadLettered uint64
}

// Total returns the number of recorded dispositions.
func (c Counts) Total() uint64 { return c.Handled + c.Requeued + c.Retried + c.DeadLettered }

// Accounting records dispositions without taking a lock on the settle path.
type Accounting struct {
	handled      atomic.Uint64
	requeued     atomic.Uint64
	retried      atomic.Uint64
	deadLettered atomic.Uint64
}

// Record adds one message to disposition.
func (a *Accounting) Record(disposition Disposition) {
	if a == nil {
		return
	}
	switch disposition {
	case Handled:
		a.handled.Add(1)
	case Requeued:
		a.requeued.Add(1)
	case Retried:
		a.retried.Add(1)
	case DeadLettered:
		a.deadLettered.Add(1)
	}
}

// RecordSettlement maps a settlement result to the message disposition axis.
// A successful settlement needs settledAs because the call result alone does
// not distinguish handled, retried, and dead-lettered messages. Unknown and
// abandoned settlements are counted as requeued because the broker must
// redeliver work whose settlement did not complete.
func (a *Accounting) RecordSettlement(outcome dispatch.SettlementOutcome, settledAs Disposition) {
	if a == nil {
		return
	}
	if outcome != dispatch.SettlementOutcomeSettled {
		a.Record(Requeued)
		return
	}
	switch settledAs {
	case Handled, Retried, DeadLettered:
		a.Record(settledAs)
	default:
		a.Record(Requeued)
	}
}

// Snapshot returns the current counters.
func (a *Accounting) Snapshot() Counts {
	if a == nil {
		return Counts{}
	}
	return Counts{Handled: a.handled.Load(), Requeued: a.requeued.Load(), Retried: a.retried.Load(), DeadLettered: a.deadLettered.Load()}
}
