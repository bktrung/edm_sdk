package kafka

import (
	"context"
	"errors"
	"sync"
)

// errCommitRoundAbandoned is the result a commit round gives the partitions its
// sender could not finish because the sender's own context ended. The caller
// that joined the round is still waiting under a live context, so it joins the
// next round instead of reporting a cancellation that was not its own.
var errCommitRoundAbandoned = errors.New("kafka: offset commit round abandoned by its sender")

// commitSender sends one offset commit request carrying every point of a round
// and returns each partition's own result. A partition missing from the result
// map committed.
type commitSender func(ctx context.Context, points map[partitionKey]int64) map[partitionKey]error

// offsetCommitter batches the offset commits of one consumer. franz-go runs
// one commit at a time, so a commit per settlement puts every settlement of the
// consumer in one queue behind a broker round trip each. The committer instead
// collects the commit points that arrive while a request is in flight into the
// next round, and sends the whole round as one request when the flight ends.
//
// The batching needs no timer and no size: a round holds what arrived during
// one round trip, so it is one point at low load and grows with the load. A
// commit still returns only after the request carrying its point finished, so
// a settlement that returned nil is committed, as it was with one request per
// settlement.
//
// A Kafka commit point is cumulative, so a round keeps one point per partition,
// the highest. Every point is a settlement the caller already made, so sending
// a point whose caller stopped waiting commits a record that was handled, and a
// lower point sent after a higher one only moves the committed offset back to a
// record that is delivered again, which is the at-least-once side.
//
// There is no committer goroutine. The caller that finds no request in flight
// sends the round it joined, and a sender that finishes hands the next round to
// that round's own callers, so the committer has no lifetime of its own to end
// with the consumer's.
//
// The zero value is ready to use and is safe for concurrent use.
type offsetCommitter struct {
	mu      sync.Mutex
	sending bool
	// round collects the points of the next request. It is nil when no caller
	// is waiting for a request that has not started.
	round *commitRound
}

type commitRound struct {
	points map[partitionKey]int64
	// errs is written by the round's sender before done is closed and only
	// read after it.
	errs map[partitionKey]error
	// done is closed when the request carrying the round has finished.
	done chan struct{}
	// lead is closed when the request before this round has finished and one
	// of the round's callers may send it. A round created while nothing was in
	// flight is sent by the caller that created it, and never needs it closed.
	lead chan struct{}
}

// commit commits point, the offset after the last settled record, for key's
// partition. It returns when the request carrying the point finished, with the
// partition's own result, or when ctx ends first.
func (o *offsetCommitter) commit(ctx context.Context, key partitionKey, point int64, send commitSender) error {
	for {
		round := o.join(key, point)
		err := o.await(ctx, round, key, send)
		if !errors.Is(err, errCommitRoundAbandoned) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (o *offsetCommitter) join(key partitionKey, point int64) *commitRound {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.round == nil {
		o.round = &commitRound{
			points: make(map[partitionKey]int64, 1),
			done:   make(chan struct{}),
			lead:   make(chan struct{}),
		}
	}
	if current, queued := o.round.points[key]; !queued || point > current {
		o.round.points[key] = point
	}
	return o.round
}

// await waits for round's request, sending it itself when the committer is
// free. A caller whose lead signal fires and loses the claim to another caller
// of the same round stops watching the signal: the winner sends the round, so
// done is the only thing left to wait for.
func (o *offsetCommitter) await(ctx context.Context, round *commitRound, key partitionKey, send commitSender) error {
	lead := round.lead
	for {
		if o.claim(round) {
			o.send(ctx, round, send)
		}
		select {
		case <-round.done:
			return round.errs[key]
		case <-lead:
			lead = nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// claim reports whether the caller may send round now: nothing is in flight and
// round is still the one collecting points. A claimed round stops collecting,
// so a point that arrives after the claim goes to the next round.
func (o *offsetCommitter) claim(round *commitRound) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.sending || o.round != round {
		return false
	}
	o.sending = true
	o.round = nil
	return true
}

// send sends round under the sender's context, publishes each partition's
// result, and hands the round collected meanwhile to its callers.
//
// A request the sender's own context ended did not fail for the other callers
// of the round, so their partitions read errCommitRoundAbandoned and join the
// next round. The next round's lead is closed after the sending flag clears,
// which is the order a woken caller's claim needs to find the committer free.
func (o *offsetCommitter) send(ctx context.Context, round *commitRound, send commitSender) {
	errs := send(ctx, round.points)
	if ctx.Err() != nil {
		for key, err := range errs {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				errs[key] = errCommitRoundAbandoned
			}
		}
	}
	round.errs = errs
	close(round.done)
	o.mu.Lock()
	o.sending = false
	next := o.round
	o.mu.Unlock()
	if next != nil {
		close(next.lead)
	}
}
