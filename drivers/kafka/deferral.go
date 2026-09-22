package kafka

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type partitionPauseReason string

const (
	partitionPauseReadAhead partitionPauseReason = "read-ahead"
	partitionPauseHeadHold  partitionPauseReason = "head-hold"
)

type partitionPauseSet map[partitionPauseReason]struct{}

func (s partitionPauseSet) add(reason partitionPauseReason) bool {
	if _, exists := s[reason]; exists {
		return false
	}
	s[reason] = struct{}{}
	return len(s) == 1
}

func (s partitionPauseSet) remove(reason partitionPauseReason) bool {
	if _, exists := s[reason]; !exists {
		return false
	}
	delete(s, reason)
	return len(s) == 0
}

func (s partitionPauseSet) empty() bool { return len(s) == 0 }

func (c *consumer) currentTime() time.Time {
	return c.clock.Now()
}

// kafkaTimestampPrecision is the resolution Kafka stores a record timestamp
// at. A producer's CreateTime is written in milliseconds, so the timestamp a
// consumer reads has already lost the sub-millisecond part of the instant the
// producer published at; the two differ by less than this constant.
const kafkaTimestampPrecision = time.Millisecond

// dueTime returns the instant a deferred record becomes due, and whether it is
// deferred at all.
//
// The due time is the record's publish instant plus its destination's declared
// delay, and the port says a delivery is never earlier than that instant. The
// stored timestamp is the publish instant rounded down to the resolution the
// broker keeps, so the earliest instant consistent with it is the next
// millisecond boundary: a due time taken from the stored value as it stands
// would deliver up to a millisecond before the instant the publisher meant.
func (c *consumer) dueTime(record *kgo.Record) (time.Time, bool) {
	delay, known := c.cfg.Delays[record.Topic]
	if !known || delay <= 0 || record.Timestamp.IsZero() {
		return time.Time{}, false
	}
	return record.Timestamp.Truncate(kafkaTimestampPrecision).Add(kafkaTimestampPrecision).Add(delay), true
}

func (c *consumer) admissionLocked(record *kgo.Record) bool {
	key := partitionKey{destination: record.Topic, partition: record.Partition}
	// A requeued record is the head of its partition's queue and has already
	// been delivered once, so the due-time hold does not apply to it: it is
	// owed now, and the fetch hold of its partition is about fetching only.
	requeued := c.requeued[key] > 0
	if !requeued {
		if due, deferred := c.dueTime(record); deferred && due.After(c.currentTime()) {
			c.setHeadHoldLocked(key, due)
			return false
		}
	}

	budget := c.budgets[record.Topic]
	if budget <= 0 {
		budget = 1
	}
	reasons := c.pauseReasons[record.Topic]
	if requeued {
		// A requeue is the redelivery of a delivery the destination already
		// has in hand, so the destination's own hold must not refuse it. A
		// user pause still does: the caller asked for this destination to
		// stop, and a requeue is not the caller taking that back.
		if !reasons.permitsRedelivery() {
			return false
		}
	} else if reasons.blocksDelivery() {
		return false
	}
	if c.unsettled[record.Topic] >= budget && !requeued {
		// The redelivery enters emission as a reuse of the charge its
		// predecessor holds, so it is not a second delivery against the
		// budget. The pause comes back when the redelivery settles.
		c.setPauseReasonLocked(record.Topic, pauseReasonPrefetch, true)
		return false
	}
	if c.outstanding[key] > 0 && !requeued {
		// The hold rule: this partition already has a delivery outstanding, and
		// the next one waits for it to settle. A requeue is the exception,
		// because the redelivery of the outstanding offset is that partition's
		// one delivery rather than a second one.
		return false
	}
	return true
}

func (c *consumer) setHeadHoldLocked(key partitionKey, due time.Time) {
	if c.heldUntil == nil {
		c.heldUntil = make(map[partitionKey]time.Time)
	}
	if previous, ok := c.heldUntil[key]; !ok || !previous.Equal(due) {
		c.heldUntil[key] = due
		c.syncHeadTimerLocked()
	}
	c.setPartitionPauseReasonLocked(key, partitionPauseHeadHold, true)
}

func (c *consumer) clearHeadHoldLocked(key partitionKey) {
	delete(c.heldUntil, key)
	c.setPartitionPauseReasonLocked(key, partitionPauseHeadHold, false)
	c.syncHeadTimerLocked()
}

func (c *consumer) setPartitionPauseReasonLocked(key partitionKey, reason partitionPauseReason, add bool) {
	if c.partitionPauses == nil {
		c.partitionPauses = make(map[partitionKey]partitionPauseSet)
	}
	reasons := c.partitionPauses[key]
	if reasons == nil {
		reasons = make(partitionPauseSet)
		c.partitionPauses[key] = reasons
	}
	if add {
		if !reasons.add(reason) {
			return
		}
		if c.client != nil {
			c.client.PauseFetchPartitions(map[string][]int32{key.destination: {key.partition}})
		}
		return
	}
	if !reasons.remove(reason) {
		return
	}
	if reasons.empty() {
		delete(c.partitionPauses, key)
		c.resumePartitionIfFreeLocked(key)
		c.wakePollLocked()
	}
}

func (c *consumer) resumePartitionIfFreeLocked(key partitionKey) {
	if c.client == nil || c.draining || c.stopped || !c.owned[key] {
		return
	}
	if reasons := c.partitionPauses[key]; !reasons.empty() {
		return
	}
	c.client.ResumeFetchPartitions(map[string][]int32{key.destination: {key.partition}})
}

// syncReadAheadPauses reconciles every partition's read-ahead reason with its
// queue: a partition at its read-ahead limit is paused, one below it is not,
// and a partition with a requeue waiting is never held for read-ahead, because
// the redelivery it is waiting for has to reach the head.
func (c *consumer) syncReadAheadPauses() {
	c.mu.Lock()
	defer c.mu.Unlock()
	counts := make(map[partitionKey]int, len(c.pending))
	for key, records := range c.pending {
		counts[key] = len(records)
	}
	for key, reasons := range c.partitionPauses {
		if _, held := reasons[partitionPauseReadAhead]; held && (counts[key] < c.readAheadLimit(key.destination) || c.requeued[key] > 0) {
			c.setPartitionPauseReasonLocked(key, partitionPauseReadAhead, false)
		}
	}
	for key, count := range counts {
		if count >= c.readAheadLimit(key.destination) && c.requeued[key] == 0 {
			c.setPartitionPauseReasonLocked(key, partitionPauseReadAhead, true)
		}
	}
}

const readAheadLimitNoHold = 100

func (c *consumer) readAheadLimit(destination string) int {
	return max(c.budgets[destination], readAheadLimitNoHold)
}

func (c *consumer) syncHeadTimerLocked() {
	var earliest time.Time
	for _, due := range c.heldUntil {
		if earliest.IsZero() || due.Before(earliest) {
			earliest = due
		}
	}
	if earliest.Equal(c.headTimerDue) {
		return
	}
	if c.headTimerSet {
		c.headTimer.Stop()
		c.headTimerSet = false
	}
	c.headTimerDue = earliest
	if !earliest.IsZero() && c.clock != nil {
		delay := earliest.Sub(c.currentTime())
		if delay < 0 {
			delay = 0
		}
		c.headTimer = c.clock.Timer(delay)
		c.headTimerSet = true
	}
	if c.headTimerChanged != nil {
		select {
		case c.headTimerChanged <- struct{}{}:
		default:
		}
	}
}

// headHoldLoop wakes the poll loop when the earliest held head record comes
// due. It lives exactly as long as the poll loop does, because the poll loop is
// the only writer of the hold set: once that loop has returned there is nothing
// left to wake, and a consumer whose client was closed without a stop would
// otherwise leave this goroutine behind.
func (c *consumer) headHoldLoop() {
	for {
		c.mu.Lock()
		done := c.pollDone
		changed := c.headTimerChanged
		var timerC <-chan time.Time
		if c.headTimerSet {
			timerC = c.headTimer.C
		}
		c.mu.Unlock()
		select {
		case <-done:
			c.mu.Lock()
			if c.headTimerSet {
				c.headTimer.Stop()
				c.headTimerSet = false
			}
			c.headTimerDue = time.Time{}
			c.mu.Unlock()
			return
		case <-changed:
			continue
		case <-timerC:
			c.mu.Lock()
			// A fire that is not the current timer's is one this loop already
			// replaced: re-arming stops the old timer, but a value it had
			// already buffered stays readable on its channel. Acting on that
			// value would stop the timer that replaced it and leave a held head
			// with no wake of its own, so the loop waits for the next state
			// change instead.
			if c.headTimerSet && c.headTimer.C == timerC {
				c.headTimer.Stop()
				c.headTimerSet = false
				c.headTimerDue = time.Time{}
				c.wakePollLocked()
			}
			c.mu.Unlock()
		}
	}
}
