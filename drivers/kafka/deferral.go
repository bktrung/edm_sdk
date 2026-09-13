package kafka

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	kafkaTimestampPrecision      = time.Millisecond
	kafkaDeferralUpperNumerator  = 3
	kafkaDeferralBandDenominator = 2
	kafkaMaxDuration             = time.Duration(1<<63 - 1)
	// kafkaDeferredHoldMinimum is the floor under a destination's hold limit.
	// One waiting record must never be enough to hold a destination's fetches:
	// the record behind it can be due sooner, and holding there is what makes
	// it wait for a due time that is not its own.
	kafkaDeferredHoldMinimum = 2
)

type deferralDecision struct {
	due     time.Time
	present bool
	wait    bool
	err     error
}

func kafkaNow() time.Time {
	return time.Now() //nolint:forbidigo // Kafka record timestamps and due headers use wall time.
}

func evaluateDeferral(record *kgo.Record, delay time.Duration, known bool, now time.Time) deferralDecision {
	due, present, err := recordDelayUntil(record)
	if err != nil {
		return deferralDecision{present: present, err: err}
	}
	if !known {
		return deferralDecision{present: present, err: errors.New("destination delay is unknown")}
	}
	if !present {
		if delay > 0 {
			return deferralDecision{err: errors.New("deferred due-time header is missing")}
		}
		return deferralDecision{}
	}
	if delay <= 0 {
		return deferralDecision{present: true, err: errors.New("destination delay is zero")}
	}
	if record.Timestamp.IsZero() {
		return deferralDecision{present: true, err: errors.New("record timestamp is missing")}
	}
	lower := record.Timestamp.Add(delay / kafkaDeferralBandDenominator)
	upperOffset, upperRepresentable := kafkaDeferralUpperOffset(delay)
	upper := time.Time{}
	if upperRepresentable {
		upper = record.Timestamp.Add(upperOffset)
	}
	if due.Before(lower) || (upperRepresentable && due.After(upper)) {
		if !upperRepresentable {
			return deferralDecision{due: due, present: true, err: fmt.Errorf("due time %s is outside destination delay band [%s, unbounded]", due, lower)}
		}
		return deferralDecision{due: due, present: true, err: fmt.Errorf("due time %s is outside destination delay band [%s, %s]", due, lower, upper)}
	}
	return deferralDecision{due: due, present: true, wait: due.After(now)}
}

func kafkaDeferralUpperOffset(delay time.Duration) (time.Duration, bool) {
	// Split before multiplying so a legal delay near MaxInt64 cannot wrap.
	half := delay / kafkaDeferralBandDenominator
	remainder := delay % kafkaDeferralBandDenominator
	available := kafkaMaxDuration - kafkaTimestampPrecision - remainder
	if half > available/kafkaDeferralUpperNumerator {
		return 0, false
	}
	return half*kafkaDeferralUpperNumerator + remainder + kafkaTimestampPrecision, true
}

func recordDelayUntil(record *kgo.Record) (time.Time, bool, error) {
	var (
		raw   []byte
		found bool
	)
	for _, header := range record.Headers {
		if header.Key != delayUntilHeader {
			continue
		}
		if found {
			return time.Time{}, true, errors.New("duplicate deferred due-time headers")
		}
		found = true
		raw = header.Value
	}
	if !found {
		return time.Time{}, false, nil
	}
	nanos, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return time.Time{}, true, fmt.Errorf("invalid deferred due-time header %q: %w", raw, err)
	}
	return time.Unix(0, nanos), true, nil
}

func outboundDue(messageDelay time.Time, destinationDelay time.Duration, known bool, now time.Time) time.Time {
	if !messageDelay.IsZero() || !known || destinationDelay <= 0 {
		return messageDelay
	}
	return now.Add(destinationDelay)
}

func (c *consumer) currentTime() time.Time {
	return c.clock.Now()
}

func (c *consumer) destinationDelay(destination string) (time.Duration, bool) {
	if c.conn == nil {
		return 0, false
	}
	return c.conn.destinationDelay(destination)
}

func (c *consumer) admissionLocked(record *kgo.Record) bool {
	destination := record.Topic
	delay, known := c.destinationDelay(destination)
	decision := evaluateDeferral(record, delay, known, c.currentTime())
	if decision.err != nil {
		c.reportDeferralErrorLocked(record, decision.err)
	}
	if decision.wait {
		// The record waits for its due time, and this destination holds it.
		// The reason gates redelivery of a record this destination already
		// delivered; it deliberately does not hold the fetches, because a
		// record behind this one may be due sooner and is only reachable by
		// reading past it. syncDeferredPauses bounds how many records may wait
		// and holds the fetches at that bound.
		c.setPauseReasonLocked(destination, pauseReasonDeferred, true)
	}

	budget := c.budgets[destination]
	if budget <= 0 {
		budget = 1
	}
	key := partitionKey{destination: destination, partition: record.Partition}
	tracker := c.trackers[key]
	hasRequeue := tracker != nil && tracker.hasRequeue(record.Offset)
	reasons := c.pauseReasons[destination]
	if decision.wait && !hasRequeue {
		return false
	}
	if hasRequeue {
		if !reasons.empty() && !reasons.permitsRedelivery() {
			return false
		}
	} else if reasons.blocksDelivery() {
		return false
	}
	if c.unsettled[destination] >= budget && !hasRequeue {
		c.setPauseReasonLocked(destination, pauseReasonPrefetch, true)
		return false
	}
	return true
}

func (s pauseReasonSet) blocksDelivery() bool {
	for reason := range s {
		if !reason.holding() {
			return true
		}
	}
	return false
}

func (c *consumer) reportDeferralErrorLocked(record *kgo.Record, cause error) {
	destination := record.Topic
	if c.reportedDeferrals == nil {
		c.reportedDeferrals = make(map[string]struct{})
	}
	if _, reported := c.reportedDeferrals[destination]; reported {
		return
	}
	c.reportedDeferrals[destination] = struct{}{}
	c.conn.log().Warn(
		"kafka deferred delivery fault",
		"destination", destination,
		"condition", cause.Error(),
		"partition", record.Partition,
		"offset", record.Offset,
	)
}

func (c *consumer) pendingDeadline(pending []*kgo.Record) (time.Time, bool) {
	now := c.currentTime()
	var (
		earliest time.Time
		found    bool
	)
	for _, record := range pending {
		delay, known := c.destinationDelay(record.Topic)
		decision := evaluateDeferral(record, delay, known, now)
		if !decision.wait || (found && !decision.due.Before(earliest)) {
			continue
		}
		earliest = decision.due
		found = true
	}
	return earliest, found
}

// syncDeferredPauses reconciles the deferred pauses with the records the poll
// loop is holding. The loop runs it before every poll, so both pauses follow
// the records in hand rather than the last admission.
//
// The two pauses are deliberately different. The deferred reason marks a
// destination that holds a record waiting for its due time and gates
// redelivery only. The hold reason is what keeps franz-go from fetching a
// destination that already holds its fill of records waiting for a due time,
// and that bound is the only thing the driver spends on records it cannot
// deliver yet. Pausing the fetches at the first waiting record instead, as the
// marker used to, holds the destination at a due time that is not the next one
// owed: every record behind that one waits for it, however much sooner its own
// due time is.
func (c *consumer) syncDeferredPauses(pending []*kgo.Record) {
	held := c.heldByDestination(pending)

	c.mu.Lock()
	defer c.mu.Unlock()
	// Releases run first, so no destination enters the map while it is ranged.
	for destination, reasons := range c.pauseReasons {
		if _, deferred := reasons[pauseReasonDeferred]; deferred && held[destination] == 0 {
			c.setPauseReasonLocked(destination, pauseReasonDeferred, false)
		}
		if _, full := reasons[pauseReasonHold]; full && held[destination] < c.deferredHoldLimit(destination) {
			c.setPauseReasonLocked(destination, pauseReasonHold, false)
		}
	}
	for destination, count := range held {
		c.setPauseReasonLocked(destination, pauseReasonDeferred, true)
		if count >= c.deferredHoldLimit(destination) {
			c.setPauseReasonLocked(destination, pauseReasonHold, true)
		}
	}
}

// heldByDestination counts, per destination, the records pending is holding
// that are not yet due. A record that is due is not one of them: the next
// flushPending delivers it.
func (c *consumer) heldByDestination(pending []*kgo.Record) map[string]int {
	if len(pending) == 0 {
		return nil
	}
	held := make(map[string]int, len(c.destinations))
	now := c.currentTime()
	for _, record := range pending {
		delay, known := c.destinationDelay(record.Topic)
		if evaluateDeferral(record, delay, known, now).wait {
			held[record.Topic]++
		}
	}
	return held
}

// deferredHoldLimit is how many records waiting for a due time a destination
// may hold before its fetches are held. The admission budget is the operator's
// read-ahead knob and bounds how much work one destination keeps in flight, and
// the floor keeps the guarantee this pause must not spend: one waiting record
// is the record the poll loop is waiting for, and holding the fetches at that
// first record is what hides everything behind it.
func (c *consumer) deferredHoldLimit(destination string) int {
	return max(c.budgets[destination], kafkaDeferredHoldMinimum)
}
