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

// evaluateDeferral decides what one record's due-time header means. delay is
// the destination's declared delay, and zero is a destination that declares
// none: a due-time header for such a destination is the fault below, not a
// state to report, because the consumer reads the delay from its own config and
// an absent destination is the answer rather than a gap in it.
func evaluateDeferral(record *kgo.Record, delay time.Duration, now time.Time) deferralDecision {
	due, present, err := recordDelayUntil(record)
	if err != nil {
		return deferralDecision{present: present, err: err}
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

func (c *consumer) admissionLocked(record *kgo.Record) bool {
	destination := record.Topic
	delay := c.cfg.Delays[destination]
	decision := evaluateDeferral(record, delay, c.currentTime())
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
		// A requeue lifts this pause so the rewound partition can be fetched,
		// and it keeps its slot charged until the redelivery settles. Pausing
		// again here would forbid the very fetch the requeue is waiting for:
		// franz-go does not fetch a paused topic, so the redelivery would never
		// arrive. The pause comes back when the redelivery fills the budget in
		// emit, or at the next refusal once the requeue has resolved.
		if !c.hasPendingRequeueLocked(destination) {
			c.setPauseReasonLocked(destination, pauseReasonPrefetch, true)
		}
		return false
	}
	if c.outstanding[key] > 0 && !hasRequeue {
		// The hold rule: this partition already has a delivery outstanding, and
		// the next one waits for it to settle. A requeue is the exception,
		// because the redelivery of the outstanding offset is that partition's
		// one delivery rather than a second one; it re-enters emission as a
		// reuse and is not charged again.
		//
		// The refused record stays in the poll loop's pending list, exactly as a
		// record over the destination budget does, and the settle that clears
		// the count wakes the loop to admit it.
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
		delay := c.cfg.Delays[record.Topic]
		decision := evaluateDeferral(record, delay, now)
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
		delay := c.cfg.Delays[record.Topic]
		if evaluateDeferral(record, delay, now).wait {
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

// syncReadAheadPauses reconciles the per-partition fetch holds with the records
// the poll loop is holding. The loop runs it before every poll, beside
// syncDeferredPauses, so a partition's fetches are held while the loop already
// carries its read-ahead limit in pending for it and released once it drops
// below.
//
// The hold is keyed by partition and never by destination. Under the hold rule
// c.unsettled[destination] can never exceed the partitions this member holds,
// so the destination budget's own pause is unreachable in most shapes and
// nothing else bounds pending; a destination-keyed bound would also let one
// slow partition stop its siblings' fetches. franz-go keeps a partition pause
// apart from a topic pause, so this hold cannot resume a pause the caller set.
//
// A partition whose redelivery a requeue is waiting for is never held. That
// redelivery arrives from the broker, and the partition's count is waiting for
// it to settle, so holding the fetch would strand a requeue that nothing else
// can release.
func (c *consumer) syncReadAheadPauses(pending []*kgo.Record) {
	c.mu.Lock()
	defer c.mu.Unlock()
	held := c.countPendingLocked(pending)
	for key := range c.readAheadPaused {
		if held[key] < c.readAheadLimit(key.destination) || c.requeued[key] > 0 {
			c.releaseReadAheadHoldLocked(key)
		}
	}
	for key, count := range held {
		if count < c.readAheadLimit(key.destination) || c.requeued[key] > 0 {
			continue
		}
		if _, already := c.readAheadPaused[key]; already {
			continue
		}
		c.holdReadAheadLocked(key)
	}
}

// countPendingLocked recounts, per partition, the records pending holds, which
// decides the partitions this pass holds and releases. The poll loop is the
// only writer and it runs this before every poll, so the count describes the
// pending list the next poll leaves behind.
//
// Nothing outside the reconcile may read the count. A settle releasing a
// partition's charge does not consult it: a count taken before this iteration's
// fetch is stale exactly while the loop is on its way to a wait, so a wake
// skipped on it would strand every record the count described.
func (c *consumer) countPendingLocked(pending []*kgo.Record) map[partitionKey]int {
	if c.pendingHeld == nil {
		c.pendingHeld = make(map[partitionKey]int, len(c.destinations))
	}
	clear(c.pendingHeld)
	for _, record := range pending {
		c.pendingHeld[partitionKey{destination: record.Topic, partition: record.Partition}]++
	}
	return c.pendingHeld
}

// readAheadLimitNoHold is the read-ahead limit this sitting measures with. It
// clears the per-partition backlog of the widest shape under measurement, which
// is the compact corpus's 1000 records over 16 partitions at about 62 records
// each, so no partition reaches the limit and the hold never toggles. The value
// is an experiment rather than configuration: the destination's prefetch share
// is the window the port was built around, and this replaces it only until the
// measurement says which of the two costs the limit carries.
const readAheadLimitNoHold = 100

// readAheadLimit is how many of one partition's records the poll loop may carry
// in pending before that partition's fetches are held. It is the destination's
// prefetch share floored at readAheadLimitNoHold: the shares the measured
// subscriptions run with (22 at the shipped concurrency, 6 below it) are all
// under that floor, so none of them is ever held, while a caller that asks for a
// wider window than the floor still gets the window it asked for. The floor is
// never below one, so a partition may always carry the successor its own settle
// is about to admit, which a limit of zero would forbid.
func (c *consumer) readAheadLimit(destination string) int {
	return max(c.budgets[destination], readAheadLimitNoHold)
}

// holdReadAheadLocked holds one partition's fetches and records the hold, so
// the next reconcile knows which partitions this driver is holding. The caller
// must hold c.mu.
func (c *consumer) holdReadAheadLocked(key partitionKey) {
	if c.readAheadPaused == nil {
		c.readAheadPaused = make(map[partitionKey]struct{})
	}
	c.readAheadPaused[key] = struct{}{}
	if c.client != nil {
		c.client.PauseFetchPartitions(map[string][]int32{key.destination: {key.partition}})
	}
}

// releaseReadAheadHoldLocked returns one partition's fetches and forgets the
// hold. The caller must hold c.mu.
func (c *consumer) releaseReadAheadHoldLocked(key partitionKey) {
	delete(c.readAheadPaused, key)
	if c.client != nil {
		c.client.ResumeFetchPartitions(map[string][]int32{key.destination: {key.partition}})
	}
}
