package kafka

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const kafkaTimestampPrecision = time.Millisecond

type deferralDecision struct {
	due  time.Time
	wait bool
	err  error
}

func kafkaNow() time.Time {
	return time.Now() //nolint:forbidigo // Kafka record timestamps and due headers use wall time.
}

func evaluateDeferral(record *kgo.Record, delay time.Duration, known bool, now time.Time) deferralDecision {
	due, present, err := recordDelayUntil(record)
	if err != nil {
		return deferralDecision{err: err}
	}
	if !known {
		return deferralDecision{err: errors.New("destination delay is unknown")}
	}
	if !present {
		if delay > 0 {
			return deferralDecision{err: errors.New("deferred due-time header is missing")}
		}
		return deferralDecision{}
	}
	if delay <= 0 {
		return deferralDecision{err: errors.New("destination delay is zero")}
	}
	if record.Timestamp.IsZero() {
		return deferralDecision{err: errors.New("record timestamp is missing")}
	}
	lower := record.Timestamp.Add(delay * 4 / 5)
	// Kafka serializes record timestamps to milliseconds, so the upper edge can round down.
	upper := record.Timestamp.Add(delay*6/5 + kafkaTimestampPrecision)
	if due.Before(lower) || due.After(upper) {
		return deferralDecision{due: due, err: fmt.Errorf("due time %s is outside destination delay band [%s, %s]", due, lower, upper)}
	}
	return deferralDecision{due: due, wait: due.After(now)}
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
	if c.now != nil {
		return c.now()
	}
	return kafkaNow()
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
		// Topic-granular pausing also stalls other partitions on this topic;
		// the bounded delay is the accepted cost of keeping pending memory bounded.
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
		if reason != pauseReasonDeferred {
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
	slog.Default().Warn(
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

func (c *consumer) clearDeferredReasons(pending []*kgo.Record) {
	var future map[string]struct{}
	if len(pending) > 0 {
		future = make(map[string]struct{}, len(pending))
		now := c.currentTime()
		for _, record := range pending {
			delay, known := c.destinationDelay(record.Topic)
			if evaluateDeferral(record, delay, known, now).wait {
				future[record.Topic] = struct{}{}
			}
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for destination, reasons := range c.pauseReasons {
		if _, deferred := reasons[pauseReasonDeferred]; !deferred {
			continue
		}
		if _, remains := future[destination]; !remains {
			// This is the only removal site for deferred: a re-check found
			// no pending record for this destination that is still not due.
			c.setPauseReasonLocked(destination, pauseReasonDeferred, false)
		}
	}
}
