package kafka

import (
	"bytes"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestEvaluateDeferralDueInPastDelivers(t *testing.T) {
	published := time.Unix(100, 0)
	record := deferredTestRecord(published, published.Add(8*time.Second))

	decision := evaluateDeferral(record, 10*time.Second, true, published.Add(9*time.Second))
	if decision.err != nil || decision.wait {
		t.Fatalf("evaluateDeferral() = %#v, want no error and no wait", decision)
	}
}

func TestEvaluateDeferralDueInFutureWaits(t *testing.T) {
	published := time.Unix(100, 0)
	record := deferredTestRecord(published, published.Add(10*time.Second))

	decision := evaluateDeferral(record, 10*time.Second, true, published)
	if decision.err != nil || !decision.wait {
		t.Fatalf("evaluateDeferral() = %#v, want no error and wait", decision)
	}
}

func TestEvaluateDeferralAcceptsLowerBandEdge(t *testing.T) {
	published := time.Unix(100, 0)
	delay := 10 * time.Second
	record := deferredTestRecord(published, published.Add(delay/2))

	decision := evaluateDeferral(record, delay, true, published.Add(delay/2))
	if decision.err != nil || decision.wait {
		t.Fatalf("evaluateDeferral() = %#v, want accepted lower edge at due time", decision)
	}
}

func TestEvaluateDeferralJitterHalfWaitsUntilDue(t *testing.T) {
	published := time.Unix(100, 0)
	delay := 10 * time.Second
	due := published.Add(delay / 2)
	record := deferredTestRecord(published, due)

	decision := evaluateDeferral(record, delay, true, published)
	if decision.err != nil || !decision.wait {
		t.Fatalf("evaluateDeferral() = %#v, want legal jitter-0.5 due time to wait", decision)
	}
}

func TestEvaluateDeferralRejectsJustBelowLowerBand(t *testing.T) {
	published := time.Unix(100, 0)
	delay := 10 * time.Second
	record := deferredTestRecord(published, published.Add(delay/2-time.Nanosecond))

	decision := evaluateDeferral(record, delay, true, published)
	if decision.err == nil || decision.wait {
		t.Fatalf("evaluateDeferral() = %#v, want lower-edge rejection without waiting", decision)
	}
}

func TestEvaluateDeferralAllowsKafkaTimestampPrecisionAtUpperBand(t *testing.T) {
	published := time.Unix(100, 0)
	delay := 10 * time.Second
	due := published.Add(delay + delay/2 + time.Millisecond)
	record := deferredTestRecord(published, due)

	decision := evaluateDeferral(record, delay, true, due)
	if decision.err != nil || decision.wait {
		t.Fatalf("evaluateDeferral() = %#v, want no error and no wait", decision)
	}
}

func TestEvaluateDeferralRejectsJustAboveUpperBand(t *testing.T) {
	published := time.Unix(100, 0)
	delay := 10 * time.Second
	upper := delay + delay/2 + time.Millisecond
	record := deferredTestRecord(published, published.Add(upper+time.Nanosecond))

	decision := evaluateDeferral(record, delay, true, published)
	if decision.err == nil || decision.wait {
		t.Fatalf("evaluateDeferral() = %#v, want upper-edge rejection without waiting", decision)
	}
}

func TestEvaluateDeferralAcceptsLargeDelayWithoutUpperOverflow(t *testing.T) {
	published := time.Unix(0, 0)
	delay := time.Duration(1<<63 - 1)
	due := published.Add(delay)
	record := deferredTestRecord(published, due)

	decision := evaluateDeferral(record, delay, true, due)
	if decision.err != nil || decision.wait {
		t.Fatalf("evaluateDeferral() = %#v, want no error and no wait for a large nominal delay", decision)
	}
}

func TestEvaluateDeferralDueExactlyNowDelivers(t *testing.T) {
	published := time.Unix(100, 0)
	now := published.Add(10 * time.Second)
	record := deferredTestRecord(published, now)

	decision := evaluateDeferral(record, 10*time.Second, true, now)
	if decision.err != nil || decision.wait {
		t.Fatalf("evaluateDeferral() = %#v, want no error and no wait", decision)
	}
}

func TestEvaluateDeferralMissingHeaderSurfacesErrorAndDelivers(t *testing.T) {
	record := &kgo.Record{Topic: "retry", Timestamp: time.Unix(100, 0)}

	decision := evaluateDeferral(record, 10*time.Second, true, time.Unix(100, 0))
	if decision.err == nil {
		t.Fatal("evaluateDeferral() error = nil, want missing-header error")
	}
	if decision.wait {
		t.Fatal("evaluateDeferral() wait = true, want delivery after surfacing error")
	}
}

func TestRecordForMessageUsesDestinationDelayWhenDueZero(t *testing.T) {
	now := time.Unix(100, 0)
	record := recordForMessage(driver.OutboundMessage{
		Destination: "retry",
		Headers:     []driver.Header{{Key: delayUntilHeader, Value: []byte("forged")}},
	}, 10*time.Second, true, now)

	due, present, err := recordDelayUntil(record)
	if err != nil || !present {
		t.Fatalf("recordDelayUntil() = %s, %t, %v; want a due-time header", due, present, err)
	}
	if !due.Equal(now.Add(10 * time.Second)) {
		t.Fatalf("due = %s, want %s", due, now.Add(10*time.Second))
	}
	if len(record.Headers) != 1 || record.Headers[0].Key != delayUntilHeader {
		t.Fatalf("headers = %#v, want only the driver-owned due-time header", record.Headers)
	}
}

func TestEvaluateDeferralZeroDelayAndZeroDueDeliversImmediately(t *testing.T) {
	record := &kgo.Record{Topic: "main", Timestamp: time.Unix(100, 0)}

	decision := evaluateDeferral(record, 0, true, time.Unix(100, 0))
	if decision.err != nil || decision.wait {
		t.Fatalf("evaluateDeferral() = %#v, want immediate delivery", decision)
	}
}

func TestEvaluateDeferralOutOfBandSurfacesErrorAndDoesNotWait(t *testing.T) {
	published := time.Unix(100, 0)
	record := deferredTestRecord(published, published.Add(30*time.Second))

	decision := evaluateDeferral(record, 10*time.Second, true, published)
	if decision.wait {
		t.Fatalf("evaluateDeferral() wait = true, want delivery after surfacing error")
	}
	if decision.err == nil {
		t.Fatal("evaluateDeferral() error = nil, want out-of-band error")
	}
}

func TestEvaluateDeferralUnknownDestinationSurfacesErrorAndDoesNotWait(t *testing.T) {
	published := time.Unix(100, 0)
	record := deferredTestRecord(published, published.Add(10*time.Second))

	decision := evaluateDeferral(record, 10*time.Second, false, published)
	if decision.err == nil {
		t.Fatal("evaluateDeferral() error = nil, want unknown-delay error")
	}
	if decision.wait {
		t.Fatal("evaluateDeferral() wait = true, want delivery after surfacing error")
	}
}

func TestEvaluateDeferralMalformedHeaderSurfacesErrorAndDelivers(t *testing.T) {
	published := time.Unix(100, 0)
	record := deferredTestRecord(published, published.Add(10*time.Second))
	record.Headers[0].Value = []byte("not-a-time")

	decision := evaluateDeferral(record, 10*time.Second, true, published)
	if decision.err == nil || decision.wait {
		t.Fatalf("evaluateDeferral() = %#v, want malformed-header error without waiting", decision)
	}
}

func TestEvaluateDeferralDuplicateHeadersSurfacesErrorAndDelivers(t *testing.T) {
	published := time.Unix(100, 0)
	record := deferredTestRecord(published, published.Add(10*time.Second))
	record.Headers = append(record.Headers, record.Headers[0])

	decision := evaluateDeferral(record, 10*time.Second, true, published)
	if decision.err == nil || decision.wait {
		t.Fatalf("evaluateDeferral() = %#v, want duplicate-header error without waiting", decision)
	}
	if !strings.Contains(decision.err.Error(), "duplicate deferred due-time headers") {
		t.Fatalf("evaluateDeferral() error = %v, want duplicate-header error", decision.err)
	}
}

func deferredTestRecord(published, due time.Time) *kgo.Record {
	return &kgo.Record{
		Topic:     "retry",
		Timestamp: published,
		Headers:   []kgo.RecordHeader{{Key: delayUntilHeader, Value: []byte(strconv.FormatInt(due.UnixNano(), 10))}},
	}
}

func deferredTestTopicRecord(topic string, partition int32, offset int64, published, due time.Time) *kgo.Record {
	return &kgo.Record{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Timestamp: published,
		Headers:   []kgo.RecordHeader{{Key: delayUntilHeader, Value: []byte(strconv.FormatInt(due.UnixNano(), 10))}},
	}
}

func TestPendingDeadlineEmptyPendingReturnsFalse(t *testing.T) {
	c := &consumer{now: func() time.Time { return time.Unix(100, 0) }}
	if deadline, ok := c.pendingDeadline(nil); ok || !deadline.IsZero() {
		t.Fatalf("pendingDeadline(nil) = %s, %t; want zero time, false", deadline, ok)
	}
	if deadline, ok := c.pendingDeadline([]*kgo.Record{}); ok || !deadline.IsZero() {
		t.Fatalf("pendingDeadline([]) = %s, %t; want zero time, false", deadline, ok)
	}
}

func TestPendingDeadlineAllRecordsDueReturnsFalse(t *testing.T) {
	now := time.Unix(100, 0)
	c := &consumer{
		conn: &conn{delays: map[string]time.Duration{"retry": 10 * time.Second}},
		now:  func() time.Time { return now },
	}
	r1 := deferredTestTopicRecord("retry", 0, 1, now.Add(-10*time.Second), now)
	r2 := deferredTestTopicRecord("retry", 0, 2, now.Add(-15*time.Second), now.Add(-5*time.Second))

	if deadline, ok := c.pendingDeadline([]*kgo.Record{r1, r2}); ok || !deadline.IsZero() {
		t.Fatalf("pendingDeadline(all due) = %s, %t; want zero time, false", deadline, ok)
	}
}

func TestPendingDeadlineReturnsEarliestDueWhenNotFirstElement(t *testing.T) {
	now := time.Unix(100, 0)
	c := &consumer{
		conn: &conn{delays: map[string]time.Duration{
			"retry.mid":  20 * time.Second,
			"retry.fast": 10 * time.Second,
			"retry.slow": 30 * time.Second,
		}},
		now: func() time.Time { return now },
	}
	r1 := deferredTestTopicRecord("retry.mid", 0, 1, now, now.Add(20*time.Second))
	r2 := deferredTestTopicRecord("retry.fast", 0, 2, now, now.Add(10*time.Second))
	r3 := deferredTestTopicRecord("retry.slow", 0, 3, now, now.Add(30*time.Second))

	wantEarliest := now.Add(10 * time.Second)
	deadline, ok := c.pendingDeadline([]*kgo.Record{r1, r2, r3})
	if !ok || !deadline.Equal(wantEarliest) {
		t.Fatalf("pendingDeadline() = %s, %t; want earliest due %s, true", deadline, ok, wantEarliest)
	}
}

func TestPendingDeadlineIgnoresUnknownDestinationAlongsideDeferrable(t *testing.T) {
	now := time.Unix(100, 0)
	c := &consumer{
		conn: &conn{delays: map[string]time.Duration{
			"retry.known": 10 * time.Second,
		}},
		now: func() time.Time { return now },
	}
	rUnknown := deferredTestTopicRecord("unknown.topic", 0, 1, now, now.Add(10*time.Second))
	wantDue := now.Add(10 * time.Second)
	rKnown := deferredTestTopicRecord("retry.known", 0, 2, now, wantDue)

	deadline, ok := c.pendingDeadline([]*kgo.Record{rUnknown, rKnown})
	if !ok || !deadline.Equal(wantDue) {
		t.Fatalf("pendingDeadline(unknown + known) = %s, %t; want %s, true", deadline, ok, wantDue)
	}
}

func TestConsumerDeferralFaultDoesNotSendError(t *testing.T) {
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	c := &consumer{
		errors:            make(chan error, 10),
		budgets:           map[string]int{"retry": 1},
		pauseReasons:      make(map[string]pauseReasonSet),
		unsettled:         make(map[string]int),
		trackers:          make(map[partitionKey]*ackTracker),
		reportedDeferrals: make(map[string]struct{}),
		now:               func() time.Time { return time.Unix(100, 0) },
	}
	record := &kgo.Record{Topic: "retry", Partition: 0, Offset: 42}
	c.mu.Lock()
	admitted := c.admissionLocked(record)
	c.mu.Unlock()

	if !admitted {
		t.Fatal("admissionLocked() = false, want true")
	}

	select {
	case err := <-c.Errors():
		t.Fatalf("consumer error sent on deferral fault: %v", err)
	default:
	}
}

func TestConsumerDeferralFaultLogsOncePerDestination(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	c := &consumer{
		errors:            make(chan error, 10),
		budgets:           map[string]int{"retry": 10},
		pauseReasons:      make(map[string]pauseReasonSet),
		unsettled:         make(map[string]int),
		trackers:          make(map[partitionKey]*ackTracker),
		reportedDeferrals: make(map[string]struct{}),
		now:               func() time.Time { return time.Unix(100, 0) },
	}

	r1 := &kgo.Record{Topic: "retry", Partition: 0, Offset: 1}
	r2 := &kgo.Record{Topic: "retry", Partition: 0, Offset: 2}

	c.mu.Lock()
	c.admissionLocked(r1)
	c.admissionLocked(r2)
	c.mu.Unlock()

	output := strings.TrimSpace(logs.String())
	var lines []string
	if len(output) > 0 {
		lines = strings.Split(output, "\n")
	}
	if len(lines) != 1 {
		t.Fatalf("logged %d deferral fault lines, want 1", len(lines))
	}
	line := lines[0]
	if !strings.Contains(line, "destination=retry") {
		t.Errorf("log line missing destination=retry: %q", line)
	}
	if !strings.Contains(line, "partition=0") {
		t.Errorf("log line missing partition=0: %q", line)
	}
	if !strings.Contains(line, "offset=1") {
		t.Errorf("log line missing offset=1: %q", line)
	}
	if !strings.Contains(line, "destination delay is unknown") {
		t.Errorf("log line missing condition: %q", line)
	}
}
