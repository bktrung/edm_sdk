package kafka

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	//nolint:depguard // this test must exercise the public f1 API against Kafka.
	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
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
	c := &consumer{clock: clock.NewFake(time.Unix(100, 0))}
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
		conn:  &conn{delays: map[string]time.Duration{"retry": 10 * time.Second}},
		clock: clock.NewFake(now),
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
		clock: clock.NewFake(now),
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
		clock: clock.NewFake(now),
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
	c := &consumer{
		conn:              &conn{logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
		errors:            make(chan error, 10),
		budgets:           map[string]int{"retry": 1},
		pauseReasons:      make(map[string]pauseReasonSet),
		unsettled:         make(map[string]int),
		trackers:          make(map[partitionKey]*ackTracker),
		reportedDeferrals: make(map[string]struct{}),
		clock:             clock.NewFake(time.Unix(100, 0)),
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
	c := &consumer{
		conn:              &conn{logger: slog.New(slog.NewTextHandler(&logs, nil))},
		errors:            make(chan error, 10),
		budgets:           map[string]int{"retry": 10},
		pauseReasons:      make(map[string]pauseReasonSet),
		unsettled:         make(map[string]int),
		trackers:          make(map[partitionKey]*ackTracker),
		reportedDeferrals: make(map[string]struct{}),
		clock:             clock.NewFake(time.Unix(100, 0)),
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

// deferralLogSink is a concurrency-safe capture buffer. The driver warns from
// its fetch goroutine while the test reads the capture, so a bare bytes.Buffer
// would race under -race.
type deferralLogSink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *deferralLogSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *deferralLogSink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// swapProcessDefault points slog.Default at sink for the rest of the test and
// restores the previous default afterwards.
func swapProcessDefault(t *testing.T, sink *deferralLogSink) {
	t.Helper()
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(sink, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
}

// deferralLoggerDriver captures the port configuration a public client hands to
// the real Kafka driver, so a test can reach the connection it opened.
type deferralLoggerDriver struct {
	Driver
	opened driver.Config
	conn   *conn
}

func (d *deferralLoggerDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	opened, err := d.Driver.Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	d.opened = cfg
	d.conn, _ = opened.(*conn)
	return opened, nil
}

// openDeferralPublicClient builds an f1 client over the real Kafka driver and
// returns the connection it opened plus the port configuration it received. A
// nil logger omits WithLogger.
func openDeferralPublicClient(t *testing.T, ctx context.Context, logger *slog.Logger) (*conn, driver.Config) {
	t.Helper()
	driverUnderTest := &deferralLoggerDriver{}
	options := []f1.Option{
		f1.WithDriver(driverUnderTest),
		f1.WithTopology(f1.TopologyNone),
	}
	if logger != nil {
		options = append(options, f1.WithLogger(logger))
	}
	client, err := f1.New(ctx, kafkaPublicTestConfig(), options...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close(ctx) })
	if driverUnderTest.conn == nil {
		t.Fatal("client did not open a Kafka connection")
	}
	return driverUnderTest.conn, driverUnderTest.opened
}

// assertDeferralFaultWarns publishes one record to a destination the connection
// knows no delay for, consumes it, and requires the deferral fault warning with
// all four identifying attributes in sink. The connection declares no topology,
// which is what leaves the destination delay unknown.
func assertDeferralFaultWarns(t *testing.T, connection *conn, sink *deferralLogSink) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	_, _, admin := openKafkaAdminTest(t)

	topic := kafkaTestTopic(t, "deferral-logger")
	group := kafkaTestTopic(t, "deferral-logger-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 1)

	producer, err := connection.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true, Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	defer func() { _ = producer.Close(ctx) }()
	publishKafkaMessage(t, producer, ctx, topic, "deferral")

	consumerValue, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{topic}, Prefetch: 1, Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer closeKafkaConsumer(consumerValue)
	message := receiveKafkaMessage(t, consumerValue)
	if err := message.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	output := sink.String()
	for _, want := range []string{
		"kafka deferred delivery fault",
		"destination=" + topic,
		"destination delay is unknown",
		"partition=0",
		"offset=0",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("logger output = %q, want %q", output, want)
		}
	}
}

func TestDeferralFaultUsesConfiguredClientLogger(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	var configured deferralLogSink
	connection, portConfig := openDeferralPublicClient(t, ctx, slog.New(slog.NewTextHandler(&configured, nil)))
	if portConfig.Logger == nil {
		t.Fatal("driver.Config.Logger = nil, want the client's logger")
	}
	assertDeferralFaultWarns(t, connection, &configured)
}

func TestDeferralFaultWithoutConfiguredLoggerUsesProcessDefault(t *testing.T) {
	t.Run("client without WithLogger", func(t *testing.T) {
		requireBroker(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		t.Cleanup(cancel)

		var processDefault deferralLogSink
		swapProcessDefault(t, &processDefault)
		connection, portConfig := openDeferralPublicClient(t, ctx, nil)
		if portConfig.Logger != nil {
			t.Fatal("driver.Config.Logger = non-nil, want nil without WithLogger")
		}
		assertDeferralFaultWarns(t, connection, &processDefault)
	})

	t.Run("zero driver config", func(t *testing.T) {
		requireBroker(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		t.Cleanup(cancel)

		var processDefault deferralLogSink
		swapProcessDefault(t, &processDefault)
		opened, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{kafkaEndpoint}})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		connection, ok := opened.(*conn)
		if !ok {
			t.Fatalf("Open() returned %T, want *conn", opened)
		}
		t.Cleanup(func() { _ = connection.Close(context.Background()) })
		assertDeferralFaultWarns(t, connection, &processDefault)
	})
}
