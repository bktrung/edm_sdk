package f1

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestClientCloseBoundsProducerFlush(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	producer := &recordingProducer{
		flushStarted: make(chan struct{}),
		flushRelease: make(chan struct{}),
	}
	client := newPublishClient(t, producer, WithClock(fake))
	client.config.Lifecycle.FlushTimeout = 5 * time.Second
	defer close(producer.flushRelease)
	if _, err := client.Publisher().Publish(context.Background(), "orders.created", "payload"); err != nil {
		t.Fatal(err)
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(context.Background()) }()
	<-producer.flushStarted
	waitForFakeTimer(t, fake)
	fake.Advance(5 * time.Second)
	err := <-closeDone
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "flush phase") {
		t.Fatalf("Close() error = %v, want flush phase deadline", err)
	}
}

func TestClientCloseBoundsConnectionClose(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	conn := &publishConn{
		info:         driver.BrokerInfo{Kind: "test", Version: "1"},
		closeStarted: make(chan struct{}),
		closeRelease: make(chan struct{}),
	}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&publishDriver{conn: conn}), WithClock(fake))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	client.config.Lifecycle.CloseTimeout = 5 * time.Second
	defer close(conn.closeRelease)

	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(context.Background()) }()
	<-conn.closeStarted
	waitForFakeTimer(t, fake)
	fake.Advance(5 * time.Second)
	err = <-closeDone
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "close phase") {
		t.Fatalf("Close() error = %v, want close phase deadline", err)
	}
}

func waitForFakeTimer(t *testing.T, fake *clock.Fake) {
	t.Helper()
	watchdog := clock.NewReal().Timer(time.Second)
	defer watchdog.Stop()
	for fake.NumWaiters() == 0 {
		select {
		case <-watchdog.C:
			t.Fatal("shutdown timeout timer was not registered")
		default:
			runtime.Gosched()
		}
	}
}

func TestPublishIsSynchronousUntilDurable(t *testing.T) {
	t.Parallel()
	producer := &recordingProducer{publishStarted: make(chan struct{}), publishRelease: make(chan struct{})}
	client := newPublishClient(t, producer)
	started := producer.publishStarted
	done := make(chan struct{})
	var id string
	var err error
	go func() {
		id, err = client.Publisher().Publish(context.Background(), "orders.created.v2", map[string]string{"id": "42"})
		close(done)
	}()
	<-started
	timer := client.options.clock.Timer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-done:
		t.Fatal("Publish returned before durable acknowledgement")
	case <-timer.C:
	}
	close(producer.publishRelease)
	waitTimer := client.options.clock.Timer(time.Second)
	defer waitTimer.Stop()
	select {
	case <-done:
	case <-waitTimer.C:
		t.Fatal("Publish did not return after durable acknowledgement")
	}
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("Publish returned an empty event ID")
	}
	if got := producer.closeCalls; got != 0 {
		t.Fatalf("producer close calls = %d, want 0 before Client.Close", got)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := producer.closeCalls; got != 1 {
		t.Fatalf("producer close calls = %d, want 1 after Client.Close", got)
	}
}

func TestPublishReusesProducerUntilClientClose(t *testing.T) {
	t.Parallel()
	producer := &recordingProducer{}
	conn := &publishConn{producer: producer, info: driver.BrokerInfo{Kind: "test", Version: "1"}}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&publishDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	if _, err := client.Publisher().Publish(context.Background(), "orders.created", "one"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Publisher().Publish(context.Background(), "orders.created", "two"); err != nil {
		t.Fatal(err)
	}
	if got := conn.producerCalls; got != 1 {
		t.Fatalf("producer creations = %d, want 1", got)
	}
	if got := producer.closeCalls; got != 0 {
		t.Fatalf("producer close calls before Client.Close = %d, want 0", got)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := producer.flushCalls; got != 1 {
		t.Fatalf("producer flush calls = %d, want 1", got)
	}
}

func TestCloseWaitsForInFlightPublish(t *testing.T) {
	t.Parallel()
	producer := &recordingProducer{publishStarted: make(chan struct{}), publishRelease: make(chan struct{})}
	client := newPublishClient(t, producer)
	publishDone := make(chan error, 1)
	go func() {
		_, err := client.Publisher().Publish(context.Background(), "orders.created", "payload")
		publishDone <- err
	}()
	<-producer.publishStarted

	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(context.Background()) }()
	timer := client.options.clock.Timer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before in-flight Publish completed: %v", err)
	case <-timer.C:
	}
	close(producer.publishRelease)
	if err := <-publishDone; err != nil {
		t.Fatalf("Publish() = %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if got := producer.flushCalls; got != 1 {
		t.Fatalf("producer flush calls = %d, want 1", got)
	}
	if got := producer.closeCalls; got != 1 {
		t.Fatalf("producer close calls = %d, want 1", got)
	}
}

func TestPublishRejectedWhenClientIsClosing(t *testing.T) {
	t.Parallel()
	producer := &recordingProducer{}
	client := newPublishClient(t, producer)
	client.mu.Lock()
	client.shutdownStarted = true
	client.mu.Unlock()

	_, err := client.Publisher().Publish(context.Background(), "orders.created", "payload")
	if err == nil || !strings.Contains(err.Error(), "client is closed") {
		t.Fatalf("Publish() error = %v, want shutdown admission error", err)
	}
	if got := len(producer.messages); got != 0 {
		t.Fatalf("producer received %d messages while client was closing", got)
	}
	client.mu.Lock()
	client.shutdownStarted = false
	client.mu.Unlock()
}

func TestPublishAttemptDuringCloseIsRefused(t *testing.T) {
	t.Parallel()
	producer := &recordingProducer{
		flushStarted: make(chan struct{}),
		flushRelease: make(chan struct{}),
	}
	client := newPublishClient(t, producer)
	var release sync.Once
	releaseFlush := func() { release.Do(func() { close(producer.flushRelease) }) }
	t.Cleanup(releaseFlush)
	if _, err := client.Publisher().Publish(context.Background(), "orders.created", "first"); err != nil {
		t.Fatal(err)
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(context.Background()) }()
	<-producer.flushStarted
	publishDone := make(chan error, 1)
	publishReturned := make(chan struct{})
	go func() {
		_, err := client.Publisher().Publish(context.Background(), "orders.created", "during-close")
		close(publishReturned)
		publishDone <- err
	}()
	waitTimer := client.options.clock.Timer(100 * time.Millisecond)
	defer waitTimer.Stop()
	select {
	case <-publishReturned:
	case <-waitTimer.C:
		t.Fatal("Publish did not refuse admission while Close was flushing")
	}
	releaseFlush()
	if err := <-closeDone; err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if err := <-publishDone; err == nil || !strings.Contains(err.Error(), "client is closed") {
		t.Fatalf("Publish() error = %v, want shutdown admission error", err)
	}
	if got := len(producer.messages); got != 1 {
		t.Fatalf("producer received %d messages, want only the pre-close publish", got)
	}
}

func TestCloseFailureDoesNotShadowSuccessfulPublish(t *testing.T) {
	t.Parallel()
	closeErr := errors.New("producer close failed")
	producer := &recordingProducer{closeErr: closeErr}
	conn := &publishConn{producer: producer, info: driver.BrokerInfo{Kind: "test", Version: "1"}}
	var logs logSink
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&publishDriver{conn: conn}),
		WithLogger(slog.New(slog.NewTextHandler(&logs, nil))),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	id, err := client.Publisher().Publish(context.Background(), "orders.created", "payload")
	if err != nil || id == "" {
		t.Fatalf("Publish() = %q, %v; want a durable ID and nil error", id, err)
	}
	if err := client.Close(context.Background()); !errors.Is(err, closeErr) {
		t.Fatalf("Close() error = %v, want %v", err, closeErr)
	}
	if !strings.Contains(logs.String(), "f1 producer close failed") {
		t.Fatalf("close log = %q, want producer close warning", logs.String())
	}
	if got := conn.closeCalls; got != 1 {
		t.Fatalf("connection close calls = %d, want 1", got)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := conn.closeCalls; got != 1 {
		t.Fatalf("connection close calls after retry = %d, want 1", got)
	}
}

func TestPublishTopicDerivationAndEnvelope(t *testing.T) {
	t.Parallel()
	producer := &recordingProducer{}
	client := newPublishClient(t, producer)
	id, err := client.Publisher().Publish(context.Background(), "com.za.order.v2.created", map[string]string{"id": "42"}, WithKey("ORD-42"), WithSubject("order-42"), WithCausedBy(&Event{envelope: Envelope{ID: "cause-1"}}))
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("Publish returned an empty event ID")
	}
	if got, want := producer.messages[0].Destination, "f1.test.com.za.order.v2.created.medium"; got != want {
		t.Fatalf("destination = %q, want %q", got, want)
	}
	if got, want := string(producer.messages[0].Key), "ORD-42"; got != want {
		t.Fatalf("partition key = %q, want %q", got, want)
	}
	header := messageHeaders(producer.messages[0])
	if got := header["f1partitionkey"]; got != "ORD-42" {
		t.Fatalf("f1partitionkey = %q, want ORD-42", got)
	}
	if got := header["f1causationid"]; got != "cause-1" {
		t.Fatalf("f1causationid = %q, want cause-1", got)
	}
	if got := header["f1correlationid"]; got != "cause-1" {
		t.Fatalf("f1correlationid = %q, want cause-1", got)
	}

	producer.messages = nil
	if _, err := client.Publisher().Publish(context.Background(), "orders.created.v2", nil); err != nil {
		t.Fatal(err)
	}
	if got, want := producer.messages[0].Destination, "f1.test.orders.created.medium"; got != want {
		t.Fatalf("trailing version destination = %q, want %q", got, want)
	}

	producer.messages = nil
	if _, err := client.Publisher().Publish(context.Background(), "orders.created.v2", nil, WithTopic("explicit.topic")); err != nil {
		t.Fatal(err)
	}
	if got, want := producer.messages[0].Destination, "f1.test.explicit.topic.medium"; got != want {
		t.Fatalf("overridden destination = %q, want %q", got, want)
	}
}

func TestPublishMaxAttemptsOptionValidationAndEncoding(t *testing.T) {
	t.Parallel()

	for _, attempts := range []int{0, -1} {
		t.Run(fmt.Sprintf("rejects %d", attempts), func(t *testing.T) {
			producer := &recordingProducer{}
			client := newPublishClient(t, producer)

			_, err := client.Publisher().Publish(context.Background(), "orders.created", "payload", WithMaxAttempts(attempts))
			if err == nil {
				t.Fatalf("Publish() with max attempts %d returned nil error", attempts)
			}
			if got := len(producer.messages); got != 0 {
				t.Fatalf("producer received %d messages for invalid max attempts %d, want 0", got, attempts)
			}
		})
	}

	producer := &recordingProducer{}
	client := newPublishClient(t, producer)
	if _, err := client.Publisher().Publish(context.Background(), "orders.created", "payload", WithMaxAttempts(4)); err != nil {
		t.Fatal(err)
	}
	if got := messageHeaders(producer.messages[0])["f1maxattempts"]; got != "4" {
		t.Fatalf("published max attempts = %q, want 4", got)
	}
}

func TestPublishDefaultKeyIsNotStampedAsPartitionKey(t *testing.T) {
	t.Parallel()
	producer := &recordingProducer{}
	client := newPublishClient(t, producer)
	_, err := client.Publisher().Publish(context.Background(), "orders.created", "payload", WithSubject("order-42"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(producer.messages[0].Key), "order-42"; got != want {
		t.Fatalf("outbound key = %q, want %q", got, want)
	}
	if _, ok := messageHeaders(producer.messages[0])["f1partitionkey"]; ok {
		t.Fatal("defaulted key was stamped as f1partitionkey")
	}

	producer.messages = nil
	id, err := client.Publisher().Publish(context.Background(), "orders.created", "payload")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(producer.messages[0].Key), id; got != want {
		t.Fatalf("ID fallback key = %q, want %q", got, want)
	}
	if _, ok := messageHeaders(producer.messages[0])["f1partitionkey"]; ok {
		t.Fatal("ID fallback was stamped as f1partitionkey")
	}
}

func TestPublishBatchReportsPartialFailuresAndWarnsPerMessage(t *testing.T) {
	t.Parallel()
	var logs logSink
	producer := &recordingProducer{publishErr: &driver.PublishError{Failed: map[int]error{
		0: errors.New("untranslated one"),
		1: errors.New("untranslated two"),
	}}}
	client := newPublishClient(t, producer, WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	result, err := client.Publisher().PublishBatch(context.Background(), []Message{
		{EventType: "orders.created", Payload: "one"},
		{EventType: "orders.created", Payload: "two"},
	})
	if err != nil {
		t.Fatalf("PublishBatch() error = %v, want nil for partial failure", err)
	}
	if got, want := result.Failed(), []int{0, 1}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("failed indexes = %v, want %v", got, want)
	}
	if got := strings.Count(logs.String(), "f1 unclassified publish error"); got != 2 {
		t.Fatalf("unclassified warning count = %d, want 2; logs=%s", got, logs.String())
	}
}

func TestNewLogsNonNativeCapabilities(t *testing.T) {
	t.Parallel()
	var logs logSink
	producer := &recordingProducer{}
	client := newPublishClient(t, producer, WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	if client == nil {
		t.Fatal("new client is nil")
	}
	output := logs.String()
	if !strings.Contains(output, "f1 capability emulated") || !strings.Contains(output, "f1 capability unavailable") {
		t.Fatalf("capability report = %q, want emulated and unavailable entries", output)
	}
}

func TestWarnUnclassifiedAcceptsNilLogger(t *testing.T) {
	t.Parallel()
	warnUnclassified(nil, errors.New("unclassified"))
}

func TestTopicForOnlyStripsTrailingVersion(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		eventType string
		want      string
	}{
		{eventType: "orders.created.v2", want: "orders.created"},
		{eventType: "orders.created", want: "orders.created"},
		{eventType: "com.za.order.v2.created", want: "com.za.order.v2.created"},
	} {
		t.Run(test.eventType, func(t *testing.T) {
			if got := topicFor(test.eventType); got != test.want {
				t.Fatalf("topicFor(%q) = %q, want %q", test.eventType, got, test.want)
			}
		})
	}
}

func newPublishClient(t *testing.T, producer *recordingProducer, opts ...Option) *Client {
	t.Helper()
	conn := &publishConn{producer: producer, info: driver.BrokerInfo{Kind: "test", Version: "1"}}
	options := []Option{WithDriver(&publishDriver{conn: conn})}
	options = append(options, opts...)
	client, err := New(context.Background(), testClientConfig(t), options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client
}

func messageHeaders(message driver.OutboundMessage) map[string]string {
	result := make(map[string]string, len(message.Headers))
	for _, header := range message.Headers {
		result[header.Key] = string(header.Value)
	}
	return result
}

type publishDriver struct {
	conn *publishConn
}

func (*publishDriver) Name() string { return "test" }

func (d *publishDriver) Capabilities() driver.Capabilities { return d.conn.caps }

func (d *publishDriver) Open(context.Context, driver.Config) (driver.Conn, error) { return d.conn, nil }

type publishConn struct {
	mu               sync.Mutex
	producer         driver.Producer
	caps             driver.Capabilities
	info             driver.BrokerInfo
	producerCalls    int
	closeCalls       int
	closeStarted     chan struct{}
	closeStartedOnce sync.Once
	closeRelease     chan struct{}
}

func (c *publishConn) Capabilities() driver.Capabilities { return c.caps }

func (c *publishConn) BrokerInfo() driver.BrokerInfo { return c.info }

func (c *publishConn) Producer(context.Context, driver.ProducerConfig) (driver.Producer, error) {
	c.mu.Lock()
	c.producerCalls++
	c.mu.Unlock()
	return c.producer, nil
}

func (*publishConn) Consumer(context.Context, driver.ConsumerConfig) (driver.Consumer, error) {
	return nil, driver.ErrUnsupported
}

func (*publishConn) Admin() driver.Admin { return nil }

func (*publishConn) Ping(context.Context) error { return nil }

func (c *publishConn) Close(context.Context) error {
	c.mu.Lock()
	c.closeCalls++
	started := c.closeStarted
	release := c.closeRelease
	if started != nil {
		c.closeStartedOnce.Do(func() { close(started) })
	}
	c.mu.Unlock()
	if release != nil {
		<-release
	}
	return nil
}

type recordingProducer struct {
	mu               sync.Mutex
	messages         []driver.OutboundMessage
	publishErr       error
	publishStarted   chan struct{}
	publishRelease   chan struct{}
	closeCalls       int
	flushCalls       int
	closeErr         error
	flushStarted     chan struct{}
	flushStartedOnce sync.Once
	flushRelease     chan struct{}
	closeStarted     chan struct{}
	closeStartedOnce sync.Once
	closeRelease     chan struct{}
}

func (p *recordingProducer) Publish(_ context.Context, messages ...driver.OutboundMessage) error {
	p.mu.Lock()
	p.messages = append([]driver.OutboundMessage(nil), messages...)
	if p.publishStarted != nil {
		close(p.publishStarted)
	}
	release := p.publishRelease
	err := p.publishErr
	p.mu.Unlock()
	if release != nil {
		<-release
	}
	return err
}

func (p *recordingProducer) Flush(context.Context) error {
	p.mu.Lock()
	p.flushCalls++
	started := p.flushStarted
	release := p.flushRelease
	if started != nil {
		p.flushStartedOnce.Do(func() { close(started) })
	}
	p.mu.Unlock()
	if release != nil {
		<-release
	}
	return nil
}

func (p *recordingProducer) Close(context.Context) error {
	p.mu.Lock()
	p.closeCalls++
	started := p.closeStarted
	release := p.closeRelease
	err := p.closeErr
	if started != nil {
		p.closeStartedOnce.Do(func() { close(started) })
	}
	p.mu.Unlock()
	if release != nil {
		<-release
	}
	return err
}
