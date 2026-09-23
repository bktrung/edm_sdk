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
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/wire"
)

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
	if got := producer.closeCalls; got != 1 {
		t.Fatalf("producer close calls after Client.Close = %d, want 1", got)
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
	if got := producer.closeCalls; got != 1 {
		t.Fatalf("producer close calls = %d, want 1", got)
	}
}

func TestPublishRejectedWhenClientIsClosing(t *testing.T) {
	t.Parallel()
	producer := &recordingProducer{}
	client := newPublishClient(t, producer)
	setClientLifecycle(client, lifecycle.Aborted)

	_, err := client.Publisher().Publish(context.Background(), "orders.created", "payload")
	if err == nil || !strings.Contains(err.Error(), "client is closed") {
		t.Fatalf("Publish() error = %v, want shutdown admission error", err)
	}
	if got := len(producer.messages); got != 0 {
		t.Fatalf("producer received %d messages while client was closing", got)
	}
	setClientLifecycle(client, lifecycle.Ready)
}

func TestPublishAttemptDuringCloseIsRefused(t *testing.T) {
	t.Parallel()
	producer := &recordingProducer{
		closeStarted: make(chan struct{}),
		closeRelease: make(chan struct{}),
	}
	client := newPublishClient(t, producer)
	var release sync.Once
	releaseClose := func() { release.Do(func() { close(producer.closeRelease) }) }
	t.Cleanup(releaseClose)
	if _, err := client.Publisher().Publish(context.Background(), "orders.created", "first"); err != nil {
		t.Fatal(err)
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(context.Background()) }()
	<-producer.closeStarted
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
		t.Fatal("Publish did not refuse admission while Close was closing the producer")
	}
	releaseClose()
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
	if got, want := failedIndexes(result), []int{0, 1}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("failed indexes = %v, want %v", got, want)
	}
	quiescePublishClient(t, client)
	if got := strings.Count(logs.String(), "f1 unclassified publish error"); got != 2 {
		t.Fatalf("unclassified warning count = %d, want 2; logs=%s", got, logs.String())
	}
}

func TestPublishReportsAFailedMessageWithoutACauseAsFailed(t *testing.T) {
	t.Parallel()
	producer := &recordingProducer{publishErr: &driver.PublishError{Failed: map[int]error{0: nil}}}
	client := newPublishClient(t, producer)
	id, err := client.Publisher().Publish(context.Background(), "orders.created", "one")
	if err == nil {
		t.Fatalf("Publish() = %q, nil; want an error for a message the driver reported as failed", id)
	}
	quiescePublishClient(t, client)
}

func TestPublishStampsTheProducerWithServiceEnvironmentAndInstance(t *testing.T) {
	t.Parallel()
	producer := &recordingProducer{}
	client := newPublishClient(t, producer)
	if _, err := client.Publisher().Publish(context.Background(), "orders.created", "one"); err != nil {
		t.Fatal(err)
	}
	quiescePublishClient(t, client)
	producer.mu.Lock()
	defer producer.mu.Unlock()
	want := "orders/test/" + client.config.InstanceID
	if got := headerValue(producer.messages[0].Headers, wire.Producer); got != want {
		t.Fatalf("producer header = %q, want %q", got, want)
	}
}

func TestPublishBatchReportsAnOutOfRangeFailureIndexAsAFailedBatch(t *testing.T) {
	t.Parallel()
	producer := &recordingProducer{publishErr: &driver.PublishError{Failed: map[int]error{5: errors.New("lost")}}}
	client := newPublishClient(t, producer)
	result, err := client.Publisher().PublishBatch(context.Background(), []Message{
		{EventType: "orders.created", Payload: "one"},
		{EventType: "orders.created", Payload: "two"},
	})
	if err == nil {
		t.Fatalf("PublishBatch() error = nil, want an error for a failure report naming no message in the batch; results=%+v", result.Results)
	}
	for i, r := range result.Results {
		if r.Err == nil || r.ID != "" {
			t.Fatalf("Results[%d] = %+v, want a failed message", i, r)
		}
	}
	quiescePublishClient(t, client)
}

func TestPublishBatchUnclassifiedFailuresDoNotReconnect(t *testing.T) {
	t.Parallel()
	var logs logSink
	producer := &recordingProducer{publishErr: &driver.PublishError{Failed: map[int]error{
		0: errors.New("untranslated one"),
		1: errors.New("untranslated two"),
	}}}
	client := newPublishClient(t, producer, WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	if _, err := client.Publisher().PublishBatch(context.Background(), []Message{
		{EventType: "orders.created", Payload: "one"},
		{EventType: "orders.created", Payload: "two"},
	}); err != nil {
		t.Fatalf("PublishBatch() error = %v, want nil for partial failure", err)
	}
	quiescePublishClient(t, client)
	if reconnectRequested(client, logs.String()) {
		t.Fatalf("a batch whose causes are all untranslated reconnected the client; logs=%s", logs.String())
	}
}

func TestPublishBatchTransientCauseStillReconnects(t *testing.T) {
	t.Parallel()
	var logs logSink
	producer := &recordingProducer{publishErr: &driver.PublishError{Failed: map[int]error{
		0: &driver.Error{Driver: "test", Op: "publish", K: driver.KindTransient, Err: errors.New("broker reset the connection")},
		1: errors.New("untranslated"),
	}}}
	client := newPublishClient(t, producer, WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	if _, err := client.Publisher().PublishBatch(context.Background(), []Message{
		{EventType: "orders.created", Payload: "one"},
		{EventType: "orders.created", Payload: "two"},
	}); err != nil {
		t.Fatalf("PublishBatch() error = %v, want nil for partial failure", err)
	}
	quiescePublishClient(t, client)
	if !reconnectRequested(client, logs.String()) {
		t.Fatalf("a batch carrying a cause its driver classified transient did not reconnect the client; logs=%s", logs.String())
	}
}

func TestSinglePublishTransientFailureRequestsReconnect(t *testing.T) {
	t.Parallel()
	var logs logSink
	cause := &driver.Error{Driver: "test", Op: "publish", K: driver.KindTransient, Err: errors.New("broker reset the connection")}
	producer := &recordingProducer{publishErr: cause}
	client := newPublishClient(t, producer, WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	if _, err := client.Publisher().Publish(context.Background(), "orders.created", "payload"); !errors.Is(err, cause) {
		t.Fatalf("Publish() error = %v, want it to wrap %v", err, cause)
	}
	quiescePublishClient(t, client)
	if !reconnectRequested(client, logs.String()) {
		t.Fatalf("a single publish its driver classified transient did not reconnect the client; logs=%s", logs.String())
	}
}

func TestSinglePublishFatalFailureDoesNotRequestReconnect(t *testing.T) {
	t.Parallel()
	var logs logSink
	cause := &driver.Error{Driver: "test", Op: "publish", K: driver.KindFatal, Err: errors.New("broker rejected the publish")}
	producer := &recordingProducer{publishErr: cause}
	client := newPublishClient(t, producer, WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	if _, err := client.Publisher().Publish(context.Background(), "orders.created", "payload"); !errors.Is(err, cause) {
		t.Fatalf("Publish() error = %v, want it to wrap %v", err, cause)
	}
	quiescePublishClient(t, client)
	if reconnectRequested(client, logs.String()) {
		t.Fatalf("a single publish its driver classified fatal reconnected the client; logs=%s", logs.String())
	}
}

func TestProducerCreationTransientFailureRequestsReconnect(t *testing.T) {
	t.Parallel()
	var logs logSink
	cause := &driver.Error{Driver: "test", Op: "producer", K: driver.KindTransient, Err: errors.New("broker closed the channel")}
	conn := &publishConn{
		producer:    &recordingProducer{},
		producerErr: cause,
		info:        driver.BrokerInfo{Kind: "test", Version: "1"},
	}
	client := newPublishClientWithConn(t, conn, WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	if _, err := client.Publisher().Publish(context.Background(), "orders.created", "payload"); !errors.Is(err, cause) {
		t.Fatalf("Publish() error = %v, want it to wrap %v", err, cause)
	}
	quiescePublishClient(t, client)
	if !reconnectRequested(client, logs.String()) {
		t.Fatalf("a producer creation its driver classified transient did not reconnect the client; logs=%s", logs.String())
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

func TestWarnUnclassifiedWithContextAcceptsNilLogger(t *testing.T) {
	t.Parallel()
	warnUnclassifiedWithContext(nil, context.Background(), errors.New("unclassified"))
}

func TestInvokeHandlerMessageSkipsTopicLookupForPlainDelivery(t *testing.T) {
	client := newPublishClient(t, &recordingProducer{})
	runner := &Runner{
		client: client,
		subscription: Subscription{
			Name:           "orders",
			HandlerTimeout: time.Second,
		},
	}
	event := &Event{envelope: Envelope{Type: "orders.created", Priority: PriorityMedium}}
	message := driver.InboundMessage{Destination: "missing.destination"}
	done := make(chan handlerResult, 1)

	client.mu.Lock()
	go func() {
		done <- invokeHandlerMessage(runner, context.Background(), HandlerFunc(func(context.Context, *Event) error {
			return nil
		}), event, message)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	select {
	case result := <-done:
		client.mu.Unlock()
		if result.stuck {
			t.Fatal("plain delivery became stuck")
		}
	case <-ctx.Done():
		client.mu.Unlock()
		<-done
		t.Fatal("plain delivery waited for the client lock during topic lookup")
	}
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
	return newPublishClientWithConn(t, conn, opts...)
}

// newPublishClientWithConn builds a client over conn, for a test that needs a
// connection shape the producer-only helper cannot express.
func newPublishClientWithConn(t *testing.T, conn *publishConn, opts ...Option) *Client {
	t.Helper()
	options := []Option{WithDriver(&publishDriver{conn: conn})}
	options = append(options, opts...)
	client, err := New(context.Background(), testClientConfig(t), options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client
}

// quiescePublishClient closes client and waits for its reconnect supervisor to
// exit, so a test can read a logger this client writes to without racing it.
// Close alone is not enough: it cancels the supervisor but does not join it,
// and the supervisor logs through this client's logger when it starts a
// reconnect.
func quiescePublishClient(t *testing.T, client *Client) {
	t.Helper()
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
	<-client.supervisorDone
}

// reconnectRequested reports whether client asked its reconnect supervisor to
// rebuild the connection. Read it only after quiescePublishClient: a request
// the supervisor never took is still in its one-slot queue, and one it took has
// already been logged through the client's logger, so the two checks together
// cover every order the supervisor can run in.
func reconnectRequested(client *Client, logs string) bool {
	select {
	case <-client.reconnectRequests:
		return true
	default:
	}
	return strings.Contains(logs, "f1 reconnect started")
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
	producerErr      error
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
	producer := c.producer
	err := c.producerErr
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return producer, nil
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
	closeErr         error
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

// failedIndexes returns the indexes of the batch messages that did not publish.
func failedIndexes(result BatchResult) []int {
	failed := make([]int, 0)
	for i, message := range result.Results {
		if message.Err != nil {
			failed = append(failed, i)
		}
	}
	return failed
}
