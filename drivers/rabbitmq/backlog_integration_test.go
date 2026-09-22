//go:build integration

package rabbitmq

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

type countingRoundTripper struct {
	next  http.RoundTripper
	calls atomic.Int64
	err   error
}

func (r *countingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	r.calls.Add(1)
	if r.err != nil {
		return nil, r.err
	}
	return r.next.RoundTrip(request)
}

func openBacklogFixture(t *testing.T, kind queueKind, name string) (*conn, *amqp.Channel) {
	t.Helper()
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	channel, err := raw.Channel()
	if err != nil {
		_ = raw.Close()
		t.Fatalf("Channel: %v", err)
	}
	_, _ = channel.QueueDelete(name, false, false, false)
	if _, err := channel.QueueDeclare(name, true, false, false, false, amqp.Table{"x-queue-type": string(kind)}); err != nil {
		_ = channel.Close()
		_ = raw.Close()
		t.Fatalf("QueueDeclare(%q): %v", name, err)
	}

	public, err := (Driver{}).Open(ctx, driver.Config{
		Endpoints: []string{defaultEndpoint},
		DriverOptions: map[string]string{
			"rabbitmq.queueType": string(kind),
		},
	})
	if err != nil {
		_ = channel.Close()
		_ = raw.Close()
		t.Fatalf("Open: %v", err)
	}
	rabbitConn := public.(*conn)
	t.Cleanup(func() {
		_, _ = channel.QueueDelete(name, false, false, false)
		_ = channel.Close()
		_ = raw.Close()
		_ = public.Close(context.Background())
	})
	return rabbitConn, channel
}

func backlogTestConsumer(connection *conn, destination string) *consumer {
	return &consumer{
		conn:  connection,
		clock: clock.NewReal(),
		cfg:   driver.ConsumerConfig{Effective: driver.Capabilities{LagQueryable: true}},
		lanes: []*lane{{destination: destination}},
	}
}

func publishBacklogMessages(t *testing.T, ctx context.Context, channel *amqp.Channel, destination string, times ...time.Time) {
	t.Helper()
	if err := channel.Confirm(false); err != nil {
		t.Fatalf("Confirm: %v", err)
	}
	confirmations := channel.NotifyPublish(make(chan amqp.Confirmation, len(times)))
	for index, timestamp := range times {
		if err := channel.PublishWithContext(ctx, "", destination, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Timestamp:    timestamp,
			Body:         []byte{byte(index)},
		}); err != nil {
			t.Fatalf("PublishWithContext: %v", err)
		}
	}
	for range times {
		select {
		case confirmation := <-confirmations:
			if !confirmation.Ack {
				t.Fatal("published backlog message was nacked")
			}
		case <-ctx.Done():
			t.Fatalf("publish confirmation: %v", ctx.Err())
		}
	}
}

func TestRabbitMQBacklogClassicReportsHead(t *testing.T) {
	const destination = "rabbitmq-driver-backlog-classic"
	connection, channel := openBacklogFixture(t, queueKindClassic, destination)
	consumer := backlogTestConsumer(connection, destination)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	first := time.Date(2026, time.January, 2, 3, 4, 5, 678_000_000, time.UTC)
	second := first.Add(time.Second)
	publishBacklogMessages(t, ctx, channel, destination, first, second)

	var sample driver.BacklogSample
	ticker := time.NewTicker(100 * time.Millisecond) //nolint:forbidigo // bounded polling interval for broker statistics
	defer ticker.Stop()
	for {
		backlog, err := consumer.Backlog(ctx)
		if err != nil {
			t.Fatalf("Backlog: %v", err)
		}
		sample = backlog[destination]
		if sample.Lag == 2 && !sample.HeadEnqueuedAt.IsZero() {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("head did not appear: %v", ctx.Err())
		case <-ticker.C:
		}
	}

	lag, err := consumer.Lag(ctx)
	if err != nil {
		t.Fatalf("Lag: %v", err)
	}
	if sample.Lag != lag[destination] || sample.Lag != 2 {
		t.Fatalf("Backlog lag = %d, Lag = %d; want both 2", sample.Lag, lag[destination])
	}
	if !sample.HeadEnqueuedAt.Equal(time.Unix(first.Unix(), 0)) {
		t.Fatalf("head = %s, want %s", sample.HeadEnqueuedAt, time.Unix(first.Unix(), 0))
	}
	if sample.HeadSource != driver.EnqueueSourceProducer {
		t.Fatalf("head source = %q, want producer", sample.HeadSource)
	}
}

func TestRabbitMQBacklogClassicEmptyIsUnknown(t *testing.T) {
	const destination = "rabbitmq-driver-backlog-classic-empty"
	connection, _ := openBacklogFixture(t, queueKindClassic, destination)
	consumer := backlogTestConsumer(connection, destination)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	backlog, err := consumer.Backlog(ctx)
	if err != nil {
		t.Fatalf("Backlog: %v", err)
	}
	sample := backlog[destination]
	if sample.Lag != 0 {
		t.Fatalf("empty lag = %d, want 0", sample.Lag)
	}
	if !sample.HeadEnqueuedAt.IsZero() || sample.HeadSource != driver.EnqueueSourceUnknown {
		t.Fatalf("empty head = %s, source %q; want zero and unknown", sample.HeadEnqueuedAt, sample.HeadSource)
	}
}

func TestRabbitMQBacklogQuorumDoesNotReadManagement(t *testing.T) {
	const destination = "rabbitmq-driver-backlog-quorum"
	connection, channel := openBacklogFixture(t, queueKindQuorum, destination)
	consumer := backlogTestConsumer(connection, destination)
	transport := &countingRoundTripper{next: http.DefaultTransport}
	connection.management.client.Transport = transport
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	publishBacklogMessages(t, ctx, channel, destination, time.Unix(1_724_123_456, 0), time.Unix(1_724_123_457, 0))
	backlog, err := consumer.Backlog(ctx)
	if err != nil {
		t.Fatalf("Backlog: %v", err)
	}
	sample := backlog[destination]
	if sample.Lag != 2 {
		t.Fatalf("quorum lag = %d, want 2", sample.Lag)
	}
	if !sample.HeadEnqueuedAt.IsZero() || sample.HeadSource != driver.EnqueueSourceUnknown {
		t.Fatalf("quorum head = %s, source %q; want zero and unknown", sample.HeadEnqueuedAt, sample.HeadSource)
	}
	if calls := transport.calls.Load(); calls != 0 {
		t.Fatalf("quorum management requests = %d, want 0", calls)
	}
}

func TestRabbitMQBacklogManagementErrorKeepsCount(t *testing.T) {
	const destination = "rabbitmq-driver-backlog-management-error"
	connection, channel := openBacklogFixture(t, queueKindClassic, destination)
	consumer := backlogTestConsumer(connection, destination)
	connection.management.client.Transport = &countingRoundTripper{
		next: http.DefaultTransport,
		err:  errors.New("injected management failure"),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	publishBacklogMessages(t, ctx, channel, destination, time.Unix(1_724_123_456, 0))
	backlog, err := consumer.Backlog(ctx)
	if err != nil {
		t.Fatalf("Backlog: %v", err)
	}
	sample := backlog[destination]
	if sample.Lag != 1 {
		t.Fatalf("management error lag = %d, want 1", sample.Lag)
	}
	if !sample.HeadEnqueuedAt.IsZero() || sample.HeadSource != driver.EnqueueSourceUnknown {
		t.Fatalf("management error head = %s, source %q; want zero and unknown", sample.HeadEnqueuedAt, sample.HeadSource)
	}
}
