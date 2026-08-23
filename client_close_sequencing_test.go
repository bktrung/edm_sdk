package f1

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// TestClientCloseDefersConnCloseUntilProducerCloseReturns proves that
// Client.Close never calls Conn.Close while the shared producer's Close
// call may still be running against that same connection. The producer's
// Close call is held open past its close timeout; Close must report the
// timeout without ever touching the connection. Once the producer's Close
// call is allowed to finish, a retried Close call must observe that and
// only then close the connection, exactly once.
func TestClientCloseDefersConnCloseUntilProducerCloseReturns(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	producer := &recordingProducer{
		closeStarted: make(chan struct{}),
		closeRelease: make(chan struct{}),
	}
	conn := &publishConn{producer: producer, info: driver.BrokerInfo{Kind: "test", Version: "1"}, closeStarted: make(chan struct{})}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&publishDriver{conn: conn}), WithClock(fake))
	if err != nil {
		t.Fatal(err)
	}
	client.config.Lifecycle.CloseTimeout = 5 * time.Second
	client.producerHandle = producer

	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(context.Background()) }()
	<-producer.closeStarted
	waitForFakeTimer(t, fake)
	fake.Advance(5 * time.Second)

	firstErr := <-closeDone
	if !errors.Is(firstErr, context.DeadlineExceeded) || !strings.Contains(firstErr.Error(), "close phase") {
		t.Fatalf("first Close() error = %v, want close phase deadline", firstErr)
	}

	waitTimer := clock.NewReal().Timer(100 * time.Millisecond)
	defer waitTimer.Stop()
	select {
	case <-conn.closeStarted:
		t.Fatal("Conn.Close started while Producer.Close had not returned")
	case <-waitTimer.C:
	}

	close(producer.closeRelease)

	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("retried Close() error = %v", err)
	}
	conn.mu.Lock()
	closeCalls := conn.closeCalls
	conn.mu.Unlock()
	if closeCalls != 1 {
		t.Fatalf("conn.Close call count = %d, want 1 once producer.Close had genuinely returned", closeCalls)
	}
	producer.mu.Lock()
	producerCloseCalls := producer.closeCalls
	producer.mu.Unlock()
	if producerCloseCalls != 1 {
		t.Fatalf("producer.Close call count = %d, want 1: a retried Close must rejoin the pending call, not start a second one", producerCloseCalls)
	}
}

func TestPublishRejectedAfterProducerCloseTimeout(t *testing.T) {
	client, producer, _, release, firstErr := beginTimedOutProducerClose(t)
	if !errors.Is(firstErr, context.DeadlineExceeded) || !strings.Contains(firstErr.Error(), "close phase") {
		t.Fatalf("first Close() error = %v, want close phase deadline", firstErr)
	}

	client.mu.Lock()
	publishProducer := client.producerHandle
	pendingClose := client.producerCloseWait
	client.mu.Unlock()
	if publishProducer != producer {
		t.Fatalf("producer available to publish = %p, want the producer still being closed = %p", publishProducer, producer)
	}
	if pendingClose == nil {
		t.Fatal("producerCloseWait = nil, want the unresolved producer Close call")
	}

	_, err := client.Publisher().Publish(context.Background(), "orders.created", "payload")
	if err == nil || !strings.Contains(err.Error(), "client is closed") {
		t.Fatalf("Publish() after unresolved producer Close = %v, want client is closed", err)
	}
	producer.mu.Lock()
	messageCount := len(producer.messages)
	producer.mu.Unlock()
	if messageCount != 0 {
		t.Fatalf("producer received %d messages while its Close was still in flight, want 0", messageCount)
	}

	release()
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("retried Close() error = %v", err)
	}
}

func TestCloseRetryRejoinsAfterProducerCloseTimeout(t *testing.T) {
	client, producer, conn, release, firstErr := beginTimedOutProducerClose(t)
	if !errors.Is(firstErr, context.DeadlineExceeded) || !strings.Contains(firstErr.Error(), "close phase") {
		t.Fatalf("first Close() error = %v, want close phase deadline", firstErr)
	}

	release()
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("retried Close() error = %v", err)
	}
	producer.mu.Lock()
	producerCloseCalls := producer.closeCalls
	producer.mu.Unlock()
	conn.mu.Lock()
	connCloseCalls := conn.closeCalls
	conn.mu.Unlock()
	if producerCloseCalls != 1 {
		t.Fatalf("producer.Close call count = %d, want 1: retry must rejoin the pending call", producerCloseCalls)
	}
	if connCloseCalls != 1 {
		t.Fatalf("conn.Close call count = %d, want 1 after producer.Close returned", connCloseCalls)
	}
}

func beginTimedOutProducerClose(t *testing.T) (*Client, *recordingProducer, *publishConn, func(), error) {
	t.Helper()
	fake := clock.NewFake(time.Unix(0, 0))
	producer := &recordingProducer{
		closeStarted: make(chan struct{}),
		closeRelease: make(chan struct{}),
	}
	conn := &publishConn{producer: producer, info: driver.BrokerInfo{Kind: "test", Version: "1"}}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&publishDriver{conn: conn}), WithClock(fake))
	if err != nil {
		t.Fatal(err)
	}
	client.config.Lifecycle.CloseTimeout = 5 * time.Second
	client.producerHandle = producer
	released := false
	release := func() {
		if !released {
			close(producer.closeRelease)
			released = true
		}
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(context.Background()) }()
	<-producer.closeStarted
	waitForFakeTimer(t, fake)
	fake.Advance(5 * time.Second)

	t.Cleanup(func() {
		release()
		_ = client.Close(context.Background())
	})
	return client, producer, conn, release, <-closeDone
}
