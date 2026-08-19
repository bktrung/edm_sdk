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
	conn := &publishConn{producer: producer, info: driver.BrokerInfo{Kind: "test", Version: "1"}}
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

	conn.mu.Lock()
	closeCalls := conn.closeCalls
	conn.mu.Unlock()
	if closeCalls != 0 {
		t.Fatalf("conn.Close was called %d time(s) while producer.Close had not returned, want 0", closeCalls)
	}

	close(producer.closeRelease)

	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("retried Close() error = %v", err)
	}
	conn.mu.Lock()
	closeCalls = conn.closeCalls
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
