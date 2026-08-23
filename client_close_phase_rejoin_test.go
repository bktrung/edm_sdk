package f1

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestClientCloseRejoinsTimedOutFlush(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	producer := &recordingProducer{
		flushStarted: make(chan struct{}),
		flushRelease: make(chan struct{}),
	}
	client := newPublishClient(t, producer, WithClock(fake))
	client.config.Lifecycle.FlushTimeout = 5 * time.Second
	if _, err := client.Publisher().Publish(context.Background(), "orders.created", "payload"); err != nil {
		t.Fatal(err)
	}

	var release sync.Once
	releaseFlush := func() { release.Do(func() { close(producer.flushRelease) }) }
	t.Cleanup(releaseFlush)

	firstDone := make(chan error, 1)
	go func() { firstDone <- client.Close(context.Background()) }()
	<-producer.flushStarted
	waitForFakeTimer(t, fake)
	fake.Advance(5 * time.Second)
	firstErr := <-firstDone
	if !errors.Is(firstErr, context.DeadlineExceeded) || !strings.Contains(firstErr.Error(), "flush phase") {
		t.Fatalf("first Close() error = %v, want flush phase deadline", firstErr)
	}

	secondDone := make(chan error, 1)
	go func() { secondDone <- client.Close(context.Background()) }()
	if waitForClosePhaseCalls(func() int {
		producer.mu.Lock()
		defer producer.mu.Unlock()
		return producer.flushCalls
	}, 2) {
		releaseFlush()
		<-secondDone
		t.Fatalf("Flush call count = 2, want the retry to rejoin the first call")
	}
	releaseFlush()
	if err := <-secondDone; err != nil {
		t.Fatalf("retried Close() error = %v", err)
	}
	producer.mu.Lock()
	flushCalls := producer.flushCalls
	producer.mu.Unlock()
	if flushCalls != 1 {
		t.Fatalf("Flush call count = %d, want 1", flushCalls)
	}
}

func TestConcurrentCloseReportsInProgress(t *testing.T) {
	producer := &recordingProducer{
		closeStarted: make(chan struct{}),
		closeRelease: make(chan struct{}),
	}
	conn := &publishConn{producer: producer, info: driver.BrokerInfo{Kind: "test", Version: "1"}}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&publishDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	var release sync.Once
	releaseProducer := func() { release.Do(func() { close(producer.closeRelease) }) }
	defer func() {
		releaseProducer()
		_ = client.Close(context.Background())
	}()
	client.producerHandle = producer

	firstDone := make(chan error, 1)
	go func() { firstDone <- client.Close(context.Background()) }()
	<-producer.closeStarted

	secondErr := client.Close(context.Background())
	if secondErr == nil {
		t.Fatal("concurrent Close returned nil while the first Close was still running")
	}
	client.mu.Lock()
	closed := client.closed
	client.mu.Unlock()
	if closed {
		t.Fatal("concurrent Close observed a closed client while the first Close was still running")
	}

	releaseProducer()
	if err := <-firstDone; err != nil {
		t.Fatalf("first Close() = %v", err)
	}
}

func TestClientCloseRejoinsTimedOutConnectionClose(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	conn := &firstCloseBlockingConn{
		Conn:    &publishConn{info: driver.BrokerInfo{Kind: "test", Version: "1"}},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&phaseTestDriver{conn: conn}), WithClock(fake))
	if err != nil {
		t.Fatal(err)
	}
	client.config.Lifecycle.CloseTimeout = 5 * time.Second
	var release sync.Once
	releaseConn := func() { release.Do(func() { close(conn.release) }) }
	t.Cleanup(releaseConn)

	firstDone := make(chan error, 1)
	go func() { firstDone <- client.Close(context.Background()) }()
	<-conn.started
	waitForFakeTimer(t, fake)
	fake.Advance(5 * time.Second)
	firstErr := <-firstDone
	if !errors.Is(firstErr, context.DeadlineExceeded) || !strings.Contains(firstErr.Error(), "close phase") {
		t.Fatalf("first Close() error = %v, want close phase deadline", firstErr)
	}

	secondDone := make(chan error, 1)
	go func() { secondDone <- client.Close(context.Background()) }()
	waitTimer := clock.NewReal().Timer(100 * time.Millisecond)
	defer waitTimer.Stop()
	select {
	case secondErr := <-secondDone:
		t.Errorf("second Close() returned %v while the first Conn.Close was still running", secondErr)
	case <-waitTimer.C:
	}
	releaseConn()
	retryTimer := clock.NewReal().Timer(time.Second)
	defer retryTimer.Stop()
	select {
	case secondErr := <-secondDone:
		if secondErr != nil {
			t.Fatalf("retried Close() error = %v", secondErr)
		}
	case <-retryTimer.C:
		t.Fatal("retried Close() did not rejoin the completed Conn.Close")
	}
	conn.mu.Lock()
	closeCalls := conn.closeCalls
	conn.mu.Unlock()
	if closeCalls != 1 {
		t.Fatalf("Conn.Close call count = %d, want 1", closeCalls)
	}
}

func TestHealthReturnsPromptlyWhileCloseFlushes(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	producer := &recordingProducer{
		flushStarted: make(chan struct{}),
		flushRelease: make(chan struct{}),
	}
	client := newPublishClient(t, producer, WithClock(fake))
	client.config.Lifecycle.FlushTimeout = 5 * time.Second
	if _, err := client.Publisher().Publish(context.Background(), "orders.created", "payload"); err != nil {
		t.Fatal(err)
	}
	var release sync.Once
	releaseFlush := func() { release.Do(func() { close(producer.flushRelease) }) }
	t.Cleanup(releaseFlush)

	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(context.Background()) }()
	<-producer.flushStarted
	healthDone := make(chan error, 1)
	go func() { healthDone <- client.Health(context.Background()) }()
	healthTimer := clock.NewReal().Timer(100 * time.Millisecond)
	defer healthTimer.Stop()
	select {
	case healthErr := <-healthDone:
		if healthErr == nil || !strings.Contains(healthErr.Error(), "client is closing") {
			t.Fatalf("Health() error = %v, want client-closing error", healthErr)
		}
	case <-healthTimer.C:
		releaseFlush()
		<-closeDone
		t.Fatal("Health() blocked while Close was flushing")
	}
	releaseFlush()
	if err := <-closeDone; err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func waitForClosePhaseCalls(read func() int, want int) bool {
	realClock := clock.NewReal()
	deadline := realClock.Timer(100 * time.Millisecond)
	defer deadline.Stop()
	for {
		if read() >= want {
			return true
		}
		tick := realClock.Timer(time.Millisecond)
		select {
		case <-deadline.C:
			tick.Stop()
			return false
		case <-tick.C:
		}
	}
}

type phaseTestDriver struct {
	conn driver.Conn
}

func (*phaseTestDriver) Name() string                      { return "test" }
func (*phaseTestDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (d *phaseTestDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return d.conn, nil
}

type firstCloseBlockingConn struct {
	driver.Conn
	mu         sync.Mutex
	closeCalls int
	started    chan struct{}
	release    chan struct{}
	once       sync.Once
}

func (c *firstCloseBlockingConn) Close(context.Context) error {
	c.mu.Lock()
	c.closeCalls++
	first := c.closeCalls == 1
	if first {
		c.once.Do(func() { close(c.started) })
	}
	c.mu.Unlock()
	if first {
		<-c.release
	}
	return nil
}
