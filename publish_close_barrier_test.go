package f1

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// blockingCodec behaves like codec.JSON, except Encode signals encodeStarted
// then blocks on encodeRelease. It exists to hold a publish open inside the
// caller-supplied encode step, the exact window a publish is admitted but
// not yet counted as in flight, before it reaches the driver.
type blockingCodec struct {
	encodeStarted     chan struct{}
	encodeStartedOnce chan struct{}
	encodeRelease     chan struct{}
}

func (blockingCodec) Name() string        { return "json" }
func (blockingCodec) ContentType() string { return "application/json" }

func (c blockingCodec) Encode(v any) ([]byte, error) {
	select {
	case <-c.encodeStartedOnce:
	default:
		close(c.encodeStartedOnce)
		close(c.encodeStarted)
	}
	<-c.encodeRelease
	return json.Marshal(v)
}

func (blockingCodec) Decode(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// TestCloseWaitsForPublishStillEncoding proves Close cannot race a publish
// that was admitted before Close began but is still running the
// caller-supplied codec step: the publish must be counted as in flight from
// admission, not from the moment it reaches the driver, or Close's idle wait
// captures a stale count and proceeds to close the connection concurrently
// with a publish that has not finished.
func TestCloseWaitsForPublishStillEncoding(t *testing.T) {
	t.Parallel()
	producer := &recordingProducer{}
	conn := &publishConn{producer: producer, info: driver.BrokerInfo{Kind: "test", Version: "1"}}
	codecFixture := blockingCodec{
		encodeStarted:     make(chan struct{}),
		encodeStartedOnce: make(chan struct{}),
		encodeRelease:     make(chan struct{}),
	}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&publishDriver{conn: conn}), WithCodec(codecFixture))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	publishDone := make(chan error, 1)
	go func() {
		_, publishErr := client.Publisher().Publish(context.Background(), "orders.created", "payload")
		publishDone <- publishErr
	}()
	<-codecFixture.encodeStarted

	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(context.Background()) }()

	timer := clock.NewReal().Timer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned while an admitted publish was still encoding: %v", err)
	case <-timer.C:
	}

	conn.mu.Lock()
	closeCalls := conn.closeCalls
	conn.mu.Unlock()
	if closeCalls != 0 {
		t.Fatalf("conn.Close was called %d time(s) while an admitted publish was still encoding, want 0", closeCalls)
	}

	close(codecFixture.encodeRelease)
	if err := <-publishDone; err != nil {
		t.Fatalf("Publish() = %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close() = %v", err)
	}
	producer.mu.Lock()
	messages := len(producer.messages)
	producer.mu.Unlock()
	if messages != 1 {
		t.Fatalf("producer received %d messages, want the admitted publish to have completed", messages)
	}
}
