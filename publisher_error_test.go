package f1

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	//nolint:depguard // publisher error-path tests use the deterministic in-memory driver.
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem"
)

func TestPublishRejectsOversizedHeadersBeforeDriverPublish(t *testing.T) {
	producer := &encodeRecordingProducer{}
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&encodeRecordingDriver{producer: producer}),
		WithObserver(&publishRecordingObserver{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	observer := client.observer.(*publishRecordingObserver)
	_, err = client.Publisher().Publish(context.Background(), "orders.created", "payload",
		WithIdempotencyKey(strings.Repeat("x", CoreMaxHeaderBytes)),
	)
	if err == nil {
		t.Fatal("Publish() error = nil, want header encoding error")
	}
	if !errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatalf("Publish() error = %v, want ErrEnvelopeTooLarge", err)
	}
	if got := producer.publishCount(); got != 0 {
		t.Fatalf("driver publish calls = %d, want 0", got)
	}

	_, finishes, _ := observer.snapshot()
	var built *FinishEvent
	for i := range finishes {
		if finishes[i].Kind == ObserverMessageBuilt {
			finish := finishes[i]
			built = &finish
			break
		}
	}
	if built == nil {
		t.Fatalf("observer finishes = %#v, want message_built finish", finishes)
	}
	if built.Outcome != ObserverOutcomeError {
		t.Fatalf("message_built outcome = %q, want error", built.Outcome)
	}
	if built.ErrorClass != ErrorClassOther {
		t.Fatalf("message_built error class = %q, want %q", built.ErrorClass, ErrorClassOther)
	}
}

type encodeRecordingDriver struct {
	producer *encodeRecordingProducer
}

func (*encodeRecordingDriver) Name() string { return inmem.New().Name() }

func (*encodeRecordingDriver) Capabilities() driver.Capabilities { return inmem.New().Capabilities() }

func (d *encodeRecordingDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	conn, err := inmem.New().Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &encodeRecordingConn{Conn: conn, producer: d.producer}, nil
}

type encodeRecordingConn struct {
	driver.Conn
	producer *encodeRecordingProducer
}

func (c *encodeRecordingConn) Producer(ctx context.Context, cfg driver.ProducerConfig) (driver.Producer, error) {
	producer, err := c.Conn.Producer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	c.producer.Producer = producer
	return c.producer, nil
}

type encodeRecordingProducer struct {
	driver.Producer
	mu    sync.Mutex
	calls int
}

func (p *encodeRecordingProducer) Publish(ctx context.Context, messages ...driver.OutboundMessage) error {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	return p.Producer.Publish(ctx, messages...)
}

func (p *encodeRecordingProducer) publishCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}
