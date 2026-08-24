package f1

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

type producerAdmissionDriver struct {
	conn driver.Conn
}

func (*producerAdmissionDriver) Name() string { return "producer-admission-test" }

func (*producerAdmissionDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }

func (d *producerAdmissionDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return d.conn, nil
}

type producerAdmissionConn struct {
	*publishConn
	build func() (driver.Producer, error)
}

func (c *producerAdmissionConn) Producer(ctx context.Context, cfg driver.ProducerConfig) (driver.Producer, error) {
	c.mu.Lock()
	c.producerCalls++
	c.mu.Unlock()
	if c.build != nil {
		return c.build()
	}
	return c.producer, nil
}

type admissionProducer struct {
	mu           sync.Mutex
	publishCalls int
	closeCalls   int
}

func (p *admissionProducer) Publish(context.Context, ...driver.OutboundMessage) error {
	p.mu.Lock()
	p.publishCalls++
	p.mu.Unlock()
	return nil
}

func (*admissionProducer) Flush(context.Context) error { return nil }

func (p *admissionProducer) Close(context.Context) error {
	p.mu.Lock()
	p.closeCalls++
	p.mu.Unlock()
	return nil
}

func (p *admissionProducer) counts() (publish, close int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.publishCalls, p.closeCalls
}

func waitForSignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatalf("timed out waiting for %s", what)
	}
}

func assertReturns(t *testing.T, done <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatalf("%s did not return while the producer build was blocked", what)
		return nil
	}
}

func TestPublishProducerBuildDoesNotHoldClientLock(t *testing.T) {
	buildStarted := make(chan struct{})
	buildRelease := make(chan struct{})
	built := &admissionProducer{}
	var started sync.Once
	conn := &producerAdmissionConn{
		publishConn: &publishConn{info: driver.BrokerInfo{Kind: "test", Version: "1"}},
		build: func() (driver.Producer, error) {
			started.Do(func() { close(buildStarted) })
			<-buildRelease
			return built, nil
		},
	}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&producerAdmissionDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	publishDone := make(chan error, 1)
	var releaseOnce sync.Once
	releaseBuild := func() { releaseOnce.Do(func() { close(buildRelease) }) }
	t.Cleanup(func() {
		releaseBuild()
		select {
		case <-publishDone:
		case <-clock.NewReal().Timer(time.Second).C:
		}
		_ = client.Close(context.Background())
	})
	go func() {
		publishDone <- publishMessages(client, context.Background(), true, driver.OutboundMessage{Destination: "orders.created"})
	}()
	waitForSignal(t, buildStarted, "producer construction")

	healthDone := make(chan error, 1)
	go func() { healthDone <- client.Health(context.Background()) }()
	if err := assertReturns(t, healthDone, "Health"); err != nil {
		t.Fatalf("Health() = %v, want nil", err)
	}

	limitsDone := make(chan struct{})
	go func() {
		_ = client.Limits()
		close(limitsDone)
	}()
	select {
	case <-limitsDone:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("Limits did not return while the producer build was blocked")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(context.Background()) }()
	if err := assertReturns(t, closeDone, "Close"); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}

	releaseBuild()
	if err := <-publishDone; err == nil {
		t.Fatal("publish succeeded after Close won during producer construction")
	}
	if publishCalls, closeCalls := built.counts(); publishCalls != 0 || closeCalls != 1 {
		t.Fatalf("built producer counts = publish %d, close %d, want 0, 1", publishCalls, closeCalls)
	}
}

func TestConcurrentFirstPublishesInstallOneProducerAndCloseLoser(t *testing.T) {
	built := make(chan struct{}, 2)
	release := make(chan struct{})
	var calls atomic.Int32
	producers := make([]*admissionProducer, 0, 2)
	var producersMu sync.Mutex
	conn := &producerAdmissionConn{
		publishConn: &publishConn{info: driver.BrokerInfo{Kind: "test", Version: "1"}},
		build: func() (driver.Producer, error) {
			producer := &admissionProducer{}
			producersMu.Lock()
			producers = append(producers, producer)
			producersMu.Unlock()
			built <- struct{}{}
			if calls.Add(1) <= 2 {
				<-release
			}
			return producer, nil
		},
	}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&producerAdmissionDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	var releaseOnce sync.Once
	releaseBuild := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		releaseBuild()
		for i := 0; i < 2; i++ {
			select {
			case <-results:
			case <-clock.NewReal().Timer(time.Second).C:
				return
			}
		}
		_ = client.Close(context.Background())
	})
	for i := 0; i < 2; i++ {
		go func() {
			results <- publishMessages(client, context.Background(), true, driver.OutboundMessage{Destination: "orders.created"})
		}()
	}
	waitForSignal(t, built, "first producer build")
	waitForSignal(t, built, "second producer build")
	releaseBuild()
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatalf("concurrent publish %d = %v, want nil", i, err)
		}
	}

	conn.mu.Lock()
	producerCalls := conn.producerCalls
	conn.mu.Unlock()
	if producerCalls != 2 {
		t.Fatalf("Conn.Producer calls = %d, want 2", producerCalls)
	}
	client.mu.Lock()
	winner, ok := client.producerHandle.(*admissionProducer)
	client.mu.Unlock()
	if !ok || winner == nil {
		t.Fatalf("installed producer = %T, want *admissionProducer", client.producerHandle)
	}
	if len(producers) != 2 {
		t.Fatalf("built producers = %d, want 2", len(producers))
	}
	for _, producer := range producers {
		publishCalls, closeCalls := producer.counts()
		if producer == winner {
			if publishCalls != 2 || closeCalls != 0 {
				t.Fatalf("winner counts = publish %d, close %d, want 2, 0", publishCalls, closeCalls)
			}
			continue
		}
		if publishCalls != 0 || closeCalls != 1 {
			t.Fatalf("loser counts = publish %d, close %d, want 0, 1", publishCalls, closeCalls)
		}
	}

	if err := publishMessages(client, context.Background(), true, driver.OutboundMessage{Destination: "orders.created"}); err != nil {
		t.Fatalf("later publish = %v, want nil", err)
	}
	conn.mu.Lock()
	producerCalls = conn.producerCalls
	conn.mu.Unlock()
	if producerCalls != 2 {
		t.Fatalf("Conn.Producer calls after reuse = %d, want 2", producerCalls)
	}
	if publishCalls, closeCalls := winner.counts(); publishCalls != 3 || closeCalls != 0 {
		t.Fatalf("reused winner counts = publish %d, close %d, want 3, 0", publishCalls, closeCalls)
	}
}

func TestPublishRejectsProducerWhenCloseWinsDuringBuild(t *testing.T) {
	built := &admissionProducer{}
	var client *Client
	conn := &producerAdmissionConn{
		publishConn: &publishConn{info: driver.BrokerInfo{Kind: "test", Version: "1"}},
		build: func() (driver.Producer, error) {
			client.closed = true
			return built, nil
		},
	}
	var err error
	client, err = New(context.Background(), testClientConfig(t), WithDriver(&producerAdmissionDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()

	err = publishMessages(client, context.Background(), true, driver.OutboundMessage{Destination: "orders.created"})
	if err == nil || err.Error() != "f1: client is closed" {
		t.Fatalf("publish after close wins = %v, want client is closed", err)
	}
	if publishCalls, closeCalls := built.counts(); publishCalls != 0 || closeCalls != 1 {
		t.Fatalf("stale producer counts = publish %d, close %d, want 0, 1", publishCalls, closeCalls)
	}
}

func TestPublishRejectsProducerFromRetiredConnection(t *testing.T) {
	built := &admissionProducer{}
	retired := &publishConn{info: driver.BrokerInfo{Kind: "test", Version: "1"}}
	current := &publishConn{info: driver.BrokerInfo{Kind: "test", Version: "1"}}
	var client *Client
	conn := &producerAdmissionConn{
		publishConn: retired,
		build: func() (driver.Producer, error) {
			client.mu.Lock()
			client.conn = current
			client.mu.Unlock()
			return built, nil
		},
	}
	var err error
	client, err = New(context.Background(), testClientConfig(t), WithDriver(&producerAdmissionDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()

	err = publishMessages(client, context.Background(), true, driver.OutboundMessage{Destination: "orders.created"})
	if err == nil || !strings.Contains(err.Error(), "reconnecting") {
		t.Fatalf("publish with retired connection = %v, want reconnecting error", err)
	}
	if kind, ok := driver.Classify(err); !ok || kind != driver.KindTransient {
		t.Fatalf("retired connection error classification = %v, %t, want transient, true", kind, ok)
	}
	if publishCalls, closeCalls := built.counts(); publishCalls != 0 || closeCalls != 1 {
		t.Fatalf("retired producer counts = publish %d, close %d, want 0, 1", publishCalls, closeCalls)
	}
}
