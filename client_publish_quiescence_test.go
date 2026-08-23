package f1

import (
	"context"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// flightCountingProducer counts publishes between admission and completion,
// and records how many were still in flight when Close ran.
type flightCountingProducer struct {
	mu              sync.Mutex
	inFlight        int
	inFlightAtClose int
	closeCalls      int
	publishStarted  chan struct{}
	publishRelease  chan struct{}
}

func (p *flightCountingProducer) Publish(context.Context, ...driver.OutboundMessage) error {
	p.mu.Lock()
	p.inFlight++
	started := p.publishStarted
	p.mu.Unlock()
	if started != nil {
		close(started)
	}
	<-p.publishRelease
	p.mu.Lock()
	p.inFlight--
	p.mu.Unlock()
	return nil
}

func (*flightCountingProducer) Flush(context.Context) error { return nil }

func (p *flightCountingProducer) Close(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closeCalls++
	p.inFlightAtClose = p.inFlight
	return nil
}

func (p *flightCountingProducer) snapshot() (inFlight, inFlightAtClose, closeCalls int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.inFlight, p.inFlightAtClose, p.closeCalls
}

// parkedRunner registers a started runner whose drain never finishes until
// the test closes done, holding Close inside the runner-drain phase at a
// deterministic point after admission has closed but before the publish-idle
// wait runs.
func parkedRunner(t *testing.T, client *Client) *Runner {
	t.Helper()
	runner := &Runner{
		client:       client,
		subscription: Subscription{Name: "orders"},
		inflight:     newInflightRegistry(),
		started:      true,
		done:         make(chan struct{}),
	}
	client.mu.Lock()
	client.runners[runner] = struct{}{}
	client.mu.Unlock()
	t.Cleanup(func() {
		select {
		case <-runner.done:
		default:
			close(runner.done)
		}
	})
	return runner
}

func waitClientClosing(t *testing.T, client *Client) {
	t.Helper()
	timer := clock.NewReal().Timer(2 * time.Second)
	defer timer.Stop()
	for {
		client.mu.Lock()
		closing := client.closing
		client.mu.Unlock()
		if closing {
			return
		}
		select {
		case <-timer.C:
			t.Fatal("Close did not reach beginClose")
		default:
			_ = clock.NewReal().Sleep(context.Background(), time.Millisecond)
		}
	}
}

func assertNotDone(t *testing.T, done <-chan error, what string) {
	t.Helper()
	timer := clock.NewReal().Timer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case err := <-done:
		t.Fatalf("%s returned while it must stay pending: %v", what, err)
	case <-timer.C:
	}
}

// TestCloseDoesNotCloseProducerWhileSuccessorPublishInFlight pins the
// producer-teardown invariant: a successor publish admitted while shutdown
// is running (a worker finishing a delivery during drain) must be waited out
// before producer.Close, because the driver contract forbids tearing down a
// producer that still owns work. The idle answer Close waits on must be read
// at wait time; reading it once when nothing was publishing yet makes the
// wait a no-op for every later generation.
func TestCloseDoesNotCloseProducerWhileSuccessorPublishInFlight(t *testing.T) {
	producer := &flightCountingProducer{publishStarted: make(chan struct{}), publishRelease: make(chan struct{})}
	conn := &publishConn{producer: producer, info: driver.BrokerInfo{Kind: "test", Version: "1"}}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&publishDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	client.producerHandle = producer
	runner := parkedRunner(t, client)

	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(context.Background()) }()
	waitClientClosing(t, client)

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- publishMessages(client, context.Background(), true, driver.OutboundMessage{Destination: "orders.created", Body: []byte("{}")})
	}()
	timer := clock.NewReal().Timer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-producer.publishStarted:
	case <-timer.C:
		t.Fatal("successor publish was not admitted during shutdown")
	}

	assertNotDone(t, closeDone, "Close")

	// Release the runner drain so Close reaches the publish-idle wait while
	// the successor publish is still in flight.
	close(runner.done)
	assertNotDone(t, closeDone, "Close")

	close(producer.publishRelease)
	if err := <-publishDone; err != nil {
		t.Fatalf("successor publish = %v, want nil", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
	if _, inFlightAtClose, _ := producer.snapshot(); inFlightAtClose != 0 {
		t.Fatalf("producer.Close ran with %d publish(es) in flight, want 0: the idle answer must be re-read at wait time, not captured before the drain", inFlightAtClose)
	}
}

// TestWaitForPublishesSpansLateGenerationsAndBarsTeardownAdmission pins two
// properties of the publish-idle phase: it may not return while any publish
// generation is live, including one that began after the wait started, and
// it must bar further successor admissions atomically with observing zero,
// so no publish can slip between the answer and producer teardown.
func TestWaitForPublishesSpansLateGenerationsAndBarsTeardownAdmission(t *testing.T) {
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&publishDriver{conn: &publishConn{info: driver.BrokerInfo{Kind: "test", Version: "1"}}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	client.mu.Lock()
	beginPublish(client)
	idleA := client.publishIdle
	client.mu.Unlock()

	waitDone := make(chan error, 1)
	go func() { waitDone <- client.waitForPublishes(context.Background()) }()

	// Let the waiter reach its first observation of generation A before the
	// swap below. The delay only strengthens mutation detection; the
	// assertions stay correct whichever order the scheduler picks, because
	// a re-reading wait stays pending on generation B in every interleaving.
	_ = clock.NewReal().Sleep(context.Background(), 5*time.Millisecond)

	// End generation A and begin generation B inside one critical section,
	// exactly as back-to-back end and begin do under contention. A waiter
	// that trusts one captured channel wakes here and reports idle while
	// generation B is still live.
	client.mu.Lock()
	close(idleA)
	client.publishIdle = make(chan struct{})
	client.mu.Unlock()

	assertNotDone(t, waitDone, "waitForPublishes")

	endPublish(client)
	if err := <-waitDone; err != nil {
		t.Fatalf("waitForPublishes() = %v, want nil", err)
	}
	client.mu.Lock()
	barricaded := client.producerTeardown
	client.mu.Unlock()
	if !barricaded {
		t.Fatal("producer teardown barrier was not set when the wait observed zero")
	}
	if err := publishMessages(client, context.Background(), true, driver.OutboundMessage{Destination: "orders.created"}); err == nil {
		t.Fatal("successor publish was admitted after producer teardown began")
	}
}

// TestWaitPublishIdleSpansLateGenerations pins the same property for the
// reconnect path: waiting for publish quiescence before retiring the old
// producer may not report idle while a newer publish generation is live,
// even one that began after the wait started.
func TestWaitPublishIdleSpansLateGenerations(t *testing.T) {
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&publishDriver{conn: &publishConn{info: driver.BrokerInfo{Kind: "test", Version: "1"}}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	client.mu.Lock()
	beginPublish(client)
	idleA := client.publishIdle
	client.mu.Unlock()

	waitDone := make(chan error, 1)
	go func() { waitDone <- client.waitPublishIdle(context.Background()) }()

	// Same settle delay as the Close-path generation test: it only
	// strengthens mutation detection, never the correctness of the assertion.
	_ = clock.NewReal().Sleep(context.Background(), 5*time.Millisecond)

	client.mu.Lock()
	close(idleA)
	client.publishIdle = make(chan struct{})
	client.mu.Unlock()

	assertNotDone(t, waitDone, "waitPublishIdle")

	endPublish(client)
	if err := <-waitDone; err != nil {
		t.Fatalf("waitPublishIdle() = %v, want nil", err)
	}
}
