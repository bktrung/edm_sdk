package f1

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
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
		publishDone <- publishMessages(client, context.Background(), driver.OutboundMessage{Destination: "orders.created"})
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
		for range 2 {
			select {
			case <-results:
			case <-clock.NewReal().Timer(time.Second).C:
				return
			}
		}
		_ = client.Close(context.Background())
	})
	for range 2 {
		go func() {
			results <- publishMessages(client, context.Background(), driver.OutboundMessage{Destination: "orders.created"})
		}()
	}
	waitForSignal(t, built, "first producer build")
	waitForSignal(t, built, "second producer build")
	releaseBuild()
	for i := range 2 {
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

	if err := publishMessages(client, context.Background(), driver.OutboundMessage{Destination: "orders.created"}); err != nil {
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
			setClientLifecycle(client, lifecycle.Closed)
			return built, nil
		},
	}
	var err error
	client, err = New(context.Background(), testClientConfig(t), WithDriver(&producerAdmissionDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()

	err = publishMessages(client, context.Background(), driver.OutboundMessage{Destination: "orders.created"})
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
			client.current = currentConnection{conn: current, epoch: client.current.epoch + 1}
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

	err = publishMessages(client, context.Background(), driver.OutboundMessage{Destination: "orders.created"})
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

func TestPublishBatchCreatesProducerAndReturnsIDs(t *testing.T) {
	built := &admissionProducer{}
	conn := &producerAdmissionConn{
		publishConn: &publishConn{info: driver.BrokerInfo{Kind: "test", Version: "1"}},
		build:       func() (driver.Producer, error) { return built, nil },
	}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&producerAdmissionDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	result, err := client.Publisher().PublishBatch(context.Background(), []Message{
		{EventType: "orders.created", Payload: "one"},
		{EventType: "orders.created", Payload: "two"},
	})
	if err != nil {
		t.Fatalf("PublishBatch() = %v, want nil", err)
	}
	if len(result.Results) != 2 {
		t.Fatalf("result count = %d, want 2", len(result.Results))
	}
	for i, message := range result.Results {
		if message.ID == "" || message.Err != nil {
			t.Fatalf("result[%d] = %#v, want ID and nil error", i, message)
		}
	}
	if publishCalls, closeCalls := built.counts(); publishCalls != 1 || closeCalls != 0 {
		t.Fatalf("producer counts = publish %d, close %d, want 1, 0", publishCalls, closeCalls)
	}
	conn.mu.Lock()
	producerCalls := conn.producerCalls
	conn.mu.Unlock()
	if producerCalls != 1 {
		t.Fatalf("Conn.Producer calls = %d, want 1", producerCalls)
	}
}

func TestPublishBatchProducerBuildDoesNotHoldClientLock(t *testing.T) {
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
	batchDone := make(chan error, 1)
	var releaseOnce sync.Once
	releaseBuild := func() { releaseOnce.Do(func() { close(buildRelease) }) }
	t.Cleanup(func() {
		releaseBuild()
		select {
		case <-batchDone:
		case <-clock.NewReal().Timer(time.Second).C:
		}
		_ = client.Close(context.Background())
	})
	go func() {
		_, err := client.Publisher().PublishBatch(context.Background(), []Message{
			{EventType: "orders.created", Payload: "payload"},
		})
		batchDone <- err
	}()
	waitForSignal(t, buildStarted, "batch producer construction")

	healthDone := make(chan error, 1)
	go func() { healthDone <- client.Health(context.Background()) }()
	if err := assertReturns(t, healthDone, "Health"); err != nil {
		t.Fatalf("Health() = %v, want nil", err)
	}
	releaseBuild()
	if err := <-batchDone; err != nil {
		t.Fatalf("PublishBatch() = %v, want nil", err)
	}
	if publishCalls, closeCalls := built.counts(); publishCalls != 1 || closeCalls != 0 {
		t.Fatalf("producer counts = publish %d, close %d, want 1, 0", publishCalls, closeCalls)
	}
}

func TestPublishBatchRejectsStaleConnectionAfterProducerBuild(t *testing.T) {
	built := &admissionProducer{}
	buildStarted := make(chan struct{})
	buildRelease := make(chan struct{})
	var startOnce sync.Once
	var releaseOnce sync.Once
	current := &publishConn{info: driver.BrokerInfo{Kind: "test", Version: "1"}}
	conn := &producerAdmissionConn{
		publishConn: &publishConn{info: driver.BrokerInfo{Kind: "test", Version: "1"}},
		build: func() (driver.Producer, error) {
			startOnce.Do(func() { close(buildStarted) })
			<-buildRelease
			return built, nil
		},
	}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&producerAdmissionDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	t.Cleanup(func() { releaseOnce.Do(func() { close(buildRelease) }) })

	batchDone := make(chan struct {
		result BatchResult
		err    error
	}, 1)
	go func() {
		result, err := client.Publisher().PublishBatch(context.Background(), []Message{
			{EventType: "orders.created", Payload: "stale"},
		})
		batchDone <- struct {
			result BatchResult
			err    error
		}{result, err}
	}()
	waitForSignal(t, buildStarted, "batch producer construction")
	client.mu.Lock()
	client.current = currentConnection{conn: current, epoch: client.current.epoch + 1}
	client.mu.Unlock()
	releaseOnce.Do(func() { close(buildRelease) })
	response := <-batchDone
	if response.err == nil || !strings.Contains(response.err.Error(), "reconnecting") {
		t.Fatalf("PublishBatch() = %v, want reconnecting error", response.err)
	}
	if kind, ok := driver.Classify(response.err); !ok || kind != driver.KindTransient {
		t.Fatalf("stale connection error classification = %v, %t, want transient, true", kind, ok)
	}
	if len(response.result.Results) != 1 || response.result.Results[0].ID != "" || response.result.Results[0].Err != nil {
		t.Fatalf("stale result = %#v, want empty result entry", response.result.Results)
	}
	if publishCalls, closeCalls := built.counts(); publishCalls != 0 || closeCalls != 1 {
		t.Fatalf("stale producer counts = publish %d, close %d, want 0, 1", publishCalls, closeCalls)
	}
}

func TestConcurrentFirstBatchPublishesInstallOneProducerAndCloseLoser(t *testing.T) {
	built := make(chan struct{}, 2)
	release := make(chan struct{})
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
			<-release
			return producer, nil
		},
	}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&producerAdmissionDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan struct {
		result BatchResult
		err    error
	}, 2)
	var releaseOnce sync.Once
	releaseBuild := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		releaseBuild()
		for range 2 {
			select {
			case <-results:
			case <-clock.NewReal().Timer(time.Second).C:
				return
			}
		}
		_ = client.Close(context.Background())
	})
	for range 2 {
		go func() {
			result, err := client.Publisher().PublishBatch(context.Background(), []Message{
				{EventType: "orders.created", Payload: "concurrent"},
			})
			results <- struct {
				result BatchResult
				err    error
			}{result, err}
		}()
	}
	waitForSignal(t, built, "first batch producer build")
	waitForSignal(t, built, "second batch producer build")
	releaseBuild()
	for i := range 2 {
		response := <-results
		if response.err != nil {
			t.Fatalf("concurrent PublishBatch %d = %v, want nil", i, response.err)
		}
		if len(response.result.Results) != 1 || response.result.Results[0].ID == "" {
			t.Fatalf("concurrent result %d = %#v, want one ID", i, response.result.Results)
		}
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
}

func TestPublishBatchRejectsReconnectInFlight(t *testing.T) {
	fake := clock.NewFake(time.Unix(400, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 1)}
	client := newReconnectTestClient(t, d, recorded, 0)
	client.reconnectRandom = func() float64 { return 1 }
	client.mu.Lock()
	epoch := client.current.epoch
	client.mu.Unlock()
	if err := client.requestReconnect(errors.New("batch reconnect"), epoch); err != nil {
		t.Fatal(err)
	}
	waitReconnectCondition(t, func() bool { return recorded.sleepCount() == 1 })

	result, err := client.Publisher().PublishBatch(context.Background(), []Message{
		{EventType: "orders.created", Payload: "during-reconnect"},
	})
	if !errors.Is(err, errClientReconnecting) {
		t.Fatalf("PublishBatch() = %v, want errClientReconnecting", err)
	}
	if len(result.Results) != 1 || result.Results[0].ID != "" || result.Results[0].Err != nil {
		t.Fatalf("reconnecting result = %#v, want empty result entry", result.Results)
	}
	fake.BlockUntil(1)
	fake.Advance(500 * time.Millisecond)
	waitReconnectCondition(t, func() bool { return !client.isReconnecting() })
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health() after the reconnect = %v, want nil", err)
	}
}

func TestPublishBatchPreservesClosedAndReconnectErrors(t *testing.T) {
	producer := &admissionProducer{}
	conn := &producerAdmissionConn{
		publishConn: &publishConn{producer: producer, info: driver.BrokerInfo{Kind: "test", Version: "1"}},
	}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&producerAdmissionDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	setClientLifecycle(client, lifecycle.Aborted)
	result, err := client.Publisher().PublishBatch(context.Background(), []Message{
		{EventType: "orders.created", Payload: "closed"},
	})
	if err == nil || !strings.Contains(err.Error(), "client is closed") {
		t.Fatalf("closed PublishBatch() = %v, want client is closed", err)
	}
	if len(result.Results) != 1 || result.Results[0].ID != "" || result.Results[0].Err != nil {
		t.Fatalf("closed result = %#v, want empty result entry", result.Results)
	}

	setClientLifecycle(client, lifecycle.Ready)
	client.mu.Lock()
	reconnectErr := errors.New("stored reconnect failure")
	client.reconnectErr = reconnectErr
	client.mu.Unlock()
	result, err = client.Publisher().PublishBatch(context.Background(), []Message{
		{EventType: "orders.created", Payload: "reconnect-error"},
	})
	if !errors.Is(err, reconnectErr) {
		t.Fatalf("reconnect-error PublishBatch() = %v, want %v", err, reconnectErr)
	}
	if len(result.Results) != 1 || result.Results[0].ID != "" || result.Results[0].Err != nil {
		t.Fatalf("reconnect-error result = %#v, want empty result entry", result.Results)
	}
}
