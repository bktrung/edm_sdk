package f1

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestNewValidatesOptionsEagerly(t *testing.T) {
	t.Parallel()
	cfg := testClientConfig(t)
	if _, err := New(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "WithDriver") {
		t.Fatalf("New() error = %v, want missing driver", err)
	}
	if _, err := New(context.Background(), cfg, WithDriver((*testDriver)(nil))); err == nil || !strings.Contains(err.Error(), "non-nil") {
		t.Fatalf("New() error = %v, want nil driver", err)
	}
	fakeDriver := &testDriver{}
	if _, err := New(context.Background(), cfg, WithDriver(fakeDriver), WithLogger(nil)); err == nil || fakeDriver.opened {
		t.Fatalf("New() error = %v, opened = %v; want option error before open", err, fakeDriver.opened)
	}
}

func TestNewRejectsNilConnection(t *testing.T) {
	client, err := New(context.Background(), testClientConfig(t), WithDriver(nilConnectionDriver{}))
	if client != nil {
		t.Fatal("New() returned a client for a nil connection")
	}
	if err == nil || err.Error() != "f1: open nil-connection driver: driver returned a nil connection" {
		t.Fatalf("New() error = %v, want nil-connection startup error", err)
	}
}

func TestNewRejectsProductionInmemDriverInjection(t *testing.T) {
	t.Parallel()
	cfg, err := LoadConfig(writeConfig(t, `
f1:
  env: prod
  service: orders
  broker:
    driver: rabbitmq
    endpoints: [amqp://broker:5672/]
    rabbitmq:
      queueType: quorum
`))
	if err != nil {
		t.Fatal(err)
	}
	var logs logSink
	_, err = New(context.Background(), cfg,
		WithDriver(&testDriver{name: "inmem", conn: &testConn{}}),
		WithLogger(slog.New(slog.NewTextHandler(&logs, nil))),
	)
	if err == nil || err.Error() != "f1: broker.driver inmem is not allowed in prod" {
		t.Fatalf("New() error = %v, want inmem production safeguard", err)
	}
	output := logs.String()
	if got := strings.Count(output, "f1 driver identity mismatch"); got != 1 {
		t.Fatalf("identity mismatch log count = %d, want 1; output = %q", got, output)
	}
	if !strings.Contains(output, "configured_driver=rabbitmq") || !strings.Contains(output, "injected_driver=inmem") {
		t.Fatalf("identity mismatch log = %q, want both driver names", output)
	}
}

func TestNewRejectsProductionRabbitMQRailForInjectedDriver(t *testing.T) {
	t.Parallel()
	cfg, err := LoadConfig(writeConfig(t, `
f1:
  env: prod
  service: orders
  broker:
    driver: kafka
    endpoints: [localhost:19092]
`))
	if err != nil {
		t.Fatal(err)
	}
	_, err = New(context.Background(), cfg, WithDriver(&testDriver{name: "rabbitmq", conn: &testConn{}}))
	if err == nil || err.Error() != "f1: broker.rabbitmq.queueType must be quorum in prod" {
		t.Fatalf("New() error = %v, want RabbitMQ production safeguard", err)
	}
}

func TestNewAllowsThirdPartyDriverIdentity(t *testing.T) {
	t.Parallel()
	cfg, err := LoadConfig(writeConfig(t, `
f1:
  env: prod
  service: orders
  broker:
    driver: kafka
    endpoints: [localhost:19092]
`))
	if err != nil {
		t.Fatal(err)
	}
	client, err := New(context.Background(), cfg,
		WithDriver(&testDriver{name: "third-party", conn: &testConn{}}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
}

func TestLogCapabilitiesWarnsForConfiguredUnavailableFeature(t *testing.T) {
	var logs logSink
	cfg, err := LoadConfig(writeConfig(t, `
f1:
  env: test
  service: orders
  broker:
    driver: inmem
  subscriptions:
    orders:
      topics: [orders.created]
      mode: orderedByKey
`))
	if err != nil {
		t.Fatal(err)
	}
	client, err := New(context.Background(), cfg,
		WithDriver(&testDriver{conn: &testConn{info: driver.BrokerInfo{Kind: "test", Version: "1"}}}),
		WithLogger(slog.New(slog.NewTextHandler(&logs, nil))),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	assertOrderedCapabilityLog(t, logs.String(), "WARN")
}

func TestLogCapabilitiesAnnouncesUnusedUnavailableFeatureAtInfo(t *testing.T) {
	var logs logSink
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&testDriver{conn: &testConn{info: driver.BrokerInfo{Kind: "test", Version: "1"}}}),
		WithLogger(slog.New(slog.NewTextHandler(&logs, nil))),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	assertOrderedCapabilityLog(t, logs.String(), "INFO")
}

func assertOrderedCapabilityLog(t *testing.T, output, wantLevel string) {
	t.Helper()
	var matches []string
	for line := range strings.SplitSeq(output, "\n") {
		if strings.Contains(line, "feature=ordered_by_key") {
			matches = append(matches, line)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("ordered_by_key log lines = %d, want 1; output = %q", len(matches), output)
	}
	if !strings.Contains(matches[0], "level="+wantLevel+" ") {
		t.Fatalf("ordered_by_key log = %q, want level %s", matches[0], wantLevel)
	}
}

func TestCloseRefusesWithResourcesOutstanding(t *testing.T) {
	t.Parallel()
	fakeDriver := &testDriver{conn: &testConn{closeErr: driver.ErrResourcesOutstanding}}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(fakeDriver))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(context.Background()); !errors.Is(err, driver.ErrResourcesOutstanding) {
		t.Fatalf("Close() error = %v", err)
	}
	fakeDriver.conn.closeErr = nil
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, want := fakeDriver.conn.closeCalls, 2; got != want {
		t.Fatalf("close calls = %d, want %d", got, want)
	}
}

func TestNewStrictPortabilityKeepsConsumerScaling(t *testing.T) {
	t.Parallel()
	caps := driver.Capabilities{PerMessageAck: true, OrderedByKey: true, NativeDelay: true, ConsumerScaling: driver.ScalingFree, MaxMessageBytes: 10, MaxHeaderBytes: 20}
	fakeDriver := &testDriver{conn: &testConn{caps: caps, info: driver.BrokerInfo{Kind: "test", Version: "1"}}}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(fakeDriver), WithStrictPortability())
	if err != nil {
		t.Fatal(err)
	}
	limits := client.Limits()
	if got := featureStatus(limits, "ordered_by_key"); got.Mode != FeatureNative {
		t.Fatalf("strict ordered_by_key mode = %v, want native", got.Mode)
	}
	if got := featureStatus(limits, "consumer_scaling"); got.Detail != "free" {
		t.Fatalf("strict consumer scaling detail = %q, want free", got.Detail)
	}
	if got := featureStatus(limits, "per_message_ack"); got.Mode != FeatureEmulated {
		t.Fatalf("strict per_message_ack mode = %v, want emulated", got.Mode)
	}
	if err := client.Health(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestHealthRejectsClosedClient(t *testing.T) {
	t.Parallel()
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&testDriver{conn: &testConn{}}))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.Health(context.Background()); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("Health() error = %v, want closed-client error", err)
	}
}

func TestCloseDrainsAllRunnersAfterOneFails(t *testing.T) {
	t.Parallel()
	cfg := testClientConfig(t)
	conn := &testConn{}
	client, err := New(context.Background(), cfg, WithDriver(&testDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	firstErr := errors.New("first runner failed")
	middleErr := errors.New("middle runner failed")
	var drainCalls atomic.Int32
	firstDone := make(chan struct{})
	middleDone := make(chan struct{})
	lastDone := make(chan struct{})
	var release sync.Once
	releaseAll := func() {
		release.Do(func() {
			close(firstDone)
			close(middleDone)
			close(lastDone)
		})
	}
	makeRunner := func(runErr error, done chan struct{}) *Runner {
		return &Runner{
			client:  client,
			started: true,
			done:    done,
			runErr:  runErr,
			cancel: func() {
				if drainCalls.Add(1) == 3 {
					releaseAll()
				}
			},
		}
	}
	first := makeRunner(firstErr, firstDone)
	middle := makeRunner(middleErr, middleDone)
	last := makeRunner(nil, lastDone)
	client.mu.Lock()
	client.runners[first] = struct{}{}
	client.runners[middle] = struct{}{}
	client.runners[last] = struct{}{}
	client.mu.Unlock()

	closeContext, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = client.Close(closeContext)
	if !errors.Is(err, middleErr) {
		t.Fatalf("Close() error = %v, want middle runner error", err)
	}
	if !errors.Is(err, firstErr) {
		t.Fatalf("Close() error = %v, want joined first runner error", err)
	}
	for name, done := range map[string]<-chan struct{}{
		"first":  firstDone,
		"middle": middleDone,
		"last":   lastDone,
	} {
		select {
		case <-done:
		default:
			t.Fatalf("%s runner was not observably drained", name)
		}
	}
	if got, want := drainCalls.Load(), int32(3); got != want {
		t.Fatalf("runner drain calls = %d, want %d", got, want)
	}

	for _, runner := range []*Runner{first, middle} {
		runner.mu.Lock()
		runner.runErr = nil
		runner.mu.Unlock()
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestCloseDoesNotWaitForHealthProbe(t *testing.T) {
	t.Parallel()
	conn := &testConn{pingStarted: make(chan struct{}), pingRelease: make(chan struct{})}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&testDriver{conn: conn}), WithClock(clock.NewReal()))
	if err != nil {
		t.Fatal(err)
	}
	healthDone := make(chan error, 1)
	go func() { healthDone <- client.Health(context.Background()) }()
	<-conn.pingStarted
	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(context.Background()) }()
	timer := client.options.clock.Timer(time.Second)
	defer timer.Stop()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-timer.C:
		t.Fatal("Close waited for a blocked Health probe")
	}
	close(conn.pingRelease)
	if err := <-healthDone; err != nil {
		t.Fatal(err)
	}
}

func TestLimitsClassifiesPortableAndUnavailableFeatures(t *testing.T) {
	t.Parallel()
	limits := limitsFor("test", driver.BrokerInfo{Kind: "test"}, driver.Capabilities{})
	got := make(map[string]FeatureMode, len(limits.Features))
	for _, feature := range limits.Features {
		got[feature.Feature] = feature.Mode
	}
	for name, want := range map[string]FeatureMode{
		"per_message_ack": FeatureEmulated,
		"native_delay":    FeatureEmulated,
		"delivery_count":  FeatureEmulated,
		"ordered_by_key":  FeatureUnavailable,
		"dlq_backstop":    FeatureUnavailable,
		"lag_metrics":     FeatureUnavailable,
	} {
		if got[name] != want {
			t.Errorf("%s mode = %v, want %v", name, got[name], want)
		}
	}
}

func TestHealthPropagatesPingError(t *testing.T) {
	t.Parallel()
	want := errors.New("broker unavailable")
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&testDriver{conn: &testConn{pingErr: want}}))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Health(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Health() error = %v, want %v", err, want)
	}
}

func featureStatus(limits Limits, name string) FeatureStatus {
	for _, feature := range limits.Features {
		if feature.Feature == name {
			return feature
		}
	}
	return FeatureStatus{Feature: name, Mode: FeatureUnavailable}
}

func testClientConfig(t *testing.T) Config {
	t.Helper()
	cfg, err := LoadConfig(writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Topology.AutoCreate = true
	return cfg
}

type testDriver struct {
	conn   *testConn
	name   string
	opened bool
}

type nilConnectionDriver struct{}

func (nilConnectionDriver) Name() string                      { return "nil-connection" }
func (nilConnectionDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (nilConnectionDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return nil, nil
}

func (d *testDriver) Name() string {
	if d.name != "" {
		return d.name
	}
	return "test"
}

func (*testDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (d *testDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	d.opened = true
	if d.conn == nil {
		d.conn = &testConn{}
	}
	return d.conn, nil
}

type testConn struct {
	caps        driver.Capabilities
	info        driver.BrokerInfo
	closeErr    error
	pingErr     error
	pingStarted chan struct{}
	pingRelease chan struct{}
	closeCalls  int
}

func (c *testConn) Capabilities() driver.Capabilities { return c.caps }
func (c *testConn) BrokerInfo() driver.BrokerInfo     { return c.info }
func (*testConn) Producer(context.Context, driver.ProducerConfig) (driver.Producer, error) {
	return nil, errors.New("not implemented")
}

func (*testConn) Consumer(context.Context, driver.ConsumerConfig) (driver.Consumer, error) {
	return nil, errors.New("not implemented")
}
func (*testConn) Admin() driver.Admin { return nil }
func (c *testConn) Ping(context.Context) error {
	if c.pingStarted != nil {
		close(c.pingStarted)
	}
	if c.pingRelease != nil {
		<-c.pingRelease
	}
	return c.pingErr
}
func (c *testConn) Close(context.Context) error { c.closeCalls++; return c.closeErr }

func TestDriverConfigForwardingAndFallback(t *testing.T) {
	t.Run("explicit instance id and rebalance timeout", func(t *testing.T) {
		cfg := Config{
			Service:    "order-service",
			InstanceID: "order-worker-1",
			Lifecycle: LifecycleConfig{
				RebalanceDrainTimeout: 12 * time.Second,
			},
			Broker: BrokerConfig{
				Endpoints:      []string{"127.0.0.1:9092"},
				ConnectTimeout: 5 * time.Second,
			},
		}
		driverCfg := driverConfig(cfg)
		if driverCfg.ClientID != "order-worker-1" {
			t.Fatalf("ClientID = %q, want %q", driverCfg.ClientID, "order-worker-1")
		}
		if driverCfg.InstanceID != "order-worker-1" {
			t.Fatalf("InstanceID = %q, want %q", driverCfg.InstanceID, "order-worker-1")
		}
		if driverCfg.RebalanceDrainTimeout != 12*time.Second {
			t.Fatalf("RebalanceDrainTimeout = %v, want %v", driverCfg.RebalanceDrainTimeout, 12*time.Second)
		}
	})

	t.Run("empty instance id falls back client id to service", func(t *testing.T) {
		cfg := Config{
			Service:    "order-service",
			InstanceID: "",
			Lifecycle: LifecycleConfig{
				RebalanceDrainTimeout: 18 * time.Second,
			},
			Broker: BrokerConfig{
				Endpoints: []string{"127.0.0.1:9092"},
			},
		}
		driverCfg := driverConfig(cfg)
		if driverCfg.ClientID != "order-service" {
			t.Fatalf("ClientID = %q, want fallback to service %q", driverCfg.ClientID, "order-service")
		}
		if driverCfg.InstanceID != "" {
			t.Fatalf("InstanceID = %q, want empty", driverCfg.InstanceID)
		}
		if driverCfg.RebalanceDrainTimeout != 18*time.Second {
			t.Fatalf("RebalanceDrainTimeout = %v, want %v", driverCfg.RebalanceDrainTimeout, 18*time.Second)
		}
	})
}
