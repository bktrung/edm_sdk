package f1

import (
	"context"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestSubscribeRejectsOrderedByKeyWithConcurrency(t *testing.T) {
	t.Parallel()
	conn := &testConn{caps: driver.Capabilities{OrderedByKey: true}, info: testBrokerInfo()}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&testDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	_, err = client.Subscribe(context.Background(), Subscription{
		Name:        "orders",
		Topics:      []string{"orders.created"},
		Mode:        OrderedByKey,
		Concurrency: 2,
	})
	if err == nil || !strings.Contains(err.Error(), "ordered_by_key") || !strings.Contains(err.Error(), "concurrency 1") {
		t.Fatalf("Subscribe() error = %v, want ordered_by_key/concurrency validation", err)
	}
}

func TestSubscribeRejectsPrefetchBelowLaneCount(t *testing.T) {
	t.Parallel()
	client := newPublishClient(t, &recordingProducer{})
	_, err := client.Subscribe(context.Background(), Subscription{
		Name:     "orders",
		Topics:   []string{"orders.created"},
		Prefetch: 11,
	})
	if err == nil || !strings.Contains(err.Error(), "prefetch 11") || !strings.Contains(err.Error(), "lane count 12") || !strings.Contains(err.Error(), "topics x priorities x (1 + retryTiers)") {
		t.Fatalf("Subscribe() error = %v, want lane-floor validation", err)
	}
}

func TestSubscribeMergesExplicitGoOverEnvironmentAndYAML(t *testing.T) {
	t.Setenv("F1_SUBSCRIPTIONS_ORDERS_CONCURRENCY", "6")
	cfg, err := LoadConfig(writeConfig(t, `
f1:
  env: test
  service: orders
  broker:
    driver: inmem
  subscriptions:
    orders:
      topics: [orders.created]
      concurrency: 8
      prefetch: 12
`))
	if err != nil {
		t.Fatal(err)
	}
	client, err := New(context.Background(), cfg, WithDriver(&testDriver{conn: &testConn{info: testBrokerInfo()}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	runner, err := client.Subscribe(context.Background(), Subscription{Name: "orders", Concurrency: 4})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := runner.config.Concurrency, 4; got != want {
		t.Fatalf("resolved concurrency = %d, want %d", got, want)
	}
	if got, want := runner.config.Prefetch, 12; got != want {
		t.Fatalf("resolved prefetch = %d, want %d", got, want)
	}
}

func TestSubscribeGoZeroCannotOverrideYAMLPolicy(t *testing.T) {
	t.Parallel()
	cfg, err := LoadConfig(writeConfig(t, `
f1:
  env: test
  service: orders
  broker:
    driver: inmem
  subscriptions:
    orders:
      topics: [orders.created]
      unmatchedPolicy: deadletter
`))
	if err != nil {
		t.Fatal(err)
	}
	client, err := New(context.Background(), cfg, WithDriver(&testDriver{conn: &testConn{info: testBrokerInfo()}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	runner, err := client.Subscribe(context.Background(), Subscription{Name: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := runner.config.UnmatchedPolicy, DeadLetter; got != want {
		t.Fatalf("resolved unmatched policy = %v, want %v", got, want)
	}
}

func TestSubscribePreservesExplicitYAMLFairnessZero(t *testing.T) {
	t.Parallel()
	cfg, err := LoadConfig(writeConfig(t, `
f1:
  env: test
  service: orders
  broker:
    driver: inmem
  subscriptions:
    orders:
      topics: [orders.created]
      fairness:
        agingEnabled: false
`))
	if err != nil {
		t.Fatal(err)
	}
	client, err := New(context.Background(), cfg, WithDriver(&testDriver{conn: &testConn{info: testBrokerInfo()}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	runner, err := client.Subscribe(context.Background(), Subscription{Name: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if runner.config.Fairness.AgingEnabled {
		t.Fatal("explicit YAML agingEnabled: false was replaced by the default")
	}
}

func TestUnmatchedDiscardReasonIsNotDeathReason(t *testing.T) {
	discarded := discardUnmatched(Envelope{})
	if discarded.Reason != DiscardUnmatched {
		t.Fatalf("discard reason = %q, want %q", discarded.Reason, DiscardUnmatched)
	}
	if DeathReason(discarded.Reason) == ReasonTerminal {
		t.Fatal("unmatched discard must not be stamped terminal")
	}
}

func TestTerminalNotificationsCannotBlockSettlement(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan struct{})
	go func() {
		notifyDiscarded(context.Background(), func(context.Context, Discarded) {
			close(started)
			<-release
		}, Discarded{Reason: DiscardUnmatched})
		close(returned)
	}()
	timer := clock.NewReal().Timer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-returned:
	case <-timer.C:
		t.Fatal("notification blocked settlement")
	}
	<-started
	close(release)
	notifyDeadLetter(context.Background(), func(context.Context, DeadLettered) { panic("notification panic") }, DeadLettered{Reason: ReasonTerminal})
}

func testBrokerInfo() driver.BrokerInfo {
	return driver.BrokerInfo{Kind: "test", Version: "1"}
}
