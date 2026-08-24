package f1

import (
	"context"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestSubscribeRejectsOrderedByKeyWhenUnavailable(t *testing.T) {
	t.Parallel()
	conn := &testConn{caps: driver.Capabilities{}, info: testBrokerInfo()}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&testDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	status := featureStatus(client.Limits(), "ordered_by_key")
	if status.Mode != FeatureUnavailable {
		t.Fatalf("ordered_by_key limit = %v, want unavailable", status.Mode)
	}
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:        "orders",
		Topics:      []string{"orders.created"},
		Mode:        OrderedByKey,
		Concurrency: 2,
	})
	if runner != nil || err == nil || !strings.Contains(err.Error(), "ordered_by_key") {
		t.Fatalf("Subscribe() = runner %v, error %v, want ordered_by_key construction error", runner, err)
	}
}

func TestSubscribeRejectsUnsupportedMode(t *testing.T) {
	client := newPublishClient(t, &recordingProducer{})
	_, err := client.Subscribe(context.Background(), Subscription{
		Name:        "orders",
		Topics:      []string{"orders.created"},
		Mode:        Mode(99),
		Concurrency: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "subscriptions.orders.mode") {
		t.Fatalf("Subscribe() error = %v, want unsupported mode", err)
	}
}

func TestSubscribeAcceptsOrderedByKeyWhenNative(t *testing.T) {
	t.Parallel()
	conn := &testConn{caps: driver.Capabilities{OrderedByKey: true}, info: testBrokerInfo()}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&testDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	status := featureStatus(client.Limits(), "ordered_by_key")
	if status.Mode != FeatureNative {
		t.Fatalf("ordered_by_key limit = %v, want native", status.Mode)
	}
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:        "orders",
		Topics:      []string{"orders.created"},
		Mode:        OrderedByKey,
		Concurrency: 2,
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v, want ordered mode to be accepted", err)
	}
	if runner == nil || runner.config.Mode != OrderedByKey || runner.config.Concurrency != 2 {
		t.Fatalf("resolved subscription = %#v, want ordered mode with concurrency 2", runner.config)
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
	discarded := discardUnmatched(Envelope{}, nil)
	if discarded.Reason != DiscardUnmatched {
		t.Fatalf("discard reason = %q, want %q", discarded.Reason, DiscardUnmatched)
	}
	if DeathReason(discarded.Reason) == ReasonTerminal {
		t.Fatal("unmatched discard must not be stamped terminal")
	}
}

func TestTerminalNotificationsPreserveBoundedContextAndPanicRecovery(t *testing.T) {
	t.Parallel()
	observed := make(chan bool, 1)
	runner := &Runner{subscription: Subscription{
		OnDeadLetter: func(ctx context.Context, _ DeadLettered) {
			deadline, hasDeadline := ctx.Deadline()
			observed <- hasDeadline && time.Until(deadline) > 0 && time.Until(deadline) <= terminalNotificationTimeout
		},
	}}
	runnerNotifyDeadLetter(runner, context.Background(), DeadLettered{Reason: ReasonTerminal})
	timer := clock.NewReal().Timer(time.Second)
	defer timer.Stop()
	select {
	case got := <-observed:
		if !got {
			t.Fatal("terminal notification context was not bounded")
		}
	case <-timer.C:
		t.Fatal("terminal notification did not start")
	}

	panicStarted := make(chan struct{})
	runner.subscription.OnDeadLetter = func(context.Context, DeadLettered) {
		close(panicStarted)
		panic("notification panic")
	}
	runnerNotifyDeadLetter(runner, context.Background(), DeadLettered{Reason: ReasonTerminal})
	select {
	case <-panicStarted:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("panic notification did not start")
	}
}

func testBrokerInfo() driver.BrokerInfo {
	return driver.BrokerInfo{Kind: "test", Version: "1"}
}
