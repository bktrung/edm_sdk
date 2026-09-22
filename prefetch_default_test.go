package f1

import (
	"context"
	"strings"
	"testing"
)

var prefetchDefaultSixTopics = []string{"t0", "t1", "t2", "t3", "t4", "t5"}

func TestPrefetchDefaultGoBuiltConfigRaisesUnnamed(t *testing.T) {
	t.Parallel()
	cfg := normalizeConfig(Config{
		Subscriptions: map[string]SubscriptionConfig{
			"orders": {Topics: prefetchDefaultSixTopics},
		},
	})
	if got, want := cfg.Subscriptions["orders"].Prefetch, 72; got != want {
		t.Fatalf("Prefetch = %d, want %d", got, want)
	}
	cfg = normalizeConfig(cfg)
	if got, want := cfg.Subscriptions["orders"].Prefetch, 72; got != want {
		t.Fatalf("second normalize Prefetch = %d, want %d", got, want)
	}
}

func TestPrefetchDefaultGoBuiltConfigKeepsNamedBelowFloor(t *testing.T) {
	t.Parallel()
	cfg := testClientConfig(t)
	cfg.Subscriptions = map[string]SubscriptionConfig{
		"orders": {Topics: prefetchDefaultSixTopics, Prefetch: 64},
	}
	cfg = normalizeConfig(cfg)
	if got, want := cfg.Subscriptions["orders"].Prefetch, 64; got != want {
		t.Fatalf("Prefetch = %d, want %d", got, want)
	}
	if err := validateConfig(cfg, "inmem"); err == nil || !strings.Contains(err.Error(), "prefetch 64") || !strings.Contains(err.Error(), "lane count 72") {
		t.Fatalf("validateConfig() error = %v, want named prefetch below lane count to fail", err)
	}
}

func TestPrefetchDefaultNewKeepsNamedBelowFloor(t *testing.T) {
	t.Parallel()
	cfg := testClientConfig(t)
	cfg.Subscriptions = map[string]SubscriptionConfig{
		"orders": {Topics: prefetchDefaultSixTopics, Prefetch: 64},
	}
	_, err := New(context.Background(), cfg, WithDriver(&testDriver{conn: &testConn{}}))
	if err == nil || !strings.Contains(err.Error(), "prefetch 64") || !strings.Contains(err.Error(), "lane count 72") {
		t.Fatalf("New() error = %v, want named prefetch below lane count to fail", err)
	}
}

func TestPrefetchDefaultSubscribeRaisesUnnamed(t *testing.T) {
	t.Parallel()
	client := newPublishClient(t, &recordingProducer{})
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:   "orders",
		Topics: prefetchDefaultSixTopics,
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v, want unnamed prefetch raised to lane count", err)
	}
	if got, want := runner.config.Prefetch, 72; got != want {
		t.Fatalf("config.Prefetch = %d, want %d", got, want)
	}
}

func TestPrefetchDefaultSubscribeKeepsNamedBelowFloor(t *testing.T) {
	t.Parallel()
	client := newPublishClient(t, &recordingProducer{})
	_, err := client.Subscribe(context.Background(), Subscription{
		Name:     "orders",
		Topics:   prefetchDefaultSixTopics,
		Prefetch: 64,
	})
	if err == nil || !strings.Contains(err.Error(), "prefetch 64") || !strings.Contains(err.Error(), "lane count 72") {
		t.Fatalf("Subscribe() error = %v, want named prefetch below lane count to fail", err)
	}
}

func TestPrefetchDefaultConfigRaisesUnnamed(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n  subscriptions:\n    orders:\n      topics: [t0, t1, t2, t3, t4, t5]\n")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v, want unnamed prefetch raised to lane count", err)
	}
	if got, want := cfg.Subscriptions["orders"].Prefetch, 72; got != want {
		t.Fatalf("Prefetch = %d, want %d", got, want)
	}
}

func TestPrefetchDefaultConfigKeepsNamedBelowFloor(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n  subscriptions:\n    orders:\n      topics: [t0, t1, t2, t3, t4, t5]\n      prefetch: 64\n")
	_, err := LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "prefetch 64") || !strings.Contains(err.Error(), "lane count 72") {
		t.Fatalf("LoadConfig() error = %v, want named prefetch below lane count to fail", err)
	}
}

func TestPrefetchDefaultSubscribeLeavesSmallAlone(t *testing.T) {
	t.Parallel()
	client := newPublishClient(t, &recordingProducer{})
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:   "orders",
		Topics: []string{"orders.created"},
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v, want success", err)
	}
	if got, want := runner.config.Prefetch, 64; got != want {
		t.Fatalf("config.Prefetch = %d, want %d", got, want)
	}
}

func TestPrefetchDefaultSubscribeEnvNamesPrefetch(t *testing.T) {
	t.Setenv("F1_SUBSCRIPTIONS_ORDERS_PREFETCH", "64")
	client := newPublishClient(t, &recordingProducer{})
	_, err := client.Subscribe(context.Background(), Subscription{
		Name:   "orders",
		Topics: prefetchDefaultSixTopics,
	})
	if err == nil || !strings.Contains(err.Error(), "prefetch 64") {
		t.Fatalf("Subscribe() error = %v, want env-named prefetch below lane count to fail", err)
	}
}
