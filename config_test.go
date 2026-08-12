package f1

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigUnknownKeyIsStartupError(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, `
f1:
  env: test
  service: orders
  broker:
    driver: inmem
  codec:
    contentMode: binary
  unknown: true
`)
	_, err := LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("LoadConfig() error = %v, want unknown-key error", err)
	}
}

func TestLoadConfigRejectsUnsupportedKnownValue(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, `
f1:
  env: test
  service: orders
  broker:
    driver: inmem
  codec:
    contentMode: structured
`)
	_, err := LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "codec.contentMode") {
		t.Fatalf("LoadConfig() error = %v, want contentMode error", err)
	}
}

func TestLoadConfigFlattensBrokerOptions(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, `
f1:
  env: test
  service: orders
  broker:
    driver: kafka
    connectTimeout: 4s
    kafka:
      compression: lz4
      sessionTimeout: 60s
  codec:
    contentMode: binary
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if got, want := cfg.Broker.DriverOptions["kafka.compression"], "lz4"; got != want {
		t.Fatalf("compression = %q, want %q", got, want)
	}
	if got, want := cfg.Broker.ConnectTimeout, 4*time.Second; got != want {
		t.Fatalf("connect timeout = %s, want %s", got, want)
	}
}

func TestLoadConfigEnvironmentOverridesYAML(t *testing.T) {
	path := writeConfig(t, `
f1:
  env: yaml
  service: orders
  broker:
    driver: inmem
  codec:
    contentMode: binary
`)
	t.Setenv("F1_ENV", "environment")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if got, want := cfg.Env, "environment"; got != want {
		t.Fatalf("Env = %q, want %q", got, want)
	}
}

func TestLoadConfigSubscriptionConfigByName(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, `
f1:
  env: test
  service: orders
  broker:
    driver: inmem
  codec:
    contentMode: binary
  subscriptions:
    orders:
      topics: [com.za.order.created]
      mode: orderedByKey
      maxDeferrals: 25
      unmatchedPolicy: deadletter
      retry:
        tiers: [1s, 5s]
      fairness:
        weights: {high: 8, normal: 4, low: 1}
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	sub := cfg.Subscriptions["orders"]
	if sub.Mode != OrderedByKey || sub.MaxDeferrals != 25 || sub.UnmatchedPolicy != DeadLetter {
		t.Fatalf("subscription = %#v", sub)
	}
	if got, want := sub.Retry.DelayFor(3), 5*time.Second; got != want {
		t.Fatalf("DelayFor(3) = %s, want %s", got, want)
	}
	if got, want := sub.Fairness.Weights[PriorityHigh], 8; got != want {
		t.Fatalf("high weight = %d, want %d", got, want)
	}
}

func TestLoadConfigAppliesNestedDefaults(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Topology.VerifyOnStart || cfg.Codec.MaxBodyBytes != 1024*1024 || !cfg.Observability.Metrics.Enabled {
		t.Fatalf("defaults = %#v", cfg)
	}
}

func TestLoadConfigBareKafkaUsesCompatibleDefaults(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: kafka\n")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := cfg.Lifecycle.RebalanceDrainTimeout, 25*time.Second; got != want {
		t.Fatalf("RebalanceDrainTimeout = %s, want %s", got, want)
	}
}

func TestLoadConfigRejectsSubscriptionWithoutTopics(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n  subscriptions:\n    orders: {}\n")
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "subscriptions.orders.topics") {
		t.Fatalf("LoadConfig() error = %v, want empty topics error", err)
	}
}

func TestLoadConfigSubscriptionDefaultsAndRemovedDLQKey(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n  subscriptions:\n    orders:\n      topics: [orders]\n")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := cfg.Subscriptions["orders"].MaxDeferrals, 24; got != want {
		t.Fatalf("MaxDeferrals = %d, want %d", got, want)
	}
	path = writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n  subscriptions:\n    orders:\n      dlq:\n        enabled: false\n")
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "dlq") {
		t.Fatalf("LoadConfig() error = %v, want removed dlq key error", err)
	}
}

func TestLoadConfigSubscriptionUsesBrokerPrefetchFallback(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n    defaultPrefetch: 128\n  subscriptions:\n    orders:\n      topics: [orders]\n")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := cfg.Subscriptions["orders"].Prefetch, 128; got != want {
		t.Fatalf("Prefetch = %d, want %d", got, want)
	}
}

func TestLoadConfigPreservesExplicitFalseAging(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n  subscriptions:\n    orders:\n      topics: [orders]\n      fairness:\n        agingEnabled: false\n")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Subscriptions["orders"].Fairness.AgingEnabled {
		t.Fatal("AgingEnabled = true, want explicit false")
	}
}

func TestLoadConfigRejectsUnknownFairnessPriority(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n  subscriptions:\n    orders:\n      topics: [orders]\n      fairness:\n        weights: {bogus: 99}\n")
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "fairness.weights.bogus") {
		t.Fatalf("LoadConfig() error = %v, want invalid priority error", err)
	}
}

func TestLoadConfigRejectsUnknownFairnessKey(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n  subscriptions:\n    orders:\n      topics: [orders]\n      fairness:\n        agingEnbaled: false\n")
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "agingEnbaled") {
		t.Fatalf("LoadConfig() error = %v, want unknown fairness key error", err)
	}
}

func TestLoadConfigFairnessMapsReplaceDefaults(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n  subscriptions:\n    orders:\n      topics: [orders]\n      priorities: [high]\n      fairness:\n        weights: {high: 3}\n")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	weights := cfg.Subscriptions["orders"].Fairness.Weights
	if len(weights) != 1 || weights[PriorityHigh] != 3 {
		t.Fatalf("weights = %#v, want only high:3", weights)
	}
}

func TestLoadConfigRequiresQuorumRabbitMQInProd(t *testing.T) {
	t.Parallel()
	for _, queueType := range []string{"", "garbage"} {
		path := writeConfig(t, "f1:\n  env: prod\n  service: orders\n  broker:\n    driver: rabbitmq\n    rabbitmq:\n      queueType: "+queueType+"\n")
		if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "queueType") {
			t.Fatalf("queueType %q: error = %v, want quorum error", queueType, err)
		}
	}
	path := writeConfig(t, "f1:\n  env: prod\n  service: orders\n  broker:\n    driver: rabbitmq\n")
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "queueType") {
		t.Fatalf("omitted queueType: error = %v, want quorum error", err)
	}
	path = writeConfig(t, "f1:\n  env: prod\n  service: orders\n  broker:\n    driver: rabbitmq\n    rabbitmq:\n      queueType: quorum\n")
	if _, err := LoadConfig(path); err != nil {
		t.Fatalf("quorum queueType error = %v", err)
	}
}

func TestSubscriptionZeroValuesAreDocumentedDefaults(t *testing.T) {
	if Mode(0) != Unordered {
		t.Fatalf("Mode(0) = %v, want Unordered", Mode(0))
	}
	if UnmatchedPolicy(0) != Ignore {
		t.Fatalf("UnmatchedPolicy(0) = %v, want Ignore", UnmatchedPolicy(0))
	}
}

func TestLoadConfigValidatesBrokerTimeoutRelationships(t *testing.T) {
	t.Parallel()
	cases := []struct{ name, yaml, want string }{
		{"kafka too long", "f1:\n  env: test\n  service: orders\n  broker:\n    driver: kafka\n  lifecycle:\n    rebalanceDrainTimeout: 30s\n", "rebalanceDrainTimeout"},
		{"kafka malformed", "f1:\n  env: test\n  service: orders\n  broker:\n    driver: kafka\n    kafka:\n      sessionTimeout: garbage\n", "sessionTimeout"},
		{"rabbit malformed", "f1:\n  env: test\n  service: orders\n  broker:\n    driver: rabbitmq\n    rabbitmq:\n      consumerTimeout: garbage\n", "consumerTimeout"},
		{"rabbit too short", "f1:\n  env: test\n  service: orders\n  broker:\n    driver: rabbitmq\n    rabbitmq:\n      consumerTimeout: 10s\n  subscriptions:\n    orders:\n      topics: [orders]\n", "consumerTimeout"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			path := writeConfig(t, test.yaml)
			if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("LoadConfig() error = %v, want %s", err, test.want)
			}
		})
	}
}

func TestLoadConfigRejectsUnknownBrokerOption(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: kafka\n    kafka:\n      typo: true\n")
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "broker.kafka.typo") {
		t.Fatalf("LoadConfig() error = %v, want broker-key error", err)
	}
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
