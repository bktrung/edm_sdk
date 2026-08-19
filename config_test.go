package f1

import (
	"math"
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
	if sub.Mode != OrderedByKey || sub.UnmatchedPolicy != DeadLetter {
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
	if _, err := LoadConfig(path); err != nil {
		t.Fatal(err)
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

func TestValidateConfigRejectsInvalidRetryValues(t *testing.T) {
	cases := invalidRetryValueCases()
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			cfg := validValidationConfig()
			retry := cfg.Subscriptions["orders"].Retry
			test.set(&retry)
			cfg.Subscriptions["orders"] = withRetry(cfg.Subscriptions["orders"], retry)
			if err := validateConfig(cfg); err == nil || !strings.Contains(err.Error(), test.field) {
				t.Fatalf("validateConfig() error = %v, want %s validation", err, test.field)
			}
		})
	}
}

func TestValidateSubscriptionRejectsInvalidRetryValues(t *testing.T) {
	cfg := validValidationConfig()
	for _, test := range invalidRetryValueCases() {
		t.Run(test.name, func(t *testing.T) {
			sub := cfg.Subscriptions["orders"]
			test.set(&sub.Retry)
			if err := validateSubscription(cfg, "orders", sub); err == nil || !strings.Contains(err.Error(), test.field) {
				t.Fatalf("validateSubscription() error = %v, want %s validation", err, test.field)
			}
		})
	}
}

func TestValidateConfigRejectsNegativeLifecycleDurations(t *testing.T) {
	cases := []struct {
		name string
		set  func(*LifecycleConfig)
	}{
		{name: "pre stop delay", set: func(cfg *LifecycleConfig) { cfg.PreStopDelay = -time.Second }},
		{name: "drain timeout", set: func(cfg *LifecycleConfig) { cfg.DrainTimeout = -time.Second }},
		{name: "handler grace", set: func(cfg *LifecycleConfig) { cfg.HandlerGrace = -time.Second }},
		{name: "flush timeout", set: func(cfg *LifecycleConfig) { cfg.FlushTimeout = -time.Second }},
		{name: "close timeout", set: func(cfg *LifecycleConfig) { cfg.CloseTimeout = -time.Second }},
		{name: "rebalance drain timeout", set: func(cfg *LifecycleConfig) { cfg.RebalanceDrainTimeout = -time.Second }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			cfg := validValidationConfig()
			cfg.Subscriptions = nil
			test.set(&cfg.Lifecycle)
			if err := validateConfig(cfg); err == nil || !strings.Contains(err.Error(), "lifecycle") {
				t.Fatalf("validateConfig() error = %v, want lifecycle validation", err)
			}
		})
	}
}

func TestValidateConfigRejectsZeroDrainTimeout(t *testing.T) {
	cfg := validValidationConfig()
	cfg.Subscriptions = nil
	cfg.Lifecycle.DrainTimeout = 0
	if err := validateConfig(cfg); err == nil || !strings.Contains(err.Error(), "drainTimeout") {
		t.Fatalf("validateConfig() error = %v, want drainTimeout validation", err)
	}
}

func TestValidateSubscriptionRejectsZeroDrainTimeout(t *testing.T) {
	cfg := validValidationConfig()
	cfg.Lifecycle.DrainTimeout = 0
	sub := cfg.Subscriptions["orders"]
	if err := validateSubscription(cfg, "orders", sub); err == nil || !strings.Contains(err.Error(), "drainTimeout") {
		t.Fatalf("validateSubscription() error = %v, want drainTimeout validation", err)
	}
}

func validValidationConfig() Config {
	cfg := defaultConfig()
	cfg.Env = "test"
	cfg.Service = "orders"
	cfg.Broker.Driver = "inmem"
	cfg.Subscriptions = map[string]SubscriptionConfig{
		"orders": {
			Topics:         []string{"orders"},
			Concurrency:    1,
			Prefetch:       64,
			Priorities:     []Priority{PriorityNormal},
			Retry:          RetryConfig{MaxAttempts: 2, Tiers: []time.Duration{time.Second}},
			HandlerTimeout: time.Second,
		},
	}
	return cfg
}

func withRetry(sub SubscriptionConfig, retry RetryConfig) SubscriptionConfig {
	sub.Retry = retry
	return sub
}

func invalidRetryValueCases() []struct {
	name  string
	field string
	set   func(*RetryConfig)
} {
	return []struct {
		name  string
		field string
		set   func(*RetryConfig)
	}{
		{name: "negative initial interval", field: "initialInterval", set: func(cfg *RetryConfig) { cfg.InitialInterval = -time.Second }},
		{name: "negative multiplier", field: "multiplier", set: func(cfg *RetryConfig) { cfg.Multiplier = -1 }},
		{name: "nan multiplier", field: "multiplier", set: func(cfg *RetryConfig) { cfg.Multiplier = math.NaN() }},
		{name: "positive infinity multiplier", field: "multiplier", set: func(cfg *RetryConfig) { cfg.Multiplier = math.Inf(1) }},
		{name: "negative infinity multiplier", field: "multiplier", set: func(cfg *RetryConfig) { cfg.Multiplier = math.Inf(-1) }},
		{name: "negative max interval", field: "maxInterval", set: func(cfg *RetryConfig) { cfg.MaxInterval = -time.Second }},
		{name: "nan jitter", field: "jitter", set: func(cfg *RetryConfig) { cfg.Jitter = math.NaN() }},
		{name: "positive infinity jitter", field: "jitter", set: func(cfg *RetryConfig) { cfg.Jitter = math.Inf(1) }},
		{name: "negative retry tier", field: "tiers", set: func(cfg *RetryConfig) { cfg.Tiers = []time.Duration{-time.Second} }},
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
