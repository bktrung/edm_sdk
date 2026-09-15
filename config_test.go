package f1

import (
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/retry"
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

// TestLoadConfigObservabilityBlockIsStartupError documents that the removed
// observability configuration tree is a strict-decode unknown key, not a
// silently ignored one: an old YAML file that still carries an
// "observability:" block now fails LoadConfig instead of round-tripping.
func TestLoadConfigObservabilityBlockIsStartupError(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, `
f1:
  env: test
  service: orders
  broker:
    driver: inmem
  observability:
    logLevel: info
`)
	_, err := LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "observability") {
		t.Fatalf("LoadConfig() error = %v, want observability unknown-key error", err)
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
    endpoints: [kafka://broker:9092]
    connectTimeout: 4s
    kafka:
      compression: lz4
      sessionTimeout: 60s
      maxExpectedInstances: "3"
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
	if got, want := cfg.Broker.DriverOptions["kafka.maxExpectedInstances"], "3"; got != want {
		t.Fatalf("max expected instances = %q, want %q", got, want)
	}
	if got, want := cfg.Broker.ConnectTimeout, 4*time.Second; got != want {
		t.Fatalf("connect timeout = %s, want %s", got, want)
	}
}

// TestLoadConfigLeavesConsumerDrainTimeoutDisabled pins the compatibility
// posture of ConsumerDrainTimeout: the default fill must not set it, because
// a nonzero default would silently start bounding the runner-drain wait of
// Close for every caller who never asked for a bound. Zero is the documented
// disabled value and the only behavior callers saw before the field existed.
func TestLoadConfigLeavesConsumerDrainTimeoutDisabled(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, `
f1:
  env: test
  service: orders
  broker:
    driver: kafka
    endpoints: [kafka://broker:9092]
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if got := cfg.Lifecycle.ConsumerDrainTimeout; got != 0 {
		t.Fatalf("default consumerDrainTimeout = %s, want zero (disabled): a default here would bound Close for callers who never set it", got)
	}
}

func TestLoadConfigAcceptsRabbitMQManagementPort(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, `
f1:
  env: test
  service: orders
  broker:
    driver: rabbitmq
    endpoints: [amqp://broker:5672/]
    rabbitmq:
      managementPort: 18080
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if got, want := cfg.Broker.DriverOptions["rabbitmq.managementPort"], "18080"; got != want {
		t.Fatalf("management port = %q, want %q", got, want)
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

func TestLoadConfigTrimsAndFiltersBrokerEndpointEnvironmentOverride(t *testing.T) {
	t.Setenv("F1_BROKER_ENDPOINTS", "amqp://a, amqp://b,, ")
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"amqp://a", "amqp://b"}
	if !reflect.DeepEqual(cfg.Broker.Endpoints, want) {
		t.Fatalf("Broker.Endpoints = %#v, want %#v", cfg.Broker.Endpoints, want)
	}
}

func TestLoadConfigRejectsUnsupportedSubscriptionMode(t *testing.T) {
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n  subscriptions:\n    orders:\n      topics: [orders.created]\n      mode: invalid\n")
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "unsupported mode") {
		t.Fatalf("LoadConfig() error = %v, want unsupported mode", err)
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
        weights: {high: 8, medium: 4, low: 1}
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
	if !cfg.Topology.VerifyOnStart || cfg.Codec.MaxBodyBytes != 1024*1024 {
		t.Fatalf("defaults = %#v", cfg)
	}
}

func TestLoadConfigBareKafkaUsesCompatibleDefaults(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: kafka\n    endpoints: [kafka://broker:9092]\n")
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

func TestLoadConfigRejectsExplicitZeroPrefetch(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n  subscriptions:\n    orders:\n      topics: [orders]\n      prefetch: 0\n")
	_, err := LoadConfig(path)
	want := "f1: subscriptions.orders.prefetch 0 must be at least lane count 12 (topics x priorities x (1 + retryTiers))"
	if err == nil || err.Error() != want {
		t.Fatalf("LoadConfig() error = %v, want %q", err, want)
	}
}

func TestLoadConfigRejectsPrefetchAboveCeiling(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, `
f1:
  env: test
  service: orders
  broker:
    driver: inmem
  subscriptions:
    orders:
      topics: [orders]
      prefetch: 65536
`)
	_, err := LoadConfig(path)
	want := "f1: subscriptions.orders.prefetch 65536 must be at most 65535"
	if err == nil || err.Error() != want {
		t.Fatalf("LoadConfig() error = %v, want %q", err, want)
	}
}

func TestLoadConfigAcceptsPrefetchAtCeiling(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, `
f1:
  env: test
  service: orders
  broker:
    driver: inmem
  subscriptions:
    orders:
      topics: [orders]
      prefetch: 65535
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := cfg.Subscriptions["orders"].Prefetch, 65535; got != want {
		t.Fatalf("Prefetch = %d, want %d", got, want)
	}
}

func TestLoadConfigSubscriptionUsesPackagePrefetchFallback(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n  subscriptions:\n    orders:\n      topics: [orders]\n")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := cfg.Subscriptions["orders"].Prefetch, defaultConfig().Broker.DefaultPrefetch; got != want {
		t.Fatalf("Prefetch = %d, want package default %d", got, want)
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
		path := writeConfig(t, "f1:\n  env: prod\n  service: orders\n  broker:\n    driver: rabbitmq\n    endpoints: [amqp://broker:5672/]\n    rabbitmq:\n      queueType: "+queueType+"\n")
		if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "queueType") {
			t.Fatalf("queueType %q: error = %v, want quorum error", queueType, err)
		}
	}
	path := writeConfig(t, "f1:\n  env: prod\n  service: orders\n  broker:\n    driver: rabbitmq\n    endpoints: [amqp://broker:5672/]\n")
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "queueType") {
		t.Fatalf("omitted queueType: error = %v, want quorum error", err)
	}
	path = writeConfig(t, "f1:\n  env: prod\n  service: orders\n  broker:\n    driver: rabbitmq\n    endpoints: [amqp://broker:5672/]\n    rabbitmq:\n      queueType: quorum\n")
	if _, err := LoadConfig(path); err != nil {
		t.Fatalf("quorum queueType error = %v", err)
	}
}

func TestLoadConfigValidatesAgainstConfiguredDriverIdentity(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, `
f1:
  env: test
  service: orders
  broker:
    driver: rabbitmq
    endpoints: [amqp://broker:5672/]
    tls:
      enabled: true
`)
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "amqps://") {
		t.Fatalf("LoadConfig() error = %v, want configured RabbitMQ TLS endpoint validation", err)
	}
}

func TestValidateConfigRequiresEndpointsForNetworkDrivers(t *testing.T) {
	t.Parallel()
	cases := []struct {
		driver  string
		wantErr bool
	}{
		{driver: "kafka", wantErr: true},
		{driver: "rabbitmq", wantErr: true},
		{driver: "inmem", wantErr: false},
	}
	for _, tc := range cases {
		t.Run(tc.driver, func(t *testing.T) {
			cfg := validValidationConfig()
			cfg.Broker.Driver = tc.driver
			err := validateConfiguredConfig(cfg)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "broker.endpoints") {
					t.Fatalf("validateConfig() error = %v, want broker.endpoints error", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateConfig() error = %v, want no endpoint requirement", err)
			}
		})
	}
}

func TestValidateConfigRejectsProductionAliasEnvironments(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		env     string
		wantErr bool
	}{
		{name: "exact production trigger", env: "prod", wantErr: false},
		{name: "production", env: "production", wantErr: true},
		{name: "Production", env: "Production", wantErr: true},
		{name: "PROD", env: "PROD", wantErr: true},
		{name: "Prod", env: "Prod", wantErr: true},
		{name: "prd", env: "prd", wantErr: true},
		{name: "staging", env: "staging", wantErr: false},
		{name: "dev", env: "dev", wantErr: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validValidationConfig()
			cfg.Env = tc.env
			cfg.Broker.Driver = "kafka"
			cfg.Broker.Endpoints = []string{"kafka://broker:9092"}
			err := validateConfiguredConfig(cfg)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), tc.env) || !strings.Contains(err.Error(), "prod") {
					t.Fatalf("validateConfig() error = %v, want offending env and prod", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateConfig() error = %v, want no alias error", err)
			}
		})
	}
}

func TestValidateConfigRequiresTLSForProductionSASL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		env       string
		sasl      string
		tls       bool
		wantError bool
	}{
		{name: "production SASL without TLS", env: "prod", sasl: "plain", wantError: true},
		{name: "production SASL with TLS", env: "prod", sasl: "plain", tls: true, wantError: false},
		{name: "production without SASL", env: "prod", wantError: false},
		{name: "staging SASL without TLS", env: "staging", sasl: "plain", wantError: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validValidationConfig()
			cfg.Env = tc.env
			cfg.Broker.Driver = "kafka"
			cfg.Broker.Endpoints = []string{"kafka://broker:9092"}
			cfg.Broker.SASL.Mechanism = tc.sasl
			cfg.Broker.TLS.Enabled = tc.tls
			err := validateConfiguredConfig(cfg)
			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), "broker.sasl.mechanism") || !strings.Contains(err.Error(), "broker.tls.enabled") {
					t.Fatalf("validateConfig() error = %v, want SASL and TLS keys", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateConfig() error = %v, want no TLS requirement", err)
			}
		})
	}
}

func TestValidateConfigRejectsTLSWithNonAmqpsEndpoint(t *testing.T) {
	t.Parallel()
	cfg := validValidationConfig()
	cfg.Broker.Driver = "rabbitmq"
	cfg.Broker.Endpoints = []string{"amqp://broker:5672/"}
	cfg.Broker.TLS.Enabled = true

	err := validateConfiguredConfig(cfg)
	if err == nil || !strings.Contains(err.Error(), "amqp") || !strings.Contains(err.Error(), "amqps://") {
		t.Fatalf("validateConfig() error = %v, want non-amqps TLS endpoint error", err)
	}
}

func TestValidateConfigAcceptsTLSWithAmqpsEndpoint(t *testing.T) {
	t.Parallel()
	cfg := validValidationConfig()
	cfg.Broker.Driver = "rabbitmq"
	cfg.Broker.Endpoints = []string{"amqps://broker:5671/"}
	cfg.Broker.TLS.Enabled = true

	if err := validateConfiguredConfig(cfg); err != nil {
		t.Fatalf("validateConfig() error = %v, want amqps TLS endpoint to validate", err)
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
		{"kafka too long", "f1:\n  env: test\n  service: orders\n  broker:\n    driver: kafka\n    endpoints: [kafka://broker:9092]\n  lifecycle:\n    rebalanceDrainTimeout: 30s\n", "rebalanceDrainTimeout"},
		{"kafka malformed", "f1:\n  env: test\n  service: orders\n  broker:\n    driver: kafka\n    endpoints: [kafka://broker:9092]\n    kafka:\n      sessionTimeout: garbage\n", "sessionTimeout"},
		{"rabbit malformed", "f1:\n  env: test\n  service: orders\n  broker:\n    driver: rabbitmq\n    endpoints: [amqp://broker:5672/]\n    rabbitmq:\n      consumerTimeout: garbage\n", "consumerTimeout"},
		{"rabbit too short", "f1:\n  env: test\n  service: orders\n  broker:\n    driver: rabbitmq\n    endpoints: [amqp://broker:5672/]\n    rabbitmq:\n      consumerTimeout: 10s\n  subscriptions:\n    orders:\n      topics: [orders]\n", "consumerTimeout"},
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
			if err := validateConfiguredConfig(cfg); err == nil || !strings.Contains(err.Error(), test.field) {
				t.Fatalf("validateConfig() error = %v, want %s validation", err, test.field)
			}
		})
	}
}

func TestValidateConfigRejectsInvalidSubscriptionModeAndPolicy(t *testing.T) {
	cfg := validValidationConfig()
	sub := cfg.Subscriptions["orders"]
	sub.Mode = Mode(99)
	if err := validateConfiguredConfig(func() Config {
		copy := cfg
		copy.Subscriptions = map[string]SubscriptionConfig{"orders": sub}
		return copy
	}()); err == nil || !strings.Contains(err.Error(), "subscriptions.orders.mode") {
		t.Fatalf("validateConfiguredConfig() mode error = %v, want unsupported mode", err)
	}
	sub = cfg.Subscriptions["orders"]
	sub.UnmatchedPolicy = UnmatchedPolicy(99)
	cfg.Subscriptions["orders"] = sub
	if err := validateConfiguredConfig(cfg); err == nil || !strings.Contains(err.Error(), "subscriptions.orders.unmatchedPolicy") {
		t.Fatalf("validateConfiguredConfig() unmatched policy error = %v, want unsupported policy", err)
	}
}

func TestValidateSubscriptionRejectsInvalidRetryValues(t *testing.T) {
	cfg := validValidationConfig()
	for _, test := range invalidRetryValueCases() {
		t.Run(test.name, func(t *testing.T) {
			sub := cfg.Subscriptions["orders"]
			test.set(&sub.Retry)
			if err := validateSubscription(cfg, cfg.Broker.Driver, "orders", sub); err == nil || !strings.Contains(err.Error(), test.field) {
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
		{name: "consumer drain timeout", set: func(cfg *LifecycleConfig) { cfg.ConsumerDrainTimeout = -time.Second }},
		{name: "close timeout", set: func(cfg *LifecycleConfig) { cfg.CloseTimeout = -time.Second }},
		{name: "rebalance drain timeout", set: func(cfg *LifecycleConfig) { cfg.RebalanceDrainTimeout = -time.Second }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			cfg := validValidationConfig()
			cfg.Subscriptions = nil
			test.set(&cfg.Lifecycle)
			if err := validateConfiguredConfig(cfg); err == nil || !strings.Contains(err.Error(), "lifecycle") {
				t.Fatalf("validateConfig() error = %v, want lifecycle validation", err)
			}
		})
	}
}

func TestRetryConfigDelayForAgreesWithInternalRetryLadder(t *testing.T) {
	cases := []struct {
		name    string
		cfg     RetryConfig
		attempt int
	}{
		{name: "negative initial interval", cfg: RetryConfig{InitialInterval: -time.Second, Multiplier: 2}, attempt: 3},
		{name: "zero initial interval", cfg: RetryConfig{Multiplier: 2}, attempt: 2},
		{name: "zero multiplier", cfg: RetryConfig{InitialInterval: time.Second}, attempt: 3},
		{name: "explicit tiers", cfg: RetryConfig{Tiers: []time.Duration{time.Second, 3 * time.Second, 9 * time.Second}}, attempt: 2},
		{name: "attempt beyond tier count", cfg: RetryConfig{Tiers: []time.Duration{time.Second, 3 * time.Second}}, attempt: 5},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			got := test.cfg.DelayFor(test.attempt)
			internalCfg := retry.Config{
				MaxAttempts:     test.cfg.MaxAttempts,
				InitialInterval: test.cfg.InitialInterval,
				Multiplier:      test.cfg.Multiplier,
				MaxInterval:     test.cfg.MaxInterval,
				Jitter:          test.cfg.Jitter,
				Tiers:           test.cfg.Tiers,
			}
			want := internalCfg.DelayFor(test.attempt)
			if got != want {
				t.Fatalf("RetryConfig.DelayFor(%d) = %v, internal/retry.Config.DelayFor(%d) = %v; want agreement", test.attempt, got, test.attempt, want)
			}
		})
	}
}

func TestValidateConfigRejectsZeroDrainTimeout(t *testing.T) {
	cfg := validValidationConfig()
	cfg.Subscriptions = nil
	cfg.Lifecycle.DrainTimeout = 0
	if err := validateConfiguredConfig(cfg); err == nil || !strings.Contains(err.Error(), "drainTimeout") {
		t.Fatalf("validateConfig() error = %v, want drainTimeout validation", err)
	}
}

func TestValidateRetryMatchesDelayForSafety(t *testing.T) {
	cases := []struct {
		name     string
		retry    RetryConfig
		attempt  int
		wantZero bool
		wantErr  bool
	}{
		{
			name:     "explicit growing multiplier",
			retry:    RetryConfig{MaxAttempts: 20, InitialInterval: time.Second, Multiplier: 5},
			attempt:  16,
			wantZero: true,
			wantErr:  true,
		},
		{
			name:     "default multiplier",
			retry:    RetryConfig{MaxAttempts: 20, InitialInterval: time.Second},
			attempt:  16,
			wantZero: true,
			wantErr:  true,
		},
		{
			name:     "default initial interval",
			retry:    RetryConfig{MaxAttempts: 20, Multiplier: 5},
			attempt:  16,
			wantZero: true,
			wantErr:  true,
		},
		{
			name:     "large initial interval",
			retry:    RetryConfig{MaxAttempts: 20, InitialInterval: 1 << 62, Multiplier: 2},
			attempt:  2,
			wantZero: true,
			wantErr:  true,
		},
		{
			name:     "shrinking nanosecond interval",
			retry:    RetryConfig{MaxAttempts: 20, InitialInterval: 100 * time.Nanosecond, Multiplier: .5},
			attempt:  19,
			wantZero: true,
			wantErr:  true,
		},
		{
			name:     "shrinking microsecond interval",
			retry:    RetryConfig{MaxAttempts: 20, InitialInterval: time.Microsecond, Multiplier: .1},
			attempt:  19,
			wantZero: true,
			wantErr:  true,
		},
		{
			name:     "shrinking interval with positive cap",
			retry:    RetryConfig{MaxAttempts: 20, InitialInterval: 100 * time.Nanosecond, Multiplier: .5, MaxInterval: time.Second},
			attempt:  8,
			wantZero: true,
			wantErr:  true,
		},
		{
			name:     "default retry config",
			retry:    defaultSubscription().Retry,
			attempt:  20,
			wantZero: false,
			wantErr:  false,
		},
		{
			name:     "implicit defaults before overflow",
			retry:    RetryConfig{MaxAttempts: 4},
			attempt:  3,
			wantZero: false,
			wantErr:  false,
		},
		{
			name:     "explicit tiers without max interval",
			retry:    RetryConfig{MaxAttempts: 20, Multiplier: 5, Tiers: []time.Duration{time.Second, 2 * time.Second}},
			attempt:  16,
			wantZero: false,
			wantErr:  false,
		},
		{
			name:     "no retries",
			retry:    RetryConfig{MaxAttempts: 1, Multiplier: 5},
			attempt:  1,
			wantZero: false,
			wantErr:  false,
		},
		{
			name:     "decaying multiplier",
			retry:    RetryConfig{MaxAttempts: 20, InitialInterval: time.Second, Multiplier: .5},
			attempt:  20,
			wantZero: false,
			wantErr:  false,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := test.retry.DelayFor(test.attempt); (got == 0) != test.wantZero {
				t.Fatalf("DelayFor(%d) = %s, want zero=%v", test.attempt, got, test.wantZero)
			}
			cfg := validValidationConfig()
			cfg.Subscriptions["orders"] = withRetry(cfg.Subscriptions["orders"], test.retry)
			err := validateConfiguredConfig(cfg)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateConfig() error = %v, want error=%v", err, test.wantErr)
			}
		})
	}
}

func TestValidateSubscriptionRejectsZeroDrainTimeout(t *testing.T) {
	cfg := validValidationConfig()
	cfg.Lifecycle.DrainTimeout = 0
	sub := cfg.Subscriptions["orders"]
	if err := validateSubscription(cfg, cfg.Broker.Driver, "orders", sub); err == nil || !strings.Contains(err.Error(), "drainTimeout") {
		t.Fatalf("validateSubscription() error = %v, want drainTimeout validation", err)
	}
}

func validateConfiguredConfig(cfg Config) error {
	return validateConfig(cfg, cfg.Broker.Driver)
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
			Priorities:     []Priority{PriorityMedium},
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

func TestLoadConfigRejectsRemovedFlushTimeout(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n  lifecycle:\n    flushTimeout: 20s\n")
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "flushTimeout") {
		t.Fatalf("LoadConfig() error = %v, want flushTimeout rejection", err)
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

func TestValidateConfigRejectsInvalidPriorityLists(t *testing.T) {
	tests := []struct {
		name string
		set  func(*Config)
		want string
	}{
		{name: "empty topology", set: func(cfg *Config) { cfg.Topology.Priorities = nil }, want: "topology.priorities"},
		{name: "invalid topology", set: func(cfg *Config) { cfg.Topology.Priorities = []Priority{Priority(99)} }, want: "topology.priorities"},
		{name: "duplicate topology", set: func(cfg *Config) { cfg.Topology.Priorities = []Priority{PriorityHigh, PriorityHigh} }, want: "topology.priorities"},
		{name: "empty subscription", set: func(cfg *Config) { cfg.Subscriptions["orders"] = withPriorities(cfg.Subscriptions["orders"], nil) }, want: "subscriptions.orders.priorities"},
		{name: "invalid subscription", set: func(cfg *Config) {
			cfg.Subscriptions["orders"] = withPriorities(cfg.Subscriptions["orders"], []Priority{Priority(99)})
		}, want: "subscriptions.orders.priorities"},
		{name: "duplicate subscription", set: func(cfg *Config) {
			cfg.Subscriptions["orders"] = withPriorities(cfg.Subscriptions["orders"], []Priority{PriorityMedium, PriorityMedium})
		}, want: "subscriptions.orders.priorities"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validValidationConfig()
			test.set(&cfg)
			if err := validateConfiguredConfig(cfg); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateConfig() error = %v, want %s validation", err, test.want)
			}
		})
	}
}

func withPriorities(sub SubscriptionConfig, priorities []Priority) SubscriptionConfig {
	sub.Priorities = priorities
	return sub
}

func TestNormalizeConfigAcceptsMinimalHandBuiltConfig(t *testing.T) {
	t.Parallel()
	cfg := normalizeConfig(Config{
		Env:     "test",
		Service: "orders",
		Broker:  BrokerConfig{Driver: "inmem"},
		Subscriptions: map[string]SubscriptionConfig{
			"orders": {Topics: []string{"orders.created"}},
		},
	})
	if err := validateConfiguredConfig(cfg); err != nil {
		t.Fatalf("validateConfiguredConfig() after normalization: %v", err)
	}
	defaults := defaultConfig()
	if cfg.Codec.ContentMode != defaults.Codec.ContentMode || cfg.Codec.MaxHeaderBytes != defaults.Codec.MaxHeaderBytes || cfg.Codec.MaxBodyBytes != defaults.Codec.MaxBodyBytes {
		t.Fatalf("codec defaults = %#v, want %#v", cfg.Codec, defaults.Codec)
	}
	if !reflect.DeepEqual(cfg.Topology.Priorities, defaults.Topology.Priorities) || cfg.Lifecycle != defaults.Lifecycle {
		t.Fatalf("topology/lifecycle defaults = %#v/%#v", cfg.Topology, cfg.Lifecycle)
	}
	sub := cfg.Subscriptions["orders"]
	subDefaults := defaultSubscription()
	if sub.Concurrency != subDefaults.Concurrency || sub.Prefetch != defaults.Broker.DefaultPrefetch || !reflect.DeepEqual(sub.Priorities, subDefaults.Priorities) || sub.Retry.MaxAttempts != subDefaults.Retry.MaxAttempts || sub.HandlerTimeout != subDefaults.HandlerTimeout {
		t.Fatalf("subscription defaults = %#v, want concurrency=%d prefetch=%d priorities=%v maxAttempts=%d handlerTimeout=%s", sub, subDefaults.Concurrency, defaults.Broker.DefaultPrefetch, subDefaults.Priorities, subDefaults.Retry.MaxAttempts, subDefaults.HandlerTimeout)
	}
}

func TestValidateConfigRejectsInvalidMaxHeaderBytes(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		value int
	}{
		{name: "zero", value: 0},
		{name: "negative", value: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := validValidationConfig()
			cfg.Codec.MaxHeaderBytes = test.value
			err := validateConfiguredConfig(cfg)
			if err == nil || err.Error() != "f1: codec.maxHeaderBytes must be positive" {
				t.Fatalf("validateConfig() error = %v, want positive maxHeaderBytes error", err)
			}
		})
	}
}

func TestNormalizeConfigUsesDefaultForGoBuiltZeroPrefetch(t *testing.T) {
	t.Parallel()
	cfg := normalizeConfig(Config{
		Broker: BrokerConfig{DefaultPrefetch: 128},
		Subscriptions: map[string]SubscriptionConfig{
			"orders": {Prefetch: 0},
		},
	})
	if got, want := cfg.Subscriptions["orders"].Prefetch, 128; got != want {
		t.Fatalf("Prefetch = %d, want broker default %d", got, want)
	}
}

func TestNormalizeConfigPreservesVerifyOnStartFalse(t *testing.T) {
	cfg := normalizeConfig(Config{Topology: TopologyConfig{VerifyOnStart: false}})
	if cfg.Topology.VerifyOnStart {
		t.Fatal("VerifyOnStart = true after normalization, want false")
	}
}

func TestLoadConfigNormalizationIsNoOp(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n  subscriptions:\n    orders:\n      topics: [orders.created]\n")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if normalized := normalizeConfig(cfg); !reflect.DeepEqual(normalized, cfg) {
		t.Fatalf("normalization changed LoadConfig result: %#v -> %#v", cfg, normalized)
	}
}
