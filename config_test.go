package f1

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/dispatch"
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
	if cfg.Env != "test" || cfg.Service != "orders" || cfg.Broker.Driver != "kafka" {
		t.Fatalf("LoadConfig() = %#v, want the file's environment, service, and broker", cfg)
	}
	if got := cfg.Broker.Endpoints; !reflect.DeepEqual(got, []string{"kafka://broker:9092"}) {
		t.Fatalf("Broker.Endpoints = %#v, want the configured endpoint", got)
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

func TestLoadConfigRejectsOrderedBufferAboveBound(t *testing.T) {
	t.Parallel()
	const concurrency = 1024
	prefetch := dispatch.MaxOrderedBufferEntries/concurrency + 1
	path := writeConfig(t, fmt.Sprintf(`
f1:
  env: test
  service: orders
  broker:
    driver: inmem
  subscriptions:
    orders:
      topics: [orders]
      mode: orderedByKey
      concurrency: %d
      prefetch: %d
`, concurrency, prefetch))
	_, err := LoadConfig(path)
	want := fmt.Sprintf("f1: subscriptions.orders: ordered mode needs concurrency x prefetch at most %d, got %d x %d", dispatch.MaxOrderedBufferEntries, concurrency, prefetch)
	if err == nil || err.Error() != want {
		t.Fatalf("LoadConfig() error = %v, want %q", err, want)
	}
}

func TestLoadConfigAcceptsOrderedBufferAtBound(t *testing.T) {
	t.Parallel()
	const concurrency = 1024
	prefetch := dispatch.MaxOrderedBufferEntries / concurrency
	path := writeConfig(t, fmt.Sprintf(`
f1:
  env: test
  service: orders
  broker:
    driver: inmem
  subscriptions:
    orders:
      topics: [orders]
      mode: orderedByKey
      concurrency: %d
      prefetch: %d
`, concurrency, prefetch))
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := cfg.Subscriptions["orders"].Prefetch, prefetch; got != want {
		t.Fatalf("Prefetch = %d, want %d", got, want)
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

func TestLoadConfigRejectsUnknownFairnessPriority(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n  subscriptions:\n    orders:\n      topics: [orders]\n      fairness:\n        weights: {bogus: 99}\n")
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "fairness.weights.bogus") {
		t.Fatalf("LoadConfig() error = %v, want invalid priority error", err)
	}
}

func TestLoadConfigRejectsUnknownFairnessKey(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n  subscriptions:\n    orders:\n      topics: [orders]\n      fairness:\n        deadlinePromotionEnbaled: false\n")
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "deadlinePromotionEnbaled") {
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
		{name: "production with a region suffix", env: "prod/us", wantErr: true},
		{name: "production with a dotted suffix", env: "prod.us", wantErr: true},
		{name: "hyphenated", env: "staging-eu", wantErr: false},
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
	rejected := cfg
	rejected.Broker.Endpoints = []string{"amqp://broker:5672/"}
	if err := validateConfiguredConfig(rejected); err == nil || !strings.Contains(err.Error(), "amqps://") {
		t.Fatalf("validateConfig() error = %v, want non-amqps TLS endpoint error", err)
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

func TestValidateConfigRejectsDrainBeyondKafkaRebalanceTimeout(t *testing.T) {
	t.Parallel()
	cfg := validValidationConfig()
	cfg.Broker.Driver = "kafka"
	cfg.Broker.Endpoints = []string{"kafka://broker:9092"}
	cfg.Broker.DriverOptions = map[string]string{"kafka.rebalanceTimeout": "10s"}
	cfg.Lifecycle.RebalanceDrainTimeout = 25 * time.Second

	err := validateConfiguredConfig(cfg)
	if err == nil {
		t.Fatal("validateConfiguredConfig() error = nil, want kafka.rebalanceTimeout validation")
	}
	for _, want := range []string{"kafka.rebalanceTimeout", "10s", "45s", "25s"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("validateConfiguredConfig() error = %v, want %q", err, want)
		}
	}
}

func TestValidateConfigAcceptsKafkaDefaultTimeoutBounds(t *testing.T) {
	t.Parallel()
	cfg := validValidationConfig()
	cfg.Broker.Driver = "kafka"
	cfg.Broker.Endpoints = []string{"kafka://broker:9092"}
	cfg.Lifecycle.RebalanceDrainTimeout = 27 * time.Second

	if err := validateConfiguredConfig(cfg); err != nil {
		t.Fatalf("validateConfiguredConfig() error = %v, want default Kafka timeout bounds to validate", err)
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

func TestValidateSubscriptionRejectsNamesThatSplitADestinationSegment(t *testing.T) {
	cfg := validValidationConfig()
	sub := cfg.Subscriptions["orders"]
	// "orders.created" + "worker" and "orders" + "created.worker" would name
	// the same queue if a dot were allowed in the subscription name.
	for _, name := range []string{"created.worker", "billing/worker", "billing worker"} {
		if err := validateSubscription(cfg, cfg.Broker.Driver, name, sub); err == nil || !strings.Contains(err.Error(), name) {
			t.Fatalf("validateSubscription(%q) error = %v, want the name refused", name, err)
		}
	}
	for _, name := range []string{"billing-worker", "billing_worker", "Worker2"} {
		if err := validateSubscription(cfg, cfg.Broker.Driver, name, sub); err != nil {
			t.Fatalf("validateSubscription(%q) error = %v, want nil", name, err)
		}
	}
}

func TestConfigAndSubscribeRejectTheSameSubscriptions(t *testing.T) {
	cases := []struct {
		name, want string
		set        func(*SubscriptionConfig)
	}{
		{"duplicate topic", "name the same topic", func(sub *SubscriptionConfig) { sub.Topics = []string{"orders", "orders"} }},
		{"versioned duplicate topic", "name the same topic", func(sub *SubscriptionConfig) { sub.Topics = []string{"orders", "orders.v2"} }},
		{"negative budget", "fairness.budgets", func(sub *SubscriptionConfig) {
			sub.Fairness.Budgets = map[Priority]time.Duration{PriorityMedium: -time.Second}
		}},
		{"negative retry weight divisor", "retryWeightDivisor", func(sub *SubscriptionConfig) { sub.Fairness.RetryWeightDivisor = -1 }},
		{"negative prefetch factor", "prefetchFactor", func(sub *SubscriptionConfig) { sub.Fairness.PrefetchFactor = -1 }},
		{"overflowing automatic prefetch factor", "prefetch 65536", func(sub *SubscriptionConfig) {
			sub.Prefetch = 0
			sub.Fairness.PrefetchFactor = int(^uint(0) >> 1)
		}},
		{"ordered buffer too large", "ordered mode", func(sub *SubscriptionConfig) {
			sub.Mode = OrderedByKey
			sub.Concurrency = 1024
			sub.Prefetch = maxPrefetch
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			cfg := validValidationConfig()
			sub := cfg.Subscriptions["orders"]
			test.set(&sub)
			cfg.Subscriptions["orders"] = sub
			if err := validateConfig(cfg, cfg.Broker.Driver); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateConfig() error = %v, want %s", err, test.want)
			}
			if err := validateSubscription(cfg, cfg.Broker.Driver, "orders", sub); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateSubscription() error = %v, want %s", err, test.want)
			}
		})
	}
}

func TestRabbitMQConsumerTimeoutValidation(t *testing.T) {
	cases := []struct {
		name            string
		handlerTimeout  time.Duration
		consumerTimeout time.Duration
		wantErr         bool
	}{
		{"overflow", 1 << 62, 90 * time.Second, true},
		{"exact boundary", time.Second + time.Nanosecond, 3*time.Second + 3*time.Nanosecond, false},
		{"one nanosecond below boundary", time.Second + time.Nanosecond, 3*time.Second + 2*time.Nanosecond, true},
		{"negative consumer timeout", time.Second, -time.Nanosecond, true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			cfg := validValidationConfig()
			cfg.Broker.Driver = "rabbitmq"
			cfg.Broker.Endpoints = []string{"amqp://broker:5672/"}
			cfg.Broker.DriverOptions = map[string]string{
				"rabbitmq.consumerTimeout": test.consumerTimeout.String(),
			}
			cfg.Lifecycle.DrainTimeout = test.handlerTimeout + time.Nanosecond
			sub := cfg.Subscriptions["orders"]
			sub.HandlerTimeout = test.handlerTimeout
			cfg.Subscriptions["orders"] = sub
			for _, validate := range []struct {
				name string
				run  func() error
			}{
				{"config", func() error { return validateConfiguredConfig(cfg) }},
				{"subscription", func() error { return validateSubscription(cfg, cfg.Broker.Driver, "orders", sub) }},
			} {
				t.Run(validate.name, func(t *testing.T) {
					err := validate.run()
					if !test.wantErr {
						if err != nil {
							t.Fatalf("validation error = %v, want nil", err)
						}
						return
					}
					const want = "f1: broker.rabbitmq.consumerTimeout must be at least subscriptions.orders.handlerTimeout x 3"
					if err == nil || err.Error() != want {
						t.Fatalf("validation error = %v, want %q", err, want)
					}
				})
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

// TestRetryConfigDelayForAppliesDocumentedDefaults pins the public DelayFor
// contract with literal delays: a non-positive InitialInterval becomes one
// second, a zero Multiplier becomes five, and Tiers select by attempt and hold
// at the last entry.
func TestRetryConfigDelayForAppliesDocumentedDefaults(t *testing.T) {
	cases := []struct {
		name    string
		cfg     RetryConfig
		attempt int
		want    time.Duration
	}{
		{name: "negative initial interval", cfg: RetryConfig{InitialInterval: -time.Second, Multiplier: 2}, attempt: 3, want: 4 * time.Second},
		{name: "zero initial interval", cfg: RetryConfig{Multiplier: 2}, attempt: 2, want: 2 * time.Second},
		{name: "zero multiplier", cfg: RetryConfig{InitialInterval: time.Second}, attempt: 3, want: 25 * time.Second},
		{name: "explicit tiers", cfg: RetryConfig{Tiers: []time.Duration{time.Second, 3 * time.Second, 9 * time.Second}}, attempt: 2, want: 3 * time.Second},
		{name: "attempt beyond tier count", cfg: RetryConfig{Tiers: []time.Duration{time.Second, 3 * time.Second}}, attempt: 5, want: 3 * time.Second},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := test.cfg.DelayFor(test.attempt); got != test.want {
				t.Fatalf("RetryConfig.DelayFor(%d) = %v, want %v", test.attempt, got, test.want)
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

func TestAutomaticSubscriptionPrefetchUsesResolvedLaneCapacitySum(t *testing.T) {
	sub := validValidationConfig().Subscriptions["orders"]
	sub.Prefetch = 0
	if got, want := automaticSubscriptionPrefetch(sub), 12; got != want {
		t.Fatalf("automaticSubscriptionPrefetch() = %d, want %d", got, want)
	}
}

// TestAutomaticSubscriptionPrefetchKeepsWeightedSharesExact pins the lane
// share computed before any clipping. Each row's product of concurrency and
// weight, or its total weight, passes the prefetch bound, which is where
// clipping the intermediate values changed a valid answer: the share was
// computed from the clipped product or divided by the clipped total.
func TestAutomaticSubscriptionPrefetchKeepsWeightedSharesExact(t *testing.T) {
	for _, test := range []struct {
		name        string
		concurrency int
		weights     map[Priority]int
		priorities  []Priority
		want        int
	}{
		{
			// ceil(256 x 1024 / 1024) x 2; clipped first it was 64 x 2.
			name:        "product of concurrency and weight past the bound",
			concurrency: 256,
			weights:     map[Priority]int{PriorityMedium: 1024},
			priorities:  []Priority{PriorityMedium},
			want:        512,
		},
		{
			// Two lanes of ceil(1024 x 40000 / 80000) x 2; a total clipped to
			// the bound made each share the minimum.
			name:        "total weight past the bound",
			concurrency: 1024,
			weights:     map[Priority]int{PriorityHigh: 40000, PriorityLow: 40000},
			priorities:  []Priority{PriorityHigh, PriorityLow},
			want:        2048,
		},
		{
			// ceil(80/13) + ceil(40/13) + max(ceil(10/13), 3), times 2.
			name:        "uneven weights round each share up",
			concurrency: 10,
			weights:     map[Priority]int{PriorityHigh: 8, PriorityMedium: 4, PriorityLow: 1},
			priorities:  []Priority{PriorityHigh, PriorityMedium, PriorityLow},
			want:        28,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sub := validValidationConfig().Subscriptions["orders"]
			sub.Prefetch = 0
			sub.Retry = RetryConfig{MaxAttempts: 1}
			sub.Concurrency = test.concurrency
			sub.Priorities = test.priorities
			sub.Fairness.Weights = test.weights
			sub.Fairness.PrefetchFactor = 2
			if got := automaticSubscriptionPrefetch(sub); got != test.want {
				t.Fatalf("automaticSubscriptionPrefetch() = %d, want %d", got, test.want)
			}
		})
	}
}

// TestFairnessWeightIsBounded pins the bound that keeps the weighted share
// exact: the largest weight is accepted, and one past it is refused with the
// range in the message.
func TestFairnessWeightIsBounded(t *testing.T) {
	for _, test := range []struct {
		weight  int
		wantErr bool
	}{
		{weight: maxFairnessWeight},
		{weight: maxFairnessWeight + 1, wantErr: true},
	} {
		cfg := validValidationConfig()
		sub := cfg.Subscriptions["orders"]
		sub.Fairness.Weights = map[Priority]int{PriorityMedium: test.weight}
		cfg.Subscriptions["orders"] = sub
		err := validateConfiguredConfig(cfg)
		if test.wantErr != (err != nil) {
			t.Fatalf("weight %d: validate = %v, want error %t", test.weight, err, test.wantErr)
		}
		if test.wantErr && !strings.Contains(err.Error(), "fairness.weights.medium must be between 1 and 65535") {
			t.Fatalf("weight %d: error = %v, want the allowed range named", test.weight, err)
		}
	}
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
		{name: "negative retry tier", field: "tiers", set: func(cfg *RetryConfig) { cfg.Tiers = []time.Duration{-time.Second} }},
		{name: "retry tier past the longest delay", field: "longest retry delay", set: func(cfg *RetryConfig) {
			cfg.Tiers = []time.Duration{maxRetryDelay + time.Millisecond}
		}},
		{name: "backoff past the longest delay", field: "longest retry delay", set: func(cfg *RetryConfig) {
			cfg.MaxAttempts = 3
			cfg.Tiers = nil
			cfg.InitialInterval = 30 * 24 * time.Hour
			cfg.Multiplier = 1
			cfg.MaxInterval = 0
		}},
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

func TestLoadConfigRejectsRemovedKeys(t *testing.T) {
	t.Parallel()
	const subscription = "  subscriptions:\n    orders:\n      topics: [orders]\n"
	for _, test := range []struct {
		name string
		key  string
		body string
	}{
		{name: "pre stop delay", key: "preStopDelay", body: "  lifecycle:\n    preStopDelay: 5s\n"},
		{name: "cost model", key: "costModel", body: subscription + "      fairness:\n        costModel: count\n"},
		{name: "retry jitter", key: "jitter", body: subscription + "      retry:\n        jitter: 0.2\n"},
		{name: "aging enabled", key: "agingEnabled", body: subscription + "      fairness:\n        agingEnabled: false\n"},
		{name: "deadline promotion", key: "disableAging", body: subscription + "      fairness:\n        disableAging: false\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: inmem\n"+test.body)
			if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), test.key) {
				t.Fatalf("LoadConfig() error = %v, want rejection naming %s", err, test.key)
			}
		})
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
	if cfg.Codec.MaxHeaderBytes != defaults.Codec.MaxHeaderBytes || cfg.Codec.MaxBodyBytes != defaults.Codec.MaxBodyBytes {
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
	if got, want := cfg.Codec.Default, "json"; got != want {
		t.Fatalf("Codec.Default = %q, want %q", got, want)
	}
	if got, want := cfg.Lifecycle.DrainTimeout, time.Minute; got != want {
		t.Fatalf("DrainTimeout = %s, want %s", got, want)
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
