package f1

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/retry"
)

// Config is the fully resolved configuration used to construct a Client.
// LoadConfig validates YAML and environment values before returning it.
type Config struct {
	Env        string `yaml:"env"`
	Service    string `yaml:"service"`
	InstanceID string `yaml:"instanceId"`

	Broker        BrokerConfig                  `yaml:"broker"`
	Topology      TopologyConfig                `yaml:"topology"`
	Codec         CodecConfig                   `yaml:"codec"`
	Lifecycle     LifecycleConfig               `yaml:"lifecycle"`
	Observability ObservabilityConfig           `yaml:"observability"`
	Subscriptions map[string]SubscriptionConfig `yaml:"subscriptions"`
}

// TopologyConfig configures the destinations a Client verifies or creates.
type TopologyConfig struct {
	AutoCreate        bool          `yaml:"autoCreate"`
	VerifyOnStart     bool          `yaml:"verifyOnStart"`
	PartitionsDefault int           `yaml:"partitionsDefault"`
	RetentionDefault  time.Duration `yaml:"retentionDefault"`
	DLQRetention      time.Duration `yaml:"dlqRetention"`
	Priorities        []Priority    `yaml:"priorities"`
}

// CodecConfig configures the codec selection and envelope size limits.
type CodecConfig struct {
	Default        string `yaml:"default"`
	ContentMode    string `yaml:"contentMode"`
	MaxHeaderBytes int    `yaml:"maxHeaderBytes"`
	MaxBodyBytes   int    `yaml:"maxBodyBytes"`
}

// LifecycleConfig configures the timing of graceful client shutdown. Zero
// disables the corresponding delay or deadline; negative values are invalid.
// DrainTimeout is the exception: it must be positive, since a disabled drain
// deadline means shutdown never completes.
type LifecycleConfig struct {
	PreStopDelay          time.Duration `yaml:"preStopDelay"`
	DrainTimeout          time.Duration `yaml:"drainTimeout"`
	HandlerGrace          time.Duration `yaml:"handlerGrace"`
	FlushTimeout          time.Duration `yaml:"flushTimeout"`
	CloseTimeout          time.Duration `yaml:"closeTimeout"`
	RebalanceDrainTimeout time.Duration `yaml:"rebalanceDrainTimeout"`
}

// ObservabilityConfig configures the client logs, metrics, and tracing.
type ObservabilityConfig struct {
	LogLevel    string            `yaml:"logLevel"`
	LogFormat   string            `yaml:"logFormat"`
	LogSampling LogSamplingConfig `yaml:"logSampling"`
	Metrics     MetricsConfig     `yaml:"metrics"`
	Tracing     TracingConfig     `yaml:"tracing"`
}

// LogSamplingConfig bounds the log volume emitted during a failure storm.
type LogSamplingConfig struct {
	Enabled    bool `yaml:"enabled"`
	Burst      int  `yaml:"burst"`
	Thereafter int  `yaml:"thereafter"`
}

// MetricsConfig configures whether the client emits metrics and their schema mode.
type MetricsConfig struct {
	Enabled      bool   `yaml:"enabled"`
	SemconvOptIn string `yaml:"semconvOptIn"`
}

// TracingConfig configures whether the client emits traces and their sampling.
type TracingConfig struct {
	Enabled              bool    `yaml:"enabled"`
	SampleRatio          float64 `yaml:"sampleRatio"`
	AlwaysSampleRetries  bool    `yaml:"alwaysSampleRetries"`
	AlwaysSampleFailures bool    `yaml:"alwaysSampleFailures"`
}

// Mode selects whether a Subscription preserves per-key delivery order.
type Mode int

const (
	// Unordered is the default delivery mode.
	Unordered Mode = iota
	// OrderedByKey requests per-key delivery order.
	OrderedByKey
)

// UnmatchedPolicy selects the action for an event with no matching handler.
type UnmatchedPolicy int

const (
	// Ignore acknowledges unmatched events without retaining a copy.
	Ignore UnmatchedPolicy = iota
	// DeadLetter retains unmatched events in the dead-letter destination.
	DeadLetter
)

// FairnessConfig configures fairness among a Subscription's delivery lanes.
type FairnessConfig struct {
	Weights            map[Priority]int
	Budgets            map[Priority]time.Duration
	RetryWeightDivisor int
	CostModel          string
	PrefetchFactor     int
	AgingEnabled       bool
}

// RetryConfig configures a Subscription's retry ladder.
type RetryConfig struct {
	MaxAttempts     int
	InitialInterval time.Duration
	Multiplier      float64
	MaxInterval     time.Duration
	Jitter          float64
	Tiers           []time.Duration
}

// DelayFor returns the retry delay for a one-based retry number. It
// delegates to internal/retry so the two consumers of this ladder - the
// message due-time calculation and the retry destination's queue TTL - never
// disagree.
func (r RetryConfig) DelayFor(attempt int) time.Duration {
	return retry.Config{
		MaxAttempts:     r.MaxAttempts,
		InitialInterval: r.InitialInterval,
		Multiplier:      r.Multiplier,
		MaxInterval:     r.MaxInterval,
		Jitter:          r.Jitter,
		Tiers:           r.Tiers,
	}.DelayFor(attempt)
}

// SubscriptionConfig carries every Subscription field that YAML can set.
type SubscriptionConfig struct {
	Topics          []string        `yaml:"topics"`
	Mode            Mode            `yaml:"mode"`
	Concurrency     int             `yaml:"concurrency"`
	Prefetch        int             `yaml:"prefetch"`
	Priorities      []Priority      `yaml:"priorities"`
	Fairness        FairnessConfig  `yaml:"fairness"`
	Retry           RetryConfig     `yaml:"retry"`
	HandlerTimeout  time.Duration   `yaml:"handlerTimeout"`
	UnmatchedPolicy UnmatchedPolicy `yaml:"unmatchedPolicy"`

	presence subscriptionPresence
}

// subscriptionPresence records YAML keys whose explicit zero value must win
// over a documented default during the later Go subscription merge.
type subscriptionPresence struct {
	Topics, Mode, Concurrency, Prefetch, Priorities  bool
	Fairness, Retry, HandlerTimeout, UnmatchedPolicy bool
}

// LoadConfig reads path, applies environment overrides, fills defaults, and
// validates the result. An empty path uses environment variables and defaults.
func LoadConfig(path string) (Config, error) {
	cfg := defaultConfig()
	if path != "" {
		data, err := os.ReadFile(path) // #nosec G304 -- LoadConfig intentionally reads its caller-selected configuration file.
		if err != nil {
			return Config{}, fmt.Errorf("f1: read config %q: %w", path, err)
		}
		if err := decodeConfig(data, &cfg); err != nil {
			return Config{}, err
		}
	}
	applyEnvironment(&cfg)
	if err := validateConfig(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func defaultConfig() Config {
	return Config{
		Broker:        BrokerConfig{ConnectTimeout: 30 * time.Second, DefaultPrefetch: 64},
		Topology:      TopologyConfig{VerifyOnStart: true, PartitionsDefault: 12, RetentionDefault: 7 * 24 * time.Hour, DLQRetention: 30 * 24 * time.Hour, Priorities: []Priority{PriorityHigh, PriorityNormal, PriorityLow}},
		Codec:         CodecConfig{Default: "json", ContentMode: "binary", MaxHeaderBytes: CoreMaxHeaderBytes, MaxBodyBytes: 1024 * 1024},
		Lifecycle:     LifecycleConfig{PreStopDelay: 5 * time.Second, DrainTimeout: time.Minute, HandlerGrace: 5 * time.Second, FlushTimeout: 20 * time.Second, CloseTimeout: 10 * time.Second, RebalanceDrainTimeout: 25 * time.Second},
		Observability: ObservabilityConfig{LogLevel: "info", LogFormat: "json", LogSampling: LogSamplingConfig{Enabled: true, Burst: 10, Thereafter: 100}, Metrics: MetricsConfig{Enabled: true}, Tracing: TracingConfig{Enabled: true, SampleRatio: 0.01, AlwaysSampleRetries: true, AlwaysSampleFailures: true}},
		Subscriptions: map[string]SubscriptionConfig{},
	}
}

func decodeConfig(data []byte, cfg *Config) error {
	raw := rawConfig{F1: rawFromConfig(*cfg)}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&raw); err != nil {
		return fmt.Errorf("f1: invalid config: %w", err)
	}
	if raw.F1 == nil {
		return fmt.Errorf("f1: missing f1 configuration")
	}
	resolved, err := raw.F1.config()
	if err != nil {
		return err
	}
	*cfg = resolved
	return nil
}

func applyEnvironment(cfg *Config) {
	if v, ok := os.LookupEnv("F1_ENV"); ok {
		cfg.Env = v
	}
	if v, ok := os.LookupEnv("F1_SERVICE"); ok {
		cfg.Service = v
	}
	if v, ok := os.LookupEnv("F1_INSTANCE_ID"); ok {
		cfg.InstanceID = v
	}
	if v, ok := os.LookupEnv("F1_BROKER_DRIVER"); ok {
		cfg.Broker.Driver = v
	}
	if v, ok := os.LookupEnv("F1_BROKER_ENDPOINTS"); ok {
		cfg.Broker.Endpoints = strings.Split(v, ",")
	}
}

func validateConfig(cfg Config) error {
	if cfg.Env == "" {
		return fmt.Errorf("f1: env must not be empty")
	}
	if cfg.Service == "" {
		return fmt.Errorf("f1: service must not be empty")
	}
	if cfg.Broker.Driver != "kafka" && cfg.Broker.Driver != "rabbitmq" && cfg.Broker.Driver != "inmem" {
		return fmt.Errorf("f1: broker.driver %q is unsupported", cfg.Broker.Driver)
	}
	if cfg.Broker.Driver == "inmem" && cfg.Env == "prod" {
		return fmt.Errorf("f1: broker.driver inmem is not allowed in prod")
	}
	if cfg.Env == "prod" && cfg.Broker.TLS.InsecureSkipVerify {
		return fmt.Errorf("f1: broker.tls.insecureSkipVerify must be false in prod")
	}
	if cfg.Env == "prod" && cfg.Topology.AutoCreate {
		return fmt.Errorf("f1: topology.autoCreate must be false in prod")
	}
	if cfg.Env == "prod" && cfg.Broker.Driver == "rabbitmq" && cfg.Broker.DriverOptions["rabbitmq.queueType"] != "quorum" {
		return fmt.Errorf("f1: broker.rabbitmq.queueType must be quorum in prod")
	}
	if cfg.Codec.ContentMode != "binary" {
		return fmt.Errorf("f1: codec.contentMode %q is unsupported", cfg.Codec.ContentMode)
	}
	if cfg.Codec.MaxBodyBytes <= 0 {
		return fmt.Errorf("f1: codec.maxBodyBytes must be positive")
	}
	if err := validateLifecycleConfig(cfg.Lifecycle); err != nil {
		return err
	}
	for name, subscription := range cfg.Subscriptions {
		if len(subscription.Topics) == 0 {
			return fmt.Errorf("f1: subscriptions.%s.topics must not be empty", name)
		}
		if subscription.Concurrency < 1 || subscription.Concurrency > 1024 {
			return fmt.Errorf("f1: subscriptions.%s.concurrency must be between 1 and 1024", name)
		}
		if err := validateRetryConfig("subscriptions."+name+".retry", subscription.Retry); err != nil {
			return err
		}
		lanes := len(subscription.Topics) * len(subscription.Priorities) * (1 + retryTiers(subscription.Retry))
		if subscription.Prefetch < lanes {
			return fmt.Errorf("f1: subscriptions.%s.prefetch %d must be at least lane count %d (topics x priorities x (1 + retryTiers))", name, subscription.Prefetch, lanes)
		}
		for priority, weight := range subscription.Fairness.Weights {
			if !priority.Valid() || weight < 1 {
				return fmt.Errorf("f1: subscriptions.%s.fairness.weights.%s must be at least 1", name, priority)
			}
		}
		if cfg.Lifecycle.DrainTimeout <= subscription.HandlerTimeout {
			return fmt.Errorf("f1: lifecycle.drainTimeout must exceed subscriptions.%s.handlerTimeout", name)
		}
	}
	if cfg.Broker.Driver == "kafka" {
		// Keep these fallback values synchronized with the Kafka driver when it lands.
		sessionTimeout, err := durationOption(cfg.Broker.DriverOptions, "kafka.sessionTimeout", 45*time.Second)
		if err != nil {
			return err
		}
		if cfg.Lifecycle.RebalanceDrainTimeout > sessionTimeout*3/5 {
			return fmt.Errorf("f1: lifecycle.rebalanceDrainTimeout must be at most 0.6 x broker.kafka.sessionTimeout")
		}
	}
	if cfg.Broker.Driver == "rabbitmq" {
		// Keep these fallback values synchronized with the RabbitMQ driver when it lands.
		consumerTimeout, err := durationOption(cfg.Broker.DriverOptions, "rabbitmq.consumerTimeout", 90*time.Second)
		if err != nil {
			return err
		}
		for name, subscription := range cfg.Subscriptions {
			if consumerTimeout < subscription.HandlerTimeout*3 {
				return fmt.Errorf("f1: broker.rabbitmq.consumerTimeout must be at least subscriptions.%s.handlerTimeout x 3", name)
			}
		}
	}
	return nil
}

func validateRetryConfig(path string, retry RetryConfig) error {
	if retry.MaxAttempts < 1 || retry.MaxAttempts > 20 {
		return fmt.Errorf("f1: %s.maxAttempts must be between 1 and 20", path)
	}
	if retry.InitialInterval < 0 {
		return fmt.Errorf("f1: %s.initialInterval must not be negative", path)
	}
	if retry.Multiplier < 0 || math.IsNaN(retry.Multiplier) || math.IsInf(retry.Multiplier, 0) {
		return fmt.Errorf("f1: %s.multiplier must be finite and non-negative", path)
	}
	if retry.MaxInterval < 0 {
		return fmt.Errorf("f1: %s.maxInterval must not be negative", path)
	}
	if retry.Jitter < 0 || retry.Jitter > .5 || math.IsNaN(retry.Jitter) || math.IsInf(retry.Jitter, 0) {
		return fmt.Errorf("f1: %s.jitter must be finite and between 0 and 0.5", path)
	}
	for i, tier := range retry.Tiers {
		if tier <= 0 {
			return fmt.Errorf("f1: %s.tiers[%d] must be positive", path, i)
		}
	}
	return nil
}

func validateLifecycleConfig(lifecycle LifecycleConfig) error {
	// drainTimeout is not part of the general zero-means-unset rule: the
	// runtime does not merge a default onto it outside the LoadConfig path,
	// so a zero value would reach the drain deadline as unbounded shutdown
	// rather than a default. It must be positive, not merely non-negative.
	if lifecycle.DrainTimeout <= 0 {
		return fmt.Errorf("f1: lifecycle.drainTimeout must be positive")
	}
	values := []struct {
		name  string
		value time.Duration
	}{
		{name: "preStopDelay", value: lifecycle.PreStopDelay},
		{name: "handlerGrace", value: lifecycle.HandlerGrace},
		{name: "flushTimeout", value: lifecycle.FlushTimeout},
		{name: "closeTimeout", value: lifecycle.CloseTimeout},
		{name: "rebalanceDrainTimeout", value: lifecycle.RebalanceDrainTimeout},
	}
	for _, item := range values {
		if item.value < 0 {
			return fmt.Errorf("f1: lifecycle.%s must not be negative", item.name)
		}
	}
	return nil
}

func durationOption(options map[string]string, key string, fallback time.Duration) (time.Duration, error) {
	value, ok := options[key]
	if !ok {
		return fallback, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("f1: %s %q must be a duration: %w", key, value, err)
	}
	return duration, nil
}

func retryTiers(retry RetryConfig) int {
	if len(retry.Tiers) > 0 {
		return len(retry.Tiers)
	}
	return retry.MaxAttempts - 1
}

type rawConfig struct {
	F1 *rawF1 `yaml:"f1"`
}
type rawF1 struct {
	Env           string                     `yaml:"env"`
	Service       string                     `yaml:"service"`
	InstanceID    string                     `yaml:"instanceId"`
	Broker        rawBroker                  `yaml:"broker"`
	Topology      rawTopology                `yaml:"topology"`
	Codec         CodecConfig                `yaml:"codec"`
	Lifecycle     LifecycleConfig            `yaml:"lifecycle"`
	Observability ObservabilityConfig        `yaml:"observability"`
	Subscriptions map[string]rawSubscription `yaml:"subscriptions"`
}

func rawFromConfig(cfg Config) *rawF1 {
	topologyPriorities := make([]rawPriority, len(cfg.Topology.Priorities))
	for i, priority := range cfg.Topology.Priorities {
		topologyPriorities[i] = rawPriority(priority)
	}
	subscriptions := make(map[string]rawSubscription, len(cfg.Subscriptions))
	for name, subscription := range cfg.Subscriptions {
		subscriptions[name] = rawSubscriptionFromConfig(subscription)
	}
	return &rawF1{
		Env: cfg.Env, Service: cfg.Service, InstanceID: cfg.InstanceID,
		Broker:   rawBroker(cfg.Broker),
		Topology: rawTopology{AutoCreate: cfg.Topology.AutoCreate, VerifyOnStart: cfg.Topology.VerifyOnStart, PartitionsDefault: cfg.Topology.PartitionsDefault, RetentionDefault: cfg.Topology.RetentionDefault, DLQRetention: cfg.Topology.DLQRetention, Priorities: topologyPriorities},
		Codec:    cfg.Codec, Lifecycle: cfg.Lifecycle, Observability: cfg.Observability, Subscriptions: subscriptions,
	}
}

func (y rawF1) config() (Config, error) {
	cfg := Config{
		Env: y.Env, Service: y.Service, InstanceID: y.InstanceID,
		Broker: y.Broker.config(), Topology: y.Topology.config(), Codec: y.Codec,
		Lifecycle: y.Lifecycle, Observability: y.Observability,
		Subscriptions: map[string]SubscriptionConfig{},
	}
	if y.Subscriptions != nil {
		cfg.Subscriptions = make(map[string]SubscriptionConfig, len(y.Subscriptions))
		for name, subscription := range y.Subscriptions {
			subscriptionConfig, err := subscription.config()
			if err != nil {
				return Config{}, fmt.Errorf("f1: subscriptions.%s: %w", name, err)
			}
			cfg.Subscriptions[name] = subscriptionConfig
		}
	}
	for name, subscription := range cfg.Subscriptions {
		if subscription.Prefetch == 0 {
			subscription.Prefetch = cfg.Broker.DefaultPrefetch
			cfg.Subscriptions[name] = subscription
		}
	}
	return cfg, nil
}

type rawMode Mode

func (m *rawMode) UnmarshalYAML(value *yaml.Node) error {
	switch value.Value {
	case "unordered":
		*m = rawMode(Unordered)
	case "orderedByKey":
		*m = rawMode(OrderedByKey)
	default:
		return fmt.Errorf("unsupported mode %q", value.Value)
	}
	return nil
}

type rawPolicy UnmatchedPolicy

func (p *rawPolicy) UnmarshalYAML(value *yaml.Node) error {
	switch value.Value {
	case "ignore":
		*p = rawPolicy(Ignore)
	case "deadletter":
		*p = rawPolicy(DeadLetter)
	default:
		return fmt.Errorf("unsupported unmatched policy %q", value.Value)
	}
	return nil
}

type rawPriority Priority

func (p *rawPriority) UnmarshalYAML(value *yaml.Node) error {
	priority, err := ParsePriority(value.Value)
	if err != nil {
		return err
	}
	*p = rawPriority(priority)
	return nil
}

type rawRetry RetryConfig

func (r *rawRetry) UnmarshalYAML(value *yaml.Node) error {
	if err := requireKnownKeys(value, "maxAttempts", "initialInterval", "multiplier", "maxInterval", "jitter", "tiers"); err != nil {
		return err
	}
	raw := struct {
		MaxAttempts     int             `yaml:"maxAttempts"`
		InitialInterval time.Duration   `yaml:"initialInterval"`
		Multiplier      float64         `yaml:"multiplier"`
		MaxInterval     time.Duration   `yaml:"maxInterval"`
		Jitter          float64         `yaml:"jitter"`
		Tiers           []time.Duration `yaml:"tiers"`
	}{MaxAttempts: r.MaxAttempts, InitialInterval: r.InitialInterval, Multiplier: r.Multiplier, MaxInterval: r.MaxInterval, Jitter: r.Jitter, Tiers: r.Tiers}
	if err := value.Decode(&raw); err != nil {
		return err
	}
	*r = rawRetry(RetryConfig(raw))
	return nil
}

type rawTopology struct {
	AutoCreate        bool          `yaml:"autoCreate"`
	VerifyOnStart     bool          `yaml:"verifyOnStart"`
	PartitionsDefault int           `yaml:"partitionsDefault"`
	RetentionDefault  time.Duration `yaml:"retentionDefault"`
	DLQRetention      time.Duration `yaml:"dlqRetention"`
	Priorities        []rawPriority `yaml:"priorities"`
}

func (y rawTopology) config() TopologyConfig {
	priorities := make([]Priority, len(y.Priorities))
	for i, priority := range y.Priorities {
		priorities[i] = Priority(priority)
	}
	return TopologyConfig{AutoCreate: y.AutoCreate, VerifyOnStart: y.VerifyOnStart, PartitionsDefault: y.PartitionsDefault, RetentionDefault: y.RetentionDefault, DLQRetention: y.DLQRetention, Priorities: priorities}
}

type rawSubscription struct {
	Topics          []string      `yaml:"topics"`
	Mode            rawMode       `yaml:"mode"`
	Concurrency     int           `yaml:"concurrency"`
	Prefetch        int           `yaml:"prefetch"`
	Priorities      []rawPriority `yaml:"priorities"`
	Fairness        rawFairness   `yaml:"fairness"`
	Retry           rawRetry      `yaml:"retry"`
	HandlerTimeout  time.Duration `yaml:"handlerTimeout"`
	UnmatchedPolicy rawPolicy     `yaml:"unmatchedPolicy"`
	presence        subscriptionPresence
}

func defaultSubscription() SubscriptionConfig {
	return SubscriptionConfig{
		Concurrency: 16, Priorities: []Priority{PriorityHigh, PriorityNormal, PriorityLow},
		Fairness: FairnessConfig{Weights: map[Priority]int{PriorityHigh: 8, PriorityNormal: 4, PriorityLow: 1}, Budgets: map[Priority]time.Duration{PriorityHigh: 5 * time.Second, PriorityNormal: 30 * time.Second, PriorityLow: 120 * time.Second}, RetryWeightDivisor: 2, CostModel: "count", PrefetchFactor: 2, AgingEnabled: true},
		Retry:    RetryConfig{MaxAttempts: 4, InitialInterval: time.Second, Multiplier: 5, MaxInterval: 30 * time.Second, Jitter: .2}, HandlerTimeout: 30 * time.Second,
	}
}

func (y *rawSubscription) UnmarshalYAML(value *yaml.Node) error {
	if err := requireKnownKeys(value, "topics", "mode", "concurrency", "prefetch", "priorities", "fairness", "retry", "handlerTimeout", "unmatchedPolicy"); err != nil {
		return err
	}
	type rawSubscriptionInput rawSubscription
	input := rawSubscriptionInput(rawSubscriptionFromConfig(defaultSubscription()))
	if err := value.Decode(&input); err != nil {
		return err
	}
	input.presence = subscriptionPresenceFromNode(value)
	*y = rawSubscription(input)
	return nil
}

func (y rawSubscription) config() (SubscriptionConfig, error) {
	priorities := make([]Priority, len(y.Priorities))
	for i, priority := range y.Priorities {
		priorities[i] = Priority(priority)
	}
	fairness, err := y.Fairness.config()
	if err != nil {
		return SubscriptionConfig{}, err
	}
	return SubscriptionConfig{Topics: y.Topics, Mode: Mode(y.Mode), Concurrency: y.Concurrency, Prefetch: y.Prefetch, Priorities: priorities, Fairness: fairness, Retry: RetryConfig(y.Retry), HandlerTimeout: y.HandlerTimeout, UnmatchedPolicy: UnmatchedPolicy(y.UnmatchedPolicy), presence: y.presence}, nil
}

func rawSubscriptionFromConfig(subscription SubscriptionConfig) rawSubscription {
	priorities := make([]rawPriority, len(subscription.Priorities))
	for i, priority := range subscription.Priorities {
		priorities[i] = rawPriority(priority)
	}
	weights := make(map[string]int, len(subscription.Fairness.Weights))
	for priority, weight := range subscription.Fairness.Weights {
		weights[priority.String()] = weight
	}
	budgets := make(map[string]time.Duration, len(subscription.Fairness.Budgets))
	for priority, budget := range subscription.Fairness.Budgets {
		budgets[priority.String()] = budget
	}
	return rawSubscription{Topics: subscription.Topics, Mode: rawMode(subscription.Mode), Concurrency: subscription.Concurrency, Prefetch: subscription.Prefetch, Priorities: priorities, Fairness: rawFairness{Weights: weights, Budgets: budgets, RetryWeightDivisor: subscription.Fairness.RetryWeightDivisor, CostModel: subscription.Fairness.CostModel, PrefetchFactor: subscription.Fairness.PrefetchFactor, AgingEnabled: subscription.Fairness.AgingEnabled}, Retry: rawRetry(subscription.Retry), HandlerTimeout: subscription.HandlerTimeout, UnmatchedPolicy: rawPolicy(subscription.UnmatchedPolicy), presence: subscription.presence}
}

func subscriptionPresenceFromNode(node *yaml.Node) subscriptionPresence {
	var presence subscriptionPresence
	if node == nil || node.Kind != yaml.MappingNode {
		return presence
	}
	for i := 0; i < len(node.Content); i += 2 {
		switch node.Content[i].Value {
		case "topics":
			presence.Topics = true
		case "mode":
			presence.Mode = true
		case "concurrency":
			presence.Concurrency = true
		case "prefetch":
			presence.Prefetch = true
		case "priorities":
			presence.Priorities = true
		case "fairness":
			presence.Fairness = true
		case "retry":
			presence.Retry = true
		case "handlerTimeout":
			presence.HandlerTimeout = true
		case "unmatchedPolicy":
			presence.UnmatchedPolicy = true
		}
	}
	return presence
}

type rawFairness struct {
	Weights            map[string]int           `yaml:"weights"`
	Budgets            map[string]time.Duration `yaml:"budgets"`
	RetryWeightDivisor int                      `yaml:"retryWeightDivisor"`
	CostModel          string                   `yaml:"costModel"`
	PrefetchFactor     int                      `yaml:"prefetchFactor"`
	AgingEnabled       bool                     `yaml:"agingEnabled"`
}

func (r *rawFairness) UnmarshalYAML(value *yaml.Node) error {
	if err := requireKnownKeys(value, "weights", "budgets", "retryWeightDivisor", "costModel", "prefetchFactor", "agingEnabled"); err != nil {
		return err
	}
	input := struct {
		Weights            map[string]int           `yaml:"weights"`
		Budgets            map[string]time.Duration `yaml:"budgets"`
		RetryWeightDivisor int                      `yaml:"retryWeightDivisor"`
		CostModel          string                   `yaml:"costModel"`
		PrefetchFactor     int                      `yaml:"prefetchFactor"`
		AgingEnabled       bool                     `yaml:"agingEnabled"`
	}{RetryWeightDivisor: r.RetryWeightDivisor, CostModel: r.CostModel, PrefetchFactor: r.PrefetchFactor, AgingEnabled: r.AgingEnabled}
	if err := value.Decode(&input); err != nil {
		return err
	}
	if input.Weights == nil {
		input.Weights = r.Weights
	}
	if input.Budgets == nil {
		input.Budgets = r.Budgets
	}
	*r = rawFairness(input)
	return nil
}

func (r rawFairness) config() (FairnessConfig, error) {
	weights := make(map[Priority]int, len(r.Weights))
	for name, weight := range r.Weights {
		priority, err := ParsePriority(name)
		if err != nil {
			return FairnessConfig{}, fmt.Errorf("f1: fairness.weights.%s: %w", name, err)
		}
		weights[priority] = weight
	}
	budgets := make(map[Priority]time.Duration, len(r.Budgets))
	for name, budget := range r.Budgets {
		priority, err := ParsePriority(name)
		if err != nil {
			return FairnessConfig{}, fmt.Errorf("f1: fairness.budgets.%s: %w", name, err)
		}
		budgets[priority] = budget
	}
	return FairnessConfig{Weights: weights, Budgets: budgets, RetryWeightDivisor: r.RetryWeightDivisor, CostModel: r.CostModel, PrefetchFactor: r.PrefetchFactor, AgingEnabled: r.AgingEnabled}, nil
}

func requireKnownKeys(node *yaml.Node, keys ...string) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("f1: expected mapping")
	}
	known := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		known[key] = struct{}{}
	}
	for i := 0; i < len(node.Content); i += 2 {
		if _, ok := known[node.Content[i].Value]; !ok {
			return fmt.Errorf("f1: unknown key %q", node.Content[i].Value)
		}
	}
	return nil
}
