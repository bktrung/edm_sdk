package f1

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/dispatch"
	kafkarules "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/kafka"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/retry"
)

// maxPrefetch is the largest value the AMQP 0-9-1 basic.qos prefetch-count
// field can carry; above it the client library's unchecked narrowing turns
// the request into its opposite.
const maxPrefetch = 65535

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
	Subscriptions map[string]SubscriptionConfig `yaml:"subscriptions"`
}

// TopologyConfig configures the destinations a Client verifies or creates.
type TopologyConfig struct {
	AutoCreate    bool       `yaml:"autoCreate"`
	VerifyOnStart bool       `yaml:"verifyOnStart"`
	Priorities    []Priority `yaml:"priorities"`
}

// CodecConfig configures the codec selection and envelope size limits.
type CodecConfig struct {
	Default        string `yaml:"default"`
	ContentMode    string `yaml:"contentMode"`
	MaxHeaderBytes int    `yaml:"maxHeaderBytes"`
	MaxBodyBytes   int    `yaml:"maxBodyBytes"`
}

// LifecycleConfig configures the timing of graceful client shutdown. A zero
// field selects the package default, so a Go caller that leaves one out
// behaves the same as a YAML config that omits it; negative values are
// invalid. ConsumerDrainTimeout is the one exception, where zero means no
// bound at all.
type LifecycleConfig struct {
	DrainTimeout time.Duration `yaml:"drainTimeout"`
	HandlerGrace time.Duration `yaml:"handlerGrace"`
	// ConsumerDrainTimeout bounds how long Close waits for subscription
	// runners to finish draining. It is its own budget, not another spend of
	// DrainTimeout: zero disables the bound and leaves the caller's context
	// as the only limit, which is the behavior every caller saw before this
	// field existed.
	ConsumerDrainTimeout  time.Duration `yaml:"consumerDrainTimeout"`
	CloseTimeout          time.Duration `yaml:"closeTimeout"`
	RebalanceDrainTimeout time.Duration `yaml:"rebalanceDrainTimeout"`
}

// Mode selects whether a Subscription preserves per-key delivery order.
type Mode int

const (
	// Unordered is the default delivery mode.
	Unordered Mode = iota
	// OrderedByKey requests per-key delivery order: deliveries that share a key
	// are handled one at a time. The guarantee ends at a retry: a delivery that
	// fails is acknowledged as soon as its retry copy is stored, which releases
	// the key, so a later delivery with that key is handled before the retry
	// comes back.
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
	PrefetchFactor     int
	// DisableDeadlinePromotion turns off serving a lane first once its oldest
	// item has waited past its budget. The zero value leaves deadline promotion
	// on, which is the default.
	DisableDeadlinePromotion bool
}

// RetryConfig configures a Subscription's retry ladder.
type RetryConfig struct {
	MaxAttempts     int
	InitialInterval time.Duration
	Multiplier      float64
	MaxInterval     time.Duration
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
	cfg = normalizeConfig(cfg)
	if err := validateConfig(cfg, cfg.Broker.Driver); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func defaultConfig() Config {
	return Config{
		Broker:        BrokerConfig{ConnectTimeout: 30 * time.Second, DefaultPrefetch: 64},
		Topology:      TopologyConfig{VerifyOnStart: true, Priorities: []Priority{PriorityHigh, PriorityMedium, PriorityLow}},
		Codec:         CodecConfig{Default: "json", ContentMode: "binary", MaxHeaderBytes: CoreMaxHeaderBytes, MaxBodyBytes: 1024 * 1024},
		Lifecycle:     LifecycleConfig{DrainTimeout: time.Minute, HandlerGrace: 5 * time.Second, CloseTimeout: 10 * time.Second, RebalanceDrainTimeout: 25 * time.Second},
		Subscriptions: map[string]SubscriptionConfig{},
	}
}

func effectiveHeaderLimit(configured, driver int) int {
	limit := CoreMaxHeaderBytes
	if configured > 0 {
		limit = min(limit, configured)
	}
	if driver > 0 {
		limit = min(limit, driver)
	}
	return limit
}

func resolvePrefetch(prefetch, brokerDefault int) int {
	if prefetch != 0 {
		return prefetch
	}
	if brokerDefault != 0 {
		return brokerDefault
	}
	return defaultConfig().Broker.DefaultPrefetch
}

func normalizeConfig(cfg Config) Config {
	defaults := defaultConfig()
	if cfg.Codec.Default == "" {
		cfg.Codec.Default = defaults.Codec.Default
	}
	if cfg.Codec.ContentMode == "" {
		cfg.Codec.ContentMode = defaults.Codec.ContentMode
	}
	if cfg.Codec.MaxBodyBytes == 0 {
		cfg.Codec.MaxBodyBytes = defaults.Codec.MaxBodyBytes
	}
	if cfg.Codec.MaxHeaderBytes == 0 {
		cfg.Codec.MaxHeaderBytes = defaults.Codec.MaxHeaderBytes
	}

	if len(cfg.Topology.Priorities) == 0 {
		cfg.Topology.Priorities = append([]Priority(nil), defaults.Topology.Priorities...)
	}
	if cfg.Lifecycle.DrainTimeout == 0 {
		cfg.Lifecycle.DrainTimeout = defaults.Lifecycle.DrainTimeout
	}
	if cfg.Lifecycle.HandlerGrace == 0 {
		cfg.Lifecycle.HandlerGrace = defaults.Lifecycle.HandlerGrace
	}
	if cfg.Lifecycle.CloseTimeout == 0 {
		cfg.Lifecycle.CloseTimeout = defaults.Lifecycle.CloseTimeout
	}
	if cfg.Lifecycle.RebalanceDrainTimeout == 0 {
		cfg.Lifecycle.RebalanceDrainTimeout = defaults.Lifecycle.RebalanceDrainTimeout
	}
	if cfg.Subscriptions == nil {
		return cfg
	}
	subscriptions := make(map[string]SubscriptionConfig, len(cfg.Subscriptions))
	subDefaults := defaultSubscription()
	for name, subscription := range cfg.Subscriptions {
		if subscription.Concurrency == 0 {
			subscription.Concurrency = subDefaults.Concurrency
		}
		if len(subscription.Priorities) == 0 {
			subscription.Priorities = append([]Priority(nil), subDefaults.Priorities...)
		}
		if subscription.Retry.MaxAttempts == 0 {
			subscription.Retry.MaxAttempts = subDefaults.Retry.MaxAttempts
		}
		if !subscription.presence.Prefetch {
			named := subscription.Prefetch != 0
			subscription.Prefetch = resolvePrefetch(subscription.Prefetch, cfg.Broker.DefaultPrefetch)
			if !named {
				subscription.Prefetch = max(subscription.Prefetch, subscriptionLaneCount(len(subscription.Topics), len(subscription.Priorities), subscription.Retry))
			}
		}
		if subscription.HandlerTimeout == 0 {
			subscription.HandlerTimeout = subDefaults.HandlerTimeout
		}
		subscriptions[name] = subscription
	}
	cfg.Subscriptions = subscriptions
	return cfg
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
		parts := strings.Split(v, ",")
		cfg.Broker.Endpoints = make([]string, 0, len(parts))
		for _, part := range parts {
			if endpoint := strings.TrimSpace(part); endpoint != "" {
				cfg.Broker.Endpoints = append(cfg.Broker.Endpoints, endpoint)
			}
		}
	}
}

var productionEnvironmentAliases = map[string]struct{}{
	"prod":       {},
	"production": {},
	"prd":        {},
}

func validateConfig(cfg Config, driverName string) error {
	if cfg.Env == "" {
		return fmt.Errorf("f1: env must not be empty")
	}
	if cfg.Service == "" {
		return fmt.Errorf("f1: service must not be empty")
	}
	if _, ok := productionEnvironmentAliases[strings.ToLower(cfg.Env)]; ok && cfg.Env != "prod" {
		return fmt.Errorf("f1: env %q must be exactly %q for production", cfg.Env, "prod")
	}
	if cfg.Broker.Driver != "kafka" && cfg.Broker.Driver != "rabbitmq" && cfg.Broker.Driver != "inmem" {
		return fmt.Errorf("f1: broker.driver %q is unsupported", cfg.Broker.Driver)
	}
	// The namespace is the driver this configuration declares, which is the
	// name a key carries in the file and the only name both callers of this
	// function agree on: LoadConfig has no driver instance at all, and New may
	// have opened one under a different name, which it logs as a mismatch
	// rather than reading back into the configuration. The check sits above the
	// rest of the driver checks so that a bad key still fails as early as the
	// decoder used to refuse it.
	if err := validateOptionKeys(cfg.Broker.DriverOptions, cfg.Broker.Driver); err != nil {
		return err
	}
	if (driverName == "kafka" || driverName == "rabbitmq") && len(cfg.Broker.Endpoints) == 0 {
		return fmt.Errorf("f1: broker.endpoints must not be empty")
	}
	if driverName == "inmem" && cfg.Env == "prod" {
		return fmt.Errorf("f1: broker.driver inmem is not allowed in prod")
	}
	if cfg.Broker.MaxReconnectAttempts < 0 {
		return fmt.Errorf("f1: broker.maxReconnectAttempts must not be negative")
	}
	if cfg.Env == "prod" && cfg.Broker.SASL.Mechanism != "" && !cfg.Broker.TLS.Enabled {
		return fmt.Errorf("f1: broker.sasl.mechanism requires broker.tls.enabled in prod")
	}
	if driverName == "rabbitmq" && cfg.Broker.TLS.Enabled {
		for _, endpoint := range cfg.Broker.Endpoints {
			parsed, err := url.Parse(endpoint)
			if err != nil {
				return errors.New("f1: broker.tls.enabled requires a valid amqps:// endpoint")
			}
			if parsed.Scheme != "amqps" {
				return fmt.Errorf("f1: broker.tls.enabled requires an amqps:// endpoint; got %q", parsed.Scheme)
			}
		}
	}

	if cfg.Env == "prod" && cfg.Broker.TLS.InsecureSkipVerify {
		return fmt.Errorf("f1: broker.tls.insecureSkipVerify must be false in prod")
	}
	if cfg.Env == "prod" && cfg.Topology.AutoCreate {
		return fmt.Errorf("f1: topology.autoCreate must be false in prod")
	}
	if cfg.Env == "prod" && driverName == "rabbitmq" && cfg.Broker.DriverOptions["rabbitmq.queueType"] != "quorum" {
		return fmt.Errorf("f1: broker.rabbitmq.queueType must be quorum in prod")
	}
	if cfg.Codec.ContentMode != "binary" {
		return fmt.Errorf("f1: codec.contentMode %q is unsupported", cfg.Codec.ContentMode)
	}
	if cfg.Codec.MaxBodyBytes <= 0 {
		return fmt.Errorf("f1: codec.maxBodyBytes must be positive")
	}
	if cfg.Codec.MaxHeaderBytes <= 0 {
		return fmt.Errorf("f1: codec.maxHeaderBytes must be positive")
	}
	if err := validateLifecycleConfig(cfg.Lifecycle); err != nil {
		return err
	}
	if err := validatePriorityList("topology.priorities", cfg.Topology.Priorities); err != nil {
		return err
	}
	for name, subscription := range cfg.Subscriptions {
		if len(subscription.Topics) == 0 {
			return fmt.Errorf("f1: subscriptions.%s.topics must not be empty", name)
		}
		if err := validatePriorityList("subscriptions."+name+".priorities", subscription.Priorities); err != nil {
			return err
		}
		if subscription.Concurrency < 1 || subscription.Concurrency > 1024 {
			return fmt.Errorf("f1: subscriptions.%s.concurrency must be between 1 and 1024", name)
		}
		if err := validateSubscriptionModeAndPolicy(name, subscription.Mode, subscription.UnmatchedPolicy); err != nil {
			return err
		}
		if err := validateRetryConfig("subscriptions."+name+".retry", subscription.Retry); err != nil {
			return err
		}
		lanes := subscriptionLaneCount(len(subscription.Topics), len(subscription.Priorities), subscription.Retry)
		if err := validatePrefetch(name, subscription.Prefetch, lanes); err != nil {
			return err
		}
		if subscription.Mode == OrderedByKey && subscription.Concurrency > dispatch.MaxOrderedBufferEntries/subscription.Prefetch {
			return fmt.Errorf("f1: subscriptions.%s: ordered mode needs concurrency x prefetch at most %d, got %d x %d", name, dispatch.MaxOrderedBufferEntries, subscription.Concurrency, subscription.Prefetch)
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
	if driverName == "kafka" {
		// Keep these fallback values synchronized with the Kafka driver.
		sessionTimeout, err := durationOption(cfg.Broker.DriverOptions, "kafka.sessionTimeout", 45*time.Second)
		if err != nil {
			return err
		}
		rebalanceTimeout, err := durationOption(cfg.Broker.DriverOptions, "kafka.rebalanceTimeout", 60*time.Second)
		if err != nil {
			return err
		}
		if kafkarules.ExceedsKafkaDrainBound(cfg.Lifecycle.RebalanceDrainTimeout, sessionTimeout) {
			return fmt.Errorf(
				"f1: lifecycle.rebalanceDrainTimeout %s exceeds 0.6 x broker.kafka.sessionTimeout %s (broker.kafka.rebalanceTimeout=%s)",
				cfg.Lifecycle.RebalanceDrainTimeout,
				sessionTimeout,
				rebalanceTimeout,
			)
		}
		if kafkarules.ExceedsKafkaDrainBound(cfg.Lifecycle.RebalanceDrainTimeout, rebalanceTimeout) {
			return fmt.Errorf(
				"f1: lifecycle.rebalanceDrainTimeout %s exceeds 0.6 x broker.kafka.rebalanceTimeout %s (broker.kafka.sessionTimeout=%s)",
				cfg.Lifecycle.RebalanceDrainTimeout,
				rebalanceTimeout,
				sessionTimeout,
			)
		}
	}
	if driverName == "rabbitmq" {
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

func validatePrefetch(name string, prefetch, lanes int) error {
	if prefetch < lanes {
		return fmt.Errorf("f1: subscriptions.%s.prefetch %d must be at least lane count %d (topics x priorities x (1 + retryTiers))", name, prefetch, lanes)
	}
	if prefetch > maxPrefetch {
		return fmt.Errorf("f1: subscriptions.%s.prefetch %d must be at most %d", name, prefetch, maxPrefetch)
	}
	return nil
}

func validatePriorityList(path string, priorities []Priority) error {
	if len(priorities) == 0 {
		return fmt.Errorf("f1: %s must not be empty", path)
	}
	seen := make(map[Priority]struct{}, len(priorities))
	for _, priority := range priorities {
		if !priority.Valid() {
			return fmt.Errorf("f1: %s contains invalid priority %d", path, priority)
		}
		if _, ok := seen[priority]; ok {
			return fmt.Errorf("f1: %s contains duplicate priority %s", path, priority.String())
		}
		seen[priority] = struct{}{}
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
	if retry.MaxAttempts > 1 && len(retry.Tiers) == 0 {
		for attempt := 1; attempt < retry.MaxAttempts; attempt++ {
			if retry.DelayFor(attempt) == 0 {
				return fmt.Errorf("f1: %s retry delay must be positive", path)
			}
		}
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
		{name: "handlerGrace", value: lifecycle.HandlerGrace},
		{name: "consumerDrainTimeout", value: lifecycle.ConsumerDrainTimeout},
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

// subscriptionLaneCount is the number of delivery lanes a subscription feeds:
// one per topic, priority and retry tier. The default prefetch and both
// prefetch validations derive from it, so the three cannot drift.
func subscriptionLaneCount(topics, priorities int, retry RetryConfig) int {
	return topics * priorities * (1 + retryTiers(retry))
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
		Topology: rawTopology{AutoCreate: cfg.Topology.AutoCreate, VerifyOnStart: cfg.Topology.VerifyOnStart, Priorities: topologyPriorities},
		Codec:    cfg.Codec, Lifecycle: cfg.Lifecycle, Subscriptions: subscriptions,
	}
}

func (y rawF1) config() (Config, error) {
	cfg := Config{
		Env: y.Env, Service: y.Service, InstanceID: y.InstanceID,
		Broker: y.Broker.config(), Topology: y.Topology.config(), Codec: y.Codec,
		Lifecycle:     y.Lifecycle,
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
	if err := requireKnownKeys(value, "maxAttempts", "initialInterval", "multiplier", "maxInterval", "tiers"); err != nil {
		return err
	}
	raw := struct {
		MaxAttempts     int             `yaml:"maxAttempts"`
		InitialInterval time.Duration   `yaml:"initialInterval"`
		Multiplier      float64         `yaml:"multiplier"`
		MaxInterval     time.Duration   `yaml:"maxInterval"`
		Tiers           []time.Duration `yaml:"tiers"`
	}{MaxAttempts: r.MaxAttempts, InitialInterval: r.InitialInterval, Multiplier: r.Multiplier, MaxInterval: r.MaxInterval, Tiers: r.Tiers}
	if err := value.Decode(&raw); err != nil {
		return err
	}
	*r = rawRetry(RetryConfig(raw))
	return nil
}

type rawTopology struct {
	AutoCreate    bool          `yaml:"autoCreate"`
	VerifyOnStart bool          `yaml:"verifyOnStart"`
	Priorities    []rawPriority `yaml:"priorities"`
}

func (y rawTopology) config() TopologyConfig {
	priorities := make([]Priority, len(y.Priorities))
	for i, priority := range y.Priorities {
		priorities[i] = Priority(priority)
	}
	return TopologyConfig{AutoCreate: y.AutoCreate, VerifyOnStart: y.VerifyOnStart, Priorities: priorities}
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
		Concurrency: 16, Priorities: []Priority{PriorityHigh, PriorityMedium, PriorityLow},
		Fairness: FairnessConfig{Weights: map[Priority]int{PriorityHigh: 8, PriorityMedium: 4, PriorityLow: 1}, Budgets: map[Priority]time.Duration{PriorityHigh: 5 * time.Second, PriorityMedium: 30 * time.Second, PriorityLow: 120 * time.Second}, RetryWeightDivisor: 2, PrefetchFactor: 2},
		Retry:    RetryConfig{MaxAttempts: 4, InitialInterval: time.Second, Multiplier: 5, MaxInterval: 30 * time.Second}, HandlerTimeout: 30 * time.Second,
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
	return rawSubscription{Topics: subscription.Topics, Mode: rawMode(subscription.Mode), Concurrency: subscription.Concurrency, Prefetch: subscription.Prefetch, Priorities: priorities, Fairness: rawFairness{Weights: weights, Budgets: budgets, RetryWeightDivisor: subscription.Fairness.RetryWeightDivisor, PrefetchFactor: subscription.Fairness.PrefetchFactor, DisableDeadlinePromotion: subscription.Fairness.DisableDeadlinePromotion}, Retry: rawRetry(subscription.Retry), HandlerTimeout: subscription.HandlerTimeout, UnmatchedPolicy: rawPolicy(subscription.UnmatchedPolicy), presence: subscription.presence}
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
	Weights                  map[string]int           `yaml:"weights"`
	Budgets                  map[string]time.Duration `yaml:"budgets"`
	RetryWeightDivisor       int                      `yaml:"retryWeightDivisor"`
	PrefetchFactor           int                      `yaml:"prefetchFactor"`
	DisableDeadlinePromotion bool                     `yaml:"disableDeadlinePromotion"`
}

func (r *rawFairness) UnmarshalYAML(value *yaml.Node) error {
	if err := requireKnownKeys(value, "weights", "budgets", "retryWeightDivisor", "prefetchFactor", "disableDeadlinePromotion"); err != nil {
		return err
	}
	input := struct {
		Weights                  map[string]int           `yaml:"weights"`
		Budgets                  map[string]time.Duration `yaml:"budgets"`
		RetryWeightDivisor       int                      `yaml:"retryWeightDivisor"`
		PrefetchFactor           int                      `yaml:"prefetchFactor"`
		DisableDeadlinePromotion bool                     `yaml:"disableDeadlinePromotion"`
	}{RetryWeightDivisor: r.RetryWeightDivisor, PrefetchFactor: r.PrefetchFactor, DisableDeadlinePromotion: r.DisableDeadlinePromotion}
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
	return FairnessConfig{Weights: weights, Budgets: budgets, RetryWeightDivisor: r.RetryWeightDivisor, PrefetchFactor: r.PrefetchFactor, DisableDeadlinePromotion: r.DisableDeadlinePromotion}, nil
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
