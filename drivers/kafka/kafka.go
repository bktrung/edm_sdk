package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// A Conn owns one client for Ping, metadata, and broker configuration. The
// producer-class and consumer clients will be created lazily by their
// resource factories and share this Conn's client identity.
//
// franz-go v1.21.6 is pinned for the Kafka 4.2.1 fixture and its fetch,
// pause, commit, and rebalance behavior.

var (
	_ driver.Driver = Driver{}
	_ driver.Conn   = (*conn)(nil)
	_ driver.Admin  = (*admin)(nil)
)

var (
	errMissingEndpoints = errors.New("kafka: broker endpoints must not be empty")
	errShareGroups      = errors.New("kafka: share groups mode is not implemented")
	errBrokerConfig     = errors.New("kafka: invalid broker configuration")
	errProtocolResponse = errors.New("kafka: invalid protocol response")
	errConnClosing      = errors.New("kafka: connection is closing")
)

// Driver is a stateless Kafka driver factory.
type Driver struct{}

type consumeMode string

const (
	classicMode                 consumeMode = "classic"
	franzMinProducerBatchBytes              = 512
	franzMaxProducerBatchBytes              = 1 << 30
	kafkaV2RecordBatchBaseBytes             = 65
)

// conn owns the single client used for connection, producer, and metadata
// operations. Consumer instances use cloned options so each can own its group.
type conn struct {
	client                *kgo.Client
	clientOpts            []kgo.Opt
	driverOptions         map[string]string
	instanceID            string
	logger                *slog.Logger
	rebalanceDrainTimeout time.Duration
	staticMembership      bool
	balancer              kgo.GroupBalancer
	delays                map[string]time.Duration
	caps                  driver.Capabilities
	info                  driver.BrokerInfo
	consumers             map[*consumer]struct{}
	producers             map[*producer]struct{}
	mu                    sync.RWMutex
	lifecycleMu           sync.RWMutex
	closeOnce             sync.Once
	closeAttempt          bool
	publishFault          atomic.Int32
	closeFault            atomic.Bool
	closing               bool
	closed                bool
}

// Name returns the stable Kafka driver key.
func (Driver) Name() string {
	return "kafka"
}

// Capabilities reports the ceiling across classic and share-group modes.
func (Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		PerMessageAck:       true,
		OrderedByKey:        true,
		NativeDeliveryCount: true,
		NativeDLQ:           false,
		ConsumerScaling:     driver.ScalingFree,
		Fanout:              driver.FanoutAtConsume,
		LagQueryable:        true,
	}
}

// Open establishes a Kafka connection and returns only after the broker has
// answered a liveness, metadata, broker-config, and API-version request.
func (Driver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("open", driver.KindTransient, err)
	}

	if _, err := resolveMode(cfg.DriverOptions); err != nil {
		return nil, classify("open", driver.KindFatal, err)
	}
	staticMembership, err := resolveStaticMembership(cfg.DriverOptions)
	if err != nil {
		return nil, classify("open", driver.KindFatal, err)
	}
	balancer, err := resolveBalancer(cfg.DriverOptions)
	if err != nil {
		return nil, classify("open", driver.KindFatal, err)
	}
	if len(cfg.Endpoints) == 0 {
		return nil, classify("open", driver.KindFatal, errMissingEndpoints)
	}

	openCtx := ctx
	if cfg.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		openCtx, cancel = context.WithTimeout(ctx, cfg.ConnectTimeout)
		defer cancel()
	}

	// The port's durability promise must not depend on franz-go's defaults. No
	// repository test can observe a leader-only acknowledgement during a broker
	// failure, so keep all-ISR acknowledgements explicit. Idempotent writes
	// remain enabled by not opting into DisableIdempotentWrite.
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Endpoints...),
		kgo.ClientID(cfg.ClientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}
	if cfg.TLS != nil && cfg.TLS.Enabled {
		if err := validateTLSConfig(cfg.Endpoints, cfg.TLS); err != nil {
			return nil, classify("open", driver.KindFatal, err)
		}
		tlsConfig, err := makeTLSConfig(cfg.TLS)
		if err != nil {
			return nil, classify("open", driver.KindFatal, err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}
	mechanism, err := makeSASLMechanism(cfg.SASL)
	if err != nil {
		return nil, classify("open", driver.KindFatal, err)
	}
	if mechanism != nil {
		opts = append(opts, kgo.SASL(mechanism))
	}
	producerBatchBytes := int32(franzMinProducerBatchBytes)
	opts = append(opts, kgo.ProducerBatchMaxBytesFn(func(string) int32 {
		return producerBatchBytes
	}))

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, classify("open", driver.KindFatal, err)
	}
	keepClient := false
	defer func() {
		if !keepClient {
			client.Close()
		}
	}()

	admin := kadm.NewClient(client)
	var (
		metadata        kadm.Metadata
		messageMaxBytes int
		versionResponse *kmsg.ApiVersionsResponse
	)
	for {
		if err := client.Ping(openCtx); err != nil {
			if retryErr := retryOpen(openCtx, err); retryErr != nil {
				return nil, retryErr
			}
			continue
		}
		metadata, err = admin.BrokerMetadata(openCtx)
		if err != nil {
			if retryErr := retryOpen(openCtx, err); retryErr != nil {
				return nil, retryErr
			}
			continue
		}
		messageMaxBytes, err = readMessageMaxBytes(openCtx, admin, metadata.Controller)
		if err != nil {
			if retryErr := retryOpen(openCtx, err); retryErr != nil {
				return nil, retryErr
			}
			continue
		}
		versionResponse, err = apiVersions(openCtx, client)
		if err != nil {
			if retryErr := retryOpen(openCtx, err); retryErr != nil {
				return nil, retryErr
			}
			continue
		}
		break
	}

	effectiveBatchBytes, err := effectiveProducerBatchBytes(messageMaxBytes)
	if err != nil {
		return nil, classify("open", driver.KindFatal, err)
	}
	producerBatchBytes = int32(effectiveBatchBytes) //nolint:gosec // effectiveProducerBatchBytes bounds the value to the int32 range.

	info := brokerInfo(metadata, versionResponse)
	caps := classicCapabilities(maxKafkaBodyBytes(effectiveBatchBytes))
	keepClient = true
	rebalanceDrainTimeout := cfg.RebalanceDrainTimeout
	if rebalanceDrainTimeout == 0 {
		rebalanceDrainTimeout = 25 * time.Second
	}
	// Resolve the fallback once, at construction, so the consume path reads a
	// non-nil logger without repeating the check.
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &conn{
		client:                client,
		clientOpts:            append([]kgo.Opt(nil), opts...),
		driverOptions:         maps.Clone(cfg.DriverOptions),
		instanceID:            cfg.InstanceID,
		logger:                logger,
		rebalanceDrainTimeout: rebalanceDrainTimeout,
		staticMembership:      staticMembership,
		balancer:              balancer,
		delays:                make(map[string]time.Duration),
		caps:                  caps,
		info:                  info,
		consumers:             make(map[*consumer]struct{}),
		producers:             make(map[*producer]struct{}),
	}, nil
}

func resolveMode(options map[string]string) (consumeMode, error) {
	value, ok := options["kafka.useShareGroups"]
	if !ok {
		value = "auto"
	}
	switch value {
	case "auto", "never":
		return classicMode, nil
	case "always":
		return "", errShareGroups
	default:
		return "", fmt.Errorf("kafka: invalid useShareGroups mode %q; supported values: auto, always, never", value)
	}
}

func resolveStaticMembership(options map[string]string) (bool, error) {
	value, ok := options["kafka.staticMembership"]
	if !ok {
		return true, nil
	}
	enabled, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("kafka: invalid staticMembership %q; must be a boolean", value)
	}
	return enabled, nil
}

func effectiveProducerBatchBytes(brokerLimit int) (int, error) {
	if brokerLimit < franzMinProducerBatchBytes {
		return 0, fmt.Errorf(
			"%w: message.max.bytes %d is below franz-go minimum %d",
			errBrokerConfig,
			brokerLimit,
			franzMinProducerBatchBytes,
		)
	}
	if brokerLimit > franzMaxProducerBatchBytes {
		return franzMaxProducerBatchBytes, nil
	}
	return brokerLimit, nil
}

func maxKafkaBodyBytes(batchLimit int) int {
	if batchLimit <= kafkaV2RecordBatchBaseBytes {
		return 0
	}

	low, high := 0, batchLimit-kafkaV2RecordBatchBaseBytes
	for low < high {
		mid := low + (high-low+1)/2
		if kafkaBareRecordBatchBytes(mid) <= batchLimit {
			low = mid
		} else {
			high = mid - 1
		}
	}
	return low
}

func kafkaPositiveVarintLen(value int) int {
	switch {
	case value < 1<<6:
		return 1
	case value < 1<<13:
		return 2
	case value < 1<<20:
		return 3
	case value < 1<<27:
		return 4
	default:
		return 5
	}
}

func kafkaBareRecordBatchBytes(bodyBytes int) int {
	recordLength := 1 + // attributes
		kbin.VarlongLen(0) + // timestamp delta
		kbin.VarintLen(0) + // offset delta
		kbin.VarintLen(0) + // empty key length
		kafkaPositiveVarintLen(bodyBytes) +
		bodyBytes +
		kbin.VarintLen(0) // empty header count
	return kafkaV2RecordBatchBaseBytes + kafkaPositiveVarintLen(recordLength) + recordLength
}

func classicCapabilities(maxMessageBytes int) driver.Capabilities {
	caps := Driver{}.Capabilities()
	caps.PerMessageAck = false
	caps.NativeDeliveryCount = false
	caps.ConsumerScaling = driver.ScalingPartitionBound
	caps.MaxMessageBytes = maxMessageBytes
	// Kafka exposes no header limit, so MaxHeaderBytes remains undeclared.
	caps.MaxHeaderBytes = 0
	return caps
}

func readMessageMaxBytes(ctx context.Context, admin *kadm.Client, controller int32) (int, error) {
	var (
		configs kadm.ResourceConfigs
		err     error
	)
	if controller >= 0 {
		configs, err = admin.DescribeBrokerConfigs(ctx, controller)
	} else {
		configs, err = admin.DescribeBrokerConfigs(ctx)
	}
	if err != nil {
		return 0, err
	}
	for _, resource := range configs {
		if resource.Err != nil {
			return 0, resource.Err
		}
		for _, config := range resource.Configs {
			if config.Key != "message.max.bytes" {
				continue
			}
			value := config.MaybeValue()
			limit, err := strconv.Atoi(value)
			if err != nil {
				return 0, fmt.Errorf("%w: invalid message.max.bytes %q", errBrokerConfig, value)
			}
			return limit, nil
		}
	}
	return 0, fmt.Errorf("%w: broker did not return message.max.bytes", errBrokerConfig)
}

func apiVersions(ctx context.Context, client *kgo.Client) (*kmsg.ApiVersionsResponse, error) {
	request := kmsg.NewPtrApiVersionsRequest()
	request.Version = 4
	request.ClientSoftwareName = "f1-kafka-driver"
	request.ClientSoftwareVersion = "1"
	response, err := client.Request(ctx, request)
	if err != nil {
		return nil, err
	}
	versions, ok := response.(*kmsg.ApiVersionsResponse)
	if !ok {
		return nil, fmt.Errorf("%w: unexpected ApiVersions response %T", errProtocolResponse, response)
	}
	if err := kerr.ErrorForCode(versions.ErrorCode); err != nil {
		return nil, err
	}
	return versions, nil
}

func brokerInfo(metadata kadm.Metadata, versions *kmsg.ApiVersionsResponse) driver.BrokerInfo {
	info := driver.BrokerInfo{
		Kind:    "kafka",
		Version: "", // Core code must use BrokerInfo.Display() rather than Version directly; Kafka does not report a release string.
		Nodes:   make([]string, 0, len(metadata.Brokers)),
		Extra:   make(map[string]string),
	}
	for _, broker := range metadata.Brokers {
		info.Nodes = append(info.Nodes, net.JoinHostPort(broker.Host, strconv.Itoa(int(broker.Port))))
	}
	if versions.FinalizedFeaturesEpoch >= 0 {
		for _, feature := range versions.FinalizedFeatures {
			if feature.Name == "metadata.version" {
				info.Extra["metadata.version"] = strconv.Itoa(int(feature.MaxVersionLevel))
				break
			}
		}
	}
	return info
}

func (c *conn) Capabilities() driver.Capabilities { return c.caps }

func (c *conn) BrokerInfo() driver.BrokerInfo {
	info := c.info
	info.Nodes = append([]string(nil), c.info.Nodes...)
	info.Extra = maps.Clone(c.info.Extra)
	return info
}

func (c *conn) Producer(ctx context.Context, cfg driver.ProducerConfig) (driver.Producer, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("producer", driver.KindTransient, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.closed {
		return nil, classify("producer", driver.KindTransient, errConnClosing)
	}
	p := &producer{client: c.client, conn: c, cfg: cfg, clock: clock.NewReal()}
	if c.producers == nil {
		c.producers = make(map[*producer]struct{})
	}
	c.producers[p] = struct{}{}
	return p, nil
}

func (c *conn) admissionError(operation string) error {
	c.mu.RLock()
	closing := c.closing || c.closed
	c.mu.RUnlock()
	if closing {
		return classify(operation, driver.KindTransient, errConnClosing)
	}
	return nil
}

func (c *conn) Consumer(ctx context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	return newConsumer(ctx, c, cfg)
}

func (c *conn) Admin() driver.Admin { return &admin{client: kadm.NewClient(c.client), conn: c} }

// log returns the connection's logger, falling back to the process default.
// Open resolves a nil Config.Logger when it builds the connection; this keeps
// the zero conn no worse than a nil Config, so a driver diagnostic never panics
// on a missing logger.
func (c *conn) log() *slog.Logger {
	if c.logger == nil {
		return slog.Default()
	}
	return c.logger
}

func (c *conn) destinationDelay(destination string) (time.Duration, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	delay, ok := c.delays[destination]
	return delay, ok
}

func (c *conn) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("ping", driver.KindTransient, err)
	}
	if err := c.client.Ping(ctx); err != nil {
		return classify("ping", kafkaErrorKind(err), err)
	}
	return nil
}

func (c *conn) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("close", driver.KindTransient, err)
	}
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	if c.closeAttempt {
		c.mu.Unlock()
		return classify("close", driver.KindTransient, errConnClosing)
	}
	if !c.closing && (len(c.consumers) != 0 || len(c.producers) != 0) {
		c.mu.Unlock()
		return classify("close", driver.KindFatal, driver.ErrResourcesOutstanding)
	}
	c.closing = true
	c.closeAttempt = true
	injected := c.closeFault.Swap(false)
	c.mu.Unlock()

	if injected {
		if _, hasDeadline := ctx.Deadline(); hasDeadline {
			<-ctx.Done()
			c.mu.Lock()
			c.closeAttempt = false
			c.mu.Unlock()
			return classify("close", driver.KindTransient, ctx.Err())
		}
		c.mu.Lock()
		c.closeAttempt = false
		c.mu.Unlock()
		return classify("close", driver.KindTransient, errors.New("injected close failure"))
	}

	c.closeOnce.Do(c.client.Close)
	c.mu.Lock()
	c.closeAttempt = false
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *conn) removeProducer(producer *producer) {
	c.mu.Lock()
	delete(c.producers, producer)
	c.mu.Unlock()
}

func validateSASLCredentials(settings *driver.SASLConfig) error {
	if settings.Username == "" || settings.Password == "" {
		return errors.New("kafka: SASL username and password must be non-empty")
	}
	return nil
}

func makeSASLMechanism(settings *driver.SASLConfig) (sasl.Mechanism, error) {
	if settings == nil {
		return nil, nil
	}
	switch strings.ToLower(settings.Mechanism) {
	case "":
		return nil, nil
	case "plain":
		if err := validateSASLCredentials(settings); err != nil {
			return nil, err
		}
		return plain.Auth{User: settings.Username, Pass: settings.Password}.AsMechanism(), nil
	case "scram-sha-256":
		if err := validateSASLCredentials(settings); err != nil {
			return nil, err
		}
		return scram.Auth{User: settings.Username, Pass: settings.Password}.AsSha256Mechanism(), nil
	case "scram-sha-512":
		if err := validateSASLCredentials(settings); err != nil {
			return nil, err
		}
		return scram.Auth{User: settings.Username, Pass: settings.Password}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("kafka: unsupported SASL mechanism %q", settings.Mechanism)
	}
}

func validateTLSConfig(endpoints []string, settings *driver.TLSConfig) error {
	if settings != nil && settings.Enabled && settings.InsecureSkipVerify && !isTestEndpoint(endpoints) {
		return errors.New("kafka: insecure TLS is only allowed for a test endpoint")
	}
	return nil
}

func makeTLSConfig(settings *driver.TLSConfig) (*tls.Config, error) {
	config := &tls.Config{InsecureSkipVerify: settings.InsecureSkipVerify} //nolint:gosec // explicitly controlled by the driver config
	if settings.CAFile != "" {
		pem, err := os.ReadFile(settings.CAFile)
		if err != nil {
			return nil, fmt.Errorf("kafka: read TLS CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("kafka: TLS CA file contains no certificates")
		}
		config.RootCAs = pool
	}
	if settings.CertFile == "" && settings.KeyFile == "" {
		return config, nil
	}
	certificate, err := tls.LoadX509KeyPair(settings.CertFile, settings.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("kafka: load TLS client certificate: %w", err)
	}
	config.Certificates = []tls.Certificate{certificate}
	return config, nil
}

func isTestEndpoint(endpoints []string) bool {
	if len(endpoints) == 0 {
		return false
	}
	for _, endpoint := range endpoints {
		if !isLoopbackEndpoint(endpoint) {
			return false
		}
	}
	return true
}

func isLoopbackEndpoint(endpoint string) bool {
	host := endpoint
	if parsedHost, _, err := net.SplitHostPort(endpoint); err == nil {
		host = parsedHost
	} else if strings.Contains(endpoint, ":") {
		host = strings.TrimPrefix(strings.TrimSuffix(endpoint, "]"), "[")
		ip := net.ParseIP(host)
		return ip != nil && ip.IsLoopback()
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func retryOpen(ctx context.Context, err error) error {
	kind := kafkaErrorKind(err)
	if kind != driver.KindTransient {
		return classify("open", kind, err)
	}
	if waitErr := waitRetry(ctx); waitErr != nil {
		return classify("open", driver.KindTransient, errors.Join(waitErr, err))
	}
	return nil
}

func kafkaErrorKind(err error) driver.Kind {
	switch {
	case errors.Is(err, kerr.TopicAuthorizationFailed),
		errors.Is(err, kerr.GroupAuthorizationFailed),
		errors.Is(err, kerr.ClusterAuthorizationFailed),
		errors.Is(err, kerr.TransactionalIDAuthorizationFailed),
		errors.Is(err, kerr.DelegationTokenAuthorizationFailed):
		return driver.KindPermission
	case errors.Is(err, kerr.MessageTooLarge):
		return driver.KindTooLarge
	case errors.Is(err, kerr.UnknownTopicOrPartition):
		return driver.KindNotFound
	case errors.Is(err, kerr.SaslAuthenticationFailed),
		errors.Is(err, kerr.UnsupportedSaslMechanism),
		errors.Is(err, kerr.IllegalSaslState),
		errors.Is(err, kerr.UnsupportedVersion),
		errors.Is(err, kerr.InvalidRequest),
		errors.Is(err, kerr.SecurityDisabled),
		errors.Is(err, errBrokerConfig),
		errors.Is(err, errProtocolResponse):
		return driver.KindFatal
	default:
		return driver.KindTransient
	}
}

func waitRetry(ctx context.Context) error {
	timer := time.NewTimer(250 * time.Millisecond) //nolint:forbidigo // Open retries wait for broker recovery and require a wall-clock delay
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func classify(op string, kind driver.Kind, err error) error {
	if err == nil {
		return nil
	}
	return &driver.Error{Driver: "kafka", Op: op, K: kind, Err: err}
}
