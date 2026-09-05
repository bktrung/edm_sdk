package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
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
)

// Driver is a stateless Kafka driver factory.
type Driver struct{}

type consumeMode string

const classicMode consumeMode = "classic"

// conn owns the single client used for connection, producer, and metadata
// operations. Consumer instances use cloned options so each can own its group.
type conn struct {
	client     *kgo.Client
	clientOpts []kgo.Opt
	caps       driver.Capabilities
	info       driver.BrokerInfo
	consumers  map[*consumer]struct{}
	mu         sync.RWMutex
	closeOnce  sync.Once
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

	info := brokerInfo(metadata, versionResponse)
	caps := classicCapabilities(messageMaxBytes)
	keepClient = true
	return &conn{
		client:     client,
		clientOpts: append([]kgo.Opt(nil), opts...),
		caps:       caps,
		info:       info,
		consumers:  make(map[*consumer]struct{}),
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

func classicCapabilities(maxMessageBytes int) driver.Capabilities {
	caps := Driver{}.Capabilities()
	caps.PerMessageAck = false
	caps.NativeDeliveryCount = false
	caps.ConsumerScaling = driver.ScalingPartitionBound
	// message.max.bytes bounds the record batch, not the value alone; whether
	// an exactly-MaxMessageBytes body is accepted remains an open question.
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
	_ = cfg
	if err := ctx.Err(); err != nil {
		return nil, classify("producer", driver.KindTransient, err)
	}
	return &producer{client: c.client}, nil
}

func (c *conn) Consumer(ctx context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	return newConsumer(ctx, c, cfg)
}

func (c *conn) Admin() driver.Admin { return &admin{client: kadm.NewClient(c.client)} }

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
	c.closeOnce.Do(c.client.Close)
	return nil
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
	timer := time.NewTimer(250 * time.Millisecond) //nolint:forbidigo // Open retries need a wall-clock wait
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
