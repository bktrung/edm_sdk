package rabbitmq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// Local fixture credentials are intentional and never used for production endpoints.
const defaultEndpoint = "amqp://guest:guest@localhost:5672/" //nolint:gosec // test fixture endpoint

var _ driver.Driver = Driver{}

// Driver is a stateless RabbitMQ driver factory.
type Driver struct{}

// Name returns the stable RabbitMQ driver key.
func (Driver) Name() string { return "rabbitmq" }

type queueKind string

const (
	queueKindQuorum  queueKind = "quorum"
	queueKindClassic queueKind = "classic"
)

func configuredQueueKind(options map[string]string) (queueKind, error) {
	switch strings.ToLower(strings.TrimSpace(options["rabbitmq.queueType"])) {
	case "", string(queueKindQuorum):
		return queueKindQuorum, nil
	case string(queueKindClassic):
		return queueKindClassic, nil
	default:
		return "", fmt.Errorf("rabbitmq: unsupported queueType %q; supported values: quorum, classic, or empty", options["rabbitmq.queueType"])
	}
}

func capabilitiesForQueueKind(kind queueKind) driver.Capabilities {
	caps := Driver{}.Capabilities()
	if kind == queueKindClassic {
		caps.NativeDeliveryCount = false
		caps.NativeDLQ = false
	}
	return caps
}

// Capabilities reports the RabbitMQ behavior used by this driver.
func (Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		PerMessageAck:        true,
		OrderedByKey:         false,
		NativePriority:       driver.PriorityStrict,
		NativePriorityLevels: 32,
		NativeDelay:          false,
		NativeDeliveryCount:  true,
		NativeDLQ:            true,
		ConsumerScaling:      driver.ScalingFree,
		Fanout:               driver.FanoutAtPublish,
		LagQueryable:         true,
	}
}

// Open establishes a RabbitMQ connection and retries failed endpoint dials
// until the caller's context or ConnectTimeout expires.
func (Driver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("open", driver.KindTransient, err)
	}

	if err := validateSASL(cfg.SASL); err != nil {
		return nil, classify("open", driver.KindFatal, err)
	}
	queueKind, err := configuredQueueKind(cfg.DriverOptions)
	if err != nil {
		return nil, classify("open", driver.KindFatal, err)
	}
	openCtx := ctx
	if cfg.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		openCtx, cancel = context.WithTimeout(ctx, cfg.ConnectTimeout)
		defer cancel()
	}

	endpoints := cfg.Endpoints
	if len(endpoints) == 0 {
		endpoints = []string{defaultEndpoint}
	}
	var lastErr error
	for {
		for _, endpoint := range endpoints {
			if err := openCtx.Err(); err != nil {
				return nil, classify("open", driver.KindTransient, err)
			}
			conn, err := dial(openCtx, endpoint, cfg)
			if err == nil {
				managedConn, connErr := newConn(conn, capabilitiesForQueueKind(queueKind), endpoint, cfg, queueKind)
				if connErr != nil {
					_ = conn.Close()
					return nil, classify("open", driver.KindFatal, connErr)
				}
				return managedConn, nil
			}
			lastErr = err
		}

		if err := openCtx.Err(); err != nil {
			if lastErr != nil {
				return nil, classify("open", driver.KindTransient, errors.Join(err, lastErr))
			}
			return nil, classify("open", driver.KindTransient, err)
		}
		if err := waitRetry(openCtx); err != nil {
			return nil, classify("open", driver.KindTransient, err)
		}
	}
}

type conn struct {
	mu           sync.RWMutex
	topologyMu   sync.Mutex
	amqp         *amqp.Connection
	caps         driver.Capabilities
	info         driver.BrokerInfo
	queueKind    queueKind
	management   *managementClient
	closed       bool
	closing      bool
	closeAttempt bool
	active       map[*consumer]struct{}
	producers    map[*producer]struct{}
	publishFault atomic.Int32 // 0 = unset; otherwise driver.Kind + 1
	closeFault   atomic.Bool
	deferred     map[string]time.Duration
	ephemeral    map[string]*amqp.Channel
}

var _ driver.Conn = (*conn)(nil)

func newConn(amqpConn *amqp.Connection, caps driver.Capabilities, endpoint string, cfg driver.Config, kind queueKind) (*conn, error) {
	management, err := newManagementClient(endpoint, cfg)
	if err != nil {
		return nil, err
	}
	return &conn{
		amqp:       amqpConn,
		caps:       caps,
		info:       brokerInfo(amqpConn),
		queueKind:  kind,
		management: management,
		active:     make(map[*consumer]struct{}),
		producers:  make(map[*producer]struct{}),
		deferred:   make(map[string]time.Duration),
		ephemeral:  make(map[string]*amqp.Channel),
	}, nil
}

func (c *conn) Capabilities() driver.Capabilities { return c.caps }

func (c *conn) BrokerInfo() driver.BrokerInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return copyBrokerInfo(c.info)
}

func (c *conn) Producer(ctx context.Context, cfg driver.ProducerConfig) (driver.Producer, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("producer", driver.KindTransient, err)
	}
	c.mu.RLock()
	if c.closed || c.closing || c.amqp.IsClosed() {
		c.mu.RUnlock()
		return nil, classify("producer", driver.KindTransient, amqp.ErrClosed)
	}
	c.mu.RUnlock()
	producer, err := newProducer(c, cfg)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.closed || c.closing || c.amqp.IsClosed() {
		c.mu.Unlock()
		_ = producer.channel.Close()
		return nil, classify("producer", driver.KindTransient, amqp.ErrClosed)
	}
	c.producers[producer] = struct{}{}
	c.mu.Unlock()
	return producer, nil
}

func (c *conn) Consumer(ctx context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("consumer", driver.KindTransient, err)
	}
	c.mu.Lock()
	err := c.consumerAdmissionLocked(cfg)
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if len(cfg.Destinations) == 0 {
		return nil, classify("consumer", driver.KindFatal, errors.New("no destinations"))
	}

	consumer, err := newConsumer(c, cfg)
	if err != nil {
		return nil, err
	}
	if hook := consumerConstructionHook; hook != nil {
		hook(consumer, consumerConstructionReady)
	}
	c.mu.Lock()
	err = c.consumerAdmissionLocked(cfg)
	if err == nil {
		c.active[consumer] = struct{}{}
		c.mu.Unlock()
		return consumer, nil
	}
	c.mu.Unlock()
	if releaseErr := consumer.Release(context.WithoutCancel(ctx)); releaseErr != nil {
		return nil, errors.Join(err, releaseErr)
	}
	return nil, err
}

func (c *conn) consumerAdmissionLocked(cfg driver.ConsumerConfig) error {
	if c.closed || c.closing || c.amqp.IsClosed() {
		return classify("consumer", driver.KindTransient, amqp.ErrClosed)
	}
	if cfg.Exclusive {
		for _, destination := range cfg.Destinations {
			for active := range c.active {
				if active.hasDestination(destination) {
					return classify("consumer", driver.KindFatal, fmt.Errorf("exclusive consumer refused: destination %q already has a consumer", destination))
				}
			}
		}
	}
	for active := range c.active {
		if !active.cfg.Exclusive {
			continue
		}
		for _, destination := range cfg.Destinations {
			if active.hasDestination(destination) {
				return classify("consumer", driver.KindFatal, fmt.Errorf("exclusive consumer refused: destination %q already has an exclusive consumer", destination))
			}
		}
	}
	return nil
}

func (c *conn) Admin() driver.Admin { return &admin{operations: &adminOperations{conn: c}} }

func (c *conn) removeConsumer(consumer *consumer) {
	c.mu.Lock()
	delete(c.active, consumer)
	c.mu.Unlock()
}

func (c *conn) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("ping", driver.KindTransient, err)
	}
	c.mu.RLock()
	if c.closed || c.closing || c.amqp.IsClosed() {
		c.mu.RUnlock()
		return classify("ping", driver.KindTransient, amqp.ErrClosed)
	}
	channel, err := c.amqp.Channel()
	c.mu.RUnlock()
	if err != nil {
		return classifyAMQP("ping", driver.KindTransient, err)
	}
	if err := channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		return classifyAMQP("ping", driver.KindTransient, err)
	}
	return nil
}

func (c *conn) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("close", driver.KindTransient, err)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	if c.closeAttempt {
		c.mu.Unlock()
		return classify("close", driver.KindTransient, errors.New("close already in progress"))
	}
	// Reject the precondition before marking teardown started so admission
	// remains open when resources are still outstanding.
	if !c.closing && (len(c.active) != 0 || len(c.producers) != 0) {
		c.mu.Unlock()
		return classify("close", driver.KindFatal, driver.ErrResourcesOutstanding)
	}
	if !c.closing {
		c.closing = true
	}
	c.closeAttempt = true
	deadline, hasDeadline := ctx.Deadline()
	var injectedErr error
	if c.closeFault.Swap(false) {
		if hasDeadline {
			c.mu.Unlock()
			<-ctx.Done()
			c.mu.Lock()
			c.closeAttempt = false
			c.mu.Unlock()
			return classify("close", driver.KindTransient, ctx.Err())
		}
		injectedErr = errors.New("injected close failure")
	}
	amqpConn := c.amqp
	ephemeral := make([]*amqp.Channel, 0, len(c.ephemeral))
	for _, channel := range c.ephemeral {
		ephemeral = append(ephemeral, channel)
	}
	c.ephemeral = nil
	c.mu.Unlock()
	for _, channel := range ephemeral {
		_ = channel.Close()
	}

	var err error
	if injectedErr != nil {
		err = injectedErr
	} else if hasDeadline {
		err = amqpConn.CloseDeadline(deadline)
	} else {
		done := make(chan error, 1)
		go func() { done <- amqpConn.Close() }()
		select {
		case err = <-done:
		case <-ctx.Done():
			c.mu.Lock()
			c.closeAttempt = false
			c.mu.Unlock()
			return classify("close", driver.KindTransient, ctx.Err())
		}
	}
	if errors.Is(err, amqp.ErrClosed) {
		c.mu.Lock()
		c.closeAttempt = false
		c.closed = true
		c.mu.Unlock()
		return nil
	}
	if err != nil {
		c.mu.Lock()
		c.closeAttempt = false
		c.mu.Unlock()
		return classifyAMQP("close", driver.KindTransient, err)
	}
	c.mu.Lock()
	c.closeAttempt = false
	c.closed = true
	c.mu.Unlock()
	return nil
}

func dial(ctx context.Context, endpoint string, cfg driver.Config) (*amqp.Connection, error) {
	amqpConfig, err := makeAMQPConfig(cfg)
	if err != nil {
		return nil, err
	}
	timeout := cfg.ConnectTimeout
	if timeout <= 0 || timeout > 30*time.Second {
		timeout = 30 * time.Second
	}
	dialer := &net.Dialer{Timeout: timeout}
	amqpConfig.Dial = dialer.Dial
	result := make(chan *amqp.Connection, 1)
	errResult := make(chan error, 1)
	go func() {
		connection, dialErr := amqp.DialConfig(endpoint, amqpConfig)
		if dialErr != nil {
			errResult <- dialErr
			return
		}
		result <- connection
	}()
	select {
	case connection := <-result:
		return connection, nil
	case err := <-errResult:
		return nil, err
	case <-ctx.Done():
		go func() {
			if connection := <-result; connection != nil {
				_ = connection.Close()
			}
		}()
		return nil, ctx.Err()
	}
}

func makeAMQPConfig(cfg driver.Config) (amqp.Config, error) {
	config := amqp.Config{Properties: amqp.NewConnectionProperties()}
	if cfg.ClientID != "" {
		config.Properties.SetClientConnectionName(cfg.ClientID)
	}
	if cfg.SASL != nil {
		switch strings.ToLower(cfg.SASL.Mechanism) {
		case "":
		case "plain":
			config.SASL = []amqp.Authentication{&amqp.PlainAuth{
				Username: cfg.SASL.Username,
				Password: cfg.SASL.Password,
			}}
		case "amqplain":
			config.SASL = []amqp.Authentication{&amqp.AMQPlainAuth{
				Username: cfg.SASL.Username,
				Password: cfg.SASL.Password,
			}}
		case "external":
			config.SASL = []amqp.Authentication{&amqp.ExternalAuth{}}
		}
	}
	if cfg.TLS == nil || !cfg.TLS.Enabled {
		return config, nil
	}
	if cfg.TLS.InsecureSkipVerify && !isTestEndpoint(cfg.Endpoints) {
		return config, errors.New("rabbitmq: insecure TLS is only allowed for a test endpoint")
	}
	tlsConfig, err := tlsConfig(cfg.TLS)
	if err != nil {
		return config, err
	}
	config.TLSClientConfig = tlsConfig
	return config, nil
}

func validateSASL(settings *driver.SASLConfig) error {
	if settings == nil {
		return nil
	}
	switch strings.ToLower(settings.Mechanism) {
	case "", "plain", "amqplain", "external":
		return nil
	default:
		return fmt.Errorf("rabbitmq: unsupported SASL mechanism %q; supported mechanisms: PLAIN, AMQPLAIN, EXTERNAL, or empty", settings.Mechanism)
	}
}

func tlsConfig(settings *driver.TLSConfig) (*tls.Config, error) {
	config := &tls.Config{InsecureSkipVerify: settings.InsecureSkipVerify} //nolint:gosec // explicitly controlled by the driver config
	if settings.CAFile != "" {
		pem, err := os.ReadFile(settings.CAFile)
		if err != nil {
			return nil, fmt.Errorf("rabbitmq: read TLS CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("rabbitmq: TLS CA file contains no certificates")
		}
		config.RootCAs = pool
	}
	if settings.CertFile == "" && settings.KeyFile == "" {
		return config, nil
	}
	certificate, err := tls.LoadX509KeyPair(settings.CertFile, settings.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: load TLS client certificate: %w", err)
	}
	config.Certificates = []tls.Certificate{certificate}
	return config, nil
}

// isTestEndpoint reports whether every endpoint in the list resolves to an
// exact loopback literal: the hostname "localhost" or an IP address for
// which net.IP.IsLoopback is true. An empty list or any endpoint that fails
// to parse, or whose host is not an exact loopback literal, makes the whole
// list ineligible for insecure TLS.
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

// isLoopbackEndpoint reports whether the host portion of endpoint is exactly
// "localhost" or an IP address that net.IP.IsLoopback confirms is loopback.
// A hostname that merely contains "localhost" or a loopback IP as a
// substring, such as an attacker-registrable "evil-localhost.example.com",
// does not qualify.
func isLoopbackEndpoint(endpoint string) bool {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return false
	}
	host := parsed.Hostname()
	if host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func waitRetry(ctx context.Context) error {
	timer := time.NewTimer(250 * time.Millisecond) //nolint:forbidigo // connection retries need a wall-clock wait and drivers have no clock port
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func brokerInfo(connection *amqp.Connection) driver.BrokerInfo {
	version := ""
	if value, ok := connection.Properties["version"].(string); ok {
		version = value
	}
	if version == "" && (connection.Major != 0 || connection.Minor != 0) {
		version = fmt.Sprintf("%d.%d", connection.Major, connection.Minor)
	}
	return driver.BrokerInfo{Kind: "rabbitmq", Version: version}
}

func copyBrokerInfo(info driver.BrokerInfo) driver.BrokerInfo {
	info.Nodes = append([]string(nil), info.Nodes...)
	if info.Extra != nil {
		info.Extra = make(map[string]string, len(info.Extra))
		for key, value := range info.Extra {
			info.Extra[key] = value
		}
	}
	return info
}

func classify(op string, kind driver.Kind, err error) error {
	if err == nil {
		return nil
	}
	return &driver.Error{Driver: "rabbitmq", Op: op, K: kind, Err: err}
}

func classifyAMQP(op string, fallback driver.Kind, err error) error {
	if err == nil {
		return nil
	}
	kind := fallback
	var amqpErr *amqp.Error
	if errors.As(err, &amqpErr) {
		switch amqpErr.Code {
		case 403:
			kind = driver.KindPermission
		case 404:
			kind = driver.KindNotFound
			err = errors.Join(driver.ErrDestinationMissing, err)
		case 501, 502, 503, 504:
			kind = driver.KindFatal
		}
	}
	return classify(op, kind, err)
}

func (c *conn) removeProducer(producer *producer) {
	c.mu.Lock()
	delete(c.producers, producer)
	c.mu.Unlock()
}
