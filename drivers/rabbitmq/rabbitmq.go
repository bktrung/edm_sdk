package rabbitmq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
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
				return newConn(conn, Driver{}.Capabilities(), endpoint), nil
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
	mu         sync.RWMutex
	topologyMu sync.Mutex
	amqp       *amqp.Connection
	caps       driver.Capabilities
	info       driver.BrokerInfo
	management *managementClient
	closed     bool
	active     map[*consumer]struct{}
	producers  map[*producer]struct{}
	exchanges  map[string]struct{}
	bindings   map[bindingKey]bool
	deferred   map[string]time.Duration
	ephemeral  map[string]*amqp.Channel
}

var _ driver.Conn = (*conn)(nil)

func newConn(amqpConn *amqp.Connection, caps driver.Capabilities, endpoint string) *conn {
	return &conn{
		amqp:       amqpConn,
		caps:       caps,
		info:       brokerInfo(amqpConn),
		management: newManagementClient(endpoint),
		active:     make(map[*consumer]struct{}),
		producers:  make(map[*producer]struct{}),
		exchanges:  make(map[string]struct{}),
		bindings:   make(map[bindingKey]bool),
		deferred:   make(map[string]time.Duration),
		ephemeral:  make(map[string]*amqp.Channel),
	}
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
	if c.closed || c.amqp.IsClosed() {
		c.mu.RUnlock()
		return nil, classify("producer", driver.KindTransient, amqp.ErrClosed)
	}
	c.mu.RUnlock()
	producer, err := newProducer(c, cfg)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.closed || c.amqp.IsClosed() {
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
	defer c.mu.Unlock()
	if c.closed || c.amqp.IsClosed() {
		return nil, classify("consumer", driver.KindTransient, amqp.ErrClosed)
	}
	if len(cfg.Destinations) == 0 {
		return nil, classify("consumer", driver.KindFatal, errors.New("no destinations"))
	}
	consumer, err := newConsumer(c, cfg)
	if err != nil {
		return nil, err
	}
	c.active[consumer] = struct{}{}
	return consumer, nil
}

func (c *conn) Admin() driver.Admin { return &admin{conn: c} }

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
	if c.closed || c.amqp.IsClosed() {
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
	if len(c.active) != 0 || len(c.producers) != 0 {
		c.mu.Unlock()
		return classify("close", driver.KindFatal, driver.ErrResourcesOutstanding)
	}
	c.closed = true
	deadline, hasDeadline := ctx.Deadline()
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
	if hasDeadline {
		err = amqpConn.CloseDeadline(deadline)
	} else {
		done := make(chan error, 1)
		go func() { done <- amqpConn.Close() }()
		select {
		case err = <-done:
		case <-ctx.Done():
			return classify("close", driver.KindTransient, ctx.Err())
		}
	}
	if errors.Is(err, amqp.ErrClosed) {
		return nil
	}
	if err != nil {
		return classifyAMQP("close", driver.KindTransient, err)
	}
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

func isTestEndpoint(endpoints []string) bool {
	if len(endpoints) == 0 {
		return true
	}
	for _, endpoint := range endpoints {
		if strings.Contains(endpoint, "localhost") || strings.Contains(endpoint, "127.0.0.1") {
			return true
		}
	}
	return false
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
