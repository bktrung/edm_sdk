package f1

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/codec"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// Client is an eagerly connected messaging client.
type Client struct {
	conn           driver.Conn
	limits         Limits
	effective      driver.Capabilities
	options        clientOptions
	config         Config
	source         string
	producer       string
	driver         string
	producerHandle driver.Producer

	mu              sync.Mutex
	closed          bool
	closing         bool
	activePublishes int
	publishIdle     chan struct{}
	runners         map[*Runner]struct{}
}

// Limits describes how the connected broker provides each SDK feature.
type Limits struct {
	Driver   string
	Broker   string
	Features []FeatureStatus
}

// FeatureStatus describes one feature under the connected broker.
type FeatureStatus struct {
	Feature string
	Mode    FeatureMode
	Detail  string
}

// FeatureMode describes whether a feature is native, emulated, or unavailable.
type FeatureMode int

const (
	// FeatureNative means the connected broker provides the feature directly.
	FeatureNative FeatureMode = iota
	// FeatureEmulated means the core provides the feature with fixed semantics.
	FeatureEmulated
	// FeatureUnavailable means the feature cannot be provided by this client.
	FeatureUnavailable
)

// New applies opts, validates them, and opens the supplied driver before
// returning. Startup errors are returned before any publish or subscribe call.
// Until subscription construction supplies Go-side defaults, cfg is expected
// to come from LoadConfig; hand-built Config values are validated as supplied.
func New(ctx context.Context, cfg Config, opts ...Option) (*Client, error) {
	options := clientOptions{codec: codec.JSON{}, clock: clock.NewReal(), logger: slog.Default()}
	for _, option := range opts {
		if option == nil {
			return nil, fmt.Errorf("f1: nil option")
		}
		if err := option(&options); err != nil {
			return nil, err
		}
	}
	if options.driver == nil {
		return nil, fmt.Errorf("f1: New requires WithDriver")
	}
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	connection, err := options.driver.Open(ctx, driverConfig(cfg))
	if err != nil {
		return nil, fmt.Errorf("f1: open %s driver: %w", options.driver.Name(), err)
	}
	capabilities := connection.Capabilities()
	effective := capabilities
	if options.strictPortability {
		effective = effective.Strict()
	}
	client := &Client{
		conn:      connection,
		effective: effective,
		options:   options,
		config:    cfg,
		source:    fmt.Sprintf("/%s/%s", cfg.Env, cfg.Service),
		producer:  fmt.Sprintf("%s/unknown/%s", cfg.Service, cfg.InstanceID),
		driver:    options.driver.Name(),
		runners:   make(map[*Runner]struct{}),
	}
	client.limits = limitsFor(options.driver.Name(), connection.BrokerInfo(), effective)
	logCapabilities(client)
	return client, nil
}

// Publisher returns a publisher using this client's connected driver and
// configured codec. Publisher options are reserved for future per-publisher
// controls and currently have no effect.
func (c *Client) Publisher(_ ...PublisherOption) *Publisher {
	return &Publisher{client: c}
}

func logCapabilities(c *Client) {
	if c.options.logger == nil {
		return
	}
	for _, feature := range c.limits.Features {
		if feature.Mode == FeatureNative {
			continue
		}
		attrs := []any{"driver", c.limits.Driver, "broker", c.limits.Broker, "feature", feature.Feature, "mode", featureModeString(feature.Mode)}
		if feature.Detail != "" {
			attrs = append(attrs, "detail", feature.Detail)
		}
		if feature.Mode == FeatureUnavailable {
			c.options.logger.Warn("f1 capability unavailable", attrs...)
		} else {
			c.options.logger.Info("f1 capability emulated", attrs...)
		}
	}
}

func featureModeString(mode FeatureMode) string {
	switch mode {
	case FeatureNative:
		return "native"
	case FeatureEmulated:
		return "emulated"
	default:
		return "unavailable"
	}
}

// Health reports whether the connected broker is reachable.
func (c *Client) Health(ctx context.Context) error {
	if c == nil {
		return fmt.Errorf("f1: client is not connected")
	}
	c.mu.Lock()
	if c.conn == nil {
		c.mu.Unlock()
		return fmt.Errorf("f1: client is not connected")
	}
	if c.closed {
		c.mu.Unlock()
		return fmt.Errorf("f1: client is closed")
	}
	if c.closing {
		c.mu.Unlock()
		return fmt.Errorf("f1: client is closing")
	}
	conn := c.conn
	c.mu.Unlock()
	return conn.Ping(ctx)
}

// Limits returns the connected broker's stable capability report.
func (c *Client) Limits() Limits {
	if c == nil {
		return Limits{}
	}
	result := c.limits
	result.Features = append([]FeatureStatus(nil), c.limits.Features...)
	return result
}

// Close flushes and releases the driver resources. It is safe to call
// repeatedly after a successful close; flush or connection errors leave the
// Client open so the caller can retry. A producer close error is returned
// after the connection has still been released. A concurrent Close call
// returns an error stating that shutdown is already in progress.
func (c *Client) Close(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	if c.closing {
		c.mu.Unlock()
		return fmt.Errorf("f1: client is closing")
	}
	c.closing = true
	idle := c.publishIdle
	runners := make([]*Runner, 0, len(c.runners))
	for runner := range c.runners {
		runners = append(runners, runner)
	}
	c.mu.Unlock()
	for _, runner := range runners {
		if err := runner.Drain(ctx); err != nil {
			c.mu.Lock()
			c.closing = false
			c.mu.Unlock()
			return err
		}
	}
	if idle != nil {
		select {
		case <-idle:
		case <-ctx.Done():
			c.mu.Lock()
			c.closing = false
			c.mu.Unlock()
			return ctx.Err()
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.producerHandle != nil {
		if err := c.producerHandle.Flush(ctx); err != nil {
			c.closing = false
			return err
		}
		producerCloseErr := c.producerHandle.Close(ctx)
		c.producerHandle = nil
		if producerCloseErr != nil && c.options.logger != nil {
			c.options.logger.Warn("f1 producer close failed", "error", producerCloseErr)
		}
		if err := c.conn.Close(ctx); err != nil {
			c.closing = false
			return errors.Join(producerCloseErr, err)
		}
		c.closed = true
		c.closing = false
		return producerCloseErr
	}
	if err := c.conn.Close(ctx); err != nil {
		c.closing = false
		return err
	}
	c.closed = true
	c.closing = false
	return nil
}

// publishMessages sends core-generated successor messages through the client's
// shared producer. allowClosing is reserved for workers finishing a delivery
// after Close has stopped admission of new application publishes.
func publishMessages(c *Client, ctx context.Context, allowClosing bool, messages ...driver.OutboundMessage) error {
	if len(messages) == 0 {
		return nil
	}
	c.mu.Lock()
	if c.closed || c.conn == nil || (!allowClosing && c.closing) {
		c.mu.Unlock()
		return errors.New("f1: client is closed")
	}
	producer := c.producerHandle
	var err error
	if producer == nil {
		producer, err = c.conn.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true, Effective: c.effective})
		if err == nil && producer == nil {
			err = errors.New("driver returned a nil producer")
		}
		if err == nil {
			c.producerHandle = producer
		}
	}
	if err == nil {
		beginPublish(c)
	}
	c.mu.Unlock()
	if err != nil {
		return err
	}
	defer endPublish(c)
	return producer.Publish(ctx, messages...)
}

func beginPublish(c *Client) {
	c.activePublishes++
	if c.activePublishes == 1 {
		c.publishIdle = make(chan struct{})
	}
}

func endPublish(c *Client) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.activePublishes--
	if c.activePublishes == 0 {
		close(c.publishIdle)
		c.publishIdle = nil
	}
}

func driverConfig(cfg Config) driver.Config {
	result := driver.Config{Endpoints: append([]string(nil), cfg.Broker.Endpoints...), ClientID: cfg.InstanceID, ConnectTimeout: cfg.Broker.ConnectTimeout, DriverOptions: cloneOptions(cfg.Broker.DriverOptions)}
	if result.ClientID == "" {
		result.ClientID = cfg.Service
	}
	if cfg.Broker.TLS.Enabled {
		tls := cfg.Broker.TLS
		result.TLS = &tls
	}
	if cfg.Broker.SASL.Mechanism != "" {
		sasl := cfg.Broker.SASL
		result.SASL = &sasl
	}
	return result
}

func cloneOptions(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func limitsFor(driverName string, info driver.BrokerInfo, caps driver.Capabilities) Limits {
	feature := func(name string, enabled bool, missing FeatureMode) FeatureStatus {
		if enabled {
			return FeatureStatus{Feature: name, Mode: FeatureNative}
		}
		return FeatureStatus{Feature: name, Mode: missing}
	}
	return Limits{Driver: driverName, Broker: info.Display(), Features: []FeatureStatus{
		feature("per_message_ack", caps.PerMessageAck, FeatureEmulated),
		feature("ordered_by_key", caps.OrderedByKey, FeatureUnavailable),
		{Feature: "priority_fairness", Mode: FeatureEmulated},
		feature("native_delay", caps.NativeDelay, FeatureEmulated),
		feature("delivery_count", caps.NativeDeliveryCount, FeatureEmulated),
		feature("dlq_backstop", caps.NativeDLQ, FeatureUnavailable),
		feature("lag_metrics", caps.LagQueryable, FeatureUnavailable),
		{Feature: "consumer_scaling", Mode: FeatureNative, Detail: caps.ConsumerScaling.String()},
	}}
}
