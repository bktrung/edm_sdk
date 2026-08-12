package f1

import (
	"context"
	"fmt"
	"sync"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/codec"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// Client is an eagerly connected messaging client.
type Client struct {
	conn      driver.Conn
	limits    Limits
	effective driver.Capabilities
	options   clientOptions

	mu     sync.Mutex
	closed bool
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
	options := clientOptions{codec: codec.JSON{}, clock: clock.NewReal()}
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
	client := &Client{conn: connection, effective: effective, options: options}
	client.limits = limitsFor(options.driver.Name(), connection.BrokerInfo(), effective)
	return client, nil
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

// Close releases the driver connection. It is safe to call repeatedly after a
// successful close; resource-outstanding errors leave the Client open so the
// caller can close its resources and retry.
func (c *Client) Close(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	if err := c.conn.Close(ctx); err != nil {
		return err
	}
	c.closed = true
	return nil
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
