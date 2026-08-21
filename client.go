package f1

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/codec"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/obs"
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
	producerHandle driver.Producer
	metrics        *obs.Metrics

	mu     sync.Mutex
	closed bool
	// closing tracks only a Close attempt currently executing. It is cleared
	// on failure so a retried Close can rejoin pending work.
	closing bool
	// shutdownStarted is guarded by mu, set once when Close is entered, and
	// never cleared. closed is terminal. It is separate from closing because
	// admission must stay closed after shutdown begins while Close remains retryable.
	shutdownStarted bool
	activePublishes int
	publishIdle     chan struct{}
	runners         map[*Runner]struct{}

	// producerCloseWait holds a still-running producer Close call from a
	// prior Close attempt that did not return within its close timeout. A
	// retried Close rejoins this same call instead of starting a second one
	// against the same producer.
	producerCloseWait <-chan error
	// flushWait holds a still-running producer Flush call from a prior Close
	// attempt. A retried Close rejoins this same call instead of starting a
	// second one against the same producer.
	flushWait <-chan error
	// connCloseWait holds a still-running connection Close call from a prior
	// Close attempt. A retried Close rejoins this same call instead of starting
	// a second one against the same connection.
	connCloseWait <-chan error
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

// New applies opts, normalizes and validates cfg, and opens the supplied driver
// before returning. Startup errors are returned before any publish or subscribe
// call. Env and Service remain required for hand-built configurations.
func New(ctx context.Context, cfg Config, opts ...Option) (*Client, error) {
	jsonCodec := codec.JSON{}
	options := clientOptions{
		codec:               jsonCodec,
		codecsByContentType: map[string]codec.Codec{jsonCodec.ContentType(): jsonCodec},
		codecsByName:        map[string]codec.Codec{jsonCodec.Name(): jsonCodec},
		clock:               clock.NewReal(),
		logger:              slog.Default(),
	}
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
	cfg = normalizeConfig(cfg)
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	if _, ok := options.codecsByName[cfg.Codec.Default]; !ok {
		return nil, fmt.Errorf("f1: codec.default %q is not registered", cfg.Codec.Default)
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
	metrics, err := obs.NewMetrics(options.meterProvider)
	if err != nil {
		_ = connection.Close(ctx)
		return nil, fmt.Errorf("f1: initialize metrics: %w", err)
	}
	client := &Client{
		conn:      connection,
		effective: effective,
		options:   options,
		config:    cfg,
		source:    fmt.Sprintf("/%s/%s", cfg.Env, cfg.Service),
		producer:  fmt.Sprintf("%s/unknown/%s", cfg.Service, cfg.InstanceID),
		metrics:   metrics,
		runners:   make(map[*Runner]struct{}),
	}
	client.limits = limitsFor(options.driver.Name(), connection.BrokerInfo(), effective)
	logCapabilities(client)
	if err := client.ensurePublisherTopology(ctx); err != nil {
		closeErr := connection.Close(ctx)
		metricsErr := metrics.Close()
		return nil, errors.Join(err, closeErr, metricsErr)
	}
	return client, nil
}

func (c *Client) topologyPolicy() driver.TopologyPolicy {
	if c.options.topologyPolicySet {
		return c.options.topologyPolicy
	}
	if c.config.Topology.AutoCreate {
		return driver.TopologyDeclare
	}
	if c.config.Topology.VerifyOnStart {
		return driver.TopologyVerify
	}
	return driver.TopologyNone
}

func (c *Client) ensurePublisherTopology(ctx context.Context) error {
	policy := c.topologyPolicy()
	if !c.options.publishTopicsSet || policy == driver.TopologyNone {
		return nil
	}
	admin := c.conn.Admin()
	if admin == nil {
		return errors.New("f1: publisher topology requires driver admin")
	}
	spec := publisherTopologySpec(c.effective, c.source, c.options.publishTopics, c.config.Topology.Priorities)
	spec.Policy = policy
	if _, err := admin.EnsureTopology(ctx, spec); err != nil {
		return fmt.Errorf("f1: ensure publisher topology: %w", err)
	}
	return nil
}

// Publisher returns a publisher using this client's connected driver and
// configured codec.
func (c *Client) Publisher() *Publisher {
	return &Publisher{client: c}
}

func (c *Client) codecForContentType(contentType string) (codec.Codec, error) {
	if contentType == "" {
		selected, ok := c.options.codecsByName[c.config.Codec.Default]
		if !ok {
			return nil, fmt.Errorf("f1: codec.default %q is not registered", c.config.Codec.Default)
		}
		return selected, nil
	}
	selected, ok := c.options.codecsByContentType[contentType]
	if !ok {
		return nil, fmt.Errorf("f1: no codec registered for datacontenttype %q", contentType)
	}
	return selected, nil
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
		warn := feature.Mode == FeatureUnavailable && capabilityRequired(c, feature.Feature)
		if warn {
			c.options.logger.Warn("f1 capability unavailable", attrs...)
		} else if feature.Mode == FeatureUnavailable {
			c.options.logger.Info("f1 capability unavailable", attrs...)
		} else {
			c.options.logger.Info("f1 capability emulated", attrs...)
		}
	}
}

func capabilityRequired(c *Client, feature string) bool {
	if c == nil || feature != "ordered_by_key" {
		return false
	}
	for _, subscription := range c.config.Subscriptions {
		if subscription.Mode == OrderedByKey {
			return true
		}
	}
	return false
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
	if c.shutdownStarted {
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

// Close drains active work and releases the driver resources. It keeps the
// Client retryable when a shutdown phase is still pending, while refusing new
// work after shutdown has begun. A timed-out phase continues in the
// background, and a retried Close rejoins it rather than starting a second
// driver call. A resolved producer-close error is logged, joined into the
// returned error, and does not prevent connection shutdown. A resolved
// connection-close error leaves the Client retryable so a later Close can
// attempt it again. Once all phases have finished, the Client is closed even
// when one of them returned an error. A concurrent Close call returns an error
// stating that shutdown is already in progress.
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
	c.shutdownStarted = true
	idle := c.publishIdle
	runners := make([]*Runner, 0, len(c.runners))
	for runner := range c.runners {
		runners = append(runners, runner)
	}
	c.mu.Unlock()

	fail := func(err error) error {
		c.mu.Lock()
		c.closing = false
		c.mu.Unlock()
		return err
	}
	runnerErrors := make(chan error, len(runners))
	var drain sync.WaitGroup
	drain.Add(len(runners))
	for _, runner := range runners {
		go func(runner *Runner) {
			defer drain.Done()
			if err := runner.Drain(ctx); err != nil {
				runnerErrors <- err
			}
		}(runner)
	}
	drain.Wait()
	close(runnerErrors)
	var drainErrors []error
	for err := range runnerErrors {
		drainErrors = append(drainErrors, err)
	}
	if err := errors.Join(drainErrors...); err != nil {
		return fail(err)
	}
	if idle != nil {
		idleTimeout := c.config.Lifecycle.DrainTimeout
		err := runWithClockTimeout(ctx, c.options.clock, idleTimeout, "drain", func(waitCtx context.Context) error {
			select {
			case <-idle:
				return nil
			case <-waitCtx.Done():
				return waitCtx.Err()
			}
		})
		if err != nil {
			return fail(err)
		}
	}

	c.mu.Lock()
	producer := c.producerHandle
	conn := c.conn
	flushWait := c.flushWait
	producerCloseWait := c.producerCloseWait
	connCloseWait := c.connCloseWait
	c.mu.Unlock()
	if producer != nil {
		if producerCloseWait == nil {
			if flushWait == nil {
				//nolint:contextcheck // this shutdown call must outlive the attempt and is rejoined on retry.
				flushWait = startShutdownPhase(context.Background(), nil, producer.Flush)
				c.mu.Lock()
				c.flushWait = flushWait
				c.mu.Unlock()
			}
			flushErr, resolved := c.joinShutdownPhase(ctx, c.config.Lifecycle.FlushTimeout, "flush", flushWait)
			if !resolved {
				return fail(flushErr)
			}
			c.mu.Lock()
			c.flushWait = nil
			c.mu.Unlock()
			if flushErr != nil {
				return fail(flushErr)
			}
		}

		if producerCloseWait == nil {
			//nolint:contextcheck // this shutdown call must outlive the attempt and is rejoined on retry.
			producerCloseWait = startShutdownPhase(context.Background(), nil, producer.Close)
			c.mu.Lock()
			c.producerCloseWait = producerCloseWait
			c.mu.Unlock()
		}
		producerCloseErr, resolved := c.joinShutdownPhase(ctx, c.config.Lifecycle.CloseTimeout, "close", producerCloseWait)
		if !resolved {
			// producer.Close has not returned. The driver's Close contract
			// requires every Producer created from a connection to be closed
			// first, so Conn.Close must not run while producer.Close may
			// still be in flight against the same connection. Keep the
			// pending call so a retried Close rejoins it instead of starting
			// a second one, and report the failure without ever touching
			// the connection.
			if c.options.logger != nil {
				c.options.logger.Warn("f1 producer close did not return in time", "error", producerCloseErr)
			}
			return fail(producerCloseErr)
		}
		c.mu.Lock()
		c.producerCloseWait = nil
		c.producerHandle = nil
		c.mu.Unlock()
		if producerCloseErr != nil && c.options.logger != nil {
			c.options.logger.Warn("f1 producer close failed", "error", producerCloseErr)
		}
		if connCloseWait == nil {
			//nolint:contextcheck // this shutdown call must outlive the attempt and is rejoined on retry.
			connCloseWait = startShutdownPhase(context.Background(), nil, conn.Close)
			c.mu.Lock()
			c.connCloseWait = connCloseWait
			c.mu.Unlock()
		}
		connErr, resolved := c.joinShutdownPhase(ctx, c.config.Lifecycle.CloseTimeout, "close", connCloseWait)
		if !resolved {
			return fail(errors.Join(producerCloseErr, connErr))
		}
		c.mu.Lock()
		c.connCloseWait = nil
		c.mu.Unlock()
		if connErr != nil {
			return fail(errors.Join(producerCloseErr, connErr))
		}
		c.mu.Lock()
		c.closed = true
		c.closing = false
		c.mu.Unlock()
		return errors.Join(producerCloseErr, c.metrics.Close())
	}
	if connCloseWait == nil {
		//nolint:contextcheck // this shutdown call must outlive the attempt and is rejoined on retry.
		connCloseWait = startShutdownPhase(context.Background(), nil, conn.Close)
		c.mu.Lock()
		c.connCloseWait = connCloseWait
		c.mu.Unlock()
	}
	connErr, resolved := c.joinShutdownPhase(ctx, c.config.Lifecycle.CloseTimeout, "close", connCloseWait)
	if !resolved {
		return fail(connErr)
	}
	c.mu.Lock()
	c.connCloseWait = nil
	c.mu.Unlock()
	if connErr != nil {
		return fail(connErr)
	}
	c.mu.Lock()
	c.closed = true
	c.closing = false
	c.mu.Unlock()
	return c.metrics.Close()
}

// startShutdownPhase starts a shutdown call with the context supplied by
// Close. Close supplies Background so the call can outlive the attempt.
func startShutdownPhase(ctx context.Context, pending <-chan error, fn func(context.Context) error) <-chan error {
	if pending != nil {
		return pending
	}
	return startPhase(ctx, fn)
}

func (c *Client) joinShutdownPhase(ctx context.Context, timeout time.Duration, phase string, done <-chan error) (error, bool) {
	return joinPhase(ctx, c.options.clock, timeout, phase, done)
}

// publishMessages sends core-generated successor messages through the client's
// shared producer. allowClosing is reserved for workers finishing a delivery
// after Close has stopped admission of new application publishes.
func publishMessages(c *Client, ctx context.Context, allowClosing bool, messages ...driver.OutboundMessage) error {
	if len(messages) == 0 {
		return nil
	}
	c.mu.Lock()
	if c.closed || c.conn == nil || (!allowClosing && c.shutdownStarted) {
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
