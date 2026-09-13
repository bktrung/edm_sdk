package f1

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"sync"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/codec"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

var errClientReconnecting = errors.New("f1: client is reconnecting")

const reconnectDriverInitialInterval = 500 * time.Millisecond

const reconnectDriverMaxInterval = 30 * time.Second

type reconnectAttempt struct {
	done chan struct{}
	err  error
}

type reconnectRequest struct {
	cause error
}

type failedSubscription struct {
	name string
	err  error
}

// Client is an eagerly connected messaging client.
type Client struct {
	conn           driver.Conn
	limits         Limits
	effective      driver.Capabilities
	options        clientOptions
	driverName     string
	config         Config
	source         string
	producer       string
	producerHandle driver.Producer

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
	// producerTeardown is guarded by mu, set once when the publish-idle wait
	// observes zero in-flight publishes, and never cleared. Setting it in the
	// same critical section as that observation closes admission for
	// core-generated successor publishes, so flush and producer close can
	// never run against a publish admitted after the wait gave its answer.
	producerTeardown    bool
	runners             map[*Runner]struct{}
	failedSubscriptions []failedSubscription

	// reconnecting reports connection usability, independently of shutdownStarted.
	reconnecting bool
	// reconnectErr records a terminal reconnect decision for the current client
	// state. It is guarded by mu and remains separate from shutdownStarted:
	// reconnect exhaustion does not mean Close has been entered.
	reconnectErr      error
	reconnect         *reconnectAttempt
	reconnectRequests chan reconnectRequest
	reconnectRandom   func() float64
	supervisorCtx     context.Context
	supervisorCancel  context.CancelFunc
	supervisorDone    chan struct{}

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
	driverName := options.driver.Name()
	configuredDriverName := cfg.Broker.Driver
	if driverName != configuredDriverName {
		logDriverIdentityMismatch(options.logger, configuredDriverName, driverName)
	}
	cfg = normalizeConfig(cfg)
	if err := validateConfig(cfg, driverName); err != nil {
		return nil, err
	}

	if _, ok := options.codecsByName[cfg.Codec.Default]; !ok {
		return nil, fmt.Errorf("f1: codec.default %q is not registered", cfg.Codec.Default)
	}
	connection, err := options.driver.Open(ctx, driverConfig(cfg, options.logger))
	if err != nil {
		return nil, fmt.Errorf("f1: open %s driver: %w", driverName, err)
	}
	if connection == nil {
		return nil, fmt.Errorf("f1: open %s driver: driver returned a nil connection", driverName)
	}
	capabilities := connection.Capabilities()
	effective := capabilities
	if options.strictPortability {
		effective = effective.Strict()
	}
	supervisorCtx, supervisorCancel := context.WithCancel(context.Background())
	client := &Client{
		conn:              connection,
		effective:         effective,
		options:           options,
		driverName:        driverName,
		config:            cfg,
		source:            fmt.Sprintf("/%s/%s", cfg.Env, cfg.Service),
		producer:          fmt.Sprintf("%s/unknown/%s", cfg.Service, cfg.InstanceID),
		reconnectRequests: make(chan reconnectRequest, 1),
		reconnectRandom:   rand.Float64,
		supervisorCtx:     supervisorCtx,
		supervisorCancel:  supervisorCancel,
		supervisorDone:    make(chan struct{}),
		runners:           make(map[*Runner]struct{}),
	}
	client.limits = limitsFor(driverName, connection.BrokerInfo(), effective)
	logCapabilities(client)
	if err := client.ensurePublisherTopology(ctx); err != nil {
		supervisorCancel()
		closeErr := connection.Close(ctx)
		return nil, errors.Join(err, closeErr)
	}
	go client.reconnectSupervisor()
	return client, nil
}

func logDriverIdentityMismatch(logger *slog.Logger, configuredName, injectedName string) {
	if logger == nil {
		return
	}
	logger.Warn("f1 driver identity mismatch",
		"configured_driver", configuredName,
		"injected_driver", injectedName,
	)
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
	c.mu.Lock()
	conn := c.conn
	effective := c.effective
	c.mu.Unlock()
	return c.ensurePublisherTopologyOn(ctx, conn, effective)
}

func (c *Client) ensurePublisherTopologyOn(ctx context.Context, conn driver.Conn, effective driver.Capabilities) error {
	policy := c.topologyPolicy()
	if !c.options.publishTopicsSet || policy == driver.TopologyNone {
		return nil
	}
	if conn == nil {
		return errors.New("f1: publisher topology requires a connected driver")
	}
	admin := conn.Admin()
	if admin == nil {
		return errors.New("f1: publisher topology requires driver admin")
	}
	spec := publisherTopologySpec(effective, c.source, c.options.publishTopics, c.config.Topology.Priorities)
	spec.Policy = policy
	diff, err := admin.EnsureTopology(ctx, spec)
	if err != nil {
		return fmt.Errorf("f1: ensure publisher topology: %w", err)
	}
	logTopologyDrift(lastResortClientLogger(c), diff)
	return nil
}

func logTopologyDrift(logger *slog.Logger, diff driver.TopologyDiff) {
	for _, drift := range diff.Drifted {
		logger.Warn("f1 topology argument drift",
			"destination", drift.Name,
			"argument", drift.Argument,
			"want", drift.Want,
			"got", drift.Got,
		)
	}
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

func configuredClientLogger(c *Client) *slog.Logger {
	if c == nil {
		return nil
	}
	return c.options.logger
}

func lastResortClientLogger(c *Client) *slog.Logger {
	if logger := configuredClientLogger(c); logger != nil {
		return logger
	}
	return slog.Default()
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

// Health reports whether the connected broker and active subscriptions are healthy.
func (c *Client) Health(ctx context.Context) error {
	if c == nil {
		return fmt.Errorf("f1: client is not connected")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return fmt.Errorf("f1: client is closed")
	}
	if c.reconnectErr != nil {
		err := c.reconnectErr
		c.mu.Unlock()
		return err
	}
	if c.conn == nil {
		c.mu.Unlock()
		return fmt.Errorf("f1: client is not connected")
	}
	if c.reconnecting {
		c.mu.Unlock()
		return fmt.Errorf("f1: client is reconnecting")
	}
	if c.shutdownStarted {
		c.mu.Unlock()
		return fmt.Errorf("f1: client is closing")
	}
	conn := c.conn
	c.mu.Unlock()
	if err := conn.Ping(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	failedErr := failedRunnerHealthLocked(c)
	c.mu.Unlock()
	return failedErr
}

func failedRunnerHealthLocked(c *Client) error {
	var errs []error
	for _, failed := range c.failedSubscriptions {
		if failed.err != nil {
			errs = append(errs, fmt.Errorf("f1: subscription %s failed: %w", failed.name, failed.err))
		}
	}
	return errors.Join(errs...)
}

func (c *Client) recordFailedRunner(runner *Runner, err error) {
	if c == nil || runner == nil || err == nil {
		return
	}
	c.mu.Lock()
	c.failedSubscriptions = append(c.failedSubscriptions, failedSubscription{name: runner.subscription.Name, err: err})
	c.mu.Unlock()
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
	runners, alreadyClosed, err := c.beginClose()
	if err != nil {
		return err
	}
	if alreadyClosed {
		return nil
	}
	if err := c.drainRunners(ctx, runners); err != nil {
		return c.failClose(err)
	}
	if err := c.waitForPublishes(ctx); err != nil {
		return c.failClose(err)
	}
	return c.closeResources(ctx)
}

func (c *Client) beginClose() ([]*Runner, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, true, nil
	}
	if c.closing {
		return nil, false, fmt.Errorf("f1: client is closing")
	}
	c.closing = true
	c.shutdownStarted = true
	runners := make([]*Runner, 0, len(c.runners))
	supervisorCancel := c.supervisorCancel
	for runner := range c.runners {
		runners = append(runners, runner)
	}
	if supervisorCancel != nil {
		supervisorCancel()
	}
	return runners, false, nil
}

func (c *Client) failClose(err error) error {
	c.mu.Lock()
	c.closing = false
	c.mu.Unlock()
	return err
}

// drainRunners waits for every subscription runner to finish draining. It is
// the one Close phase whose budget is optional: ConsumerDrainTimeout bounds
// the wait the same way runWithClockTimeout bounds its sibling phases, and a
// zero value keeps the historical shape in which only the caller's context
// can end it. The bound exists because a driver-side wedge between cancel
// and finishRunner would otherwise hold Close open no matter what the rest
// of the shutdown discipline promised.
func (c *Client) drainRunners(ctx context.Context, runners []*Runner) error {
	return runWithClockTimeout(ctx, c.options.clock, c.config.Lifecycle.ConsumerDrainTimeout, "consumer drain", func(drainCtx context.Context) error {
		runnerErrors := make(chan error, len(runners))
		var drain sync.WaitGroup
		drain.Add(len(runners))
		for _, runner := range runners {
			go func(runner *Runner) {
				defer drain.Done()
				if err := runner.Drain(drainCtx); err != nil {
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
		return errors.Join(drainErrors...)
	})
}

// publishQuiescence waits until the client has no publish in flight. A
// single observation of the idle channel goes stale twice over: it reads nil
// when nothing is publishing yet, and a generation it holds is closed while
// a successor publish immediately starts the next one. The wait therefore
// re-reads the live publish state under c.mu after every generation closes,
// so each answer names the state at that moment. onQuiescent, when set, runs
// under c.mu at the instant zero is observed, letting the caller bar further
// admissions atomically with the observation.
func (c *Client) publishQuiescence(waitCtx context.Context, onQuiescent func()) error {
	for {
		c.mu.Lock()
		if c.activePublishes == 0 {
			if onQuiescent != nil {
				onQuiescent()
			}
			c.mu.Unlock()
			return nil
		}
		idle := c.publishIdle
		c.mu.Unlock()
		select {
		case <-idle:
		case <-waitCtx.Done():
			return waitCtx.Err()
		}
	}
}

func (c *Client) waitForPublishes(ctx context.Context) error {
	idleTimeout := c.config.Lifecycle.DrainTimeout
	return runWithClockTimeout(ctx, c.options.clock, idleTimeout, "drain", func(waitCtx context.Context) error {
		return c.publishQuiescence(waitCtx, func() {
			c.producerTeardown = true
		})
	})
}

func (c *Client) closeResources(ctx context.Context) error {
	c.mu.Lock()
	producer := c.producerHandle
	conn := c.conn
	c.mu.Unlock()
	if producer == nil {
		return c.closeConnection(ctx, conn, nil)
	}
	if err := c.flushProducer(ctx, producer); err != nil {
		return c.failClose(err)
	}
	producerCloseErr, resolved := c.closeProducer(ctx, producer)
	if !resolved {
		return c.failClose(producerCloseErr)
	}
	return c.closeConnection(ctx, conn, producerCloseErr)
}

func (c *Client) flushProducer(ctx context.Context, producer driver.Producer) error {
	c.mu.Lock()
	producerCloseWait := c.producerCloseWait
	flushWait := c.flushWait
	c.mu.Unlock()
	if producerCloseWait != nil {
		return nil
	}
	if flushWait == nil {
		//nolint:contextcheck // this shutdown call must outlive the attempt and is rejoined on retry.
		flushWait = startShutdownPhase(context.Background(), producer.Flush)
		c.mu.Lock()
		c.flushWait = flushWait
		c.mu.Unlock()
	}
	flushErr, resolved := c.joinShutdownPhase(ctx, c.config.Lifecycle.FlushTimeout, "flush", flushWait)
	if !resolved {
		return flushErr
	}
	c.mu.Lock()
	c.flushWait = nil
	c.mu.Unlock()
	if flushErr != nil {
		return flushErr
	}
	return nil
}

func (c *Client) closeProducer(ctx context.Context, producer driver.Producer) (error, bool) {
	c.mu.Lock()
	producerCloseWait := c.producerCloseWait
	c.mu.Unlock()
	if producerCloseWait == nil {
		//nolint:contextcheck // this shutdown call must outlive the attempt and is rejoined on retry.
		producerCloseWait = startShutdownPhase(context.Background(), producer.Close)
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
		return producerCloseErr, false
	}
	c.mu.Lock()
	c.producerCloseWait = nil
	c.producerHandle = nil
	c.mu.Unlock()
	if producerCloseErr != nil && c.options.logger != nil {
		c.options.logger.Warn("f1 producer close failed", "error", producerCloseErr)
	}
	return producerCloseErr, true
}

func (c *Client) closeConnection(ctx context.Context, conn driver.Conn, producerCloseErr error) error {
	c.mu.Lock()
	connCloseWait := c.connCloseWait
	c.mu.Unlock()
	if connCloseWait == nil {
		//nolint:contextcheck // this shutdown call must outlive the attempt and is rejoined on retry.
		connCloseWait = startShutdownPhase(context.Background(), conn.Close)
		c.mu.Lock()
		c.connCloseWait = connCloseWait
		c.mu.Unlock()
	}
	connErr, resolved := c.joinShutdownPhase(ctx, c.config.Lifecycle.CloseTimeout, "close", connCloseWait)
	closeErr := connErr
	if producerCloseErr != nil {
		closeErr = errors.Join(producerCloseErr, connErr)
	}
	if !resolved {
		return c.failClose(closeErr)
	}
	c.mu.Lock()
	c.connCloseWait = nil
	c.mu.Unlock()
	if connErr != nil {
		return c.failClose(closeErr)
	}
	return c.finishClose(producerCloseErr)
}

func (c *Client) finishClose(producerCloseErr error) error {
	c.mu.Lock()
	c.closed = true
	c.closing = false
	c.mu.Unlock()
	return producerCloseErr
}

// startShutdownPhase starts a shutdown call with the context supplied by
// Close. Close supplies Background so the call can outlive the attempt.
func startShutdownPhase(ctx context.Context, fn func(context.Context) error) <-chan error {
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
	if err := publishAdmissionLocked(c, allowClosing); err != nil {
		c.mu.Unlock()
		return err
	}
	producer := c.producerHandle
	conn := c.conn
	effective := c.effective
	if producer != nil {
		beginPublish(c)
		c.mu.Unlock()
	} else {
		c.mu.Unlock()
		builtProducer, err := conn.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true, Effective: effective})
		if err != nil {
			requestReconnectOnTransient(c, err)
			return err
		}
		if builtProducer == nil {
			return errors.New("driver returned a nil producer")
		}

		var loser driver.Producer
		c.mu.Lock()
		err = publishAdmissionLocked(c, allowClosing)
		if err == nil && !sameConnection(c.conn, conn) {
			err = c.reconnectingError("publish")
		}
		if err == nil {
			if c.producerHandle != nil {
				producer = c.producerHandle
				loser = builtProducer
			} else {
				producer = builtProducer
				c.producerHandle = builtProducer
			}
			beginPublish(c)
		}
		c.mu.Unlock()
		if loser != nil {
			closeDiscardedProducer(c, loser, ctx)
		}
		if err != nil {
			closeDiscardedProducer(c, builtProducer, ctx)
			return err
		}
	}
	defer endPublish(c)
	err := producer.Publish(ctx, messages...)
	requestReconnectOnTransient(c, err)
	return err
}

func publishAdmissionLocked(c *Client, allowClosing bool) error {
	if c.closed || c.conn == nil || (!allowClosing && c.shutdownStarted) || (allowClosing && c.producerTeardown) {
		return errors.New("f1: client is closed")
	}
	if c.reconnectErr != nil {
		return c.reconnectErr
	}
	if c.reconnecting {
		return c.reconnectingError("publish")
	}
	return nil
}

func closeDiscardedProducer(c *Client, producer driver.Producer, ctx context.Context) {
	if producer == nil {
		return
	}
	if err := producer.Close(context.WithoutCancel(ctx)); err != nil {
		lastResortClientLogger(c).Warn("f1 discarded producer close failed", "error", err)
	}
}

func requestReconnectOnTransient(c *Client, err error) {
	if c == nil || err == nil {
		return
	}
	if kind, classified := driver.Classify(err); !classified || kind != driver.KindTransient {
		return
	}
	_, _ = c.requestReconnect(err)
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

func driverConfig(cfg Config, logger *slog.Logger) driver.Config {
	result := driver.Config{
		Endpoints:             append([]string(nil), cfg.Broker.Endpoints...),
		ClientID:              cfg.InstanceID,
		InstanceID:            cfg.InstanceID,
		RebalanceDrainTimeout: cfg.Lifecycle.RebalanceDrainTimeout,
		ConnectTimeout:        cfg.Broker.ConnectTimeout,
		Logger:                logger,
		DriverOptions:         cloneOptions(cfg.Broker.DriverOptions),
	}
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
	maps.Copy(cloned, values)
	return cloned
}
