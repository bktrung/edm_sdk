package f1

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/codec"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/version"
)

var errClientReconnecting = errors.New("f1: client is reconnecting")

const reconnectDriverInitialInterval = 500 * time.Millisecond

const reconnectDriverMaxInterval = 30 * time.Second

// reconnectRequest is a caller's ask: rebuild the connection that carried this
// epoch. The epoch is what lets the supervisor drop a request the swap has
// already answered, because the connection the caller asked about is no longer
// the one the client is on.
type reconnectRequest struct {
	cause error
	epoch uint64
}

// A failedSubscription records one stopped subscription by name. owner is the
// runner that wrote the entry. An exit record replaces an entry only while it
// is still that runner's, so a runner that a newer runner of the same name has
// superseded cannot put its failure back into Health.
type failedSubscription struct {
	name  string
	err   error
	owner *Runner
}

// currentConnection is the connection the client is on, together with the
// number that names that incarnation. The two are one value because they are
// one fact: the number counts the incarnations of the connection, and the
// supervisor installs a new connection with its new number in a single critical
// section. A reader that holds mu therefore takes the connection and the number
// it will later compare against in one read, and no read of the client can pair
// a connection with another incarnation's number.
type currentConnection struct {
	conn  driver.Conn
	epoch uint64
}

// unreleasedConsumer is a consumer whose Release failed, kept with the
// connection epoch it was opened on. A driver keeps such a consumer
// registered, so the connection it belongs to refuses to close until a later
// Release succeeds: the client owes it one more attempt, on the next Client
// close, and on the swap that retires the connection it belongs to.
type unreleasedConsumer struct {
	epoch    uint64
	consumer driver.Consumer
}

// Client owns a broker connection and coordinates publishing, subscriptions,
// health checks, and shutdown. Create a Client with New; its zero value is not
// usable. Client methods may be called concurrently.
type Client struct {
	limits         Limits
	effective      driver.Capabilities
	options        clientOptions
	observer       Observer
	traceInjector  TraceInjector
	observerPanics map[ObserverKind]struct{}
	// backlogPollInterval is the resolved poll interval. Zero never survives
	// New: zero becomes 15s and a negative value disables the loop.
	backlogPollInterval time.Duration
	driverName          string
	serverAddress       string
	serverPort          int
	config              Config
	source              string
	producer            string
	producerHandle      driver.Producer

	mu sync.Mutex
	// lifecycle is the client's lifecycle, stored rather than derived from a
	// set of Close flags: Ready until Close is entered, Draining while the
	// Close attempt that entered it runs, Aborted when that attempt gave up
	// part way, and Closed once its resources are released. Aborted is not
	// terminal, because a failed Close may be retried and re-enters Draining,
	// and Closed is: a Close that already finished succeeds again without
	// moving anything.
	lifecycle *lifecycle.Machine
	// conn is the connection axis, stored rather than derived from a
	// reconnecting flag: connReconnecting while an attempt is rebuilding the
	// connection, connLive otherwise, which is the zero value. The other two
	// axis values are not stored here. connNone is current.conn being nil, the
	// one place that fact lives, and connFailed is the retained reconnectErr,
	// which is the value a refused caller is returned.
	conn            connState
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
	// unreleasedConsumers holds the consumers whose Release failed and which
	// the driver therefore still has registered, each with the connection
	// epoch it was opened on. A kept consumer is released again before the
	// connection that carries it is closed: by the next Client.Close for the
	// live connection, and by the swap that retires the connection it belongs
	// to.
	unreleasedConsumers []unreleasedConsumer

	// current is the connection the client is on and the number that names it.
	// Its epoch is 1 for the connection New opened, and a nil conn means the
	// client holds none.
	current     currentConnection
	retirements []*retiredConnection
	// reconnectErr records a terminal reconnect decision for the current client
	// state. It is guarded by mu and is the value the failed connection axis
	// carries: reconnect exhaustion does not mean Close has been entered, and a
	// swap clears it because the connection it describes is gone.
	reconnectErr      error
	reconnectRequests chan reconnectRequest
	reconnectRandom   func() float64
	supervisorCtx     context.Context
	supervisorCancel  context.CancelFunc
	supervisorDone    chan struct{}
	// attemptErr is the outcome of the attempt that last released
	// attemptEnded, and is read only after that release.
	attemptErr error
	// attemptEnded is released, and immediately replaced, when the connection
	// incarnation changes and when a reconnect attempt ends without changing
	// it. A waiter captures it under mu beside the epoch and compares epochs
	// after it fires: a moved epoch is a new connection, and an unchanged one
	// means the attempt ended and the connection state is what it is.
	attemptEnded chan struct{}

	// producerCloseWait holds a still-running producer Close call from a
	// prior Close attempt that did not return within its close timeout. A
	// retried Close rejoins this same call instead of starting a second one
	// against the same producer.
	producerCloseWait <-chan error
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
	backlogPollInterval := options.backlogPollInterval
	if backlogPollInterval == 0 {
		backlogPollInterval = 15 * time.Second
	} else if backlogPollInterval > 0 && backlogPollInterval < time.Second {
		return nil, fmt.Errorf("f1: WithBacklogPollInterval requires at least 1s, got %v", backlogPollInterval)
	}
	options.backlogPollInterval = backlogPollInterval
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
		current:             currentConnection{conn: connection, epoch: 1},
		lifecycle:           lifecycle.New(),
		effective:           effective,
		options:             options,
		observer:            options.observer,
		backlogPollInterval: backlogPollInterval,
		driverName:          driverName,
		config:              cfg,
		source:              fmt.Sprintf("/%s/%s", cfg.Env, cfg.Service),
		producer:            fmt.Sprintf("%s/%s/%s", cfg.Service, cfg.Env, cfg.InstanceID),
		reconnectRequests:   make(chan reconnectRequest, 1),
		reconnectRandom:     rand.Float64,
		supervisorCtx:       supervisorCtx,
		supervisorCancel:    supervisorCancel,
		supervisorDone:      make(chan struct{}),
		attemptEnded:        make(chan struct{}),
		runners:             make(map[*Runner]struct{}),
	}
	if client.observer != nil {
		client.observerPanics = make(map[ObserverKind]struct{})
		if injector, ok := client.observer.(TraceInjector); ok {
			client.traceInjector = injector
		}
		if len(cfg.Broker.Endpoints) > 0 {
			host, port := endpointAddress(cfg.Broker.Endpoints[0])
			client.serverAddress = host
			client.serverPort = port
		}
	}
	client.limits = limitsFor(driverName, connection.BrokerInfo(), effective)
	// The client owns a connection from here, so its lifecycle leaves the
	// machine's Starting: everything admission reads is Ready until Close is
	// entered. The client never visits Reconnecting or Failed, which are runner
	// states; a connection being rebuilt is the connection axis, not the
	// lifecycle.
	_ = client.lifecycle.Transition(lifecycle.Ready)
	logCapabilities(client)
	if err := client.ensurePublisherTopology(ctx); err != nil {
		supervisorCancel()
		closeErr := connection.Close(ctx)
		return nil, errors.Join(err, closeErr)
	}
	if client.observer != nil {
		client.observeRecord(PointEvent{
			Kind:          ObserverDriverSelected,
			At:            client.options.clock.Now(),
			DriverName:    driverName,
			SDKVersion:    version.SDK(),
			ServerAddress: client.serverAddress,
			ServerPort:    client.serverPort,
		})
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
	conn := c.current.conn
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

// Publisher returns a reusable publisher bound to this Client's connection and
// configured codec. Publish calls through it may run concurrently. A nil Client
// returns an unconnected Publisher.
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

// Health checks the broker connection and reports stopped subscription runners.
// It returns a ping error, an admission error, or an error for unhealthy
// subscriptions. A nil Client returns an error.
func (c *Client) Health(ctx context.Context) error {
	if c == nil {
		return fmt.Errorf("f1: client is not connected")
	}
	c.mu.Lock()
	if err := c.admit(workHealth, 0); err != nil {
		c.mu.Unlock()
		return err
	}
	conn := c.current.conn
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

// recordFailedSubscription records err against the subscription name, owned by
// the runner that observed it. A name that is already recorded has its error
// and owner replaced in place, so the early record taken at a fatal consumer
// error and the record taken when a runner stops collapse into one entry, a
// newer runner of that name takes the entry over from the runner it replaced,
// and the list is bounded by the number of distinct subscription names.
// Entries are kept ordered by name so Health's text does not depend on the
// order in which runners stopped.
func (c *Client) recordFailedSubscription(name string, err error, owner *Runner) {
	if c == nil || err == nil || owner == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recordFailedSubscriptionLocked(name, err, owner)
}

// recordFailedSubscriptionLocked writes an entry a runner has not written
// before, and marks that runner as having recorded. The caller holds c.mu.
func (c *Client) recordFailedSubscriptionLocked(name string, err error, owner *Runner) {
	owner.recordedFailure = true
	for i := range c.failedSubscriptions {
		switch {
		case c.failedSubscriptions[i].name == name:
			c.failedSubscriptions[i].err = err
			c.failedSubscriptions[i].owner = owner
			return
		case c.failedSubscriptions[i].name > name:
			c.failedSubscriptions = slices.Insert(c.failedSubscriptions, i, failedSubscription{name: name, err: err, owner: owner})
			return
		}
	}
	c.failedSubscriptions = append(c.failedSubscriptions, failedSubscription{name: name, err: err, owner: owner})
}

// recordRunnerExitLocked writes the failure that stopped runner. A runner that
// has not recorded before writes an entry as it always did. A runner that
// already recorded has already told Health about this name, and may update
// that entry only while the entry is still its own: it never inserts one, and
// it never overwrites another runner's. So a runner whose name a newer runner
// has cleared, or a newer runner whose failure has taken the entry over, keeps
// the old exit from re-marking a subscription that is no longer it. The caller
// holds c.mu.
func (c *Client) recordRunnerExitLocked(runner *Runner, err error) {
	if !runner.recordedFailure {
		c.recordFailedSubscriptionLocked(runner.subscription.Name, err, runner)
		return
	}
	for i := range c.failedSubscriptions {
		if c.failedSubscriptions[i].name != runner.subscription.Name || c.failedSubscriptions[i].owner != runner {
			continue
		}
		c.failedSubscriptions[i].err = err
		return
	}
}

// clearFailedSubscription removes the recorded failure for name. A runner that
// starts afresh under that name has replaced whatever stopped its predecessor,
// so the record no longer describes the client. A runner rebuilding its
// consumer through a reconnect has replaced nothing and does not clear.
func (c *Client) clearFailedSubscription(name string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failedSubscriptions = slices.DeleteFunc(c.failedSubscriptions, func(failed failedSubscription) bool {
		return failed.name == name
	})
}

// Close drains active work and refuses new work after shutdown begins. It waits
// for the reconnect supervisor and finishes retired teardowns before current
// resources; nil requires every retired teardown to have succeeded. Running
// teardowns are rejoined, not duplicated; each failed retirement gets one new
// attempt per call, skipping successful stages, with no background retry.
// Each wait is bounded by Lifecycle.CloseTimeout and ctx; multiple retirements
// can make total shutdown exceed one CloseTimeout. A retirement error or timeout
// leaves unfinished resources owned and stops shutdown before current teardown.
// A current producer-close error is returned but does not prevent current
// connection shutdown. A current connection-close error leaves the Client
// retryable. A concurrent call returns an error while shutdown is in progress.
// A nil or fully closed Client returns nil.
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
	if err := c.waitForSupervisor(ctx); err != nil {
		return c.failClose(err)
	}
	if err := c.closeRetirements(ctx); err != nil {
		return c.failClose(err)
	}
	return c.closeResources(ctx)
}

// waitForSupervisor transfers retirement ownership to Close. If the supervisor
// timed out joining an old teardown, it may still be running, but after this
// barrier no reconnect waiter can consume its result or register another one.
func (c *Client) waitForSupervisor(ctx context.Context) error {
	if c.supervisorDone == nil {
		return nil
	}
	return runWithClockTimeout(ctx, c.options.clock, c.config.Lifecycle.CloseTimeout, "reconnect", func(waitCtx context.Context) error {
		select {
		case <-c.supervisorDone:
			return nil
		case <-waitCtx.Done():
			return waitCtx.Err()
		}
	})
}

func (c *Client) beginClose() ([]*Runner, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// A Close that has already finished succeeds again without doing anything,
	// and one that is already running is refused so the caller keeps to one
	// phase sequence at a time. Aborted proceeds: it is a Close that failed
	// part way, either a bound expired or the connection close did not return,
	// and a retried Close re-enters the drain. That is why the lifecycle keeps
	// Aborted apart from Draining: a retried Close moves Aborted back to
	// Draining, and only Closed is terminal.
	switch c.lifecycleLocked() {
	case lifecycle.Closed:
		return nil, true, nil
	case lifecycle.Draining:
		return nil, false, fmt.Errorf("f1: client is closing")
	}
	_ = c.lifecycle.Transition(lifecycle.Draining)
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
	// The attempt is over and its bound expired, so the lifecycle returns to
	// the retryable state: admission stays shut and a later Close may enter the
	// drain again. The resources this attempt did not release stay where they
	// are, which is what makes the retry a continuation.
	_ = c.lifecycle.Transition(lifecycle.Aborted)
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

// keepUnreleasedConsumer records a consumer whose Release failed, with the
// connection epoch it was opened on. The driver still has the consumer
// registered, which is what makes the connection carrying it refuse to close,
// so the client owes it one more release. A consumer that is already kept is
// kept once: a second failed release of the same consumer is the same entry.
func (c *Client) keepUnreleasedConsumer(consumer driver.Consumer, epoch uint64) {
	if c == nil || consumer == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, kept := range c.unreleasedConsumers {
		if kept.consumer == consumer {
			return
		}
	}
	c.unreleasedConsumers = append(c.unreleasedConsumers, unreleasedConsumer{epoch: epoch, consumer: consumer})
}

// forgetUnreleasedConsumer drops one kept consumer whose release attempt
// resolved it, either because it released or because the connection it
// belonged to is gone.
func (c *Client) forgetUnreleasedConsumer(entry unreleasedConsumer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unreleasedConsumers = slices.DeleteFunc(c.unreleasedConsumers, func(kept unreleasedConsumer) bool {
		return kept.consumer == entry.consumer
	})
}

// releaseKeptConsumers re-releases the consumers a failed Release left
// registered on the driver, bounded by Lifecycle.CloseTimeout, before the
// connection carrying them is closed. Each success drops the entry; each
// failure keeps it and is returned joined, so a retried Client.Close attempts
// it again. This is the second attempt the client owes a consumer whose
// teardown failed while the client was still running, and it is what lets a
// close that failed once succeed on a later call.
func (c *Client) releaseKeptConsumers(ctx context.Context) error {
	c.mu.Lock()
	kept := append([]unreleasedConsumer(nil), c.unreleasedConsumers...)
	c.mu.Unlock()
	var errs []error
	for _, entry := range kept {
		err := runWithClockTimeout(ctx, c.options.clock, c.config.Lifecycle.CloseTimeout, "close", entry.consumer.Release)
		if err != nil {
			errs = append(errs, fmt.Errorf("f1: release consumer on connection epoch %d: %w", entry.epoch, err))
			continue
		}
		c.forgetUnreleasedConsumer(entry)
	}
	return errors.Join(errs...)
}

// retireKeptConsumers leaves failed releases owned by their retired epoch.
// The enclosing retirement attempt bounds waiting, not individual driver calls,
// so a hung Release cannot be overtaken by the connection close.
func (c *Client) retireKeptConsumers(ctx context.Context, epoch uint64) error {
	c.mu.Lock()
	kept := make([]unreleasedConsumer, 0, len(c.unreleasedConsumers))
	for _, entry := range c.unreleasedConsumers {
		if entry.epoch == epoch {
			kept = append(kept, entry)
		}
	}
	c.mu.Unlock()
	for _, entry := range kept {
		if err := entry.consumer.Release(ctx); err != nil {
			return fmt.Errorf("f1: release consumer on connection epoch %d: %w", entry.epoch, err)
		}
		c.forgetUnreleasedConsumer(entry)
	}
	return nil
}

func (c *Client) closeResources(ctx context.Context) error {
	c.mu.Lock()
	producer := c.producerHandle
	conn := c.current.conn
	c.mu.Unlock()
	// A consumer whose Release failed is still registered on the driver, and a
	// driver refuses to close a connection that still carries one. Attempting
	// it here is what makes the close that failed on that release succeed on a
	// later call; a release the driver still refuses keeps the entry and fails
	// this close, which stays retryable.
	if err := c.releaseKeptConsumers(ctx); err != nil {
		return c.failClose(err)
	}
	if producer == nil {
		return c.closeConnection(ctx, conn, nil)
	}
	producerCloseErr, resolved := c.closeProducer(ctx, producer)
	if !resolved {
		return c.failClose(producerCloseErr)
	}
	return c.closeConnection(ctx, conn, producerCloseErr)
}

func (c *Client) closeProducer(ctx context.Context, producer driver.Producer) (error, bool) {
	c.mu.Lock()
	producerCloseWait := c.producerCloseWait
	c.mu.Unlock()
	if producerCloseWait == nil {
		//nolint:contextcheck // this shutdown call must outlive the attempt and is rejoined on retry.
		producerCloseWait = startPhase(context.Background(), producer.Close)
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
		connCloseWait = startPhase(context.Background(), conn.Close)
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
	_ = c.lifecycle.Transition(lifecycle.Closed)
	c.mu.Unlock()
	return producerCloseErr
}

func (c *Client) joinShutdownPhase(ctx context.Context, timeout time.Duration, phase string, done <-chan error) (error, bool) {
	return joinPhase(ctx, c.options.clock, timeout, phase, done)
}

// errNilProducer is the refusal a driver that returned no producer gets. Each
// publish path decides for itself how to report it, because one of them treats
// it as a creation failure and the other does not, and a caller that compares
// against it can keep that difference without a second build path.
var errNilProducer = errors.New("driver returned a nil producer")

// producerResult is what asking for the shared producer produced: the producer
// to publish through, the connection incarnation it was admitted on, and at
// most one failure. refused is the admission that turned the publish away,
// which a caller returns unchanged because that is exactly what its own gate
// returned before; buildErr is the driver's own failure or a nil producer,
// which each caller reports in its own way. A result with neither failure
// carries a producer, and one with either carries no producer.
type producerResult struct {
	producer driver.Producer
	epoch    uint64
	refused  error
	buildErr error
}

// sharedProducer returns the client's shared producer for a publish of kind,
// building it when the client holds none yet.
//
// claim is the incarnation the caller's own admission captured, or zero for a
// caller that captured none. The build happens outside the lock, because it
// reaches the broker, and the admission it must satisfy is taken again when it
// is installed: a producer built across a swap, or one built while a Close
// entered the drain, belongs to a client state that may no longer admit this
// publish, so it is closed instead of installed. Two callers that build at the
// same time install one winner and the loser is closed, which is why the
// producer is installed at most once per client.
//
// onProducer, when set, runs under c.mu at the moment the producer is the
// caller's to publish through, before the lock is released. publishMessages
// passes beginPublish: its publish accounting has to start in the same critical
// section as the admission of the producer it will publish through, or a Close
// waiting for the publish-idle channel could observe zero in flight and tear
// the producer down between the two.
func (c *Client) sharedProducer(ctx context.Context, kind workKind, claim uint64, onProducer func(*Client)) producerResult {
	c.mu.Lock()
	if err := c.admit(kind, claim); err != nil {
		c.mu.Unlock()
		return producerResult{refused: err}
	}
	// The connection and its incarnation are one value, read in the section
	// that decides whether this call builds a producer: the producer is built
	// on that connection, and the admission after it compares the claim that
	// came with it.
	current := c.current
	conn := current.conn
	epoch := current.epoch
	effective := c.effective
	producer := c.producerHandle
	if producer != nil {
		if onProducer != nil {
			onProducer(c)
		}
		c.mu.Unlock()
		return producerResult{producer: producer, epoch: epoch}
	}
	c.mu.Unlock()

	built, err := conn.Producer(ctx, driver.ProducerConfig{Effective: effective})
	if err == nil && built == nil {
		err = errNilProducer
	}
	if err != nil {
		return producerResult{epoch: epoch, buildErr: err}
	}

	var loser driver.Producer
	c.mu.Lock()
	err = c.admit(kind, epoch)
	if err == nil {
		if c.producerHandle != nil {
			producer = c.producerHandle
			loser = built
		} else {
			producer = built
			c.producerHandle = built
		}
		if onProducer != nil {
			onProducer(c)
		}
	}
	c.mu.Unlock()
	if loser != nil {
		closeDiscardedProducer(c, loser, ctx)
	}
	if err != nil {
		closeDiscardedProducer(c, built, ctx)
		return producerResult{epoch: epoch, refused: err}
	}
	return producerResult{producer: producer, epoch: epoch}
}

// publishMessages sends core-generated successor messages through the client's
// shared producer, on the admission every publish through that producer uses.
func publishMessages(c *Client, ctx context.Context, messages ...driver.OutboundMessage) error {
	if len(messages) == 0 {
		return nil
	}
	result := c.sharedProducer(ctx, workPublish, 0, beginPublish)
	if result.refused != nil {
		return result.refused
	}
	if result.buildErr != nil {
		// The driver's own failure is evidence about the connection; a driver
		// that returned no producer at all said nothing about it.
		if !errors.Is(result.buildErr, errNilProducer) {
			requestReconnectOnTransient(ctx, c, result.buildErr, result.epoch)
		}
		return result.buildErr
	}
	defer endPublish(c)
	err := result.producer.Publish(ctx, messages...)
	requestReconnectOnTransient(ctx, c, err, result.epoch)
	return err
}

func closeDiscardedProducer(c *Client, producer driver.Producer, ctx context.Context) {
	if producer == nil {
		return
	}
	if err := producer.Close(context.WithoutCancel(ctx)); err != nil {
		lastResortClientLogger(c).Warn("f1 discarded producer close failed", "error", err)
	}
}

// requestReconnectOnTransient requests a reconnect when err is evidence that
// the connection is unhealthy, naming the connection the failed call was made
// on by its epoch, so a request a swap has already answered is dropped.
//
// A *driver.PublishError is that evidence only when a failed message carries an
// error the driver itself classified transient; a driver that said transient
// for a message observed something about the connection. PublishError.Kind, by
// contrast, reports the worst classification among the failed messages and
// counts an untranslated cause as transient, so a batch whose causes are all
// untranslated reports KindTransient and reports it as classified. That default
// is a retry hint for the caller and says nothing about the socket, so it must
// not bring the connection down.
//
// A call whose own context was canceled is not that evidence on its own: the
// caller withdrew the call, so an error reporting that same cancellation is
// about the caller, and a shutdown that cancels its in-flight publishes must
// not start a reconnect. A driver error that reports something else is still
// evidence, because a publish the caller gave up on can have failed on a
// connection that was already broken when it was canceled. A context past its
// deadline counts whenever the driver classified the error transient, because
// a broker that stopped confirming is exactly what runs a publish out of time.
//
// The request itself carries no context: the attempt it starts runs on the
// client's own supervisor context, so a request made from a canceled call is
// already detached from that cancellation.
func requestReconnectOnTransient(ctx context.Context, c *Client, err error, epoch uint64) {
	if c == nil || err == nil {
		return
	}
	if errors.Is(ctx.Err(), context.Canceled) && errors.Is(err, context.Canceled) {
		return
	}
	if kind, classified := driver.Classify(err); !classified || kind != driver.KindTransient {
		return
	}
	if partial, ok := errors.AsType[*driver.PublishError](err); ok && !carriesTransientCause(partial) {
		return
	}
	_ = c.requestReconnect(err, epoch)
}

// carriesTransientCause reports whether any failed message in a batch carries
// an error its driver classified transient.
func carriesTransientCause(partial *driver.PublishError) bool {
	for _, cause := range partial.Failed {
		if kind, classified := driver.Classify(cause); classified && kind == driver.KindTransient {
			return true
		}
	}
	return false
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
