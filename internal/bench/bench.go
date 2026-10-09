// Package bench measures an f1 client against a driver through the public API.
//
// Every operation it performs crosses the public surface - New, Publish,
// Subscribe, Run - so a number it reports describes what a service author
// gets, not what a driver's own Consumer costs. It is handed a driver.Driver
// instead of choosing one, which keeps it driver-agnostic by construction and
// inside the import boundary that keeps core code away from concrete adapters.
//
// Settlement is the one thing the public API does not expose. The harness
// therefore wraps the driver at the port, as the test helper package does, and
// counts a delivery as settled only when its Ack returns nil. Counting
// anywhere earlier - at delivery, or on handler entry - reports the rate of
// the step before settlement, which is a bigger number about a different
// thing, so the harness checks where its clock stopped rather than trusting it.
package bench

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"runtime"
	"runtime/metrics"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

const (
	// lane is the one delivery lane every measurement uses. Multi-lane
	// contention is a separate question from throughput, and the port has no
	// cross-driver lane semantics to compare yet.
	lane = f1.PriorityMedium

	// connectTimeout bounds one client's own connect attempts, so an
	// unreachable endpoint fails the measurement instead of waiting out the
	// caller's deadline.
	connectTimeout = 10 * time.Second

	// handlerTimeout stays under drainTimeout because the core refuses a
	// subscription whose handler timeout the drain budget does not exceed.
	handlerTimeout = 5 * time.Second
	drainTimeout   = 30 * time.Second
	closeTimeout   = 10 * time.Second

	// pruneAttempts bounds how often cleanup retries a delete that a consumer
	// group still holds, and pruneWait is the delay between those attempts. A
	// group keeps membership for a moment after its process exits, and the
	// port's delete guard refuses until that clears.
	pruneAttempts = 10
	pruneWait     = 500 * time.Millisecond

	// cleanupTimeout bounds one cleanup, which runs after the caller's context
	// may already be done.
	cleanupTimeout = 2 * time.Minute

	// sampleInterval is how often a consume measurement samples the consumer's
	// backlog and the process's live heap. One backlog query is the whole cost
	// of a tick, which is what makes a tick this short affordable, and a corpus
	// whose arrival spreads over seconds is seen at this resolution.
	sampleInterval = 100 * time.Millisecond

	// orderedKeys is how many keys an ordered measurement spreads its corpus
	// over. One key would serialise the whole corpus through one handler slot
	// and measure the key choice rather than the ordering path, and a key per
	// message would leave nothing for the ordering to serialise. The set is
	// fixed rather than derived from the corpus, so two corpora of different
	// sizes divide the same way and their figures compare.
	orderedKeys = 64

	// kafkaDriverName is the driver whose destinations carry a partition count.
	// The count is configured through a Kafka option, so the harness sets it
	// only for that driver: the same key on another driver is a configuration
	// error rather than an ignored setting.
	kafkaDriverName = "kafka"

	// rabbitMQDriverName is the driver whose destinations carry a queue kind.
	// The kind is configured through a RabbitMQ option, and it is set only for
	// that driver for the reason the partition count is.
	rabbitMQDriverName = "rabbitmq"
)

// sequence numbers each harness, so two harnesses built from one namespace
// still create different broker destinations.
var sequence atomic.Int64

// Config describes one measurement's client and subscription shape.
//
// A zero Concurrency keeps the shipped handler concurrency. The harness sets a
// shape only where a measurement needs one the shipped defaults do not have.
type Config struct {
	// Namespace names everything one run creates. It must be unique per
	// process; the harness appends a counter so several runs of one namespace
	// still differ. Only lower-case letters, digits, dot, underscore and dash
	// can reach a broker, so anything else is refused before a client is built.
	Namespace string
	// Endpoint is the broker endpoint the client connects to. It is empty for
	// the in-memory driver, which dials nothing.
	Endpoint string
	// Messages is the corpus size of one measurement.
	Messages int
	// Publishers is the number of goroutines that publish the corpus.
	Publishers int
	// Concurrency is the handler concurrency of the single lane.
	Concurrency int
	// Prefetch is the subscription's in-flight budget: how many messages the
	// consumer may hold ahead of settlement across the destinations it reads.
	// A zero value keeps the shipped default, which is the rule the harness
	// follows for every shape a measurement does not need to state, so a
	// measurement that configures nothing measures what a caller who
	// configures nothing gets. The core refuses a budget below the
	// subscription's lane count, and caps one above the total its lanes can
	// hold, so a value here is a request and not always the budget the driver
	// is given.
	Prefetch int
	// Priorities is the priority set the subscription consumes, which is how
	// many main destinations one subscription opens: the core derives one
	// destination per topic and priority, and a driver with a channel per
	// destination opens one channel per priority. A measurement with two
	// priorities therefore reads one subscription over two destinations and two
	// channels, sharing the handler concurrency between them, and its publishers
	// spread the corpus over the set evenly so neither destination is loaded
	// more than the other. Empty keeps the one lane every measurement had before
	// this field existed.
	//
	// The in-flight budget is the subscription's, not a destination's: the core
	// allocates it across the destinations, so two priorities means half the
	// budget each unless the measurement asks for more.
	Priorities []f1.Priority
	// HandlerWork is how long the consume measurement's handler holds each
	// delivery before returning. It applies to the consume measurement alone:
	// the publish, latency and retry measurements keep the handler that returns
	// at once, so their numbers stay comparable with the ones taken before this
	// field existed. A zero value is that handler.
	HandlerWork time.Duration
	// RetryTier is the retry delay the retry measurement configures as the only
	// tier of a two-attempt ladder, so the handler's failed first attempt comes
	// back after exactly this delay. Other measurements keep the shipped
	// ladder, and a retry measurement without a tier is refused.
	RetryTier time.Duration
	// Partitions is the partition count the measurement's destinations are
	// created with, on a driver whose destinations have partitions. It is
	// applied through the driver's documented partition-floor option, which
	// gives that many partitions to a destination that names none, so a
	// measurement of a partition count exercises the lever a deployment pulls
	// and not a seam. A zero value names no count and leaves the decision to
	// the broker, which is what a deployment that configures nothing gets. It
	// is ignored on a driver whose destinations have no partitions.
	Partitions int
	// QueueType is the kind of queue a RabbitMQ measurement asks its
	// destinations to be created as, named the way the driver's option names it
	// ("quorum" or "classic"). It travels as the driver's documented queue-type
	// option, which is the lever a deployment pulls, and an empty value names no
	// kind and leaves the decision to the driver's own default, which is the
	// rule the harness follows for every setting a measurement does not state.
	// The value is not interpreted here: an unsupported one is refused by the
	// driver that documents it. It is ignored on a driver whose queues have no
	// kind.
	QueueType string
	// Fairness overrides the subscription fairness shape for a measurement.
	// The zero value keeps the SDK defaults; a sweep can use a smaller
	// prefetch factor to keep every core destination window below a tested
	// broker-prefetch value without changing production defaults.
	Fairness f1.FairnessConfig
	// BrokerPrefetch is the RabbitMQ per-destination broker credit. Zero leaves
	// the driver coupled to the core window, while a positive value raises only
	// the broker credit and leaves the core in-flight budget unchanged.
	// It is ignored by drivers other than RabbitMQ.
	BrokerPrefetch int
	// Ordered makes the subscription an ordered one, so deliveries sharing a
	// key are handled one at a time, and makes the publishers spread the corpus
	// over a fixed key set, so the ordering has something to serialise. A
	// measurement without it keeps the unordered default.
	Ordered bool
	// Backlog publishes the consume measurement's corpus before its measured
	// window opens, holding every delivery back from the core until it does, so
	// the window contains the backlog's drain and not the load that made it.
	// Without it the window contains both, and a cell whose consumer settles
	// faster than the corpus is published reports the publisher rather than the
	// consumer: the window then ends no earlier than the last publish returns,
	// whatever the consumer did. With it, the client's own publish rate does not
	// bound what the consumer can be measured at, and the measured window is the
	// consumer's alone. The CPU a measurement costs is not: it is read around
	// the whole call, load included, which is why a sweep reads it beside a
	// publish-only control.
	//
	// It applies to the consume measurement. Every other measurement keeps the
	// shape it had, and so does a consume measurement without it.
	Backlog bool
	// PublishBatch is how many corpus messages one publish call carries when
	// the corpus is loaded. A zero value keeps the single-message path, which
	// is what every measurement used before this field existed and is the path
	// the publish measurements report.
	//
	// It exists because the two are far apart: a single-message publish waits
	// for its own durable acknowledgement, so the loader offers about two and a
	// half thousand messages a second whatever the publisher count, while a
	// call carrying a batch of them fills a window of the driver's own size and
	// is confirmed as one. A backlog measurement that wants a corpus far bigger
	// than the single-message path can fill in a reasonable time sets this and
	// reads its load rate against the control cell with the same batch size.
	//
	// The corpus load runs before a backlog measurement's window opens, so a
	// batch here changes what the load costs and not what the window measures.
	// It does change the CPU a measurement reports, because that figure covers
	// the whole call rather than the window alone.
	PublishBatch int
	// CountDuplicates reports duplicate deliveries instead of failing the run
	// on them. A measurement without it fails on any delivery its shape did not
	// expect, which is what every measurement did before this field existed, and
	// is what keeps a regression from hiding inside a rate.
	CountDuplicates bool
	// Timeout bounds one measurement, from client construction to drain.
	Timeout time.Duration
}

// Result is one measurement's output.
type Result struct {
	// Messages is the corpus size of the measurement.
	Messages int
	// Publishers is the number of concurrent publishers that loaded the corpus.
	Publishers int
	// Concurrency is the handler concurrency of the one lane.
	Concurrency int
	// HandlerWork is the handler hold time the measurement configured. The
	// consume measurement is the one that applies it, and a zero value is a
	// handler that returns at once.
	HandlerWork time.Duration
	// Elapsed is the measured window.
	Elapsed time.Duration
	// Latencies is one publish-to-ack duration per message, in sequence order.
	// It is populated only by the latency measurement.
	Latencies []time.Duration
	// SettleLatencies is one settle-latency duration per settlement the
	// measured window recorded, in the order the settlements happened: the
	// time from the pump offering the delivery to the core to the ack call the
	// runtime made for it. It is populated by every measurement that settles a
	// corpus, which is every one but the publish measurement, and it is the
	// figure a rate cannot show: two shapes with the same rate settle a
	// delivery in very different times.
	SettleLatencies []time.Duration
	// SettleSequences is the corpus sequence that produced each settle latency.
	// It is parallel to SettleLatencies and lets a caller split the samples by
	// priority without changing the aggregate percentile accessors.
	SettleSequences []int
	// RetryTier is the retry delay the measurement configured, and zero when it
	// kept the shipped ladder.
	RetryTier time.Duration
	// Duplicates is how many deliveries arrived past the ones this shape
	// expects, summed over the corpus. A corpus message is delivered once per
	// attempt when the path is working, so the count is zero unless the run
	// delivered a message more often than that: a rate cannot show that, and a
	// run that fails on it cannot report it.
	Duplicates int
	// LagMax is the largest backlog the consumer reported over the measured
	// window, in messages, taken across the destinations the subscription
	// reads. It is populated by the consume measurement, and only when the
	// driver declares that it can be queried at all; it is zero otherwise.
	LagMax int64
	// HeapMax is the largest number of bytes the process's live heap held over
	// the measured window. It is populated by the consume measurement, and it
	// is what shows a subscription holding on to work it has not handed over.
	HeapMax uint64
	// GoroutineDelta is how many goroutines the process gained between the
	// client being built and the client being closed with its consumer stopped,
	// which is where a run that leaves work behind shows up.
	GoroutineDelta int
}

// Rate reports messages per second over the measured window.
func (r Result) Rate() float64 {
	if r.Elapsed <= 0 {
		return 0
	}
	return float64(r.Messages) / r.Elapsed.Seconds()
}

// Percentile returns the p-th percentile of the recorded latencies by nearest
// rank: the smallest recorded latency that at least p percent of the samples
// are at or below. A result without latencies has no percentile and returns
// zero.
func (r Result) Percentile(p float64) time.Duration {
	return percentile(r.Latencies, p)
}

// SettlePercentile returns the p-th percentile of the recorded settle
// latencies by nearest rank, with the same meaning and the same empty case as
// Percentile.
func (r Result) SettlePercentile(p float64) time.Duration {
	return percentile(r.SettleLatencies, p)
}

// SettlePercentileForPriority returns the p-th percentile of settle latencies
// produced by priority. Sequence numbers select priorities by the same modulo
// mapping the harness uses when publishing. A result without labelled samples
// or priorities returns zero.
func (r Result) SettlePercentileForPriority(priority f1.Priority, priorities []f1.Priority, p float64) time.Duration {
	if len(priorities) == 0 {
		return 0
	}
	durations := make([]time.Duration, 0, len(r.SettleLatencies))
	for index, seq := range r.SettleSequences {
		if index >= len(r.SettleLatencies) {
			break
		}
		if priorities[seq%len(priorities)] == priority {
			durations = append(durations, r.SettleLatencies[index])
		}
	}
	return percentile(durations, p)
}

// percentile returns the p-th percentile of one duration sample by nearest
// rank.
func percentile(durations []time.Duration, p float64) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	ranked := slices.Clone(durations)
	slices.Sort(ranked)
	rank := int(math.Ceil(p / 100 * float64(len(ranked))))
	rank = min(max(rank, 1), len(ranked))
	return ranked[rank-1]
}

// Harness runs one measurement at a time for one driver and one namespace.
//
// A harness is not safe for concurrent use, and it is meant to be used once:
// it names its topic and subscription when it is built, deletes them when
// Close is called, and one measurement per name is what keeps a later run from
// reading an earlier run's messages or offsets.
type Harness struct {
	cfg          Config
	drv          driver.Driver
	clk          clock.Clock
	topic        string
	subscription string
	// keys is the key set the publishers of an ordered measurement spread the
	// corpus over, built once so a publisher allocates nothing per message for
	// a key. It is empty for an unordered measurement, whose messages carry no
	// key.
	keys []string

	// declared holds every broker name the core asked the driver to create, so
	// cleanup deletes exactly what this run made. Reading the names back from
	// the topology specs keeps the harness from restating the core's naming
	// rules, which is what a hand-written name list would drift from.
	mu        sync.Mutex
	declared  map[string]struct{}
	exchanges map[string]struct{}
}

// New returns a harness for one driver and one measurement shape.
func New(drv driver.Driver, cfg Config) (*Harness, error) {
	if drv == nil {
		return nil, errors.New("bench: driver is nil")
	}
	if err := validateNamespace(cfg.Namespace); err != nil {
		return nil, err
	}
	if cfg.Messages < 1 {
		return nil, fmt.Errorf("bench: messages must be at least 1, got %d", cfg.Messages)
	}
	if cfg.Publishers < 1 {
		return nil, fmt.Errorf("bench: publishers must be at least 1, got %d", cfg.Publishers)
	}
	if cfg.Concurrency < 0 {
		return nil, fmt.Errorf("bench: concurrency must not be negative, got %d", cfg.Concurrency)
	}
	if cfg.Prefetch < 0 {
		return nil, fmt.Errorf("bench: prefetch must not be negative, got %d", cfg.Prefetch)
	}
	if cfg.BrokerPrefetch < 0 {
		return nil, fmt.Errorf("bench: broker prefetch must not be negative, got %d", cfg.BrokerPrefetch)
	}
	if cfg.RetryTier < 0 {
		return nil, fmt.Errorf("bench: retry tier must not be negative, got %s", cfg.RetryTier)
	}
	if cfg.HandlerWork < 0 {
		return nil, fmt.Errorf("bench: handler work must not be negative, got %s", cfg.HandlerWork)
	}
	if cfg.Timeout <= 0 {
		return nil, errors.New("bench: timeout must be positive")
	}
	if cfg.Partitions < 0 {
		return nil, fmt.Errorf("bench: partitions must not be negative, got %d", cfg.Partitions)
	}
	if cfg.PublishBatch < 0 {
		return nil, fmt.Errorf("bench: publish batch must not be negative, got %d", cfg.PublishBatch)
	}
	// A priority a subscription names twice would have the core derive one
	// destination per entry and ask the driver for the same destination twice,
	// which every adapter refuses. The shape is a defect wherever it comes from,
	// so it is refused here rather than left to the driver to report as a
	// duplicate destination.
	seenPriorities := make(map[f1.Priority]struct{}, len(cfg.Priorities))
	for _, priority := range cfg.Priorities {
		if _, exists := seenPriorities[priority]; exists {
			return nil, fmt.Errorf("bench: priority %q is configured twice", priority)
		}
		seenPriorities[priority] = struct{}{}
	}
	name := fmt.Sprintf("%s-%d", cfg.Namespace, sequence.Add(1))
	harness := &Harness{
		cfg:          cfg,
		drv:          drv,
		clk:          clock.NewReal(),
		topic:        name,
		subscription: name,
		declared:     make(map[string]struct{}),
		exchanges:    make(map[string]struct{}),
	}
	if cfg.Ordered {
		harness.keys = make([]string, orderedKeys)
		for index := range orderedKeys {
			harness.keys[index] = strconv.Itoa(index)
		}
	}
	return harness, nil
}

// Publish measures single-message publish throughput with cfg.Publishers
// concurrent publishers and no consumer.
//
// The window starts immediately before the first publish of the corpus and
// stops when the last publish call returns.
func (h *Harness) Publish(ctx context.Context) (Result, error) {
	return h.measure(ctx, modePublish)
}

// Consume measures consume-and-settle throughput for one runner on one lane.
//
// The window starts immediately before the first publish of the corpus, after
// the subscription is declared and the runner is ready, and stops when the
// last of the corpus has settled.
func (h *Harness) Consume(ctx context.Context) (Result, error) {
	return h.measure(ctx, modeConsume)
}

// Latency measures end-to-end latency, from the publish call to the returned
// ack.
//
// The window starts immediately before the first publish of the corpus and
// stops when the last of the corpus has settled. Each message is published
// only after the previous one settled, so the recorded durations are the round
// trips a caller would see rather than the arrival spread of a burst.
func (h *Harness) Latency(ctx context.Context) (Result, error) {
	return h.measure(ctx, modeLatency)
}

// Retry measures retry-path throughput, with a handler that fails every
// message once and succeeds on its retry copy.
//
// The window starts immediately before the first publish of the corpus and
// stops when the last message's retry copy has settled, so the elapsed time
// contains one tier delay per message. The tier delay is reported beside the
// rate rather than subtracted from it.
func (h *Harness) Retry(ctx context.Context) (Result, error) {
	if h.cfg.RetryTier <= 0 {
		return Result{}, errors.New("bench: retry measurement needs a retry tier")
	}
	return h.measure(ctx, modeRetry)
}

// Close deletes the broker names this harness's runs created, so a later run
// never reads this run's messages or offsets. A name that was never created is
// skipped: the run did not make it.
func (h *Harness) Close(ctx context.Context) error {
	destinations, exchanges := h.cleanupNames()
	if len(destinations) == 0 && len(exchanges) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	conn, err := h.drv.Open(ctx, driver.Config{
		Endpoints:      h.endpoints(),
		ClientID:       "f1-bench-cleanup",
		ConnectTimeout: connectTimeout,
	})
	if err != nil {
		return fmt.Errorf("bench: cleanup open %s: %w", h.drv.Name(), err)
	}
	defer func() {
		// A connection that will not close still leaves the broker names
		// deleted on the broker, which is what cleanup owes.
		_ = conn.Close(context.WithoutCancel(ctx))
	}()
	maintenance, ok := conn.Admin().(driver.Maintenance)
	if !ok {
		return fmt.Errorf("bench: %s admin does not implement driver.Maintenance", h.drv.Name())
	}
	exchangeSet := make(map[string]struct{}, len(exchanges))
	for _, name := range exchanges {
		exchangeSet[name] = struct{}{}
	}
	remainingDestinations := purge(ctx, maintenance, destinations)
	remainingExchanges := exchanges
	for attempt := 0; attempt < pruneAttempts && (len(remainingDestinations) > 0 || len(remainingExchanges) > 0); attempt++ {
		names := make([]string, 0, len(remainingDestinations)+len(remainingExchanges))
		names = append(names, remainingDestinations...)
		names = append(names, remainingExchanges...)
		remaining := prune(ctx, maintenance, names)
		remainingDestinations, remainingExchanges = splitCleanupNames(remaining, exchangeSet)
		if len(remainingDestinations) == 0 && len(remainingExchanges) == 0 {
			return nil
		}
		if err := h.clk.Sleep(ctx, pruneWait); err != nil {
			break
		}
	}
	for _, name := range remainingExchanges {
		benchmarkLogger().Warn("benchmark cleanup could not delete exchange",
			"exchange", name, "reason", "driver maintenance port could not delete it")
	}
	if len(remainingDestinations) == 0 {
		return nil
	}
	return fmt.Errorf("bench: %d destinations still present after %d prune attempts: %s",
		len(remainingDestinations), pruneAttempts, strings.Join(remainingDestinations, ", "))
}

// purge empties the named destinations and returns the ones that may still be
// deleted. A destination that could not be emptied is kept rather than
// dropped: pruning refuses one that holds messages, and the retry in Close is
// what turns that refusal into a result instead of a silent leak.
func purge(ctx context.Context, maintenance driver.Maintenance, names []string) []string {
	remaining := make([]string, 0, len(names))
	for _, name := range names {
		if _, err := maintenance.Purge(ctx, name); err != nil && errors.Is(err, driver.ErrDestinationMissing) {
			continue
		}
		remaining = append(remaining, name)
	}
	return remaining
}

func prune(ctx context.Context, maintenance driver.Maintenance, names []string) []string {
	results, err := maintenance.Prune(ctx, names)
	if err != nil {
		return names
	}
	deleted := make(map[string]struct{}, len(results))
	for _, result := range results {
		if result.Deleted {
			deleted[result.Name] = struct{}{}
		}
	}
	remaining := make([]string, 0, len(names))
	for _, name := range names {
		if _, ok := deleted[name]; !ok {
			remaining = append(remaining, name)
		}
	}
	return remaining
}

func splitCleanupNames(names []string, exchanges map[string]struct{}) (destinations, exchangeNames []string) {
	destinations = make([]string, 0, len(names))
	exchangeNames = make([]string, 0, len(names))
	for _, name := range names {
		if _, isExchange := exchanges[name]; isExchange {
			exchangeNames = append(exchangeNames, name)
			continue
		}
		destinations = append(destinations, name)
	}
	return destinations, exchangeNames
}

func (h *Harness) cleanupNames() (destinations, exchanges []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	destinations = make([]string, 0, len(h.declared))
	exchanges = make([]string, 0, len(h.exchanges))
	for name := range h.declared {
		if _, isExchange := h.exchanges[name]; isExchange {
			exchanges = append(exchanges, name)
			continue
		}
		destinations = append(destinations, name)
	}
	slices.Sort(destinations)
	slices.Sort(exchanges)
	return destinations, exchanges
}

func (h *Harness) noteDeclared(spec driver.TopologySpec) {
	if len(spec.Exchanges) == 0 && len(spec.Destinations) == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, exchange := range spec.Exchanges {
		if strings.HasPrefix(exchange.Name, "f1.bench.") {
			h.declared[exchange.Name] = struct{}{}
			h.exchanges[exchange.Name] = struct{}{}
		}
	}
	for _, destination := range spec.Destinations {
		if strings.HasPrefix(destination.Name, "f1.bench.") {
			h.declared[destination.Name] = struct{}{}
		}
	}
}

func (h *Harness) endpoints() []string {
	if h.cfg.Endpoint == "" {
		return nil
	}
	return []string{h.cfg.Endpoint}
}

func validateNamespace(namespace string) error {
	if namespace == "" {
		return errors.New("bench: namespace must not be empty")
	}
	for _, char := range namespace {
		switch {
		case char >= 'a' && char <= 'z', char >= '0' && char <= '9':
		case char == '.', char == '_', char == '-':
		default:
			return fmt.Errorf("bench: namespace %q contains %q, which a broker destination name cannot carry", namespace, char)
		}
	}
	return nil
}

// clientConfig is the configuration every measurement runs on: the shipped
// defaults except where a measurement needs a known shape, and one lane.
func (h *Harness) clientConfig() f1.Config {
	broker := f1.BrokerConfig{
		Driver:         h.drv.Name(),
		Endpoints:      h.endpoints(),
		ConnectTimeout: connectTimeout,
		DriverOptions:  h.driverOptions(),
	}
	return f1.Config{
		Env:        "bench",
		Service:    "bench",
		InstanceID: h.subscription,
		Broker:     broker,
		Topology: f1.TopologyConfig{
			AutoCreate: true,
			Priorities: h.priorities(),
		},
		Codec: f1.CodecConfig{
			Default:        "json",
			MaxHeaderBytes: f1.CoreMaxHeaderBytes,
			MaxBodyBytes:   1 << 20,
		},
		Lifecycle: f1.LifecycleConfig{
			DrainTimeout: drainTimeout,
			HandlerGrace: handlerTimeout,
			CloseTimeout: closeTimeout,
		},
	}
}

// driverOptions is the driver options a measurement's config carries: the
// settings the harness exposes that travel as a documented driver option, and
// nothing else, so a measurement of one exercises the lever a deployment pulls.
// A key belongs to one driver, and a client is built for one driver, so at most
// one of these applies; a setting this harness does not expose is absent rather
// than set to a value the harness guessed.
func (h *Harness) driverOptions() map[string]string {
	options := make(map[string]string, 2)
	if h.drv.Name() == kafkaDriverName && h.cfg.Partitions > 0 {
		// The count travels as the driver option a deployment would set. Its
		// floor half is a guard rather than a measurement: the topics a run
		// creates are new, so nothing this run declares has fewer partitions
		// than it asks for.
		options["kafka.maxExpectedInstances"] = strconv.Itoa(h.cfg.Partitions)
	}
	if h.drv.Name() == rabbitMQDriverName {
		if h.cfg.QueueType != "" {
			// The kind travels the same way, and the driver refuses a value it
			// does not support, so an option this harness carries and the driver
			// does not document fails the run instead of measuring a shape nobody
			// asked for.
			options["rabbitmq.queueType"] = h.cfg.QueueType
		}
		if h.cfg.BrokerPrefetch > 0 {
			options["rabbitmq.brokerPrefetch"] = strconv.Itoa(h.cfg.BrokerPrefetch)
		}
	}
	if len(options) == 0 {
		return nil
	}
	return options
}

// priorities is the priority set a measurement's subscription consumes and its
// publishers address. A measurement that configures none keeps the harness's
// one lane, which is the shape every measurement had before the setting
// existed.
func (h *Harness) priorities() []f1.Priority {
	if len(h.cfg.Priorities) == 0 {
		return []f1.Priority{lane}
	}
	return h.cfg.Priorities
}

// mode selects one measurement's handler policy and publish shape.
type mode int

const (
	modePublish mode = iota
	modeConsume
	modeLatency
	modeRetry
)

// attempts is the number of times one corpus message is published, delivered
// and settled in this measurement. The retry path is the one shape where that
// is more than one: the core republishes the body to a retry destination and
// settles both deliveries.
func (m mode) attempts() int {
	if m == modeRetry {
		return 2
	}
	return 1
}

// run is one measurement's live state: the tracker that counts at the port, the
// client, and the subscription runner.
type run struct {
	h      *Harness
	m      mode
	tr     *tracker
	client *f1.Client
	// wrapped is the one counting driver this run handed the client. The
	// connection and the consumer the client opens hang off it, which is how
	// the sampler reaches the consumer to ask it for its backlog.
	wrapped *countingDriver
	// sample is the run's sampler, and is nil for a measurement that records no
	// backlog and no heap. The teardown stops it before it releases anything the
	// sampler queries, so this field is the sampler's whole lifetime.
	sample *sampler

	ready     chan struct{}
	readyOnce sync.Once
	runner    *f1.Runner
	runDone   chan struct{}
	runErr    error
	// hold is true while a backlog measurement's pump must keep deliveries away
	// from the core, and gate is closed to release them. The pump reads hold on
	// every delivery and the measurement sets it once, before the corpus is
	// published, so a delivery the driver hands over while the corpus is still
	// going out waits at the gate, and one handed over after the window opened
	// does not. The gate is what releases a parked pump, and a teardown releases
	// it through the consumer's own stopped channel instead.
	hold atomic.Bool
	gate chan struct{}
	// drained reports that the subscription's consumer has already been
	// stopped, so stop does not drain a runner that is finished.
	drained bool

	stopOnce sync.Once
	stopErr  error
}

func (h *Harness) measure(ctx context.Context, m mode) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, h.cfg.Timeout)
	defer cancel()
	r := &run{
		h:     h,
		m:     m,
		tr:    newTracker(h.clk, h.cfg.Messages, m.attempts()),
		ready: make(chan struct{}),
		gate:  make(chan struct{}),
	}
	r.wrapped = r.wrappedDriver()
	// The baseline the goroutine delta is read against is taken before the
	// client exists, so it holds the harness's own goroutines and the driver
	// code already loaded, and what is left of the delta at the end is what
	// this run left behind.
	before := runtime.NumGoroutine()
	client, err := f1.New(ctx, h.clientConfig(),
		f1.WithDriver(r.wrapped),
		f1.WithPublishTopics(h.topic),
		f1.WithLogger(benchmarkLogger()),
	)
	if err != nil {
		return Result{}, fmt.Errorf("bench: %s new client: %w", h.drv.Name(), err)
	}
	r.client = client
	defer func() {
		// The explicit stops below carry the error that matters; this backstop
		// only keeps a connection from outliving a measurement that returned
		// before reaching one.
		_ = r.stop(ctx)
	}()
	if err := r.subscribe(ctx); err != nil {
		return Result{}, errors.Join(err, r.stop(ctx))
	}
	// One warm-up message settles outside the window, before the clocks below
	// start. It proves the entry point routes to a bound destination and the
	// consumer is attached and joined: a broker whose subscription becomes
	// ready when its topology is declared, and not when its consumer joins,
	// would otherwise have that join charged to the first message of the
	// corpus. The tracker records nothing before the window opens, so the
	// warm-up message and its settlement - one settlement, or two on the retry
	// path - stay out of the corpus count.
	if err := r.warmUp(ctx); err != nil {
		return Result{}, errors.Join(err, r.stop(ctx))
	}
	if m == modePublish {
		// A publish measurement needs the subscription's topology and not its
		// consumer. On a broker that fans out at publish time the entry point
		// is an exchange, and a publish nothing is bound to comes back
		// unroutable, so the subscription has to be declared first. Its
		// consumer is drained away once the warm-up has proved it is joined:
		// leaving it running would put a second workload on the machine whose
		// publish rate is being measured, and a group left before it has
		// joined cannot be left at all.
		if err := r.drainRunner(ctx); err != nil {
			return Result{}, errors.Join(err, r.stop(ctx))
		}
	}
	start := h.clk.Now()
	if m == modeConsume && h.cfg.Backlog {
		// A backlog measurement makes its corpus before it opens the interval
		// it measures. Its window is armed first, so the tracker sees every
		// publish, and no delivery or settlement, of that corpus: the gate
		// holds every delivery out of the core's reach until the load is done,
		// and the interval the rate is computed over starts when the last
		// publish call returns.
		r.hold.Store(true)
		r.tr.arm()
		if err := r.publishBulk(ctx); err != nil {
			return Result{}, errors.Join(err, r.stop(ctx))
		}
		start = h.clk.Now()
	} else {
		r.tr.arm()
	}
	// A consume measurement samples while its window is open: the backlog the
	// consumer has not read and the live heap are both figures the run's rate
	// cannot show, and both belong to the interval the clock covers. The
	// sampler belongs to the run, so the teardown stops it before the teardown
	// releases the consumer it queries, and its goroutine is gone before the
	// delta counted below is read. A backlog measurement starts it after its
	// corpus, so the heap and the backlog it reports are the drain's.
	if m == modeConsume {
		r.sample = newSampler(ctx, h.clk, r.wrapped.consumer(), h.drv.Capabilities().LagQueryable)
	}
	switch {
	case m == modeConsume && h.cfg.Backlog:
		// Every delivery this run has accepted so far is waiting at the gate;
		// opening it starts the drain the measured interval covers.
		close(r.gate)
	case m == modeLatency:
		err = r.publishSequential(ctx)
	default:
		err = r.publishBulk(ctx)
	}
	if err != nil {
		return Result{}, errors.Join(err, r.stop(ctx))
	}
	if m == modePublish {
		elapsed := h.clk.Since(start)
		atEnd := r.tr.snapshot()
		if err := r.stop(ctx); err != nil {
			return Result{}, err
		}
		if err := atEnd.verifyPublished(h.cfg.Messages, "when the measurement ended"); err != nil {
			return Result{}, err
		}
		return Result{
			Messages:       h.cfg.Messages,
			Publishers:     h.cfg.Publishers,
			Concurrency:    h.cfg.Concurrency,
			Elapsed:        elapsed,
			GoroutineDelta: runtime.NumGoroutine() - before,
		}, nil
	}
	if err := r.await(ctx, r.tr.hasStopped); err != nil {
		return Result{}, errors.Join(err, r.stop(ctx))
	}
	elapsed := r.tr.stopTime().Sub(start)
	atStop := r.tr.snapshot()
	var (
		lagMax  int64
		heapMax uint64
	)
	if r.sample != nil {
		// The window has closed, so the sampling is stopped here rather than
		// left to the teardown: a sample taken while the run is torn down is a
		// figure about a consumer that is being released, not about the
		// interval that was measured.
		r.sample.stop()
		lagMax, heapMax = r.sample.lagMax(), r.sample.heapMax()
	}
	if err := r.stop(ctx); err != nil {
		return Result{}, err
	}
	// The sample is taken once the client is closed and the consumer it opened
	// has stopped, which is the quiet point the delta is meaningful at. Both
	// verifications run first so a run that failed on its own counts is
	// reported as that rather than as the goroutines it was holding.
	if err := atStop.verify(h.cfg.Messages, m.attempts(), h.cfg.CountDuplicates, "when the clock stopped"); err != nil {
		return Result{}, err
	}
	afterStop := r.tr.snapshot()
	if err := afterStop.verify(h.cfg.Messages, m.attempts(), h.cfg.CountDuplicates, "after the run stopped"); err != nil {
		return Result{}, err
	}
	settleLatencies, settleSequences := r.tr.settledLatencies()
	return Result{
		Messages:        h.cfg.Messages,
		Publishers:      h.cfg.Publishers,
		Concurrency:     h.cfg.Concurrency,
		HandlerWork:     h.cfg.HandlerWork,
		Elapsed:         elapsed,
		Latencies:       r.tr.latencies(),
		SettleLatencies: settleLatencies,
		SettleSequences: settleSequences,
		RetryTier:       h.cfg.RetryTier,
		Duplicates:      afterStop.duplicates,
		LagMax:          lagMax,
		HeapMax:         heapMax,
		GoroutineDelta:  runtime.NumGoroutine() - before,
	}, nil
}

func (r *run) wrappedDriver() *countingDriver {
	return &countingDriver{
		inner:     r.h.drv,
		tr:        r.tr,
		ready:     r.ready,
		readyOnce: &r.readyOnce,
		onSpec:    r.h.noteDeclared,
		hold:      &r.hold,
		gate:      r.gate,
	}
}

// subscription is the shape every measurement subscribes with: one topic on
// one priority, one handler, the configured handler concurrency and in-flight
// budget, and the retry ladder the measurement needs. It is built here rather
// than inside subscribe so the shape a measurement states is one thing to
// read, and so a test can ask what a configuration resolves to without a
// broker and without a client.
//
// A zero Concurrency or Prefetch travels as zero rather than as the value the
// package ships with, which is what keeps the shipped defaults measured by the
// measurements that say they measure them.
func (r *run) subscription() f1.Subscription {
	sub := f1.Subscription{
		Name:           r.h.subscription,
		Topics:         []string{r.h.topic},
		Priorities:     r.h.priorities(),
		Concurrency:    r.h.cfg.Concurrency,
		Prefetch:       r.h.cfg.Prefetch,
		Fairness:       r.h.cfg.Fairness,
		HandlerTimeout: handlerTimeout,
		Handlers:       map[string]f1.Handler{r.h.topic: f1.HandlerFunc(r.handle)},
	}
	if r.h.cfg.Ordered {
		sub.Mode = f1.OrderedByKey
	}
	if r.m == modeRetry {
		// Two attempts, one tier: the first delivery fails and the retry copy
		// arrives after exactly the delay the measurement reports.
		sub.Retry = f1.RetryConfig{MaxAttempts: 2, Tiers: []time.Duration{r.h.cfg.RetryTier}}
	}
	return sub
}

// subscribe starts the measurement's subscription and blocks until its
// topology has been declared. Every measurement needs the subscription: the
// publish measurement for the destination its entry point routes to, and the
// others for the consumer that settles the corpus.
func (r *run) subscribe(ctx context.Context) error {
	runner, err := r.client.Subscribe(ctx, r.subscription())
	if err != nil {
		return fmt.Errorf("bench: subscribe: %w", err)
	}
	r.runner = runner
	r.runDone = make(chan struct{})
	go func() {
		defer close(r.runDone)
		r.runErr = runner.Run(ctx)
	}()
	select {
	case <-r.ready:
		return nil
	case <-r.runDone:
		return fmt.Errorf("bench: subscription stopped before it was declared: %w", r.runError())
	case <-ctx.Done():
		return fmt.Errorf("bench: subscription was not declared: %w", ctx.Err())
	}
}

// handle is the subscription's handler. It records the delivery, holds it for
// the configured work when this measurement applies one, and for the retry
// measurement fails every first attempt.
//
// The hold is a wait on the harness clock, and the context's error is returned
// when that wait is cut short: a delivery whose wait was interrupted was not
// handled, and settling it would count a message the handler never finished.
func (r *run) handle(ctx context.Context, event *f1.Event) error {
	r.tr.noteDelivery(string(event.Raw()))
	if r.m == modeConsume && r.h.cfg.HandlerWork > 0 {
		if err := r.h.clk.Sleep(ctx, r.h.cfg.HandlerWork); err != nil {
			return err
		}
	}
	if r.m == modeRetry && event.Attempt() < 2 {
		return errors.New("bench: forced retry")
	}
	return nil
}

// publishOptions is the option set one corpus message is published with. An
// ordered measurement attaches the key its sequence maps to, which is what
// gives the subscription's ordering something to serialise; an unordered
// measurement sends no key, exactly as every measurement did before ordering
// existed. A measurement over more than one priority sends each sequence to the
// priority its index maps to, so the corpus is split evenly across the
// destinations and neither is the one thing the rate describes.
func (r *run) publishOptions(seq int) []f1.PublishOption {
	var options []f1.PublishOption
	if r.h.cfg.Ordered {
		options = append(options, f1.WithKey(r.h.keys[seq%orderedKeys]))
	}
	if priorities := r.h.priorities(); len(priorities) > 1 {
		options = append(options, f1.WithPriority(priorities[seq%len(priorities)]))
	}
	return options
}

// publish sends one corpus message.
func (r *run) publish(ctx context.Context, seq int) error {
	_, err := r.client.Publisher().Publish(ctx, r.h.topic, payload{Seq: seq}, r.publishOptions(seq)...)
	return err
}

// publishBulk distributes the corpus across the configured publishers, by the
// path the measurement configured: the single-message one, which is what every
// measurement did before the batch setting existed, or the chunked one.
func (r *run) publishBulk(ctx context.Context) error {
	if r.h.cfg.PublishBatch > 0 {
		return r.publishChunked(ctx)
	}
	var (
		wait   sync.WaitGroup
		mu     sync.Mutex
		failed error
	)
	for publisher := range r.h.cfg.Publishers {
		wait.Go(func() {
			for seq := publisher; seq < r.h.cfg.Messages; seq += r.h.cfg.Publishers {
				if err := r.publish(ctx, seq); err != nil {
					mu.Lock()
					failed = errors.Join(failed, fmt.Errorf("bench: publish %d: %w", seq, err))
					mu.Unlock()
					return
				}
			}
		})
	}
	wait.Wait()
	return failed
}

// publishChunked loads the corpus one call per chunk, with each publisher
// owning a contiguous span of it so no two publishers send the same sequence
// and no sequence goes unsent.
func (r *run) publishChunked(ctx context.Context) error {
	var (
		wait   sync.WaitGroup
		mu     sync.Mutex
		failed error
	)
	for _, span := range publishSpans(r.h.cfg.Messages, r.h.cfg.Publishers) {
		wait.Go(func() {
			for start := span[0]; start < span[1]; start += r.h.cfg.PublishBatch {
				end := min(start+r.h.cfg.PublishBatch, span[1])
				messages := make([]f1.Message, 0, end-start)
				for seq := start; seq < end; seq++ {
					messages = append(messages, f1.Message{
						EventType: r.h.topic,
						Payload:   payload{Seq: seq},
						Opts:      r.publishOptions(seq),
					})
				}
				if _, err := r.client.Publisher().PublishBatch(ctx, messages); err != nil {
					mu.Lock()
					failed = errors.Join(failed, fmt.Errorf("bench: publish %d-%d: %w", start, end, err))
					mu.Unlock()
					return
				}
			}
		})
	}
	wait.Wait()
	return failed
}

// publishSpans splits a corpus into one contiguous span per publisher. The
// spans cover every sequence exactly once, which is what lets a chunked
// publisher treat its span as the whole of its share: the tracker fails the
// measurement on a sequence that was never published, so a span list that left
// a gap would be a wrong corpus rather than a slower one.
func publishSpans(messages, publishers int) [][2]int {
	size := (messages + publishers - 1) / publishers
	spans := make([][2]int, 0, publishers)
	for start := 0; start < messages; start += size {
		spans = append(spans, [2]int{start, min(start+size, messages)})
	}
	return spans
}

// publishSequential publishes one message at a time and waits for each one to
// settle before publishing the next, which is what makes the recorded
// durations round trips rather than queue depths.
func (r *run) publishSequential(ctx context.Context) error {
	for seq := range r.h.cfg.Messages {
		r.tr.notePublishedAt(seq, r.h.clk.Now())
		if err := r.publish(ctx, seq); err != nil {
			return fmt.Errorf("bench: publish %d: %w", seq, err)
		}
		settled := seq + 1
		if err := r.await(ctx, func() bool { return r.tr.settledTotal() >= settled }); err != nil {
			return err
		}
	}
	return nil
}

// await blocks until check holds, the subscription stops, or ctx expires. It
// wakes on the tracker's own signal rather than re-checking on a timer, so a
// measurement never spends a core asking whether it is finished.
func (r *run) await(ctx context.Context, check func() bool) error {
	for {
		changed := r.tr.changed()
		if check() {
			return nil
		}
		select {
		case <-changed:
		case <-r.runDone:
			return fmt.Errorf("bench: subscription stopped before the measurement finished: %w", r.runError())
		case <-ctx.Done():
			return fmt.Errorf("bench: awaiting settlement: %w", ctx.Err())
		}
	}
}

func (r *run) runError() error {
	if r.runErr == nil {
		return errors.New("runner returned")
	}
	return r.runErr
}

// warmUp publishes one corpus message and waits for it to settle, which proves
// the entry point routes to a bound destination and the consumer is attached.
// It runs outside every measured window.
func (r *run) warmUp(ctx context.Context) error {
	if err := r.publish(ctx, 0); err != nil {
		return fmt.Errorf("bench: warm-up publish: %w", err)
	}
	want := r.m.attempts()
	return r.await(ctx, func() bool { return r.tr.preludeSettlements() >= want })
}

// drainRunner stops the subscription's consumer and leaves the client, and the
// destinations the subscription declared, in place. A runner that ended on its
// own reports why: a failed release leaves the consumer open, and a driver that
// refuses to close a connection with a consumer on it would otherwise surface
// only as a cleanup failure.
func (r *run) drainRunner(ctx context.Context) error {
	if r.runner == nil {
		return nil
	}
	drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), drainTimeout)
	defer cancel()
	if err := r.runner.Drain(drainCtx); err != nil {
		return fmt.Errorf("bench: drain before publishing: %w", err)
	}
	select {
	case <-r.runDone:
	case <-drainCtx.Done():
		return fmt.Errorf("bench: runner did not stop after draining: %w", drainCtx.Err())
	}
	r.drained = true
	return r.runErr
}

// stop drains the subscription and closes the client exactly once.
func (r *run) stop(ctx context.Context) error {
	r.stopOnce.Do(func() {
		r.stopErr = r.stopNow(ctx)
	})
	return r.stopErr
}

func (r *run) stopNow(ctx context.Context) error {
	var problems []error
	// The sampler goes first: it queries the consumer, and everything below
	// releases it. Stopping it here rather than at the callers covers every
	// path out of a measurement, including the ones that return on an error
	// and never reach the window's own end.
	r.sample.stop()
	parent := context.WithoutCancel(ctx)
	if r.runner != nil && !r.drained {
		drainCtx, cancel := context.WithTimeout(parent, drainTimeout)
		if err := r.runner.Drain(drainCtx); err != nil {
			problems = append(problems, fmt.Errorf("bench: drain: %w", err))
		}
		select {
		case <-r.runDone:
		case <-drainCtx.Done():
			problems = append(problems, fmt.Errorf("bench: runner did not stop: %w", drainCtx.Err()))
		}
		cancel()
	}
	if r.client != nil {
		closeCtx, cancel := context.WithTimeout(parent, closeTimeout)
		if err := r.client.Close(closeCtx); err != nil {
			problems = append(problems, fmt.Errorf("bench: close client: %w", err))
		}
		cancel()
	}
	return errors.Join(problems...)
}

// benchmarkLogger reports the driver diagnostics a benchmark run has to show.
// The core logs a routine notification - a Kafka group assignment, among
// others - at info level for every consumer it opens, and a benchmark's output
// is the numbers it reports. Warnings and errors still reach stderr: a
// capability the driver does not have, or a condition it survived, is a reason
// to read a number differently.
func benchmarkLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// heapLiveBytes is the runtime/metrics figure for the bytes live objects
// occupy: the heap in use, without the spans and the free space the runtime
// holds for it. It is read through runtime/metrics and never through
// runtime.ReadMemStats, which stops the world: a stop-the-world read during a
// measured window would delay the very corpus the window exists to time.
const heapLiveBytes = "/memory/classes/heap/objects:bytes"

// sampler records, over one measured window, the two figures a rate cannot
// show: the backlog the consumer has not read yet, and the live heap the
// process holds.
//
// Both are recorded as the largest value seen rather than as an average. The
// question they answer is whether something grew without bound, and a mean over
// a window that starts empty and ends drained answers it with a number that
// hides its own peak.
type sampler struct {
	clk      clock.Clock
	consumer driver.Consumer
	// queryLag is cleared when the consumer cannot answer for its backlog, so
	// one refusal does not turn into a query on every tick. It is written by
	// the sampling goroutine alone after the sampler is built.
	queryLag bool

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once

	mu       sync.Mutex
	peakLag  int64
	peakHeap uint64
}

// newSampler starts sampling and returns it. The consumer may be nil, and a
// driver that cannot be asked for its backlog is not asked: the heap is then
// the whole of what is recorded.
func newSampler(parent context.Context, clk clock.Clock, consumer driver.Consumer, queryLag bool) *sampler {
	ctx, cancel := context.WithCancel(parent)
	s := &sampler{
		clk:      clk,
		consumer: consumer,
		queryLag: queryLag && consumer != nil,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	go s.run(ctx)
	return s
}

// run samples until ctx is done and then closes done, which is how a
// measurement waits for the last query to return before it counts the process's
// goroutines.
func (s *sampler) run(ctx context.Context) {
	defer close(s.done)
	// The first sample is taken before the first wait, so a window shorter than
	// a sample interval still carries a figure instead of the zero value a
	// sleep-then-sample loop would leave behind.
	heap := []metrics.Sample{{Name: heapLiveBytes}}
	for {
		if s.queryLag {
			backlog, err := s.consumer.Lag(ctx)
			if err != nil {
				// A measurement does not fail over a figure it reports beside
				// the rate, and a driver whose backlog cannot be read has
				// nothing to contribute to it. The heap sample continues.
				s.queryLag = false
			} else {
				s.noteLag(backlog)
			}
		}
		metrics.Read(heap)
		s.noteHeap(heap[0].Value)
		if err := s.clk.Sleep(ctx, sampleInterval); err != nil {
			return
		}
	}
}

// noteLag records the largest backlog the consumer reported, across the
// destinations it reads.
func (s *sampler) noteLag(backlog map[string]int64) {
	var largest int64
	for _, depth := range backlog {
		largest = max(largest, depth)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peakLag = max(s.peakLag, largest)
}

// noteHeap records the live heap a sample read.
func (s *sampler) noteHeap(value metrics.Value) {
	if value.Kind() != metrics.KindUint64 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peakHeap = max(s.peakHeap, value.Uint64())
}

// stop ends the sampling and waits for it to return. A nil sampler is a
// measurement that records neither figure, which is every mode but consume, and
// stopping one is a no-op so a teardown can stop whatever it has.
func (s *sampler) stop() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		s.cancel()
		<-s.done
	})
}

// lagMax reports the largest backlog recorded.
func (s *sampler) lagMax() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peakLag
}

// heapMax reports the largest live heap recorded, in bytes.
func (s *sampler) heapMax() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peakHeap
}

// payload is one corpus message. Its encoded form is what the tracker keys a
// publish, a delivery and a settlement on, so a message and its retry copy -
// republished with the same body and one more attempt - land on one sequence.
type payload struct {
	Seq int `json:"seq"`
}

// corpusBody is the encoded form of one corpus payload. The core encodes the
// payload with the JSON codec, whose encoding of this single-field struct is
// exactly this, so the harness recognises a delivery without decoding every
// body. A mismatch is not silent: the body is then unknown to the tracker and
// the correctness check fails on it.
func corpusBody(seq int) string {
	return fmt.Sprintf(`{"seq":%d}`, seq)
}

// tracker records what one measurement published, delivered and settled, keyed
// by the corpus sequence a body carries.
type tracker struct {
	clk      clock.Clock
	attempts int
	want     int

	mu          sync.Mutex
	seqByBody   map[string]int
	publishes   []int
	deliveries  []int
	settles     []int
	publishedAt []time.Time
	settledAt   []time.Time
	// settleLatencies holds one delivery-to-ack-call duration per settlement
	// recorded in the window, in the order the settlements happened. It is
	// appended to rather than indexed by sequence: a redelivery of one
	// sequence is a settlement of its own, and a p99 over an index would keep
	// only one of them.
	settleLatencies []time.Duration
	// settleSequences is parallel to settleLatencies and carries the corpus
	// sequence that produced each sample.
	settleSequences []int

	// settled counts the settlements that advanced the corpus, and is what the
	// measurement ends on. It is maintained as it happens rather than summed
	// from settles, which is O(corpus) per message and would put the harness's
	// own work inside the window it measures.
	settled int
	// duplicates counts deliveries past the ones the shape expects.
	duplicates int

	unknownPublishes   int
	unknownDeliveries  int
	unknownSettlements int

	stopped       bool
	stopAt        time.Time
	settledAtStop int

	// recording is false until the measured window opens, so the warm-up a
	// measurement may run before it is not counted as part of the corpus.
	recording bool

	// prelude counts settlements recorded before the window opened. A
	// measurement waits for its warm-up to finish through this counter, so the
	// wait does not depend on the corpus counts the warm-up is excluded from.
	prelude int

	change chan struct{}
}

func newTracker(clk clock.Clock, corpus, attempts int) *tracker {
	seqByBody := make(map[string]int, corpus)
	for seq := range corpus {
		seqByBody[corpusBody(seq)] = seq
	}
	return &tracker{
		clk:             clk,
		attempts:        attempts,
		want:            corpus * attempts,
		seqByBody:       seqByBody,
		publishes:       make([]int, corpus),
		deliveries:      make([]int, corpus),
		settles:         make([]int, corpus),
		publishedAt:     make([]time.Time, corpus),
		settledAt:       make([]time.Time, corpus),
		settleLatencies: make([]time.Duration, 0, corpus*attempts),
		settleSequences: make([]int, 0, corpus*attempts),
		change:          make(chan struct{}),
	}
}

// arm opens the measured window. Everything recorded before it - the warm-up a
// publish measurement runs, and the settlement it waits for - is dropped, so a
// corpus count covers exactly one window.
func (t *tracker) arm() {
	t.mu.Lock()
	defer t.mu.Unlock()
	clear(t.publishes)
	clear(t.deliveries)
	clear(t.settles)
	clear(t.publishedAt)
	clear(t.settledAt)
	t.settleLatencies = t.settleLatencies[:0]
	t.settleSequences = t.settleSequences[:0]
	t.prelude = 0
	t.recording = true
	t.signalLocked()
}

// preludeSettlements reports how many settlements were recorded before the
// measured window opened.
func (t *tracker) preludeSettlements() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.prelude
}

// changed returns a channel that the next recorded event closes.
func (t *tracker) changed() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.change
}

func (t *tracker) signalLocked() {
	close(t.change)
	t.change = make(chan struct{})
}

func (t *tracker) notePublishedAt(seq int, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.recording {
		return
	}
	t.publishedAt[seq] = at
}

// notePublish records the messages one producer call published. A partial
// failure published only the indexes the driver did not report as failed.
func (t *tracker) notePublish(messages []driver.OutboundMessage, err error) {
	var failed map[int]error
	if partial, ok := errors.AsType[*driver.PublishError](err); ok {
		failed = partial.Failed
	} else if err != nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.recording {
		return
	}
	for index, message := range messages {
		if _, skip := failed[index]; skip {
			continue
		}
		seq, known := t.seqByBody[string(message.Body)]
		if !known {
			t.unknownPublishes++
			continue
		}
		t.publishes[seq]++
	}
	t.signalLocked()
}

func (t *tracker) noteDelivery(body string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.recording {
		return
	}
	seq, known := t.seqByBody[body]
	if !known {
		t.unknownDeliveries++
	} else {
		t.deliveries[seq]++
		if t.deliveries[seq] > t.attempts {
			t.duplicates++
		}
	}
	t.signalLocked()
}

// noteSettled records one Ack that returned nil and stops the measurement when
// the expected number of settlements has returned. This is the only place a
// measurement ends: a delivery, or a handler that has been entered, is a step
// before settlement, and stopping there would report a rate for work that has
// not finished.
//
// latency is how long that delivery spent between reaching the core and the
// ack call, which is the interval the rate cannot show. A settlement of a body
// that was never published has no latency to record: the harness did not see
// the delivery it would have measured from.
//
// A settlement counts towards the total only while its sequence has fewer than
// the shape's attempts recorded. A redelivery arrives after its sequence is
// already complete, and counting it would reach the expected total while a
// message was still unsettled, ending the window early and reporting a rate for
// a corpus that had not finished.
func (t *tracker) noteSettled(body string, latency time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.recording {
		t.prelude++
		t.signalLocked()
		return
	}
	seq, known := t.seqByBody[body]
	if !known {
		t.unknownSettlements++
		t.signalLocked()
		return
	}
	if !t.stopped {
		// A settlement that arrives after the clock stopped is evidence that
		// the corpus finished, and is counted as such, but it is not part of
		// the interval the percentiles describe: a drain-time redelivery can
		// take arbitrarily long, and one of them would move a p99 that is read
		// as the shape of the measured window.
		t.settleLatencies = append(t.settleLatencies, latency)
		t.settleSequences = append(t.settleSequences, seq)
	}
	t.settles[seq]++
	if t.settles[seq] <= t.attempts {
		t.settled++
	}
	if t.settledAt[seq].IsZero() {
		t.settledAt[seq] = t.clk.Now()
	}
	t.stopIfReachedLocked(t.settled)
	t.signalLocked()
}

// stopIfReachedLocked stops the measurement once count has reached the number
// of settlements the corpus expects.
func (t *tracker) stopIfReachedLocked(count int) {
	if t.stopped || count < t.want {
		return
	}
	t.stopped = true
	t.stopAt = t.clk.Now()
	t.settledAtStop = count
}

func (t *tracker) settledTotal() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.settled
}

func (t *tracker) hasStopped() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopped
}

func (t *tracker) stopTime() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopAt
}

// latencies returns each message's publish-to-ack duration, in sequence order.
func (t *tracker) latencies() []time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	latencies := make([]time.Duration, 0, len(t.settles))
	for seq := range t.settles {
		if t.settledAt[seq].IsZero() || t.publishedAt[seq].IsZero() {
			continue
		}
		latencies = append(latencies, t.settledAt[seq].Sub(t.publishedAt[seq]))
	}
	return latencies
}

// settledLatencies returns copies of the measured window's settle latencies
// and their corpus sequences, in the order the settlements were recorded.
func (t *tracker) settledLatencies() ([]time.Duration, []int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.settleLatencies), slices.Clone(t.settleSequences)
}

// snapshot is the tracker's state at one instant.
type snapshot struct {
	publishes  []int
	deliveries []int
	settles    []int

	duplicates int

	unknownPublishes   int
	unknownDeliveries  int
	unknownSettlements int

	stopped       bool
	settledAtStop int
}

func (t *tracker) snapshot() snapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	return snapshot{
		publishes:          slices.Clone(t.publishes),
		deliveries:         slices.Clone(t.deliveries),
		settles:            slices.Clone(t.settles),
		duplicates:         t.duplicates,
		unknownPublishes:   t.unknownPublishes,
		unknownDeliveries:  t.unknownDeliveries,
		unknownSettlements: t.unknownSettlements,
		stopped:            t.stopped,
		settledAtStop:      t.settledAtStop,
	}
}

// verifyPublished checks what a publish measurement can claim: every corpus
// message was published exactly once, and nothing else was.
func (s snapshot) verifyPublished(corpus int, when string) error {
	var problems []error
	for seq := range corpus {
		if s.publishes[seq] != 1 {
			problems = append(problems, fmt.Errorf("%s: message %d was published %d times, want 1", when, seq, s.publishes[seq]))
		}
	}
	if s.unknownPublishes != 0 {
		problems = append(problems, fmt.Errorf("%s: %d published bodies were not corpus messages", when, s.unknownPublishes))
	}
	return errors.Join(problems...)
}

// verify checks the measurement's correctness claim at one instant: at the
// instant the clock stopped every corpus message had settled, and every corpus
// message was published, delivered and settled exactly once per attempt, with
// nothing that was not published arriving at all. when names the instant, so a
// failure says which check saw it.
//
// countDuplicates makes a delivery or a settlement past the shape's attempts a
// figure to report rather than a failure, which is how a measurement of a path
// that trades duplicates for something else keeps its correctness claim on
// everything else. A shortfall still fails, and so does a publish past the
// shape's attempts: neither is what that trade produces. A measurement without
// it fails on a delivery it did not expect, which is what every measurement
// did before the flag existed.
func (s snapshot) verify(corpus, attempts int, countDuplicates bool, when string) error {
	var problems []error
	if s.stopped && s.settledAtStop != corpus*attempts {
		problems = append(problems, fmt.Errorf(
			"%s: %d deliveries had settled, want %d: the clock stopped before every message settled",
			when, s.settledAtStop, corpus*attempts))
	}
	for seq := range corpus {
		if s.publishes[seq] != attempts {
			problems = append(problems, fmt.Errorf("%s: message %d was published %d times, want %d", when, seq, s.publishes[seq], attempts))
		}
		if s.deliveries[seq] != attempts && (!countDuplicates || s.deliveries[seq] <= attempts) {
			problems = append(problems, fmt.Errorf("%s: message %d was delivered %d times, want %d", when, seq, s.deliveries[seq], attempts))
		}
		if s.settles[seq] != attempts && (!countDuplicates || s.settles[seq] <= attempts) {
			problems = append(problems, fmt.Errorf("%s: message %d was settled %d times, want %d", when, seq, s.settles[seq], attempts))
		}
	}
	if s.unknownPublishes != 0 {
		problems = append(problems, fmt.Errorf("%s: %d published bodies were not corpus messages", when, s.unknownPublishes))
	}
	if s.unknownDeliveries != 0 {
		problems = append(problems, fmt.Errorf("%s: %d deliveries carried a body that was never published", when, s.unknownDeliveries))
	}
	if s.unknownSettlements != 0 {
		problems = append(problems, fmt.Errorf("%s: %d settlements carried a body that was never published", when, s.unknownSettlements))
	}
	return errors.Join(problems...)
}

// countingDriver wraps the driver under test. It changes no behaviour: it
// forwards the driver value, the connection, and the admin with their meaning
// intact, and only records what crosses the port.
type countingDriver struct {
	inner     driver.Driver
	tr        *tracker
	ready     chan struct{}
	readyOnce *sync.Once
	onSpec    func(driver.TopologySpec)
	// hold and gate are the run's backlog gate, which the pump waits on before
	// it hands a delivery to the core. They are nil for a run that holds
	// nothing back.
	hold *atomic.Bool
	gate chan struct{}

	// conn is the connection the client opened through this wrapper. The core
	// keeps its connection to itself, so this is where the wrapper finds the
	// consumer the core opened, which is the one a backlog sample queries. The
	// read is the last write the client made, and a measurement queries it after
	// the subscription exists.
	conn atomic.Pointer[countingConn]
}

func (d *countingDriver) Name() string { return d.inner.Name() }

func (d *countingDriver) Capabilities() driver.Capabilities { return d.inner.Capabilities() }

// consumer returns the consumer the client opened through this wrapper, and nil
// before it has opened one.
func (d *countingDriver) consumer() *countingConsumer {
	conn := d.conn.Load()
	if conn == nil {
		return nil
	}
	return conn.consumer.Load()
}

func (d *countingDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	conn, err := d.inner.Open(ctx, cfg)
	if err != nil || conn == nil {
		return conn, err
	}
	wrapped := &countingConn{
		Conn: conn, tr: d.tr, ready: d.ready, readyOnce: d.readyOnce, onSpec: d.onSpec,
		hold: d.hold, gate: d.gate,
	}
	d.conn.Store(wrapped)
	return wrapped, nil
}

type countingConn struct {
	driver.Conn
	tr        *tracker
	ready     chan struct{}
	readyOnce *sync.Once
	onSpec    func(driver.TopologySpec)
	hold      *atomic.Bool
	gate      chan struct{}

	// consumer is the last consumer this connection handed out, which is the one
	// the subscription's runner reads. It is kept here rather than derived from
	// the tracker because a backlog query is a question about a live consumer,
	// not a count.
	consumer atomic.Pointer[countingConsumer]
}

func (c *countingConn) Admin() driver.Admin {
	admin := c.Conn.Admin()
	if admin == nil {
		return nil
	}
	return &countingAdmin{Admin: admin, ready: c.ready, readyOnce: c.readyOnce, onSpec: c.onSpec}
}

func (c *countingConn) Producer(ctx context.Context, cfg driver.ProducerConfig) (driver.Producer, error) {
	producer, err := c.Conn.Producer(ctx, cfg)
	if err != nil || producer == nil {
		return producer, err
	}
	return &countingProducer{Producer: producer, tr: c.tr}, nil
}

func (c *countingConn) Consumer(ctx context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	consumer, err := c.Conn.Consumer(ctx, cfg)
	if err != nil || consumer == nil {
		return consumer, err
	}
	wrapped := &countingConsumer{
		Consumer: consumer,
		tr:       c.tr,
		out:      make(chan driver.InboundMessage),
		stopped:  make(chan struct{}),
		drained:  make(chan struct{}),
		hold:     c.hold,
		gate:     c.gate,
	}
	c.consumer.Store(wrapped)
	go wrapped.pump()
	return wrapped, nil
}

type countingProducer struct {
	driver.Producer
	tr *tracker
}

func (p *countingProducer) Publish(ctx context.Context, messages ...driver.OutboundMessage) error {
	err := p.Producer.Publish(ctx, messages...)
	p.tr.notePublish(messages, err)
	return err
}

// countingAdmin records the broker names the core asks for, which is what
// cleanup later deletes, and signals readiness when the subscription's own
// topology has been declared.
//
// The scope is the marker: the core sets it only on a subscription's topology,
// and the runner returns from this call before it opens a consumer, so a
// publish that waits for this signal cannot arrive before the destinations and
// their bindings exist.
type countingAdmin struct {
	driver.Admin
	ready     chan struct{}
	readyOnce *sync.Once
	onSpec    func(driver.TopologySpec)
}

func (a *countingAdmin) EnsureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	diff, err := a.Admin.EnsureTopology(ctx, spec)
	if err != nil {
		return diff, err
	}
	if a.onSpec != nil {
		a.onSpec(spec)
	}
	if a.ready != nil && len(spec.Scope) > 0 {
		a.readyOnce.Do(func() { close(a.ready) })
	}
	return diff, nil
}

// countingConsumer forwards every consumer verb and rewrites the settlement of
// each delivery, so a message counts as settled only when its Ack returns nil.
type countingConsumer struct {
	driver.Consumer
	tr      *tracker
	out     chan driver.InboundMessage
	stopped chan struct{}
	// hold and gate are the run's backlog gate: while hold is set the pump
	// keeps the deliveries the driver handed it out of the core's reach, so a
	// measurement that opens its window on a backlog it published first sees no
	// delivery before that window opens. They are nil, or a gate that is never
	// held, for a measurement that does not hold anything back.
	hold *atomic.Bool
	gate chan struct{}
	// drained is closed when the pump goroutine has returned, which is how a
	// caller knows the one goroutine this wrapper adds is gone. The goroutine
	// delta is counted after it, so a wrapper cannot be mistaken for a leak.
	drained chan struct{}
	once    sync.Once
}

func (c *countingConsumer) Messages() <-chan driver.InboundMessage { return c.out }

// pump copies deliveries from the driver onto the channel the core reads, which
// is the only place the harness can reach the Settle of a delivery it did not
// create. It stamps each one with the instant it hands it over, which is where
// that delivery's settle latency starts: the port is the last place the harness
// stands before the core takes the message over.
//
// It holds deliveries back while the run's gate is shut, which is how a
// measurement publishes a corpus before it opens the window it measures. The
// stamp is taken after that wait, so the interval a settle latency describes
// excludes the wait the harness imposed on it.
//
// The stamp is taken before the send below, so the interval starts when this
// pump offers the delivery to the core and not when the core takes it: the
// unbuffered send's own wait is inside the interval.
//
// It stops when the driver closes its channel, and also when Stop or Release
// has returned, so a core that has stopped reading cannot leave this goroutine
// blocked on a send that will never be taken.
func (c *countingConsumer) pump() {
	defer close(c.drained)
	defer close(c.out)
	source := c.Consumer.Messages()
	for {
		select {
		case message, ok := <-source:
			if !ok {
				return
			}
			if !c.released() {
				return
			}
			if message.Settle != nil {
				message.Settle = countingSettler{
					inner:       message.Settle,
					tr:          c.tr,
					body:        string(message.Body),
					deliveredAt: c.tr.clk.Now(),
				}
			}
			select {
			case c.out <- message:
			case <-c.stopped:
				return
			}
		case <-c.stopped:
			return
		}
	}
}

// released blocks while the run holds its deliveries back and reports whether
// the pump should keep going. A run that holds nothing back passes straight
// through, and a teardown releases a parked pump rather than waiting for a
// window that may never open.
func (c *countingConsumer) released() bool {
	if c.hold == nil || !c.hold.Load() {
		return true
	}
	select {
	case <-c.gate:
		return true
	case <-c.stopped:
		return false
	}
}

// Stop stops the wrapped consumer and waits for the pump goroutine to return.
// The wait is what makes the goroutines this port was asked to stop count as
// stopped: the wrapper's own goroutine would otherwise be free to outlive the
// call, and every later count of the process's goroutines would carry it.
func (c *countingConsumer) Stop(ctx context.Context) error {
	err := c.Consumer.Stop(ctx)
	c.once.Do(func() { close(c.stopped) })
	<-c.drained
	return err
}

// Release releases the wrapped consumer and waits for the pump goroutine to
// return, for the reason Stop does.
func (c *countingConsumer) Release(ctx context.Context) error {
	err := c.Consumer.Release(ctx)
	c.once.Do(func() { close(c.stopped) })
	<-c.drained
	return err
}

// countingSettler records a settlement when Ack returns nil, and records the
// settle latency beside it: the time from the pump offering this delivery to
// the core to the ack call itself.
//
// The latency is taken at the call and not at its return, so the interval is
// everything the delivery spent on its way through the core - the handover, the
// queue, the handler and the decision - and the ack's own cost is not in it.
// Nack is forwarded untouched: a delivery the broker was told to take back was
// not settled, and counting it would report a rate the settlement path never
// reached.
type countingSettler struct {
	inner driver.Settler
	tr    *tracker
	body  string
	// deliveredAt is when the pump offered this delivery to the core.
	deliveredAt time.Time
}

func (s countingSettler) Ack(ctx context.Context) error {
	latency := s.tr.clk.Since(s.deliveredAt)
	err := s.inner.Ack(ctx)
	if err == nil {
		s.tr.noteSettled(s.body, latency)
	}
	return err
}

func (s countingSettler) Nack(ctx context.Context, options driver.NackOptions) error {
	return s.inner.Nack(ctx, options)
}
