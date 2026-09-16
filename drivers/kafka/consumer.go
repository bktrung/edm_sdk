package kafka

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

type pauseReason string

const (
	pauseReasonDeferred   pauseReason = "deferred"
	pauseReasonHold       pauseReason = "hold-full"
	pauseReasonPrefetch   pauseReason = "prefetch-full"
	pauseReasonAckGap     pauseReason = "ack-gap"
	pauseReasonUserPaused pauseReason = "user-paused"
)

type pauseReasonSet map[pauseReason]struct{}

func (s pauseReasonSet) add(reason pauseReason) bool {
	if s == nil {
		return true
	}
	if _, exists := s[reason]; exists {
		return false
	}
	s[reason] = struct{}{}
	return len(s) == 1
}

func (s pauseReasonSet) remove(reason pauseReason) bool {
	if len(s) == 0 {
		return false
	}
	if _, exists := s[reason]; !exists {
		return false
	}
	delete(s, reason)
	return len(s) == 0
}

func (s pauseReasonSet) empty() bool { return len(s) == 0 }

// holding reports whether reason is about records the driver is already holding
// rather than a condition of the destination that has to clear. A holding
// reason never keeps a record whose due time has arrived from being delivered:
// the records it names are the ones that become deliverable.
func (r pauseReason) holding() bool {
	return r == pauseReasonDeferred || r == pauseReasonHold
}

// holdsFetches reports whether reason keeps franz-go from fetching the
// destination. A deferred reason does not: the records behind the one that is
// waiting for its due time still have to be reachable, so the fetches are held
// by pauseReasonHold instead, at the count that bounds them.
func (r pauseReason) holdsFetches() bool {
	return r != pauseReasonDeferred
}

// holdsFetches reports whether any reason in the set keeps franz-go from
// fetching the destination.
func (s pauseReasonSet) holdsFetches() bool {
	for reason := range s {
		if reason.holdsFetches() {
			return true
		}
	}
	return false
}

func (s pauseReasonSet) permitsRedelivery() bool {
	// A deferred reason remains a correctness gate for a requeued record:
	// allowing that record through would deliver it before its due time.
	for reason := range s {
		if reason != pauseReasonPrefetch && reason != pauseReasonAckGap {
			return false
		}
	}
	return true
}

type partitionKey struct {
	destination string
	partition   int32
}
type leaveRequest struct {
	group      string
	instanceID string
	static     bool
}

type leaveFunc func(context.Context, leaveRequest) error

type consumer struct {
	conn             *conn
	client           *kgo.Client
	cfg              driver.ConsumerConfig
	group            string
	instanceID       string
	staticMembership bool
	synthesizedGroup bool
	destinations     []string
	budgets          map[string]int
	messages         chan driver.InboundMessage
	errors           chan error
	errorsMu         sync.Mutex
	errorsClosed     bool
	pollCtx          context.Context
	cancelPoll       context.CancelFunc
	pollDone         chan struct{}
	stopDone         chan struct{}
	pauseReasons     map[string]pauseReasonSet
	// unsettled counts every delivery admitted to Messages until its settler
	// reaches a terminal state. A revoked settler releases its slot when its
	// settlement completes, whether that settlement committed before the revoke
	// or was refused as revoked after it; a draining revoked settler remains
	// counted until its caller reaches a terminal state, so draining still
	// observes every outstanding delivery.
	unsettled map[string]int
	// outstanding counts, per partition, the deliveries this consumer emitted
	// for it and has not released. It carries the hold rule: at most one
	// delivery per partition is outstanding, so the admission gate refuses a
	// partition's next record while its count is non-zero. It follows the
	// destination slot's charge and release points exactly, which is what keeps
	// a requeue's redelivery holding its partition until it settles.
	outstanding map[partitionKey]int
	// readAheadPaused holds the partitions whose fetches this driver is holding
	// because the poll loop already carries the partition's read-ahead limit in
	// pending, and pendingHeld counts, per partition, what pending held when
	// that was last reconciled. A settle reads that count to tell whether the
	// partition it just freed still has a record waiting for it.
	readAheadPaused map[partitionKey]struct{}
	pendingHeld     map[partitionKey]int
	settlers        map[*settler]struct{}
	trackers        map[partitionKey]*ackTracker
	// owned holds the partitions this consumer currently owns. The rebalance
	// callbacks are its only writers and admission reads it under c.mu, so a
	// revoke takes effect over the records the poll loop is already holding: a
	// record whose partition is not owned here is dropped, not delivered. A
	// settlement is refused by the revoked tracker rather than by ownership,
	// because the revoke lets a delivery settle between marking the partition
	// not owned and revoking its tracker: that window is the wait it runs, and
	// a partition that comes back to this member settles through the ownership
	// it comes back under.
	owned map[partitionKey]bool
	// inHand holds the records the poll loop has taken from franz-go and not
	// yet resolved. A fresh tracker for a partition takes its base from the
	// lowest offset here rather than from the record that created it, because
	// delivery follows due times and not log order: a record published behind
	// a nearer due time is delivered after it, so the record that creates the
	// tracker can be a higher offset than one still waiting in hand, and a base
	// taken from it would report the waiting record as already settled.
	inHand                map[*kgo.Record]struct{}
	settlerCh             chan struct{}
	rebalanceDrainTimeout time.Duration
	requeued              map[partitionKey]int
	discarded             map[partitionKey]map[int64]struct{}
	reportedDeferrals     map[string]struct{}
	maxAckGap             int64
	laneFaultDestination  string
	clock                 clock.Clock
	// offsetMu serializes CommitOffsetsSync with SetOffsets because franz-go
	// forbids those operations from running concurrently.
	offsetMu sync.Mutex
	// assignmentMu serializes a rebalance callback against an in-run partition
	// rewind. A revoke holds it while it decides the partition's fate, and
	// resetOffset takes it before it asks whether the tracker it was handed is
	// still the partition's, so a requeue cannot move a cursor across the
	// moment its partition changes hands.
	assignmentMu sync.Mutex
	// deliveryMu closes the gap between admission and the Messages send, so a
	// teardown cannot close the channel under a sender.
	deliveryMu sync.Mutex
	leaveMu    sync.Mutex
	mu         sync.Mutex
	draining   bool
	stopped    bool
	// pollCancel interrupts the poll wait in flight and pollWakePending holds a
	// wake that arrived while no wait was running. The loop keeps records
	// admission refused in its own pending queue, and franz-go never offers
	// those again, so the settlement that frees the slot they are waiting for
	// has to reach the loop through this pair rather than through the broker.
	pollCancel      context.CancelFunc
	pollWakePending bool
	leaveRequested  bool
	leaveErr        error
	leaveCtx        context.Context
	leaveCancel     context.CancelFunc
	leaveFn         leaveFunc
	// leaveStatic leaves a static classic group by InstanceID and leaveDynamic
	// leaves through the owned franz-go client. Both are installed during
	// construction so tests execute the production membership selector in
	// leaveGroup against injected operations. leaveFn remains the whole-loop
	// seam used to prove bounded cancellation.
	leaveStatic       func(ctx context.Context, group, instanceID string) error
	leaveDynamic      func(ctx context.Context) error
	leaveClosed       bool
	leaveStarted      bool
	leaveRequestC     chan struct{}
	leaveStopC        chan struct{}
	leaveFinished     chan struct{}
	leaveFinishOnce   sync.Once
	leaveLoopDone     chan struct{}
	forwarderStopC    chan struct{}
	forwarderStopOnce sync.Once
}

type settler struct {
	owner  *consumer
	record *kgo.Record
	// tracker is the partition's tracker at the moment this delivery was
	// admitted. A revoke drops that tracker, and holding the pointer is what
	// makes a settlement that arrives after the partition moved fail with the
	// classified revoked error instead of committing. Guarded by owner.mu, like
	// tracker.
	tracker *ackTracker
	// drainingRevoked marks a delivery whose partition was revoked while this
	// consumer was draining. Nothing can redeliver it by then, so its requeue
	// completes without an error rather than reporting a revocation the caller
	// can do nothing about. Guarded by owner.mu, like tracker.
	drainingRevoked bool
	requeued        bool
	slotReleased    bool
	key             partitionKey
	mu              sync.Mutex
	settled         bool
}

// completeRevocation ends a delivery whose partition this consumer no longer
// owns: its settlement has been refused, so the delivery completes on the
// caller's side and releases the charge it holds on the partition.
func (s *settler) completeRevocation() {
	s.owner.mu.Lock()
	_, retained := s.owner.settlers[s]
	s.owner.mu.Unlock()
	if retained {
		s.owner.completeSettlement(s, false)
	}
}

// Ack commits the next offset after the contiguous acknowledged prefix and
// releases this delivery's destination prefetch slot. An out-of-order
// acknowledgement waits for every lower offset before committing.
func (s *settler) Ack(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("ack", driver.KindTransient, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.owner.mu.Lock()
	if s.settled {
		s.owner.mu.Unlock()
		return classify("ack", driver.KindFatal, driver.ErrAlreadySettled)
	}
	tracker := s.owner.settlementTrackerLocked(s)
	s.owner.mu.Unlock()
	if tracker == nil {
		s.completeRevocation()
		return classify("ack", driver.KindFatal, ErrRevoked)
	}

	if err := tracker.Ack(s.record.Offset, func(commitPoint int64) error {
		return s.owner.commitOffset(ctx, s.key, commitPoint)
	}); err != nil {
		if errors.Is(err, ErrRevoked) {
			s.completeRevocation()
		} else if !errors.Is(err, errAckTrackerAlreadySettled) {
			s.owner.refreshAckGap(s.record.Topic)
		}
		return classifySettlement("ack", err)
	}
	s.owner.completeSettlement(s, false)
	return nil
}

// Nack either commits and discards a record or rewinds its active partition
// cursor for an in-run redelivery. Requeue does not advance the tracker base.
// CountAsFailure is a no-op because classic Kafka groups expose no delivery count.
func (s *settler) Nack(ctx context.Context, options driver.NackOptions) error {
	if err := ctx.Err(); err != nil {
		return classify("nack", driver.KindTransient, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.owner.mu.Lock()
	if s.settled {
		s.owner.mu.Unlock()
		return classify("nack", driver.KindFatal, driver.ErrAlreadySettled)
	}
	tracker := s.owner.settlementTrackerLocked(s)
	drainingRevoked := s.drainingRevoked && s.owner.draining
	s.owner.mu.Unlock()
	if tracker == nil {
		if drainingRevoked && options.Requeue {
			s.owner.completeSettlement(s, false)
			return nil
		}
		s.completeRevocation()
		return classify("nack", driver.KindFatal, ErrRevoked)
	}

	if options.Requeue {
		s.requeued = true
		s.owner.mu.Lock()
		if err := tracker.Release(s.record.Offset); err != nil {
			s.owner.mu.Unlock()
			if errors.Is(err, ErrRevoked) {
				s.completeRevocation()
			}
			return classifySettlement("nack", err)
		}
		s.owner.requeued[s.key]++
		s.owner.mu.Unlock()
		resetOffset := s.record.Offset
		if lowest, ok := tracker.lowestRequeue(); ok {
			resetOffset = lowest
		}
		if !s.owner.resetOffset(s.key, tracker, resetOffset) {
			if drainingRevoked {
				s.owner.completeSettlement(s, false)
				return nil
			}
			// The partition moved to another consumer while this requeue was
			// in flight, so this consumer cannot rewind it. The new owner's
			// own fetch of the uncommitted offset is the redelivery the caller
			// asked for.
			s.completeRevocation()
			return classifySettlement("nack", ErrRevoked)
		}
		s.owner.resumeForRedelivery(s.record.Topic)
		s.owner.mu.Lock()
		draining := s.owner.draining
		s.owner.mu.Unlock()
		s.owner.completeSettlement(s, !draining)
		return nil
	}

	if err := tracker.Ack(s.record.Offset, func(commitPoint int64) error {
		return s.owner.commitOffset(ctx, s.key, commitPoint)
	}); err != nil {
		if errors.Is(err, ErrRevoked) {
			s.completeRevocation()
		} else if !errors.Is(err, errAckTrackerAlreadySettled) {
			s.owner.refreshAckGap(s.record.Topic)
		}
		return classifySettlement("nack", err)
	}
	s.owner.noteDiscarded(s.key, tracker, s.record.Offset)
	slog.Default().Warn(
		"discarding Kafka record",
		"topic", s.record.Topic,
		"partition", s.record.Partition,
		"offset", s.record.Offset,
	)
	s.owner.completeSettlement(s, false)
	return nil
}

// settlementTrackerLocked returns the tracker a settlement of s commits
// through, or nil when this consumer has to refuse it. The caller must hold
// c.mu.
//
// A delivery is admitted under the tracker of the ownership in force, and a
// revoke detaches and revokes that tracker, so a settlement arriving after the
// revoke fails. One case is not a partition that moved away: the lane balancer
// is eager, so every membership change revokes every partition and reassigns
// the ones this member keeps, and a delivery the caller still holds then
// outlives the ownership it was made under while its partition never left this
// consumer. The partition is owned here again, so the live settlement path is
// the tracker of the current ownership. An ownership that has not admitted a
// record yet has no tracker, and the delivery's own offset is where that
// tracker starts, which is what builds one; the offset a settlement commits is
// the base the new ownership owes, so a commit through it cannot pass a record
// this consumer still has to deliver.
func (c *consumer) settlementTrackerLocked(s *settler) *ackTracker {
	tracker := c.trackers[s.key]
	if tracker != nil && tracker == s.tracker {
		return tracker
	}
	if !c.owned[s.key] {
		return s.tracker
	}
	if tracker == nil {
		tracker = c.trackerForLocked(s.record)
	}
	return tracker
}

func classifySettlement(operation string, err error) error {
	switch {
	case errors.Is(err, ErrRevoked):
		return classify(operation, driver.KindFatal, ErrRevoked)
	case errors.Is(err, errAckTrackerAlreadySettled):
		return classify(operation, driver.KindFatal, driver.ErrAlreadySettled)
	default:
		return classify(operation, kafkaErrorKind(err), err)
	}
}

var (
	errExclusiveConsumer = errors.New("exclusive consumer conflict")

	_ driver.Consumer = (*consumer)(nil)
	_ driver.Settler  = (*settler)(nil)
)

func newConsumer(ctx context.Context, connection *conn, cfg driver.ConsumerConfig) (*consumer, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("consumer", driver.KindTransient, err)
	}
	if err := connection.admissionError("consumer"); err != nil {
		return nil, err
	}
	maxAckGap, err := resolveMaxAckGap(connection.driverOptions)
	if err != nil {
		return nil, classify("consumer", driver.KindFatal, err)
	}
	seen := make(map[string]struct{}, len(cfg.Destinations))
	for _, destination := range cfg.Destinations {
		if _, exists := seen[destination]; exists {
			return nil, classify("consumer", driver.KindFatal, fmt.Errorf("duplicate destination %q", destination))
		}
		seen[destination] = struct{}{}
	}

	group := cfg.Group
	synthesized := false
	if group == "" {
		var err error
		group, err = newConsumerGroup()
		if err != nil {
			return nil, classify("consumer", driver.KindFatal, err)
		}
		synthesized = true
	}
	staticMembership := connection.staticMembership
	if connection.driverOptions != nil {
		staticMembership, err = resolveStaticMembership(connection.driverOptions)
		if err != nil {
			return nil, classify("consumer", driver.KindFatal, err)
		}
	}

	drainTimeout := connection.rebalanceDrainTimeout
	if drainTimeout == 0 {
		drainTimeout = defaultKafkaRebalanceDrainTimeout
	}
	// The revoke wait holds the whole rebalance open, so the pair is refused
	// here, where the bound in force is known, rather than surfacing as a lost
	// assignment on the first rebalance.
	if _, err := resolveRebalanceTimeout(connection.driverOptions, drainTimeout); err != nil {
		return nil, classify("consumer", driver.KindFatal, err)
	}

	pollCtx, cancelPoll := context.WithCancel(context.Background())
	consumer := &consumer{
		conn:                  connection,
		cfg:                   cfg,
		group:                 group,
		instanceID:            connection.instanceID,
		staticMembership:      staticMembership,
		synthesizedGroup:      synthesized,
		destinations:          append([]string(nil), cfg.Destinations...),
		budgets:               make(map[string]int, len(cfg.Destinations)),
		messages:              make(chan driver.InboundMessage, totalPrefetch(cfg)),
		errors:                make(chan error, 8),
		pollCtx:               pollCtx,
		cancelPoll:            cancelPoll,
		leaveCtx:              context.Background(),
		pollDone:              make(chan struct{}),
		stopDone:              make(chan struct{}),
		leaveRequestC:         make(chan struct{}, 1),
		leaveStopC:            make(chan struct{}),
		leaveFinished:         make(chan struct{}),
		leaveLoopDone:         make(chan struct{}),
		forwarderStopC:        make(chan struct{}),
		pauseReasons:          make(map[string]pauseReasonSet, len(cfg.Destinations)),
		unsettled:             make(map[string]int, len(cfg.Destinations)),
		outstanding:           make(map[partitionKey]int),
		readAheadPaused:       make(map[partitionKey]struct{}),
		pendingHeld:           make(map[partitionKey]int),
		settlers:              make(map[*settler]struct{}),
		trackers:              make(map[partitionKey]*ackTracker),
		owned:                 make(map[partitionKey]bool),
		inHand:                make(map[*kgo.Record]struct{}),
		settlerCh:             make(chan struct{}, 1),
		rebalanceDrainTimeout: drainTimeout,
		requeued:              make(map[partitionKey]int),
		discarded:             make(map[partitionKey]map[int64]struct{}),
		reportedDeferrals:     make(map[string]struct{}),
		maxAckGap:             maxAckGap,
		clock:                 clock.NewReal(),
	}
	consumer.leaveFn = consumer.leaveGroup
	for index, destination := range cfg.Destinations {
		consumer.budgets[destination] = destinationPrefetch(cfg, destination, index)
	}

	opts, err := consumerClientOpts(connection, cfg, group, consumer)
	if err != nil {
		cancelPoll()
		return nil, classify("consumer", driver.KindFatal, err)
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		cancelPoll()
		return nil, classify("consumer", driver.KindFatal, err)
	}
	consumer.client = client
	reserved := false
	defer func() {
		if !reserved {
			cancelPoll()
			client.Close()
		}
	}()
	// Install the membership operations before any goroutine can read them:
	// the poll loop starts below, and the leave loop starts on first use.
	consumer.leaveStatic = func(ctx context.Context, group, instanceID string) error {
		responses, err := kadm.NewClient(connection.client).LeaveGroup(ctx, kadm.LeaveGroup(group).InstanceIDs(instanceID))
		if err != nil {
			return err
		}
		return responses.Error()
	}
	consumer.leaveDynamic = func(ctx context.Context) error {
		return consumer.client.LeaveGroupContext(ctx)
	}
	if err := connection.reserveConsumer(consumer); err != nil {
		if errors.Is(err, errConnClosing) {
			return nil, classify("consumer", driver.KindTransient, err)
		}
		return nil, classify("consumer", driver.KindFatal, err)
	}
	reserved = true
	// The poll context is owned by the consumer and canceled by Drain or Stop.
	//nolint:contextcheck // this goroutine uses the consumer-owned cancellation context.
	go consumer.poll(pollCtx)
	return consumer, nil
}

func consumerClientOpts(connection *conn, cfg driver.ConsumerConfig, group string, consumer *consumer) ([]kgo.Opt, error) {
	staticMembership := connection.staticMembership
	if connection.driverOptions != nil {
		if resolved, err := resolveStaticMembership(connection.driverOptions); err == nil {
			staticMembership = resolved
		} else {
			return nil, err
		}
	}
	balancer := connection.balancer
	if balancer == nil {
		// Directly constructed connection facades predate the stored selection;
		// preserve franz-go's cooperative default for those paths. Open always
		// stores the configured balancer before a consumer can be created.
		balancer = kgo.CooperativeStickyBalancer()
	}

	opts := append([]kgo.Opt(nil), connection.clientOpts...)
	opts = append(opts,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(cfg.Destinations...),
		kgo.DisableAutoCommit(),
		consumerStartOffset(cfg.StartAt),
		kgo.Balancers(balancer),
		// Franz-go defaults FetchMaxWait to 5000ms. When a
		// multi-destination consumer settles a message on a destination whose
		// broker partition is currently exhausted, franz-go issues a Fetch
		// request for that destination which the broker holds for up to
		// MaxWaitMillis. While that single-broker request is held, other unpaused
		// destinations on the same broker cannot be fetched, stalling refill
		// deliveries for up to 5 seconds. Bounding FetchMaxWait prevents an
		// exhausted destination from starving ready destinations.
		kgo.FetchMaxWait(50*time.Millisecond),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(func(ctx context.Context, cl *kgo.Client, partitions map[string][]int32) {
			consumer.onPartitionsAssigned(ctx, cl, partitions)
		}),
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, partitions map[string][]int32) {
			consumer.onPartitionsRevoked(ctx, cl, partitions)
		}),
		kgo.OnPartitionsLost(func(ctx context.Context, cl *kgo.Client, partitions map[string][]int32) {
			consumer.onPartitionsLost(ctx, cl, partitions)
		}),
	)
	if staticMembership && connection.instanceID != "" {
		opts = append(opts, kgo.InstanceID(connection.instanceID))
	}
	return opts, nil
}

const defaultKafkaMaxAckGap int64 = 10000

func resolveMaxAckGap(options map[string]string) (int64, error) {
	value, ok := options["kafka.maxAckGap"]
	if !ok {
		return defaultKafkaMaxAckGap, nil
	}
	gap, err := strconv.ParseInt(value, 10, 64)
	if err != nil || gap <= 0 {
		return 0, fmt.Errorf("kafka: invalid maxAckGap %q; must be a positive integer", value)
	}
	return gap, nil
}

// Consume-path knob defaults. They are franz-go's own defaults, passed
// explicitly so an unset key still has a value the driver chose and a test can
// observe.
//
// defaultKafkaSessionTimeout matches the fallback that config validation uses
// to bound lifecycle.rebalanceDrainTimeout. That bound is only meaningful if
// the session timeout the broker enforces is the one validation measured, so
// the two values must move together.
const (
	defaultKafkaFetchMaxBytes         int32 = 50 << 20
	defaultKafkaSessionTimeout              = 45 * time.Second
	defaultKafkaRebalanceTimeout            = 60 * time.Second
	defaultKafkaRebalanceDrainTimeout       = 25 * time.Second
)

// resolveConsumerOptions translates the broker.kafka.* keys that govern the
// consume path into franz-go options. It returns an option for every knob,
// including the ones the operator left unset, for the same reason the producer
// side does: the values in force are the driver's, not a library default.
func resolveConsumerOptions(options map[string]string) ([]kgo.Opt, error) {
	fetchMaxBytes, err := resolveFetchMaxBytes(options)
	if err != nil {
		return nil, err
	}
	sessionTimeout, err := resolveSessionTimeout(options)
	if err != nil {
		return nil, err
	}
	rebalanceTimeout, err := resolveRebalanceTimeoutOption(options)
	if err != nil {
		return nil, err
	}
	return []kgo.Opt{
		kgo.FetchMaxBytes(fetchMaxBytes),
		kgo.SessionTimeout(sessionTimeout),
		kgo.RebalanceTimeout(rebalanceTimeout),
	}, nil
}

// resolveFetchMaxBytes maps kafka.fetchMaxBytes onto the ceiling a consumer
// asks each broker for in a fetch. The wire field is an int32, so the parse is
// bounded at 32 bits and a larger value is refused rather than wrapped into a
// negative ceiling.
func resolveFetchMaxBytes(options map[string]string) (int32, error) {
	value, ok := options["kafka.fetchMaxBytes"]
	if !ok {
		return defaultKafkaFetchMaxBytes, nil
	}
	bytes, err := strconv.ParseInt(value, 10, 32)
	if err != nil || bytes <= 0 {
		return 0, fmt.Errorf("kafka: invalid kafka.fetchMaxBytes %q; must be an integer between 1 and %d", value, math.MaxInt32)
	}
	return int32(bytes), nil
}

// resolveSessionTimeout maps kafka.sessionTimeout onto the group member session
// timeout. The member carries the value in its join request, so it is the
// timeout the coordinator expires the member on rather than a local
// approximation of it.
func resolveSessionTimeout(options map[string]string) (time.Duration, error) {
	value, ok := options["kafka.sessionTimeout"]
	if !ok {
		return defaultKafkaSessionTimeout, nil
	}
	timeout, err := time.ParseDuration(value)
	if err != nil || timeout <= 0 {
		return 0, fmt.Errorf("kafka: invalid kafka.sessionTimeout %q; must be a positive duration", value)
	}
	return timeout, nil
}

// resolveRebalanceTimeout maps kafka.rebalanceTimeout onto the group member
// rebalance timeout: the window the broker allows a member to complete a
// rebalance before removing it from the group, and refuses a timeout that does
// not contain the revoke wait.
//
// drainTimeout is the bound a revoke wait runs under, or zero when the caller
// does not know it. The bound is a lifecycle setting rather than a
// broker.kafka.* key, so the option path resolves the timeout alone and the
// consumer build, which knows the bound in force, is where the pair is refused.
// A rebalance callback that waits for a delivery holds the whole rebalance
// open, and the broker boots a member whose callback outlives this timeout;
// what the drain had not committed then arrives as a loss, not as the duplicate
// at-least-once permits. A timeout at or below the bound is refused because the
// wait is then allowed to outlast the window that has to contain it.
func resolveRebalanceTimeout(options map[string]string, drainTimeout time.Duration) (time.Duration, error) {
	timeout, err := resolveRebalanceTimeoutOption(options)
	if err != nil {
		return 0, err
	}
	if drainTimeout > 0 && timeout <= drainTimeout {
		return 0, fmt.Errorf(
			"kafka: kafka.rebalanceTimeout %s must be above the revoke wait bound %s; a rebalance callback that outlives the timeout loses the assignment",
			timeout, drainTimeout,
		)
	}
	return timeout, nil
}

// resolveRebalanceTimeoutOption parses kafka.rebalanceTimeout against its
// default, with no opinion on the revoke wait: resolveRebalanceTimeout is where
// the pair is judged.
func resolveRebalanceTimeoutOption(options map[string]string) (time.Duration, error) {
	value, ok := options["kafka.rebalanceTimeout"]
	if !ok {
		return defaultKafkaRebalanceTimeout, nil
	}
	timeout, err := time.ParseDuration(value)
	if err != nil || timeout <= 0 {
		return 0, fmt.Errorf("kafka: invalid kafka.rebalanceTimeout %q; must be a positive duration", value)
	}
	return timeout, nil
}

func newConsumerGroup() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("kafka: generate consumer group id: %w", err)
	}
	return "f1-kafka-consumer-" + hex.EncodeToString(raw[:]), nil
}

func consumerStartOffset(start driver.StartPosition) kgo.ConsumerOpt {
	if start == driver.StartLatest {
		return kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd())
	}
	return kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())
}

func totalPrefetch(cfg driver.ConsumerConfig) int {
	total := 0
	for index, destination := range cfg.Destinations {
		total += destinationPrefetch(cfg, destination, index)
	}
	if total < 1 {
		return 1
	}
	return total
}

func destinationPrefetch(cfg driver.ConsumerConfig, destination string, index int) int {
	if value := cfg.PerDestination[destination]; value > 0 {
		return value
	}
	if cfg.Prefetch > 0 && len(cfg.Destinations) > 0 {
		base := cfg.Prefetch / len(cfg.Destinations)
		if index < cfg.Prefetch%len(cfg.Destinations) {
			base++
		}
		if base > 0 {
			return base
		}
	}
	return 1
}

func (c *consumer) Messages() <-chan driver.InboundMessage { return c.messages }
func (c *consumer) Errors() <-chan error                   { return c.errors }

func (c *consumer) requestLeaveLocked() {
	if c.leaveClosed {
		return
	}
	if !c.leaveStarted {
		timeout := c.rebalanceDrainTimeout
		if timeout <= 0 {
			timeout = defaultKafkaRebalanceDrainTimeout
		}
		c.leaveCtx, c.leaveCancel = context.WithTimeout(context.Background(), timeout)
		c.leaveStarted = true
		go c.leaveLoop()
	}
	select {
	case c.leaveRequestC <- struct{}{}:
	default:
	}
}

func (c *consumer) requestLeave() {
	c.leaveMu.Lock()
	c.requestLeaveLocked()
	c.leaveMu.Unlock()
}

// requestLeaveAndWait asks the leave loop to leave the group and returns the
// leave's error alone. A caller whose decision also depends on whether the
// leave finished, rather than on the wait being abandoned, uses waitForLeave.
func (c *consumer) requestLeaveAndWait(ctx context.Context) error {
	_, err := c.waitForLeave(ctx)
	return err
}

// waitForLeave asks the leave loop to leave the group and waits for the
// outcome. It reports whether the leave is over: true means leaveErr carries
// the outcome the loop reached, which may be nil, or that the consumer had
// already been torn down, which reports nil; false means ctx ended first, the
// loop is still running, and a later call with a fresh context observes that
// same outcome.
//
// The two returns cannot be told apart by the error. The leave runs on its own
// timeout, so a leave that finished can fail with a context error of its own;
// and finishLeave closes leaveFinished once and keeps leaveErr, so the outcome
// the loop reached is the only one any later wait will see.
func (c *consumer) waitForLeave(ctx context.Context) (finished bool, leaveErr error) {
	c.leaveMu.Lock()
	c.mu.Lock()
	if c.leaveClosed {
		c.mu.Unlock()
		c.leaveMu.Unlock()
		return true, nil
	}
	c.leaveRequested = true
	c.requestLeaveLocked()
	c.mu.Unlock()
	c.leaveMu.Unlock()
	select {
	case <-c.leaveFinished:
	case <-ctx.Done():
		// The context ending means this caller gave up only if the leave really
		// is still in flight. Both cases are ready whenever the leave finished
		// around the moment ctx ended, and select then picks between ready
		// cases at random, so the finished outcome is re-checked here rather
		// than left to that coin flip: it is the outcome the caller has to act
		// on, and taking the context instead would skip the teardown a finished
		// leave runs.
		select {
		case <-c.leaveFinished:
		default:
			return false, ctx.Err()
		}
	}
	c.mu.Lock()
	leaveErr = c.leaveErr
	c.mu.Unlock()
	return true, leaveErr
}

func (c *consumer) finishLeave(err error) {
	c.mu.Lock()
	c.leaveErr = err
	c.mu.Unlock()
	c.leaveFinishOnce.Do(func() {
		close(c.leaveFinished)
	})
	if err != nil {
		c.sendError(classify("consumer.leave", kafkaErrorKind(err), err))
	}
}

func (c *consumer) leaveGroup(ctx context.Context, request leaveRequest) error {
	// The membership selector lives in production: forcing the static
	// condition false routes every member through the dynamic operation,
	// which the leave-selection test observes.
	var err error
	if request.static && request.instanceID != "" {
		err = c.leaveStatic(ctx, request.group, request.instanceID)
	} else {
		err = c.leaveDynamic(ctx)
	}
	// UNKNOWN_MEMBER_ID means the coordinator does not know this member, which
	// is the state a leave exists to reach: the member was expired, or the
	// shutdown landed before its join completed, as a rolling deploy does. It
	// is reported as a clean leave so it does not surface as a failure. Every
	// other code keeps its error, including FENCED_INSTANCE_ID, where the
	// member is still in the group and the caller has to know.
	if errors.Is(err, kerr.UnknownMemberID) {
		return nil
	}
	return err
}

func (c *consumer) leaveLoop() {
	defer func() {
		if c.leaveCancel != nil {
			c.leaveCancel()
		}
		close(c.leaveLoopDone)
	}()
	select {
	case <-c.leaveRequestC:
		request := leaveRequest{
			group:      c.group,
			instanceID: c.instanceID,
			static:     c.staticMembership,
		}
		c.finishLeave(c.leaveFn(c.leaveCtx, request))
	case <-c.leaveStopC:
		c.finishLeave(nil)
	}
}

// claimPollWake registers cancel as the interrupt for the poll wait about to
// begin and reports whether a wake is already pending. It takes c.mu itself, so
// the caller must not hold it.
//
// The pending flush and this registration are two steps, so a wake that lands
// between them would otherwise be lost: the loop would block on a broker that
// has nothing left to send, still holding the records the wake was meant to
// release.
func (c *consumer) claimPollWake(cancel context.CancelFunc) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pollWakePending {
		c.pollWakePending = false
		return true
	}
	c.pollCancel = cancel
	return false
}

// releasePollWake forgets the interrupt registered by claimPollWake. A wake
// that races this call cancels an already finished wait, which is harmless.
func (c *consumer) releasePollWake() {
	c.mu.Lock()
	c.pollCancel = nil
	c.mu.Unlock()
}

// wakePollLocked interrupts the poll wait so the loop admits again over the
// records it is holding. It never blocks: a wake that finds no wait in progress
// records itself for the next one. The caller must hold c.mu.
func (c *consumer) wakePollLocked() {
	c.pollWakePending = true
	if c.pollCancel != nil {
		c.pollCancel()
	}
}

func (c *consumer) poll(ctx context.Context) {
	defer close(c.pollDone)
	defer c.client.AllowRebalance()
	pending := make([]*kgo.Record, 0, 1)
	for {
		if !c.flushPending(&pending) {
			return
		}
		c.syncDeferredPauses(pending)
		c.syncReadAheadPauses(pending)

		fetchCtx := ctx
		bounded := false
		var cancelFetch context.CancelFunc
		if due, ok := c.pendingDeadline(pending); ok {
			fetchCtx, cancelFetch = context.WithDeadline(ctx, due)
			bounded = true
		} else {
			fetchCtx, cancelFetch = context.WithCancel(ctx)
		}
		if c.claimPollWake(cancelFetch) {
			cancelFetch()
			continue
		}
		// The whole response is taken, not one record of it. Whatever is left
		// buffered in the client is what the next prefetch pause throws away:
		// franz-go strips a paused topic's buffered records and rewinds its
		// cursor, so the read-ahead is fetched and buffered again, once per
		// pause. Draining each fetch into pending leaves the pause nothing to
		// strip, and pending is bounded by the records one response carried,
		// because while a destination is at budget for the prefetch reason no
		// further fetch for it is issued.
		fetches := c.client.PollRecords(fetchCtx, 0)
		// A wake ends the wait with a synthetic cancellation fetch. It reports
		// no broker fault; it reports that admission has to run over pending
		// again, which is what a bounded poll's own timeout also asks for.
		woken := fetchCtx.Err() != nil && ctx.Err() == nil
		cancelFetch()
		c.releasePollWake()
		if ctx.Err() != nil || fetches.IsClientClosed() {
			c.client.AllowRebalance()
			return
		}
		if !c.handleFetches(&pending, fetches, bounded || woken) {
			return
		}
	}
}

func (c *consumer) handleFetches(pending *[]*kgo.Record, fetches kgo.Fetches, bounded bool) bool {
	c.mu.Lock()
	if c.inHand == nil {
		c.inHand = make(map[*kgo.Record]struct{})
	}
	fetches.EachRecord(c.tagRecordLocked)
	c.mu.Unlock()
	defer c.client.AllowRebalance()

	for _, fetchErr := range fetches.Errors() {
		if bounded && (errors.Is(fetchErr.Err, context.DeadlineExceeded) || errors.Is(fetchErr.Err, context.Canceled)) {
			continue
		}
		kind := kafkaErrorKind(fetchErr.Err)
		if kind != driver.KindTransient {
			// A non-retryable fetch error cannot recover by polling again.
			// Reclassify it as fatal for this subscription while retaining
			// the Kafka cause.
			kind = driver.KindFatal
		}
		c.sendError(classify("consumer", kind, fmt.Errorf("kafka fetch %s[%d]: %w", fetchErr.Topic, fetchErr.Partition, fetchErr.Err)))
		if kind == driver.KindFatal {
			return false
		}
	}
	for record := range fetches.RecordsAll() {
		if c.isRecordStale(record) {
			c.discardStaleRecord(record)
			continue
		}
		if !c.canDeliver(record) {
			*pending = append(*pending, record)
			continue
		}
		delivered, active := c.emit(record)
		if !active {
			return false
		}
		if !delivered {
			*pending = append(*pending, record)
		}
	}
	return true
}

func (c *consumer) flushPending(pending *[]*kgo.Record) bool {
	for len(*pending) > 0 {
		records := *pending
		index := -1
		for i, record := range records {
			if c.isRecordStale(record) {
				copy(records[i:], records[i+1:])
				records[len(records)-1] = nil
				*pending = records[:len(records)-1]
				c.discardStaleRecord(record)
				index = -2
				break
			}
			if c.canDeliver(record) {
				index = i
				break
			}
		}
		if index == -2 {
			continue
		}
		if index < 0 {
			return true
		}
		record := records[index]
		copy(records[index:], records[index+1:])
		records[len(records)-1] = nil
		*pending = records[:len(records)-1]
		delivered, active := c.emit(record)
		if !active {
			return false
		}
		if !delivered {
			*pending = append(*pending, record)
		}
	}
	return true
}

func (c *consumer) canDeliver(record *kgo.Record) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped || c.draining {
		return false
	}
	if c.laneFaultDestination == record.Topic {
		return false
	}
	if c.isRecordStaleLocked(record) {
		return false
	}
	return c.admissionLocked(record)
}

// isRecordStale reports whether a record the loop is holding is one this
// consumer must drop rather than deliver: it belongs to a partition this
// consumer does not own. The poll loop asks before it admits a record and
// again before it keeps one, so a revoke empties the pending list of the
// partitions it took away on the loop's next pass, and the broker's own
// redelivery to the new owner is the copy that remains.
func (c *consumer) isRecordStale(record *kgo.Record) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isRecordStaleLocked(record)
}

// tagRecord records a record the poll loop has taken from franz-go and not yet
// resolved. A fresh tracker for the record's partition takes its base from the
// lowest offset in hand, which is why the loop tags every record of a fetch
// response before it admits any of them.
func (c *consumer) tagRecord(record *kgo.Record) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tagRecordLocked(record)
}

func (c *consumer) tagRecordLocked(record *kgo.Record) {
	if c.inHand == nil {
		c.inHand = make(map[*kgo.Record]struct{})
	}
	c.inHand[record] = struct{}{}
}

func (c *consumer) isRecordStaleLocked(record *kgo.Record) bool {
	return !c.owned[partitionKey{destination: record.Topic, partition: record.Partition}]
}

func (c *consumer) discardStaleRecord(record *kgo.Record) {
	c.mu.Lock()
	delete(c.inHand, record)
	c.mu.Unlock()
}

// releaseSlotsLocked returns up to slots destination slots that unsettled deliveries were
// charged, and clears the pause that stopped fetching for the destination when the return takes
// it from full to not-full.
//
// The caller must hold c.mu.
func (c *consumer) releaseSlotsLocked(destination string, slots int) {
	charged := c.unsettled[destination]
	if slots > charged {
		slots = charged
	}
	if slots > 0 {
		c.unsettled[destination] = charged - slots
	}
	budget := c.budgets[destination]
	if budget > 0 && charged-slots < budget {
		// The destination is not full any more, so the pause that stopped
		// fetching has to go.
		if c.client != nil {
			c.setPauseReasonLocked(destination, pauseReasonPrefetch, false)
		}
	}
}

// chargeDeliveryLocked records that key's partition has one more delivery
// outstanding. It is called once per emitted delivery, at the destination
// slot's own charge: a redelivery a requeue admitted re-enters emission as a
// reuse and is not charged again, because the slot its predecessor charged is
// still held.
//
// A consumer built as a literal by a test has no counter and no charged
// delivery, which is what a nil map means here. The caller must hold c.mu.
func (c *consumer) chargeDeliveryLocked(key partitionKey) {
	if c.outstanding == nil {
		c.outstanding = make(map[partitionKey]int)
	}
	c.outstanding[key]++
}

// releasePartitionLocked returns up to slots of one partition's outstanding
// deliveries, and wakes the poll loop when that takes the count to zero.
//
// The wake is the hold rule's turnaround path. A record admission refused stays
// in the loop's pending list and franz-go never offers it again, so without the
// wake the partition would wait for the next broker response, up to the fetch
// wait, for a delivery its own settle has just made possible. The wake is
// recorded in the same critical section that takes the count to zero, under
// c.mu, so a wake that races the poll loop's registration is not lost; that is
// the pattern claimPollWake uses for the flush race.
//
// The wake is unconditional rather than gated on a snapshot of what is waiting
// on the partition: the loop's read-ahead hold is recomputed once per iteration,
// after that iteration's admission pass, so a settle landing in between clears
// the charge while the mirror still reads zero, and the gate would drop exactly
// the wake the window needed. The loop already re-runs admission on every wake,
// so the extra iteration is the harmless direction to be wrong in.
//
// The charge and the release are the destination slot's own, so a second
// release is already refused by slotReleased and the clamp here only keeps the
// two counters in step. The caller must hold c.mu.
func (c *consumer) releasePartitionLocked(key partitionKey, slots int) {
	charged := c.outstanding[key]
	if slots > charged {
		slots = charged
	}
	if slots == 0 {
		return
	}
	remaining := charged - slots
	if remaining == 0 {
		delete(c.outstanding, key)
		c.wakePollLocked()
		return
	}
	c.outstanding[key] = remaining
}

func (c *consumer) emit(record *kgo.Record) (delivered, active bool) {
	c.deliveryMu.Lock()
	defer c.deliveryMu.Unlock()
	c.mu.Lock()
	if c.stopped || c.draining {
		c.mu.Unlock()
		return false, false
	}
	if c.isRecordStaleLocked(record) {
		c.mu.Unlock()
		return false, true
	}
	if !c.admissionLocked(record) {
		c.mu.Unlock()
		return false, true
	}
	budget := c.budgets[record.Topic]
	if budget <= 0 {
		budget = 1
	}
	key := partitionKey{destination: record.Topic, partition: record.Partition}
	tracker := c.trackerForLocked(record)
	reused, deliver, err := tracker.TrackRedelivery(record.Offset)
	if err != nil {
		c.mu.Unlock()
		return false, true
	}
	if !deliver {
		delete(c.inHand, record)
		c.mu.Unlock()
		return true, true
	}
	if reused {
		c.requeued[key]--
		if c.requeued[key] == 0 {
			delete(c.requeued, key)
		}
		c.pauseAfterRedeliveryLocked(record.Topic)
	}
	settler := &settler{
		owner: c,
		record: &kgo.Record{
			Topic:       record.Topic,
			Partition:   record.Partition,
			Offset:      record.Offset,
			LeaderEpoch: record.LeaderEpoch,
		},
		tracker: tracker,
		key:     key,
	}
	c.settlers[settler] = struct{}{}
	delete(c.inHand, record)
	if !reused {
		c.unsettled[record.Topic]++
		c.chargeDeliveryLocked(key)
	}
	if c.unsettled[record.Topic] >= budget {
		c.setPauseReasonLocked(record.Topic, pauseReasonPrefetch, true)
	}
	message := inboundMessage(record, settler)
	c.mu.Unlock()
	select {
	case <-c.forwarderStopC:
		c.abortSettler(settler, reused)
		return false, false
	case c.messages <- message:
		return true, true
	}
}

func (c *consumer) stopForwarders() {
	c.forwarderStopOnce.Do(func() {
		close(c.forwarderStopC)
	})
}

func (c *consumer) abortSettler(settler *settler, reused bool) {
	c.mu.Lock()
	delete(c.settlers, settler)
	if !reused {
		c.releaseSlotsLocked(settler.record.Topic, 1)
		c.releasePartitionLocked(settler.key, 1)
	}
	c.signalSettlerDoneLocked()
	shouldLeave := c.draining && len(c.settlers) == 0 && !c.leaveRequested
	if shouldLeave {
		c.leaveRequested = true
	}
	c.mu.Unlock()
	if shouldLeave {
		c.requestLeave()
	}
}

func (c *consumer) detachAllTrackersLocked() []*ackTracker {
	trackers := make([]*ackTracker, 0, len(c.trackers))
	for key, tracker := range c.trackers {
		trackers = append(trackers, tracker)
		delete(c.trackers, key)
	}
	for settler := range c.settlers {
		delete(c.settlers, settler)
	}
	clear(c.requeued)
	clear(c.discarded)
	clear(c.unsettled)
	clear(c.outstanding)
	clear(c.readAheadPaused)
	clear(c.pendingHeld)
	clear(c.inHand)
	clear(c.owned)
	return trackers
}

// trackerForLocked returns the tracker of the record's partition, building one
// when the partition has none. A partition has one tracker for as long as this
// consumer owns it: a revoke detaches the tracker, so the partition's next
// assignment builds a fresh one instead of inheriting the commit point of an
// ownership that ended.
func (c *consumer) trackerForLocked(record *kgo.Record) *ackTracker {
	key := partitionKey{destination: record.Topic, partition: record.Partition}
	if tracker := c.trackers[key]; tracker != nil {
		return tracker
	}
	if c.trackers == nil {
		c.trackers = make(map[partitionKey]*ackTracker)
	}
	tracker := newAckTracker(c.trackerBaseLocked(key, record.Offset))
	c.trackers[key] = tracker
	return tracker
}

// trackerBaseLocked returns the offset a fresh tracker for key must start at.
// Offsets below the base count as settled, so the base is the lowest offset on
// the partition this consumer still owes a delivery for.
//
// The first record delivered is not that offset, because delivery follows due
// times rather than log order: a record published ahead of a nearer due time is
// delivered after it, so the lowest offset the consumer holds can be the last
// one delivered. Starting the tracker at the delivered record's own offset
// would then report the held record as already settled and drop it, losing the
// offset and letting the commit pass over it. The records the loop has in hand
// for this partition, which is every record whose delivery is still owed, are
// what the base is taken from instead.
func (c *consumer) trackerBaseLocked(key partitionKey, base int64) int64 {
	for record := range c.inHand {
		if record.Topic != key.destination || record.Partition != key.partition || record.Offset >= base {
			continue
		}
		base = record.Offset
	}
	return base
}

func (c *consumer) completeSettlement(settler *settler, preserveUnsettled bool) {
	c.mu.Lock()
	if preserveUnsettled && (settler.tracker == nil || c.trackers[settler.key] != settler.tracker) {
		preserveUnsettled = false
	}
	settler.settled = true
	if tracker := settler.tracker; tracker != nil && c.trackers[settler.key] == tracker {
		c.reconcileDiscardedLocked(settler.key, tracker)
	}
	if _, exists := c.settlers[settler]; !exists {
		if !preserveUnsettled && !settler.slotReleased {
			c.releaseSlotsLocked(settler.record.Topic, 1)
			c.releasePartitionLocked(settler.key, 1)
			settler.slotReleased = true
		}
		c.refreshAckGapLocked(settler.record.Topic)
		c.signalSettlerDoneLocked()
		c.mu.Unlock()
		return
	}
	delete(c.settlers, settler)
	if !preserveUnsettled && !settler.slotReleased {
		c.releaseSlotsLocked(settler.record.Topic, 1)
		c.releasePartitionLocked(settler.key, 1)
	}
	budget := c.budgets[settler.record.Topic]
	if budget > 0 && (preserveUnsettled || c.unsettled[settler.record.Topic] < budget) {
		c.setPauseReasonLocked(settler.record.Topic, pauseReasonPrefetch, false)
	}
	c.refreshAckGapLocked(settler.record.Topic)
	c.signalSettlerDoneLocked()
	shouldLeave := c.draining && len(c.settlers) == 0 && !c.leaveRequested
	if shouldLeave {
		c.leaveRequested = true
	}
	c.mu.Unlock()
	if shouldLeave {
		c.requestLeave()
	}
}

func (c *consumer) noteDiscarded(key partitionKey, tracker *ackTracker, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.trackers[key] != tracker || tracker.CommitPoint() > offset {
		return
	}
	if c.discarded == nil {
		c.discarded = make(map[partitionKey]map[int64]struct{})
	}
	if c.discarded[key] == nil {
		c.discarded[key] = make(map[int64]struct{})
	}
	c.discarded[key][offset] = struct{}{}
	c.reconcileDiscardedLocked(key, tracker)
}

func (c *consumer) hasPendingRequeueLocked(destination string) bool {
	for key, count := range c.requeued {
		if key.destination == destination && count > 0 {
			return true
		}
	}
	return false
}

func (c *consumer) reconcileDiscardedLocked(key partitionKey, tracker *ackTracker) {
	if tracker == nil || len(c.discarded[key]) == 0 {
		return
	}
	base := tracker.CommitPoint()
	for offset := range c.discarded[key] {
		if offset < base {
			delete(c.discarded[key], offset)
		}
	}
	if len(c.discarded[key]) == 0 {
		delete(c.discarded, key)
	}
}

func (c *consumer) refreshAckGap(destination string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refreshAckGapLocked(destination)
}

func (c *consumer) refreshAckGapLocked(destination string) {
	exceeded := false
	for key, tracker := range c.trackers {
		if key.destination == destination && tracker.Gap() > c.maxAckGap {
			exceeded = true
			break
		}
	}
	c.setPauseReasonLocked(destination, pauseReasonAckGap, exceeded)
}

func (c *consumer) commitOffset(ctx context.Context, key partitionKey, commitPoint int64) error {
	c.offsetMu.Lock()
	defer c.offsetMu.Unlock()
	var commitErr error
	c.client.CommitOffsetsSync(ctx, map[string]map[int32]kgo.EpochOffset{
		key.destination: {
			key.partition: {Epoch: -1, Offset: commitPoint},
		},
	}, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, response *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			commitErr = err
			return
		}
		for _, topic := range response.Topics {
			for _, partition := range topic.Partitions {
				if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
					commitErr = err
					return
				}
			}
		}
	})
	return commitErr
}

func (c *consumer) resetOffset(key partitionKey, tracker *ackTracker, offset int64) bool {
	c.assignmentMu.Lock()
	defer c.assignmentMu.Unlock()
	c.mu.Lock()
	current := c.trackers[key] == tracker
	c.mu.Unlock()
	if !current {
		return false
	}
	c.offsetMu.Lock()
	defer c.offsetMu.Unlock()
	c.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		key.destination: {
			key.partition: {Epoch: -1, Offset: offset},
		},
	})
	return true
}

func (c *consumer) resumeForRedelivery(destination string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumeForRedeliveryLocked(destination)
}

func (c *consumer) resumeForRedeliveryLocked(destination string) {
	if c.draining || c.stopped || !c.hasPendingRequeueLocked(destination) {
		return
	}
	reasons := c.pauseReasons[destination]
	if !reasons.empty() && reasons.permitsRedelivery() {
		c.client.ResumeFetchTopics(destination)
	}
}

func (c *consumer) pauseAfterRedeliveryLocked(destination string) {
	if c.draining || c.stopped || c.hasPendingRequeueLocked(destination) {
		return
	}
	reasons := c.pauseReasons[destination]
	if !reasons.empty() && reasons.permitsRedelivery() {
		c.client.PauseFetchTopics(destination)
	}
}

func inboundMessage(record *kgo.Record, settler *settler) driver.InboundMessage {
	headers := make([]driver.Header, 0, len(record.Headers))
	for _, header := range record.Headers {
		if header.Key == delayUntilHeader {
			continue
		}
		headers = append(headers, driver.Header{Key: header.Key, Value: append([]byte(nil), header.Value...)})
	}
	receivedAt := kafkaNow()
	return driver.InboundMessage{
		Destination:   record.Topic,
		Key:           append([]byte(nil), record.Key...),
		Headers:       headers,
		Body:          append([]byte(nil), record.Value...),
		DeliveryCount: -1,
		ReceivedAt:    receivedAt,
		Ref: driver.BrokerRef{
			Partition: record.Partition,
			Offset:    record.Offset,
			Raw:       fmt.Sprintf("%s/%d/%d", record.Topic, record.Partition, record.Offset),
		},
		Settle: settler,
	}
}

func (c *consumer) Pause(destinations ...string) error {
	return c.setUserPaused(destinations, true)
}

func (c *consumer) Resume(destinations ...string) error {
	return c.setUserPaused(destinations, false)
}

func (c *consumer) setUserPaused(destinations []string, paused bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return classify("consumer", driver.KindFatal, errors.New("consumer stopped"))
	}
	if c.draining {
		return classify("consumer", driver.KindFatal, errors.New("consumer draining"))
	}
	if len(destinations) == 0 {
		destinations = c.destinations
	}
	for _, destination := range destinations {
		if _, ok := c.budgets[destination]; !ok {
			return classify("consumer", driver.KindNotFound, driver.ErrDestinationMissing)
		}
		laneFaulted := !paused && c.laneFaultDestination == destination
		if laneFaulted {
			c.laneFaultDestination = ""
		}
		c.setPauseReasonLocked(destination, pauseReasonUserPaused, paused)
		if laneFaulted && c.pauseReasons[destination].empty() && !c.draining && !c.stopped {
			c.client.ResumeFetchTopics(destination)
		}
		if !paused {
			c.resumeForRedeliveryLocked(destination)
		}
	}
	return nil
}

func (c *consumer) setPauseReasonLocked(destination string, reason pauseReason, add bool) {
	reasons := c.pauseReasons[destination]
	if reasons == nil {
		reasons = make(pauseReasonSet)
		c.pauseReasons[destination] = reasons
	}
	if _, present := reasons[reason]; present == add {
		return
	}
	paused := reasons.holdsFetches()
	if add {
		reasons.add(reason)
	} else {
		reasons.remove(reason)
	}
	switch {
	case !paused && reasons.holdsFetches():
		c.client.PauseFetchTopics(destination)
	case !add && !reasons.holdsFetches() && !c.draining && !c.stopped:
		// A removal that leaves the fetches unheld releases them, whether or
		// not the set ever held them. The deferred reason is the only reason
		// that does not hold the fetches itself: it is the gate that refuses
		// a requeued record before its due time, so a record the poll loop
		// took out of franz-go and could not admit is waiting for exactly
		// this removal, and resuming fetch does not reach it.
		//
		// Ending the wait is part of the release. The poll that follows a
		// release with nothing waiting has no deadline, so a broker with
		// nothing left to hand over produces no other wake and the record is
		// never admitted again.
		//
		// The resume is unconditional because fetching can also be paused on
		// this destination from outside this bookkeeping, which a lane fault
		// does; resuming a destination that is not paused is a no-op in
		// franz-go.
		c.client.ResumeFetchTopics(destination)
		c.wakePollLocked()
	}
}

func (c *consumer) Drain(ctx context.Context) error {
	_, err := c.drain(ctx)
	return err
}

// drain is Drain's body. It reports whether the leave it waited on finished, so
// its caller can tell a leave that reached an outcome from a wait this caller's
// context abandoned; Drain itself discards that and returns the error alone. A
// false return with a nil error means no leave was waited on at all.
func (c *consumer) drain(ctx context.Context) (leaveFinished bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, classify("drain", driver.KindTransient, err)
	}
	c.stopForwarders()
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return false, nil
	}
	if !c.draining {
		c.draining = true
		c.cancelPoll()
		// PollRecords can race with franz's background fetchers; pause source
		// topics before waiting so Drain cannot accumulate more broker records.
		c.client.PauseFetchTopics(c.destinations...)
	}
	pollDone := c.pollDone
	c.mu.Unlock()
	if err := c.waitPoll(ctx, "drain", pollDone); err != nil {
		return false, err
	}
	c.mu.Lock()
	leaveNow := len(c.settlers) == 0 && !c.leaveRequested
	c.mu.Unlock()
	if leaveNow {
		finished, leaveErr := c.waitForLeave(ctx)
		if leaveErr != nil {
			return finished, classify("drain", kafkaErrorKind(leaveErr), leaveErr)
		}
		return finished, nil
	}
	return false, nil
}

func (c *consumer) waitPoll(ctx context.Context, operation string, pollDone <-chan struct{}) error {
	select {
	case <-pollDone:
		return nil
	case <-ctx.Done():
		return classify(operation, driver.KindTransient, ctx.Err())
	}
}

func (c *consumer) Stop(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return stopContextError(err)
	}
	// A drain that met a finished leave does not end the stop here, whether that
	// leave succeeded or not: the consumer is torn down below and the error is
	// returned after it, the same rule Release follows. The checks below still
	// apply, so an outstanding settler still refuses the stop with the consumer
	// registered, and a concurrent Stop or Release that tears down first still
	// makes this call return nil. The early return stays for every other drain
	// failure, including a leave the caller's context abandoned, because nothing
	// has finished there and the consumer has to stay registered for a later
	// Stop or Release to complete the leave.
	drainFinished, drainErr := c.drain(ctx)
	if drainErr != nil && !drainFinished {
		if errors.Is(drainErr, context.DeadlineExceeded) && !errors.Is(drainErr, driver.ErrDrainTimeout) {
			return classify("stop", driver.KindTransient, fmt.Errorf("%w: %w", driver.ErrDrainTimeout, drainErr))
		}
		return drainErr
	}

	c.mu.Lock()
	if c.stopped {
		stopDone := c.stopDone
		c.mu.Unlock()
		select {
		case <-stopDone:
			return nil
		case <-ctx.Done():
			return stopContextError(ctx.Err())
		}
	}
	if len(c.settlers) != 0 {
		count := len(c.settlers)
		c.mu.Unlock()
		return classify("stop", driver.KindFatal, fmt.Errorf("%w: %d outstanding messages", driver.ErrResourcesOutstanding, count))
	}
	c.mu.Unlock()

	// Same rule as Release: a finished leave ends the consumer whether or not it
	// succeeded. The broker expires the member on its session timeout anyway, so
	// staying registered buys no membership and strands the connection, because
	// conn.Close refuses while any consumer is registered and finishLeave keeps
	// leaveErr for every later wait. The error is not swallowed: finishLeave
	// already sent it to Errors(), and Stop returns it below, unless another
	// caller had already stopped the consumer, whose completed stop returns nil.
	// Only a context that ended while the leave was still in flight returns here,
	// and that consumer stays registered so a later call completes the leave.
	leaveFinished, leaveErr := c.waitForLeave(ctx)
	if !leaveFinished {
		return stopContextError(leaveErr)
	}

	c.mu.Lock()
	if c.stopped {
		stopDone := c.stopDone
		c.mu.Unlock()
		select {
		case <-stopDone:
			return nil
		case <-ctx.Done():
			return stopContextError(ctx.Err())
		}
	}
	c.stopped = true
	c.mu.Unlock()

	c.closeTeardown(ctx)
	if leaveErr != nil {
		return stopContextError(leaveErr)
	}
	return nil
}

func (c *consumer) closeTeardown(ctx context.Context) {
	if c.client != nil {
		c.client.AllowRebalance()
	}
	c.leaveMu.Lock()
	leaveStarted := false
	if !c.leaveClosed {
		c.leaveClosed = true
		leaveStarted = c.leaveStarted
		if leaveStarted {
			if c.leaveCancel != nil {
				c.leaveCancel()
			}
			close(c.leaveStopC)
		}
	}
	c.leaveMu.Unlock()
	if leaveStarted {
		select {
		case <-c.leaveLoopDone:
		case <-ctx.Done():
			if c.client != nil {
				c.client.Close()
			}
		}
	}
	if c.client != nil {
		c.client.Close()
	}
	c.conn.removeConsumer(c)
	if c.synthesizedGroup {
		if err := c.deleteGroup(ctx); err != nil {
			c.sendError(err)
		}
	}
	c.errorsMu.Lock()
	if !c.errorsClosed {
		c.errorsClosed = true
		close(c.errors)
	}
	c.errorsMu.Unlock()
	c.deliveryMu.Lock()
	close(c.messages)
	c.deliveryMu.Unlock()
	close(c.stopDone)
}

func stopContextError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return classify("stop", driver.KindTransient, fmt.Errorf("%w: %w", driver.ErrDrainTimeout, err))
	}
	return classify("stop", driver.KindTransient, err)
}

func (c *consumer) deleteGroup(ctx context.Context) error {
	response, err := kadm.NewClient(c.conn.client).DeleteGroup(ctx, c.group)
	if err != nil {
		if errors.Is(err, kerr.GroupIDNotFound) {
			return nil
		}
		return classify("consumer.delete_group", kafkaErrorKind(err), err)
	}
	if response.Err != nil && !errors.Is(response.Err, kerr.GroupIDNotFound) {
		return classify("consumer.delete_group", kafkaErrorKind(response.Err), response.Err)
	}
	return nil
}

func (c *consumer) Release(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("release", driver.KindTransient, err)
	}
	c.stopForwarders()

	c.mu.Lock()
	if c.stopped {
		stopDone := c.stopDone
		c.mu.Unlock()
		select {
		case <-stopDone:
			return nil
		case <-ctx.Done():
			return classify("release", driver.KindTransient, ctx.Err())
		}
	}
	if !c.draining {
		c.draining = true
		c.cancelPoll()
		c.client.PauseFetchTopics(c.destinations...)
	}
	pollDone := c.pollDone
	c.mu.Unlock()

	if err := c.waitPoll(ctx, "release", pollDone); err != nil {
		return err
	}
	c.assignmentMu.Lock()
	c.mu.Lock()
	trackers := c.detachAllTrackersLocked()
	c.mu.Unlock()
	c.assignmentMu.Unlock()

	for _, tracker := range trackers {
		tracker.Drop()
	}

	// The wait has two returns and they mean different things: the leave
	// finished, with whatever error it carries, or this caller's context ended
	// while the leave was still in flight. Only the second leaves the consumer
	// with work still pending, so only the second keeps it registered.
	leaveFinished, leaveErr := c.waitForLeave(ctx)
	if !leaveFinished {
		return classify("release", kafkaErrorKind(leaveErr), leaveErr)
	}
	// A finished leave ends the consumer whether or not it succeeded. The
	// broker expires the member on its session timeout anyway, so holding the
	// registration buys no membership and strands the connection: conn.Close
	// refuses while any consumer is registered, and no retry can clear this,
	// because finishLeave keeps leaveErr for every later wait. The error is not
	// swallowed: finishLeave already sent it to Errors(), and this call returns
	// it below, unless another caller had already stopped the consumer, whose
	// completed stop returns nil.

	c.mu.Lock()
	if c.stopped {
		stopDone := c.stopDone
		c.mu.Unlock()
		select {
		case <-stopDone:
			return nil
		case <-ctx.Done():
			return classify("release", driver.KindTransient, ctx.Err())
		}
	}
	c.stopped = true
	c.mu.Unlock()

	c.closeTeardown(ctx)
	if leaveErr != nil {
		return classify("release", kafkaErrorKind(leaveErr), leaveErr)
	}
	return nil
}

func (c *consumer) Lag(ctx context.Context) (map[string]int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("lag", driver.KindTransient, err)
	}
	if !c.effectiveCapabilities().LagQueryable {
		return nil, classify("lag", driver.KindFatal, driver.ErrUnsupported)
	}
	c.mu.Lock()
	destinations := append([]string(nil), c.destinations...)
	group := c.group
	c.mu.Unlock()

	admin := kadm.NewClient(c.conn.client)
	starts, err := listKafkaOffsets(ctx, func(listCtx context.Context) (kadm.ListedOffsets, error) {
		return admin.ListStartOffsets(listCtx, destinations...)
	})
	if err != nil {
		return nil, classifyLagError(err)
	}
	ends, err := listKafkaOffsets(ctx, func(listCtx context.Context) (kadm.ListedOffsets, error) {
		return admin.ListEndOffsets(listCtx, destinations...)
	})
	if err != nil {
		return nil, classifyLagError(err)
	}
	committed, err := admin.FetchOffsets(ctx, group)
	if err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
		return nil, classify("lag", kafkaErrorKind(err), err)
	}

	lag := make(map[string]int64, len(destinations))
	for _, destination := range destinations {
		partitions, ok := ends[destination]
		if !ok {
			return nil, classify("lag", driver.KindNotFound, fmt.Errorf("destination %q is missing: %w", destination, driver.ErrDestinationMissing))
		}
		var total int64
		for partition, end := range partitions {
			start, ok := starts.Lookup(destination, partition)
			if !ok {
				return nil, classify("lag", driver.KindNotFound, fmt.Errorf("destination %q partition %d has no start offset", destination, partition))
			}
			committedOffset := start.Offset
			if response, exists := committed.Lookup(destination, partition); exists {
				if response.Err != nil {
					return nil, classifyKafkaOffsetError("lag", response.Err)
				}
				if response.At >= 0 {
					committedOffset = response.At
				}
			}
			if end.Offset > committedOffset {
				total += end.Offset - committedOffset
			}
		}
		lag[destination] = total
	}
	return lag, nil
}

func classifyLagError(err error) error {
	return classifyKafkaOffsetError("lag", err)
}

func classifyKafkaOffsetError(operation string, err error) error {
	kind := kafkaErrorKind(err)
	if kind == driver.KindNotFound {
		err = errors.Join(driver.ErrDestinationMissing, err)
	}
	return classify(operation, kind, err)
}

func listKafkaOffsets(ctx context.Context, list func(context.Context) (kadm.ListedOffsets, error)) (kadm.ListedOffsets, error) {
	retryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		offsets, err := list(retryCtx)
		if err == nil {
			err = offsets.Error()
		}
		if err == nil || !errors.Is(err, kerr.UnknownTopicOrPartition) {
			return offsets, err
		}
		timer := time.NewTimer(50 * time.Millisecond) //nolint:forbidigo // Kafka metadata propagation is broker-driven and requires a wall-clock retry
		select {
		case <-retryCtx.Done():
			timer.Stop()
			if ctx.Err() != nil {
				return offsets, ctx.Err()
			}
			return offsets, err
		case <-timer.C:
		}
	}
}

func (c *consumer) effectiveCapabilities() driver.Capabilities {
	if c.cfg.Effective == (driver.Capabilities{}) {
		return c.conn.caps
	}
	return c.cfg.Effective
}

func (c *conn) reserveConsumer(csm *consumer) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.closed {
		return errConnClosing
	}
	if c.consumers == nil {
		c.consumers = make(map[*consumer]struct{})
	}
	// The conflict check and registry insertion stay under one lock. If they
	// were separate, two constructors could both inspect the old registry
	// before either one inserted its exclusive claim.
	for active := range c.consumers {
		if !csm.cfg.Exclusive && !active.cfg.Exclusive {
			continue
		}
		for _, destination := range csm.destinations {
			for _, activeDestination := range active.destinations {
				if destination == activeDestination {
					return fmt.Errorf("%w on destination %q", errExclusiveConsumer, destination)
				}
			}
		}
	}
	c.consumers[csm] = struct{}{}
	return nil
}

func (c *conn) removeConsumer(consumer *consumer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.consumers, consumer)
}

func (c *consumer) sendError(err error) {
	if err == nil {
		return
	}
	kind, classified := driver.Classify(err)
	c.errorsMu.Lock()
	defer c.errorsMu.Unlock()
	if c.errorsClosed {
		return
	}
	if classified && (kind == driver.KindFatal || kind == driver.KindNotFound || kind == driver.KindTooLarge || kind == driver.KindPermission) {
		select {
		case c.errors <- err:
			return
		default:
		}
		select {
		case <-c.errors:
		default:
		}
		select {
		case c.errors <- err:
		default:
		}
		return
	}
	select {
	case c.errors <- err:
	default:
	}
}

func (c *consumer) sendRebalanceError(event string, partitions map[string][]int32) {
	if len(partitions) == 0 {
		return
	}
	c.mu.Lock()
	intentionalClose := c.draining || c.stopped
	c.mu.Unlock()
	if intentionalClose {
		return
	}
	c.sendError(classify("consumer", driver.KindNotification, fmt.Errorf("kafka partitions %s: %v", event, partitions)))
}

func (c *consumer) signalSettlerDoneLocked() {
	if c.settlerCh != nil {
		select {
		case c.settlerCh <- struct{}{}:
		default:
		}
	}
}

// waitForSettlers waits until no settler of keys is outstanding, or until the
// bound expires. It is a condition wait and not a poll: signalSettlerDoneLocked
// runs in the same critical section that takes a revoked partition's charge to
// zero, so the delivery that settles during the window wakes this wait.
func (c *consumer) waitForSettlers(keys map[partitionKey]struct{}, timeout time.Duration) {
	timer := c.clock.Timer(timeout)
	defer timer.Stop()
	for {
		c.mu.Lock()
		has := c.hasSettlersForLocked(keys)
		c.mu.Unlock()
		if !has {
			return
		}
		select {
		case <-c.settlerCh:
		case <-timer.C:
			return
		}
	}
}

func (c *consumer) hasSettlersForLocked(keys map[partitionKey]struct{}) bool {
	for s := range c.settlers {
		if _, ok := keys[s.key]; ok {
			return true
		}
	}
	return false
}

// revokedAssignment is one partition a rebalance callback took away, carrying
// the tracker that ownership ran under so the callback can commit what that
// ownership settled once the wait is over.
type revokedAssignment struct {
	key     partitionKey
	tracker *ackTracker
}

// dropTracker marks one partition not owned and detaches its tracker, which it
// returns. A partition this consumer does not own is one admission drops
// pending records for and one whose deliveries fail their settlement, so
// revocation wins over a commit; detaching the tracker is what makes both true
// without tombstoning the deliveries themselves.
//
// A delivery the revoke finds still in Messages has never reached its caller,
// so the settlement its charge waits for can never arrive: it is dropped and
// its charge released, because the partition's next owner has to be able to
// admit its first record and nothing is left here that could settle it. A
// delivery the caller already holds keeps its charge, which is the settlement
// the revoke wait waits for.
//
// A draining consumer detaches the deliveries too: nothing will redeliver them
// by then, so a requeue of one completes without an error instead of reporting
// a revocation the caller can do nothing about.
//
// It takes deliveryMu and then c.mu, and a caller that must keep a whole
// multi-partition revoke indivisible takes assignmentMu around it. A revoke
// that is not indivisible is still safe: the partitions are marked one at a
// time and an admission in between lands on a partition whose revocation is
// about to be applied to it.
func (c *consumer) dropTracker(destination string, partition int32) *ackTracker {
	c.deliveryMu.Lock()
	defer c.deliveryMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	key := partitionKey{destination: destination, partition: partition}
	delete(c.owned, key)
	tracker := c.trackers[key]
	delete(c.trackers, key)
	buffered := c.discardBufferedLocked(key)
	for _, settler := range buffered {
		delete(c.settlers, settler)
		c.releaseSlotsLocked(destination, 1)
		c.releasePartitionLocked(key, 1)
	}
	if len(buffered) > 0 {
		c.signalSettlerDoneLocked()
	}
	if c.draining {
		for settler := range c.settlers {
			if settler.key != key {
				continue
			}
			settler.drainingRevoked = true
			settler.tracker = nil
		}
	}
	return tracker
}

// discardBufferedLocked takes the deliveries of key that are still in Messages,
// which are the ones the caller has not read, and returns them. Every other
// message is put back in the order it was drained, so one partition's revoke
// does not reorder another's deliveries. The caller holds deliveryMu, so no
// admission is between a delivery's charge and its send, and c.mu.
func (c *consumer) discardBufferedLocked(key partitionKey) []*settler {
	buffered := make([]*settler, 0, 1)
	var kept []driver.InboundMessage
drain:
	for {
		select {
		case message, ok := <-c.messages:
			if !ok {
				break drain
			}
			settler, ok := message.Settle.(*settler)
			if ok && settler.key == key {
				buffered = append(buffered, settler)
				continue
			}
			kept = append(kept, message)
		default:
			break drain
		}
	}
	// The channel cannot be full: it held every drained message a moment ago
	// and the dropped ones are the difference.
	for _, message := range kept {
		c.messages <- message
	}
	return buffered
}

// unown marks every partition of a rebalance callback not owned, one call to
// dropTracker per partition. The caller must hold assignmentMu and must not
// hold c.mu.
func (c *consumer) unown(partitions map[string][]int32) []revokedAssignment {
	revoked := make([]revokedAssignment, 0, len(partitions))
	for topic, partitionList := range partitions {
		for _, partition := range partitionList {
			key := partitionKey{destination: topic, partition: partition}
			revoked = append(revoked, revokedAssignment{key: key, tracker: c.dropTracker(topic, partition)})
		}
	}
	return revoked
}

// revokeTrackers tombstones the trackers of a revoke, so a settlement that
// arrives after the wait fails with the classified revoked error and commits
// nothing.
func (c *consumer) revokeTrackers(revoked []revokedAssignment) {
	for _, assignment := range revoked {
		if assignment.tracker != nil {
			assignment.tracker.Drop()
		}
	}
}

// commitSettledPrefix commits what each revoked ownership had settled. The
// offset a revoked tracker advanced to is the first record this consumer did not
// deliver, so the partition's next owner starts there and redelivers whatever
// the revoke did not reach. A commit that fails is reported on Errors() and
// changes nothing else: the next owner then redelivers from the offset the
// broker does have committed.
func (c *consumer) commitSettledPrefix(ctx context.Context, revoked []revokedAssignment) {
	for _, assignment := range revoked {
		if assignment.tracker == nil {
			continue
		}
		if err := c.commitOffset(ctx, assignment.key, assignment.tracker.CommitPoint()); err != nil {
			c.sendError(classify("consumer.revoke", kafkaErrorKind(err), err))
		}
	}
}

// settleBound is the bound a revoke wait runs under.
func (c *consumer) settleBound() time.Duration {
	if c.rebalanceDrainTimeout > 0 {
		return c.rebalanceDrainTimeout
	}
	return defaultKafkaRebalanceDrainTimeout
}

// onPartitionsAssigned marks the partitions this consumer now owns and resumes
// their fetches, which the revoke that preceded them paused. Ownership is the
// one fact the assignment and the revoke write, and a fresh assignment starts
// the partition's tracker over: the tracker of the ownership that ended is not
// this one's to inherit.
func (c *consumer) onPartitionsAssigned(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
	if len(partitions) == 0 {
		return
	}
	c.assignmentMu.Lock()
	c.mu.Lock()
	if c.owned == nil {
		c.owned = make(map[partitionKey]bool)
	}
	for topic, partitionList := range partitions {
		for _, partition := range partitionList {
			key := partitionKey{destination: topic, partition: partition}
			c.owned[key] = true
			// The pause on a partition is otherwise the read-ahead hold in
			// deferral.go, and franz-go is being resumed here, so the hold must
			// not still claim this driver is holding the partition.
			delete(c.readAheadPaused, key)
		}
	}
	c.mu.Unlock()
	c.assignmentMu.Unlock()
	if c.client != nil {
		c.client.ResumeFetchPartitions(partitions)
	}
	c.sendRebalanceError("assigned", partitions)
}

// onPartitionsRevoked pauses the revoked partitions, marks them not owned, waits
// under one deadline for the deliveries they still hold, marks their trackers
// revoked and commits the settled prefix. A delivery the deadline does not reach
// is left to the partition's next owner, which redelivers it: a duplicate, which
// is the direction at-least-once permits. A loss is not, which is what the wait
// and the commit are here to prevent.
func (c *consumer) onPartitionsRevoked(ctx context.Context, _ *kgo.Client, partitions map[string][]int32) {
	if len(partitions) == 0 {
		return
	}
	// The fetches stop first, so no new record of a revoked partition is
	// fetched while the deliveries in hand drain.
	if c.client != nil {
		c.client.PauseFetchPartitions(partitions)
	}
	c.assignmentMu.Lock()
	revoked := c.unown(partitions)
	c.mu.Lock()
	draining := c.draining
	requestLeave := draining && !c.leaveRequested
	if requestLeave {
		c.leaveRequested = true
	}
	c.mu.Unlock()
	c.assignmentMu.Unlock()
	c.sendRebalanceError("revoked", partitions)
	if draining {
		// A draining consumer commits nothing more, so the revoked trackers are
		// tombstoned without the wait: the deliveries still in hand are the
		// caller's to abandon.
		c.revokeTrackers(revoked)
		if requestLeave {
			c.requestLeave()
		}
		return
	}
	keys := make(map[partitionKey]struct{}, len(revoked))
	for _, assignment := range revoked {
		keys[assignment.key] = struct{}{}
	}
	c.waitForSettlers(keys, c.settleBound())
	c.revokeTrackers(revoked)
	c.commitSettledPrefix(ctx, revoked)
}

// onPartitionsLost marks the lost partitions not owned, drops their pending
// records and tombstones their trackers, then returns. It does not wait and does
// not commit: a lost assignment means the coordinator can no longer accept this
// member's commits, which is why franz-go's own default here is a no-op. What
// this consumer did not commit is redelivered by the partition's next owner.
func (c *consumer) onPartitionsLost(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
	if len(partitions) == 0 {
		return
	}
	c.assignmentMu.Lock()
	revoked := c.unown(partitions)
	c.mu.Lock()
	draining := c.draining
	requestLeave := draining && !c.leaveRequested
	if requestLeave {
		c.leaveRequested = true
	}
	c.mu.Unlock()
	c.assignmentMu.Unlock()
	c.revokeTrackers(revoked)
	if requestLeave {
		c.requestLeave()
	}
	c.sendRebalanceError("lost", partitions)
}
