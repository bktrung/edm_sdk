package kafka

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
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

type consumerHandoff struct {
	source  *consumer
	message driver.InboundMessage
}

type transferReservation struct {
	key    partitionKey
	offset int64
}

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
	// reaches a terminal state. An ordinary revoked settler releases its slot
	// when ownership transfers to the queued handoff; a draining revoked
	// settler remains counted until its caller reaches a terminal state, so
	// draining still observes every outstanding delivery.
	unsettled             map[string]int
	settlers              map[*settler]struct{}
	trackers              map[partitionKey]*ackTracker
	trackerGenerations    map[partitionKey]uint64
	assignmentGenerations map[partitionKey]uint64
	activeGenerations     map[partitionKey]uint64
	fenced                map[partitionKey]bool
	recordGenerations     map[*kgo.Record]uint64
	// tenures outlives an assignment generation. A cooperative rebalance that
	// keeps a partition on the same member bumps the assignment generation
	// without ending that member's ownership, so records already fetched must
	// stay deliverable; a revocation ends the tenure instead, which refuses
	// the records fetched under it. Bumped at revoke, never at assign.
	tenures map[partitionKey]uint64
	// settledTransfers contains exact offsets settled after ownership moved;
	// a higher Kafka offset does not imply that a lower offset was delivered.
	settledTransfers     map[partitionKey]map[int64]struct{}
	pendingTransfers     map[partitionKey]map[int64]*ackTracker
	transferReservations map[partitionKey]map[int64]struct{}
	// selfTransfers lists settlers this consumer tombstoned and handed off
	// whose partition can come back to it. A cooperative rebalance revokes a
	// partition and may re-assign it to the same member across a generation
	// bump; that member is then the new owner and may settle again.
	selfTransfers         map[partitionKey][]*settler
	handoffRecords        map[*kgo.Record]struct{}
	handoffReservations   map[partitionKey]map[int64]struct{}
	handoffMarkers        map[partitionKey]map[int64]struct{}
	handoffs              map[partitionKey]map[int64]driver.InboundMessage
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
	offsetMu     sync.Mutex
	assignmentMu sync.Mutex
	// deliveryMu closes the gap between admission and Messages send so revoke
	// cleanup cannot tombstone a settler before its message is enqueued.
	deliveryMu sync.Mutex
	leaveMu    sync.Mutex
	mu         sync.Mutex
	draining   bool
	stopped    bool
	// leftConnection marks a consumer that has left the connection and holds no
	// queued copies, so requeueHandoff forwards a copy to a registered consumer
	// instead of storing it. moveQueuedHandoffsOnLeave sets it in the same hold
	// that takes the queue, so a copy is either carried by that snapshot or sees
	// the flag. Deliberately not c.stopped, whose meaning is "accepts no new
	// work" and which is set while the consumer is still registered.
	leftConnection bool
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
	owner            *consumer
	record           *kgo.Record
	handoffDelivered bool
	tracker          *ackTracker
	handoff          driver.InboundMessage
	// commitSuppressed marks a delivery another consumer already committed.
	// The broker no longer holds the offset, so the caller's settlement
	// completes successfully instead of reporting a revocation it did not
	// cause. Guarded by owner.mu, like tracker.
	commitSuppressed bool
	// forceRevoked marks a delivery the shed path must not retain: the
	// ownership decision already went against it.
	forceRevoked bool
	// transferSettled marks a retained settler whose offset also travels as a
	// queued handoff, so a commit that wins suppresses the queued copy.
	transferSettled bool
	drainingRevoked bool
	requeued        bool
	slotReleased    bool
	tombstoned      bool
	key             partitionKey
	mu              sync.Mutex
	settled         bool
}

func (s *settler) completeRevocation() {
	if s.handoffDelivered {
		s.owner.clearHandoffMarker(s.key, s.record.Offset)
		s.handoffDelivered = false
	}
	// A tombstoned source is detached from c.settlers immediately so the
	// destination slot can be reused. Its public settlement still completes
	// the settler lifecycle; slotReleased prevents a second production
	// counter decrement.
	s.owner.mu.Lock()
	_, retained := s.owner.settlers[s]
	tombstoned := s.tombstoned
	s.owner.mu.Unlock()
	if retained || tombstoned {
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
	tracker := s.tracker
	suppressed := s.commitSuppressed
	s.owner.mu.Unlock()
	if suppressed {
		// A peer's commit already removed the offset from the broker. The
		// caller's acknowledgement is correct and completes without error.
		s.owner.completeSettlement(s, false)
		return nil
	}
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
	tracker := s.tracker
	suppressed := s.commitSuppressed
	drainingRevoked := s.drainingRevoked && s.owner.draining
	s.owner.mu.Unlock()
	if suppressed {
		// The offset is already committed, so neither a discard nor a
		// requeue can change the broker's state.
		s.owner.completeSettlement(s, false)
		return nil
	}
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
		if s.handoffDelivered {
			s.owner.clearHandoffMarker(s.key, s.record.Offset)
			s.handoffDelivered = false
		}
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
			// The key moved to another consumer, so this consumer cannot
			// rewind it. A queued handoff for the offset is the redelivery
			// path the caller asked for, and it delivers the record once the
			// source slot is released here.
			s.owner.mu.Lock()
			_, queued := s.owner.handoffs[s.key][s.record.Offset]
			s.owner.mu.Unlock()
			if queued {
				s.owner.completeSettlement(s, false)
				go s.owner.redriveHandoffs(s.key)
				return nil
			}
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
		drainTimeout = 25 * time.Second
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
		settlers:              make(map[*settler]struct{}),
		trackers:              make(map[partitionKey]*ackTracker),
		trackerGenerations:    make(map[partitionKey]uint64),
		handoffRecords:        make(map[*kgo.Record]struct{}),
		handoffReservations:   make(map[partitionKey]map[int64]struct{}),
		handoffMarkers:        make(map[partitionKey]map[int64]struct{}),
		assignmentGenerations: make(map[partitionKey]uint64),
		activeGenerations:     make(map[partitionKey]uint64),
		fenced:                make(map[partitionKey]bool),
		recordGenerations:     make(map[*kgo.Record]uint64),
		tenures:               make(map[partitionKey]uint64),
		settledTransfers:      make(map[partitionKey]map[int64]struct{}),
		pendingTransfers:      make(map[partitionKey]map[int64]*ackTracker),
		handoffs:              make(map[partitionKey]map[int64]driver.InboundMessage),
		transferReservations:  make(map[partitionKey]map[int64]struct{}),
		selfTransfers:         make(map[partitionKey][]*settler),
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
		// Franz-go defaults FetchMaxWait to 5000ms for non-share groups. When a
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
	defaultKafkaFetchMaxBytes    int32 = 50 << 20
	defaultKafkaSessionTimeout         = 45 * time.Second
	defaultKafkaRebalanceTimeout       = 60 * time.Second
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
	rebalanceTimeout, err := resolveRebalanceTimeout(options)
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
// rebalance before removing it from the group.
func resolveRebalanceTimeout(options map[string]string) (time.Duration, error) {
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
			timeout = 25 * time.Second
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

func (c *consumer) requestLeaveAndWait(ctx context.Context) error {
	c.leaveMu.Lock()
	c.mu.Lock()
	if c.leaveClosed {
		c.mu.Unlock()
		c.leaveMu.Unlock()
		return nil
	}
	c.leaveRequested = true
	c.requestLeaveLocked()
	c.mu.Unlock()
	c.leaveMu.Unlock()
	select {
	case <-c.leaveFinished:
		c.mu.Lock()
		err := c.leaveErr
		c.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
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
	if request.static && request.instanceID != "" {
		return c.leaveStatic(ctx, request.group, request.instanceID)
	}
	return c.leaveDynamic(ctx)
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

		var fetches kgo.Fetches
		if bounded {
			fetches = c.client.PollRecords(fetchCtx, 0)
		} else {
			fetches = c.client.PollRecords(fetchCtx, 1)
		}
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
	if c.recordGenerations == nil {
		c.recordGenerations = make(map[*kgo.Record]uint64)
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
		delivered, active, _ := c.emit(record)
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
		delivered, active, _ := c.emit(record)
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

func (c *consumer) isRecordStale(record *kgo.Record) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isRecordStaleLocked(record)
}

func (c *consumer) tagRecord(record *kgo.Record) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.recordGenerations == nil {
		c.recordGenerations = make(map[*kgo.Record]uint64)
	}
	c.tagRecordLocked(record)
}

func (c *consumer) tagRecordLocked(record *kgo.Record) {
	key := partitionKey{destination: record.Topic, partition: record.Partition}
	c.recordGenerations[record] = c.tenureLocked(key)
}

// tenureLocked reports the ownership tenure records for key are tagged with.
// A consumer built without assignment callbacks has no tenure map entry, so
// the assignment generation stands in for it.
func (c *consumer) tenureLocked(key partitionKey) uint64 {
	if tenure := c.tenures[key]; tenure != 0 {
		return tenure
	}
	return c.activeGenerations[key]
}

func (c *consumer) isRecordStaleLocked(record *kgo.Record) bool {
	key := partitionKey{destination: record.Topic, partition: record.Partition}
	if offsets := c.settledTransfers[key]; offsets != nil {
		if _, settled := offsets[record.Offset]; settled {
			return true
		}
	}
	if offsets := c.transferReservations[key]; offsets != nil {
		if _, reserved := offsets[record.Offset]; reserved {
			if _, handoff := c.handoffRecords[record]; !handoff {
				return true
			}
		}
	}
	if offsets := c.handoffMarkers[key]; offsets != nil {
		if _, marked := offsets[record.Offset]; marked {
			if _, handoff := c.handoffRecords[record]; !handoff {
				return true
			}
		}
	}
	if c.fenced != nil && c.fenced[key] {
		return true
	}
	activeGen, ok := c.activeGenerations[key]
	if !ok || activeGen == 0 {
		return true
	}
	// Compare against the tenure, not the assignment generation: a record
	// fetched before a cooperative reassignment of a partition this member
	// kept is still this member's to deliver.
	recordGen, ok := c.recordGenerations[record]
	if !ok || recordGen == 0 || recordGen != c.tenureLocked(key) {
		return true
	}
	return false
}

func (c *consumer) discardStaleRecord(record *kgo.Record) {
	c.mu.Lock()
	delete(c.recordGenerations, record)
	c.mu.Unlock()
}

func (c *consumer) markSettledTransfer(key partitionKey, offset int64) {
	if c.conn == nil {
		return
	}
	c.conn.mu.RLock()
	consumers := make([]*consumer, 0, len(c.conn.consumers))
	for consumer := range c.conn.consumers {
		if consumer == c {
			continue
		}
		consumers = append(consumers, consumer)
	}
	c.conn.mu.RUnlock()
	for _, consumer := range consumers {
		consumer.suppressSettledTransfer(key, offset)
	}
}

func (c *consumer) reserveTransfer(key partitionKey, offset int64) {
	if c.conn == nil {
		return
	}
	c.conn.mu.RLock()
	consumers := make([]*consumer, 0, len(c.conn.consumers))
	for consumer := range c.conn.consumers {
		if consumer != c {
			consumers = append(consumers, consumer)
		}
	}
	c.conn.mu.RUnlock()
	for _, consumer := range consumers {
		consumer.reserveTransferOffset(key, offset)
	}
}

func (c *consumer) reserveTransferOffset(key partitionKey, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.transferReservations == nil {
		c.transferReservations = make(map[partitionKey]map[int64]struct{})
	}
	offsets := c.transferReservations[key]
	if offsets == nil {
		offsets = make(map[int64]struct{})
		c.transferReservations[key] = offsets
	}
	offsets[offset] = struct{}{}
}

func (c *consumer) clearTransferReservation(key partitionKey, offset int64) {
	if c.conn == nil {
		return
	}
	c.conn.mu.RLock()
	consumers := make([]*consumer, 0, len(c.conn.consumers))
	for consumer := range c.conn.consumers {
		if consumer != c {
			consumers = append(consumers, consumer)
		}
	}
	c.conn.mu.RUnlock()
	for _, consumer := range consumers {
		consumer.clearTransferReservationOffset(key, offset)
	}
}

func (c *consumer) clearTransferReservationOffset(key partitionKey, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	offsets := c.transferReservations[key]
	delete(offsets, offset)
	if len(offsets) == 0 {
		delete(c.transferReservations, key)
	}
}

// releaseSlotsLocked returns up to slots destination slots that unsettled deliveries were
// charged, and makes queued handoffs dispatchable when the return takes the destination from
// full to not-full. That crossing is the only event that can reopen a full destination: a
// delivery holds its slot from admission until terminal settlement, so nothing else returns it,
// and the offset a queued handoff carries stays filtered from the broker's own redelivery by its
// reservation until a target owns it. Without the trigger a handoff queued while every eligible
// owner was at budget is stranded, never delivered and never settled.
//
// The dispatch is launched on its own goroutine rather than inline, because this runs under c.mu
// while the dispatch path takes the connection lock and then peer consumer locks, and that order
// cannot be taken in reverse from here.
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
		// The destination is not full any more: the pause that stopped fetching has to
		// go before the dispatch, or every eligible target refuses the handoff it is
		// being offered.
		if c.client != nil {
			c.setPauseReasonLocked(destination, pauseReasonPrefetch, false)
		}
	}
	if slots == 0 || budget <= 0 || charged < budget || charged-slots >= budget || c.conn == nil {
		return
	}
	go c.redriveDestinationHandoffs(destination)
}

func (c *consumer) suppressSettledTransfer(key partitionKey, offset int64) {
	c.deliveryMu.Lock()
	defer c.deliveryMu.Unlock()
	c.mu.Lock()
	if c.stopped || c.messages == nil {
		c.mu.Unlock()
		return
	}
	if c.settledTransfers == nil {
		c.settledTransfers = make(map[partitionKey]map[int64]struct{})
	}
	offsets := c.settledTransfers[key]
	if offsets == nil {
		offsets = make(map[int64]struct{})
		c.settledTransfers[key] = offsets
	}
	offsets[offset] = struct{}{}
	reservations := c.transferReservations[key]
	delete(reservations, offset)
	if len(reservations) == 0 {
		delete(c.transferReservations, key)
	}
	// A target settlement also resolves the source's pending commit
	// coverage. The source handoff may already have been taken from its
	// queue, so this independent entry must be cleared here.
	pending := c.pendingTransfers[key]
	delete(pending, offset)
	if len(pending) == 0 {
		delete(c.pendingTransfers, key)
	}
	kept := make([]driver.InboundMessage, 0, len(c.messages))
drain:
	for {
		select {
		case message, ok := <-c.messages:
			if !ok {
				break drain
			}
			settler, ok := message.Settle.(*settler)
			if ok && settler.key == key && settler.record.Offset == offset {
				continue
			}
			kept = append(kept, message)
		default:
			break drain
		}
	}
	var tracker *ackTracker
	reconcile := false
	if current := c.trackers[key]; current != nil {
		tracker = current
	}
	for settler := range c.settlers {
		if settler.key != key || settler.record.Offset != offset {
			continue
		}
		if tracker == nil {
			tracker = settler.tracker
		}
		settler.handoffDelivered = false
		settler.tombstoned = true
		settler.commitSuppressed = true
		delete(c.settlers, settler)
		if !settler.slotReleased {
			c.releaseSlotsLocked(key.destination, 1)
			settler.slotReleased = true
		}
		reconcile = true
	}
	if c.client != nil && c.budgets[key.destination] > 0 && c.unsettled[key.destination] < c.budgets[key.destination] {
		c.setPauseReasonLocked(key.destination, pauseReasonPrefetch, false)
	}
	c.signalSettlerDoneLocked()
	c.mu.Unlock()
	if reconcile && tracker != nil {
		_ = tracker.Ack(offset, nil)
	}
	for _, message := range kept {
		c.messages <- message
	}
}

func (c *consumer) clearHandoffMarker(key partitionKey, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	markers := c.handoffMarkers[key]
	if _, ok := markers[offset]; !ok {
		return
	}
	delete(markers, offset)
	if len(markers) == 0 {
		delete(c.handoffMarkers, key)
	}
}

func (c *consumer) emit(record *kgo.Record) (delivered, active, created bool) {
	c.deliveryMu.Lock()
	defer c.deliveryMu.Unlock()
	c.mu.Lock()
	if c.stopped || c.draining {
		c.mu.Unlock()
		return false, false, false
	}
	if c.isRecordStaleLocked(record) {
		c.mu.Unlock()
		return false, true, false
	}
	if !c.admissionLocked(record) {
		c.mu.Unlock()
		return false, true, false
	}
	budget := c.budgets[record.Topic]
	if budget <= 0 {
		budget = 1
	}
	key := partitionKey{destination: record.Topic, partition: record.Partition}
	tracker := c.trackers[key]
	if tracker == nil || c.trackerGenerations[key] != c.activeGenerations[key] {
		tracker = c.trackerForLocked(record)
	}
	reused, deliver, err := tracker.TrackRedelivery(record.Offset)
	if err != nil {
		c.mu.Unlock()
		return false, true, false
	}
	if !deliver {
		if _, handoff := c.handoffRecords[record]; handoff {
			for settler := range c.settlers {
				if settler.key == key && settler.record.Offset == record.Offset {
					settler.handoffDelivered = true
					break
				}
			}
		}
		c.mu.Unlock()
		return true, true, false
	}
	if reused {
		c.requeued[key]--
		if c.requeued[key] == 0 {
			delete(c.requeued, key)
		}
		c.pauseAfterRedeliveryLocked(record.Topic)
	}
	_, handoffDelivered := c.handoffRecords[record]
	settler := &settler{
		owner: c,
		record: &kgo.Record{
			Topic:       record.Topic,
			Partition:   record.Partition,
			Offset:      record.Offset,
			LeaderEpoch: record.LeaderEpoch,
		},
		tracker:          tracker,
		handoffDelivered: handoffDelivered,
		key:              key,
	}
	c.settlers[settler] = struct{}{}
	delete(c.recordGenerations, record)
	if !reused {
		c.unsettled[record.Topic]++
	}
	if c.unsettled[record.Topic] >= budget {
		c.setPauseReasonLocked(record.Topic, pauseReasonPrefetch, true)
	}
	// No transfer guard is needed here. A record for a fenced or revoked key is
	// already stale before this point (isRecordStaleLocked), and a settler
	// created below cannot be tombstoned from another goroutine because the
	// tombstones are set under c.mu, which is held until the handover below.
	message := inboundMessage(record, settler)
	settler.handoff = message
	c.mu.Unlock()
	select {
	case <-c.forwarderStopC:
		c.abortSettler(settler, reused)
		return false, false, false
	case c.messages <- message:
		return true, true, true
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
	clear(c.pendingTransfers)
	clear(c.requeued)
	clear(c.discarded)
	clear(c.unsettled)
	clear(c.recordGenerations)
	clear(c.tenures)
	clear(c.activeGenerations)
	clear(c.fenced)
	return trackers
}

func (c *consumer) trackerForLocked(record *kgo.Record) *ackTracker {
	key := partitionKey{destination: record.Topic, partition: record.Partition}
	// A reassigned key must not reuse the revoked generation's tracker: the
	// captured snapshot still points at the old tracker, and a later
	// transfer drops exactly that generation. Fall through and create a
	// fresh tracker when the generations disagree.
	if tracker := c.trackers[key]; tracker != nil && c.trackerGenerations[key] == c.activeGenerations[key] {
		return tracker
	}
	generation := c.activeGenerations[key]
	if generation == 0 {
		return nil
	}
	if c.trackers == nil {
		c.trackers = make(map[partitionKey]*ackTracker)
	}
	if c.trackerGenerations == nil {
		c.trackerGenerations = make(map[partitionKey]uint64)
	}
	c.trackerGenerations[key] = generation
	tracker := newAckTracker(c.trackerBaseLocked(key, record.Offset), generation)
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
// offset. The records fetched for this partition and not delivered yet, which
// is every record whose delivery is still owed, are what the base is taken from
// instead. A record the loop is about to discard does not count, so an offset
// that will never be delivered cannot hold the base down.
//
// A reserved offset below them still has no live path on this consumer: its
// handoff is queued, or taken and not yet emitted. Starting the tracker past it
// would make TrackRedelivery report the handoff as already settled, dropping
// the offset's only live settlement path.
func (c *consumer) trackerBaseLocked(key partitionKey, base int64) int64 {
	for record := range c.recordGenerations {
		if record.Topic != key.destination || record.Partition != key.partition || record.Offset >= base {
			continue
		}
		if c.isRecordStaleLocked(record) {
			continue
		}
		base = record.Offset
	}
	settled := c.settledTransfers[key]
	for offset := range c.transferReservations[key] {
		if _, done := settled[offset]; done || offset >= base {
			continue
		}
		base = offset
	}
	return base
}

func (c *consumer) discardBufferedSettlersLocked(tracker *ackTracker) (map[*settler]struct{}, []driver.InboundMessage) {
	buffered := make(map[*settler]struct{})
	kept := make([]driver.InboundMessage, 0, len(c.messages))
drain:
	for {
		select {
		case message, ok := <-c.messages:
			if !ok {
				break drain
			}
			settler, ok := message.Settle.(*settler)
			// A nil tracker matches nothing: only a live generation owns
			// buffered messages, and draining tombstones leave no channel
			// copies behind.
			if ok && tracker != nil && settler.tracker == tracker {
				buffered[settler] = struct{}{}
				continue
			}
			kept = append(kept, message)
		default:
			break drain
		}
	}
	return buffered, kept
}

func (c *consumer) storeHandoffLocked(settler *settler) {
	if c.handoffs == nil {
		c.handoffs = make(map[partitionKey]map[int64]driver.InboundMessage)
	}
	offsets := c.handoffs[settler.key]
	if offsets == nil {
		offsets = make(map[int64]driver.InboundMessage)
		c.handoffs[settler.key] = offsets
	}
	offsets[settler.record.Offset] = settler.handoff
	// Record commit coverage for the transferred offset: if the source's
	// in-flight commit succeeds after the handoff is taken, the pending
	// entry makes completeSettlement broadcast suppression before the
	// target admits it.
	if settler.tracker != nil {
		if c.pendingTransfers == nil {
			c.pendingTransfers = make(map[partitionKey]map[int64]*ackTracker)
		}
		pending := c.pendingTransfers[settler.key]
		if pending == nil {
			pending = make(map[int64]*ackTracker)
			c.pendingTransfers[settler.key] = pending
		}
		pending[settler.record.Offset] = settler.tracker
	}
}

func (c *consumer) removeHandoffLocked(key partitionKey, offset int64) {
	offsets := c.handoffs[key]
	delete(offsets, offset)
	if len(offsets) == 0 {
		delete(c.handoffs, key)
	}
}

// revokedPartition captures the exact generation owned at revoke time: the
// tracker and every live settler seen before the fence went up. The transfer
// below detaches only this generation, so a reassigned generation emitting
// concurrently is never mistaken for abandoned ownership.
type revokedPartition struct {
	key     partitionKey
	tracker *ackTracker
	owned   map[*settler]*ackTracker
}

// snapshotRevokedLocked fences every revoked partition and captures the live
// settlers owned before the fence. The caller must hold c.mu.
func (c *consumer) snapshotRevokedLocked(partitions map[string][]int32) ([]revokedPartition, map[partitionKey]struct{}) {
	if c.fenced == nil {
		c.fenced = make(map[partitionKey]bool)
	}
	if c.tenures == nil {
		c.tenures = make(map[partitionKey]uint64)
	}
	revoked := make([]revokedPartition, 0, len(partitions))
	keys := make(map[partitionKey]struct{}, len(partitions))
	for topic, partitionList := range partitions {
		for _, partition := range partitionList {
			key := partitionKey{destination: topic, partition: partition}
			keys[key] = struct{}{}
			c.fenced[key] = true
			c.tenures[key]++
			delete(c.activeGenerations, key)
			rp := revokedPartition{key: key, tracker: c.trackers[key]}
			if rp.tracker == nil {
				for held := range c.settlers {
					if held.key == key && held.tracker != nil && !held.tombstoned {
						rp.tracker = held.tracker
						break
					}
				}
			}
			for held := range c.settlers {
				if held.key != key || held.tracker == nil || held.tombstoned || held.tracker != rp.tracker {
					continue
				}
				if rp.owned == nil {
					rp.owned = make(map[*settler]*ackTracker)
				}
				rp.owned[held] = held.tracker
				if !c.draining && held.handoff.Settle != nil {
					if c.pendingTransfers == nil {
						c.pendingTransfers = make(map[partitionKey]map[int64]*ackTracker)
					}
					pending := c.pendingTransfers[key]
					if pending == nil {
						pending = make(map[int64]*ackTracker)
						c.pendingTransfers[key] = pending
					}
					pending[held.record.Offset] = held.tracker
				}
			}
			revoked = append(revoked, rp)
		}
	}
	return revoked, keys
}

// reserveRevoked transfers the exact offsets captured at revoke to every
// peer before the callback returns. A cooperative target may poll the
// broker immediately after assignment, so asynchronous handoff alone is
// too late to prevent it from exposing the source offset first.
func (c *consumer) reserveRevoked(revoked []revokedPartition) {
	reserved := make(map[partitionKey]map[int64]struct{})
	for _, rp := range revoked {
		offsets := reserved[rp.key]
		if offsets == nil {
			offsets = make(map[int64]struct{})
			reserved[rp.key] = offsets
		}
		for settler := range rp.owned {
			offsets[settler.record.Offset] = struct{}{}
		}
	}
	for key, offsets := range reserved {
		for offset := range offsets {
			c.reserveTransfer(key, offset)
		}
	}
}

// revokeAndTransfer waits out in-flight callers off the assignment callback,
// then transfers each revoked generation. The wait is bounded by the caller's
// drain timeout and runs inside the callback, so a caller that can no longer
// settle observes ErrRevoked before the new assignment delivers; handoff
// emission stays asynchronous, and the bound is what keeps a slow caller from
// holding the group open for the whole rebalance.
func (c *consumer) revokeAndTransfer(revoked []revokedPartition, keys map[partitionKey]struct{}, timeout time.Duration) {
	// A caller-held delivery the bound does not reach is transferred, not
	// retained: its exact offset travels as the queued handoff and the source
	// caller observes ErrRevoked, so one offset never has two live settlement
	// paths.
	c.waitForSettlers(keys, timeout)
	for _, rp := range revoked {
		c.dropTrackerAfterRevoke(rp)
	}
}

func (c *consumer) dropTrackerAfterRevoke(rp revokedPartition) {
	c.dropTrackerExclusive(rp)
}

func (c *consumer) dropTrackerAfterLost(rp revokedPartition) {
	// Lost assignments never wait: revoke immediately, then transfer.
	if rp.tracker != nil {
		rp.tracker.Drop()
	}
	c.dropTrackerExclusive(rp)
}

// transferResult carries post-unlock work out of the transfer critical
// section: peer reservations, unrelated buffered messages, and dispatch.
type transferResult struct {
	key          partitionKey
	reservations []int64
	kept         []driver.InboundMessage
	dispatch     bool
}

func (c *consumer) dropTracker(destination string, partition int32) {
	key := partitionKey{destination: destination, partition: partition}
	c.assignmentMu.Lock()
	defer c.assignmentMu.Unlock()
	c.deliveryMu.Lock()
	defer c.deliveryMu.Unlock()
	c.mu.Lock()
	tracker := c.trackers[key]
	if tracker == nil {
		for settler := range c.settlers {
			if settler.key != key {
				continue
			}
			tracker = settler.tracker
			if tracker != nil {
				break
			}
		}
	}
	if tracker == nil {
		c.mu.Unlock()
		return
	}
	buffered, kept := c.discardBufferedSettlersLocked(tracker)
	reservations := make([]int64, 0)
	retainTracker := false
	if !c.draining {
		for settler := range c.settlers {
			if settler.key != key || settler.forceRevoked {
				continue
			}
			if _, wasBuffered := buffered[settler]; wasBuffered {
				continue
			}
			if settler.tracker == tracker && tracker.holds(settler.record.Offset) {
				retainTracker = true
				break
			}
		}
	}
	if !retainTracker {
		tracker.Drop()
		delete(c.pendingTransfers, key)
	}
	pending := tracker.Unacked()
	for settler := range c.settlers {
		if settler.key != key {
			continue
		}
		settlerTracker := settler.tracker
		if settlerTracker == nil {
			continue
		}
		if settlerTracker == tracker && tracker.holds(settler.record.Offset) {
			pending--
		}
		if settlerTracker != tracker {
			settlerTracker.Drop()
		}
		if _, wasBuffered := buffered[settler]; wasBuffered {
			delete(c.settlers, settler)
			c.releaseSlotsLocked(destination, 1)
			continue
		}
		if retainTracker && !settler.forceRevoked && settlerTracker == tracker && tracker.holds(settler.record.Offset) {
			if settler.handoff.Settle != nil {
				c.storeHandoffLocked(settler)
			}
			reservations = append(reservations, settler.record.Offset)
			settler.transferSettled = true
			settler.tombstoned = false
			settler.drainingRevoked = false
			continue
		}
		if settlerTracker == tracker && settler.handoff.Settle != nil {
			c.storeHandoffLocked(settler)
			reservations = append(reservations, settler.record.Offset)
		}
		settler.drainingRevoked = c.draining
		settler.tombstoned = true
		settler.tracker = nil
	}
	delete(c.trackers, key)
	delete(c.requeued, key)
	delete(c.discarded, key)
	delete(c.activeGenerations, key)
	if pending < 0 {
		pending = 0
	}
	if pending > c.unsettled[destination] {
		pending = c.unsettled[destination]
	}
	c.releaseSlotsLocked(destination, pending)
	budget := c.budgets[destination]
	if budget > 0 && c.unsettled[destination] < budget {
		c.setPauseReasonLocked(destination, pauseReasonPrefetch, false)
	}
	c.refreshAckGapLocked(destination)
	c.mu.Unlock()
	for _, offset := range reservations {
		c.reserveTransfer(key, offset)
	}
	for _, message := range kept {
		c.messages <- message
	}
	go c.dispatchHandoffs(key)
}

// dropTrackerExclusive transfers exactly the generation captured at revoke
// time. Settlers admitted under a newer generation after reassignment are
// not in the snapshot and are never detached.
func (c *consumer) dropTrackerExclusive(rp revokedPartition) {
	c.assignmentMu.Lock()
	defer c.assignmentMu.Unlock()
	c.deliveryMu.Lock()
	defer c.deliveryMu.Unlock()
	c.mu.Lock()
	if rp.tracker == nil && len(rp.owned) == 0 {
		c.mu.Unlock()
		return
	}
	result := c.transferOwnershipLocked(rp)
	c.mu.Unlock()
	c.finishTransfer(result)
}

// transferOwnershipLocked preserves the queued handoff first, then tombstones
// and detaches every revoked source settler and releases its destination
// prefetch slot exactly once. The queued copy becomes the offset's only live
// settlement path and one active target delivers it. The old caller observes
// ErrRevoked unless a broker commit already succeeded, whose success stands
// and suppresses the queued copy before it can become a second live delivery.
// A buffered delivery is removed from Messages here; a delivery the caller
// already holds settles through its detached settler, which never re-charges
// the released slot. Draining settlers stay retained until their caller
// reaches a terminal state so Nack during drain still succeeds. The caller
// must hold assignmentMu, deliveryMu, and c.mu.
func (c *consumer) transferOwnershipLocked(rp revokedPartition) transferResult {
	key := rp.key
	destination := key.destination
	tracker := rp.tracker
	result := transferResult{key: key, reservations: make([]int64, 0), dispatch: !c.draining}
	if c.selfTransfers == nil {
		// Initialized here rather than only in newConsumer because tests
		// build consumers directly as well.
		c.selfTransfers = make(map[partitionKey][]*settler)
	}

	var kept []driver.InboundMessage
	discardedBuffered := map[*settler]struct{}(nil)
	if !c.draining {
		discardedBuffered, kept = c.discardBufferedSettlersLocked(tracker)
		result.kept = kept
	}
	for settler, snapTracker := range rp.owned {
		if _, ok := c.settlers[settler]; !ok {
			continue
		}
		if settler.tracker != snapTracker {
			continue
		}
		if settler.tracker != tracker {
			settler.tracker.Drop()
		}
		if c.draining {
			// Drain keeps revoked settlers, including buffered deliveries,
			// until their caller reaches a terminal state. No handoff is
			// queued during shutdown, and the existing message stays in
			// Messages for its caller to Nack or Ack.
			settler.drainingRevoked = true
			settler.tombstoned = true
			settler.tracker = nil
			continue
		}
		if settler.handoff.Settle != nil {
			c.storeHandoffLocked(settler)
			result.reservations = append(result.reservations, settler.record.Offset)
		}
		// Detach after queueing: the queued copy, not this settler, is the
		// offset's live path from here on. The destination slot stays charged
		// until the caller's terminal settlement releases it, which is the
		// admission-until-settlement accounting every other delivery uses.
		// Releasing the slot here would let the source refill its budget while
		// its caller still holds a delivery that occupies a slot.
		settler.tombstoned = true
		settler.tracker = nil
		delete(c.settlers, settler)
		// Keep the settler reachable for the assignment path: if this member
		// is handed the partition back, it is the new owner and may settle
		// this delivery again.
		c.selfTransfers[key] = append(c.selfTransfers[key], settler)
		// A buffered delivery discarded above never reached a caller, so no
		// terminal settlement can release its slot; release it here.
		if _, dropped := discardedBuffered[settler]; dropped {
			c.forgetSelfTransferLocked(settler)
			if !settler.slotReleased {
				c.releaseSlotsLocked(destination, 1)
				settler.slotReleased = true
			}
		}
	}
	if tracker != nil {
		tracker.Drop()
	}
	delete(c.trackers, key)
	delete(c.trackerGenerations, key)
	delete(c.requeued, key)
	delete(c.discarded, key)
	delete(c.activeGenerations, key)
	budget := c.budgets[destination]
	if budget > 0 && c.unsettled[destination] < budget {
		c.setPauseReasonLocked(destination, pauseReasonPrefetch, false)
	}
	c.refreshAckGapLocked(destination)
	c.signalSettlerDoneLocked()
	return result
}

// regrantSelfTransfersLocked re-grants settlement authority when a revoked
// partition comes back to the member that transferred it. A cooperative
// rebalance revokes a partition and can re-assign it to the same member across
// a generation bump, and the new owner is then this consumer: the delivery the
// caller still holds keeps its one live settlement path instead of staying
// revoked, and the queued handoff for the offset is cleared rather than
// delivered as a second copy of a record the caller already has. The offset
// stays reserved until that settlement commits, so a broker redelivery of the
// same offset is still filtered.
// The caller must hold assignmentMu and c.mu.
func (c *consumer) regrantSelfTransfersLocked(partitions map[string][]int32) {
	if len(c.selfTransfers) == 0 {
		return
	}
	for topic, partitionList := range partitions {
		for _, partition := range partitionList {
			key := partitionKey{destination: topic, partition: partition}
			pending := c.selfTransfers[key]
			if len(pending) == 0 {
				continue
			}
			delete(c.selfTransfers, key)
			for _, settler := range pending {
				if settler.settled || !settler.tombstoned {
					continue
				}
				// A handoff already taken by another owner means that owner
				// holds the offset's live path; re-granting here would give
				// the offset two.
				if _, queued := c.handoffs[key][settler.record.Offset]; !queued {
					continue
				}
				tracker := c.trackers[key]
				if tracker == nil {
					tracker = c.trackerForLocked(settler.record)
				}
				settler.tombstoned = false
				settler.tracker = tracker
				c.settlers[settler] = struct{}{}
				c.removeHandoffLocked(key, settler.record.Offset)
			}
		}
	}
}

// forgetSelfTransferLocked drops a settled settler from the re-grant list so
// the list cannot grow with every transfer. The caller must hold c.mu.
func (c *consumer) forgetSelfTransferLocked(settler *settler) {
	pending := c.selfTransfers[settler.key]
	if len(pending) == 0 {
		return
	}
	kept := pending[:0]
	for _, candidate := range pending {
		if candidate != settler {
			kept = append(kept, candidate)
		}
	}
	if len(kept) == 0 {
		delete(c.selfTransfers, settler.key)
		return
	}
	c.selfTransfers[settler.key] = kept
}

func (c *consumer) finishTransfer(result transferResult) {
	for _, offset := range result.reservations {
		c.reserveTransfer(result.key, offset)
	}
	for _, message := range result.kept {
		c.messages <- message
	}
	if result.dispatch {
		go c.dispatchHandoffs(result.key)
	}
}

func (c *consumer) dropAllTrackers() {
	c.assignmentMu.Lock()
	c.mu.Lock()
	keySet := make(map[partitionKey]struct{}, len(c.trackers)+len(c.settlers))
	for key := range c.trackers {
		keySet[key] = struct{}{}
	}
	for settler := range c.settlers {
		keySet[settler.key] = struct{}{}
	}
	keys := make([]partitionKey, 0, len(keySet))
	for key := range keySet {
		keys = append(keys, key)
	}
	c.mu.Unlock()
	c.assignmentMu.Unlock()

	for _, key := range keys {
		c.dropTracker(key.destination, key.partition)
	}
}

func (c *consumer) completeSettlement(settler *settler, preserveUnsettled bool) {
	c.mu.Lock()
	if preserveUnsettled && (settler.tracker == nil || c.trackers[settler.key] != settler.tracker) {
		preserveUnsettled = false
	}
	settler.settled = true
	tracker := settler.tracker
	transferSettled := settler.transferSettled || settler.handoffDelivered
	markerOffsets := make(map[int64]struct{})
	clearReservation := false
	commitPoint := int64(-1)
	if tracker != nil {
		commitPoint = tracker.CommitPoint()
		if pending := c.pendingTransfers[settler.key]; pending != nil {
			for offset, pendingTracker := range pending {
				if pendingTracker == tracker && commitPoint > offset {
					markerOffsets[offset] = struct{}{}
					delete(pending, offset)
				}
			}
			if len(pending) == 0 {
				delete(c.pendingTransfers, settler.key)
			}
		}
		if transferSettled {
			if commitPoint > settler.record.Offset {
				markerOffsets[settler.record.Offset] = struct{}{}
			} else {
				pending := c.pendingTransfers[settler.key]
				if pending == nil {
					pending = make(map[int64]*ackTracker)
					c.pendingTransfers[settler.key] = pending
				}
				pending[settler.record.Offset] = tracker
			}
		}
		// A commit that won before the transfer detached this settler must
		// suppress the queued copy: the broker already has the offset, so
		// the target must never observe a second live delivery for it.
		if commitPoint > settler.record.Offset {
			if _, queued := c.handoffs[settler.key][settler.record.Offset]; queued {
				markerOffsets[settler.record.Offset] = struct{}{}
			}
		}
	}
	if settler.requeued && !settler.tombstoned && tracker != nil {
		if pending := c.pendingTransfers[settler.key]; pending != nil {
			if pending[settler.record.Offset] == tracker {
				delete(pending, settler.record.Offset)
				clearReservation = true
			}
			if len(pending) == 0 {
				delete(c.pendingTransfers, settler.key)
			}
		}
	}
	if len(markerOffsets) > 0 {
		if c.settledTransfers == nil {
			c.settledTransfers = make(map[partitionKey]map[int64]struct{})
		}
		offsets := c.settledTransfers[settler.key]
		if offsets == nil {
			offsets = make(map[int64]struct{})
			c.settledTransfers[settler.key] = offsets
		}
		for offset := range markerOffsets {
			offsets[offset] = struct{}{}
		}
	}
	for offset := range markerOffsets {
		c.removeHandoffLocked(settler.key, offset)
	}
	if tracker != nil && c.trackers[settler.key] == tracker {
		c.reconcileDiscardedLocked(settler.key, tracker)
	}
	if _, exists := c.settlers[settler]; !exists {
		c.forgetSelfTransferLocked(settler)
		if !preserveUnsettled && !settler.slotReleased {
			c.releaseSlotsLocked(settler.record.Topic, 1)
			settler.slotReleased = true
		}
		c.refreshAckGapLocked(settler.record.Topic)
		c.signalSettlerDoneLocked()
		c.mu.Unlock()
		if clearReservation {
			c.clearTransferReservation(settler.key, settler.record.Offset)
		}
		for offset := range markerOffsets {
			c.markSettledTransfer(settler.key, offset)
		}
		go c.redriveHandoffs(settler.key)
		return
	}
	delete(c.settlers, settler)
	if !preserveUnsettled && !settler.slotReleased {
		c.releaseSlotsLocked(settler.record.Topic, 1)
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
	if clearReservation {
		c.clearTransferReservation(settler.key, settler.record.Offset)
	}
	for offset := range markerOffsets {
		c.markSettledTransfer(settler.key, offset)
	}
	if shouldLeave {
		c.requestLeave()
	}
	go c.redriveHandoffs(settler.key)
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
	if err := ctx.Err(); err != nil {
		return classify("drain", driver.KindTransient, err)
	}
	c.stopForwarders()
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil
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
		return err
	}
	c.mu.Lock()
	leaveNow := len(c.settlers) == 0 && !c.leaveRequested
	c.mu.Unlock()
	if leaveNow {
		if err := c.requestLeaveAndWait(ctx); err != nil {
			return classify("drain", kafkaErrorKind(err), err)
		}
	}
	return nil
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
	if err := c.Drain(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, driver.ErrDrainTimeout) {
			return classify("stop", driver.KindTransient, fmt.Errorf("%w: %w", driver.ErrDrainTimeout, err))
		}
		return err
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

	if err := c.requestLeaveAndWait(ctx); err != nil {
		return stopContextError(err)
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
	// The move runs after the client is closed, so no rebalance callback can
	// queue another handoff here, and after the consumer is unregistered, so no
	// peer can take one from this queue. The snapshot is then complete.
	c.moveQueuedHandoffsOnLeave()
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
	// Every settler still here is a live settlement path this Release is about
	// to abandon without settling, its offset uncommitted, so the peer
	// reservation that filtered the broker's copy has to go while this member
	// still owns the partition: a peer assigned the partition after the leave
	// fetches from the committed offset and moves past a record the reservation
	// hid. An offset with a queued handoff is left out, because that copy still
	// needs the reservation until it is delivered or settled. Stop cannot reach
	// any of this: it refuses while a settler is outstanding.
	reservations := make([]transferReservation, 0)
	for settler := range c.settlers {
		if _, queued := c.handoffs[settler.key][settler.record.Offset]; queued {
			continue
		}
		reservations = append(reservations, transferReservation{key: settler.key, offset: settler.record.Offset})
	}
	trackers := c.detachAllTrackersLocked()
	c.mu.Unlock()
	c.assignmentMu.Unlock()

	for _, reservation := range reservations {
		c.clearTransferReservation(reservation.key, reservation.offset)
	}
	for _, tracker := range trackers {
		tracker.Drop()
	}

	if err := c.requestLeaveAndWait(ctx); err != nil {
		return classify("release", kafkaErrorKind(err), err)
	}

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

func (c *consumer) waitForSettlers(keys map[partitionKey]struct{}, timeout time.Duration) map[partitionKey]struct{} {
	timer := c.clock.Timer(timeout)
	defer timer.Stop()
	for {
		c.mu.Lock()
		has := c.hasSettlersForLocked(keys)
		c.mu.Unlock()
		if !has {
			return nil
		}
		select {
		case <-c.settlerCh:
		case <-timer.C:
			c.mu.Lock()
			timedOut := make(map[partitionKey]struct{})
			for settler := range c.settlers {
				if settler.tombstoned {
					continue
				}
				if _, ok := keys[settler.key]; ok {
					timedOut[settler.key] = struct{}{}
				}
			}
			c.mu.Unlock()
			return timedOut
		}
	}
}

func (c *consumer) hasSettlersForLocked(keys map[partitionKey]struct{}) bool {
	for s := range c.settlers {
		if s.tombstoned {
			continue
		}
		if _, ok := keys[s.key]; ok {
			return true
		}
	}
	return false
}

func (c *consumer) takeHandoffs(partitions map[string][]int32) []consumerHandoff {
	if c.conn == nil {
		return nil
	}
	wanted := make(map[partitionKey]struct{})
	for destination, partitionList := range partitions {
		for _, partition := range partitionList {
			wanted[partitionKey{destination: destination, partition: partition}] = struct{}{}
		}
	}
	// Every consumer that holds a queue for these partitions is a source,
	// including this one: a key reassigned back to its previous owner must
	// re-adopt the handoff it queued, or the offset keeps a reservation and
	// no live settlement path while no peer owns the key.
	c.conn.mu.RLock()
	consumers := make([]*consumer, 0, len(c.conn.consumers))
	for consumer := range c.conn.consumers {
		consumers = append(consumers, consumer)
	}
	c.conn.mu.RUnlock()
	var handoffs []consumerHandoff
	for _, source := range consumers {
		source.mu.Lock()
		for key, offsets := range source.handoffs {
			if _, ok := wanted[key]; !ok {
				continue
			}
			for offset, message := range offsets {
				handoffs = append(handoffs, consumerHandoff{source: source, message: message})
				delete(offsets, offset)
			}
			if len(offsets) == 0 {
				delete(source.handoffs, key)
			}
		}
		source.mu.Unlock()
	}
	slices.SortFunc(handoffs, func(left, right consumerHandoff) int {
		if left.message.Destination < right.message.Destination {
			return -1
		}
		if left.message.Destination > right.message.Destination {
			return 1
		}
		if left.message.Ref.Partition < right.message.Ref.Partition {
			return -1
		}
		if left.message.Ref.Partition > right.message.Ref.Partition {
			return 1
		}
		if left.message.Ref.Offset < right.message.Ref.Offset {
			return -1
		}
		if left.message.Ref.Offset > right.message.Ref.Offset {
			return 1
		}
		return 0
	})
	return handoffs
}

func (c *consumer) requeueHandoff(message driver.InboundMessage) {
	key := partitionKey{destination: message.Destination, partition: message.Ref.Partition}
	c.mu.Lock()
	if c.leftConnection {
		// This consumer has left the connection, so a copy stored here would sit
		// in a queue no dispatch scans. Forward it instead; forwardHandoffs takes
		// its own locks, so c.mu is released first.
		c.mu.Unlock()
		c.forwardHandoffs(message)
		return
	}
	if c.handoffs == nil {
		c.handoffs = make(map[partitionKey]map[int64]driver.InboundMessage)
	}
	offsets := c.handoffs[key]
	if offsets == nil {
		offsets = make(map[int64]driver.InboundMessage)
		c.handoffs[key] = offsets
	}
	offsets[message.Ref.Offset] = message
	c.mu.Unlock()
}

// forwardHandoffs places copies this consumer can no longer queue on a
// registered consumer, which offers each one to whichever consumer is eligible,
// now or at that consumer's next capacity release.
//
// It takes its own locks: the caller must hold none. The peer snapshot is taken
// under conn.mu.RLock and released before any consumer lock is taken, and each
// requeue and dispatch takes one consumer's mutex at a time.
func (c *consumer) forwardHandoffs(handoffs ...driver.InboundMessage) {
	if c.conn == nil || len(handoffs) == 0 {
		return
	}
	keys := make(map[partitionKey]struct{}, len(handoffs))
	for _, message := range handoffs {
		keys[partitionKey{destination: message.Destination, partition: message.Ref.Partition}] = struct{}{}
	}
	c.conn.mu.RLock()
	var peer *consumer
	for candidate := range c.conn.consumers {
		if candidate != c {
			peer = candidate
			break
		}
	}
	c.conn.mu.RUnlock()
	if peer == nil {
		// Nothing is left to filter these offsets, so dropping the copies loses
		// nothing. Every reservation for them lived on a consumer that has since
		// left the connection, and a reservation stops filtering the moment its
		// holder stops fetching; the broker's own redelivery of each uncommitted
		// offset is therefore visible again and arrives at the next start. A copy
		// kept here instead would sit in a queue no dispatch scans.
		return
	}
	for _, message := range handoffs {
		peer.requeueHandoff(message)
	}
	for key := range keys {
		go peer.dispatchHandoffs(key)
	}
}

// moveQueuedHandoffsOnLeave hands the queued copies held by c to a registered
// consumer before c leaves the connection, so each offset keeps a live
// settlement path. It marks c as left in the same hold that takes the queue, so
// no copy can be stored here after the snapshot: a concurrent requeue either
// takes c.mu before that hold, and its copy travels in the snapshot, or takes it
// after, and its copy is forwarded.
func (c *consumer) moveQueuedHandoffsOnLeave() {
	if c.conn == nil {
		return
	}
	c.mu.Lock()
	handoffs := make([]driver.InboundMessage, 0)
	keys := make(map[partitionKey]struct{})
	for key, offsets := range c.handoffs {
		for _, message := range offsets {
			handoffs = append(handoffs, message)
		}
		keys[key] = struct{}{}
	}
	clear(c.handoffs)
	for key := range keys {
		// This consumer never commits again, so its commit coverage cannot
		// suppress a moved copy and means nothing to a peer.
		delete(c.pendingTransfers, key)
	}
	c.leftConnection = true
	c.mu.Unlock()
	c.forwardHandoffs(handoffs...)
}

func (c *consumer) emitHandoff(source *consumer, message driver.InboundMessage) {
	key := partitionKey{destination: message.Destination, partition: message.Ref.Partition}
	c.mu.Lock()
	if offsets := c.settledTransfers[key]; offsets != nil {
		if _, marked := offsets[message.Ref.Offset]; marked {
			reservations := c.transferReservations[key]
			delete(reservations, message.Ref.Offset)
			if len(reservations) == 0 {
				delete(c.transferReservations, key)
			}
			c.mu.Unlock()
			return
		}
	}
	if offsets := c.handoffMarkers[key]; offsets != nil {
		if _, marked := offsets[message.Ref.Offset]; marked {
			reservations := c.transferReservations[key]
			delete(reservations, message.Ref.Offset)
			if len(reservations) == 0 {
				delete(c.transferReservations, key)
			}
			c.mu.Unlock()
			return
		}
	}
	// The offset keeps exactly one live settlement path until the source's
	// caller releases it. A live settler here that already carries this
	// handoff means the copy is obsolete; any other live settler here would
	// charge this consumer's prefetch budget for an offset it can already
	// settle, and a live settler on the source means the source's caller can
	// still settle the offset, so delivering a second copy would lose the
	// one-delivery-per-record accounting the target's budget depends on.
	// Keep the copy queued and the reservation in place: the source's Ack
	// suppresses the copy, and its Nack or drain releases the offset.
	liveSettler := false
	handoffLive := false
	for settler := range c.settlers {
		if settler.key == key && settler.record.Offset == message.Ref.Offset &&
			!settler.tombstoned && settler.tracker != nil {
			liveSettler = true
			handoffLive = settler.handoffDelivered
			break
		}
	}
	sourceHolds := false
	if source != nil && source != c {
		source.mu.Lock()
		for settler := range source.settlers {
			if settler.key == key && settler.record.Offset == message.Ref.Offset &&
				!settler.tombstoned && settler.tracker != nil {
				sourceHolds = true
				break
			}
		}
		source.mu.Unlock()
	}
	if (liveSettler && !handoffLive) || sourceHolds {
		c.mu.Unlock()
		if source != nil {
			source.requeueHandoff(message)
		}
		return
	}
	if liveSettler {
		reservations := c.transferReservations[key]
		delete(reservations, message.Ref.Offset)
		if len(reservations) == 0 {
			delete(c.transferReservations, key)
		}
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	record := &kgo.Record{
		Topic:     message.Destination,
		Partition: message.Ref.Partition,
		Offset:    message.Ref.Offset,
		Key:       append([]byte(nil), message.Key...),
		Value:     append([]byte(nil), message.Body...),
		Headers:   make([]kgo.RecordHeader, len(message.Headers)),
	}
	for index, header := range message.Headers {
		record.Headers[index] = kgo.RecordHeader{
			Key:   header.Key,
			Value: append([]byte(nil), header.Value...),
		}
	}
	c.tagRecord(record)
	c.mu.Lock()
	for settler := range c.settlers {
		if settler.key == key && settler.record.Offset == message.Ref.Offset &&
			!settler.tombstoned && settler.tracker != nil {
			transferReservations := c.transferReservations[key]
			delete(transferReservations, message.Ref.Offset)
			if len(transferReservations) == 0 {
				delete(c.transferReservations, key)
			}
			delete(c.recordGenerations, record)
			c.mu.Unlock()
			return
		}
	}
	if c.handoffReservations == nil {
		c.handoffReservations = make(map[partitionKey]map[int64]struct{})
	}
	reservations := c.handoffReservations[key]
	if reservations == nil {
		reservations = make(map[int64]struct{})
		c.handoffReservations[key] = reservations
	}
	reservations[message.Ref.Offset] = struct{}{}
	if c.handoffRecords == nil {
		c.handoffRecords = make(map[*kgo.Record]struct{})
	}
	c.handoffRecords[record] = struct{}{}
	c.mu.Unlock()
	delivered, _, created := c.emit(record)
	c.mu.Lock()
	requeue := !delivered && source != nil
	if !requeue {
		// Only a delivered or suppressed ownership result clears the exact
		// reservation. A requeued handoff keeps it so broker redelivery
		// stays filtered until one target owns the offset.
		transferReservations := c.transferReservations[key]
		delete(transferReservations, message.Ref.Offset)
		if len(transferReservations) == 0 {
			delete(c.transferReservations, key)
		}
	}
	delete(c.recordGenerations, record)
	delete(c.handoffRecords, record)
	reservations = c.handoffReservations[key]
	delete(reservations, message.Ref.Offset)
	if len(reservations) == 0 {
		delete(c.handoffReservations, key)
	}
	if created {
		if c.handoffMarkers == nil {
			c.handoffMarkers = make(map[partitionKey]map[int64]struct{})
		}
		markers := c.handoffMarkers[key]
		if markers == nil {
			markers = make(map[int64]struct{})
			c.handoffMarkers[key] = markers
		}
		markers[message.Ref.Offset] = struct{}{}
	}
	c.mu.Unlock()
	// Every failed emission returns ownership to the queue: fenced, stale,
	// inactive, and full targets all keep the handoff alive for the next
	// eligible target instead of dropping it.
	if requeue {
		source.requeueHandoff(message)
	}
}

func (c *consumer) redriveHandoffs(key partitionKey) {
	if c.conn == nil {
		return
	}
	c.conn.mu.RLock()
	sources := make([]*consumer, 0, len(c.conn.consumers))
	for source := range c.conn.consumers {
		sources = append(sources, source)
	}
	c.conn.mu.RUnlock()
	for _, source := range sources {
		source.dispatchHandoffs(key)
	}
}

// redriveDestinationHandoffs re-offers every queued handoff for destination. It is the capacity
// release's half of the handoff protocol: a queued handoff is proof the record has no other path
// back, since its offset reservation keeps the broker's redelivery filtered, so it must be
// offered again whenever the destination it belongs to stops being full.
func (c *consumer) redriveDestinationHandoffs(destination string) {
	if c.conn == nil {
		return
	}
	c.conn.mu.RLock()
	consumers := make([]*consumer, 0, len(c.conn.consumers))
	for consumer := range c.conn.consumers {
		consumers = append(consumers, consumer)
	}
	c.conn.mu.RUnlock()
	keys := make(map[partitionKey]struct{})
	for _, consumer := range consumers {
		consumer.mu.Lock()
		for key := range consumer.handoffs {
			if key.destination == destination {
				keys[key] = struct{}{}
			}
		}
		consumer.mu.Unlock()
	}
	for key := range keys {
		c.dispatchHandoffs(key)
	}
}

func (c *consumer) dispatchHandoffs(key partitionKey) {
	if c.conn == nil {
		return
	}
	c.conn.mu.RLock()
	consumers := make([]*consumer, 0, len(c.conn.consumers))
	for consumer := range c.conn.consumers {
		consumers = append(consumers, consumer)
	}
	c.conn.mu.RUnlock()
	partitions := map[string][]int32{key.destination: {key.partition}}
	for _, target := range consumers {
		target.mu.Lock()
		_, assigned := target.activeGenerations[key]
		fenced := target.fenced[key]
		target.mu.Unlock()
		if !assigned || fenced {
			continue
		}
		for _, handoff := range target.takeHandoffs(partitions) {
			target.reserveTransferOffset(key, handoff.message.Ref.Offset)
			target.emitHandoff(handoff.source, handoff.message)
		}
	}
}

func (c *consumer) onPartitionsAssigned(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
	if len(partitions) == 0 {
		return
	}
	c.assignmentMu.Lock()
	c.mu.Lock()
	if c.assignmentGenerations == nil {
		c.assignmentGenerations = make(map[partitionKey]uint64)
	}
	if c.activeGenerations == nil {
		c.activeGenerations = make(map[partitionKey]uint64)
	}
	if c.fenced == nil {
		c.fenced = make(map[partitionKey]bool)
	}
	if c.tenures == nil {
		c.tenures = make(map[partitionKey]uint64)
	}
	for topic, partitionList := range partitions {
		for _, partition := range partitionList {
			key := partitionKey{destination: topic, partition: partition}
			c.assignmentGenerations[key]++
			gen := c.assignmentGenerations[key]
			c.activeGenerations[key] = gen
			c.fenced[key] = false
			if c.tenures[key] == 0 {
				c.tenures[key] = 1
			}
		}
	}
	c.regrantSelfTransfersLocked(partitions)
	c.mu.Unlock()
	c.assignmentMu.Unlock()
	go c.emitAssignedHandoffs(partitions)
	c.sendRebalanceError("assigned", partitions)
}

func (c *consumer) emitAssignedHandoffs(partitions map[string][]int32) {
	for _, handoff := range c.takeHandoffs(partitions) {
		c.reserveTransferOffset(partitionKey{
			destination: handoff.message.Destination,
			partition:   handoff.message.Ref.Partition,
		}, handoff.message.Ref.Offset)
		c.emitHandoff(handoff.source, handoff.message)
	}
}

func (c *consumer) onPartitionsRevoked(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
	if len(partitions) == 0 {
		return
	}
	// Capture under assignmentMu in assignmentMu -> c.mu order: the snapshot
	// then fences the same generation the transfer later detaches, and a
	// concurrent assignment cannot slip a new generation into it.
	c.assignmentMu.Lock()
	c.mu.Lock()
	draining := c.draining
	requestLeave := draining && !c.leaveRequested
	if requestLeave {
		c.leaveRequested = true
	}
	revoked, keys := c.snapshotRevokedLocked(partitions)
	c.mu.Unlock()
	c.assignmentMu.Unlock()
	if !draining {
		c.reserveRevoked(revoked)
	}
	c.sendRebalanceError("revoked", partitions)
	if draining {
		c.dropAllTrackers()
		if requestLeave {
			c.requestLeave()
		}
		return
	}
	timeout := c.rebalanceDrainTimeout
	if timeout <= 0 {
		timeout = 25 * time.Second
	}
	// The settle window is bounded and runs here so the caller of this
	// callback sees the transfer complete before it returns. Handoff emission
	// stays asynchronous.
	c.revokeAndTransfer(revoked, keys, timeout)
}

func (c *consumer) onPartitionsLost(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
	if len(partitions) == 0 {
		return
	}
	c.assignmentMu.Lock()
	c.mu.Lock()
	draining := c.draining
	requestLeave := draining && !c.leaveRequested
	if requestLeave {
		c.leaveRequested = true
	}
	revoked, _ := c.snapshotRevokedLocked(partitions)
	c.mu.Unlock()
	c.assignmentMu.Unlock()
	if !draining {
		c.reserveRevoked(revoked)
	}
	for _, rp := range revoked {
		c.dropTrackerAfterLost(rp)
	}
	if requestLeave {
		c.requestLeave()
	}
	c.sendRebalanceError("lost", partitions)
}
