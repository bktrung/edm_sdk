package kafka

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	kafkarules "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/kafka"
)

type pauseReason string

const (
	pauseReasonPrefetch   pauseReason = "prefetch-full"
	pauseReasonUserPaused pauseReason = "user-paused"
)

type pauseReasonSet map[pauseReason]struct{}

func (s pauseReasonSet) add(reason pauseReason) bool {
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

func (s pauseReasonSet) blocksDelivery() bool {
	return len(s) > 0
}

func (s pauseReasonSet) permitsRedelivery() bool {
	for reason := range s {
		if reason != pauseReasonPrefetch {
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
	// admitted is the aggregate of destination charges, including requeues
	// that retain their original charge until redelivery settles.
	admitted int
	// outstanding counts, per partition, the deliveries this consumer emitted
	// for it and has not released. It carries the hold rule: at most one
	// delivery per partition is outstanding, so the admission gate refuses a
	// partition's next record while its count is non-zero. It follows the
	// destination slot's charge and release points exactly, which is what keeps
	// a requeue's redelivery holding its partition until it settles.
	outstanding map[partitionKey]int
	// partitionPauses is the sole source of partition fetch pauses. Keeping
	// read-ahead and head-hold in one set prevents either release from
	// reopening a partition still held for the other reason.
	partitionPauses map[partitionKey]partitionPauseSet
	pending         map[partitionKey][]*kgo.Record
	heldUntil       map[partitionKey]time.Time
	// dirty holds the partitions that a partition-scoped event - a settle that
	// freed the partition's hold, a requeue, a cleared partition pause, new
	// records fetched for it - may have made admissible since the last
	// admission pass. partialScan reports that every event since the last full
	// pass was one of those, so the next pass may visit the dirty partitions
	// alone. Its zero value is a full pass, and every other wake, a rebalance
	// callback, and a poll iteration that was not a wake clear it: an event
	// that did not name its partition is answered by visiting all of them, as
	// every pass did before the set existed.
	//
	// The set exists for cost, not for correctness: at one settlement per
	// delivery a full pass per wake made the loop's work per delivery grow
	// with the partitions owned.
	dirty       map[partitionKey]struct{}
	partialScan bool
	// flushKeys is the admission pass's list of partitions to visit, reused
	// across passes. Only the poll loop runs a pass, so it reads the list
	// without c.mu once the list is filled.
	flushKeys []partitionKey
	// visitedKeys collects every partition one flushPending call visited, and
	// visitedAll records that one of its passes was full. The read-ahead
	// reconciliation after the flush reads them: a queue's length and its
	// requeue count change only where a pass visited, or through an event
	// that marks the partition for the next one. Both belong to the poll loop.
	visitedKeys []partitionKey
	visitedAll  bool
	// lastAdmitted is the partition the last fresh admission came from. A full
	// pass visits partitions in key order starting after it, so when one slot
	// is contended every waiting partition gets a turn before any gets a
	// second one. The caller must hold c.mu.
	lastAdmitted partitionKey
	settlers     map[*settler]struct{}
	trackers     map[partitionKey]*ackTracker
	// owned holds the partitions this consumer currently owns. The rebalance
	// callbacks are its only writers and admission reads it under c.mu, so a
	// revoke takes effect over the records the poll loop is already holding.
	owned map[partitionKey]bool
	// warned holds the destinations this consumer has already reported as
	// underprovisioned. It is never cleared: the warning is about the shape of
	// the subscription and its destination, which a later rebalance does not
	// change, so a member that churns would otherwise repeat it on every
	// membership change.
	warned map[string]bool
	// inHand holds the records the poll loop has taken from franz-go and not
	// yet resolved. A fresh tracker for a partition takes its base from the
	// lowest offset here rather than from the record that created it: the
	// lowest offset the consumer still owes a delivery for is what the base
	// means, and the records this loop is holding for the partition are the
	// ones whose delivery is still owed.
	inHand map[*kgo.Record]struct{}
	// fetchScratch holds the records of the fetch response the poll loop took
	// in hand last time it polled. The loop takes them in hand and collects
	// them in one pass and queues them in a second pass over this slice, so a
	// response is walked once for its records rather than once per pass.
	fetchScratch []*kgo.Record
	// requeued counts, per partition, the records waiting at the head of the
	// queue that a caller put back. A redelivery replaces the delivery that
	// held the partition rather than adding a second one, so the partition's
	// single outstanding charge stays with the record, the destination's
	// budget is not charged twice, and the read-ahead pause that counted the
	// delivery in flight does not block the redelivery it is waiting for.
	requeued              map[partitionKey]int
	settlerCh             chan struct{}
	rebalanceDrainTimeout time.Duration
	laneFaultDestination  string
	clock                 clock.Clock
	headTimer             clock.Timer
	headTimerSet          bool
	headTimerDue          time.Time
	headTimerChanged      chan struct{}
	// offsetMu serializes CommitOffsetsSync with SetOffsets because franz-go
	// forbids those operations from running concurrently.
	offsetMu  sync.Mutex
	committer offsetCommitter
	// commitFn sends a committer round; newConsumer installs sendCommits, and
	// a test replaces it to settle without a group coordinator.
	commitFn          commitSender
	backlogMu         sync.Mutex
	backlogProbeMu    sync.Mutex
	backlogClient     *kgo.Client
	backlogPartitions map[string][]int32
	// assignmentMu keeps a rebalance callback indivisible against the rest of
	// the consumer. A multi-partition revoke holds it so no admission or
	// settlement can land between two partitions of the same revocation and
	// observe a half-applied reassignment.
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

// commitRecord commits this delivery's offset through the selected tracker.
// Only the tracker that admitted it may skip offsets franz-go did not deliver;
// a renewed owner must still settle its exact base offset. admitted is
// s.tracker read under owner.mu in the same section that selected tracker,
// because a draining revoke clears s.tracker under that lock.
func (s *settler) commitRecord(ctx context.Context, tracker, admitted *ackTracker) error {
	commit := func(commitPoint int64) error {
		return s.owner.commitOffset(ctx, s.key, commitPoint)
	}
	if tracker == admitted {
		return tracker.AckOwn(s.record.Offset, commit)
	}
	return tracker.Ack(s.record.Offset, commit)
}

// Ack acknowledges this delivery and commits the Kafka offset after its record.
// Each partition permits only one outstanding delivery at a time. The tracker
// that admitted the delivery may skip offsets franz-go never delivered, but a
// settlement routed through a newer tracker must match that tracker's cursor.
func (s *settler) Ack(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("ack", driver.KindTransient, err)
	}
	// The handoff runs after s.mu is released, which the defer order gives: it
	// takes deliveryMu, and a settler's lock is no part of that order.
	handOff := false
	defer func() {
		if handOff {
			s.owner.handOff(s.key)
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()

	s.owner.mu.Lock()
	if s.settled {
		s.owner.mu.Unlock()
		return classify("ack", driver.KindFatal, driver.ErrAlreadySettled)
	}
	tracker := s.owner.settlementTrackerLocked(s)
	admitted := s.tracker
	s.owner.mu.Unlock()
	if tracker == nil {
		s.completeRevocation()
		return classify("ack", driver.KindFatal, ErrRevoked)
	}

	if err := s.commitRecord(ctx, tracker, admitted); err != nil {
		if errors.Is(err, ErrRevoked) {
			s.completeRevocation()
		}
		return classifySettlement("ack", err)
	}
	handOff = !s.owner.completeSettlement(s, false)
	return nil
}

// Nack either commits and discards a record or puts it back at the head of its
// partition's queue for an in-run redelivery. Requeue does not advance the
// cursor: the record's offset is still unsettled, so a partition's next owner
// redelivers it.
// CountAsFailure is a no-op because classic Kafka groups expose no delivery count.
func (s *settler) Nack(ctx context.Context, options driver.NackOptions) error {
	if err := ctx.Err(); err != nil {
		return classify("nack", driver.KindTransient, err)
	}
	// The handoff runs after s.mu is released, as in Ack.
	handOff := false
	defer func() {
		if handOff {
			s.owner.handOff(s.key)
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()

	s.owner.mu.Lock()
	if s.settled {
		s.owner.mu.Unlock()
		return classify("nack", driver.KindFatal, driver.ErrAlreadySettled)
	}
	tracker := s.owner.settlementTrackerLocked(s)
	admitted := s.tracker
	draining := s.owner.draining
	drainingRevoked := s.drainingRevoked && draining
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
		s.owner.mu.Lock()
		// A requeue queues the redelivery this consumer owes, and it owes one
		// here only while the partition is still this consumer's and the delivery
		// is still settling under the tracker that admitted it, and nothing has
		// stopped this consumer admitting. All three conditions are needed.
		// settlementTrackerLocked returns the admitted tracker when the partition
		// is not owned, so identity alone would queue a redelivery on a revoked
		// partition; it returns the current ownership's tracker for a retained
		// partition whose ownership was renewed, so the map's tracker alone would
		// queue a redelivery whose charge completeSettlement has already released.
		// Every other case leaves the offset uncommitted for the ownership in
		// force, which fetches the record itself: the poll loop drops a record of a
		// partition this consumer does not own through its stale pass, but the
		// count this added is not the stale pass's to drop, and a queued redelivery
		// admitted as a reuse would take the hold rule and the budget with no
		// charge behind it.
		if s.owner.owned[s.key] && s.owner.trackers[s.key] == s.tracker && tracker == s.tracker && !draining {
			s.owner.requeueLocked(s)
		}
		s.owner.mu.Unlock()
		// A queued redelivery inherits the delivery's charge, so the partition
		// stays held and the destination's slot stays counted until the redelivery
		// settles. Queueing nothing is the exception, and for the same reason as
		// above: a charge kept here has nothing left to hold and would shrink the
		// destination's window for the rest of this consumer's life. Draining is
		// part of that condition because the two are not the same instant: a
		// requeue can arrive after Drain marked the consumer and before the leave's
		// revoke reaches the partition, and this settler is gone from c.settlers by
		// the time the revoke runs, so the revoke's own release has nothing to
		// release.
		handOff = !s.owner.completeSettlement(s, !draining)
		return nil
	}

	if err := s.commitRecord(ctx, tracker, admitted); err != nil {
		if errors.Is(err, ErrRevoked) {
			s.completeRevocation()
		}
		return classifySettlement("nack", err)
	}
	s.owner.conn.log().Warn(
		"discarding Kafka record",
		"topic", s.record.Topic,
		"partition", s.record.Partition,
		"offset", s.record.Offset,
	)
	handOff = !s.owner.completeSettlement(s, false)
	return nil
}

// settlementTrackerLocked returns the tracker a settlement of s commits
// through, or nil when this consumer has to refuse it. The caller must hold
// c.mu.
//
// A delivery is admitted under the tracker of the ownership in force, and a
// revoke detaches and revokes that tracker, so a settlement arriving after the
// revoke fails. One case is not a partition that moved away, and both protocol
// families reach it: an eager balancer ("sticky", "range") revokes every
// partition on every membership change and reassigns the ones this member
// keeps, and a cooperative one re-assigns a partition to this member out of
// another member's hands and revokes it here first. A delivery the caller still
// holds then outlives the ownership it was made under while its partition never
// left this consumer. The partition is owned here again, so the live settlement
// path is the tracker of the current ownership. An ownership that has not
// admitted a record yet has no tracker, and the delivery's own offset is where
// that tracker starts, which is what builds one; the offset a settlement
// commits is the base the new ownership owes, so a commit through it cannot
// pass a record this consumer still has to deliver.
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
	var err error
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
	if err := validateKafkaDrainTimeout(connection.driverOptions, drainTimeout); err != nil {
		return nil, classify("consumer", driver.KindFatal, err)
	}
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
		partitionPauses:       make(map[partitionKey]partitionPauseSet),
		pending:               make(map[partitionKey][]*kgo.Record),
		heldUntil:             make(map[partitionKey]time.Time),
		settlers:              make(map[*settler]struct{}),
		trackers:              make(map[partitionKey]*ackTracker),
		owned:                 make(map[partitionKey]bool),
		warned:                make(map[string]bool),
		inHand:                make(map[*kgo.Record]struct{}),
		settlerCh:             make(chan struct{}, 1),
		rebalanceDrainTimeout: drainTimeout,
		requeued:              make(map[partitionKey]int),
		clock:                 clock.NewReal(),
		headTimerChanged:      make(chan struct{}, 1),
	}
	consumer.leaveFn = consumer.leaveGroup
	consumer.commitFn = consumer.sendCommits
	for index, destination := range cfg.Destinations {
		consumer.budgets[destination] = cfg.DestinationPrefetch(index)
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
	// kgo.NewClient already started the group goroutines, and onPartitionsAssigned
	// reaches resumePartitionIfFreeLocked, which reads c.client under this lock.
	consumer.mu.Lock()
	consumer.client = client
	consumer.mu.Unlock()
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
	go consumer.headHoldLoop()
	//nolint:contextcheck // this goroutine uses the consumer-owned cancellation context.
	go consumer.poll(pollCtx)
	return consumer, nil
}

func consumerClientOpts(connection *conn, cfg driver.ConsumerConfig, group string, consumer *consumer) ([]kgo.Opt, error) {
	fetchMaxWait, err := resolveFetchMaxWait(connection.driverOptions)
	if err != nil {
		return nil, err
	}
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
		// An exhausted destination cannot block a sibling destination on the same
		// broker for longer than this value. franz-go defaults FetchMaxWait to
		// 5000ms; this driver's 50ms default is deliberately below Kafka's own
		// 500ms default because it multiplexes destinations over one client.
		kgo.FetchMaxWait(fetchMaxWait),
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

// Consume-path knob defaults. Values are passed explicitly so an unset key
// still has a value the driver chose and tests can observe.
//
// defaultKafkaSessionTimeout matches the fallback that config validation uses
// to bound lifecycle.rebalanceDrainTimeout. That bound is only meaningful if
// the session timeout the broker enforces is the one validation measured, so
// the two values must move together.
const (
	defaultKafkaFetchMaxWait                = 50 * time.Millisecond
	maxKafkaFetchMaxWait                    = time.Duration(math.MaxInt32) * time.Millisecond
	defaultKafkaFetchMaxBytes         int32 = 50 << 20
	defaultKafkaSessionTimeout              = 45 * time.Second
	defaultKafkaRebalanceTimeout            = 60 * time.Second
	defaultKafkaRebalanceDrainTimeout       = 25 * time.Second
)

const fetchMaxWaitOption = "kafka.fetchMaxWait"

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

// resolveFetchMaxWait maps kafka.fetchMaxWait onto the maximum time Kafka
// holds an empty fetch. The value must be a whole number of milliseconds in
// franz-go's int32 wire range, so an invalid operator value cannot silently
// become zero or wrap around.
func resolveFetchMaxWait(options map[string]string) (time.Duration, error) {
	value, ok := options[fetchMaxWaitOption]
	if !ok {
		return defaultKafkaFetchMaxWait, nil
	}
	wait, err := time.ParseDuration(value)
	if err != nil || wait < time.Millisecond || wait > maxKafkaFetchMaxWait || wait%time.Millisecond != 0 {
		return 0, fmt.Errorf(
			"kafka: invalid %s %q; must be a whole-millisecond duration from 1ms to %s",
			fetchMaxWaitOption,
			value,
			maxKafkaFetchMaxWait,
		)
	}
	return wait, nil
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

// validateKafkaDrainTimeout rejects a revoke wait that can outlive either
// broker group timeout. Direct driver callers need the same protection as the
// root configuration path.
func validateKafkaDrainTimeout(options map[string]string, drainTimeout time.Duration) error {
	sessionTimeout, err := resolveSessionTimeout(options)
	if err != nil {
		return err
	}
	rebalanceTimeout, err := resolveRebalanceTimeoutOption(options)
	if err != nil {
		return err
	}
	if kafkarules.ExceedsKafkaDrainBound(drainTimeout, sessionTimeout) {
		return fmt.Errorf(
			"kafka: revoke wait bound %s exceeds 0.6 x kafka.sessionTimeout %s (kafka.rebalanceTimeout=%s)",
			drainTimeout,
			sessionTimeout,
			rebalanceTimeout,
		)
	}
	if kafkarules.ExceedsKafkaDrainBound(drainTimeout, rebalanceTimeout) {
		return fmt.Errorf(
			"kafka: revoke wait bound %s exceeds 0.6 x kafka.rebalanceTimeout %s (kafka.sessionTimeout=%s)",
			drainTimeout,
			rebalanceTimeout,
			sessionTimeout,
		)
	}
	return nil
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
	for index := range cfg.Destinations {
		total += cfg.DestinationPrefetch(index)
	}
	if cfg.Prefetch > 0 && total > cfg.Prefetch {
		total = cfg.Prefetch
	}
	if total < 1 {
		return 1
	}
	return total
}

// Messages returns the channel of delivered messages. The channel closes when Stop or Release completes.
func (c *consumer) Messages() <-chan driver.InboundMessage { return c.messages }

// Errors returns asynchronous consumer errors. The channel closes when Stop or Release completes.
func (c *consumer) Errors() <-chan error { return c.errors }

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

// wakePollLocked interrupts the poll wait so the loop admits again over all
// the records it is holding. It never blocks: a wake that finds no wait in
// progress records itself for the next one. The caller must hold c.mu.
func (c *consumer) wakePollLocked() {
	c.partialScan = false
	c.interruptPollLocked()
}

// wakePartitionLocked is wakePollLocked for an event that can only have made
// key's own head admissible, so the pass the wake starts may visit key alone.
// The caller must hold c.mu.
func (c *consumer) wakePartitionLocked(key partitionKey) {
	c.markDirtyLocked(key)
	c.interruptPollLocked()
}

// markDirtyLocked adds key to the partitions the next admission pass visits.
// The caller must hold c.mu.
func (c *consumer) markDirtyLocked(key partitionKey) {
	if c.dirty == nil {
		c.dirty = make(map[partitionKey]struct{})
	}
	c.dirty[key] = struct{}{}
}

func (c *consumer) interruptPollLocked() {
	c.pollWakePending = true
	if c.pollCancel != nil {
		c.pollCancel()
	}
}

func (c *consumer) poll(ctx context.Context) {
	defer close(c.pollDone)
	defer c.client.AllowRebalance()
	for {
		if !c.flushPending() {
			return
		}
		c.syncReadAheadAfterFlush()

		fetchCtx, cancelFetch := context.WithCancel(ctx)
		if c.claimPollWake(cancelFetch) {
			cancelFetch()
			continue
		}
		fetches := c.client.PollRecords(fetchCtx, 0)
		woken := fetchCtx.Err() != nil && ctx.Err() == nil
		cancelFetch()
		c.releasePollWake()
		if !woken {
			// A broker response, not a wake, ended the wait: the pass after it
			// visits every partition, which bounds how long anything a
			// partition-scoped mark missed can wait to one response.
			c.mu.Lock()
			c.partialScan = false
			c.mu.Unlock()
		}
		if ctx.Err() != nil || fetches.IsClientClosed() {
			c.client.AllowRebalance()
			return
		}
		if !c.handleFetches(fetches, woken) {
			return
		}
	}
}

func (c *consumer) handleFetches(fetches kgo.Fetches, bounded bool) bool {
	c.mu.Lock()
	records := c.fetchScratch[:0]
	if count := fetches.NumRecords(); count > cap(records) {
		records = make([]*kgo.Record, 0, count)
	}
	// One pass takes every record of the response in hand and collects it, so
	// the queueing pass walks the collected records rather than the response a
	// second time.
	fetches.EachRecord(func(record *kgo.Record) {
		c.tagRecordLocked(record)
		records = append(records, record)
	})
	for _, record := range records {
		key := partitionKey{destination: record.Topic, partition: record.Partition}
		c.pending[key] = append(c.pending[key], record)
		c.markDirtyLocked(key)
	}
	// The records belong to in-hand and to the queues from here on, and holding
	// the slice of them past this point would keep the response they came from
	// alive until the next poll with nothing left to read them for.
	clear(records)
	c.fetchScratch = records[:0]
	c.mu.Unlock()
	defer c.client.AllowRebalance()

	for _, fetchErr := range fetches.Errors() {
		if bounded && (errors.Is(fetchErr.Err, context.DeadlineExceeded) || errors.Is(fetchErr.Err, context.Canceled)) {
			continue
		}
		kind := kafkaErrorKind(fetchErr.Err)
		if kind != driver.KindTransient {
			kind = driver.KindFatal
		}
		c.sendError(classify("consumer", kind, fmt.Errorf("kafka fetch %s[%d]: %w", fetchErr.Topic, fetchErr.Partition, fetchErr.Err)))
		if kind == driver.KindFatal {
			return false
		}
	}
	return true
}

// flushPending admits the head record of every partition whose head is due and
// whose partition and destination admit a delivery, and repeats until no
// further admission is possible. A queue whose head belongs to a partition this
// consumer no longer owns is dropped whole: a not-owned partition's records are
// never delivered, and every record behind that head belongs to the same
// partition.
func (c *consumer) flushPending() bool {
	c.visitedKeys = c.visitedKeys[:0]
	c.visitedAll = false
	for {
		c.mu.Lock()
		keys := c.flushKeys[:0]
		full := !c.partialScan
		if full {
			for key := range c.pending {
				keys = append(keys, key)
			}
			// Map order is random, which shares a contended slot out only on
			// average. Key order, starting after the partition admitted last,
			// shares it out in turn: a partition waiting behind a busy one is
			// reached before the busy one is visited again.
			slices.SortFunc(keys, comparePartitionKeys)
			start, found := slices.BinarySearchFunc(keys, c.lastAdmitted, comparePartitionKeys)
			if found {
				start++
			}
			slices.Reverse(keys[:start])
			slices.Reverse(keys[start:])
			slices.Reverse(keys)
		} else {
			for key := range c.dirty {
				if _, queued := c.pending[key]; queued {
					keys = append(keys, key)
				}
			}
		}
		clear(c.dirty)
		c.partialScan = true
		c.flushKeys = keys
		c.mu.Unlock()
		c.visitedAll = c.visitedAll || full
		c.visitedKeys = append(c.visitedKeys, keys...)
		if len(keys) == 0 {
			break
		}
		progress := false
		for _, key := range keys {
			outcome := c.deliverHead(key)
			if outcome == headStopped {
				return false
			}
			if outcome == headHeld {
				continue
			}
			if !full {
				// A head that left its queue can leave the next record of its
				// partition admissible at once: a head whose offset is already
				// committed is skipped without charging the partition. A full
				// pass repeats and finds it; a partial pass visits only what is
				// marked, so the partition marks itself.
				c.mu.Lock()
				c.markDirtyLocked(key)
				c.mu.Unlock()
			}
			progress = true
		}
		if !progress {
			break
		}
		if full {
			// A full pass that admitted something repeats in full, as every
			// pass did before the partition-scoped wake. A partial pass
			// repeats only over what was marked while it ran: the partitions
			// it admitted from now hold a delivery, and nothing else changed
			// that did not mark itself.
			c.mu.Lock()
			c.partialScan = false
			c.mu.Unlock()
		}
	}
	return true
}

func comparePartitionKeys(a, b partitionKey) int {
	if order := strings.Compare(a.destination, b.destination); order != 0 {
		return order
	}
	return cmp.Compare(a.partition, b.partition)
}

// tagRecordLocked records a record the poll loop has taken from franz-go and
// not yet resolved. The caller holds c.mu. A fresh tracker for the record's partition takes its base from the
// lowest offset in hand, which is why the loop tags every record of a fetch
// response before it admits any of them.
func (c *consumer) tagRecordLocked(record *kgo.Record) {
	if c.inHand == nil {
		c.inHand = make(map[*kgo.Record]struct{})
	}
	c.inHand[record] = struct{}{}
}

// isRecordStaleLocked reports whether a record the loop is holding is one this
// consumer must drop rather than deliver: it belongs to a partition this
// consumer does not own. The poll loop asks before it admits a record and
// again before it keeps one, so a revoke empties the pending list of the
// partitions it took away on the loop's next pass, and the broker's own
// redelivery to the new owner is the copy that remains. The caller must hold
// c.mu.
func (c *consumer) isRecordStaleLocked(record *kgo.Record) bool {
	return !c.owned[partitionKey{destination: record.Topic, partition: record.Partition}]
}

// releaseSlotsLocked returns up to slots destination slots that unsettled deliveries were
// charged, and clears the pause that stopped fetching for the destination when the return takes
// it from full to not-full.
//
// It reports whether the return freed a slot other partitions may be waiting for: the aggregate
// was full, or the destination's budget was. Either way it wakes the poll loop for a pass over
// every partition, since any of them may be the one that was refused.
//
// The caller must hold c.mu.
func (c *consumer) releaseSlotsLocked(destination string, slots int) (contended bool) {
	charged := c.unsettled[destination]
	if slots > charged {
		slots = charged
	}
	budget := c.budgets[destination]
	if slots > 0 {
		c.unsettled[destination] = charged - slots
		wasFull := c.cfg.Prefetch > 0 && c.admitted >= c.cfg.Prefetch
		c.admitted -= slots
		if wasFull && c.admitted < c.cfg.Prefetch {
			contended = true
		}
		if budget > 0 && charged >= budget && charged-slots < budget {
			contended = true
		}
		if contended {
			c.wakePollLocked()
		}
	}
	if budget > 0 && charged-slots < budget {
		// The destination is not full any more, so the pause that stopped
		// fetching has to go.
		if c.client != nil {
			c.setPauseReasonLocked(destination, pauseReasonPrefetch, false)
		}
	}
	return contended
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
// deliveries, and marks the partition for the next admission pass when that
// takes the count to zero.
//
// The mark does not wake the poll loop. A settlement made through Ack or Nack
// hands the partition's next record over itself (handOff), or, when it freed
// a full slot, has already woken the loop through releaseSlotsLocked; every other
// release comes from a revoke or a teardown, which leaves nothing on the
// partition to deliver. The mark is the fallback that bounds the wait for a
// record the handoff did not reach to the loop's next pass.
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
		c.markDirtyLocked(key)
		return
	}
	c.outstanding[key] = remaining
}

// headOutcome is what deliverHead did with the head of a partition's queue.
type headOutcome uint8

const (
	// headHeld: the head stayed, or the queue was empty or stale and dropped.
	headHeld headOutcome = iota
	// headSkipped: the head's offset was already committed, so it left the
	// queue without a delivery or a charge.
	headSkipped
	// headDelivered: the head was charged and sent on Messages.
	headDelivered
	// headStopped: the consumer is stopping or draining and admits nothing.
	headStopped
)

// deliverHead delivers the head record of key's queue when its partition and
// destination admit it. It is the one admission step, and two goroutines run
// it: the poll loop over the partitions a pass visits, and a settling caller
// over its own partition (handOff).
//
// The admission check, the removal of the head and the charge happen in one
// critical section under deliveryMu and c.mu, and that is what keeps a
// partition's order with two admitting goroutines. Were the head removed
// before the admission taking the charge, the interleaving that breaks it is:
// the poll loop removes record 1 and releases c.mu; the settling caller removes
// record 2, which is now the head, and also sees no delivery outstanding; the
// settling caller takes deliveryMu first and delivers record 2 before record 1.
// Here the second caller finds record 1's charge, or record 1 still at the head.
//
// The send on Messages happens under deliveryMu, so a teardown cannot close the
// channel under it, and it does not block: every message in the channel holds
// an admission slot, and the channel is sized to the effective aggregate budget.
func (c *consumer) deliverHead(key partitionKey) headOutcome {
	c.deliveryMu.Lock()
	defer c.deliveryMu.Unlock()
	c.mu.Lock()
	if c.stopped || c.draining {
		c.mu.Unlock()
		return headStopped
	}
	records := c.pending[key]
	if len(records) == 0 {
		delete(c.pending, key)
		c.mu.Unlock()
		return headHeld
	}
	record := records[0]
	if c.isRecordStaleLocked(record) {
		// Every record of the queue belongs to the same partition, and offsets
		// increase along it, so a stale head makes the whole queue stale. Each
		// one leaves the in-hand set with it: a record left there would be read
		// as an offset this consumer still owes a delivery for, and the
		// partition's next tracker would start its cursor below the offset it is
		// delivered from.
		for _, queued := range records {
			delete(c.inHand, queued)
		}
		delete(c.pending, key)
		c.clearHeadHoldLocked(key)
		c.mu.Unlock()
		return headHeld
	}
	if !c.admissionLocked(record) {
		c.mu.Unlock()
		return headHeld
	}
	if len(records) == 1 {
		delete(c.pending, key)
	} else {
		c.pending[key] = records[1:]
	}
	budget := c.budgets[record.Topic]
	if budget <= 0 {
		budget = 1
	}
	tracker := c.trackerForLocked(record)
	reused := c.requeued[key] > 0
	if !reused && tracker.CommitPoint() > record.Offset {
		delete(c.inHand, record)
		c.mu.Unlock()
		return headSkipped
	}
	if reused {
		c.requeued[key]--
		if c.requeued[key] == 0 {
			delete(c.requeued, key)
		}
	}
	settler := &settler{
		owner:   c,
		record:  retainedRecord(record),
		tracker: tracker,
		key:     key,
	}
	c.settlers[settler] = struct{}{}
	delete(c.inHand, record)
	if !reused {
		c.unsettled[record.Topic]++
		c.admitted++
		c.chargeDeliveryLocked(key)
		c.lastAdmitted = key
	}
	if c.unsettled[record.Topic] >= budget {
		c.setPauseReasonLocked(record.Topic, pauseReasonPrefetch, true)
	}
	message := inboundMessage(record, settler, c.currentTime())
	c.mu.Unlock()
	select {
	case <-c.forwarderStopC:
		c.abortSettler(settler)
		return headStopped
	case c.messages <- message:
		return headDelivered
	}
}

// handOff delivers key's next record from the goroutine that just settled the
// partition's delivery, which is the event that made the record admissible.
// The poll loop is not woken for it: waking it cost a cancelled fetch wait and
// an admission pass per settlement, for the one partition this caller already
// knows. An admission the handoff finds refused is one another event releases,
// and that event wakes the loop: a destination slot freed, a pause lifted, a
// held head coming due.
//
// A settlement that freed a full slot does not hand off: other partitions may
// be waiting for that slot, and the handoff would win it every time. Walk the
// race: partition A holds the one slot of a full aggregate or destination
// budget and has a backlog, and partition B's head was refused for want of
// that slot. A's Ack releases the slot and wakes the poll loop, which is
// parked in its fetch wait. The settling goroutine goes straight on to the
// handoff and admits A's next record before the loop has even returned from
// the wait, so the loop's pass finds the slot taken again and B is refused
// again, for as long as A has records. The loop owns that slot instead: its
// pass visits partitions in turn (see lastAdmitted), and the release already
// woke it, so skipping the handoff costs no extra wake.
//
// The read-ahead reason of the partition is reconciled here too, because the
// loop that reconciles it after its passes may stay parked in its fetch wait: a
// partition held at its read-ahead limit whose queue the handoffs drain would
// otherwise stay paused with nothing left to deliver.
func (c *consumer) handOff(key partitionKey) {
	outcome := c.deliverHead(key)
	for outcome == headSkipped {
		outcome = c.deliverHead(key)
	}
	if outcome == headStopped {
		return
	}
	c.mu.Lock()
	c.syncReadAheadPauseLocked(key)
	c.mu.Unlock()
}

func (c *consumer) stopForwarders() {
	c.forwarderStopOnce.Do(func() {
		close(c.forwarderStopC)
	})
}

func (c *consumer) abortSettler(settler *settler) {
	c.mu.Lock()
	delete(c.settlers, settler)
	// Even a redelivery must return its retained charge: this emission never
	// reached a caller, and no remaining settler can release it during drain.
	c.releaseSlotsLocked(settler.record.Topic, 1)
	c.releasePartitionLocked(settler.key, 1)
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
	clear(c.unsettled)
	c.admitted = 0
	clear(c.outstanding)
	clear(c.partitionPauses)
	clear(c.pending)
	clear(c.heldUntil)
	c.syncHeadTimerLocked()
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
// The delivered record is the head of its partition's queue, so it is normally
// that lowest offset. The records the loop has in hand for the partition, and
// the ones still queued behind its head, are what the base is taken from
// instead: a base above a record whose delivery is still owed would report that
// record as already settled and drop it, losing the offset and letting the
// commit pass over it.
func (c *consumer) trackerBaseLocked(key partitionKey, base int64) int64 {
	for record := range c.inHand {
		if record.Topic == key.destination && record.Partition == key.partition && record.Offset < base {
			base = record.Offset
		}
	}
	for _, record := range c.pending[key] {
		if record.Offset < base {
			base = record.Offset
		}
	}
	return base
}

// completeSettlement ends a settler's delivery and releases its charge, unless
// preserveUnsettled keeps the charge for a queued redelivery. It reports
// whether the release freed a slot other partitions may be waiting for, in
// which case the caller leaves the partition's next record to the poll loop
// rather than handing it over (see handOff).
func (c *consumer) completeSettlement(settler *settler, preserveUnsettled bool) (contended bool) {
	c.mu.Lock()
	if preserveUnsettled && (settler.tracker == nil || c.trackers[settler.key] != settler.tracker) {
		preserveUnsettled = false
	}
	settler.settled = true
	c.clearHeadHoldLocked(settler.key)
	if _, exists := c.settlers[settler]; !exists {
		if !preserveUnsettled && !settler.slotReleased {
			contended = c.releaseSlotsLocked(settler.record.Topic, 1)
			c.releasePartitionLocked(settler.key, 1)
			settler.slotReleased = true
		}
		c.signalSettlerDoneLocked()
		c.mu.Unlock()
		return contended
	}
	delete(c.settlers, settler)
	if !preserveUnsettled && !settler.slotReleased {
		contended = c.releaseSlotsLocked(settler.record.Topic, 1)
		c.releasePartitionLocked(settler.key, 1)
		settler.slotReleased = true
	}
	budget := c.budgets[settler.record.Topic]
	if budget > 0 && (preserveUnsettled || c.unsettled[settler.record.Topic] < budget) {
		c.setPauseReasonLocked(settler.record.Topic, pauseReasonPrefetch, false)
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
	return contended
}

// requeueLocked puts the record a settler delivered back at the head of its
// partition's queue. The queue's other records keep their order behind it, so
// the partition redelivers the requeued record before them, and the committed
// cursor does not move: the record's offset is still unsettled, so if this
// consumer leaves before the redelivery settles, the partition's next owner
// redelivers it too. The Nack that requeued hands the redelivery over itself
// (handOff) once it has released its locks.
func (c *consumer) requeueLocked(s *settler) {
	c.requeued[s.key]++
	if c.pending == nil {
		c.pending = make(map[partitionKey][]*kgo.Record)
	}
	c.pending[s.key] = append([]*kgo.Record{s.record}, c.pending[s.key]...)
	c.markDirtyLocked(s.key)
}

// commitOffset commits one partition's cursor through the consumer's
// committer, which sends it in one request with the other partitions' commits
// that are waiting at the same time, and returns once that request finished.
func (c *consumer) commitOffset(ctx context.Context, key partitionKey, commitPoint int64) error {
	send := c.commitFn
	if send == nil {
		// A consumer built as a literal by a test has no seam installed.
		send = c.sendCommits
	}
	return c.committer.commit(ctx, key, commitPoint, send)
}

// sendCommits sends one commit request for every point of a committer round.
// A revoke callback reaches this through commitSettledPrefix, so the client is
// read under c.mu and a nil client fails the round instead of panicking; the
// lock is released before the commit, which is a broker call. The nil branch is
// defensive: the client has one writer and a commit follows an admitted
// delivery, so nothing depends on the kind it carries, and both call sites
// re-classify a commit failure anyway.
func (c *consumer) sendCommits(ctx context.Context, points map[partitionKey]int64) map[partitionKey]error {
	results := make(map[partitionKey]error, len(points))
	fail := func(err error) map[partitionKey]error {
		for key := range points {
			results[key] = err
		}
		return results
	}
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return fail(classify("consumer", driver.KindFatal, errors.New("consumer has no client")))
	}
	offsets := make(map[string]map[int32]kgo.EpochOffset, 1)
	for key, point := range points {
		if offsets[key.destination] == nil {
			offsets[key.destination] = make(map[int32]kgo.EpochOffset)
		}
		offsets[key.destination][key.partition] = kgo.EpochOffset{Epoch: -1, Offset: point}
	}
	c.offsetMu.Lock()
	defer c.offsetMu.Unlock()
	client.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, response *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			fail(err)
			return
		}
		// A partition the response leaves out was not committed; the broker
		// answers every partition of a request, so this is defensive.
		fail(errCommitResponseMissing)
		for _, topic := range response.Topics {
			for _, partition := range topic.Partitions {
				key := partitionKey{destination: topic.Topic, partition: partition.Partition}
				if _, sent := points[key]; sent {
					results[key] = kerr.ErrorForCode(partition.ErrorCode)
				}
			}
		}
	})
	return results
}

// errCommitResponseMissing is the result of a partition an offset commit
// response did not answer.
var errCommitResponseMissing = errors.New("kafka: offset commit response omitted the partition")

// retainedRecord copies the fetch record into memory this consumer owns. A
// settler keeps the copy so a requeue can redeliver the record the broker
// carried: franz-go reuses fetch buffers, and the delivered message is the
// application's copy to change, so neither the fetch buffer nor the message the
// handler held is a sound source for a redelivery.
func retainedRecord(record *kgo.Record) *kgo.Record {
	retained := &kgo.Record{
		Topic:       record.Topic,
		Partition:   record.Partition,
		Offset:      record.Offset,
		LeaderEpoch: record.LeaderEpoch,
		Timestamp:   record.Timestamp,
		Key:         append([]byte(nil), record.Key...),
		Value:       append([]byte(nil), record.Value...),
	}
	if len(record.Headers) > 0 {
		retained.Headers = make([]kgo.RecordHeader, len(record.Headers))
		for index, header := range record.Headers {
			retained.Headers[index] = kgo.RecordHeader{
				Key:   header.Key,
				Value: append([]byte(nil), header.Value...),
			}
		}
	}
	return retained
}

func kafkaEnqueueFields(timestamp time.Time, timestampType int8) (time.Time, driver.EnqueueSource) {
	if timestamp.IsZero() {
		return time.Time{}, driver.EnqueueSourceUnknown
	}
	switch timestampType {
	case 0:
		return timestamp, driver.EnqueueSourceProducer
	case 1:
		return timestamp, driver.EnqueueSourceBroker
	default:
		return time.Time{}, driver.EnqueueSourceUnknown
	}
}

func inboundMessage(record *kgo.Record, settler *settler, receivedAt time.Time) driver.InboundMessage {
	headers := make([]driver.Header, 0, len(record.Headers))
	for _, header := range record.Headers {
		headers = append(headers, driver.Header{Key: header.Key, Value: append([]byte(nil), header.Value...)})
	}
	enqueuedAt, enqueuedAtSource := kafkaEnqueueFields(record.Timestamp, record.Attrs.TimestampType())
	return driver.InboundMessage{
		Destination:      record.Topic,
		Key:              append([]byte(nil), record.Key...),
		Headers:          headers,
		Body:             append([]byte(nil), record.Value...),
		DeliveryCount:    -1,
		ReceivedAt:       receivedAt,
		EnqueuedAt:       enqueuedAt,
		EnqueuedAtSource: enqueuedAtSource,
		Ref: driver.BrokerRef{
			Partition: record.Partition,
			Offset:    record.Offset,
			Raw:       record.Topic + "/" + strconv.FormatInt(int64(record.Partition), 10) + "/" + strconv.FormatInt(record.Offset, 10),
		},
		Settle: settler,
	}
}

// Pause pauses delivery from the listed topics. With no topics, it pauses every topic this consumer subscribes to.
func (c *consumer) Pause(destinations ...string) error {
	return c.setUserPaused(destinations, true)
}

// Resume resumes delivery from the listed topics. With no topics, it resumes every topic this consumer subscribes to.
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
	if add {
		if !reasons.add(reason) {
			return
		}
		if c.client != nil {
			c.client.PauseFetchTopics(destination)
		}
		return
	}
	if !reasons.remove(reason) {
		return
	}
	if reasons.empty() && !c.draining && !c.stopped {
		if c.client != nil {
			c.client.ResumeFetchTopics(destination)
		}
		c.wakePollLocked()
	}
}

// Drain stops fetching new messages while keeping outstanding messages settleable.
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
	// A handoff can have passed admission and released c.mu without sending
	// yet. Stop its blocked send before taking deliveryMu, then fence draining
	// with that send: otherwise Drain can return while the handoff still has
	// both the stop channel and Messages ready and chooses to send.
	c.deliveryMu.Lock()
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		c.deliveryMu.Unlock()
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
	c.deliveryMu.Unlock()
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

// Stop drains the consumer and closes its channels after every delivered message has been settled.
// It returns ErrResourcesOutstanding if deliveries remain unsettled.
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
	c.closeBacklogClient()
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

// Release abandons unsettled deliveries without committing their offsets, then closes the consumer.
func (c *consumer) Release(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("release", driver.KindTransient, err)
	}
	c.stopForwarders()
	c.deliveryMu.Lock()

	c.mu.Lock()
	if c.stopped {
		stopDone := c.stopDone
		c.mu.Unlock()
		c.deliveryMu.Unlock()
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
	c.deliveryMu.Unlock()

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

// Lag returns the backlog count for each topic this consumer subscribes to,
// summed across all of its partitions against the group's committed offsets.
// It is the group's lag, not only this member's share.
func (c *consumer) Lag(ctx context.Context) (map[string]int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("lag", driver.KindTransient, err)
	}
	if !c.effectiveCapabilities().LagQueryable {
		return nil, classify("lag", driver.KindFatal, driver.ErrUnsupported)
	}
	snapshot, err := c.readKafkaOffsetSnapshot(ctx, "lag")
	if err != nil {
		return nil, err
	}
	lag, _, err := snapshot.lagAndProbes("lag")
	if err != nil {
		return nil, err
	}
	return lag, nil
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
		// Make room by evicting the oldest error, unless that one is fatal:
		// the core fails the subscription only on a fatal error, so a
		// merely severe one must not push it out.
		select {
		case evicted := <-c.errors:
			if evictedKind, evictedClassified := driver.Classify(evicted); evictedClassified && evictedKind == driver.KindFatal && kind != driver.KindFatal {
				err = evicted
			}
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
	for _, record := range c.pending[key] {
		delete(c.inHand, record)
	}
	delete(c.pending, key)
	delete(c.heldUntil, key)
	delete(c.partitionPauses, key)
	queuedRequeues := c.requeued[key]
	delete(c.requeued, key)
	c.syncHeadTimerLocked()
	if queuedRequeues > 0 {
		// A queued requeue held its delivery's destination slot and its
		// partition's charge for the redelivery this consumer owed, and the
		// partition is leaving, so nothing here will make that redelivery and the
		// charges have nothing left to hold. The offset is still uncommitted, so
		// the copy the partition's next owner fetches is the redelivery.
		c.releaseSlotsLocked(destination, queuedRequeues)
		c.releasePartitionLocked(key, queuedRequeues)
	}
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

// partitionWarning is one destination this member cannot fill: it holds fewer
// partitions of it than the subscription gives the destination delivery slots,
// so the slots the partitions cannot back stay idle however much work is
// waiting. The count is collected under c.mu and the warning is written after
// c.mu is released.
type partitionWarning struct {
	destination string
	held        int
	budget      int
}

// assignmentWarningsLocked reports the destinations this assignment leaves this
// member short of, once each, and records them as warned. The caller holds c.mu.
//
// The count compared is the partitions this member holds for the destination
// rather than the partitions this callback carried. A cooperative rebalance
// calls the callback once per round and names only the partitions that changed
// hands, so a member's retained partitions are never named again and the
// callback's own payload is a share of the assignment rather than the whole of
// it. Accumulating in c.owned is what makes the count the member's real ceiling.
//
// The budget is the destination's resolved prefetch, which is the same number
// the admission gate charges: a destination with a PerDestination entry uses it,
// and every other destination uses the subscription's prefetch divided across
// the destinations. A destination whose assignment meets its budget is not
// recorded, so a member that grows into its budget on a later rebalance is
// silent then.
//
// A destination named by an assignment that leaves this member short is warned
// on that callback, and the warning is not retracted. For a cooperative
// protocol the first callback of a rebalance can carry a part of the
// destination's final assignment, the partitions no other member held, with the
// rest arriving in a later callback of the same rebalance, so a member whose
// first callback is short and whose second fills its budget is warned about a
// shortfall it does not end up with. Nothing in the callback says whether
// another round follows it, so the choice is between this false positive and
// not warning at all until a round that may never come. It is accepted, and the
// remedy is on the operator's side: a deployment that really did scale past its
// partition count warns on every member on every rebalance, and one that
// rebalanced a partition into place warns once.
func (c *consumer) assignmentWarningsLocked(assigned map[string][]int32) []partitionWarning {
	if c.warned == nil {
		c.warned = make(map[string]bool)
	}
	var warnings []partitionWarning
	for destination := range assigned {
		if c.warned[destination] {
			continue
		}
		budget := c.budgets[destination]
		if budget <= 0 {
			continue
		}
		// A retry-tier destination is fed by the retry ladder rather than by the
		// partition count, and the records on it are ones a handler has to wait
		// out: its slots describe a delay budget, so a short partition count
		// there is not the throughput ceiling this warning is about.
		if c.cfg.Delays[destination] > 0 {
			continue
		}
		held := c.ownedPartitionsLocked(destination)
		if held >= budget {
			continue
		}
		c.warned[destination] = true
		warnings = append(warnings, partitionWarning{destination: destination, held: held, budget: budget})
	}
	return warnings
}

// ownedPartitionsLocked counts the partitions of one destination this consumer
// holds. The caller holds c.mu.
func (c *consumer) ownedPartitionsLocked(destination string) int {
	held := 0
	for key := range c.owned {
		if key.destination == destination {
			held++
		}
	}
	return held
}

// logPartitionWarnings writes one warning per destination this member cannot
// fill. It is called with no lock held. The destination's partition count is the
// ceiling, and the levers an operator has are that count, which re-maps the keys
// already published to the destination, and the instance count the subscription
// was sized for: the lever attribute names the configured one.
func (c *consumer) logPartitionWarnings(warnings []partitionWarning) {
	if c.conn == nil {
		// Unit consumers that exercise assignment accounting without opening a
		// connection have nowhere to send the diagnostic. A live consumer always
		// has conn; keep the zero-value test harness from turning the accounting
		// check into a nil dereference.
		return
	}
	for _, warning := range warnings {
		lever := "destination partition count"
		if floor, err := resolveMaxExpectedInstances(c.conn.driverOptions); err == nil && floor > 0 {
			lever = "broker.kafka.maxExpectedInstances"
		}
		c.conn.log().Warn(
			"Kafka consumer holds fewer partitions than the subscription's slot budget; raising the destination's partition count gives this member more of them and re-maps the keys published to it",
			"destination", warning.destination,
			"assigned_partitions", warning.held,
			"budget", warning.budget,
			"lever", lever,
		)
	}
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
			delete(c.heldUntil, key)
			delete(c.partitionPauses, key)
			c.resumePartitionIfFreeLocked(key)
		}
	}
	warnings := c.assignmentWarningsLocked(partitions)
	c.syncHeadTimerLocked()
	// Ownership changed, which no partition-scoped mark describes.
	c.partialScan = false
	c.mu.Unlock()
	c.assignmentMu.Unlock()
	// The log is written with both locks released: the handler is the caller's,
	// so it may block, and c.mu is the lock the poll loop and every settler of
	// this consumer need.
	c.logPartitionWarnings(warnings)
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
	c.assignmentMu.Lock()
	c.mu.Lock()
	for topic, partitionList := range partitions {
		for _, partition := range partitionList {
			key := partitionKey{destination: topic, partition: partition}
			c.setPartitionPauseReasonLocked(key, partitionPauseHeadHold, true)
		}
	}
	c.mu.Unlock()
	revoked := c.unown(partitions)
	c.mu.Lock()
	// The next pass visits every partition, so it drops the queues of the
	// partitions this callback took away whatever woke it.
	c.partialScan = false
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
	c.mu.Lock()
	for topic, partitionList := range partitions {
		for _, partition := range partitionList {
			key := partitionKey{destination: topic, partition: partition}
			c.setPartitionPauseReasonLocked(key, partitionPauseHeadHold, true)
		}
	}
	c.mu.Unlock()
	revoked := c.unown(partitions)
	c.mu.Lock()
	// The next pass visits every partition, so it drops the queues of the
	// partitions this callback took away whatever woke it.
	c.partialScan = false
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
