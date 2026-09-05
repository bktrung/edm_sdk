package kafka

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type pauseReason string

const (
	pauseReasonDeferred   pauseReason = "deferred"
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

func (s pauseReasonSet) permitsRedelivery() bool {
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

type consumer struct {
	conn               *conn
	client             *kgo.Client
	cfg                driver.ConsumerConfig
	group              string
	synthesizedGroup   bool
	destinations       []string
	budgets            map[string]int
	messages           chan driver.InboundMessage
	errors             chan error
	errorsMu           sync.Mutex
	pollCtx            context.Context
	cancelPoll         context.CancelFunc
	pollDone           chan struct{}
	stopDone           chan struct{}
	pauseReasons       map[string]pauseReasonSet
	unsettled          map[string]int
	settlers           map[*settler]struct{}
	trackers           map[partitionKey]*ackTracker
	trackerGenerations map[partitionKey]uint64
	requeued           map[partitionKey]int
	discarded          map[partitionKey]map[int64]struct{}
	maxAckGap          int64
	offsetMu           sync.Mutex
	assignmentMu       sync.Mutex
	mu                 sync.Mutex
	draining           bool
	stopped            bool
}

type settler struct {
	owner   *consumer
	record  *kgo.Record
	tracker *ackTracker
	key     partitionKey
	mu      sync.Mutex
	settled bool
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
	s.owner.mu.Unlock()
	if tracker == nil {
		return classify("ack", driver.KindFatal, ErrRevoked)
	}

	if err := tracker.Ack(s.record.Offset, func(commitPoint int64) error {
		return s.owner.commitOffset(ctx, s.key, commitPoint)
	}); err != nil {
		if !errors.Is(err, ErrRevoked) && !errors.Is(err, errAckTrackerAlreadySettled) {
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
	s.owner.mu.Unlock()
	if tracker == nil {
		return classify("nack", driver.KindFatal, ErrRevoked)
	}

	if options.Requeue {
		s.owner.mu.Lock()
		if err := tracker.Release(s.record.Offset); err != nil {
			s.owner.mu.Unlock()
			return classifySettlement("nack", err)
		}
		s.owner.requeued[s.key]++
		s.owner.mu.Unlock()
		resetOffset := s.record.Offset
		if lowest, ok := tracker.lowestRequeue(); ok {
			resetOffset = lowest
		}
		if !s.owner.resetOffset(s.key, tracker, resetOffset) {
			return classifySettlement("nack", ErrRevoked)
		}
		s.owner.resumeForRedelivery(s.record.Topic)
		s.owner.completeSettlement(s, true)
		return nil
	}

	if err := tracker.Ack(s.record.Offset, func(commitPoint int64) error {
		return s.owner.commitOffset(ctx, s.key, commitPoint)
	}); err != nil {
		if !errors.Is(err, ErrRevoked) && !errors.Is(err, errAckTrackerAlreadySettled) {
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
	_ driver.Consumer = (*consumer)(nil)
	_ driver.Settler  = (*settler)(nil)
)

func newConsumer(ctx context.Context, connection *conn, cfg driver.ConsumerConfig) (*consumer, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("consumer", driver.KindTransient, err)
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

	pollCtx, cancelPoll := context.WithCancel(context.Background())
	consumer := &consumer{
		conn:               connection,
		cfg:                cfg,
		group:              group,
		synthesizedGroup:   synthesized,
		destinations:       append([]string(nil), cfg.Destinations...),
		budgets:            make(map[string]int, len(cfg.Destinations)),
		messages:           make(chan driver.InboundMessage, totalPrefetch(cfg)),
		errors:             make(chan error, 8),
		pollCtx:            pollCtx,
		cancelPoll:         cancelPoll,
		pollDone:           make(chan struct{}),
		stopDone:           make(chan struct{}),
		pauseReasons:       make(map[string]pauseReasonSet, len(cfg.Destinations)),
		unsettled:          make(map[string]int, len(cfg.Destinations)),
		settlers:           make(map[*settler]struct{}),
		trackers:           make(map[partitionKey]*ackTracker),
		trackerGenerations: make(map[partitionKey]uint64),
		requeued:           make(map[partitionKey]int),
		discarded:          make(map[partitionKey]map[int64]struct{}),
		maxAckGap:          maxAckGap,
	}
	for index, destination := range cfg.Destinations {
		consumer.budgets[destination] = destinationPrefetch(cfg, destination, index)
	}

	opts := append([]kgo.Opt(nil), connection.clientOpts...)
	opts = append(opts,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(cfg.Destinations...),
		kgo.DisableAutoCommit(),
		consumerStartOffset(cfg.StartAt),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
			consumer.sendRebalanceError("revoked", partitions)
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
			consumer.sendRebalanceError("lost", partitions)
		}),
	)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		cancelPoll()
		return nil, classify("consumer", driver.KindFatal, err)
	}
	consumer.client = client
	connection.registerConsumer(consumer)
	go consumer.poll()
	return consumer, nil
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

func (c *consumer) poll() {
	defer close(c.pollDone)
	pending := make([]*kgo.Record, 0, 1)
	for {
		if !c.flushPending(&pending) {
			return
		}

		fetches := c.client.PollRecords(c.pollCtx, 1)
		if c.pollCtx.Err() != nil || fetches.IsClientClosed() {
			return
		}
		for _, fetchErr := range fetches.Errors() {
			kind := kafkaErrorKind(fetchErr.Err)
			if kind != driver.KindTransient {
				// A non-retryable fetch error cannot recover by polling again.
				// Reclassify it as fatal for this subscription so the core
				// cancels its fetch runner while retaining the Kafka cause.
				kind = driver.KindFatal
			}
			c.sendError(classify("consumer", kind, fmt.Errorf("kafka fetch %s[%d]: %w", fetchErr.Topic, fetchErr.Partition, fetchErr.Err)))
			if kind == driver.KindFatal {
				return
			}
		}
		for record := range fetches.RecordsAll() {
			if !c.canDeliver(record) {
				pending = append(pending, record)
				continue
			}
			delivered, active := c.emit(record)
			if !active {
				return
			}
			if !delivered {
				pending = append(pending, record)
			}
		}
	}
}

func (c *consumer) flushPending(pending *[]*kgo.Record) bool {
	for len(*pending) > 0 {
		records := *pending
		index := -1
		for i, record := range records {
			if c.canDeliver(record) {
				index = i
				break
			}
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
	destination := record.Topic
	key := partitionKey{destination: destination, partition: record.Partition}
	budget := c.budgets[destination]
	if budget <= 0 {
		budget = 1
	}
	reasons := c.pauseReasons[destination]
	tracker := c.trackers[key]
	hasRequeue := tracker != nil && tracker.hasRequeue(record.Offset)
	if c.unsettled[destination] >= budget && !hasRequeue {
		c.setPauseReasonLocked(destination, pauseReasonPrefetch, true)
		reasons = c.pauseReasons[destination]
	}
	return (reasons.empty() || (hasRequeue && reasons.permitsRedelivery())) &&
		(c.unsettled[destination] < budget || hasRequeue)
}

func (c *consumer) emit(record *kgo.Record) (delivered, active bool) {
	c.mu.Lock()
	if c.stopped || c.draining {
		c.mu.Unlock()
		return false, false
	}
	budget := c.budgets[record.Topic]
	if budget <= 0 {
		budget = 1
	}
	key := partitionKey{destination: record.Topic, partition: record.Partition}
	tracker := c.trackers[key]
	hasRequeue := tracker != nil && tracker.hasRequeue(record.Offset)
	reasons := c.pauseReasons[record.Topic]
	if (!reasons.empty() && (!hasRequeue || !reasons.permitsRedelivery())) ||
		(c.unsettled[record.Topic] >= budget && !hasRequeue) {
		if c.unsettled[record.Topic] >= budget && !hasRequeue {
			c.setPauseReasonLocked(record.Topic, pauseReasonPrefetch, true)
		}
		c.mu.Unlock()
		return false, true
	}
	if tracker == nil {
		tracker = c.trackerForLocked(record)
	}
	reused, deliver, err := tracker.TrackRedelivery(record.Offset)
	if err != nil {
		c.mu.Unlock()
		return false, true
	}
	if !deliver {
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
	if !reused {
		c.unsettled[record.Topic]++
	}
	if c.unsettled[record.Topic] >= budget {
		c.setPauseReasonLocked(record.Topic, pauseReasonPrefetch, true)
	}
	message := inboundMessage(record, settler)
	c.mu.Unlock()

	c.messages <- message
	return true, true
}

func (c *consumer) trackerForLocked(record *kgo.Record) *ackTracker {
	key := partitionKey{destination: record.Topic, partition: record.Partition}
	if tracker := c.trackers[key]; tracker != nil {
		return tracker
	}
	if c.trackers == nil {
		c.trackers = make(map[partitionKey]*ackTracker)
	}
	if c.trackerGenerations == nil {
		c.trackerGenerations = make(map[partitionKey]uint64)
	}
	generation := c.trackerGenerations[key] + 1
	c.trackerGenerations[key] = generation
	tracker := newAckTracker(record.Offset, generation)
	c.trackers[key] = tracker
	return tracker
}

func (c *consumer) dropTracker(destination string, partition int32) {
	key := partitionKey{destination: destination, partition: partition}
	c.assignmentMu.Lock()
	defer c.assignmentMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	tracker := c.trackers[key]
	if tracker == nil {
		return
	}
	tracker.Drop()
	pending := tracker.Unacked()
	delete(c.trackers, key)
	delete(c.requeued, key)
	delete(c.discarded, key)
	for settler := range c.settlers {
		if settler.tracker != tracker {
			continue
		}
		if !tracker.holds(settler.record.Offset) {
			pending++
		}
		delete(c.settlers, settler)
	}
	if pending > c.unsettled[destination] {
		pending = c.unsettled[destination]
	}
	c.unsettled[destination] -= pending
	budget := c.budgets[destination]
	if budget > 0 && c.unsettled[destination] < budget {
		c.setPauseReasonLocked(destination, pauseReasonPrefetch, false)
	}
	c.refreshAckGapLocked(destination)
}

func (c *consumer) completeSettlement(settler *settler, preserveUnsettled bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	settler.settled = true
	if c.trackers[settler.key] == settler.tracker {
		c.reconcileDiscardedLocked(settler.key, settler.tracker)
	}
	if _, exists := c.settlers[settler]; !exists {
		c.refreshAckGapLocked(settler.record.Topic)
		return
	}
	delete(c.settlers, settler)
	if !preserveUnsettled && c.unsettled[settler.record.Topic] > 0 {
		c.unsettled[settler.record.Topic]--
	}
	budget := c.budgets[settler.record.Topic]
	if budget > 0 && (preserveUnsettled || c.unsettled[settler.record.Topic] < budget) {
		c.setPauseReasonLocked(settler.record.Topic, pauseReasonPrefetch, false)
	}
	c.refreshAckGapLocked(settler.record.Topic)
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
	receivedAt := record.Timestamp
	if receivedAt.IsZero() {
		receivedAt = time.Now() //nolint:forbidigo // the port requires receipt time and drivers have no clock dependency
	}
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
		c.setPauseReasonLocked(destination, pauseReasonUserPaused, paused)
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
	if add {
		if !reasons.add(reason) {
			return
		}
		if len(reasons) == 1 {
			c.client.PauseFetchTopics(destination)
		}
	} else {
		if !reasons.remove(reason) {
			return
		}
		if len(reasons) == 0 && !c.draining && !c.stopped {
			c.client.ResumeFetchTopics(destination)
		}
	}
}

func (c *consumer) Drain(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("drain", driver.KindTransient, err)
	}
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
	return c.waitPoll(ctx, "drain", pollDone)
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

	if err := c.client.LeaveGroupContext(ctx); err != nil {
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

	c.client.Close()
	c.conn.removeConsumer(c)
	if c.synthesizedGroup {
		if err := c.deleteGroup(ctx); err != nil {
			c.sendError(err)
		}
	}
	close(c.messages)
	close(c.errors)
	close(c.stopDone)
	return nil
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
	c.mu.Lock()
	stopped := c.stopped
	c.mu.Unlock()
	if stopped {
		return nil
	}
	return classify("release", driver.KindFatal, driver.ErrUnsupported)
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
		timer := time.NewTimer(50 * time.Millisecond) //nolint:forbidigo // Kafka metadata propagation needs a bounded retry
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

func (c *conn) registerConsumer(csm *consumer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.consumers == nil {
		c.consumers = make(map[*consumer]struct{})
	}
	c.consumers[csm] = struct{}{}
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
	if classified && kind != driver.KindTransient {
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
	c.sendError(classify("consumer", driver.KindTransient, fmt.Errorf("kafka partitions %s: %v", event, partitions)))
}
