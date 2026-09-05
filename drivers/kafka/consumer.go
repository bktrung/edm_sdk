package kafka

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type pauseReason string

const (
	pauseReasonDeferred   pauseReason = "deferred"
	pauseReasonPrefetch   pauseReason = "prefetch-full"
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

type consumer struct {
	conn             *conn
	client           *kgo.Client
	cfg              driver.ConsumerConfig
	group            string
	synthesizedGroup bool
	destinations     []string
	budgets          map[string]int
	messages         chan driver.InboundMessage
	errors           chan error
	errorsMu         sync.Mutex
	pollCtx          context.Context
	cancelPoll       context.CancelFunc
	pollDone         chan struct{}
	stopDone         chan struct{}
	pauseReasons     map[string]pauseReasonSet
	unsettled        map[string]int
	settlers         map[*settler]struct{}
	mu               sync.Mutex
	draining         bool
	stopped          bool
}

type settler struct {
	owner   *consumer
	record  *kgo.Record
	mu      sync.Mutex
	settled bool
}

// Ack commits the record immediately and releases its destination prefetch slot.
// This provisional settler does not preserve a contiguous committed prefix, so
// out-of-order acknowledgements can over-commit.
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
	s.owner.mu.Unlock()

	if err := s.owner.client.CommitRecords(ctx, s.record); err != nil {
		return classify("ack", kafkaErrorKind(err), err)
	}

	s.owner.mu.Lock()
	s.settled = true
	delete(s.owner.settlers, s)
	if s.owner.unsettled[s.record.Topic] > 0 {
		s.owner.unsettled[s.record.Topic]--
	}
	budget := s.owner.budgets[s.record.Topic]
	if budget > 0 && s.owner.unsettled[s.record.Topic] < budget {
		s.owner.setPauseReasonLocked(s.record.Topic, pauseReasonPrefetch, false)
	}
	s.owner.mu.Unlock()
	return nil
}

// Nack reports that Kafka settlement through this provisional consumer is not
// implemented. Callers receive a classified unsupported error and the delivery
// remains outstanding. A canceled context still returns a transient error.
func (s *settler) Nack(ctx context.Context, _ driver.NackOptions) error {
	if err := ctx.Err(); err != nil {
		return classify("nack", driver.KindTransient, err)
	}
	return classify("nack", driver.KindFatal, driver.ErrUnsupported)
}

var (
	_ driver.Consumer = (*consumer)(nil)
	_ driver.Settler  = (*settler)(nil)
)

func newConsumer(ctx context.Context, connection *conn, cfg driver.ConsumerConfig) (*consumer, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("consumer", driver.KindTransient, err)
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
		conn:             connection,
		cfg:              cfg,
		group:            group,
		synthesizedGroup: synthesized,
		destinations:     append([]string(nil), cfg.Destinations...),
		budgets:          make(map[string]int, len(cfg.Destinations)),
		messages:         make(chan driver.InboundMessage, totalPrefetch(cfg)),
		errors:           make(chan error, 8),
		pollCtx:          pollCtx,
		cancelPoll:       cancelPoll,
		pollDone:         make(chan struct{}),
		stopDone:         make(chan struct{}),
		pauseReasons:     make(map[string]pauseReasonSet, len(cfg.Destinations)),
		unsettled:        make(map[string]int, len(cfg.Destinations)),
		settlers:         make(map[*settler]struct{}),
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
			if !c.canDeliver(record.Topic) {
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
			if c.canDeliver(record.Topic) {
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

func (c *consumer) canDeliver(destination string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped || c.draining {
		return false
	}
	budget := c.budgets[destination]
	if budget <= 0 {
		budget = 1
	}
	reasons := c.pauseReasons[destination]
	if c.unsettled[destination] >= budget {
		c.setPauseReasonLocked(destination, pauseReasonPrefetch, true)
		reasons = c.pauseReasons[destination]
	}
	return reasons.empty() && c.unsettled[destination] < budget
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
	if !c.pauseReasons[record.Topic].empty() || c.unsettled[record.Topic] >= budget {
		if c.unsettled[record.Topic] >= budget {
			c.setPauseReasonLocked(record.Topic, pauseReasonPrefetch, true)
		}
		c.mu.Unlock()
		return false, true
	}
	settler := &settler{
		owner: c,
		record: &kgo.Record{
			Topic:       record.Topic,
			Partition:   record.Partition,
			Offset:      record.Offset,
			LeaderEpoch: record.LeaderEpoch,
		},
	}
	c.settlers[settler] = struct{}{}
	c.unsettled[record.Topic]++
	if c.unsettled[record.Topic] >= budget {
		c.setPauseReasonLocked(record.Topic, pauseReasonPrefetch, true)
	}
	message := inboundMessage(record, settler)
	c.mu.Unlock()

	c.messages <- message
	return true, true
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
