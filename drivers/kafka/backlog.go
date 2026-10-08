package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

var _ driver.BacklogReader = (*consumer)(nil)

type kafkaOffsetSnapshot struct {
	destinations []string
	starts       kadm.ListedOffsets
	ends         kadm.ListedOffsets
	committed    kadm.OffsetResponses
}

type backlogPartitionProbe struct {
	destination string
	partition   int32
	lag         int64
	committed   int64
	fetches     kgo.Fetches
}

func (c *consumer) readKafkaOffsetSnapshot(ctx context.Context, operation string) (kafkaOffsetSnapshot, error) {
	c.mu.Lock()
	destinations := append([]string(nil), c.destinations...)
	group := c.group
	c.mu.Unlock()

	admin := kadm.NewClient(c.conn.client)
	starts, err := listKafkaOffsets(ctx, func(listCtx context.Context) (kadm.ListedOffsets, error) {
		return admin.ListStartOffsets(listCtx, destinations...)
	})
	if err != nil {
		return kafkaOffsetSnapshot{}, classifyKafkaOffsetError(operation, err)
	}
	ends, err := listKafkaOffsets(ctx, func(listCtx context.Context) (kadm.ListedOffsets, error) {
		return admin.ListEndOffsets(listCtx, destinations...)
	})
	if err != nil {
		return kafkaOffsetSnapshot{}, classifyKafkaOffsetError(operation, err)
	}
	committed, err := admin.FetchOffsets(ctx, group)
	if err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
		return kafkaOffsetSnapshot{}, classify(operation, kafkaErrorKind(err), err)
	}
	return kafkaOffsetSnapshot{
		destinations: destinations,
		starts:       starts,
		ends:         ends,
		committed:    committed,
	}, nil
}

func (s kafkaOffsetSnapshot) lagAndProbes(operation string) (map[string]int64, map[string][]backlogPartitionProbe, error) {
	lag := make(map[string]int64, len(s.destinations))
	probes := make(map[string][]backlogPartitionProbe, len(s.destinations))
	for _, destination := range s.destinations {
		partitions, ok := s.ends[destination]
		if !ok {
			return nil, nil, classify(operation, driver.KindNotFound, fmt.Errorf("destination %q is missing: %w", destination, driver.ErrDestinationMissing))
		}
		var total int64
		for partition, end := range partitions {
			start, ok := s.starts.Lookup(destination, partition)
			if !ok {
				return nil, nil, classify(operation, driver.KindNotFound, fmt.Errorf("destination %q partition %d has no start offset", destination, partition))
			}
			committedOffset := start.Offset
			if response, exists := s.committed.Lookup(destination, partition); exists {
				if response.Err != nil {
					return nil, nil, classifyKafkaOffsetError(operation, response.Err)
				}
				if response.At >= 0 {
					committedOffset = response.At
				}
			}
			if end.Offset > committedOffset {
				total += end.Offset - committedOffset
				probes[destination] = append(probes[destination], backlogPartitionProbe{
					destination: destination,
					partition:   partition,
					lag:         end.Offset - committedOffset,
					committed:   committedOffset,
				})
			}
		}
		lag[destination] = total
	}
	return lag, probes, nil
}

func selectKafkaBacklogHead(partitions []backlogPartitionProbe) (time.Time, driver.EnqueueSource, bool) {
	return selectKafkaBacklogHeadFrom(kafkaBacklogPartitionIndex(partitions), partitions)
}

// selectKafkaBacklogHeadFrom is selectKafkaBacklogHead over an index already
// built, so one backlog read indexes its fetch batch once for every
// destination.
func selectKafkaBacklogHeadFrom(index map[partitionKey]*kgo.FetchPartition, partitions []backlogPartitionProbe) (time.Time, driver.EnqueueSource, bool) {
	var (
		head       time.Time
		headSource driver.EnqueueSource
		found      bool
	)
	for _, partition := range partitions {
		if partition.lag == 0 {
			continue
		}
		record, ok := kafkaBacklogPartitionHead(index, partition)
		if !ok {
			return time.Time{}, driver.EnqueueSourceUnknown, false
		}
		enqueuedAt, source := kafkaEnqueueFields(record.Timestamp, record.Attrs.TimestampType())
		if enqueuedAt.IsZero() {
			return time.Time{}, driver.EnqueueSourceUnknown, false
		}
		if !found || enqueuedAt.Before(head) {
			head = enqueuedAt
			headSource = source
			found = true
		}
	}
	return head, headSource, found
}

// kafkaBacklogPartitionIndex maps a partition of the probes' fetch batches to
// the fetched partition that carried it. Every probe of one backlog read is
// probed with the same batch, and a batch carries an entry for each of the
// partitions it was asked about, so the batches are indexed once each and every
// probe is then served by a lookup: that replaces one walk of the whole batch
// per probe. Where a batch carries a partition under more than one entry, the
// index keeps the one a walk of the batch reaches first, which is the entry
// such a walk answers from.
func kafkaBacklogPartitionIndex(partitions []backlogPartitionProbe) map[partitionKey]*kgo.FetchPartition {
	index := make(map[partitionKey]*kgo.FetchPartition, len(partitions))
	walked := make(map[*kgo.Fetch]struct{}, len(partitions))
	for _, partition := range partitions {
		if len(partition.fetches) == 0 {
			continue
		}
		batch := &partition.fetches[0]
		if _, done := walked[batch]; done {
			continue
		}
		walked[batch] = struct{}{}
		for fetchIndex := range partition.fetches {
			fetch := &partition.fetches[fetchIndex]
			for topicIndex := range fetch.Topics {
				topic := &fetch.Topics[topicIndex]
				for partitionIndex := range topic.Partitions {
					fetched := &topic.Partitions[partitionIndex]
					key := partitionKey{destination: topic.Topic, partition: fetched.Partition}
					if _, indexed := index[key]; !indexed {
						index[key] = fetched
					}
				}
			}
		}
	}
	return index
}

// kafkaBacklogPartitionHead returns the first record the indexed batch carried
// for the probe's partition at or above the committed offset, skipping control
// records. It reports false for every case a head is not knowable from the
// batch: the batch carried no entry for the partition, the entry is an error,
// or every record it carried is below the committed offset or a control record.
func kafkaBacklogPartitionHead(index map[partitionKey]*kgo.FetchPartition, partition backlogPartitionProbe) (*kgo.Record, bool) {
	fetched, indexed := index[partitionKey{destination: partition.destination, partition: partition.partition}]
	if !indexed || fetched.Err != nil {
		return nil, false
	}
	for _, record := range fetched.Records {
		if record.Offset < partition.committed || record.Attrs.IsControl() {
			continue
		}
		return record, true
	}
	return nil, false
}

// Backlog returns lag and, when available, the enqueue time of the oldest unread record for each subscribed topic.
func (c *consumer) Backlog(ctx context.Context) (map[string]driver.BacklogSample, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("backlog", driver.KindTransient, err)
	}
	if !c.effectiveCapabilities().LagQueryable {
		return nil, classify("backlog", driver.KindFatal, driver.ErrUnsupported)
	}
	snapshot, err := c.readKafkaOffsetSnapshot(ctx, "backlog")
	if err != nil {
		return nil, err
	}
	lag, probes, err := snapshot.lagAndProbes("backlog")
	if err != nil {
		return nil, err
	}
	fetches := c.probeBacklog(ctx, probes)
	index := kafkaBacklogPartitionIndex([]backlogPartitionProbe{{fetches: fetches}})

	out := make(map[string]driver.BacklogSample, len(lag))
	for _, destination := range snapshot.destinations {
		sample := driver.BacklogSample{Lag: lag[destination]}
		if sample.Lag > 0 {
			sample.HeadEnqueuedAt, sample.HeadSource, _ = selectKafkaBacklogHeadFrom(index, probes[destination])
		}
		out[destination] = sample
	}
	return out, nil
}

func (c *consumer) probeBacklog(ctx context.Context, probes map[string][]backlogPartitionProbe) kgo.Fetches {
	partitions := make(map[string]map[int32]kgo.Offset)
	for destination, destinationProbes := range probes {
		for _, probe := range destinationProbes {
			if partitions[destination] == nil {
				partitions[destination] = make(map[int32]kgo.Offset)
			}
			partitions[destination][probe.partition] = kgo.NewOffset().At(probe.committed)
		}
	}
	if len(partitions) == 0 {
		return nil
	}
	probeCtx, cancelProbe := context.WithCancel(ctx)
	stopOnConsumerClose := context.AfterFunc(c.pollCtx, cancelProbe) //nolint:contextcheck // the consumer-owned context interrupts probes during teardown
	defer func() {
		stopOnConsumerClose()
		cancelProbe()
	}()

	select {
	case c.backlogProbeToken <- struct{}{}:
	case <-probeCtx.Done():
		return nil
	}
	defer func() { <-c.backlogProbeToken }()
	// Ownership can arrive at the same instant the caller's context ends, and a
	// select picks at random between ready cases, so the context is checked
	// again here: a caller that has given up must not start a probe, and must
	// not leave one behind for the next caller to wait on.
	if probeCtx.Err() != nil {
		return nil
	}
	c.mu.Lock()
	stopped := c.stopped
	c.mu.Unlock()
	if stopped {
		return nil
	}
	c.backlogMu.Lock()
	if c.backlogClient == nil {
		opts := append([]kgo.Opt(nil), c.conn.clientOpts...)
		opts = append(opts,
			kgo.ConsumePartitions(partitions),
			kgo.KeepControlRecords(),
			kgo.MaxConcurrentFetches(0),
		)
		client, err := kgo.NewClient(opts...)
		if err != nil {
			c.backlogMu.Unlock()
			return nil
		}
		c.backlogClient = client
	} else {
		c.backlogClient.RemoveConsumePartitions(c.backlogPartitions)
		c.backlogClient.AddConsumePartitions(partitions)
	}
	c.backlogPartitions = make(map[string][]int32, len(partitions))
	for destination, destinationPartitions := range partitions {
		for partition := range destinationPartitions {
			c.backlogPartitions[destination] = append(c.backlogPartitions[destination], partition)
		}
	}
	client := c.backlogClient
	c.backlogMu.Unlock()

	wanted := make(map[partitionKey]struct{})
	for destination, destinationProbes := range probes {
		for _, probe := range destinationProbes {
			wanted[partitionKey{destination: destination, partition: probe.partition}] = struct{}{}
		}
	}
	return pollKafkaBacklog(probeCtx, client, wanted)
}

type kafkaBacklogFetcher interface {
	PollFetches(context.Context) kgo.Fetches
}

func pollKafkaBacklog(ctx context.Context, client kafkaBacklogFetcher, wanted map[partitionKey]struct{}) kgo.Fetches {
	var fetches kgo.Fetches
	seen := make(map[partitionKey]struct{}, len(wanted))
	for len(seen) < len(wanted) && ctx.Err() == nil {
		batch := client.PollFetches(ctx)
		fetches = append(fetches, batch...)
		// A failed direct partition can remain non-empty on every poll, so
		// retrying after PollFetches reports an error can wait forever.
		if len(batch.Errors()) != 0 {
			break
		}
		for _, fetch := range batch {
			for _, topic := range fetch.Topics {
				for _, partition := range topic.Partitions {
					if partition.Err != nil {
						continue
					}
					key := partitionKey{destination: topic.Topic, partition: partition.Partition}
					if _, ok := wanted[key]; ok {
						seen[key] = struct{}{}
					}
				}
			}
		}
		if len(batch) == 0 {
			break
		}
	}
	return fetches
}

func (c *consumer) closeBacklogClient() {
	// A consumer built as a literal has no token and never probed, so there is
	// no owner to wait for; a nil channel send is never ready.
	if token := c.backlogProbeToken; token != nil {
		// This send has no context escape of its own: it ends because the Stop
		// or Release that reached teardown cancelled pollCtx first, which ends
		// the owning probe's probeCtx through probeBacklog's AfterFunc and makes
		// that probe return the token.
		token <- struct{}{}
		defer func() { <-token }()
	}
	c.backlogMu.Lock()
	client := c.backlogClient
	c.backlogClient = nil
	c.backlogPartitions = nil
	c.backlogMu.Unlock()
	if client != nil {
		client.Close()
	}
}
