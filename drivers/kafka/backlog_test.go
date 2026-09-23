package kafka

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestKafkaBacklogHeadSelection(t *testing.T) {
	t.Parallel()
	oldest := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	newer := oldest.Add(time.Second)

	tests := []struct {
		name       string
		partitions []backlogPartitionProbe
		wantAt     time.Time
		wantSource driver.EnqueueSource
		wantKnown  bool
	}{
		{
			name: "skip records below committed offset",
			partitions: []backlogPartitionProbe{{
				destination: "orders",
				partition:   0,
				lag:         2,
				committed:   5,
				fetches: kafkaTestFetches(t, "orders", 0, 5,
					kafkaTestRecord(t, "orders", 0, 4, oldest.Add(-time.Second), 0, false),
					kafkaTestRecord(t, "orders", 0, 5, oldest.Add(-500*time.Millisecond), 0, true),
					kafkaTestRecord(t, "orders", 0, 6, oldest, 0, false),
				),
			}},
			wantAt:     oldest,
			wantSource: driver.EnqueueSourceProducer,
			wantKnown:  true,
		},
		{
			name: "oldest across partitions",
			partitions: []backlogPartitionProbe{
				{
					destination: "orders",
					partition:   0,
					lag:         1,
					committed:   0,
					fetches: kafkaTestFetches(t, "orders", 0, 0,
						kafkaTestRecord(t, "orders", 0, 0, newer, 0, false)),
				},
				{
					destination: "orders",
					partition:   1,
					lag:         1,
					committed:   0,
					fetches: kafkaTestFetches(t, "orders", 1, 0,
						kafkaTestRecord(t, "orders", 1, 0, oldest, 1, false)),
				},
			},
			wantAt:     oldest,
			wantSource: driver.EnqueueSourceBroker,
			wantKnown:  true,
		},
		{
			name: "unreadable partition makes head unknown",
			partitions: []backlogPartitionProbe{
				{
					destination: "orders",
					partition:   0,
					lag:         1,
					committed:   0,
					fetches: kafkaTestFetches(t, "orders", 0, 0,
						kafkaTestRecord(t, "orders", 0, 0, oldest, 0, false)),
				},
				{
					destination: "orders",
					partition:   1,
					lag:         1,
					committed:   0,
					fetches:     kafkaTestFetchError("orders", 1, errKafkaBacklogProbe),
				},
			},
			wantKnown: false,
		},
		{
			name: "zero lag does not probe",
			partitions: []backlogPartitionProbe{{
				destination: "orders",
				partition:   0,
				fetches:     nil,
			}},
			wantKnown: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotAt, gotSource, gotKnown := selectKafkaBacklogHead(test.partitions)
			if gotKnown != test.wantKnown {
				t.Fatalf("known = %t, want %t", gotKnown, test.wantKnown)
			}
			if !test.wantKnown {
				if !gotAt.IsZero() || gotSource != driver.EnqueueSourceUnknown {
					t.Fatalf("unknown head = %v/%v, want zero/unknown", gotAt, gotSource)
				}
				return
			}
			if !gotAt.Equal(test.wantAt) {
				t.Fatalf("head = %v, want %v", gotAt, test.wantAt)
			}
			if gotSource != test.wantSource {
				t.Fatalf("source = %v, want %v", gotSource, test.wantSource)
			}
		})
	}
}

// TestKafkaBacklogHeadSelectionSharesOneBatch pins the selection over the shape
// a real probe has: every partition of one read is probed with the same fetch
// batch, which can carry several destinations, so a partition entry has to stay
// reachable through the probes that follow the one that walked it and it has to
// stay the entry of its own destination. The head is the oldest unread record
// across all of them.
func TestKafkaBacklogHeadSelectionSharesOneBatch(t *testing.T) {
	t.Parallel()
	oldest := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	newer := oldest.Add(time.Second)

	batch := append(
		kafkaTestFetches(t, "orders", 0, 0,
			kafkaTestRecord(t, "orders", 0, 0, newer, 0, false)),
		kafkaTestFetches(t, "events", 0, 0,
			kafkaTestRecord(t, "events", 0, 0, oldest, 1, false))...,
	)
	probes := []backlogPartitionProbe{
		{destination: "orders", partition: 0, lag: 1, committed: 0, fetches: batch},
		{destination: "events", partition: 0, lag: 1, committed: 0, fetches: batch},
	}

	gotAt, gotSource, gotKnown := selectKafkaBacklogHead(probes)
	if !gotKnown {
		t.Fatal("shared-batch head is unknown")
	}
	if !gotAt.Equal(oldest) || gotSource != driver.EnqueueSourceBroker {
		t.Fatalf("head = %v/%v, want %v/%v", gotAt, gotSource, oldest, driver.EnqueueSourceBroker)
	}
}

// TestKafkaBacklogHeadSelectionTakesTheFirstPartitionEntry pins which entry of
// a batch answers when the batch carries the same partition more than once: the
// one a walk of the batch reaches first, so the second entry never overrides
// the head the first one establishes.
func TestKafkaBacklogHeadSelectionTakesTheFirstPartitionEntry(t *testing.T) {
	t.Parallel()
	first := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	second := first.Add(time.Second)

	batch := append(
		kafkaTestFetches(t, "orders", 0, 0,
			kafkaTestRecord(t, "orders", 0, 0, first, 0, false)),
		kafkaTestFetches(t, "orders", 0, 0,
			kafkaTestRecord(t, "orders", 0, 0, second, 0, false))...,
	)
	probes := []backlogPartitionProbe{{destination: "orders", partition: 0, lag: 1, committed: 0, fetches: batch}}

	gotAt, _, gotKnown := selectKafkaBacklogHead(probes)
	if !gotKnown {
		t.Fatal("head is unknown")
	}
	if !gotAt.Equal(first) {
		t.Fatalf("head = %v, want the first entry's %v", gotAt, first)
	}
}

func TestPollKafkaBacklogStopsOnPartitionError(t *testing.T) {
	t.Parallel()
	const (
		topic     = "orders"
		partition = int32(1)
	)
	client := &kafkaBacklogPoller{
		batches: []kgo.Fetches{
			kafkaTestFetchError(topic, partition, errKafkaBacklogProbe),
			nil,
		},
	}
	wanted := map[partitionKey]struct{}{
		{destination: topic, partition: partition}: {},
	}
	fetches := pollKafkaBacklog(context.Background(), client, wanted)
	if client.polls != 1 {
		t.Fatalf("PollFetches calls = %d, want 1", client.polls)
	}
	_, _, known := selectKafkaBacklogHead([]backlogPartitionProbe{{
		destination: topic,
		partition:   partition,
		lag:         1,
		fetches:     fetches,
	}})
	if known {
		t.Fatal("partition-error fetch produced a known head")
	}
}

type kafkaBacklogPoller struct {
	batches []kgo.Fetches
	polls   int
}

func (p *kafkaBacklogPoller) PollFetches(context.Context) kgo.Fetches {
	p.polls++
	if len(p.batches) == 0 {
		return nil
	}
	batch := p.batches[0]
	p.batches = p.batches[1:]
	return batch
}

var errKafkaBacklogProbe = &kafkaBacklogProbeError{}

type kafkaBacklogProbeError struct{}

func (*kafkaBacklogProbeError) Error() string { return "backlog probe failed" }

func kafkaTestFetches(t *testing.T, topic string, partition int32, offset int64, records ...*kgo.Record) kgo.Fetches {
	t.Helper()
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic: topic,
		Partitions: []kgo.FetchPartition{{
			Partition: partition,
			Records:   records,
		}},
	}}}}
}

func kafkaTestFetchError(topic string, partition int32, err error) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic:      topic,
		Partitions: []kgo.FetchPartition{{Partition: partition, Err: err}},
	}}}}
}

func kafkaTestRecord(t *testing.T, topic string, partition int32, offset int64, timestamp time.Time, timestampType int8, control bool) *kgo.Record {
	t.Helper()
	attrs := int16(timestampType << 3)
	if control {
		attrs |= 1 << 5
	}
	wireRecord := kmsg.Record{OffsetDelta: 0, TimestampDelta64: 0, Value: []byte("value")}
	recordBody := wireRecord.AppendTo(nil)[1:]
	wireRecord.Length = int32(len(recordBody)) //nolint:gosec // the hand-built test record is bounded
	batch := kmsg.RecordBatch{
		FirstOffset:          offset,
		PartitionLeaderEpoch: 0,
		Magic:                2,
		Attributes:           attrs,
		LastOffsetDelta:      0,
		FirstTimestamp:       timestamp.UnixMilli(),
		MaxTimestamp:         timestamp.UnixMilli(),
		ProducerID:           -1,
		ProducerEpoch:        -1,
		FirstSequence:        -1,
		NumRecords:           1,
		Records:              wireRecord.AppendTo(nil),
	}
	raw := batch.AppendTo(nil)
	binary.BigEndian.PutUint32(raw[8:], uint32(len(raw)-12)) //nolint:gosec // the hand-built test batch is bounded
	binary.BigEndian.PutUint32(raw[17:], crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))
	partitionResponse := &kmsg.FetchResponseTopicPartition{
		Partition:     partition,
		HighWatermark: offset + 1,
		RecordBatches: raw,
	}
	fetchPartition, _ := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{
		KeepControlRecords: true,
		Offset:             offset,
		Topic:              topic,
		Partition:          partition,
	}, partitionResponse, kgo.DefaultDecompressor(), nil)
	if fetchPartition.Err != nil {
		t.Fatalf("ProcessFetchPartition: %v", fetchPartition.Err)
	}
	if len(fetchPartition.Records) != 1 {
		t.Fatalf("ProcessFetchPartition returned %d records, want one", len(fetchPartition.Records))
	}
	return fetchPartition.Records[0]
}
