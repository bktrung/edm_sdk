package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

const (
	holdRuleDestination = "hold-rule"
	holdRuleBudget      = 10
	holdRuleTimeout     = time.Second
)

func TestConsumerHoldRuleRefusesSecondRecordOfPartition(t *testing.T) {
	c, key := newHoldRuleConsumer(t, holdRuleBudget)
	first := holdRuleRecord(key, 0)
	second := holdRuleRecord(key, 1)
	c.tagRecord(first)
	c.tagRecord(second)
	seedQueuedRecords(c, first, second)
	if !c.flushPending() {
		t.Fatal("flushPending returned false")
	}
	if queued := queuedRecords(c, key); len(queued) != 1 || queued[0] != second {
		t.Fatalf("pending = %v, want the second record", queued)
	}
	delivery := holdRuleDelivery(t, c)
	c.completeSettlement(delivery.Settle.(*settler), false)
	if !c.flushPending() {
		t.Fatal("flushPending returned false")
	}
	if queued := queuedRecords(c, key); len(queued) != 0 {
		t.Fatalf("pending after settlement = %v, want empty", queued)
	}
}

func TestConsumerHoldRuleKeepsARequeuePendingPartitionOffTheHold(t *testing.T) {
	c, key := newHoldRuleConsumer(t, holdRuleBudget)
	limit := c.readAheadLimit(key.destination)
	pending := make([]*kgo.Record, 0, limit)
	for offset := range int64(limit) {
		record := holdRuleRecord(key, offset)
		c.tagRecord(record)
		pending = append(pending, record)
	}
	seedQueuedRecords(c, pending...)
	c.mu.Lock()
	c.requeued[key] = 1
	c.mu.Unlock()
	c.syncReadAheadPauses()
	if _, held := c.partitionPauses[key][partitionPauseReadAhead]; held {
		t.Fatal("read-ahead hold set while a requeue is pending")
	}
}

func TestConsumerHoldRuleReleasesAHeldPartitionWhenARequeueArrives(t *testing.T) {
	c, key := newHoldRuleConsumer(t, holdRuleBudget)
	limit := c.readAheadLimit(key.destination)
	pending := make([]*kgo.Record, 0, limit)
	for offset := range int64(limit) {
		record := holdRuleRecord(key, offset)
		c.tagRecord(record)
		pending = append(pending, record)
	}
	seedQueuedRecords(c, pending...)
	c.syncReadAheadPauses()
	if _, held := c.partitionPauses[key][partitionPauseReadAhead]; !held {
		t.Fatal("read-ahead hold not set at its limit")
	}
	c.mu.Lock()
	c.requeued[key] = 1
	c.mu.Unlock()
	c.syncReadAheadPauses()
	if _, held := c.partitionPauses[key][partitionPauseReadAhead]; held {
		t.Fatal("read-ahead hold survived a pending requeue")
	}
}

func TestConsumerHoldRuleReleasesADroppedBufferedDelivery(t *testing.T) {
	c, key := newHoldRuleConsumer(t, holdRuleBudget)
	record := holdRuleRecord(key, 0)
	c.tagRecord(record)
	seedQueuedRecords(c, record)
	if !c.flushPending() {
		t.Fatal("flushPending returned false")
	}
	c.dropTracker(key.destination, key.partition)
	if got := c.outstanding[key]; got != 0 {
		t.Fatalf("outstanding after drop = %d, want 0", got)
	}
}

func TestConsumerHoldRuleKeepsADeliveryStillOutChargedAcrossARevoke(t *testing.T) {
	c, key := newHoldRuleConsumer(t, holdRuleBudget)
	first := holdRuleRecord(key, 0)
	second := holdRuleRecord(key, 1)
	c.tagRecord(first)
	c.tagRecord(second)
	seedQueuedRecords(c, first, second)
	if !c.flushPending() {
		t.Fatal("flushPending returned false")
	}
	delivery := holdRuleDelivery(t, c)
	c.dropTracker(key.destination, key.partition)
	if got := c.outstanding[key]; got != 1 {
		t.Fatalf("outstanding after revoke = %d, want 1", got)
	}
	c.completeSettlement(delivery.Settle.(*settler), false)
	if got := c.outstanding[key]; got != 0 {
		t.Fatalf("outstanding after settlement = %d, want 0", got)
	}
}

func newHoldRuleConsumer(t *testing.T, budget int) (*consumer, partitionKey) {
	t.Helper()
	c := newLeaveTestConsumer(t, newLeaveTestConnection(holdRuleDestination), holdRuleDestination, budget)
	c.onPartitionsAssigned(context.Background(), nil, map[string][]int32{holdRuleDestination: {0}})
	return c, partitionKey{destination: holdRuleDestination, partition: 0}
}

// seedQueuedRecords seeds a consumer's per-partition queues with records in fetch
// order, which is the state the poll loop leaves them in before it flushes, and
// marks their partitions for the next admission pass as a fetch does.
func seedQueuedRecords(c *consumer, records ...*kgo.Record) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending == nil {
		c.pending = make(map[partitionKey][]*kgo.Record)
	}
	for _, record := range records {
		key := partitionKey{destination: record.Topic, partition: record.Partition}
		c.pending[key] = append(c.pending[key], record)
		c.markDirtyLocked(key)
	}
}

// queuedRecords returns a partition's queued records.
func queuedRecords(c *consumer, key partitionKey) []*kgo.Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*kgo.Record(nil), c.pending[key]...)
}

func holdRuleRecord(key partitionKey, offset int64) *kgo.Record {
	return &kgo.Record{Topic: key.destination, Partition: key.partition, Offset: offset, Key: []byte(key.destination), Value: []byte("hold-rule")}
}

func holdRuleDelivery(t *testing.T, c *consumer) driver.InboundMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), holdRuleTimeout)
	defer cancel()
	select {
	case message := <-c.messages:
		return message
	case <-ctx.Done():
		t.Fatal("timed out waiting for hold-rule delivery")
		return driver.InboundMessage{}
	}
}

// TestSettlementDoesNotRaceADrainingRevoke settles a delivery while a revoke
// of its partition lands during drain. The revoke clears the settler's
// tracker under the consumer lock, so the settlement must read it under the
// same lock; run with -race.
func TestSettlementDoesNotRaceADrainingRevoke(t *testing.T) {
	for range 20 {
		c, key := newHoldRuleConsumer(t, holdRuleBudget)
		record := holdRuleRecord(key, 0)
		c.tagRecord(record)
		seedQueuedRecords(c, record)
		if !c.flushPending() {
			t.Fatal("flushPending returned false")
		}
		delivery := holdRuleDelivery(t, c)
		c.mu.Lock()
		c.draining = true
		c.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		started := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			close(started)
			_ = delivery.Settle.Ack(ctx)
		}()
		<-started
		c.dropTracker(key.destination, key.partition)
		<-done
		cancel()
	}
}

// tagRecord is tagRecordLocked for a test that does not hold c.mu.
func (c *consumer) tagRecord(record *kgo.Record) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tagRecordLocked(record)
}
