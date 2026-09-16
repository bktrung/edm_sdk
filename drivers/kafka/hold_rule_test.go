package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// holdRuleDestination is the destination the hold rule's broker-free cases
// drive. They need no broker: the rule is a decision over the records the poll
// loop is already holding, so the cases below hand the loop a fetch response,
// let it admit what it can, and read back what it kept.
const holdRuleDestination = "hold-rule"

// holdRuleBudget is the destination prefetch share those cases run with. It is
// deliberately far above the rule's bound of one, so a case that sees a second
// delivery on a partition is reading the rule and not a destination budget.
const holdRuleBudget = 10

// holdRuleTimeout bounds every wait for an emission, so a rule that admits
// nothing fails its case instead of hanging it.
const holdRuleTimeout = time.Second

// TestConsumerHoldRuleRefusesSecondRecordOfPartition pins the rule itself: a
// partition's next record is not delivered while its predecessor is
// outstanding, and it is delivered once the predecessor settles.
func TestConsumerHoldRuleRefusesSecondRecordOfPartition(t *testing.T) {
	c, key := newHoldRuleConsumer(t, holdRuleBudget)

	first := holdRuleRecord(key, 0)
	second := holdRuleRecord(key, 1)
	c.tagRecord(first)
	c.tagRecord(second)

	pending := []*kgo.Record{first, second}
	if !c.flushPending(&pending) {
		t.Fatal("flushPending returned false")
	}
	if got := c.outstanding[key]; got != 1 {
		t.Fatalf("outstanding deliveries on %+v = %d, want 1: the admitted record is the partition's one delivery", key, got)
	}
	if len(pending) != 1 || pending[0] != second {
		t.Fatalf("pending after the first record was admitted = %d records (%+v), want the second record alone: a partition holds one delivery at a time", len(pending), pending)
	}

	held := holdRuleDelivery(t, c)
	c.completeSettlement(held.Settle.(*settler), false)

	if !c.flushPending(&pending) {
		t.Fatal("flushPending returned false")
	}
	if len(pending) != 0 {
		t.Fatalf("pending after the partition's delivery settled = %d records, want 0: the settle admits the record the rule refused", len(pending))
	}
	next := holdRuleDelivery(t, c)
	if next.Ref.Offset != second.Offset {
		t.Fatalf("the delivery admitted after the settle = %+v, want the record at offset %d", next.Ref, second.Offset)
	}
}

// TestConsumerHoldRuleKeepsPartitionChargedAcrossRequeue pins the requeue half
// of the rule: a Nack with Requeue keeps the partition charged, because the
// redelivery of that offset is the partition's one delivery. The requeued
// offset itself is admitted (it re-enters emission as a reuse and is not
// charged again), and the next record waits for the redelivery to settle.
//
// A count that dropped at the requeue would admit the next record while the
// requeued offset is still owed, which is two deliveries in flight on one
// partition, the state the rule exists to forbid.
func TestConsumerHoldRuleKeepsPartitionChargedAcrossRequeue(t *testing.T) {
	c, key := newHoldRuleConsumer(t, holdRuleBudget)

	first := holdRuleRecord(key, 0)
	second := holdRuleRecord(key, 1)
	c.tagRecord(first)
	c.tagRecord(second)

	pending := []*kgo.Record{first, second}
	if !c.flushPending(&pending) {
		t.Fatal("flushPending returned false")
	}
	held := holdRuleDelivery(t, c)
	if err := held.Settle.Nack(context.Background(), driver.NackOptions{Requeue: true}); err != nil {
		t.Fatalf("Nack the held delivery with Requeue: %v", err)
	}
	if got := c.outstanding[key]; got != 1 {
		t.Fatalf("outstanding deliveries on %+v after a requeue = %d, want 1: the redelivery is the partition's one delivery", key, got)
	}

	if !c.flushPending(&pending) {
		t.Fatal("flushPending returned false")
	}
	if len(pending) != 1 || pending[0] != second {
		t.Fatalf("pending after a requeue = %d records (%+v), want the second record alone: the partition is still charged while its redelivery is owed", len(pending), pending)
	}

	redelivered := holdRuleRecord(key, first.Offset)
	c.tagRecord(redelivered)
	pending = append(pending, redelivered)
	if !c.flushPending(&pending) {
		t.Fatal("flushPending returned false")
	}
	if got := c.outstanding[key]; got != 1 {
		t.Fatalf("outstanding deliveries on %+v after the requeued record arrived = %d, want 1: a redelivery reuses its predecessor's charge", key, got)
	}
	if len(pending) != 1 || pending[0] != second {
		t.Fatalf("pending after the requeued record was admitted = %d records (%+v), want the second record alone", len(pending), pending)
	}

	repeat := holdRuleDelivery(t, c)
	if repeat.Ref.Offset != first.Offset {
		t.Fatalf("the redelivery = %+v, want offset %d", repeat.Ref, first.Offset)
	}
	c.completeSettlement(repeat.Settle.(*settler), false)

	if !c.flushPending(&pending) {
		t.Fatal("flushPending returned false")
	}
	if len(pending) != 0 {
		t.Fatalf("pending after the redelivery settled = %d records, want 0: the redelivery's settle frees the partition", len(pending))
	}
}

// TestConsumerHoldRuleBoundsPendingAndHoldsThePartition pins the read-ahead
// bound. A partition whose handler never settles has one delivery in flight and
// every later record refused, so nothing but this bound stops the poll loop
// from carrying the whole destination in pending. The case feeds the loop one
// fetch response per round, holds every partition whose fetches are held, and
// requires pending to stop growing at the limit plus one response.
//
// It also pins both edges of the hold: the partition comes off the hold at the
// limit and goes back on below it, which is the boundary that decides whether a
// settling delivery finds its successor already fetched.
//
// The case reads the limit from the consumer rather than fixing it, and feeds
// rounds until the partition is held: the limit is a package constant the
// sitting moves, and the bound is the same bound at any value of it.
func TestConsumerHoldRuleBoundsPendingAndHoldsThePartition(t *testing.T) {
	const response = 5
	c, key := newHoldRuleConsumer(t, 3)
	limit := c.readAheadLimit(key.destination)
	rounds := limit/response + 2

	pending := make([]*kgo.Record, 0, limit+response)
	offset := int64(0)
	for range rounds {
		if !holdRulePartitionHeld(c, key) {
			for range response {
				record := holdRuleRecord(key, offset)
				offset++
				c.tagRecord(record)
				pending = append(pending, record)
			}
		}
		if !c.flushPending(&pending) {
			t.Fatal("flushPending returned false")
		}
		c.syncReadAheadPauses(pending)
	}
	if got := len(pending); got > limit+response {
		t.Fatalf("pending held %d records of one partition over %d rounds, want at most %d: the hold rule plus the read-ahead hold bound the records the loop carries, and one response is all the overshoot a hold can leave",
			got, rounds, limit+response)
	}
	if !holdRulePartitionHeld(c, key) {
		t.Fatalf("the partition's fetches were never held, with %d of its records waiting in pending against a read-ahead limit of %d", len(pending), limit)
	}

	// The hold comes off only once the partition's records drop below the
	// limit, so every settle that leaves the limit or more waiting keeps it and
	// the one that takes the count under the limit clears it. Each settle admits
	// exactly one of the records the rule refused, so the count falls by one per
	// step and the boundary is reached whatever the limit is.
	carried := len(pending)
	for carried >= limit {
		delivery := holdRuleDelivery(t, c)
		c.completeSettlement(delivery.Settle.(*settler), false)
		if !c.flushPending(&pending) {
			t.Fatal("flushPending returned false")
		}
		c.syncReadAheadPauses(pending)
		carried--
		if got := len(pending); got != carried {
			t.Fatalf("pending after a settle = %d records, want %d: each settle admits exactly one of the records the rule refused", got, carried)
		}
		if held := holdRulePartitionHeld(c, key); held != (carried >= limit) {
			t.Fatalf("partition held = %t with %d records in pending against a read-ahead limit of %d, want %t", held, carried, limit, carried >= limit)
		}
	}
}

// TestConsumerHoldRuleHoldStaysInertAtTheNoHoldLimit pins the premise of the
// discriminator sitting: at the limit the sitting runs with, one partition's
// whole backlog of the measured corpus sits below the limit, so the reconcile
// holds nothing and the throughput result cannot be the hold toggling on a
// partition at its boundary.
//
// The backlog is the measured corpus over the partition count the wide cell
// runs at. The limit this replaces was the destination's prefetch share, 22 for
// that shape, which the same backlog exceeds, which is why a partition at the
// boundary toggled once per delivery there.
func TestConsumerHoldRuleHoldStaysInertAtTheNoHoldLimit(t *testing.T) {
	const backlog = 1000 / 16
	c, key := newHoldRuleConsumer(t, holdRuleBudget)
	limit := c.readAheadLimit(key.destination)
	if limit <= backlog {
		t.Fatalf("read-ahead limit = %d, want more than the %d records one partition of the measured corpus carries", limit, backlog)
	}

	pending := make([]*kgo.Record, 0, backlog)
	for offset := range int64(backlog) {
		record := holdRuleRecord(key, offset)
		c.tagRecord(record)
		pending = append(pending, record)
	}
	if !c.flushPending(&pending) {
		t.Fatal("flushPending returned false")
	}
	c.syncReadAheadPauses(pending)

	if got := len(c.readAheadPaused); got != 0 {
		t.Fatalf("partitions held = %d with %d records of one partition waiting in pending against a read-ahead limit of %d, want 0: the limit is above the measured backlog, so no partition may be held", got, len(pending), limit)
	}
	if holdRulePartitionHeld(c, key) {
		t.Fatalf("the client is holding the partition's fetches with %d records in pending against a read-ahead limit of %d, want no hold", len(pending), limit)
	}
}

// TestConsumerHoldRuleReleasesADroppedBufferedDelivery pins the revoke side of
// the mirror: a delivery the revoke drops before its caller ever sees it
// returns the partition's charge, so the partition is admissible on the new
// owner instead of staying blocked for a settlement that can no longer come.
func TestConsumerHoldRuleReleasesADroppedBufferedDelivery(t *testing.T) {
	c, key := newHoldRuleConsumer(t, holdRuleBudget)

	buffered := holdRuleRecord(key, 0)
	c.tagRecord(buffered)
	pending := []*kgo.Record{buffered}
	if !c.flushPending(&pending) {
		t.Fatal("flushPending returned false")
	}
	if got := c.outstanding[key]; got != 1 {
		t.Fatalf("outstanding deliveries on %+v after the admit = %d, want 1", key, got)
	}

	// The delivery is still in the consumer's own channel, which is what a
	// revoke finds when the caller has not taken it yet.
	c.dropTracker(key.destination, key.partition)

	if got := c.outstanding[key]; got != 0 {
		t.Fatalf("outstanding deliveries on %+v after the revoke dropped the buffered delivery = %d, want 0: a delivery the revoke drops must not keep its partition charged", key, got)
	}
	if got := c.unsettled[key.destination]; got != 0 {
		t.Fatalf("unsettled deliveries on %q after the revoke = %d, want 0: the partition's charge mirrors the destination slot at every release", key.destination, got)
	}

	// The reassigned partition admits its next record: the dropped delivery's
	// charge is gone, so the rule has nothing to refuse it for.
	c.onPartitionsAssigned(context.Background(), nil, map[string][]int32{key.destination: {key.partition}})
	next := holdRuleRecord(key, 1)
	c.tagRecord(next)
	pending = []*kgo.Record{next}
	if !c.flushPending(&pending) {
		t.Fatal("flushPending returned false")
	}
	if got := c.outstanding[key]; got != 1 {
		t.Fatalf("outstanding deliveries on %+v after the reassigned partition's first record = %d, want 1: the dropped delivery released the partition", key, got)
	}
}

// TestConsumerHoldRuleKeepsADeliveryStillOutChargedAcrossARevoke pins the other
// side: a revoke that finds the delivery already with its caller leaves the
// partition charged, because the settlement that charge waits for is still
// owed, and the partition's next record stays refused until it arrives.
//
// Releasing the charge there is the handoff double count: the partition would
// admit a second delivery while the first is unsettled, which is the state the
// rule exists to forbid, and the count would be released a second time when the
// first one settles.
func TestConsumerHoldRuleKeepsADeliveryStillOutChargedAcrossARevoke(t *testing.T) {
	c, key := newHoldRuleConsumer(t, holdRuleBudget)

	first := holdRuleRecord(key, 0)
	second := holdRuleRecord(key, 1)
	c.tagRecord(first)
	c.tagRecord(second)
	pending := []*kgo.Record{first, second}
	if !c.flushPending(&pending) {
		t.Fatal("flushPending returned false")
	}
	held := holdRuleDelivery(t, c)

	c.dropTracker(key.destination, key.partition)

	if got := c.outstanding[key]; got != 1 {
		t.Fatalf("outstanding deliveries on %+v after a revoke took the partition off this consumer with the delivery still out = %d, want 1: the settlement the delivery owes is still the partition's charge", key, got)
	}
	if got := c.unsettled[key.destination]; got != 1 {
		t.Fatalf("unsettled deliveries on %q after the revoke = %d, want 1: the partition's charge mirrors the destination slot at every release", key.destination, got)
	}
	if len(pending) != 1 || pending[0] != second {
		t.Fatalf("pending after the revoke = %d records (%+v), want the second record alone: the partition is still charged while its delivery is unsettled", len(pending), pending)
	}

	c.completeSettlement(held.Settle.(*settler), false)

	if got := c.outstanding[key]; got != 0 {
		t.Fatalf("outstanding deliveries on %+v after the held delivery settled = %d, want 0: the settlement releases the charge, revoked or not", key, got)
	}
	if got := c.unsettled[key.destination]; got != 0 {
		t.Fatalf("unsettled deliveries on %q after the held delivery settled = %d, want 0: the settlement releases the slot with the charge", key.destination, got)
	}
}

// newHoldRuleConsumer builds the broker-free consumer the rule's cases drive,
// with partition 0 of one destination assigned and the destination's budget set
// to budget.
func newHoldRuleConsumer(t *testing.T, budget int) (*consumer, partitionKey) {
	t.Helper()
	c := newLeaveTestConsumer(t, newLeaveTestConnection(holdRuleDestination), holdRuleDestination, budget)
	c.onPartitionsAssigned(context.Background(), nil, map[string][]int32{holdRuleDestination: {0}})
	key := partitionKey{destination: holdRuleDestination, partition: 0}
	if generation := c.activeGenerations[key]; generation == 0 {
		t.Fatalf("active generation for %+v = 0, want the assignment's generation", key)
	}
	return c, key
}

// holdRuleRecord is one record of the case's partition at offset.
func holdRuleRecord(key partitionKey, offset int64) *kgo.Record {
	return &kgo.Record{
		Topic:     key.destination,
		Partition: key.partition,
		Offset:    offset,
		Key:       []byte(key.destination),
		Value:     []byte("hold-rule"),
	}
}

// holdRuleDelivery takes the next delivery the consumer emitted, under the
// case's deadline.
func holdRuleDelivery(t *testing.T, c *consumer) driver.InboundMessage {
	t.Helper()
	timer := c.clock.Timer(holdRuleTimeout)
	defer timer.Stop()
	select {
	case message := <-c.messages:
		return message
	case <-timer.C:
		t.Fatal("no delivery was emitted within the case's deadline")
		return driver.InboundMessage{}
	}
}

// holdRulePartitionHeld reports whether the client is holding fetches for one
// partition, which is the broker-facing effect of the read-ahead bound. It asks
// the client rather than the consumer's own bookkeeping, so a case that reads
// true is reading a hold franz-go was given.
func holdRulePartitionHeld(c *consumer, key partitionKey) bool {
	for _, partition := range c.client.PauseFetchPartitions(nil)[key.destination] {
		if partition == key.partition {
			return true
		}
	}
	return false
}
