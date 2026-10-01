package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// newTwoPartitionConsumer is a hold-rule consumer that owns partitions 0 and 1
// of one destination.
func newTwoPartitionConsumer(t *testing.T, budget int) (c *consumer, first, second partitionKey) {
	t.Helper()
	c = newLeaveTestConsumer(t, newLeaveTestConnection(holdRuleDestination), holdRuleDestination, budget)
	c.onPartitionsAssigned(context.Background(), nil, map[string][]int32{holdRuleDestination: {0, 1}})
	return c, partitionKey{destination: holdRuleDestination, partition: 0}, partitionKey{destination: holdRuleDestination, partition: 1}
}

// TestSettlementWakeVisitsOnlyTheSettledPartition proves the admission pass a
// settlement starts visits the settled partition alone, which is what keeps
// the poll loop's work per delivery from growing with the partitions owned.
//
// The other partition is put in a state that any visit changes and no
// partition-scoped event describes: its ownership is withdrawn without the
// rebalance callback, so a pass that visited it would drop its queue as stale.
// The queue surviving the settlement's pass is the proof the pass did not
// visit it, and the settled partition's next record being delivered is the
// proof the pass ran.
func TestSettlementWakeVisitsOnlyTheSettledPartition(t *testing.T) {
	c, settled, other := newTwoPartitionConsumer(t, holdRuleBudget)
	head := holdRuleRecord(settled, 0)
	next := holdRuleRecord(settled, 1)
	otherHead := holdRuleRecord(other, 0)
	for _, record := range []*kgo.Record{head, next, otherHead} {
		c.tagRecord(record)
	}
	seedQueuedRecords(c, head, next, otherHead)
	if !c.flushPending() {
		t.Fatal("flushPending returned false")
	}
	deliveries := map[partitionKey]driver.InboundMessage{}
	for range 2 {
		delivery := holdRuleDelivery(t, c)
		deliveries[delivery.Settle.(*settler).key] = delivery
	}

	behind := holdRuleRecord(other, 1)
	c.tagRecord(behind)
	c.mu.Lock()
	c.pending[other] = append(c.pending[other], behind)
	c.owned[other] = false
	c.mu.Unlock()

	c.completeSettlement(deliveries[settled].Settle.(*settler), false)
	if !c.flushPending() {
		t.Fatal("flushPending returned false")
	}
	if delivery := holdRuleDelivery(t, c); delivery.Settle.(*settler).record.Offset != next.Offset {
		t.Fatalf("delivered offset %d, want the settled partition's next record %d", delivery.Settle.(*settler).record.Offset, next.Offset)
	}
	if queued := queuedRecords(c, other); len(queued) != 1 || queued[0] != behind {
		t.Fatalf("other partition's queue = %v, want it untouched by the settlement's pass", queued)
	}
}

// TestDestinationReleaseAdmitsAnotherPartition guards the wake the targeted
// pass must not narrow. A partition refused because its destination's budget
// was full is freed by a settlement on a different partition, so the release
// of the budget has to start a pass over every partition: a pass over the
// settled partition alone would strand the refused one.
func TestDestinationReleaseAdmitsAnotherPartition(t *testing.T) {
	c, first, second := newTwoPartitionConsumer(t, 1)
	firstHead := holdRuleRecord(first, 0)
	secondHead := holdRuleRecord(second, 0)
	c.tagRecord(firstHead)
	c.tagRecord(secondHead)
	seedQueuedRecords(c, firstHead, secondHead)
	if !c.flushPending() {
		t.Fatal("flushPending returned false")
	}
	delivered := holdRuleDelivery(t, c)
	waiting := second
	if delivered.Settle.(*settler).key == second {
		waiting = first
	}

	c.completeSettlement(delivered.Settle.(*settler), false)
	if !c.flushPending() {
		t.Fatal("flushPending returned false")
	}
	if delivery := holdRuleDelivery(t, c); delivery.Settle.(*settler).key != waiting {
		t.Fatalf("delivered partition %v, want the partition the full budget refused, %v", delivery.Settle.(*settler).key, waiting)
	}
}

// TestPartialPassDeliversPastACommittedHead proves a partial pass keeps
// admitting from a partition whose head emit skipped. A record whose offset is
// already committed - a copy fetched again - is skipped without charging the
// partition, so the record behind it is admissible at once, and only a mark
// the partition leaves on itself brings a partial pass back to it.
func TestPartialPassDeliversPastACommittedHead(t *testing.T) {
	c, key := newHoldRuleConsumer(t, holdRuleBudget)
	first := holdRuleRecord(key, 0)
	c.tagRecord(first)
	seedQueuedRecords(c, first)
	if !c.flushPending() {
		t.Fatal("flushPending returned false")
	}
	delivery := holdRuleDelivery(t, c)
	s := delivery.Settle.(*settler)
	if err := s.tracker.AckOwn(first.Offset, func(int64) error { return nil }); err != nil {
		t.Fatalf("ack: %v", err)
	}
	c.completeSettlement(s, false)

	again := holdRuleRecord(key, 0)
	next := holdRuleRecord(key, 1)
	c.tagRecord(again)
	c.tagRecord(next)
	seedQueuedRecords(c, again, next)
	if !c.flushPending() {
		t.Fatal("flushPending returned false")
	}
	if delivery := holdRuleDelivery(t, c); delivery.Settle.(*settler).record.Offset != next.Offset {
		t.Fatalf("delivered offset %d, want the record behind the committed copy, %d", delivery.Settle.(*settler).record.Offset, next.Offset)
	}
}

// TestPartialPassReconcilesReadAheadOfVisitedPartition proves the read-ahead
// reconciliation after a partial pass reaches the partitions that pass
// visited. The pass is partial because nothing but a fetch-shaped mark has
// happened since the previous pass, and the partition it visits is left at its
// read-ahead limit, so the fetch pause has to be set by the reconciliation
// that runs over the visited partitions alone.
func TestPartialPassReconcilesReadAheadOfVisitedPartition(t *testing.T) {
	c, key := newHoldRuleConsumer(t, holdRuleBudget)
	if !c.flushPending() {
		t.Fatal("flushPending returned false")
	}
	limit := c.readAheadLimit(key.destination)
	records := make([]*kgo.Record, 0, limit+1)
	for offset := range int64(limit + 1) {
		record := holdRuleRecord(key, offset)
		c.tagRecord(record)
		records = append(records, record)
	}
	seedQueuedRecords(c, records...)
	if !c.flushPending() {
		t.Fatal("flushPending returned false")
	}
	if c.visitedAll {
		t.Fatal("the pass after a fetch-shaped mark was full, want partial")
	}
	c.syncReadAheadAfterFlush()
	if _, held := c.partitionPauses[key][partitionPauseReadAhead]; !held {
		t.Fatalf("read-ahead hold not set with %d records queued at a limit of %d", len(queuedRecords(c, key)), limit)
	}
}

// commitNothing is a commitSender that accepts every point, which lets a unit
// consumer settle without a group coordinator.
func commitNothing(_ context.Context, points map[partitionKey]int64) map[partitionKey]error {
	return make(map[partitionKey]error, len(points))
}

// TestAckHandsTheNextRecordOver proves an Ack delivers its partition's next
// record itself: the record is on Messages when Ack returns, with no admission
// pass in between and no wake left for the poll loop.
func TestAckHandsTheNextRecordOver(t *testing.T) {
	c, key := newHoldRuleConsumer(t, holdRuleBudget)
	c.commitFn = commitNothing
	head := holdRuleRecord(key, 0)
	next := holdRuleRecord(key, 1)
	c.tagRecord(head)
	c.tagRecord(next)
	seedQueuedRecords(c, head, next)
	if !c.flushPending() {
		t.Fatal("flushPending returned false")
	}
	delivery := holdRuleDelivery(t, c)
	c.mu.Lock()
	c.pollWakePending = false
	c.mu.Unlock()

	if err := delivery.Settle.Ack(context.Background()); err != nil {
		t.Fatalf("Ack = %v, want nil", err)
	}
	select {
	case message := <-c.messages:
		if got := message.Settle.(*settler).record.Offset; got != next.Offset {
			t.Fatalf("handed over offset %d, want %d", got, next.Offset)
		}
	default:
		t.Fatal("Ack returned without handing the partition's next record over")
	}
	if headHoldWoken(c) {
		t.Fatal("Ack woke the poll loop for a record it handed over itself")
	}
}

// TestDrainFencesSettlementHandoff holds the delivery critical section while an
// actual Ack settles and queues its handoff behind it. Drain must not declare
// admission closed while that section is still in flight, even though the poll
// loop is already done and the settlement no longer holds a delivery slot.
func TestDrainFencesSettlementHandoff(t *testing.T) {
	c, key := newHoldRuleConsumer(t, holdRuleBudget)
	c.commitFn = commitNothing
	head := holdRuleRecord(key, 0)
	next := holdRuleRecord(key, 1)
	behind := holdRuleRecord(key, 2)
	c.tagRecord(head)
	c.tagRecord(next)
	c.tagRecord(behind)
	seedQueuedRecords(c, head, next, behind)
	if !c.flushPending() {
		t.Fatal("flushPending returned false")
	}
	delivery := holdRuleDelivery(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), holdRuleTimeout)
	defer cancel()
	// Holding the existing boundary stands in for a sender admitted before
	// Drain but not yet finished. It also keeps Ack's real handoff pending;
	// no send or admission logic is reproduced in the test.
	c.deliveryMu.Lock()
	locked := true
	defer func() {
		if locked {
			c.deliveryMu.Unlock()
		}
	}()
	ackDone := make(chan error, 1)
	go func() {
		ackDone <- delivery.Settle.Ack(ctx)
	}()
	select {
	case <-c.settlerCh:
	case <-ctx.Done():
		t.Fatal("Ack did not complete settlement before its handoff")
	}

	drainDone := make(chan error, 1)
	go func() {
		drainDone <- c.Drain(ctx)
	}()
	select {
	case <-c.forwarderStopC:
	case <-ctx.Done():
		t.Fatal("Drain did not stop blocked emissions")
	}
	// The old Drain ignores deliveryMu and returns during this interval:
	// pollDone is closed, no settlers remain, and the fixture's leave succeeds.
	// The fence must keep Drain pending until the section is released.
	timer := c.clock.Timer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case err := <-drainDone:
		t.Fatalf("Drain returned with an in-flight delivery section: %v", err)
	case <-timer.C:
	case <-ctx.Done():
		t.Fatal("timed out checking the drain admission boundary")
	}
	c.deliveryMu.Unlock()
	locked = false

	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("Drain = %v, want nil", err)
		}
	case <-ctx.Done():
		t.Fatal("Drain did not finish after the delivery section ended")
	}
	select {
	case err := <-ackDone:
		if err != nil {
			t.Fatalf("Ack = %v, want nil", err)
		}
	case <-ctx.Done():
		t.Fatal("Ack's handoff did not finish after Drain")
	}
	// The queued handoff may acquire the fence first and send before Drain
	// marks draining. Such a buffered delivery remains settleable; its Ack
	// after Drain must not hand the record behind it over.
	select {
	case message := <-c.messages:
		if got := message.Settle.(*settler).record.Offset; got != next.Offset {
			t.Fatalf("buffered delivery offset = %d, want %d", got, next.Offset)
		}
		if err := message.Settle.Ack(ctx); err != nil {
			t.Fatalf("Ack after Drain = %v, want nil", err)
		}
	default:
	}
	c.handOff(key)
	select {
	case message := <-c.messages:
		t.Fatalf("post-drain handoff delivered offset %d", message.Settle.(*settler).record.Offset)
	default:
	}
	if err := c.Drain(ctx); err != nil {
		t.Fatalf("second Drain = %v, want nil", err)
	}
	if err := c.Stop(ctx); err != nil {
		t.Fatalf("Stop = %v, want nil", err)
	}
}

// TestHandOffKeepsPartitionOrderAgainstTheLoop runs the handoff and the poll
// loop's admission pass over one partition at the same time, many times, and
// checks every delivery comes in offset order. Both goroutines admit from the
// same queue, so an admission that took the head before the charge could hand
// the second record out ahead of the first; run with -race.
func TestHandOffKeepsPartitionOrderAgainstTheLoop(t *testing.T) {
	const records = 50
	c, key := newHoldRuleConsumer(t, holdRuleBudget)
	c.commitFn = commitNothing
	queued := make([]*kgo.Record, 0, records)
	for offset := range int64(records) {
		record := holdRuleRecord(key, offset)
		c.tagRecord(record)
		queued = append(queued, record)
	}
	seedQueuedRecords(c, queued...)

	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		for range records * 4 {
			c.mu.Lock()
			c.partialScan = false
			c.mu.Unlock()
			if !c.flushPending() {
				return
			}
		}
	}()
	for want := range int64(records) {
		delivery := holdRuleDelivery(t, c)
		if got := delivery.Settle.(*settler).record.Offset; got != want {
			t.Fatalf("delivered offset %d, want %d", got, want)
		}
		if err := delivery.Settle.Ack(context.Background()); err != nil {
			t.Fatalf("Ack(%d) = %v, want nil", want, err)
		}
	}
	<-loopDone
}

func newAggregateAdmissionConsumer(t *testing.T, prefetch int) (*consumer, partitionKey, partitionKey) {
	t.Helper()
	c := newLeaveTestConsumer(t, newLeaveTestConnection("admission-a"), "admission-a", 2)
	c.cfg.Prefetch = prefetch
	c.cfg.Destinations = []string{"admission-a", "admission-b"}
	c.cfg.PerDestination = map[string]int{"admission-a": 2, "admission-b": 2}
	c.destinations = c.cfg.Destinations
	c.budgets["admission-b"] = 2
	c.messages = make(chan driver.InboundMessage, totalPrefetch(c.cfg))
	c.commitFn = commitNothing
	c.onPartitionsAssigned(context.Background(), nil, map[string][]int32{"admission-a": {0}, "admission-b": {0}})
	return c, partitionKey{destination: "admission-a", partition: 0}, partitionKey{destination: "admission-b", partition: 0}
}

func TestAggregateAdmissionAckRefillsAnotherDestination(t *testing.T) {
	c, first, second := newAggregateAdmissionConsumer(t, 1)
	seedQueuedRecords(c, holdRuleRecord(first, 0))
	if !c.flushPending() {
		t.Fatal("initial admission stopped")
	}
	held := holdRuleDelivery(t, c)
	seedQueuedRecords(c, holdRuleRecord(second, 0))
	if !c.flushPending() {
		t.Fatal("aggregate admission stopped instead of holding pending")
	}
	select {
	case message := <-c.Messages():
		t.Fatalf("delivery beyond aggregate Prefetch=1: %q", message.Destination)
	default:
	}
	if err := held.Settle.Ack(context.Background()); err != nil {
		t.Fatalf("Ack() = %v", err)
	}
	// No new fetch or partition wake arrives for second. Returning the
	// aggregate charge must schedule it even though first had spare capacity.
	if !c.flushPending() {
		t.Fatal("refill stopped")
	}
	refill := holdRuleDelivery(t, c)
	if refill.Destination != second.destination {
		t.Fatalf("refill destination = %q, want %q", refill.Destination, second.destination)
	}
	if err := refill.Settle.Ack(context.Background()); err != nil {
		t.Fatalf("refill Ack() = %v", err)
	}
}

func TestAggregateAdmissionRequeueRetainsOneCharge(t *testing.T) {
	c, first, second := newAggregateAdmissionConsumer(t, 1)
	seedQueuedRecords(c, holdRuleRecord(first, 0))
	c.flushPending()
	held := holdRuleDelivery(t, c)
	seedQueuedRecords(c, holdRuleRecord(second, 0))
	c.flushPending()
	if err := held.Settle.Nack(context.Background(), driver.NackOptions{Requeue: true}); err != nil {
		t.Fatalf("Nack(requeue) = %v", err)
	}
	redelivery := holdRuleDelivery(t, c)
	if redelivery.Destination != held.Destination || redelivery.Ref != held.Ref {
		t.Fatalf("redelivery = %+v, want original destination and reference %+v", redelivery, held)
	}
	c.flushPending()
	select {
	case message := <-c.Messages():
		t.Fatalf("requeue freed aggregate capacity for %q", message.Destination)
	default:
	}
	if err := redelivery.Settle.Ack(context.Background()); err != nil {
		t.Fatalf("redelivery Ack() = %v", err)
	}
	c.flushPending()
	refill := holdRuleDelivery(t, c)
	if refill.Destination != second.destination {
		t.Fatalf("refill destination = %q, want %q", refill.Destination, second.destination)
	}
	if err := refill.Settle.Ack(context.Background()); err != nil {
		t.Fatalf("refill Ack() = %v", err)
	}
}

func TestAggregateAdmissionRevocationPreservesCallerCharge(t *testing.T) {
	c, first, second := newAggregateAdmissionConsumer(t, 1)
	seedQueuedRecords(c, holdRuleRecord(first, 0))
	c.flushPending()
	held := holdRuleDelivery(t, c)
	seedQueuedRecords(c, holdRuleRecord(second, 0))
	c.flushPending()
	tracker := c.dropTracker(first.destination, first.partition)
	if tracker == nil {
		t.Fatal("dropTracker() returned nil tracker")
	}
	tracker.Drop()
	c.flushPending()
	select {
	case message := <-c.Messages():
		t.Fatalf("revoked caller delivery freed capacity before settlement: %q", message.Destination)
	default:
	}
	if err := held.Settle.Ack(context.Background()); !errors.Is(err, ErrRevoked) {
		t.Fatalf("Ack() of revoked delivery = %v, want %v", err, ErrRevoked)
	}
	c.flushPending()
	refill := holdRuleDelivery(t, c)
	if refill.Destination != second.destination {
		t.Fatalf("refill destination = %q, want %q", refill.Destination, second.destination)
	}
	if err := refill.Settle.Ack(context.Background()); err != nil {
		t.Fatalf("refill Ack() = %v", err)
	}
}

func TestAggregateAdmissionRevocationReturnsBufferedCharge(t *testing.T) {
	c, first, second := newAggregateAdmissionConsumer(t, 1)
	seedQueuedRecords(c, holdRuleRecord(first, 0))
	c.flushPending()
	seedQueuedRecords(c, holdRuleRecord(second, 0))
	c.flushPending()
	c.dropTracker(first.destination, first.partition)
	c.flushPending()
	refill := holdRuleDelivery(t, c)
	if refill.Destination != second.destination {
		t.Fatalf("refill destination = %q, want %q", refill.Destination, second.destination)
	}
	if err := refill.Settle.Ack(context.Background()); err != nil {
		t.Fatalf("refill Ack() = %v", err)
	}
}

func TestAggregateAdmissionZeroKeepsDestinationWindows(t *testing.T) {
	c, first, second := newAggregateAdmissionConsumer(t, 0)
	seedQueuedRecords(c, holdRuleRecord(first, 0), holdRuleRecord(second, 0))
	c.flushPending()
	held := []driver.InboundMessage{holdRuleDelivery(t, c), holdRuleDelivery(t, c)}
	seen := make(map[string]bool)
	for _, message := range held {
		seen[message.Destination] = true
		if err := message.Settle.Ack(context.Background()); err != nil {
			t.Fatalf("Ack() = %v", err)
		}
	}
	if !seen[first.destination] || !seen[second.destination] {
		t.Fatalf("destinations = %v, want both destination windows usable with zero Prefetch", seen)
	}
}

func TestAggregateAdmissionDrainRevokedDeliveryReleasesCharge(t *testing.T) {
	c, first, second := newAggregateAdmissionConsumer(t, 1)
	seedQueuedRecords(c, holdRuleRecord(first, 0))
	c.flushPending()
	held := holdRuleDelivery(t, c)
	seedQueuedRecords(c, holdRuleRecord(second, 0))
	c.flushPending()
	if err := c.Drain(context.Background()); err != nil {
		t.Fatalf("Drain() = %v", err)
	}
	c.dropTracker(first.destination, first.partition)
	if err := held.Settle.Ack(context.Background()); !errors.Is(err, ErrRevoked) {
		t.Fatalf("Ack() of draining revoked delivery = %v, want %v", err, ErrRevoked)
	}
	select {
	case message := <-c.Messages():
		t.Fatalf("drain admitted pending destination %q", message.Destination)
	default:
	}
	if c.admitted != 0 {
		t.Fatalf("aggregate charge after draining settlement = %d, want 0", c.admitted)
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() after draining settlement = %v", err)
	}
}

func TestAggregateAdmissionCanceledEmissionReturnsCharge(t *testing.T) {
	for _, requeue := range []bool{false, true} {
		name := "new-delivery"
		if requeue {
			name = "redelivery"
		}
		t.Run(name, func(t *testing.T) {
			c, first, _ := newAggregateAdmissionConsumer(t, 1)
			seedQueuedRecords(c, holdRuleRecord(first, 0))
			if requeue {
				c.flushPending()
				held := holdRuleDelivery(t, c)
				if err := c.Pause(first.destination); err != nil {
					t.Fatalf("Pause() = %v", err)
				}
				if err := held.Settle.Nack(context.Background(), driver.NackOptions{Requeue: true}); err != nil {
					t.Fatalf("Nack(requeue) = %v", err)
				}
				if err := c.Resume(first.destination); err != nil {
					t.Fatalf("Resume() = %v", err)
				}
			}
			// Neither branch may send after the forwarder fence. With no
			// receiver the cancellation branch deterministically wins.
			c.messages = make(chan driver.InboundMessage)
			c.stopForwarders()
			if outcome := c.deliverHead(first); outcome != headStopped {
				t.Fatalf("canceled emission outcome = %v, want stopped", outcome)
			}
			if c.admitted != 0 || c.unsettled[first.destination] != 0 || c.outstanding[first] != 0 {
				t.Fatalf("canceled emission retained charges: aggregate=%d destination=%d partition=%d",
					c.admitted, c.unsettled[first.destination], c.outstanding[first])
			}
			if err := c.Stop(context.Background()); err != nil {
				t.Fatalf("Stop() after canceled emission = %v", err)
			}
		})
	}
}

// assertWaitingPartitionServed runs a partition with a long backlog against
// one with a single ready record, where one slot is all the two share, and
// checks the ready partition is served within one turn per competitor. Each
// turn is an actual Ack followed by the admission pass the release woke.
//
// The race this guards: an Ack released the full slot, woke the poll loop, and
// then handed its own partition's next record over from the settling
// goroutine before the loop ran. The busy partition took the slot back every
// time, so the waiting one was served only when the busy one's queue ran dry.
func assertWaitingPartitionServed(t *testing.T, c *consumer, busy, waiting partitionKey) {
	t.Helper()
	const backlog = 8
	records := make([]*kgo.Record, 0, backlog+1)
	for offset := range int64(backlog) {
		records = append(records, holdRuleRecord(busy, offset))
	}
	records = append(records, holdRuleRecord(waiting, 0))
	for _, record := range records {
		c.tagRecord(record)
	}
	seedQueuedRecords(c, records...)
	if !c.flushPending() {
		t.Fatal("flushPending returned false")
	}
	delivery := holdRuleDelivery(t, c)
	for turn := range 2 {
		if delivery.Settle.(*settler).key == waiting {
			return
		}
		if err := delivery.Settle.Ack(context.Background()); err != nil {
			t.Fatalf("turn %d: Ack = %v", turn, err)
		}
		if !c.flushPending() {
			t.Fatal("flushPending returned false")
		}
		delivery = holdRuleDelivery(t, c)
	}
	if delivery.Settle.(*settler).key != waiting {
		t.Fatalf("partition %v still waiting after two turns of %v", waiting, busy)
	}
}

// TestAggregateSlotIsSharedInTurn proves a partition waiting for the
// aggregate slot is not starved by a busy partition of another destination
// that settles over and over.
func TestAggregateSlotIsSharedInTurn(t *testing.T) {
	c, busy, waiting := newAggregateAdmissionConsumer(t, 1)
	assertWaitingPartitionServed(t, c, busy, waiting)
}

// TestDestinationSlotIsSharedInTurn is the same property for a destination's
// own budget, which two partitions of one destination compete for.
func TestDestinationSlotIsSharedInTurn(t *testing.T) {
	c, busy, waiting := newTwoPartitionConsumer(t, 1)
	c.commitFn = commitNothing
	assertWaitingPartitionServed(t, c, busy, waiting)
}
