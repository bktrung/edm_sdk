package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestConsumerAdmissionUsesRecordTimestamp(t *testing.T) {
	// A broker stores a record's timestamp in whole milliseconds, so the instant
	// the producer published at is somewhere inside the millisecond the stored
	// value names. The due time is the end of that millisecond plus the delay,
	// which is the earliest instant consistent with the stored timestamp and the
	// only one that never lands before the publisher's instant plus the delay.
	now := time.Unix(100, 0).Add(750 * time.Microsecond)
	key := partitionKey{destination: "retry", partition: 0}
	consumer := &consumer{
		cfg:             driver.ConsumerConfig{Delays: map[string]time.Duration{"retry": 10 * time.Second}},
		budgets:         map[string]int{"retry": 1},
		pauseReasons:    make(map[string]pauseReasonSet),
		partitionPauses: make(map[partitionKey]partitionPauseSet),
		heldUntil:       make(map[partitionKey]time.Time),
		pending:         make(map[partitionKey][]*kgo.Record),
		owned:           map[partitionKey]bool{key: true},
		unsettled:       make(map[string]int),
		outstanding:     make(map[partitionKey]int),
		trackers:        make(map[partitionKey]*ackTracker),
		clock:           clock.NewFake(now),
	}
	record := &kgo.Record{
		Topic: key.destination, Partition: key.partition, Offset: 0,
		Timestamp: now.Truncate(time.Millisecond),
	}
	consumer.mu.Lock()
	if consumer.admissionLocked(record) {
		consumer.mu.Unlock()
		t.Fatal("admissionLocked() = true, want the future timestamp held")
	}
	consumer.mu.Unlock()
	want := record.Timestamp.Add(time.Millisecond).Add(10 * time.Second)
	if got := consumer.heldUntil[key]; !got.Equal(want) {
		t.Fatalf("heldUntil = %s, want %s", got, want)
	}
	if want.Before(now.Add(10 * time.Second)) {
		t.Fatalf("due time %s is before the publish instant plus the delay %s", want, now.Add(10*time.Second))
	}
	consumer.clock.(*clock.Fake).Advance(want.Sub(now))
	consumer.mu.Lock()
	if !consumer.admissionLocked(record) {
		consumer.mu.Unlock()
		t.Fatal("admissionLocked() = false at due time, want true")
	}
	consumer.mu.Unlock()
}

func TestPartitionPauseReasonsReleaseIndependently(t *testing.T) {
	key := partitionKey{destination: "topic", partition: 0}
	consumer := &consumer{
		partitionPauses: make(map[partitionKey]partitionPauseSet),
		owned:           map[partitionKey]bool{key: true},
		pollWakePending: false,
		clock:           clock.NewReal(),
	}
	consumer.mu.Lock()
	consumer.setPartitionPauseReasonLocked(key, partitionPauseReadAhead, true)
	consumer.setPartitionPauseReasonLocked(key, partitionPauseHeadHold, true)
	consumer.setPartitionPauseReasonLocked(key, partitionPauseHeadHold, false)
	if reasons := consumer.partitionPauses[key]; reasons.empty() {
		consumer.mu.Unlock()
		t.Fatal("clearing headHold removed readAhead")
	}
	if consumer.pollWakePending {
		consumer.mu.Unlock()
		t.Fatal("clearing headHold woke polling while readAhead still held the partition")
	}
	consumer.setPartitionPauseReasonLocked(key, partitionPauseReadAhead, false)
	if _, held := consumer.partitionPauses[key]; held {
		consumer.mu.Unlock()
		t.Fatal("clearing the last pause reason left the partition held")
	}
	if !consumer.pollWakePending {
		consumer.mu.Unlock()
		t.Fatal("clearing the last pause reason did not wake polling")
	}
	consumer.mu.Unlock()
}

func TestHeldHeadRevokeDropsQueueAndTimer(t *testing.T) {
	now := time.Unix(100, 0)
	key := partitionKey{destination: "topic", partition: 0}
	consumer := &consumer{
		cfg:             driver.ConsumerConfig{Destinations: []string{key.destination}},
		budgets:         map[string]int{key.destination: 1},
		pauseReasons:    make(map[string]pauseReasonSet),
		partitionPauses: make(map[partitionKey]partitionPauseSet),
		pending:         map[partitionKey][]*kgo.Record{key: {{Topic: key.destination, Partition: key.partition, Offset: 7}}},
		heldUntil:       map[partitionKey]time.Time{key: now.Add(time.Minute)},
		owned:           map[partitionKey]bool{key: true},
		trackers:        make(map[partitionKey]*ackTracker),
		settlers:        make(map[*settler]struct{}),
		messages:        make(chan driver.InboundMessage, 1),
		errors:          make(chan error, 1),
		unsettled:       make(map[string]int),
		outstanding:     make(map[partitionKey]int),
		inHand:          make(map[*kgo.Record]struct{}),
		settlerCh:       make(chan struct{}, 1),
		clock:           clock.NewFake(now),
	}
	consumer.partitionPauses[key] = partitionPauseSet{partitionPauseHeadHold: {}}
	consumer.mu.Lock()
	consumer.syncHeadTimerLocked()
	consumer.mu.Unlock()
	consumer.onPartitionsLost(context.Background(), nil, map[string][]int32{key.destination: {key.partition}})
	if len(consumer.pending) != 0 || len(consumer.heldUntil) != 0 || len(consumer.partitionPauses) != 0 {
		t.Fatalf("revoked state pending=%v heldUntil=%v pauses=%v, want all empty", consumer.pending, consumer.heldUntil, consumer.partitionPauses)
	}
	if consumer.pollWakePending {
		t.Fatal("revocation woke polling for stale held work")
	}
}

// TestHeadHoldTimerWakesThePollLoop pins the consumer's one head-hold timer at
// the unit level: the earliest held head arms it, time reaching that due fires
// it, and the fire wakes the poll loop, which is what re-runs admission and
// hands the record over. Time reaching less than the due must not wake the loop,
// because the loop admits whatever it can on every wake, and a later held head
// must re-arm the timer, because the loop consumes a wake once.
func TestHeadHoldTimerWakesThePollLoop(t *testing.T) {
	now := time.Unix(100, 0)
	key := partitionKey{destination: "topic", partition: 0}
	clk := clock.NewFake(now)
	pollDone := make(chan struct{})
	consumer := &consumer{
		cfg:              driver.ConsumerConfig{Delays: map[string]time.Duration{key.destination: time.Second}},
		budgets:          map[string]int{key.destination: 1},
		pauseReasons:     make(map[string]pauseReasonSet),
		partitionPauses:  make(map[partitionKey]partitionPauseSet),
		heldUntil:        make(map[partitionKey]time.Time),
		pending:          make(map[partitionKey][]*kgo.Record),
		owned:            map[partitionKey]bool{key: true},
		unsettled:        make(map[string]int),
		outstanding:      make(map[partitionKey]int),
		trackers:         make(map[partitionKey]*ackTracker),
		pollDone:         pollDone,
		headTimerChanged: make(chan struct{}, 1),
		clock:            clk,
	}
	consumer.mu.Lock()
	consumer.setHeadHoldLocked(key, now.Add(time.Second))
	consumer.mu.Unlock()
	// The loop is waited on after pollDone is closed, so the test does not leave
	// it running; the defers run in reverse, which is what puts the close first.
	var loop sync.WaitGroup
	loop.Go(func() {
		consumer.headHoldLoop()
	})
	defer loop.Wait()
	defer close(pollDone)

	clk.Advance(time.Second - time.Millisecond)
	if headHoldWoken(consumer) {
		t.Fatal("the poll loop woke before the held head came due")
	}
	clk.Advance(time.Millisecond)
	waitHeadHoldState(t, "the head-hold timer to fire and wake the poll loop", func() bool {
		return headHoldWoken(consumer)
	})

	// A later held head, with nothing else happening in between, is what the
	// retained timer has to re-arm for: the loop consumes a wake once, so a fire
	// that left the timer unarmed would leave the record behind it waiting for an
	// event that never comes.
	consumer.mu.Lock()
	consumer.pollWakePending = false
	consumer.setHeadHoldLocked(key, clk.Now().Add(time.Second))
	consumer.mu.Unlock()
	clk.Advance(time.Second)
	waitHeadHoldState(t, "the head-hold timer to re-arm for a later held head", func() bool {
		return headHoldWoken(consumer)
	})
}

// TestHeadHoldTimerRearmsPastAnExpiredHold pins the wake a fire owes the holds
// it did not clear. Two holds are set and the first is already due when its
// timer fires, with nothing clearing it: that is the state a held head reaches
// when its destination is paused, because only a resume delivers it. The timer
// must arm for the second hold, whose due is the next one to fire, instead of
// re-choosing the expired entry, which would arm an immediate timer, or leaving
// the second hold with no wake at all.
func TestHeadHoldTimerRearmsPastAnExpiredHold(t *testing.T) {
	now := time.Unix(100, 0)
	expired := partitionKey{destination: "expired", partition: 0}
	live := partitionKey{destination: "live", partition: 0}
	clk := clock.NewFake(now)
	pollDone := make(chan struct{})
	consumer := &consumer{
		partitionPauses:  make(map[partitionKey]partitionPauseSet),
		heldUntil:        make(map[partitionKey]time.Time),
		pollDone:         pollDone,
		headTimerChanged: make(chan struct{}, 1),
		clock:            clk,
	}
	consumer.mu.Lock()
	consumer.setHeadHoldLocked(expired, now.Add(time.Second))
	consumer.setHeadHoldLocked(live, now.Add(time.Minute))
	consumer.mu.Unlock()
	// The loop is waited on after pollDone is closed, so the test does not leave
	// it running; the defers run in reverse, which is what puts the close first.
	var loop sync.WaitGroup
	loop.Go(func() {
		consumer.headHoldLoop()
	})
	defer loop.Wait()
	defer close(pollDone)

	clk.Advance(time.Second)
	waitHeadHoldState(t, "the timer to re-arm for the next held head", func() bool {
		return headHoldDue(consumer).Equal(now.Add(time.Minute))
	})
	consumer.mu.Lock()
	consumer.pollWakePending = false
	consumer.mu.Unlock()

	// An arm at the expired hold would be due at once, so the poll loop would be
	// woken again before the second hold's due.
	clk.Advance(time.Minute - time.Millisecond)
	if headHoldWoken(consumer) {
		t.Fatal("the timer re-armed at the expired hold and woke polling before the second hold came due")
	}
	clk.Advance(time.Millisecond)
	waitHeadHoldState(t, "the poll loop to wake when the second held head came due", func() bool {
		return headHoldWoken(consumer)
	})
}

// waitHeadHoldState polls state on the real clock until it holds. The waits use
// a real clock's Sleep because the fake clock these tests hold only moves when
// the test advances it and the loop that reacts to it runs in its own
// goroutine; the context bounds every wait.
func waitHeadHoldState(t *testing.T, description string, state func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for !state() {
		if err := clock.NewReal().Sleep(ctx, time.Millisecond); err != nil {
			t.Fatalf("timed out waiting for %s", description)
		}
	}
}

func headHoldWoken(c *consumer) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pollWakePending
}

func headHoldDue(c *consumer) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.headTimerDue
}

// requeueRevokeConsumer builds the smallest consumer that can answer a
// requeue: one destination with one slot, one owned partition charged with one
// in-flight delivery, and that delivery's tracker current.
func requeueRevokeConsumer(t *testing.T, key partitionKey, tracker *ackTracker) (*consumer, *settler) {
	t.Helper()
	record := &kgo.Record{Topic: key.destination, Partition: key.partition, Offset: 4}
	consumer := &consumer{
		cfg:             driver.ConsumerConfig{Destinations: []string{key.destination}},
		budgets:         map[string]int{key.destination: 1},
		pauseReasons:    make(map[string]pauseReasonSet),
		partitionPauses: make(map[partitionKey]partitionPauseSet),
		pending:         make(map[partitionKey][]*kgo.Record),
		heldUntil:       make(map[partitionKey]time.Time),
		requeued:        make(map[partitionKey]int),
		owned:           map[partitionKey]bool{key: true},
		trackers:        map[partitionKey]*ackTracker{key: tracker},
		settlers:        make(map[*settler]struct{}),
		messages:        make(chan driver.InboundMessage, 1),
		errors:          make(chan error, 1),
		unsettled:       map[string]int{key.destination: 1},
		admitted:        1,
		outstanding:     map[partitionKey]int{key: 1},
		inHand:          make(map[*kgo.Record]struct{}),
		settlerCh:       make(chan struct{}, 1),
		clock:           clock.NewFake(time.Unix(100, 0)),
	}
	settler := &settler{owner: consumer, record: record, tracker: tracker, key: key}
	consumer.settlers[settler] = struct{}{}
	return consumer, settler
}

// TestConsumerRequeueAfterRevokeQueuesNoRedelivery pins what a requeue does when
// the partition left this consumer before the handler answered the delivery.
// Nothing here will redeliver the record, so queueing it would leave a count on
// a partition this consumer does not own: the poll loop drops the record through
// its stale pass but not the count, and a later ownership of the partition would
// read that count as a redelivery already in hand and admit its first head with
// the due-time hold, the hold rule and the budget all bypassed.
func TestConsumerRequeueAfterRevokeQueuesNoRedelivery(t *testing.T) {
	key := partitionKey{destination: "topic", partition: 0}
	tracker := newAckTracker(4)
	consumer, settler := requeueRevokeConsumer(t, key, tracker)

	// The revoke takes the partition first and leaves the delivery the caller
	// holds charged, which is the settlement its wait waits for.
	consumer.dropTracker(key.destination, key.partition)
	if err := settler.Nack(context.Background(), driver.NackOptions{Requeue: true}); err != nil {
		t.Fatalf("Nack(requeue) after the revoke = %v, want nil", err)
	}
	if got := consumer.requeued[key]; got != 0 {
		t.Fatalf("requeued = %d after a requeue on a revoked partition, want 0", got)
	}
	if queued := consumer.pending[key]; len(queued) != 0 {
		t.Fatalf("pending = %d records after a requeue on a revoked partition, want 0", len(queued))
	}
	if got := consumer.unsettled[key.destination]; got != 0 {
		t.Fatalf("unsettled = %d, want the revoked delivery's destination slot released", got)
	}
	if got := consumer.outstanding[key]; got != 0 {
		t.Fatalf("outstanding = %d, want the revoked delivery's partition charge released", got)
	}
}

// TestConsumerRequeueAfterReassignmentQueuesNoOldCopy pins the third case: the
// delivery outlived the ownership that admitted it and the partition came back
// to this same consumer, which is what an eager balancer does to every partition
// it keeps. The settler's tracker is no longer the one in force, so the requeue
// is not this consumer's redelivery to make: queueing it would leave the count
// without the charge completeSettlement is about to release, and the redelivery
// would then be admitted as a reuse with the hold rule and the budget behind no
// charge at all. The ownership in force fetches the record from the offset that
// is still uncommitted.
func TestConsumerRequeueAfterReassignmentQueuesNoOldCopy(t *testing.T) {
	key := partitionKey{destination: "topic", partition: 0}
	admitted := newAckTracker(4)
	reassigned := newAckTracker(4)
	consumer, settler := requeueRevokeConsumer(t, key, admitted)

	consumer.dropTracker(key.destination, key.partition)
	consumer.onPartitionsAssigned(context.Background(), nil, map[string][]int32{key.destination: {key.partition}})
	consumer.mu.Lock()
	consumer.trackers[key] = reassigned
	consumer.unsettled[key.destination] = 1
	consumer.admitted = 1
	consumer.outstanding[key] = 1
	consumer.mu.Unlock()

	if err := settler.Nack(context.Background(), driver.NackOptions{Requeue: true}); err != nil {
		t.Fatalf("Nack(requeue) after the reassignment = %v, want nil", err)
	}
	if got := consumer.requeued[key]; got != 0 {
		t.Fatalf("requeued = %d after a requeue under a renewed ownership, want 0", got)
	}
	if queued := consumer.pending[key]; len(queued) != 0 {
		t.Fatalf("pending = %d records after a requeue under a renewed ownership, want 0", len(queued))
	}
	if got := consumer.unsettled[key.destination]; got != 0 {
		t.Fatalf("unsettled = %d, want the delivery's destination slot released", got)
	}
	if got := consumer.outstanding[key]; got != 0 {
		t.Fatalf("outstanding = %d, want the delivery's partition charge released", got)
	}
}

// TestConsumerRevokeReleasesAQueuedRequeueCharge pins the other order: the
// requeue lands while the partition is still this consumer's, so the redelivery
// is queued and keeps the delivery's charges, and the revoke that arrives after
// drops both the queue and those charges. The offset stays uncommitted, so the
// partition's next owner redelivers the record; a charge kept for a redelivery
// this consumer will never make would shrink the destination's window for the
// rest of its life.
//
// The destination is paused by the caller, which is the state that keeps a
// requeue queued: without it the Nack hands the redelivery over at once.
func TestConsumerRevokeReleasesAQueuedRequeueCharge(t *testing.T) {
	key := partitionKey{destination: "topic", partition: 0}
	tracker := newAckTracker(4)
	consumer, settler := requeueRevokeConsumer(t, key, tracker)
	consumer.pauseReasons[key.destination] = pauseReasonSet{pauseReasonUserPaused: {}}

	if err := settler.Nack(context.Background(), driver.NackOptions{Requeue: true}); err != nil {
		t.Fatalf("Nack(requeue) on an owned partition = %v, want nil", err)
	}
	if got := consumer.requeued[key]; got != 1 {
		t.Fatalf("requeued = %d after an owned requeue, want the redelivery queued", got)
	}
	if got := consumer.unsettled[key.destination]; got != 1 {
		t.Fatalf("unsettled = %d after an owned requeue, want the charge kept for the redelivery", got)
	}
	consumer.dropTracker(key.destination, key.partition)
	if got := consumer.unsettled[key.destination]; got != 0 {
		t.Fatalf("unsettled = %d after the revoke dropped the queued redelivery, want 0", got)
	}
	if got := consumer.outstanding[key]; got != 0 {
		t.Fatalf("outstanding = %d after the revoke dropped the queued redelivery, want 0", got)
	}
	if got := consumer.requeued[key]; got != 0 {
		t.Fatalf("requeued = %d after the revoke, want 0", got)
	}
}

// TestConsumerRequeueHandsTheRedeliveryOver proves a requeue on an owned,
// unpaused partition is redelivered by the Nack itself, without a poll loop:
// the redelivery is on Messages when Nack returns, as a reuse of the charges
// its delivery held, and a revoke after it releases those charges through the
// buffered delivery it discards.
func TestConsumerRequeueHandsTheRedeliveryOver(t *testing.T) {
	key := partitionKey{destination: "topic", partition: 0}
	tracker := newAckTracker(4)
	consumer, settler := requeueRevokeConsumer(t, key, tracker)

	if err := settler.Nack(context.Background(), driver.NackOptions{Requeue: true}); err != nil {
		t.Fatalf("Nack(requeue) on an owned partition = %v, want nil", err)
	}
	if got := len(consumer.messages); got != 1 {
		t.Fatalf("Messages holds %d deliveries after the requeue, want the redelivery", got)
	}
	if got := consumer.requeued[key]; got != 0 {
		t.Fatalf("requeued = %d after the handoff, want the redelivery taken", got)
	}
	if got, want := consumer.unsettled[key.destination], 1; got != want {
		t.Fatalf("unsettled = %d after the handoff, want %d: the redelivery reuses the charge", got, want)
	}
	consumer.dropTracker(key.destination, key.partition)
	if got := consumer.unsettled[key.destination]; got != 0 {
		t.Fatalf("unsettled = %d after the revoke discarded the buffered redelivery, want 0", got)
	}
	if got := consumer.outstanding[key]; got != 0 {
		t.Fatalf("outstanding = %d after the revoke discarded the buffered redelivery, want 0", got)
	}
}
