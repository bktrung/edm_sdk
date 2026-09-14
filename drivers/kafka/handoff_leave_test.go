package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestStopMovesQueuedHandoffToPeer(t *testing.T) {
	const (
		destination = "leave-stop"
		offset      = int64(7)
	)
	connection := newLeaveTestConnection(destination)
	source := newLeaveTestConsumer(t, connection, destination, 1)
	target := newLeaveTestConsumer(t, connection, destination, 1)
	key := partitionKey{destination: destination, partition: 0}

	occupied := seedOccupiedTarget(t, target, key, 99)
	seedQueuedHandoff(source, key, offset)
	target.transferReservations[key] = map[int64]struct{}{offset: {}}

	if err := source.Stop(context.Background()); err != nil {
		t.Fatalf("source Stop: %v", err)
	}
	target.completeSettlement(occupied, false)
	waitForLeaveTestHandoff(t, target, offset, "after source Stop and slot release")
}

func TestReleaseAfterRegrantClearsPeerReservation(t *testing.T) {
	const (
		destination = "leave-regrant"
		offset      = int64(17)
	)
	connection := newLeaveTestConnection(destination)
	source := newLeaveTestConsumer(t, connection, destination, 1)
	target := newLeaveTestConsumer(t, connection, destination, 1)
	key := partitionKey{destination: destination, partition: 0}
	tracker := newAckTracker(offset, 1)
	if err := tracker.Track(offset); err != nil {
		t.Fatalf("Track: %v", err)
	}
	held := &settler{
		owner:   source,
		record:  &kgo.Record{Topic: destination, Partition: 0, Offset: offset},
		tracker: tracker,
		handoff: leaveTestMessage(destination, offset),
		key:     key,
	}
	held.handoff.Settle = held
	source.trackers[key] = tracker
	source.trackerGenerations[key] = 1
	source.activeGenerations[key] = 1
	source.assignmentGenerations[key] = 1
	source.tenures[key] = 1
	source.settlers[held] = struct{}{}
	source.unsettled[destination] = 1
	target.activeGenerations[key] = 1
	target.tenures[key] = 1
	target.fenced[key] = true

	source.assignmentMu.Lock()
	source.deliveryMu.Lock()
	source.mu.Lock()
	result := source.transferOwnershipLocked(revokedPartition{
		key:     key,
		tracker: tracker,
		owned:   map[*settler]*ackTracker{held: tracker},
	})
	source.mu.Unlock()
	source.deliveryMu.Unlock()
	source.assignmentMu.Unlock()
	source.finishTransfer(result)

	if got := len(source.handoffs[key]); got != 1 {
		t.Fatalf("source handoffs after transfer = %d, want 1", got)
	}
	if got := len(target.transferReservations[key]); got != 1 {
		t.Fatalf("target reservations after transfer = %d, want 1", got)
	}
	source.onPartitionsAssigned(context.Background(), nil, map[string][]int32{destination: {0}})
	if got := len(source.handoffs[key]); got != 0 {
		t.Fatalf("source handoffs after self-regrant = %d, want 0", got)
	}
	if err := source.Release(context.Background()); err != nil {
		t.Fatalf("source Release: %v", err)
	}
	if _, reserved := target.transferReservations[key][offset]; reserved {
		t.Fatalf("peer reservation for offset %d remains after source Release", offset)
	}
}

func TestReleaseMovesQueuedHandoffToPeer(t *testing.T) {
	const (
		destination = "leave-release"
		offset      = int64(7)
	)
	connection := newLeaveTestConnection(destination)
	source := newLeaveTestConsumer(t, connection, destination, 1)
	target := newLeaveTestConsumer(t, connection, destination, 1)
	key := partitionKey{destination: destination, partition: 0}

	occupied := seedOccupiedTarget(t, target, key, 99)
	seedQueuedHandoff(source, key, offset)
	target.transferReservations[key] = map[int64]struct{}{offset: {}}

	if err := source.Release(context.Background()); err != nil {
		t.Fatalf("source Release: %v", err)
	}
	target.completeSettlement(occupied, false)
	waitForLeaveTestHandoff(t, target, offset, "after source Release and slot release")
}

// TestInFlightHandoffRequeuedToLeftSourceReachesTarget covers the copy a peer
// takes before its source leaves: the emission fails on the full target, and
// the requeue lands on a source that has already left the connection.
func TestInFlightHandoffRequeuedToLeftSourceReachesTarget(t *testing.T) {
	const (
		destination = "leave-inflight"
		offset      = int64(7)
	)
	connection := newLeaveTestConnection(destination)
	source := newLeaveTestConsumer(t, connection, destination, 1)
	target := newLeaveTestConsumer(t, connection, destination, 1)
	key := partitionKey{destination: destination, partition: 0}

	occupied := seedOccupiedTarget(t, target, key, 99)
	seedQueuedHandoff(source, key, offset)
	target.transferReservations[key] = map[int64]struct{}{offset: {}}

	// The take happens while the source is still registered: it removes the copy
	// under the source's mutex and leaves it in the caller's hands.
	taken := target.takeHandoffs(map[string][]int32{destination: {0}})
	if len(taken) != 1 {
		t.Fatalf("takeHandoffs took %d copies, want 1", len(taken))
	}
	if err := source.Stop(context.Background()); err != nil {
		t.Fatalf("source Stop: %v", err)
	}
	// The target is at budget, so this emission fails and hands the copy back to
	// the source it was taken from, which has left by now.
	target.emitHandoff(taken[0].source, taken[0].message)
	target.completeSettlement(occupied, false)
	waitForLeaveTestHandoff(t, target, offset, "after an in-flight take, source Stop, a failed emit and slot release")
}

// TestRequeueIntoLeftConsumerReachesTarget covers the other route into a
// consumer that has left: a stale peer is handed a copy, the way the move and
// emitHandoff do when the consumer they place it on has gone.
func TestRequeueIntoLeftConsumerReachesTarget(t *testing.T) {
	const (
		destination = "leave-requeue"
		offset      = int64(7)
	)
	connection := newLeaveTestConnection(destination)
	peer := newLeaveTestConsumer(t, connection, destination, 1)
	target := newLeaveTestConsumer(t, connection, destination, 1)
	key := partitionKey{destination: destination, partition: 0}

	occupied := seedOccupiedTarget(t, target, key, 99)
	target.transferReservations[key] = map[int64]struct{}{offset: {}}
	message, _ := queuedHandoffMessage(peer, key, offset)

	if err := peer.Stop(context.Background()); err != nil {
		t.Fatalf("peer Stop: %v", err)
	}
	peer.requeueHandoff(message)
	target.completeSettlement(occupied, false)
	waitForLeaveTestHandoff(t, target, offset, "after a requeue into a consumer that has left")
}

// TestReleaseKeepsReservationForSettlerWithQueuedHandoff covers the exclusion
// in Release: a settler still in c.settlers whose offset also has a queued copy
// keeps its peer reservation, because that copy is moved and still needs it.
func TestReleaseKeepsReservationForSettlerWithQueuedHandoff(t *testing.T) {
	const (
		destination = "leave-retained"
		offset      = int64(17)
	)
	connection := newLeaveTestConnection(destination)
	source := newLeaveTestConsumer(t, connection, destination, 1)
	peer := newLeaveTestConsumer(t, connection, destination, 1)
	key := partitionKey{destination: destination, partition: 0}
	tracker := newAckTracker(offset, 1)
	if err := tracker.Track(offset); err != nil {
		t.Fatalf("Track: %v", err)
	}
	_, held := queuedHandoffMessage(source, key, offset)
	held.tracker = tracker
	held.tombstoned = false
	source.trackers[key] = tracker
	source.trackerGenerations[key] = 1
	source.settlers[held] = struct{}{}
	source.unsettled[destination] = 1
	// The revoked-awaiting-transfer state: the partition is fenced and its
	// generation is gone, while the delivery the caller holds is still this
	// consumer's. That is where onPartitionsRevoked reaches dropAllTrackers.
	source.fenced[key] = true

	// A revoke that lands while this member is leaving runs dropTracker with
	// draining set, which is the only way production reaches it: the delivery
	// stays in c.settlers, its offset is queued as a copy, and the copy's offset
	// is reserved on every peer.
	source.draining = true
	source.dropTracker(destination, 0)

	if err := source.Release(context.Background()); err != nil {
		t.Fatalf("source Release: %v", err)
	}
	peer.mu.Lock()
	_, reserved := peer.transferReservations[key][offset]
	_, queued := peer.handoffs[key][offset]
	peer.mu.Unlock()
	if !reserved {
		t.Fatalf("peer reservation for offset %d was cleared by Release while a copy of it was still queued", offset)
	}
	if !queued {
		t.Fatalf("peer does not hold the queued copy for offset %d after source Release", offset)
	}
}

// newLeaveTestConnection returns a connection with the destination declared at
// no delay, which is the state of a destination that is not deferred: an
// undeclared destination faults every admission instead.
func newLeaveTestConnection(destination string) *conn {
	return &conn{
		consumers: make(map[*consumer]struct{}),
		delays:    map[string]time.Duration{destination: 0},
	}
}

func newLeaveTestConsumer(t *testing.T, connection *conn, destination string, budget int) *consumer {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers("localhost:1"), noDialKafkaOption())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(client.Close)
	pollDone := make(chan struct{})
	close(pollDone)
	messagesCapacity := budget
	if messagesCapacity < 1 {
		messagesCapacity = 1
	}
	c := &consumer{
		conn:                  connection,
		client:                client,
		cfg:                   driver.ConsumerConfig{Destinations: []string{destination}, Prefetch: budget},
		group:                 "leave-test-group",
		destinations:          []string{destination},
		budgets:               map[string]int{destination: budget},
		messages:              make(chan driver.InboundMessage, messagesCapacity),
		errors:                make(chan error, 1),
		pollDone:              pollDone,
		stopDone:              make(chan struct{}),
		pauseReasons:          make(map[string]pauseReasonSet),
		unsettled:             make(map[string]int),
		settlers:              make(map[*settler]struct{}),
		trackers:              make(map[partitionKey]*ackTracker),
		trackerGenerations:    make(map[partitionKey]uint64),
		assignmentGenerations: make(map[partitionKey]uint64),
		activeGenerations:     make(map[partitionKey]uint64),
		fenced:                make(map[partitionKey]bool),
		recordGenerations:     make(map[*kgo.Record]uint64),
		tenures:               make(map[partitionKey]uint64),
		settledTransfers:      make(map[partitionKey]map[int64]struct{}),
		pendingTransfers:      make(map[partitionKey]map[int64]*ackTracker),
		transferReservations:  make(map[partitionKey]map[int64]struct{}),
		selfTransfers:         make(map[partitionKey][]*settler),
		handoffRecords:        make(map[*kgo.Record]struct{}),
		handoffReservations:   make(map[partitionKey]map[int64]struct{}),
		handoffMarkers:        make(map[partitionKey]map[int64]struct{}),
		handoffs:              make(map[partitionKey]map[int64]driver.InboundMessage),
		settlerCh:             make(chan struct{}, 1),
		requeued:              make(map[partitionKey]int),
		discarded:             make(map[partitionKey]map[int64]struct{}),
		clock:                 clock.NewReal(),
		cancelPoll:            func() {},
		leaveRequestC:         make(chan struct{}, 1),
		leaveStopC:            make(chan struct{}),
		leaveFinished:         make(chan struct{}),
		leaveLoopDone:         make(chan struct{}),
		forwarderStopC:        make(chan struct{}),
		leaveFn:               func(context.Context, leaveRequest) error { return nil },
	}
	connection.mu.Lock()
	connection.consumers[c] = struct{}{}
	connection.mu.Unlock()
	return c
}

func leaveTestMessage(destination string, offset int64) driver.InboundMessage {
	return driver.InboundMessage{
		Destination: destination,
		Body:        []byte("handoff"),
		Ref: driver.BrokerRef{
			Partition: 0,
			Offset:    offset,
		},
	}
}

// queuedHandoffMessage builds a copy the way a transfer leaves one: the message
// carries the settler admission created for it, and that settler's handoff is
// the message itself.
func queuedHandoffMessage(owner *consumer, key partitionKey, offset int64) (driver.InboundMessage, *settler) {
	message := leaveTestMessage(key.destination, offset)
	held := &settler{
		owner:      owner,
		record:     &kgo.Record{Topic: key.destination, Partition: key.partition, Offset: offset},
		key:        key,
		tombstoned: true,
	}
	// Admission sets both directions on the same settler: Settle is the settler
	// the caller receives, and handoff is the copy the settler carries. A copy
	// helper that sets only one of them queues nothing, because
	// storeHandoffLocked stores only when handoff.Settle is set.
	message.Settle = held
	held.handoff = message
	return message, held
}

// seedQueuedHandoff models what a transfer leaves on the source: the delivery's
// settler detached from c.settlers and tombstoned with its tracker dropped, and
// a queued copy carrying that settler whose offset is the record's only live
// path. The destination slot stays charged until the caller settles, which is
// why unsettled is set here.
func seedQueuedHandoff(source *consumer, key partitionKey, offset int64) {
	message, held := queuedHandoffMessage(source, key, offset)
	source.handoffs[key] = map[int64]driver.InboundMessage{offset: message}
	source.selfTransfers[key] = []*settler{held}
	source.unsettled[key.destination] = 1
}

func seedOccupiedTarget(t *testing.T, target *consumer, key partitionKey, offset int64) *settler {
	t.Helper()
	tracker := newAckTracker(0, 1)
	if err := tracker.Track(offset); err != nil {
		t.Fatalf("occupied Track: %v", err)
	}
	target.activeGenerations[key] = 1
	target.tenures[key] = 1
	target.trackers[key] = tracker
	target.trackerGenerations[key] = 1
	target.unsettled[key.destination] = 1
	occupied := &settler{
		owner:   target,
		record:  &kgo.Record{Topic: key.destination, Partition: key.partition, Offset: offset},
		tracker: tracker,
		key:     key,
	}
	target.settlers[occupied] = struct{}{}
	return occupied
}

func waitForLeaveTestHandoff(t *testing.T, target *consumer, offset int64, suffix string) {
	t.Helper()
	timer := clock.NewReal().Timer(time.Second)
	defer timer.Stop()
	select {
	case message := <-target.Messages():
		if message.Ref.Offset != offset {
			t.Fatalf("target received offset %d, want %d", message.Ref.Offset, offset)
		}
	case <-timer.C:
		t.Fatalf("target did not receive the queued handoff %s", suffix)
	}
}
