package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
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

// TestReleaseTearsDownAfterAFailedLeave covers the failure that used to strand
// a connection: the leave finishes with an error, and Release has to end the
// consumer anyway. The error stays visible on both channels out.
func TestReleaseTearsDownAfterAFailedLeave(t *testing.T) {
	const destination = "leave-failed"
	connection := newLeaveTestConnection(destination)
	c := newLeaveTestConsumer(t, connection, destination, 1)
	c.leaveFn = func(context.Context, leaveRequest) error { return kerr.CoordinatorNotAvailable }

	err := c.Release(context.Background())
	if !errors.Is(err, kerr.CoordinatorNotAvailable) {
		t.Fatalf("Release error = %v, want it to wrap %v", err, kerr.CoordinatorNotAvailable)
	}
	if leaveTestConsumerRegistered(c) {
		t.Fatal("consumer is still registered after a leave that finished with an error")
	}
	timer := clock.NewReal().Timer(time.Second)
	defer timer.Stop()
	select {
	case got, open := <-c.Errors():
		if !open {
			t.Fatal("Errors() closed before delivering the leave error")
		}
		if !errors.Is(got, kerr.CoordinatorNotAvailable) {
			t.Fatalf("Errors() delivered %v, want it to wrap %v", got, kerr.CoordinatorNotAvailable)
		}
	case <-timer.C:
		t.Fatal("Errors() did not deliver the leave error")
	}
	if _, open := <-c.Errors(); open {
		t.Fatal("Errors() is still open after Release")
	}
	select {
	case _, open := <-c.Messages():
		if open {
			t.Fatal("Messages() delivered a message after Release")
		}
	case <-timer.C:
		t.Fatal("Messages() was not closed by Release")
	}
	if err := c.Release(context.Background()); err != nil {
		t.Fatalf("second Release = %v, want nil", err)
	}
}

// TestReleaseTreatsUnknownMemberAsLeft covers the leave that arrives before the
// join completed, or after the member was expired: UNKNOWN_MEMBER_ID is the
// state a leave exists to reach, so it is not an error and nothing is reported
// on Errors(). Both membership paths reach that mapping through leaveGroup.
func TestReleaseTreatsUnknownMemberAsLeft(t *testing.T) {
	tests := []struct {
		name    string
		install func(c *consumer)
	}{
		{
			name: "static instance",
			install: func(c *consumer) {
				c.staticMembership = true
				c.instanceID = "leave-test-instance"
				c.leaveStatic = func(context.Context, string, string) error { return kerr.UnknownMemberID }
			},
		},
		{
			name: "dynamic member",
			install: func(c *consumer) {
				c.leaveDynamic = func(context.Context) error { return kerr.UnknownMemberID }
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const destination = "leave-unknown-member"
			connection := newLeaveTestConnection(destination)
			c := newLeaveTestConsumer(t, connection, destination, 1)
			c.leaveFn = c.leaveGroup
			tc.install(c)

			if err := c.Release(context.Background()); err != nil {
				t.Fatalf("Release = %v, want nil when the member had already left the group", err)
			}
			select {
			case got, open := <-c.Errors():
				if open {
					t.Fatalf("Errors() delivered %v after a leave that found the member already out of the group", got)
				}
			default:
				t.Fatal("Errors() is still open after Release")
			}
			if leaveTestConsumerRegistered(c) {
				t.Fatal("consumer is still registered after a leave that found the member already out of the group")
			}
		})
	}
}

// TestReleaseKeepsConsumerWhenCallerContextEndsFirst covers the retry path: a
// caller that gives up on the wait has not ended the leave, so the consumer
// stays registered and a later Release with a fresh context completes it.
func TestReleaseKeepsConsumerWhenCallerContextEndsFirst(t *testing.T) {
	const destination = "leave-context"
	connection := newLeaveTestConnection(destination)
	c := newLeaveTestConsumer(t, connection, destination, 1)
	leaveFinished := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.leaveFn = func(context.Context, leaveRequest) error {
		// End the caller's context with the leave still in flight. The leave
		// runs on its own context, so it stays here until the test releases it.
		cancel()
		<-leaveFinished
		return nil
	}

	err := c.Release(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Release error = %v, want %v", err, context.Canceled)
	}
	if kind, classified := driver.Classify(err); !classified || kind != driver.KindTransient {
		t.Fatalf("Release error kind = %v (classified %v), want %v", kind, classified, driver.KindTransient)
	}
	if !leaveTestConsumerRegistered(c) {
		t.Fatal("consumer was deregistered by a Release whose caller context ended before the leave finished")
	}

	close(leaveFinished)
	if err := c.Release(context.Background()); err != nil {
		t.Fatalf("second Release = %v, want nil", err)
	}
	if leaveTestConsumerRegistered(c) {
		t.Fatal("consumer is still registered after the second Release")
	}
}

// TestWaitForLeavePrefersAFinishedOutcome covers the one interleaving where
// both returns of the wait are ready at once: the leave finished and the
// caller's context ended before the wait looked. The finished leave is what the
// caller has to act on, so it has to win.
func TestWaitForLeavePrefersAFinishedOutcome(t *testing.T) {
	const destination = "leave-tie"
	connection := newLeaveTestConnection(destination)
	c := newLeaveTestConsumer(t, connection, destination, 1)
	wantErr := errors.New("leave failed")
	// Start neither the leave loop nor a teardown: the outcome is staged below,
	// and a running loop would overwrite it with its own.
	c.leaveMu.Lock()
	c.leaveStarted = true
	c.leaveMu.Unlock()
	c.mu.Lock()
	c.leaveErr = wantErr
	c.mu.Unlock()
	close(c.leaveFinished)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Both cases are ready, and select picks between ready cases at random, so
	// a single pass could take the context branch by luck. Repeating it is what
	// makes a random pick fail rather than a one-in-two chance of passing.
	for i := range 100 {
		finished, err := c.waitForLeave(ctx)
		if !finished {
			t.Fatalf("waitForLeave reported the caller's context ended on iteration %d, although the leave had finished", i)
		}
		if !errors.Is(err, wantErr) {
			t.Fatalf("waitForLeave error = %v, want %v", err, wantErr)
		}
	}
}

// TestStopTearsDownAfterAFailedLeave covers a Stop with no Drain before it, so
// the leave is met inside the stop's own drain step: the consumer has to be
// torn down anyway, and the error stays visible on both channels out.
func TestStopTearsDownAfterAFailedLeave(t *testing.T) {
	const destination = "stop-failed"
	connection := newLeaveTestConnection(destination)
	c := newLeaveTestConsumer(t, connection, destination, 1)
	c.leaveFn = func(context.Context, leaveRequest) error { return kerr.CoordinatorNotAvailable }

	err := c.Stop(context.Background())
	if !errors.Is(err, kerr.CoordinatorNotAvailable) {
		t.Fatalf("Stop error = %v, want it to wrap %v", err, kerr.CoordinatorNotAvailable)
	}
	if leaveTestConsumerRegistered(c) {
		t.Fatal("consumer is still registered after a leave that finished with an error")
	}
	timer := clock.NewReal().Timer(time.Second)
	defer timer.Stop()
	select {
	case got, open := <-c.Errors():
		if !open {
			t.Fatal("Errors() closed before delivering the leave error")
		}
		if !errors.Is(got, kerr.CoordinatorNotAvailable) {
			t.Fatalf("Errors() delivered %v, want it to wrap %v", got, kerr.CoordinatorNotAvailable)
		}
	case <-timer.C:
		t.Fatal("Errors() did not deliver the leave error")
	}
	if _, open := <-c.Errors(); open {
		t.Fatal("Errors() is still open after Stop")
	}
	select {
	case _, open := <-c.Messages():
		if open {
			t.Fatal("Messages() delivered a message after Stop")
		}
	case <-timer.C:
		t.Fatal("Messages() was not closed by Stop")
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop = %v, want nil", err)
	}
}

// TestStopAfterDrainTearsDownAfterAFailedLeave covers the runner's own order:
// Drain requests the leave and reports its failure without tearing anything
// down, and the Stop that follows has to finish the job.
func TestStopAfterDrainTearsDownAfterAFailedLeave(t *testing.T) {
	const destination = "stop-after-drain"
	connection := newLeaveTestConnection(destination)
	c := newLeaveTestConsumer(t, connection, destination, 1)
	c.leaveFn = func(context.Context, leaveRequest) error { return kerr.CoordinatorNotAvailable }

	if err := c.Drain(context.Background()); !errors.Is(err, kerr.CoordinatorNotAvailable) {
		t.Fatalf("Drain error = %v, want it to wrap %v", err, kerr.CoordinatorNotAvailable)
	}
	if !leaveTestConsumerRegistered(c) {
		t.Fatal("Drain deregistered the consumer; Drain never tears down")
	}
	if err := c.Stop(context.Background()); !errors.Is(err, kerr.CoordinatorNotAvailable) {
		t.Fatalf("Stop error = %v, want it to wrap %v", err, kerr.CoordinatorNotAvailable)
	}
	if leaveTestConsumerRegistered(c) {
		t.Fatal("consumer is still registered after Stop met the finished leave in its own wait")
	}
}

// TestStopKeepsConsumerWhenCallerContextEndsFirst covers the retry path on the
// runner's order, which is the route where the stop's own wait meets the leave:
// a Drain requests the leave and gives up on it, then a Stop waits for it in
// turn. A caller that gives up on that wait has not ended the leave, so the
// consumer stays registered and a later Stop completes it.
func TestStopKeepsConsumerWhenCallerContextEndsFirst(t *testing.T) {
	const destination = "stop-context"
	connection := newLeaveTestConnection(destination)
	c := newLeaveTestConsumer(t, connection, destination, 1)
	leaveReleased := make(chan struct{})
	drainCtx, cancelDrain := context.WithCancel(context.Background())
	defer cancelDrain()
	c.leaveFn = func(context.Context, leaveRequest) error {
		// End the drain's context with the leave still in flight. The leave runs
		// on its own context, so it stays here until the test releases it.
		cancelDrain()
		<-leaveReleased
		return nil
	}

	if err := c.Drain(drainCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Drain error = %v, want %v", err, context.Canceled)
	}
	if !leaveTestConsumerRegistered(c) {
		t.Fatal("Drain deregistered the consumer; Drain never tears down")
	}

	stopCtx, cancelStop := context.WithCancel(context.Background())
	defer cancelStop()
	stopDone := make(chan error, 1)
	go func() { stopDone <- c.Stop(stopCtx) }()
	// The drain above already asked for the leave, so the only leave request
	// left is the one this Stop makes from its own wait. Taking that request is
	// what says the stop reached its own wait, which is where the context has to
	// end: ending it earlier would fail Stop's opening context check instead.
	timer := clock.NewReal().Timer(time.Second)
	defer timer.Stop()
	select {
	case <-c.leaveRequestC:
	case <-timer.C:
		t.Fatal("Stop did not reach its own leave wait")
	}
	cancelStop()
	// A second timer, not the one above: the wait it bounds can only be
	// satisfied by the stop returning after the cancel, so a timer already
	// running would be a second ready case racing that return.
	stopTimer := clock.NewReal().Timer(time.Second)
	defer stopTimer.Stop()
	var err error
	select {
	case err = <-stopDone:
	case <-stopTimer.C:
		t.Fatal("Stop did not return after its context ended")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop error = %v, want %v", err, context.Canceled)
	}
	if kind, classified := driver.Classify(err); !classified || kind != driver.KindTransient {
		t.Fatalf("Stop error kind = %v (classified %v), want %v", kind, classified, driver.KindTransient)
	}
	if !leaveTestConsumerRegistered(c) {
		t.Fatal("consumer was deregistered by a Stop whose caller context ended before the leave finished")
	}

	close(leaveReleased)
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop = %v, want nil", err)
	}
	if leaveTestConsumerRegistered(c) {
		t.Fatal("consumer is still registered after the second Stop")
	}
}

// TestStopKeepsConsumerWhenDrainContextEndsFirst covers the other route to the
// same rule: a Stop with no Drain before it meets the leave inside its own
// drain step, and a context that ends there returns as that step's error, with
// the consumer left registered for a later Stop.
func TestStopKeepsConsumerWhenDrainContextEndsFirst(t *testing.T) {
	const destination = "stop-drain-context"
	connection := newLeaveTestConnection(destination)
	c := newLeaveTestConsumer(t, connection, destination, 1)
	leaveReleased := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.leaveFn = func(context.Context, leaveRequest) error {
		cancel()
		<-leaveReleased
		return nil
	}

	err := c.Stop(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop error = %v, want %v", err, context.Canceled)
	}
	if kind, classified := driver.Classify(err); !classified || kind != driver.KindTransient {
		t.Fatalf("Stop error kind = %v (classified %v), want %v", kind, classified, driver.KindTransient)
	}
	if !leaveTestConsumerRegistered(c) {
		t.Fatal("consumer was deregistered by a Stop whose context ended inside its drain step")
	}

	close(leaveReleased)
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop = %v, want nil", err)
	}
	if leaveTestConsumerRegistered(c) {
		t.Fatal("consumer is still registered after the second Stop")
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

// leaveTestConsumerRegistered reports whether the connection still holds the
// consumer, which is the registration conn.Close refuses on.
func leaveTestConsumerRegistered(c *consumer) bool {
	c.conn.mu.RLock()
	defer c.conn.mu.RUnlock()
	_, registered := c.conn.consumers[c]
	return registered
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
