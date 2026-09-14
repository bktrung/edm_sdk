package kafka

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// Bounds for the two lock tests below. The yield bounds are what keep the
// watch in front of the emission goroutine cheap, and the lock bound is
// generous enough that an emission blocked on nothing cannot look like one
// blocked on a peer's lock on a loaded machine.
const (
	handoffLockSettleYields = 1000
	handoffLockWatchYields  = 250
	handoffLockBound        = 100 * time.Millisecond
)

// TestReciprocalHandoffEmissionsDoNotDeadlock drives an emission from one
// consumer into a second while the second consumer's mu is held, which is the
// state that consumer's own emission into the first runs in: a rebalance that
// swaps partitions between two members reaches it, because each consumer then
// emits the handoffs it queued into the peer while holding its own lock. The
// emission must take the peer's mu without holding anything, so the two can
// never take the same pair of locks in opposite orders.
func TestReciprocalHandoffEmissionsDoNotDeadlock(t *testing.T) {
	const (
		iterations  = 200
		destination = "handoff-lock-reciprocal"
		offset      = int64(11)
	)
	blocked := 0
	for range iterations {
		connection := newLeaveTestConnection(destination)
		target := newLeaveTestConsumer(t, connection, destination, 1)
		source := newLeaveTestConsumer(t, connection, destination, 1)
		key := partitionKey{destination: destination, partition: 0}
		// The source holds a live delivery for the offset, so the emission
		// finds it in the source check and hands the copy straight back: the
		// emission's whole remaining work is the lock order this test is about.
		seedOccupiedTarget(t, source, key, offset)
		if reciprocalHandoffBlocked(target, source, leaveTestMessage(destination, offset)) {
			blocked++
		}
	}
	if blocked != 0 {
		t.Fatalf("reciprocal emissions blocked on the peer lock in %d/%d iterations", blocked, iterations)
	}
}

// reciprocalHandoffBlocked reports whether an emission from target into source
// could not reach the target's mu while source's mu was held. That is the state
// a reciprocal emission puts it in: an emission that holds the target's lock
// while it waits for the source's cannot release it, so the acquire below
// cannot complete either, and a failed acquire is proof on its own, because the
// emission is the only other holder the target's lock can have here.
//
// It holds source.mu for the whole attempt and releases both locks before it
// returns, so a blocked attempt leaves no lock held by this goroutine. The
// emission starts on its own goroutine and is handed the processor before the
// target's lock is touched at all, so it reaches its first lock unobstructed;
// the watch that follows then finds the target's lock held when the emission
// took it first.
func reciprocalHandoffBlocked(target, source *consumer, message driver.InboundMessage) bool {
	source.mu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		target.emitHandoff(source, message)
	}()
	for range handoffLockSettleYields {
		runtime.Gosched()
	}
	emitterHeldTarget := false
	for range handoffLockWatchYields {
		runtime.Gosched()
		if target.mu.TryLock() {
			target.mu.Unlock()
			continue
		}
		emitterHeldTarget = true
		break
	}
	tookTarget := acquireWithin(&target.mu, handoffLockBound)
	source.mu.Unlock()
	if tookTarget {
		target.mu.Unlock()
	}
	returned := waitForClose(done, handoffLockBound)
	return !tookTarget || !returned || emitterHeldTarget
}

// TestConcurrentNackAndEmissionOfHandoffRecordDoNotRace requeues a handoff
// delivery on one goroutine while the emission path marks the same delivery on
// another. A requeue clears the marker and the emission sets it, so the two
// have to hold the same lock over that field; the race detector is what
// reports it when they do not.
//
// The two run concurrently and several fixtures run per test run, because the
// detector only reports the pair when the emission's hold of the owner lock
// starts after the requeue released it: an emission that wins the lock first
// orders its write before the requeue's read.
func TestConcurrentNackAndEmissionOfHandoffRecordDoNotRace(t *testing.T) {
	const (
		iterations  = 100
		destination = "handoff-lock-marker"
		offset      = int64(13)
	)
	for range iterations {
		connection := newLeaveTestConnection(destination)
		owner := newLeaveTestConsumer(t, connection, destination, 1)
		key := partitionKey{destination: destination, partition: 0}
		tracker := newAckTracker(offset, 1)
		if err := tracker.Track(offset); err != nil {
			t.Fatalf("Track: %v", err)
		}
		record := &kgo.Record{Topic: destination, Partition: 0, Offset: offset}
		// The state both paths need to reach the marker: the tracker still
		// holds the offset, so the emission takes its not-deliverable branch,
		// the record is known as a handoff record, and the delivery is live and
		// already carries the handoff.
		held := &settler{
			owner:            owner,
			record:           record,
			tracker:          tracker,
			handoffDelivered: true,
			key:              key,
		}
		owner.trackers[key] = tracker
		owner.trackerGenerations[key] = 1
		owner.activeGenerations[key] = 1
		owner.tenures[key] = 1
		owner.settlers[held] = struct{}{}
		owner.handoffRecords[record] = struct{}{}
		owner.recordGenerations[record] = 1
		owner.handoffMarkers[key] = map[int64]struct{}{offset: {}}

		start := make(chan struct{})
		nackDone := make(chan error, 1)
		emitDone := make(chan struct{})
		go func() {
			<-start
			nackDone <- held.Nack(context.Background(), driver.NackOptions{Requeue: true})
		}()
		go func() {
			<-start
			defer close(emitDone)
			owner.emit(record)
		}()
		close(start)

		if !waitForClose(emitDone, handoffLockBound) {
			t.Fatal("emission did not return")
		}
		timer := clock.NewReal().Timer(handoffLockBound)
		select {
		case err := <-nackDone:
			timer.Stop()
			if err != nil {
				t.Fatalf("Nack: %v", err)
			}
		case <-timer.C:
			t.Fatal("Nack did not return")
		}
	}
}

// acquireWithin takes mu on its own goroutine and reports whether the acquire
// completed within the bound. The caller releases the lock, which need not be
// the goroutine that took it. A failed acquire leaves that helper goroutine
// blocked on mu, because a mutex wait cannot be cancelled: the fixture it
// belongs to is not used again.
func acquireWithin(mu *sync.Mutex, bound time.Duration) bool {
	acquired := make(chan struct{})
	go func() {
		mu.Lock()
		close(acquired)
	}()
	timer := clock.NewReal().Timer(bound)
	defer timer.Stop()
	select {
	case <-acquired:
		return true
	case <-timer.C:
		return false
	}
}

// waitForClose reports whether done was closed within the bound.
func waitForClose(done <-chan struct{}, bound time.Duration) bool {
	timer := clock.NewReal().Timer(bound)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}
