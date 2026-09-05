package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestAckTrackerInOrderAcks(t *testing.T) {
	tracker := newAckTracker(10, 1)
	for offset := int64(10); offset < 15; offset++ {
		if err := tracker.Track(offset); err != nil {
			t.Fatalf("Track(%d): %v", offset, err)
		}
		if err := tracker.Ack(offset, nil); err != nil {
			t.Fatalf("Ack(%d): %v", offset, err)
		}
		if got := tracker.CommitPoint(); got != offset+1 {
			t.Fatalf("CommitPoint after Ack(%d) = %d, want %d", offset, got, offset+1)
		}
	}
	if got := tracker.Unacked(); got != 0 {
		t.Fatalf("Unacked = %d, want 0", got)
	}
	if got := tracker.Gap(); got != 0 {
		t.Fatalf("Gap = %d, want 0", got)
	}
}

func TestAckTrackerOutOfOrderAcks(t *testing.T) {
	tracker := newAckTracker(0, 1)
	for offset := range 4 {
		if err := tracker.Track(int64(offset)); err != nil {
			t.Fatalf("Track(%d): %v", offset, err)
		}
	}
	if err := tracker.Ack(3, nil); err != nil {
		t.Fatalf("Ack(3): %v", err)
	}
	if got := tracker.CommitPoint(); got != 0 {
		t.Fatalf("CommitPoint after Ack(3) = %d, want 0", got)
	}
	if got := tracker.Gap(); got != 3 {
		t.Fatalf("Gap after Ack(3) = %d, want 3", got)
	}
	if err := tracker.Ack(1, nil); err != nil {
		t.Fatalf("Ack(1): %v", err)
	}
	if got := tracker.CommitPoint(); got != 0 {
		t.Fatalf("CommitPoint after Ack(1) = %d, want 0", got)
	}
	if err := tracker.Ack(0, nil); err != nil {
		t.Fatalf("Ack(0): %v", err)
	}
	if got := tracker.CommitPoint(); got != 2 {
		t.Fatalf("CommitPoint after Ack(0) = %d, want 2", got)
	}
	if got := tracker.Gap(); got != 1 {
		t.Fatalf("Gap after Ack(0) = %d, want 1", got)
	}
	if err := tracker.Ack(2, nil); err != nil {
		t.Fatalf("Ack(2): %v", err)
	}
	if got := tracker.CommitPoint(); got != 4 {
		t.Fatalf("CommitPoint after Ack(2) = %d, want 4", got)
	}
	if got := tracker.Gap(); got != 0 {
		t.Fatalf("Gap after Ack(2) = %d, want 0", got)
	}
}

func TestAckTrackerRandomInterleaving(t *testing.T) {
	const (
		base  = int64(100)
		count = 1000
	)
	tracker := newAckTracker(base, 1)
	for offset := base; offset < base+count; offset++ {
		if err := tracker.Track(offset); err != nil {
			t.Fatalf("Track(%d): %v", offset, err)
		}
	}
	offsets := make([]int64, count)
	for index := range offsets {
		offsets[index] = base + int64(index)
	}
	interleaved := make([]int64, count)
	for index := range interleaved {
		// 379 is coprime with 1000, so this visits every offset once.
		interleaved[index] = offsets[(index*379)%count]
	}
	offsets = interleaved
	for _, offset := range offsets {
		if err := tracker.Ack(offset, nil); err != nil {
			t.Fatalf("Ack(%d): %v", offset, err)
		}
	}
	if got := tracker.CommitPoint(); got != base+count {
		t.Fatalf("CommitPoint = %d, want %d", got, base+count)
	}
	if got := tracker.Gap(); got != 0 {
		t.Fatalf("Gap = %d, want 0", got)
	}
	if got := tracker.Unacked(); got != 0 {
		t.Fatalf("Unacked = %d, want 0", got)
	}
}

func TestAckTrackerRejectsDuplicateAck(t *testing.T) {
	tracker := newAckTracker(0, 1)
	if err := tracker.Track(0); err != nil {
		t.Fatalf("Track(0): %v", err)
	}
	if err := tracker.Ack(0, nil); err != nil {
		t.Fatalf("first Ack(0): %v", err)
	}
	if err := tracker.Ack(0, nil); !errors.Is(err, errAckTrackerAlreadySettled) {
		t.Fatalf("second Ack(0) = %v, want duplicate error", err)
	}
	if got := tracker.CommitPoint(); got != 1 {
		t.Fatalf("CommitPoint = %d, want 1", got)
	}
}

func TestAckTrackerRollbackOnCommitFailure(t *testing.T) {
	tracker := newAckTracker(0, 1)
	for offset := range 2 {
		if err := tracker.Track(int64(offset)); err != nil {
			t.Fatalf("Track(%d): %v", offset, err)
		}
	}
	if err := tracker.Ack(1, nil); err != nil {
		t.Fatalf("Ack(1): %v", err)
	}
	commitErr := errors.New("commit failed")
	if err := tracker.Ack(0, func(int64) error { return commitErr }); !errors.Is(err, commitErr) {
		t.Fatalf("Ack(0) = %v, want commit failure", err)
	}
	if got := tracker.CommitPoint(); got != 0 {
		t.Fatalf("CommitPoint after failed commit = %d, want 0", got)
	}
	if got := tracker.Gap(); got != 1 {
		t.Fatalf("Gap after failed commit = %d, want 1", got)
	}
	if got := tracker.Unacked(); got != 1 {
		t.Fatalf("Unacked after failed commit = %d, want 1", got)
	}
	if err := tracker.Ack(0, nil); err != nil {
		t.Fatalf("retry Ack(0): %v", err)
	}
	if got := tracker.CommitPoint(); got != 2 {
		t.Fatalf("CommitPoint after retry = %d, want 2", got)
	}
}

func TestAckTrackerRequeueRetainsUnackedOffset(t *testing.T) {
	tracker := newAckTracker(0, 1)
	if err := tracker.Track(0); err != nil {
		t.Fatalf("Track(0): %v", err)
	}
	if err := tracker.Release(0); err != nil {
		t.Fatalf("Release(0): %v", err)
	}
	if got, ok := tracker.lowestRequeue(); !ok || got != 0 {
		t.Fatalf("lowestRequeue = %d, %t, want 0, true", got, ok)
	}
	if got := tracker.Unacked(); got != 1 {
		t.Fatalf("Unacked after Release = %d, want 1", got)
	}
	reused, deliver, err := tracker.TrackRedelivery(0)
	if err != nil {
		t.Fatalf("TrackRedelivery(0): %v", err)
	}
	if !reused || !deliver {
		t.Fatalf("TrackRedelivery(0) = reused %t deliver %t, want true true", reused, deliver)
	}
	if got := tracker.Unacked(); got != 1 {
		t.Fatalf("Unacked after redelivery = %d, want 1", got)
	}
	if err := tracker.Ack(0, nil); err != nil {
		t.Fatalf("Ack(0): %v", err)
	}
	if got := tracker.Unacked(); got != 0 {
		t.Fatalf("Unacked after redelivery Ack = %d, want 0", got)
	}
}

func TestAckTrackerLowestRequeue(t *testing.T) {
	tracker := newAckTracker(10, 1)
	for offset := int64(10); offset <= 11; offset++ {
		if err := tracker.Track(offset); err != nil {
			t.Fatalf("Track(%d): %v", offset, err)
		}
	}
	if err := tracker.Release(11); err != nil {
		t.Fatalf("Release(11): %v", err)
	}
	if err := tracker.Release(10); err != nil {
		t.Fatalf("Release(10): %v", err)
	}
	if got, ok := tracker.lowestRequeue(); !ok || got != 10 {
		t.Fatalf("lowestRequeue = %d, %t, want 10, true", got, ok)
	}
	reused, deliver, err := tracker.TrackRedelivery(10)
	if err != nil || !reused || !deliver {
		t.Fatalf("TrackRedelivery(10) = %t, %t, %v, want true, true, nil", reused, deliver, err)
	}
	if got, ok := tracker.lowestRequeue(); !ok || got != 11 {
		t.Fatalf("lowestRequeue after offset 10 = %d, %t, want 11, true", got, ok)
	}
}

func TestAckTrackerGenerationTombstone(t *testing.T) {
	tracker := newAckTracker(4, 9)
	if got := tracker.Generation(); got != 9 {
		t.Fatalf("Generation = %d, want 9", got)
	}
	if err := tracker.Track(4); err != nil {
		t.Fatalf("Track(4): %v", err)
	}
	tracker.Drop()
	if err := tracker.Ack(4, nil); !errors.Is(err, ErrRevoked) {
		t.Fatalf("Ack after Drop = %v, want ErrRevoked", err)
	}
	if err := tracker.Release(4); !errors.Is(err, ErrRevoked) {
		t.Fatalf("Release after Drop = %v, want ErrRevoked", err)
	}
	if err := tracker.Track(5); !errors.Is(err, ErrRevoked) {
		t.Fatalf("Track after Drop = %v, want ErrRevoked", err)
	}
	if got := tracker.CommitPoint(); got != 4 {
		t.Fatalf("CommitPoint after Drop = %d, want 4", got)
	}
	if got := tracker.Unacked(); got != 1 {
		t.Fatalf("Unacked after Drop = %d, want 1", got)
	}
}

func TestAckTrackerConcurrentAcks(t *testing.T) {
	const count = 1000
	tracker := newAckTracker(0, 1)
	for offset := range count {
		if err := tracker.Track(int64(offset)); err != nil {
			t.Fatalf("Track(%d): %v", offset, err)
		}
	}
	var group sync.WaitGroup
	for offset := range count {
		group.Add(1)
		go func(offset int) {
			defer group.Done()
			if err := tracker.Ack(int64(offset), nil); err != nil {
				t.Errorf("Ack(%d): %v", offset, err)
			}
		}(offset)
	}
	group.Wait()
	if got := tracker.CommitPoint(); got != count {
		t.Fatalf("CommitPoint = %d, want %d", got, count)
	}
	if got := tracker.Unacked(); got != 0 {
		t.Fatalf("Unacked = %d, want 0", got)
	}
}

func TestAckTrackerMaxGapAndOptions(t *testing.T) {
	if got, err := resolveMaxAckGap(nil); err != nil || got != defaultKafkaMaxAckGap {
		t.Fatalf("resolveMaxAckGap(nil) = %d, %v, want %d, nil", got, err, defaultKafkaMaxAckGap)
	}
	if got, err := resolveMaxAckGap(map[string]string{"kafka.maxAckGap": "7"}); err != nil || got != 7 {
		t.Fatalf("resolveMaxAckGap(7) = %d, %v, want 7, nil", got, err)
	}
	for _, value := range []string{"", "0", "-1", "9223372036854775808"} {
		if _, err := resolveMaxAckGap(map[string]string{"kafka.maxAckGap": value}); err == nil {
			t.Fatalf("resolveMaxAckGap(%q) returned nil error", value)
		}
	}
}

func TestConsumerTrackerGenerationAndCounter(t *testing.T) {
	const destination = "topic"
	consumer := &consumer{
		trackers:     make(map[partitionKey]*ackTracker),
		settlers:     make(map[*settler]struct{}),
		unsettled:    map[string]int{destination: 2},
		budgets:      map[string]int{destination: 2},
		pauseReasons: make(map[string]pauseReasonSet),
	}
	firstRecord := &kgo.Record{Topic: destination, Partition: 0, Offset: 10}
	consumer.mu.Lock()
	first := consumer.trackerForLocked(firstRecord)
	consumer.mu.Unlock()
	if first.Generation() != 1 {
		t.Fatalf("first tracker generation = %d, want 1", first.Generation())
	}
	if err := first.Track(10); err != nil {
		t.Fatalf("Track(10): %v", err)
	}
	if err := first.Track(11); err != nil {
		t.Fatalf("Track(11): %v", err)
	}
	if got := first.Unacked(); got != consumer.unsettled[destination] {
		t.Fatalf("tracker unacked = %d, consumer unsettled = %d", got, consumer.unsettled[destination])
	}
	racingSettler := &settler{tracker: first, record: &kgo.Record{Topic: destination, Partition: 0, Offset: 10}}
	consumer.mu.Lock()
	consumer.settlers[racingSettler] = struct{}{}
	consumer.mu.Unlock()
	if err := first.Ack(10, nil); err != nil {
		t.Fatalf("Ack(10) before drop: %v", err)
	}

	consumer.dropTracker(destination, 0)
	if got := consumer.unsettled[destination]; got != 0 {
		t.Fatalf("unsettled after tracker drop = %d, want 0", got)
	}
	consumer.mu.Lock()
	second := consumer.trackerForLocked(&kgo.Record{Topic: destination, Partition: 0, Offset: 10})
	consumer.mu.Unlock()
	if second.Generation() != 2 {
		t.Fatalf("second tracker generation = %d, want 2", second.Generation())
	}
	if second == first {
		t.Fatal("tracker was reused after drop")
	}
}

func TestSettlerRejectsTombstonedTracker(t *testing.T) {
	tracker := newAckTracker(0, 1)
	tracker.Drop()
	settler := &settler{
		owner:   &consumer{},
		record:  &kgo.Record{Topic: "topic", Partition: 0, Offset: 0},
		tracker: tracker,
		key:     partitionKey{destination: "topic", partition: 0},
	}
	err := settler.Ack(context.Background())
	if !errors.Is(err, ErrRevoked) {
		t.Fatalf("Ack on dropped tracker = %v, want ErrRevoked", err)
	}
	if errors.Is(err, driver.ErrAlreadySettled) {
		t.Fatalf("Ack on dropped tracker = %v, unexpectedly already-settled", err)
	}
}
