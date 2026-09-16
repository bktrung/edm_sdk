package kafka

import (
	"errors"
	"sync"
)

var (
	// ErrRevoked reports that a settlement targeted a dropped partition assignment.
	ErrRevoked = errors.New("kafka: partition assignment revoked")

	errAckTrackerAlreadySettled = errors.New("kafka: offset already acknowledged")
)

type ackTracker struct {
	// mu guards internal offset bookkeeping state: base, acked, outstanding,
	// requeued, and the revoked tombstone flag.
	mu sync.Mutex
	// commitMu serializes Ack's commit callback and failure rollback for this
	// partition so that broker commits never regress and a rollback cannot trample
	// a concurrent advance. It is deliberately not acquired by Track or Drop,
	// ensuring admission and tombstoning never wait on broker network I/O.
	commitMu    sync.Mutex
	base        int64
	acked       map[int64]struct{}
	outstanding map[int64]int
	requeued    map[int64]int
	revoked     bool
}

func newAckTracker(base int64) *ackTracker {
	return &ackTracker{
		base:        base,
		acked:       make(map[int64]struct{}),
		outstanding: make(map[int64]int),
		requeued:    make(map[int64]int),
	}
}

func (t *ackTracker) Track(offset int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.revoked {
		return ErrRevoked
	}
	t.outstanding[offset]++
	return nil
}

func (t *ackTracker) TrackRedelivery(offset int64) (reused, deliver bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.revoked {
		return false, false, ErrRevoked
	}
	if t.requeued[offset] > 0 {
		t.requeued[offset]--
		if t.requeued[offset] == 0 {
			delete(t.requeued, offset)
		}
		return true, true, nil
	}
	if offset < t.base || t.outstanding[offset] > 0 {
		return false, false, nil
	}
	if _, exists := t.acked[offset]; exists {
		return false, false, nil
	}
	t.outstanding[offset]++
	return false, true, nil
}

func (t *ackTracker) Ack(offset int64, commit func(int64) error) error {
	t.commitMu.Lock()
	defer t.commitMu.Unlock()

	t.mu.Lock()
	if t.revoked {
		t.mu.Unlock()
		return ErrRevoked
	}
	if offset < t.base {
		t.mu.Unlock()
		return errAckTrackerAlreadySettled
	}
	if _, exists := t.acked[offset]; exists {
		t.mu.Unlock()
		return errAckTrackerAlreadySettled
	}

	hadOutstanding := t.outstanding[offset] > 0
	if hadOutstanding {
		t.outstanding[offset]--
		if t.outstanding[offset] == 0 {
			delete(t.outstanding, offset)
		}
	}

	oldBase := t.base
	t.acked[offset] = struct{}{}
	advanced := make([]int64, 0, 1)
	for {
		if _, exists := t.acked[t.base]; !exists {
			break
		}
		delete(t.acked, t.base)
		advanced = append(advanced, t.base)
		t.base++
	}
	commitPoint := t.base
	t.mu.Unlock()

	if len(advanced) > 0 && commit != nil {
		if err := commit(commitPoint); err != nil {
			t.mu.Lock()
			revoked := t.revoked
			t.base = oldBase
			for _, advancedOffset := range advanced {
				t.acked[advancedOffset] = struct{}{}
			}
			delete(t.acked, offset)
			if hadOutstanding {
				t.outstanding[offset]++
			}
			t.mu.Unlock()
			if revoked {
				return ErrRevoked
			}
			return err
		}
	}
	return nil
}

func (t *ackTracker) Release(offset int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.revoked {
		return ErrRevoked
	}
	if t.outstanding[offset] > 0 {
		t.requeued[offset]++
	}
	return nil
}

func (t *ackTracker) lowestRequeue() (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var lowest int64
	haveLowest := false
	for offset, count := range t.requeued {
		if count > 0 && (!haveLowest || offset < lowest) {
			lowest = offset
			haveLowest = true
		}
	}
	return lowest, haveLowest
}

func (t *ackTracker) hasRequeue(offset int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.requeued[offset] > 0
}

func (t *ackTracker) Drop() {
	t.mu.Lock()
	t.revoked = true
	t.mu.Unlock()
}

func (t *ackTracker) CommitPoint() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.base
}

func (t *ackTracker) Gap() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var highest int64
	haveHighest := false
	for offset := range t.acked {
		if !haveHighest || offset > highest {
			highest = offset
			haveHighest = true
		}
	}
	if !haveHighest || highest <= t.base {
		return 0
	}
	return highest - t.base
}

func (t *ackTracker) holds(offset int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.outstanding[offset] > 0
}

func (t *ackTracker) Unacked() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	count := 0
	for _, outstanding := range t.outstanding {
		count += outstanding
	}
	return count
}
