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

// ackTracker is the committed cursor for one owned partition. Ack advances the
// cursor one offset at a time; Drop permanently rejects settlements from the
// ownership that created the tracker.
type ackTracker struct {
	mu       sync.Mutex
	commitMu sync.Mutex
	base     int64
	revoked  bool
}

func newAckTracker(base int64) *ackTracker {
	return &ackTracker{base: base}
}

// Ack advances the cursor through offset and commits the next offset. A failed
// commit restores the old cursor unless Drop won the race while the broker call
// was in flight.
func (t *ackTracker) Ack(offset int64, commit func(int64) error) error {
	t.commitMu.Lock()
	defer t.commitMu.Unlock()

	t.mu.Lock()
	if t.revoked {
		t.mu.Unlock()
		return ErrRevoked
	}
	if offset != t.base {
		t.mu.Unlock()
		return errAckTrackerAlreadySettled
	}
	oldBase := t.base
	t.base = offset + 1
	t.mu.Unlock()

	if commit == nil {
		return nil
	}
	if err := commit(offset + 1); err != nil {
		t.mu.Lock()
		revoked := t.revoked
		if !revoked {
			t.base = oldBase
		}
		t.mu.Unlock()
		if revoked {
			return ErrRevoked
		}
		return err
	}
	return nil
}

// Drop revokes this tracker without waiting for an in-flight commit.
func (t *ackTracker) Drop() {
	t.mu.Lock()
	t.revoked = true
	t.mu.Unlock()
}

// CommitPoint returns the next offset this tracker has not committed.
func (t *ackTracker) CommitPoint() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.base
}
