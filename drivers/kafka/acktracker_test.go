package kafka

import (
	"errors"
	"testing"
)

func TestAckTrackerInOrderAcks(t *testing.T) {
	tracker := newAckTracker(10)
	for offset := int64(10); offset < 15; offset++ {
		if err := tracker.Ack(offset, nil); err != nil {
			t.Fatalf("Ack(%d): %v", offset, err)
		}
		if got := tracker.CommitPoint(); got != offset+1 {
			t.Fatalf("CommitPoint after Ack(%d) = %d, want %d", offset, got, offset+1)
		}
	}
}

func TestAckTrackerRejectsOutOfOrderAck(t *testing.T) {
	tracker := newAckTracker(10)
	if err := tracker.Ack(11, nil); !errors.Is(err, errAckTrackerAlreadySettled) {
		t.Fatalf("Ack(11) = %v, want cursor error", err)
	}
	if got := tracker.CommitPoint(); got != 10 {
		t.Fatalf("CommitPoint after rejected Ack = %d, want 10", got)
	}
}

func TestAckTrackerRollbackOnCommitFailure(t *testing.T) {
	tracker := newAckTracker(0)
	commitErr := errors.New("commit failed")
	if err := tracker.Ack(0, func(int64) error { return commitErr }); !errors.Is(err, commitErr) {
		t.Fatalf("Ack(0) = %v, want commit failure", err)
	}
	if got := tracker.CommitPoint(); got != 0 {
		t.Fatalf("CommitPoint after failed commit = %d, want 0", got)
	}
	if err := tracker.Ack(0, nil); err != nil {
		t.Fatalf("retry Ack(0): %v", err)
	}
	if got := tracker.CommitPoint(); got != 1 {
		t.Fatalf("CommitPoint after retry = %d, want 1", got)
	}
}

func TestAckTrackerDropRevokesSettlements(t *testing.T) {
	tracker := newAckTracker(4)
	tracker.Drop()
	if err := tracker.Ack(4, nil); !errors.Is(err, ErrRevoked) {
		t.Fatalf("Ack after Drop = %v, want ErrRevoked", err)
	}
	if got := tracker.CommitPoint(); got != 4 {
		t.Fatalf("CommitPoint after Drop = %d, want 4", got)
	}
}

func TestAckTrackerRevocationWinsFailedCommit(t *testing.T) {
	tracker := newAckTracker(0)
	commitEntered := make(chan struct{})
	commitRelease := make(chan struct{})
	ackDone := make(chan error, 1)
	go func() {
		ackDone <- tracker.Ack(0, func(int64) error {
			close(commitEntered)
			<-commitRelease
			return errors.New("commit failed")
		})
	}()
	<-commitEntered
	tracker.Drop()
	close(commitRelease)
	if err := <-ackDone; !errors.Is(err, ErrRevoked) {
		t.Fatalf("Ack after revoked commit failure = %v, want ErrRevoked", err)
	}
	if got := tracker.CommitPoint(); got != 1 {
		t.Fatalf("CommitPoint after revoked commit failure = %d, want 1", got)
	}
}
