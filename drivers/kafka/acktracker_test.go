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

func TestAckTrackerRoutedAckRejectsGap(t *testing.T) {
	tracker := newAckTracker(10)
	if err := tracker.Ack(12, nil); !errors.Is(err, errAckTrackerAlreadySettled) {
		t.Fatalf("Ack(12) = %v, want cursor error", err)
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

func TestAckTrackerOwnAckAdvancesAcrossGap(t *testing.T) {
	tracker := newAckTracker(10)
	var committed int64
	if err := tracker.AckOwn(12, func(offset int64) error {
		committed = offset
		return nil
	}); err != nil {
		t.Fatalf("AckOwn(12) = %v, want success across the own-tracker gap", err)
	}
	if committed != 13 {
		t.Fatalf("committed offset = %d, want 13", committed)
	}
	if got := tracker.CommitPoint(); got != 13 {
		t.Fatalf("CommitPoint after AckOwn(12) = %d, want 13", got)
	}
}

func TestAckTrackerRejectsSettledOffsetOnBothPaths(t *testing.T) {
	for _, tc := range []struct {
		name string
		ack  func(*ackTracker) error
	}{
		{name: "routed", ack: func(tracker *ackTracker) error { return tracker.Ack(9, nil) }},
		{name: "own", ack: func(tracker *ackTracker) error { return tracker.AckOwn(9, nil) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracker := newAckTracker(10)
			if err := tc.ack(tracker); !errors.Is(err, errAckTrackerAlreadySettled) {
				t.Fatalf("ack below base = %v, want cursor error", err)
			}
			if got := tracker.CommitPoint(); got != 10 {
				t.Fatalf("CommitPoint after rejected ack = %d, want 10", got)
			}
		})
	}
}

func TestAckTrackerOwnAckRollbackOnGapCommitFailure(t *testing.T) {
	tracker := newAckTracker(10)
	commitErr := errors.New("commit failed")
	var attempted int64
	if err := tracker.AckOwn(12, func(offset int64) error {
		attempted = offset
		return commitErr
	}); !errors.Is(err, commitErr) {
		t.Fatalf("AckOwn(12) = %v, want commit failure", err)
	}
	if attempted != 13 {
		t.Fatalf("failed commit offset = %d, want 13", attempted)
	}
	if got := tracker.CommitPoint(); got != 10 {
		t.Fatalf("CommitPoint after failed gap commit = %d, want 10", got)
	}
	var committed int64
	if err := tracker.AckOwn(12, func(offset int64) error {
		committed = offset
		return nil
	}); err != nil {
		t.Fatalf("retry AckOwn(12): %v", err)
	}
	if committed != 13 {
		t.Fatalf("retry committed offset = %d, want 13", committed)
	}
	if got := tracker.CommitPoint(); got != 13 {
		t.Fatalf("CommitPoint after retry = %d, want 13", got)
	}
}
