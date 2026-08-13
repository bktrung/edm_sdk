package f1

import (
	"context"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// TestInflightRegistryAccountsEverySettlementOutcome verifies all named outcomes sum to receipt.
func TestInflightRegistryAccountsEverySettlementOutcome(t *testing.T) {
	r := newInflightRegistry()
	ids := make([]uint64, 5)
	for i := range ids {
		ids[i] = r.Add(driver.InboundMessage{})
	}
	outcomes := []settlementOutcome{
		settlementOutcomeSettled,
		settlementOutcomeRequeued,
		settlementOutcomeUnknown,
		settlementOutcomeAbandoned,
		settlementOutcomeSettled,
	}
	for i, outcome := range outcomes {
		r.RemoveAs(ids[i], outcome)
	}

	counts := r.Counts()
	if counts.received != 5 {
		t.Fatalf("received = %d, want 5", counts.received)
	}
	total := counts.settled + counts.requeued + counts.unknown + counts.abandoned
	if total != counts.received {
		t.Fatalf("outcome total = %d, received = %d", total, counts.received)
	}
	if r.Len() != 0 {
		t.Fatalf("registry length = %d, want zero", r.Len())
	}
	if err := r.WaitZero(context.Background()); err != nil {
		t.Fatalf("WaitZero() = %v", err)
	}
}
