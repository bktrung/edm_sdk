package lifecycle

import (
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/dispatch"
)

func TestAccountingReadsRegistryDispositions(t *testing.T) {
	registry := dispatch.NewRegistry()
	outcomes := []dispatch.SettlementOutcome{
		dispatch.SettlementOutcomeSettled,
		dispatch.SettlementOutcomeSettled,
		dispatch.SettlementOutcomeSettled,
		dispatch.SettlementOutcomeRequeued,
		dispatch.SettlementOutcomeUnknown,
		dispatch.SettlementOutcomeAbandoned,
	}
	dispositions := []Disposition{Handled, Retried, DeadLettered, DeadLettered, Retried, Handled}
	ids := make([]uint64, len(outcomes))
	for i := range ids {
		ids[i] = registry.Add()
	}
	for i, outcome := range outcomes {
		registry.SetDisposition(ids[i], dispositions[i])
		registry.RemoveAs(ids[i], outcome)
	}

	counts := NewAccounting(registry).Snapshot()
	want := Counts{Handled: 1, Requeued: 3, Retried: 1, DeadLettered: 1}
	if counts != want {
		t.Fatalf("disposition counts = %+v, want %+v", counts, want)
	}
	if counts.Total() != uint64(len(outcomes)) {
		t.Fatalf("disposition total = %d, received = %d", counts.Total(), len(outcomes))
	}
}
