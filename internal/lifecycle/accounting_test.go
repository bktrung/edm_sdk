package lifecycle

import (
	"sync"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/dispatch"
)

func TestAccountingCountsEveryDisposition(t *testing.T) {
	var accounting Accounting
	dispositions := []Disposition{Handled, Requeued, Retried, DeadLettered}
	const workers = 8
	const each = 1000
	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func(offset int) {
			defer group.Done()
			for i := 0; i < each; i++ {
				accounting.Record(dispositions[(offset+i)%len(dispositions)])
			}
		}(worker)
	}
	group.Wait()
	counts := accounting.Snapshot()
	if counts.Total() != workers*each {
		t.Fatalf("accounting total = %d, want %d", counts.Total(), workers*each)
	}
}

func TestAccountingNilAndUnknownDisposition(t *testing.T) {
	var accounting *Accounting
	accounting.Record(Handled)
	if got := accounting.Snapshot(); got.Total() != 0 {
		t.Fatalf("nil accounting snapshot = %+v", got)
	}
	var value Accounting
	value.Record(Disposition(99))
	if got := value.Snapshot(); got.Total() != 0 {
		t.Fatalf("unknown disposition snapshot = %+v", got)
	}
}

func TestAccountingSettlementMappingSumsToReceived(t *testing.T) {
	var accounting Accounting
	received := []struct {
		outcome     dispatch.SettlementOutcome
		disposition Disposition
	}{
		{outcome: dispatch.SettlementOutcomeSettled, disposition: Handled},
		{outcome: dispatch.SettlementOutcomeSettled, disposition: Retried},
		{outcome: dispatch.SettlementOutcomeSettled, disposition: DeadLettered},
		{outcome: dispatch.SettlementOutcomeRequeued, disposition: Handled},
		{outcome: dispatch.SettlementOutcomeUnknown, disposition: Handled},
		{outcome: dispatch.SettlementOutcomeAbandoned, disposition: Handled},
	}
	registry := dispatch.NewRegistry()
	ids := make([]uint64, len(received))
	for i := range ids {
		ids[i] = registry.Add()
	}
	for i, message := range received {
		registry.RemoveAs(ids[i], message.outcome)
		accounting.RecordSettlement(message.outcome, message.disposition)
	}
	counts := accounting.Snapshot()
	if counts.Total() != uint64(len(received)) {
		t.Fatalf("lifecycle total = %d, received = %d", counts.Total(), len(received))
	}
	if counts.Handled != 1 || counts.Requeued != 3 || counts.Retried != 1 || counts.DeadLettered != 1 {
		t.Fatalf("lifecycle counts = %+v", counts)
	}
	settlements := registry.Counts()
	if settlements.Received != uint64(len(received)) || settlements.Settled != 3 || settlements.Requeued != 1 || settlements.Unknown != 1 || settlements.Abandoned != 1 {
		t.Fatalf("settlement counts = %+v", settlements)
	}
	if total := settlements.Settled + settlements.Requeued + settlements.Unknown + settlements.Abandoned; total != settlements.Received {
		t.Fatalf("settlement total = %d, received = %d", total, settlements.Received)
	}
}
