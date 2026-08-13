package dispatch

import (
	"context"
	"sync"
	"testing"
)

func TestRegistryAccountsBothAxesExactlyOnce(t *testing.T) {
	const received = 2000

	registry := NewRegistry()
	ids := make([]uint64, received)
	var addGroup sync.WaitGroup
	for index := range ids {
		addGroup.Add(1)
		go func(index int) {
			defer addGroup.Done()
			ids[index] = registry.Add()
		}(index)
	}
	addGroup.Wait()

	var wantSettlements SettlementCounts
	outcomes := []SettlementOutcome{SettlementOutcomeSettled, SettlementOutcomeRequeued, SettlementOutcomeUnknown, SettlementOutcomeAbandoned}
	dispositions := []Disposition{DispositionHandled, DispositionRequeued, DispositionRetried, DispositionDeadLettered}
	var wantDispositions DispositionCounts
	for index := range ids {
		outcome := outcomes[index%len(outcomes)]
		switch outcome {
		case SettlementOutcomeSettled:
			wantSettlements.Settled++
			switch dispositions[(index/len(outcomes))%len(dispositions)] {
			case DispositionHandled:
				wantDispositions.Handled++
			case DispositionRequeued:
				wantDispositions.Requeued++
			case DispositionRetried:
				wantDispositions.Retried++
			case DispositionDeadLettered:
				wantDispositions.DeadLettered++
			}
		case SettlementOutcomeRequeued:
			wantSettlements.Requeued++
			wantDispositions.Requeued++
		case SettlementOutcomeUnknown:
			wantSettlements.Unknown++
			wantDispositions.Requeued++
		case SettlementOutcomeAbandoned:
			wantSettlements.Abandoned++
			wantDispositions.Requeued++
		}
	}

	var removeGroup sync.WaitGroup
	for index, id := range ids {
		removeGroup.Add(1)
		go func(index int, id uint64) {
			defer removeGroup.Done()
			registry.SetDisposition(id, dispositions[(index/len(outcomes))%len(dispositions)])
			registry.RemoveAs(id, outcomes[index%len(outcomes)])
			registry.RemoveAs(id, SettlementOutcomeAbandoned)
		}(index, id)
	}
	removeGroup.Wait()

	counts := registry.Counts()
	if counts.Received != received {
		t.Fatalf("received = %d, want %d", counts.Received, received)
	}
	if counts.Settlements != wantSettlements {
		t.Fatalf("settlement counts = %+v, want %+v", counts.Settlements, wantSettlements)
	}
	if counts.Dispositions != wantDispositions {
		t.Fatalf("disposition counts = %+v, want %+v", counts.Dispositions, wantDispositions)
	}
	if counts.Settlements.Total() != counts.Received {
		t.Fatalf("settlement total = %d, received = %d", counts.Settlements.Total(), counts.Received)
	}
	if counts.Dispositions.Total() != counts.Received {
		t.Fatalf("disposition total = %d, received = %d", counts.Dispositions.Total(), counts.Received)
	}
	if registry.Len() != 0 {
		t.Fatalf("registry length = %d, want zero", registry.Len())
	}
	if err := registry.WaitZero(context.Background()); err != nil {
		t.Fatalf("WaitZero() = %v", err)
	}
}
