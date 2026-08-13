package dispatch

import (
	"context"
	"errors"
	"testing"
)

func TestRegistryAccountsEverySettlementOutcome(t *testing.T) {
	registry := NewRegistry()
	ids := make([]uint64, 5)
	for i := range ids {
		ids[i] = registry.Add()
	}
	outcomes := []SettlementOutcome{
		SettlementOutcomeSettled,
		SettlementOutcomeRequeued,
		SettlementOutcomeUnknown,
		SettlementOutcomeAbandoned,
		SettlementOutcomeSettled,
	}
	for i, outcome := range outcomes {
		registry.RemoveAs(ids[i], outcome)
	}
	counts := registry.Counts()
	if counts.Received != 5 {
		t.Fatalf("received = %d, want 5", counts.Received)
	}
	if got := counts.Settlements.Settled + counts.Settlements.Requeued + counts.Settlements.Unknown + counts.Settlements.Abandoned; got != counts.Received {
		t.Fatalf("outcome total = %d, received = %d", got, counts.Received)
	}
	if registry.Len() != 0 {
		t.Fatalf("registry length = %d, want zero", registry.Len())
	}
	if err := registry.WaitZero(context.Background()); err != nil {
		t.Fatalf("WaitZero() = %v", err)
	}
}

func TestRegistryNilAndDuplicateRemoval(t *testing.T) {
	var registry *Registry
	if registry.Len() != 0 || registry.WaitZero(context.Background()) != nil {
		t.Fatal("nil registry should be empty and immediately complete")
	}
	value := NewRegistry()
	value.Remove(999)
	id := value.Add()
	value.Remove(id)
	value.Remove(id)
	if value.Counts().Received != 1 || value.Counts().Settlements.Settled != 1 {
		t.Fatalf("duplicate removal changed counts: %+v", value.Counts())
	}
}

func TestRegistryRejectsInvalidInputsAndWaitCancellation(t *testing.T) {
	var nilRegistry *Registry
	if nilRegistry.Add() != 0 || nilRegistry.Counts() != (Counts{}) {
		t.Fatal("nil registry should ignore Add and Counts")
	}
	nilRegistry.SetDisposition(1, DispositionHandled)
	nilRegistry.RemoveAs(1, SettlementOutcomeSettled)

	registry := NewRegistry()
	id := registry.Add()
	registry.SetDisposition(id, Disposition(255))
	registry.SetDisposition(id+1, DispositionHandled)
	registry.RemoveAs(id, SettlementOutcome(255))
	counts := registry.Counts()
	if counts.Settlements.Unknown != 1 || counts.Dispositions.Requeued != 1 {
		t.Fatalf("invalid outcome counts = %+v, want one unknown requeue", counts)
	}
	if err := registry.WaitZero(context.Background()); err != nil {
		t.Fatalf("WaitZero() = %v", err)
	}

	pending := NewRegistry()
	pending.Add()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := pending.WaitZero(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitZero(cancelled) = %v, want context canceled", err)
	}
	pending.Remove(1)
}
