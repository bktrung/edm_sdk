package dispatch

import (
	"context"
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

func TestRegistryWaitForSubset(t *testing.T) {
	registry := NewRegistry()
	first, second := registry.Add(), registry.Add()
	registry.Remove(first)
	if err := registry.WaitFor(context.Background(), []uint64{first}); err != nil {
		t.Fatal(err)
	}
	if registry.Len() != 1 {
		t.Fatalf("registry len = %d, want 1", registry.Len())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := registry.WaitFor(ctx, []uint64{second}); err == nil {
		t.Fatal("WaitFor must respect cancellation")
	}
	registry.Remove(second)
}

func TestRegistryNilAndDuplicateRemoval(t *testing.T) {
	var registry *Registry
	if registry.Len() != 0 || registry.WaitZero(context.Background()) != nil || registry.WaitFor(context.Background(), nil) != nil {
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
