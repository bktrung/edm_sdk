package dispatch

import (
	"context"
	"errors"
	"testing"
)

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
	if got := value.Len(); got != 0 {
		t.Fatalf("registry length after duplicate removal = %d, want zero", got)
	}
	if err := value.WaitZero(context.Background()); err != nil {
		t.Fatalf("WaitZero() = %v", err)
	}
}

func TestRegistryAddRemoveLenAndCancellation(t *testing.T) {
	var nilRegistry *Registry
	if nilRegistry.Add() != 0 {
		t.Fatal("nil registry should ignore Add")
	}

	registry := NewRegistry()
	first := registry.Add()
	second := registry.Add()
	if got := registry.Len(); got != 2 {
		t.Fatalf("registry length after two Add = %d, want 2", got)
	}
	registry.Remove(first)
	if got := registry.Len(); got != 1 {
		t.Fatalf("registry length after Remove = %d, want 1", got)
	}
	registry.Remove(second)
	if got := registry.Len(); got != 0 {
		t.Fatalf("registry length after both Remove = %d, want 0", got)
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
	if err := pending.WaitZero(context.Background()); err != nil {
		t.Fatalf("WaitZero() = %v after the delivery settled", err)
	}
}
