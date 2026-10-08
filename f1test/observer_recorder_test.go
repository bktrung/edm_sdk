package f1test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
)

func TestRecorderCallsAreOrderedAndCopied(t *testing.T) {
	recorder := NewRecorder()
	ctx := context.Background()
	start := f1.StartEvent{Kind: f1.ObserverProcess, At: time.Unix(1, 2), EventType: "orders.created"}
	gotCtx, token := recorder.Start(ctx, start)
	if gotCtx != ctx {
		t.Fatalf("Start returned a different context")
	}
	wantToken := f1.Token{Kind: start.Kind, Start: start.At, Handle: 1}
	if token != wantToken {
		t.Fatalf("token = %#v, want %#v", token, wantToken)
	}
	results := []f1.ObserverMessageResult{{ID: "message-1"}}
	recorder.Finish(token, f1.FinishEvent{
		Kind:    f1.ObserverProcess,
		At:      time.Unix(2, 3),
		Outcome: f1.ObserverOutcomeOK,
		Results: results,
	})
	recorder.Record(f1.PointEvent{Kind: f1.ObserverDeliveryReceived, At: time.Unix(3, 4)})

	calls := recorder.Calls()
	if len(calls) != 3 {
		t.Fatalf("Calls length = %d, want 3", len(calls))
	}
	if calls[0].Op != ObserverOpStart || calls[0].Start != start || calls[0].Token != token {
		t.Fatalf("start call = %#v", calls[0])
	}
	if calls[1].Op != ObserverOpFinish || calls[1].Finish.Outcome != f1.ObserverOutcomeOK || calls[1].Token != token {
		t.Fatalf("finish call = %#v", calls[1])
	}
	if calls[1].Finish.Results[0].ID != "message-1" {
		t.Fatalf("finish results = %#v", calls[1].Finish.Results)
	}
	if calls[2].Op != ObserverOpRecord || calls[2].Point.Kind != f1.ObserverDeliveryReceived {
		t.Fatalf("record call = %#v", calls[2])
	}

	calls[0].Start.EventType = "changed"
	calls[1].Finish.Results[0].ID = "changed"
	again := recorder.Calls()
	if len(again) != 3 {
		t.Fatalf("Calls exposed internal slice, got length %d", len(again))
	}
	if again[0].Start.EventType != "orders.created" || again[1].Finish.Results[0].ID != "message-1" {
		t.Fatalf("Calls exposed internal event data: %#v", again)
	}
	results[0].ID = "caller mutation"
	if recorder.Calls()[1].Finish.Results[0].ID != "message-1" {
		t.Fatalf("Finish results retained caller storage")
	}
}

func TestRecorderWaitReturnsOnMatchingCall(t *testing.T) {
	recorder := NewRecorder()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	waited := make(chan error, 1)
	go func() {
		waited <- recorder.Wait(ctx, func(calls []ObserverCall) bool {
			return len(calls) == 1 && calls[0].Kind == f1.ObserverDeliveryReceived
		})
	}()

	recorder.Record(f1.PointEvent{Kind: f1.ObserverDeliveryReceived})
	if err := <-waited; err != nil {
		t.Fatalf("Wait returned error: %v", err)
	}
}

func TestRecorderWaitReturnsContextError(t *testing.T) {
	recorder := NewRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	waited := make(chan error, 1)
	go func() {
		waited <- recorder.Wait(ctx, func([]ObserverCall) bool { return false })
	}()
	cancel()
	if err := <-waited; !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait error = %v, want %v", err, context.Canceled)
	}
}

func TestRecorderConcurrentCalls(t *testing.T) {
	recorder := NewRecorder()
	const workers = 8
	const rounds = 32
	wantCalls := workers * rounds * 3
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	waited := make(chan error, 1)
	go func() {
		waited <- recorder.Wait(ctx, func(calls []ObserverCall) bool { return len(calls) == wantCalls })
	}()

	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for range rounds {
				_, token := recorder.Start(context.Background(), f1.StartEvent{Kind: f1.ObserverProcess})
				recorder.Finish(token, f1.FinishEvent{Kind: f1.ObserverProcess})
				recorder.Record(f1.PointEvent{Kind: f1.ObserverDeliveryReceived})
			}
		}()
	}
	group.Wait()
	if err := <-waited; err != nil {
		t.Fatalf("Wait returned error: %v", err)
	}

	calls := recorder.Calls()
	if len(calls) != wantCalls {
		t.Fatalf("Calls length = %d, want %d", len(calls), wantCalls)
	}
	starts := make(map[uint64]struct{}, workers*rounds)
	finishes := make(map[uint64]struct{}, workers*rounds)
	for _, call := range calls {
		switch call.Op {
		case ObserverOpStart:
			starts[call.Token.Handle] = struct{}{}
		case ObserverOpFinish:
			finishes[call.Token.Handle] = struct{}{}
		}
	}
	if len(starts) != workers*rounds || len(finishes) != workers*rounds {
		t.Fatalf("start handles = %d, finish handles = %d, want %d each", len(starts), len(finishes), workers*rounds)
	}
	for handle := uint64(1); handle <= uint64(workers*rounds); handle++ {
		if _, ok := starts[handle]; !ok {
			t.Fatalf("missing start handle %d", handle)
		}
		if _, ok := finishes[handle]; !ok {
			t.Fatalf("missing finish handle %d", handle)
		}
	}
}
