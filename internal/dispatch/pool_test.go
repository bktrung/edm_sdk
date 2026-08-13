package dispatch

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPoolRunsAllWork(t *testing.T) {
	pool, err := NewPool(context.Background(), 4, false, 8)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	count := 0
	for i := 0; i < 20; i++ {
		if err := pool.Submit(context.Background(), Work{Run: func(context.Context) { mu.Lock(); count++; mu.Unlock() }}); err != nil {
			t.Fatal(err)
		}
	}
	pool.Close()
	if count != 20 {
		t.Fatalf("completed work = %d, want 20", count)
	}
}

func TestOrderedPoolKeepsEqualKeysOnOneWorker(t *testing.T) {
	pool, err := NewPool(context.Background(), 4, true, 8)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	sequence := make([]int, 0, 10)
	for i := 0; i < 10; i++ {
		value := i
		if err := pool.Submit(context.Background(), Work{Key: []byte("same"), Run: func(context.Context) { mu.Lock(); sequence = append(sequence, value); mu.Unlock() }}); err != nil {
			t.Fatal(err)
		}
	}
	pool.Close()
	for i, value := range sequence {
		if value != i {
			t.Fatalf("ordered sequence = %v", sequence)
		}
	}
}

func TestPoolValidationFreeSignalAndClosedSubmit(t *testing.T) {
	if _, err := NewPool(context.Background(), 0, false, 1); err == nil {
		t.Fatal("zero concurrency must fail")
	}
	var parent context.Context
	if _, err := NewPool(parent, 1, false, 1); err != nil {
		t.Fatal(err)
	}
	p, err := NewPool(context.Background(), 1, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Submit(context.Background(), Work{Run: func(context.Context) {}}); err != nil {
		t.Fatal(err)
	}
	if err := p.WaitFree(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.Close()
	if err := p.Submit(context.Background(), Work{Run: func(context.Context) {}}); err == nil {
		t.Fatal("submit after close must fail")
	}
	if err := p.Submit(context.Background(), Work{}); err == nil {
		t.Fatal("nil work must fail")
	}
}

func TestPoolWaitFreeHonorsCancellation(t *testing.T) {
	p, err := NewPool(context.Background(), 1, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.WaitFree(ctx); err == nil {
		t.Fatal("WaitFree must respect cancellation")
	}
}

func TestPoolSubmitHonorsCancellationWhenQueueIsFull(t *testing.T) {
	p, err := NewPool(context.Background(), 1, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	if err := p.Submit(context.Background(), Work{Run: func(context.Context) { close(started); <-release }}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.Submit(context.Background(), Work{Run: func(context.Context) {}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Submit(ctx, Work{Run: func(context.Context) {}}); err == nil {
		t.Fatal("full queue submit must respect cancellation")
	}
	close(release)
	p.Close()
	p.Close()
}

func TestPoolCloseUnblocksBlockedSubmit(t *testing.T) {
	p, err := NewPool(context.Background(), 1, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	if err := p.Submit(context.Background(), Work{Run: func(context.Context) {
		close(started)
		<-release
	}}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.Submit(context.Background(), Work{Run: func(context.Context) {}}); err != nil {
		t.Fatal(err)
	}

	submitDone := make(chan error, 1)
	go func() {
		submitDone <- p.Submit(context.Background(), Work{Run: func(context.Context) {}})
	}()
	for i := 0; i < 100000; i++ {
		p.mu.Lock()
		active := p.active
		p.mu.Unlock()
		if active > 0 {
			break
		}
		if i == 99999 {
			t.Fatal("blocked submit did not reach the pool")
		}
		runtime.Gosched()
	}

	closeDone := make(chan struct{})
	go func() {
		p.Close()
		close(closeDone)
	}()
	<-p.closing
	for i := 0; i < 100000; i++ {
		select {
		case err := <-submitDone:
			if err == nil {
				t.Fatal("blocked submit succeeded after Close")
			}
			close(release)
			<-closeDone
			return
		default:
			runtime.Gosched()
		}
	}
	t.Fatal("Close did not unblock the blocked submit")
}

func TestPoolNilOperations(t *testing.T) {
	var pool *Pool
	if err := pool.Submit(context.Background(), Work{}); err == nil {
		t.Fatal("nil pool submit must fail")
	}
	if err := pool.WaitFree(context.Background()); err == nil {
		t.Fatal("nil pool WaitFree must fail")
	}
}

func TestPoolSubmitAndWaitCancellationPaths(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	pool, err := NewPool(parent, 1, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	if err := pool.Submit(context.Background(), Work{Run: func(context.Context) {
		close(started)
		<-release
	}}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := pool.Submit(context.Background(), Work{Run: func(context.Context) {}}); err != nil {
		t.Fatal(err)
	}
	cancelParent()
	if err := pool.Submit(context.Background(), Work{Run: func(context.Context) {}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Submit() after parent cancellation = %v, want context canceled", err)
	}
	if err := pool.WaitFree(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitFree after parent cancellation = %v, want context canceled", err)
	}
	close(release)
	pool.Close()

	resized, err := NewPool(context.Background(), 2, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	var resizedRuns atomic.Int32
	for i := 0; i < 2; i++ {
		if err := resized.Submit(context.Background(), Work{Run: func(context.Context) { resizedRuns.Add(1) }}); err != nil {
			t.Fatal(err)
		}
	}
	resized.Close()
	if got, want := resizedRuns.Load(), int32(2); got != want {
		t.Fatalf("resized pool runs = %d, want %d", got, want)
	}
}

func TestPoolSubmitReturnsClosedDuringClose(t *testing.T) {
	pool, err := NewPool(context.Background(), 1, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	if err := pool.Submit(context.Background(), Work{Run: func(context.Context) {
		close(started)
		<-release
	}}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := pool.Submit(context.Background(), Work{Run: func(context.Context) {}}); err != nil {
		t.Fatal(err)
	}
	submitDone := make(chan error, 1)
	go func() {
		submitDone <- pool.Submit(context.Background(), Work{Run: func(context.Context) {}})
	}()
	for i := 0; i < 100000; i++ {
		pool.mu.Lock()
		active := pool.active
		pool.mu.Unlock()
		if active > 0 {
			break
		}
		if i == 99999 {
			t.Fatal("blocked submit did not reach the pool")
		}
		runtime.Gosched()
	}
	closeDone := make(chan struct{})
	go func() {
		pool.Close()
		close(closeDone)
	}()
	<-pool.closing
	if err := <-submitDone; err == nil || err.Error() != "dispatch: pool is closed" {
		t.Fatalf("blocked Submit() error = %v, want pool closed", err)
	}
	close(release)
	<-closeDone
}
