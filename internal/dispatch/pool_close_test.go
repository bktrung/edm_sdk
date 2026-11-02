package dispatch

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
)

func TestPoolCloseRunsEveryAcceptedSubmission(t *testing.T) {
	const iterations = 1000
	const submitters = 4

	for iteration := range iterations {
		pool, err := NewPool(context.Background(), 1, false, 1)
		if err != nil {
			t.Fatal(err)
		}

		var accepted atomic.Int64
		var runs atomic.Int64
		started := make(chan struct{})
		release := make(chan struct{})
		if err := pool.Submit(context.Background(), Work{Run: func(context.Context) {
			runs.Add(1)
			close(started)
			<-release
		}}); err != nil {
			t.Fatal(err)
		}
		accepted.Add(1)
		<-started

		if err := pool.Submit(context.Background(), Work{Run: func(context.Context) {
			runs.Add(1)
		}}); err != nil {
			t.Fatal(err)
		}
		accepted.Add(1)

		startSubmitters := make(chan struct{})
		submitDone := make(chan error, submitters)
		for range submitters {
			go func() {
				<-startSubmitters
				err := pool.Submit(context.Background(), Work{Run: func(context.Context) {
					runs.Add(1)
				}})
				if err == nil {
					accepted.Add(1)
				}
				submitDone <- err
			}()
		}
		close(startSubmitters)
		for spin := range 100000 {
			pool.mu.Lock()
			active := pool.active
			pool.mu.Unlock()
			if active >= submitters {
				break
			}
			if spin == 99999 {
				t.Fatal("submitters did not reach the pool")
			}
			runtime.Gosched()
		}

		closeDone := make(chan struct{})
		go func() {
			pool.Close()
			close(closeDone)
		}()
		<-pool.closing
		close(release)
		<-closeDone
		for range submitters {
			<-submitDone
		}

		if got, want := runs.Load(), accepted.Load(); got != want {
			t.Fatalf("iteration %d: Run calls = %d, accepted submissions = %d", iteration, got, want)
		}
	}
}

func TestPoolCloseRunsDrainedWorkWithALiveContext(t *testing.T) {
	const iterations = 100

	for iteration := range iterations {
		pool, err := NewPool(context.Background(), 1, false, 2)
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

		var drainedErr error
		drainedRan := false
		if err := pool.Submit(context.Background(), Work{Run: func(ctx context.Context) {
			drainedRan = true
			drainedErr = ctx.Err()
		}}); err != nil {
			t.Fatal(err)
		}

		closeDone := make(chan struct{})
		go func() {
			pool.Close()
			close(closeDone)
		}()
		// Close has stopped admission and every Submit has returned, so the
		// queued item can now only run in the worker's drain. Releasing the
		// running item here lets that drain start while Close is still joining.
		<-pool.idle
		close(release)
		<-closeDone

		if !drainedRan {
			t.Fatalf("iteration %d: queued work did not run before Close returned", iteration)
		}
		if drainedErr != nil {
			t.Fatalf("iteration %d: drained work saw context error %v, want a live context until Close joins the workers", iteration, drainedErr)
		}
	}
}
