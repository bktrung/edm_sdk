package dispatch

import (
	"context"
	"errors"
	"hash/fnv"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestPoolRunsAllWork(t *testing.T) {
	pool, err := NewPool(context.Background(), 4, false, 8)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	count := 0
	for range 20 {
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
	for i := range 10 {
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

func TestQueueIndexKeepsHighBitHashInRange(t *testing.T) {
	const (
		queueCount    uint32 = 4
		key                  = "key-10"
		expectedHash         = uint32(3421672898) // FNV-1a sum for key-10; high bit set.
		expectedIndex uint32 = 2
	)

	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	if got := hash.Sum32(); got != expectedHash {
		t.Fatalf("FNV-1a sum for %q = %d, want %d", key, got, expectedHash)
	}
	if expectedHash&0x80000000 == 0 {
		t.Fatalf("FNV-1a sum for %q = %#x, want high bit set", key, expectedHash)
	}

	got := queueIndex(hash.Sum32(), queueCount)
	if got >= queueCount {
		t.Fatalf("queue index = %d, want range [0, %d)", got, queueCount)
	}
	if got != expectedIndex {
		t.Fatalf("queue index for %q = %d, want %d", key, got, expectedIndex)
	}
}

func TestQueueIndexPreservesFNVDistribution(t *testing.T) {
	const queueCount uint32 = 4
	tests := []struct {
		key      string
		hash     uint32
		expected uint32
	}{
		{key: "", hash: 2166136261, expected: 1},
		{key: "same", hash: 3440134715, expected: 3},
		{key: "alpha", hash: 1569418667, expected: 3},
		{key: "beta", hash: 2944525511, expected: 3},
		{key: "key-0", hash: 1491088857, expected: 1},
		{key: "key-1", hash: 1474311238, expected: 2},
		{key: "key-10", hash: 3421672898, expected: 2},
		{key: "key-11", hash: 3438450517, expected: 1},
	}

	for _, test := range tests {
		hash := fnv.New32a()
		_, _ = hash.Write([]byte(test.key))
		if got := hash.Sum32(); got != test.hash {
			t.Fatalf("FNV-1a sum for %q = %d, want %d", test.key, got, test.hash)
		}
		if got := queueIndex(hash.Sum32(), queueCount); got != test.expected {
			t.Fatalf("queue index for %q = %d, want %d", test.key, got, test.expected)
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
	release := make(chan struct{})
	if err := p.Submit(context.Background(), Work{Run: func(context.Context) { <-release }}); err != nil {
		t.Fatal(err)
	}
	if p.Free() {
		t.Fatal("pool reported free while an accepted item was unfinished")
	}
	close(release)
	waitForFreeSignal(t, p)
	p.Close()
	if err := p.Submit(context.Background(), Work{Run: func(context.Context) {}}); err == nil {
		t.Fatal("submit after close must fail")
	}
	if err := p.Submit(context.Background(), Work{}); err == nil {
		t.Fatal("nil work must fail")
	}
}

func TestPoolFreeTracksAcceptedWorkAndNeverBlocksSubmit(t *testing.T) {
	p, err := NewPool(context.Background(), 1, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if !p.Free() {
		t.Fatal("new pool must report free")
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
	if p.Free() {
		t.Fatal("pool reported free with one accepted item unfinished")
	}

	// A submit made while Free reports true must not block: the count it is
	// derived from is what bounds the queue, not the workers' progress.
	close(release)
	waitForFreeSignal(t, p)
	if !p.Free() {
		t.Fatal("pool did not report free after its only item finished")
	}
	done := make(chan error, 1)
	go func() {
		done <- p.Submit(context.Background(), Work{Run: func(context.Context) {}})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("Submit blocked while Free reported true")
	}
}

// TestOrderedPoolFreeTracksIdleWorkers covers the ordered rule: the pool can
// take more work while some worker has nothing queued and nothing running, even
// though another worker already has this key's next item behind the running
// one. Reporting on the total instead would stall the idle worker.
func TestOrderedPoolFreeTracksIdleWorkers(t *testing.T) {
	const workers = 2
	pool, err := NewPool(context.Background(), workers, true, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	keys := distinctQueueKeys(t, workers)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	// Deferred after pool.Close, so it runs before it: a failure below must
	// unblock the handlers, or Close would wait on them until the test binary
	// times out.
	defer unblock()
	started := make(chan string, workers+1)
	submit := func(key string) {
		t.Helper()
		if err := pool.Submit(context.Background(), Work{Key: []byte(key), Run: func(context.Context) {
			started <- key
			<-release
		}}); err != nil {
			t.Fatal(err)
		}
	}
	submit(keys[0])
	select {
	case <-started:
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("first worker did not start")
	}
	// Three more of the same key: the worker runs one and queues the rest, so
	// the accepted count reaches the queue depth while the other worker is
	// still idle. Ordered mode must keep reporting free there; the depth is the
	// caller's bound, not this check's.
	submit(keys[0])
	submit(keys[0])
	submit(keys[0])
	if !pool.Free() {
		t.Fatal("ordered pool reported busy at the queue depth while one worker was idle")
	}
	submit(keys[1])
	select {
	case key := <-started:
		if key != keys[1] {
			t.Fatalf("second worker started key %q, want %q", key, keys[1])
		}
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("idle worker did not start the other key")
	}
	if pool.Free() {
		t.Fatal("ordered pool reported free while every worker had work")
	}
	unblock()
	waitForFreeSignal(t, pool)
	if !pool.Free() {
		t.Fatal("ordered pool did not report free after both keys finished")
	}
}

// distinctQueueKeys returns one key per worker, at index i for worker i.
func distinctQueueKeys(t *testing.T, workers int) []string {
	t.Helper()
	byWorker := make([]string, workers)
	found := 0
	for i := 0; found < workers; i++ {
		if i == 10000 {
			t.Fatalf("found keys for %d of %d workers", found, workers)
		}
		key := "ordered-key-" + strconv.Itoa(i)
		hash := fnv.New32a()
		_, _ = hash.Write([]byte(key))
		worker := queueIndex(hash.Sum32(), uint32(workers)) //nolint:gosec // worker count is the test's own, far below MaxUint32.
		if byWorker[worker] == "" {
			byWorker[worker] = key
			found++
		}
	}
	return byWorker
}

func waitForFreeSignal(t *testing.T, p *Pool) {
	t.Helper()
	select {
	case <-p.FreeSignal():
	case <-clock.NewReal().Timer(5 * time.Second).C:
		t.Fatal("no free signal after an item finished")
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
	for i := range 100000 {
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
	for range 100000 {
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
	if pool.Free() {
		t.Fatal("nil pool must not report free")
	}
	if pool.FreeSignal() != nil {
		t.Fatal("nil pool must not expose a wake-up channel")
	}
}

func TestPoolSubmitCancellationPaths(t *testing.T) {
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
	if pool.Free() {
		t.Fatal("pool reported free while two accepted items were unfinished")
	}
	close(release)
	pool.Close()

	resized, err := NewPool(context.Background(), 2, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	var resizedRuns atomic.Int32
	for range 2 {
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
	for i := range 100000 {
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
