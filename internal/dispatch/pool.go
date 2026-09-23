package dispatch

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"unsafe"

	"golang.org/x/sync/errgroup"
)

// Work is one handler invocation. Run must include the complete settlement
// path when ordering is required.
type Work struct {
	// Key selects the worker in ordered mode. Submit hashes it synchronously
	// and nothing reads it afterwards, so the caller may reuse or mutate the
	// slice once Submit returns.
	Key []byte
	// Run is the invocation itself. It must not read Key, because the caller
	// owns that slice again as soon as Submit returns.
	Run func(context.Context)
}

// MaxOrderedBufferEntries is the largest total ordered buffer in Work entries.
// It is computed as 64 MiB / unsafe.Sizeof(Work{}), which is 67,108,864 / 32
// = 2,097,152 on 64-bit platforms.
const MaxOrderedBufferEntries = int((64 << 20) / unsafe.Sizeof(Work{}))

// Pool owns worker goroutines and optionally assigns equal keys to one worker.
type Pool struct {
	ordered    bool
	queueDepth int
	queues     []chan Work
	ctx        context.Context
	cancel     context.CancelFunc
	group      *errgroup.Group
	mu         sync.Mutex
	closed     bool
	closing    chan struct{}
	// active counts Submit calls that have not returned, so Close can wait
	// for them before it stops the workers.
	active int
	// outstanding counts accepted work that has not finished in unordered
	// mode: queued plus running. One queue serves every worker there, so this
	// exact count is the whole answer for Free. Ordered mode bounds each
	// worker instead, and reads no total, so it neither maintains nor reads
	// this counter. The capacity-1 free channel below is a wake-up hint that
	// drops extras and can therefore never be counted.
	outstanding int
	// unfinished[i] counts the accepted work not finished for worker i, queued
	// plus running, and idleWorkers counts the workers with none. Ordered mode
	// reports Free from them, because there a busy worker says nothing about the
	// others: work for one key queues behind that key's earlier items, so a
	// single saturated key must not stop the pool handing other keys to idle
	// workers.
	unfinished  []int
	idleWorkers int
	idle        chan struct{}
	free        chan struct{}
}

// queueIndex reduces hash to a worker slot while both operands are still
// unsigned. Narrowing a uint32 with the high bit set to int first produces a
// negative dividend, and Go's % keeps the dividend's sign, so reducing after
// that narrowing can send the index negative and panic the queue lookup.
// Reducing before narrowing keeps the result in [0, queueCount).
func queueIndex(hash, queueCount uint32) uint32 {
	return hash % queueCount
}

// NewPool creates a worker pool. Ordered mode hashes each key to one worker;
// unordered mode uses the same shared queue for every worker. queueSize is
// the depth of each worker queue, raised to concurrency when it is smaller.
//
//nolint:contextcheck // the pool context is deliberately derived from parent.
func NewPool(parent context.Context, concurrency int, ordered bool, queueSize int) (*Pool, error) {
	if concurrency < 1 {
		return nil, errors.New("dispatch: concurrency must be positive")
	}
	if queueSize < concurrency {
		queueSize = concurrency
	}
	if ordered && concurrency > MaxOrderedBufferEntries/queueSize {
		return nil, fmt.Errorf("dispatch: ordered buffer %d x %d exceeds %d Work entries", concurrency, queueSize, MaxOrderedBufferEntries)
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	p := &Pool{ordered: ordered, queueDepth: queueSize, ctx: ctx, cancel: cancel, group: new(errgroup.Group), closing: make(chan struct{}), idle: make(chan struct{}), free: make(chan struct{}, 1)}
	if ordered {
		p.queues = make([]chan Work, concurrency)
		p.unfinished = make([]int, concurrency)
		p.idleWorkers = concurrency
		for i := range p.queues {
			p.queues[i] = make(chan Work, queueSize)
			p.start(i, p.queues[i])
		}
	} else {
		queue := make(chan Work, queueSize)
		p.queues = []chan Work{queue}
		for i := range concurrency {
			p.start(i, queue)
		}
	}
	return p, nil
}

// Free reports whether the pool can accept another item without blocking.
//
// Unordered mode answers from the exact count of accepted work that has not
// finished: fewer unfinished items than the queue depth means the shared queue
// still has a free slot, and a caller that submits only while Free reports true
// never blocks in Submit.
//
// Ordered mode answers from the workers instead: true while some worker has
// nothing queued and nothing running, which is when a key hashing to that
// worker can start. A key whose worker is busy waits for that worker, which is
// the ordering guarantee and not a stall, so the total in flight must not stop
// the loop: while any worker is idle the loop keeps choosing, and different
// keys run concurrently up to the worker count. The queue depth in this mode is
// the caller's bound, not this check's.
func (p *Pool) Free() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ordered {
		return p.idleWorkers > 0
	}
	return p.outstanding < p.queueDepth
}

// FreeSignal returns the channel signalled after every finished item.
//
// The channel holds one token and drops further ones, so it is a wake-up hint
// to be paired with a Free check, never a count of idle workers.
func (p *Pool) FreeSignal() <-chan struct{} {
	if p == nil {
		return nil
	}
	return p.free
}

// Submit queues work, returning ctx or pool cancellation when no slot is free.
//
//nolint:contextcheck // submission cancellation is intentionally caller-owned.
func (p *Pool) Submit(ctx context.Context, work Work) error {
	if p == nil || work.Run == nil {
		return errors.New("dispatch: nil pool or work")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	queue := p.queues[0]
	// The worker index is chosen here and nowhere else: Submit counts the item
	// against the queue it selects, and the cancellation paths and the worker
	// that runs it release that same index. Hashing a second time could land on
	// another queue and drift the counts.
	worker := 0
	if p.ordered {
		hash := fnv.New32a()
		_, _ = hash.Write(work.Key)
		worker = int(queueIndex(hash.Sum32(), uint32(len(p.queues)))) //nolint:gosec // queue count is the configured worker count, far below MaxUint32.
		queue = p.queues[worker]
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.New("dispatch: pool is closed")
	}
	p.active++
	if p.ordered {
		// A worker's count moves 0 -> 1 only on its first item, so the idle
		// count moves at most once per accepted item.
		if p.unfinished[worker] == 0 {
			p.idleWorkers--
		}
		p.unfinished[worker]++
	} else {
		p.outstanding++
	}
	p.mu.Unlock()
	defer p.finishSubmit()
	select {
	case queue <- work:
		return nil
	case <-ctx.Done():
		p.release(worker)
		return ctx.Err()
	case <-p.ctx.Done():
		p.release(worker)
		return p.ctx.Err()
	case <-p.closing:
		p.release(worker)
		return errors.New("dispatch: pool is closed")
	}
}

// Close stops accepting work and waits for queued work to finish.
func (p *Pool) Close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.closing)
	if p.active == 0 {
		close(p.idle)
	}
	idle := p.idle
	p.mu.Unlock()
	<-idle
	_ = p.group.Wait()
	p.cancel()
}

func (p *Pool) finishSubmit() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.active--
	if p.closed && p.active == 0 {
		close(p.idle)
	}
}

// release records one accepted item as finished and wakes a waiter.
//
// worker is the worker the item was counted against by Submit: the queue
// selected there, or the worker that ran it. Unordered mode has one queue for
// every worker and reads only the total, so it passes 0.
//
// The decrement happens before the hint is sent, so a waiter that checks Free
// after receiving the hint cannot miss this completion: either the counts
// already reflect it, or the buffered token is still there to be received.
func (p *Pool) release(worker int) {
	p.mu.Lock()
	if p.ordered {
		p.unfinished[worker]--
		if p.unfinished[worker] == 0 {
			p.idleWorkers++
		}
	} else {
		p.outstanding--
	}
	p.mu.Unlock()
	select {
	case p.free <- struct{}{}:
	default:
	}
}

func (p *Pool) start(worker int, queue <-chan Work) {
	p.group.Go(func() error {
		for {
			select {
			case work := <-queue:
				work.Run(p.ctx)
				p.release(worker)
			case <-p.closing:
				<-p.idle
				for {
					select {
					case work := <-queue:
						work.Run(p.ctx)
						p.release(worker)
					default:
						return nil
					}
				}
			}
		}
	})
}
