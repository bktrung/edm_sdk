package dispatch

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"

	"golang.org/x/sync/errgroup"
)

// Work is one handler invocation. Run must include the complete settlement
// path when ordering is required.
type Work struct {
	Key []byte
	Run func(context.Context)
}

// Pool owns worker goroutines and optionally assigns equal keys to one worker.
type Pool struct {
	ordered bool
	queues  []chan Work
	ctx     context.Context
	cancel  context.CancelFunc
	group   *errgroup.Group
	mu      sync.Mutex
	closed  bool
	closing chan struct{}
	active  int
	idle    chan struct{}
	free    chan struct{}
}

// NewPool creates a worker pool. Ordered mode hashes each key to one worker;
// unordered mode uses the same shared queue for every worker.
//
//nolint:contextcheck // the pool context is deliberately derived from parent.
func NewPool(parent context.Context, concurrency int, ordered bool, queueSize int) (*Pool, error) {
	if concurrency < 1 {
		return nil, errors.New("dispatch: concurrency must be positive")
	}
	if queueSize < concurrency {
		queueSize = concurrency
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	p := &Pool{ordered: ordered, ctx: ctx, cancel: cancel, group: new(errgroup.Group), closing: make(chan struct{}), idle: make(chan struct{}), free: make(chan struct{}, 1)}
	if ordered {
		p.queues = make([]chan Work, concurrency)
		for i := range p.queues {
			p.queues[i] = make(chan Work, queueSize)
			p.start(i, p.queues[i])
		}
	} else {
		queue := make(chan Work, queueSize)
		p.queues = []chan Work{queue}
		for i := 0; i < concurrency; i++ {
			p.start(i, queue)
		}
	}
	return p, nil
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
	if p.ordered {
		hash := fnv.New32a()
		_, _ = hash.Write(work.Key)
		queue = p.queues[int(hash.Sum32())%len(p.queues)]
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.New("dispatch: pool is closed")
	}
	p.active++
	p.mu.Unlock()
	defer p.finishSubmit()
	select {
	case queue <- work:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return p.ctx.Err()
	case <-p.closing:
		return errors.New("dispatch: pool is closed")
	}
}

// WaitFree returns when a worker completes one item.
//
//nolint:contextcheck // the caller context intentionally controls this wait.
func (p *Pool) WaitFree(ctx context.Context) error {
	if p == nil {
		return errors.New("dispatch: nil pool")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-p.free:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return p.ctx.Err()
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

func (p *Pool) start(_ int, queue <-chan Work) {
	p.group.Go(func() error {
		for {
			select {
			case work := <-queue:
				work.Run(p.ctx)
				select {
				case p.free <- struct{}{}:
				default:
				}
			case <-p.closing:
				for {
					select {
					case work := <-queue:
						work.Run(p.ctx)
						select {
						case p.free <- struct{}{}:
						default:
						}
					default:
						return nil
					}
				}
			}
		}
	})
}
