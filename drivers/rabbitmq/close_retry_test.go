package rabbitmq

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

type closeBarrierConn struct {
	net.Conn
	entered     chan struct{}
	release     chan struct{}
	finished    chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
	finishOnce  sync.Once
}

func (c *closeBarrierConn) Close() error {
	c.enterOnce.Do(func() { close(c.entered) })
	<-c.release
	err := c.Conn.Close()
	c.finishOnce.Do(func() { close(c.finished) })
	return err
}

func (c *closeBarrierConn) unblock() {
	c.releaseOnce.Do(func() { close(c.release) })
}

func newCloseBarrierPeer(t *testing.T) (*conn, *channelOpenPeer, *closeBarrierConn) {
	t.Helper()
	barrier := &closeBarrierConn{entered: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
	c, peer := newChannelOpenPeer(t, func(socket net.Conn) net.Conn {
		barrier.Conn = socket
		return barrier
	})
	t.Cleanup(barrier.unblock)
	return c, peer, barrier
}

func waitCloseSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	timer := clock.NewReal().Timer(channelOpenTestLimit)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatal("close barrier did not complete")
	}
}

func closeWithin(t *testing.T, c *conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), channelOpenTestLimit)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close after release: %v", err)
	}
}

func TestCloseRetryJoinsCancelledAttempt(t *testing.T) {
	c, peer := newChannelOpenPeer(t)
	peer.ignoreClose.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- c.Close(ctx) }()
	peer.next(t, 10, 50)
	cancel()
	assertChannelOpenTransient(t, channelOpenOutcome(t, result), context.Canceled)
	retryCtx, cancelRetry := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancelRetry()
	assertChannelOpenTransient(t, c.Close(retryCtx), context.DeadlineExceeded)
	if err := peer.reply(0, 10, 51, nil); err != nil {
		t.Fatal(err)
	}
	closeWithin(t, c)
	waitCloseSignal(t, peer.done)
}

func TestConcurrentCloseJoinsRunningAttempt(t *testing.T) {
	t.Run("joiner_deadline", func(t *testing.T) {
		c, peer := newChannelOpenPeer(t)
		peer.ignoreClose.Store(true)
		result := make(chan error, 1)
		go func() { result <- c.Close(context.Background()) }()
		peer.next(t, 10, 50)
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()
		assertChannelOpenTransient(t, c.Close(ctx), context.DeadlineExceeded)
		if err := peer.reply(0, 10, 51, nil); err != nil {
			t.Fatal(err)
		}
		if err := channelOpenOutcome(t, result); err != nil {
			t.Fatalf("initiating Close: %v", err)
		}
		closeWithin(t, c)
		waitCloseSignal(t, peer.done)
	})
	t.Run("initiator_deadline", func(t *testing.T) {
		c, peer, barrier := newCloseBarrierPeer(t)
		peer.ignoreClose.Store(true)
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- c.Close(ctx) }()
		peer.next(t, 10, 50)
		joinCtx, cancelJoin := context.WithTimeout(context.Background(), channelOpenTestLimit)
		defer cancelJoin()
		observed := &closeWaitObserved{Context: joinCtx, waiting: make(chan struct{})}
		joined := make(chan error, 1)
		go func() { joined <- c.Close(observed) }()
		// Done is evaluated only after Close captures the running outcome.
		// Keep transport Close gated until the joiner is actually waiting.
		waitCloseSignal(t, observed.waiting)
		waitCloseSignal(t, barrier.entered)
		assertChannelOpenTransient(t, channelOpenOutcome(t, result), context.DeadlineExceeded)
		select {
		case err := <-joined:
			t.Fatalf("joiner escaped blocked transport Close: %v", err)
		default:
		}
		barrier.unblock()
		if err := channelOpenOutcome(t, joined); err != nil {
			t.Fatalf("joiner inherited initiating Close timeout: %v", err)
		}
		waitCloseSignal(t, barrier.finished)
		waitCloseSignal(t, peer.done)
	})
}

type closeWaitObserved struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *closeWaitObserved) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestCloseDeadlineCancellationKeepsAttemptOwned(t *testing.T) {
	c, peer, barrier := newCloseBarrierPeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), channelOpenTestLimit)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- c.Close(ctx) }()
	peer.next(t, 10, 50)
	waitCloseSignal(t, barrier.entered)
	cancel()
	assertChannelOpenTransient(t, channelOpenOutcome(t, result), context.Canceled)
	retryCtx, cancelRetry := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancelRetry()
	assertChannelOpenTransient(t, c.Close(retryCtx), context.DeadlineExceeded)
	barrier.unblock()
	closeWithin(t, c)
	waitCloseSignal(t, barrier.finished)
	waitCloseSignal(t, peer.done)
}

func TestCloseJoinsTransportShutdownBeforeDetachedWork(t *testing.T) {
	c, peer, barrier := newCloseBarrierPeer(t)
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	finished := make(chan struct{})
	c.startChannelCloseWithFinish(func() error {
		<-release
		return nil
	}, func(error) { close(finished) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- c.Close(ctx) }()
	peer.next(t, 10, 50)
	waitCloseSignal(t, barrier.entered)
	cancel()
	assertChannelOpenTransient(t, channelOpenOutcome(t, result), context.Canceled)
	// Releasing detached work must not allow a retry to escape a live transport.
	once.Do(func() { close(release) })
	waitCloseSignal(t, finished)
	retryCtx, cancelRetry := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancelRetry()
	go func() { result <- c.Close(retryCtx) }()
	assertChannelOpenTransient(t, channelOpenOutcome(t, result), context.DeadlineExceeded)
	barrier.unblock()
	closeWithin(t, c)
	waitCloseSignal(t, barrier.finished)
	waitCloseSignal(t, peer.done)
}

type closeDeadlineObserved struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (c *closeDeadlineObserved) Deadline() (time.Time, bool) {
	deadline, ok := c.Context.Deadline()
	c.once.Do(func() { close(c.observed) })
	return deadline, ok
}

func TestCloseFaultRetryPreservesAdmission(t *testing.T) {
	for _, name := range []string{"no_deadline", "deadline"} {
		t.Run(name, func(t *testing.T) {
			c, peer := newChannelOpenPeer(t)
			c.closeFault.Store(true)
			ctx := context.Background()
			cancel := func() {}
			if name == "deadline" {
				ctx, cancel = context.WithTimeout(ctx, channelOpenTestLimit)
			}
			defer cancel()
			observed := &closeDeadlineObserved{Context: ctx, observed: make(chan struct{})}
			result := make(chan error, 1)
			go func() { result <- c.Close(observed) }()
			waitCloseSignal(t, observed.observed)
			if name == "deadline" {
				joinCtx, cancelJoin := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancelJoin()
				assertChannelOpenTransient(t, c.Close(joinCtx), context.DeadlineExceeded)
				cancel()
				assertChannelOpenTransient(t, channelOpenOutcome(t, result), context.Canceled)
			} else {
				err := channelOpenOutcome(t, result)
				if err == nil {
					t.Fatal("injected Close succeeded")
				}
				kind, classified := driver.Classify(err)
				if !classified || kind != driver.KindTransient {
					t.Fatalf("injected Close = %v, want classified transient", err)
				}
			}
			if _, err := c.Producer(context.Background(), driver.ProducerConfig{}); err == nil {
				t.Fatal("Producer admitted after failed teardown")
			}
			closeWithin(t, c)
			peer.next(t, 10, 50)
			waitCloseSignal(t, peer.done)
		})
	}
}
