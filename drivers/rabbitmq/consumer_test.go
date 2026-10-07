package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestInboundMessageReceivedAtIsReceiptTime(t *testing.T) {
	receiptAt := time.Date(2030, time.January, 2, 3, 4, 5, 6, time.UTC)
	timestamp := receiptAt.Add(-time.Hour)
	delivery := amqp.Delivery{
		Timestamp: timestamp,
		Headers: amqp.Table{
			"cloudEvents:specversion": "1.0",
			"cloudEvents:id":          "event-1",
			"cloudEvents:source":      "/test",
			"cloudEvents:type":        "test.event",
		},
	}

	consumer := &consumer{clock: clock.NewFake(receiptAt)}
	message := consumer.inboundMessage("orders", delivery, nil, false, false)
	if !message.ReceivedAt.Equal(receiptAt) {
		t.Fatalf("ReceivedAt = %s, want fake receipt time %s", message.ReceivedAt, receiptAt)
	}

	var timeHeader string
	for _, header := range message.Headers {
		if header.Key == "time" {
			timeHeader = string(header.Value)
			break
		}
	}
	gotTimestamp, err := time.Parse(time.RFC3339Nano, timeHeader)
	if err != nil {
		t.Fatalf("time header = %q: %v", timeHeader, err)
	}
	if !gotTimestamp.Equal(timestamp) {
		t.Fatalf("time header = %s, want %s", gotTimestamp, timestamp)
	}

	message = consumer.inboundMessage("orders", amqp.Delivery{}, nil, false, false)
	if !message.ReceivedAt.Equal(receiptAt) {
		t.Fatalf("zero-timestamp ReceivedAt = %s, want fake receipt time %s", message.ReceivedAt, receiptAt)
	}
}

// teardownJoin names one of the three goroutine joins Release waits on, so the
// bound test and the no-leak test cover the same set.
type teardownJoin struct {
	name  string
	group func(*consumer) *sync.WaitGroup
}

func teardownJoins() []teardownJoin {
	return []teardownJoin{
		{name: "readers", group: func(c *consumer) *sync.WaitGroup { return &c.readers }},
		{name: "forwarders", group: func(c *consumer) *sync.WaitGroup { return &c.forward }},
		{name: "watchers", group: func(c *consumer) *sync.WaitGroup { return &c.events }},
	}
}

// TestReleaseBoundsEachTeardownJoinWithItsContext pins that a Release whose
// context is cancelled while it joins a teardown goroutine returns the context
// error instead of blocking, for each of the three joins. The wedge is a
// WaitGroup with no matching Done, so the join is the only thing that can hold
// Release back.
func TestReleaseBoundsEachTeardownJoinWithItsContext(t *testing.T) {
	for _, join := range teardownJoins() {
		t.Run(join.name, func(t *testing.T) {
			c := newForwarderTestConsumer()
			join.group(c).Add(1)
			t.Cleanup(func() { join.group(c).Done() })

			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() { result <- c.Release(ctx) }()
			waitForwarderState(t, "Release to begin its teardown", func() bool {
				c.mu.Lock()
				defer c.mu.Unlock()
				return c.stopped
			})
			cancel()

			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Release() error = %v, want context.Canceled", err)
				}
				kind, classified := driver.Classify(err)
				if !classified || kind != driver.KindTransient {
					t.Fatalf("Release() error = %v, want a transient classification", err)
				}
			case <-time.After(2 * time.Second): //nolint:forbidigo // bound a regression that blocks in an uncancelled join
				t.Fatal("Release did not return after its context was cancelled")
			}
		})
	}
}

// TestReleaseRetriesTheTeardownAfterAFailedAttempt pins that a Release whose
// join timed out leaves the consumer registered with Messages open, so the
// retry the client makes finishes the teardown instead of reporting a release
// that never happened - which is what would make Close meet an outstanding
// consumer.
func TestReleaseRetriesTheTeardownAfterAFailedAttempt(t *testing.T) {
	c := newForwarderTestConsumer()
	c.conn.active = map[*consumer]struct{}{c: {}}
	c.forward.Add(1)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := c.Release(ctx)
	if err == nil {
		t.Fatal("Release() = nil while a join was wedged, want an error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Release() error = %v, want context.DeadlineExceeded", err)
	}
	assertTeardownState(t, c, false)

	c.forward.Done()
	if err := c.Release(context.Background()); err != nil {
		t.Fatalf("retried Release() = %v, want nil", err)
	}
	assertTeardownState(t, c, true)
}

// TestReleaseEarlyReturnLeavesNoWedgedJoin pins that a Release that returns on
// its cancelled context leaves the join it abandoned able to finish: the joiner
// goroutine was still running at the early return, and once its wedge clears the
// WaitGroup drains. The WaitGroup standing in for the goroutine is the seam the
// production paths share, so a leak here is a leak in the bound.
func TestReleaseEarlyReturnLeavesNoWedgedJoin(t *testing.T) {
	for _, join := range teardownJoins() {
		t.Run(join.name, func(t *testing.T) {
			c := newForwarderTestConsumer()
			group := join.group(c)
			release := make(chan struct{})
			exited := make(chan struct{})
			var releaseOnce sync.Once
			releaseWedge := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(releaseWedge)
			group.Go(func() {
				<-release
				close(exited)
			})

			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() { result <- c.Release(ctx) }()
			waitForwarderState(t, "Release to begin its teardown", func() bool {
				c.mu.Lock()
				defer c.mu.Unlock()
				return c.stopped
			})
			cancel()

			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Release() error = %v, want context.Canceled", err)
				}
			case <-time.After(2 * time.Second): //nolint:forbidigo // bound a regression that blocks in an uncancelled join
				t.Fatal("Release did not return after its context was cancelled")
			}

			select {
			case <-exited:
				t.Fatal("the joined goroutine exited before its wedge was released")
			default:
			}

			releaseWedge()
			joined := make(chan struct{})
			go func() {
				group.Wait()
				close(joined)
			}()
			select {
			case <-joined:
			case <-time.After(2 * time.Second): //nolint:forbidigo // bound a join that never drains after its wedge cleared
				t.Fatal("the joined goroutine did not exit after its wedge was released")
			}
		})
	}
}

// messagesClosed reports whether c.Messages has been closed. The consumer's
// Messages channel is empty in these tests, so a ready receive can only be the
// closed channel.
func messagesClosed(c *consumer) bool {
	select {
	case _, ok := <-c.Messages():
		return !ok
	default:
		return false
	}
}

// TestReleaseBoundsLaneCloseAndRetriesIt verifies that a pending lane close is
// bounded by Release's context and that a retry joins the original close.
func TestReleaseBoundsLaneCloseAndRetriesIt(t *testing.T) {
	c := newForwarderTestConsumer()
	c.conn.active = map[*consumer]struct{}{c: {}}
	testLane := &lane{owner: c, destination: "pending-close"}
	c.lanes = []*lane{testLane}

	started := make(chan struct{})
	releaseClose := make(chan struct{})
	var releaseCloseOnce sync.Once
	releaseCloseFn := func() { releaseCloseOnce.Do(func() { close(releaseClose) }) }
	t.Cleanup(releaseCloseFn)
	var startOnce sync.Once
	var starts atomic.Int32
	c.channelCloseHook = func(got *lane) error {
		if got != testLane {
			t.Errorf("close hook lane = %p, want %p", got, testLane)
		}
		starts.Add(1)
		startOnce.Do(func() { close(started) })
		<-releaseClose
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- c.Release(ctx) }()
	select {
	case <-started:
	case <-time.After(2 * time.Second): //nolint:forbidigo // bound a direct close seam
		t.Fatal("Release did not start the lane close")
	}
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Release() error = %v, want context.Canceled", err)
		}
		kind, classified := driver.Classify(err)
		if !classified || kind != driver.KindTransient {
			t.Fatalf("Release() error = %v, want a transient classification", err)
		}
	case <-time.After(2 * time.Second): //nolint:forbidigo // bound the context regression
		t.Fatal("Release did not return after its context was cancelled")
	}
	if len(c.conn.active) != 1 {
		t.Fatalf("consumers registered after failed Release = %d, want 1", len(c.conn.active))
	}
	if messagesClosed(c) {
		t.Fatal("Messages closed by a failed Release")
	}

	releaseCloseFn()
	detached := make(chan struct{})
	go func() {
		c.conn.detachWatch.Wait()
		close(detached)
	}()
	select {
	case <-detached:
	case <-time.After(2 * time.Second): //nolint:forbidigo // bound a detached close leak
		t.Fatal("lane close did not leave detachWatch")
	}

	if err := c.Release(context.Background()); err != nil {
		t.Fatalf("retried Release() = %v, want nil", err)
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("lane close starts = %d, want 1", got)
	}
	if len(c.conn.active) != 0 {
		t.Fatalf("consumers registered after retried Release = %d, want 0", len(c.conn.active))
	}
	if !messagesClosed(c) {
		t.Fatal("Messages still open after retried Release")
	}
}

func TestStopTakesOverFailedReleaseTeardown(t *testing.T) {
	c, release, calls, cancel, unblock, starts := blockedReleaseFixture(t)
	c.events.Add(1)
	var joinOnce sync.Once
	releaseJoin := func() { joinOnce.Do(c.events.Done) }
	t.Cleanup(releaseJoin)
	cancel()
	err := teardownResult(t, release)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("failed Release = %v, want context.Canceled", err)
	}
	if kind, ok := driver.Classify(err); !ok || kind != driver.KindTransient {
		t.Errorf("failed Release = %v, want transient classification", err)
	}
	assertTeardownState(t, c, false)
	c.mu.Lock()
	outstanding, settlers := c.outstanding, len(c.settlers)
	c.mu.Unlock()
	if outstanding != 1 || settlers != 1 {
		t.Fatalf("failed Release accounting = outstanding %d, settlers %d, want 1, 1", outstanding, settlers)
	}

	ctx := newTeardownWaitContext()
	stop := startTeardownCall(t, c, calls, func() error { return c.Stop(ctx) })
	assertTeardownState(t, c, false)
	if got := starts.Load(); got != 1 {
		t.Errorf("lane close starts during takeover = %d, want 1", got)
	}
	unblock()
	waitForwarderState(t, "retained broker close completion", func() bool {
		l := c.lanes[0]
		l.closeMu.Lock()
		defer l.closeMu.Unlock()
		if l.closeOutcome == nil {
			return false
		}
		select {
		case <-l.closeOutcome.done:
			return true
		default:
			return false
		}
	})
	returned := !awaitTeardownWait(t, ctx, stop)
	// Closing the retained broker outcome is insufficient: the Stop owner must
	// also join the watcher left behind by the failed Release before finalizing.
	assertTeardownState(t, c, false)
	if !returned {
		select {
		case err := <-stop:
			t.Errorf("Stop returned %v before joining the remaining watcher", err)
			returned = true
		case <-time.After(20 * time.Millisecond): //nolint:forbidigo // observe the local join after broker close completion
		}
	}
	releaseJoin()
	if !returned {
		if err := teardownResult(t, stop); err != nil {
			t.Errorf("Stop takeover = %v, want nil despite abandoned settlers", err)
		}
	}
	assertTeardownState(t, c, true)
	for _, call := range []func() error{
		func() error { return c.Stop(context.Background()) },
		func() error { return c.Release(context.Background()) },
	} {
		if err := teardownResult(t, startTeardownCall(t, c, calls, call)); err != nil {
			t.Errorf("repeated teardown after takeover = %v, want nil", err)
		}
		assertTeardownState(t, c, true)
	}
	if got := starts.Load(); got != 1 {
		t.Errorf("lane close starts after takeover = %d, want 1", got)
	}
}

func TestLockBeforeContextAcquiresLockReleasedDuringFinalSleep(t *testing.T) {
	fake := clock.NewFake(time.Time{})
	deadline := fake.Now().Add(500 * time.Microsecond)
	var mu sync.Mutex
	mu.Lock()
	var unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(mu.Unlock) }
	t.Cleanup(unlock)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	type lockResult struct {
		locked          bool
		deadlineReached bool
	}
	result := make(chan lockResult, 1)
	go func() {
		locked, reached := lockBeforeContext(ctx, fake, &mu, deadline)
		if locked {
			mu.Unlock()
		}
		result <- lockResult{locked: locked, deadlineReached: reached}
	}()
	waitForwarderState(t, "lane lock backoff timer", func() bool { return fake.NumWaiters() == 1 })
	// The RPC finishes inside the 1ms sleep, whose wakeup is past the deadline.
	fake.Advance(250 * time.Microsecond)
	unlock()
	fake.Advance(750 * time.Microsecond)
	wait := clock.NewReal().Timer(time.Second)
	defer wait.Stop()
	select {
	case got := <-result:
		if !got.locked || got.deadlineReached {
			t.Fatalf("lockBeforeContext = (locked=%v, deadlineReached=%v), want (true, false)", got.locked, got.deadlineReached)
		}
	case <-wait.C:
		t.Fatal("lockBeforeContext did not return after the final sleep")
	}
}
