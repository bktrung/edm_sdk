package rabbitmq

import (
	"context"
	"errors"
	"sync"
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
				return c.releasing
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
	c.mu.Lock()
	finished := c.releaseFinished
	c.mu.Unlock()
	if finished {
		t.Fatal("a failed Release marked the consumer released")
	}
	if len(c.conn.active) != 1 {
		t.Fatalf("consumers registered after a failed Release = %d, want 1", len(c.conn.active))
	}
	if messagesClosed(c) {
		t.Fatal("Messages closed by a failed Release")
	}

	c.forward.Done()
	if err := c.Release(context.Background()); err != nil {
		t.Fatalf("retried Release() = %v, want nil", err)
	}
	c.mu.Lock()
	finished = c.releaseFinished
	c.mu.Unlock()
	if !finished {
		t.Fatal("the retried Release did not mark the consumer released")
	}
	if len(c.conn.active) != 0 {
		t.Fatalf("consumers registered after the retried Release = %d, want 0", len(c.conn.active))
	}
	if !messagesClosed(c) {
		t.Fatal("Messages still open after the retried Release")
	}
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
			group.Add(1)
			go func() {
				defer group.Done()
				<-release
				close(exited)
			}()

			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() { result <- c.Release(ctx) }()
			waitForwarderState(t, "Release to begin its teardown", func() bool {
				c.mu.Lock()
				defer c.mu.Unlock()
				return c.releasing
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
