package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// TestReleaseTearsDownAfterAFailedLeave covers the failure that used to strand
// a connection: the leave finishes with an error, and Release has to end the
// consumer anyway. The error stays visible on both channels out.
func TestReleaseTearsDownAfterAFailedLeave(t *testing.T) {
	const destination = "leave-failed"
	connection := newLeaveTestConnection(destination)
	c := newLeaveTestConsumer(t, connection, destination, 1)
	c.leaveFn = func(context.Context, leaveRequest) error { return kerr.CoordinatorNotAvailable }

	err := c.Release(context.Background())
	if !errors.Is(err, kerr.CoordinatorNotAvailable) {
		t.Fatalf("Release error = %v, want it to wrap %v", err, kerr.CoordinatorNotAvailable)
	}
	if leaveTestConsumerRegistered(c) {
		t.Fatal("consumer is still registered after a leave that finished with an error")
	}
	timer := clock.NewReal().Timer(time.Second)
	defer timer.Stop()
	select {
	case got, open := <-c.Errors():
		if !open {
			t.Fatal("Errors() closed before delivering the leave error")
		}
		if !errors.Is(got, kerr.CoordinatorNotAvailable) {
			t.Fatalf("Errors() delivered %v, want it to wrap %v", got, kerr.CoordinatorNotAvailable)
		}
	case <-timer.C:
		t.Fatal("Errors() did not deliver the leave error")
	}
	if _, open := <-c.Errors(); open {
		t.Fatal("Errors() is still open after Release")
	}
	select {
	case _, open := <-c.Messages():
		if open {
			t.Fatal("Messages() delivered a message after Release")
		}
	case <-timer.C:
		t.Fatal("Messages() was not closed by Release")
	}
	if err := c.Release(context.Background()); err != nil {
		t.Fatalf("second Release = %v, want nil", err)
	}
}

// TestReleaseTreatsUnknownMemberAsLeft covers the leave that arrives before the
// join completed, or after the member was expired: UNKNOWN_MEMBER_ID is the
// state a leave exists to reach, so it is not an error and nothing is reported
// on Errors(). Both membership paths reach that mapping through leaveGroup.
func TestReleaseTreatsUnknownMemberAsLeft(t *testing.T) {
	tests := []struct {
		name    string
		install func(c *consumer)
	}{
		{
			name: "static instance",
			install: func(c *consumer) {
				c.staticMembership = true
				c.instanceID = "leave-test-instance"
				c.leaveStatic = func(context.Context, string, string) error { return kerr.UnknownMemberID }
			},
		},
		{
			name: "dynamic member",
			install: func(c *consumer) {
				c.leaveDynamic = func(context.Context) error { return kerr.UnknownMemberID }
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const destination = "leave-unknown-member"
			connection := newLeaveTestConnection(destination)
			c := newLeaveTestConsumer(t, connection, destination, 1)
			c.leaveFn = c.leaveGroup
			tc.install(c)

			if err := c.Release(context.Background()); err != nil {
				t.Fatalf("Release = %v, want nil when the member had already left the group", err)
			}
			select {
			case got, open := <-c.Errors():
				if open {
					t.Fatalf("Errors() delivered %v after a leave that found the member already out of the group", got)
				}
			default:
				t.Fatal("Errors() is still open after Release")
			}
			if leaveTestConsumerRegistered(c) {
				t.Fatal("consumer is still registered after a leave that found the member already out of the group")
			}
		})
	}
}

// TestReleaseKeepsConsumerWhenCallerContextEndsFirst covers the retry path: a
// caller that gives up on the wait has not ended the leave, so the consumer
// stays registered and a later Release with a fresh context completes it.
func TestReleaseKeepsConsumerWhenCallerContextEndsFirst(t *testing.T) {
	const destination = "leave-context"
	connection := newLeaveTestConnection(destination)
	c := newLeaveTestConsumer(t, connection, destination, 1)
	leaveFinished := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.leaveFn = func(context.Context, leaveRequest) error {
		// End the caller's context with the leave still in flight. The leave
		// runs on its own context, so it stays here until the test releases it.
		cancel()
		<-leaveFinished
		return nil
	}

	err := c.Release(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Release error = %v, want %v", err, context.Canceled)
	}
	if kind, classified := driver.Classify(err); !classified || kind != driver.KindTransient {
		t.Fatalf("Release error kind = %v (classified %v), want %v", kind, classified, driver.KindTransient)
	}
	if !leaveTestConsumerRegistered(c) {
		t.Fatal("consumer was deregistered by a Release whose caller context ended before the leave finished")
	}

	close(leaveFinished)
	if err := c.Release(context.Background()); err != nil {
		t.Fatalf("second Release = %v, want nil", err)
	}
	if leaveTestConsumerRegistered(c) {
		t.Fatal("consumer is still registered after the second Release")
	}
}

// TestWaitForLeavePrefersAFinishedOutcome covers the one interleaving where
// both returns of the wait are ready at once: the leave finished and the
// caller's context ended before the wait looked. The finished leave is what the
// caller has to act on, so it has to win.
func TestWaitForLeavePrefersAFinishedOutcome(t *testing.T) {
	const destination = "leave-tie"
	connection := newLeaveTestConnection(destination)
	c := newLeaveTestConsumer(t, connection, destination, 1)
	wantErr := errors.New("leave failed")
	// Start neither the leave loop nor a teardown: the outcome is staged below,
	// and a running loop would overwrite it with its own.
	c.leaveMu.Lock()
	c.leaveStarted = true
	c.leaveMu.Unlock()
	c.mu.Lock()
	c.leaveErr = wantErr
	c.mu.Unlock()
	close(c.leaveFinished)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Both cases are ready, and select picks between ready cases at random, so
	// a single pass could take the context branch by luck. Repeating it is what
	// makes a random pick fail rather than a one-in-two chance of passing.
	for i := range 100 {
		finished, err := c.waitForLeave(ctx)
		if !finished {
			t.Fatalf("waitForLeave reported the caller's context ended on iteration %d, although the leave had finished", i)
		}
		if !errors.Is(err, wantErr) {
			t.Fatalf("waitForLeave error = %v, want %v", err, wantErr)
		}
	}
}

// TestStopTearsDownAfterAFailedLeave covers a Stop with no Drain before it, so
// the leave is met inside the stop's own drain step: the consumer has to be
// torn down anyway, and the error stays visible on both channels out.
func TestStopTearsDownAfterAFailedLeave(t *testing.T) {
	const destination = "stop-failed"
	connection := newLeaveTestConnection(destination)
	c := newLeaveTestConsumer(t, connection, destination, 1)
	c.leaveFn = func(context.Context, leaveRequest) error { return kerr.CoordinatorNotAvailable }

	err := c.Stop(context.Background())
	if !errors.Is(err, kerr.CoordinatorNotAvailable) {
		t.Fatalf("Stop error = %v, want it to wrap %v", err, kerr.CoordinatorNotAvailable)
	}
	if leaveTestConsumerRegistered(c) {
		t.Fatal("consumer is still registered after a leave that finished with an error")
	}
	timer := clock.NewReal().Timer(time.Second)
	defer timer.Stop()
	select {
	case got, open := <-c.Errors():
		if !open {
			t.Fatal("Errors() closed before delivering the leave error")
		}
		if !errors.Is(got, kerr.CoordinatorNotAvailable) {
			t.Fatalf("Errors() delivered %v, want it to wrap %v", got, kerr.CoordinatorNotAvailable)
		}
	case <-timer.C:
		t.Fatal("Errors() did not deliver the leave error")
	}
	if _, open := <-c.Errors(); open {
		t.Fatal("Errors() is still open after Stop")
	}
	select {
	case _, open := <-c.Messages():
		if open {
			t.Fatal("Messages() delivered a message after Stop")
		}
	case <-timer.C:
		t.Fatal("Messages() was not closed by Stop")
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop = %v, want nil", err)
	}
}

// TestStopAfterDrainTearsDownAfterAFailedLeave covers the runner's own order:
// Drain requests the leave and reports its failure without tearing anything
// down, and the Stop that follows has to finish the job.
func TestStopAfterDrainTearsDownAfterAFailedLeave(t *testing.T) {
	const destination = "stop-after-drain"
	connection := newLeaveTestConnection(destination)
	c := newLeaveTestConsumer(t, connection, destination, 1)
	c.leaveFn = func(context.Context, leaveRequest) error { return kerr.CoordinatorNotAvailable }

	if err := c.Drain(context.Background()); !errors.Is(err, kerr.CoordinatorNotAvailable) {
		t.Fatalf("Drain error = %v, want it to wrap %v", err, kerr.CoordinatorNotAvailable)
	}
	if !leaveTestConsumerRegistered(c) {
		t.Fatal("Drain deregistered the consumer; Drain never tears down")
	}
	if err := c.Stop(context.Background()); !errors.Is(err, kerr.CoordinatorNotAvailable) {
		t.Fatalf("Stop error = %v, want it to wrap %v", err, kerr.CoordinatorNotAvailable)
	}
	if leaveTestConsumerRegistered(c) {
		t.Fatal("consumer is still registered after Stop met the finished leave in its own wait")
	}
}

// TestStopKeepsConsumerWhenCallerContextEndsFirst covers the retry path on the
// runner's order, which is the route where the stop's own wait meets the leave:
// a Drain requests the leave and gives up on it, then a Stop waits for it in
// turn. A caller that gives up on that wait has not ended the leave, so the
// consumer stays registered and a later Stop completes it.
func TestStopKeepsConsumerWhenCallerContextEndsFirst(t *testing.T) {
	const destination = "stop-context"
	connection := newLeaveTestConnection(destination)
	c := newLeaveTestConsumer(t, connection, destination, 1)
	leaveReleased := make(chan struct{})
	drainCtx, cancelDrain := context.WithCancel(context.Background())
	defer cancelDrain()
	c.leaveFn = func(context.Context, leaveRequest) error {
		// End the drain's context with the leave still in flight. The leave runs
		// on its own context, so it stays here until the test releases it.
		cancelDrain()
		<-leaveReleased
		return nil
	}

	if err := c.Drain(drainCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Drain error = %v, want %v", err, context.Canceled)
	}
	if !leaveTestConsumerRegistered(c) {
		t.Fatal("Drain deregistered the consumer; Drain never tears down")
	}

	stopCtx, cancelStop := context.WithCancel(context.Background())
	defer cancelStop()
	stopDone := make(chan error, 1)
	go func() { stopDone <- c.Stop(stopCtx) }()
	// The drain above already asked for the leave, so the only leave request
	// left is the one this Stop makes from its own wait. Taking that request is
	// what says the stop reached its own wait, which is where the context has to
	// end: ending it earlier would fail Stop's opening context check instead.
	timer := clock.NewReal().Timer(time.Second)
	defer timer.Stop()
	select {
	case <-c.leaveRequestC:
	case <-timer.C:
		t.Fatal("Stop did not reach its own leave wait")
	}
	cancelStop()
	// A second timer, not the one above: the wait it bounds can only be
	// satisfied by the stop returning after the cancel, so a timer already
	// running would be a second ready case racing that return.
	stopTimer := clock.NewReal().Timer(time.Second)
	defer stopTimer.Stop()
	var err error
	select {
	case err = <-stopDone:
	case <-stopTimer.C:
		t.Fatal("Stop did not return after its context ended")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop error = %v, want %v", err, context.Canceled)
	}
	if kind, classified := driver.Classify(err); !classified || kind != driver.KindTransient {
		t.Fatalf("Stop error kind = %v (classified %v), want %v", kind, classified, driver.KindTransient)
	}
	if !leaveTestConsumerRegistered(c) {
		t.Fatal("consumer was deregistered by a Stop whose caller context ended before the leave finished")
	}

	close(leaveReleased)
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop = %v, want nil", err)
	}
	if leaveTestConsumerRegistered(c) {
		t.Fatal("consumer is still registered after the second Stop")
	}
}

// TestStopKeepsConsumerWhenDrainContextEndsFirst covers the other route to the
// same rule: a Stop with no Drain before it meets the leave inside its own
// drain step, and a context that ends there returns as that step's error, with
// the consumer left registered for a later Stop.
func TestStopKeepsConsumerWhenDrainContextEndsFirst(t *testing.T) {
	const destination = "stop-drain-context"
	connection := newLeaveTestConnection(destination)
	c := newLeaveTestConsumer(t, connection, destination, 1)
	leaveReleased := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.leaveFn = func(context.Context, leaveRequest) error {
		cancel()
		<-leaveReleased
		return nil
	}

	err := c.Stop(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop error = %v, want %v", err, context.Canceled)
	}
	if kind, classified := driver.Classify(err); !classified || kind != driver.KindTransient {
		t.Fatalf("Stop error kind = %v (classified %v), want %v", kind, classified, driver.KindTransient)
	}
	if !leaveTestConsumerRegistered(c) {
		t.Fatal("consumer was deregistered by a Stop whose context ended inside its drain step")
	}

	close(leaveReleased)
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop = %v, want nil", err)
	}
	if leaveTestConsumerRegistered(c) {
		t.Fatal("consumer is still registered after the second Stop")
	}
}

// newLeaveTestConnection returns a connection with the destination declared at
// no delay, which is the state of a destination that is not deferred: an
// undeclared destination faults every admission instead.
func newLeaveTestConnection(destination string) *conn {
	return &conn{
		consumers: make(map[*consumer]struct{}),
		delays:    map[string]time.Duration{destination: 0},
	}
}

// newLeaveTestConsumer builds the smallest consumer the teardown paths touch: a
// joined-looking group member whose broker calls are injected through leaveFn,
// with the maps the settle, drain and leave bookkeeping read. Nothing here
// fetches: the poll is already done and the franz-go client cannot dial.
func newLeaveTestConsumer(t *testing.T, connection *conn, destination string, budget int) *consumer {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers("localhost:1"), noDialKafkaOption())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(client.Close)
	pollDone := make(chan struct{})
	close(pollDone)
	messagesCapacity := budget
	if messagesCapacity < 1 {
		messagesCapacity = 1
	}
	c := &consumer{
		conn:            connection,
		client:          client,
		cfg:             driver.ConsumerConfig{Destinations: []string{destination}, Prefetch: budget},
		group:           "leave-test-group",
		destinations:    []string{destination},
		budgets:         map[string]int{destination: budget},
		messages:        make(chan driver.InboundMessage, messagesCapacity),
		errors:          make(chan error, 1),
		pollDone:        pollDone,
		stopDone:        make(chan struct{}),
		pauseReasons:    make(map[string]pauseReasonSet),
		unsettled:       make(map[string]int),
		outstanding:     make(map[partitionKey]int),
		settlers:        make(map[*settler]struct{}),
		trackers:        make(map[partitionKey]*ackTracker),
		owned:           make(map[partitionKey]bool),
		requeued:        make(map[partitionKey]int),
		discarded:       make(map[partitionKey]map[int64]struct{}),
		readAheadPaused: make(map[partitionKey]struct{}),
		pendingHeld:     make(map[partitionKey]int),
		settlerCh:       make(chan struct{}, 1),
		clock:           clock.NewReal(),
		cancelPoll:      func() {},
		leaveRequestC:   make(chan struct{}, 1),
		leaveStopC:      make(chan struct{}),
		leaveFinished:   make(chan struct{}),
		leaveLoopDone:   make(chan struct{}),
		forwarderStopC:  make(chan struct{}),
		leaveFn:         func(context.Context, leaveRequest) error { return nil },
	}
	connection.mu.Lock()
	connection.consumers[c] = struct{}{}
	connection.mu.Unlock()
	return c
}

// leaveTestConsumerRegistered reports whether the connection still holds the
// consumer, which is the registration conn.Close refuses on.
func leaveTestConsumerRegistered(c *consumer) bool {
	c.conn.mu.RLock()
	defer c.conn.mu.RUnlock()
	_, registered := c.conn.consumers[c]
	return registered
}
