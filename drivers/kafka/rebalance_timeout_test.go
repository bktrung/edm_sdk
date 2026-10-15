package kafka

import (
	"context"
	"errors"
	"maps"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestConsumerBuildChecksRebalanceTimeout(t *testing.T) {
	const explicitDrainTimeout = 250 * time.Millisecond

	cases := []struct {
		name             string
		drainTimeout     time.Duration
		rebalanceTimeout time.Duration
		wantErr          bool
	}{
		{
			name:             "equal explicit revoke wait bound",
			drainTimeout:     explicitDrainTimeout,
			rebalanceTimeout: explicitDrainTimeout,
			wantErr:          true,
		},
		{
			name:             "below default revoke wait bound",
			rebalanceTimeout: defaultKafkaRebalanceDrainTimeout - time.Second,
			wantErr:          true,
		},
		{
			name:             "above explicit revoke wait bound",
			drainTimeout:     explicitDrainTimeout,
			rebalanceTimeout: explicitDrainTimeout + time.Second,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bound := tc.drainTimeout
			if bound == 0 {
				bound = defaultKafkaRebalanceDrainTimeout
			}
			connection := &conn{
				clientOpts: []kgo.Opt{noDialKafkaOption()},
				driverOptions: map[string]string{
					"kafka.rebalanceTimeout": tc.rebalanceTimeout.String(),
				},
				rebalanceDrainTimeout: tc.drainTimeout,
			}

			value, err := connection.Consumer(context.Background(), driver.ConsumerConfig{
				Group:        "rebalance-timeout-test",
				Destinations: []string{"topic"},
				Prefetch:     1,
			})
			if tc.wantErr {
				if value != nil {
					if built, ok := value.(*consumer); !ok || built != nil {
						t.Fatalf("Consumer() value = %T, want nil *consumer", value)
					}
				}
				if err == nil {
					t.Fatal("Consumer() error = nil, want rebalance-timeout refusal")
				}
				for _, want := range []string{
					"kafka.rebalanceTimeout",
					tc.rebalanceTimeout.String(),
					bound.String(),
				} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("Consumer() error = %q, want %q", err, want)
					}
				}
				var classified *driver.Error
				if !errors.As(err, &classified) {
					t.Fatalf("Consumer() error = %T, want *driver.Error", err)
				}
				if got := classified.Kind(); got != driver.KindFatal {
					t.Fatalf("Consumer() error kind = %v, want %v", got, driver.KindFatal)
				}
				return
			}

			if err != nil {
				t.Fatalf("Consumer() error = %v, want nil", err)
			}
			built, ok := value.(*consumer)
			if !ok {
				t.Fatalf("Consumer() value = %T, want *consumer", value)
			}
			stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := built.Stop(stopCtx); err != nil {
				t.Fatalf("Stop() error = %v", err)
			}
			select {
			case <-built.stopDone:
			default:
				t.Fatal("Stop() returned before consumer stopped")
			}
		})
	}
}

// revokeCommitTestPartitions is the revoked assignment the batched-commit tests
// drive: partitions 0, 1 and 2 carry a tracker and partition 3 was revoked with
// no tracker at all.
func revokeCommitTestPartitions() map[string][]int32 {
	return map[string][]int32{"topic": {0, 1, 2, 3}}
}

// newRevokeCommitConsumer returns a consumer that owns partitions 0, 1 and 2 of
// "topic" and holds one unsettled delivery on partition 0, so a revoke of the
// assignment waits for that delivery and then commits the prefixes the other
// partitions settled. Partition 0's tracker is at 41 with nothing settled,
// partition 1 settled through offset 12, and partition 2 settled through offset
// 7.
func newRevokeCommitConsumer(t *testing.T, drainTimeout time.Duration) (*consumer, *clock.Fake, *settler) {
	t.Helper()
	held := partitionKey{destination: "topic", partition: 0}
	settledOne := partitionKey{destination: "topic", partition: 1}
	settledTwo := partitionKey{destination: "topic", partition: 2}
	heldTracker := newAckTracker(41)
	oneTracker := newAckTracker(10)
	for offset := int64(10); offset <= 12; offset++ {
		if err := oneTracker.Ack(offset, nil); err != nil {
			t.Fatalf("Ack(%d) = %v, want nil", offset, err)
		}
	}
	twoTracker := newAckTracker(7)
	if err := twoTracker.Ack(7, nil); err != nil {
		t.Fatalf("Ack(7) = %v, want nil", err)
	}
	pending := &settler{
		record:  &kgo.Record{Topic: "topic", Partition: 0, Offset: 41},
		tracker: heldTracker,
		key:     held,
	}
	fakeClock := clock.NewFake(time.Unix(0, 0))
	c := &consumer{
		errors:                make(chan error, 8),
		clock:                 fakeClock,
		rebalanceDrainTimeout: drainTimeout,
		trackers:              map[partitionKey]*ackTracker{held: heldTracker, settledOne: oneTracker, settledTwo: twoTracker},
		owned:                 map[partitionKey]bool{held: true, settledOne: true, settledTwo: true},
		settlers:              map[*settler]struct{}{pending: {}},
		settlerCh:             make(chan struct{}, 1),
		pauseReasons:          make(map[string]pauseReasonSet),
	}
	return c, fakeClock, pending
}

// waitForFakeWaiter blocks until the fake clock has a waiter registered, so a
// test never advances time before the code under test armed its timer. A
// real-clock guard bounds the wait: code whose timer is missing has nothing to
// register, and failing here names that instead of hanging.
func waitForFakeWaiter(t *testing.T, fakeClock *clock.Fake) {
	t.Helper()
	guard := clock.NewReal().Timer(time.Second)
	defer guard.Stop()
	for fakeClock.NumWaiters() == 0 {
		select {
		case <-guard.C:
			t.Fatal("the revoke did not arm its fake timer")
		default:
			runtime.Gosched()
		}
	}
}

// requireRevokeRunning fails when the revoke callback has already returned.
func requireRevokeRunning(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
		t.Fatal("onPartitionsRevoked returned before its bound ended")
	default:
	}
}

// revokeCommitExpiredErrors asserts that the consumer reported want bound-expired
// errors, one per partition the bound left uncommitted. The rebalance
// notification the callback also publishes is skipped.
func revokeCommitExpiredErrors(t *testing.T, c *consumer, want int) {
	t.Helper()
	found := 0
	for drained := false; !drained; {
		select {
		case err := <-c.Errors():
			if !errors.Is(err, errRevokeCommitBound) {
				continue
			}
			if kind, classified := driver.Classify(err); !classified || kind != driver.KindTransient {
				t.Fatalf("revoke commit error %v classified %v, %v; want a transient driver error", err, kind, classified)
			}
			found++
		default:
			drained = true
		}
	}
	if found != want {
		t.Fatalf("bound-expired errors = %d, want %d", found, want)
	}
}

// TestConsumerRevokeCommitIsBatchedAndBounded drives the revoke's final commit
// through the injected clock. A held settler keeps the wait for its whole bound,
// and a commit sender that answers only when its context ends keeps the one
// batched request open until the commit's own bound expires.
func TestConsumerRevokeCommitIsBatchedAndBounded(t *testing.T) {
	const drainTimeout = 300 * time.Millisecond
	const commitBound = drainTimeout / 3

	t.Run("the bound ends the batched request", func(t *testing.T) {
		c, fakeClock, held := newRevokeCommitConsumer(t, drainTimeout)
		points := make(chan map[partitionKey]int64, 1)
		observed := make(chan error, 1)
		calls := 0
		c.commitFn = func(ctx context.Context, sent map[partitionKey]int64) map[partitionKey]error {
			calls++
			points <- sent
			<-ctx.Done()
			observed <- ctx.Err()
			return nil
		}

		start := fakeClock.Now()
		revokeDone := make(chan struct{})
		go func() {
			c.onPartitionsRevoked(context.Background(), nil, revokeCommitTestPartitions())
			close(revokeDone)
		}()

		// Partition 0's delivery never settles, so the wait runs to its bound.
		waitForFakeWaiter(t, fakeClock)
		requireRevokeRunning(t, revokeDone)
		fakeClock.Advance(drainTimeout)

		// One request carries every revoked partition that has a tracker, at the
		// prefix the wait left behind. Partition 3 has none and is absent.
		want := map[partitionKey]int64{
			{destination: "topic", partition: 0}: 41,
			{destination: "topic", partition: 1}: 13,
			{destination: "topic", partition: 2}: 8,
		}
		if got := <-points; !maps.Equal(got, want) {
			t.Fatalf("revoke commit points = %v, want %v", got, want)
		}

		// The request is still unanswered, and the callback owes it the commit
		// timer alone: the advance above consumed the wait's timer.
		waitForFakeWaiter(t, fakeClock)
		requireRevokeRunning(t, revokeDone)
		fakeClock.Advance(commitBound)

		select {
		case <-revokeDone:
		case <-clock.NewReal().Timer(time.Second).C:
			t.Fatal("onPartitionsRevoked did not return after drain plus one third")
		}
		if elapsed := fakeClock.Since(start); elapsed != drainTimeout+commitBound {
			t.Fatalf("revoke returned after %s of fake time, want %s", elapsed, drainTimeout+commitBound)
		}
		if err := <-observed; err == nil {
			t.Fatal("commit sender was not cancelled at the bound")
		}
		if calls != 1 {
			t.Fatalf("commit sender calls = %d, want 1", calls)
		}
		if err := held.tracker.Ack(41, nil); !errors.Is(err, ErrRevoked) {
			t.Fatalf("Ack after the revoke = %v, want ErrRevoked", err)
		}
		revokeCommitExpiredErrors(t, c, len(want))
	})

	t.Run("a bound with no room starts no request", func(t *testing.T) {
		c, fakeClock, _ := newRevokeCommitConsumer(t, 2*time.Nanosecond)
		commits := 0
		c.commitFn = func(context.Context, map[partitionKey]int64) map[partitionKey]error {
			commits++
			return nil
		}

		revokeDone := make(chan struct{})
		go func() {
			c.onPartitionsRevoked(context.Background(), nil, revokeCommitTestPartitions())
			close(revokeDone)
		}()

		waitForFakeWaiter(t, fakeClock)
		fakeClock.Advance(2 * time.Nanosecond)

		select {
		case <-revokeDone:
		case <-clock.NewReal().Timer(time.Second).C:
			t.Fatal("onPartitionsRevoked did not return without starting a request")
		}
		if commits != 0 {
			t.Fatalf("commit sender calls = %d, want none with a bound of %s", commits, c.commitBound())
		}
		revokeCommitExpiredErrors(t, c, 3)
	})
}
