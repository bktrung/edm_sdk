package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestWaitForTopicStateRetriesTransientListingError(t *testing.T) {
	fakeClock := clock.NewFake(time.Unix(0, 0))
	const simulatedPoll = 2 * time.Second
	const expectedCalls = 2
	transientCause := errors.New("temporary metadata listing failure")
	transientErr := classify("list_topics", kafkaErrorKind(transientCause), transientCause)
	kind, classified := driver.Classify(transientErr)
	if !classified || kind != driver.KindTransient {
		t.Fatalf("driver.Classify(transientErr) = (%v, %t), want (%v, true)", kind, classified, driver.KindTransient)
	}
	calls := 0
	installTopicWaitTestState(t, fakeClock, 0, func(_ *admin, _ context.Context, _ string, names ...string) (kadm.TopicDetails, error) {
		calls++
		if calls == 1 {
			fakeClock.Advance(simulatedPoll)
			return kadm.TopicDetails{}, transientErr
		}
		return visibleTopicDetails(names...), nil
	})

	if err := (&admin{}).waitForTopicState(context.Background(), "ensure_topology", []string{"transient"}, topicMustExist); err != nil {
		t.Fatalf("waitForTopicState returned error: %v", err)
	}
	if calls != expectedCalls {
		t.Fatalf("listTopics calls=%d, want %d", calls, expectedCalls)
	}
}

func TestWaitForTopicStateReturnsNonTransientListingError(t *testing.T) {
	fakeClock := clock.NewFake(time.Unix(0, 0))
	nonTransientErr := classify("list_topics", kafkaErrorKind(kerr.TopicAuthorizationFailed), kerr.TopicAuthorizationFailed)
	kind, classified := driver.Classify(nonTransientErr)
	if !classified || kind == driver.KindTransient {
		t.Fatalf("driver.Classify(nonTransientErr) = (%v, %t), want a classified non-transient error", kind, classified)
	}
	calls := 0
	installTopicWaitTestState(t, fakeClock, 0, func(_ *admin, _ context.Context, _ string, names ...string) (kadm.TopicDetails, error) {
		calls++
		if calls == 1 {
			defer fakeClock.Advance(kafkaTopologyVisibilityPoll)
			return kadm.TopicDetails{}, nonTransientErr
		}
		return visibleTopicDetails(names...), nil
	})

	err := (&admin{}).waitForTopicState(context.Background(), "ensure_topology", []string{"forbidden"}, topicMustExist)
	if !errors.Is(err, nonTransientErr) {
		t.Fatalf("waitForTopicState error=%v, want original listing error %v", err, nonTransientErr)
	}
	if err != nonTransientErr { //nolint:errorlint // identity is the assertion: the wait must return the listing error unchanged
		t.Fatalf("waitForTopicState error=%T(%v), want exact listing error %T(%v)", err, err, nonTransientErr, nonTransientErr)
	}
	if calls != 1 {
		t.Fatalf("listTopics calls=%d, want 1", calls)
	}
}

func TestWaitForTopicStateWaitsPastPreviousFiveSecondBound(t *testing.T) {
	fakeClock := clock.NewFake(time.Unix(0, 0))
	const visibleAfterPolls = 5
	const simulatedPoll = 2 * time.Second
	calls := 0
	installTopicWaitTestState(t, fakeClock, 0, func(_ *admin, ctx context.Context, _ string, names ...string) (kadm.TopicDetails, error) {
		calls++
		if calls < visibleAfterPolls {
			fakeClock.Advance(simulatedPoll)
			if calls == visibleAfterPolls-1 {
				watchdog := clock.NewReal().Timer(100 * time.Millisecond)
				defer watchdog.Stop()
				select {
				case <-ctx.Done():
					return kadm.TopicDetails{}, nil
				case <-watchdog.C:
				}
			}
			return kadm.TopicDetails{}, nil
		}
		return visibleTopicDetails(names...), nil
	})

	if err := (&admin{}).waitForTopicState(context.Background(), "ensure_topology", []string{"delayed"}, topicMustExist); err != nil {
		t.Fatalf("waitForTopicState returned error: %v", err)
	}
	if calls != visibleAfterPolls {
		t.Fatalf("listTopics calls=%d, want %d", calls, visibleAfterPolls)
	}
}

func TestWaitForTopicStateStopsWhenCallerCancels(t *testing.T) {
	fakeClock := clock.NewFake(time.Unix(0, 0))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	installTopicWaitTestState(t, fakeClock, 60*time.Second, func(_ *admin, waitCtx context.Context, _ string, _ ...string) (kadm.TopicDetails, error) {
		close(started)
		<-waitCtx.Done()
		return kadm.TopicDetails{}, nil
	})

	result := make(chan error, 1)
	go func() {
		result <- (&admin{}).waitForTopicState(ctx, "ensure_topology", []string{"cancelled"}, topicMustExist)
	}()
	<-started
	cancel()
	watchdog := clock.NewReal().Timer(time.Second)
	defer watchdog.Stop()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waitForTopicState error=%v, want context.Canceled", err)
		}
	case <-watchdog.C:
		t.Fatal("waitForTopicState did not stop the broker call after cancellation")
	}
}

func TestWaitForTopicStateStopsAtCeilingWithoutCallerDeadline(t *testing.T) {
	fakeClock := clock.NewFake(time.Unix(0, 0))
	const ceiling = 10 * time.Second
	started := make(chan struct{})
	installTopicWaitTestState(t, fakeClock, ceiling, func(_ *admin, ctx context.Context, _ string, _ ...string) (kadm.TopicDetails, error) {
		close(started)
		<-ctx.Done()
		return kadm.TopicDetails{}, nil
	})

	result := make(chan error, 1)
	go func() {
		result <- (&admin{}).waitForTopicState(context.Background(), "ensure_topology", []string{"missing"}, topicMustExist)
	}()
	<-started
	fakeClock.Advance(ceiling)
	watchdog := clock.NewReal().Timer(time.Second)
	defer watchdog.Stop()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("waitForTopicState error=%v, want context.DeadlineExceeded", err)
		}
		if !strings.Contains(err.Error(), `topics ["missing"] did not become present`) {
			t.Fatalf("waitForTopicState error=%q, want topic name and present-state detail", err)
		}
	case <-watchdog.C:
		t.Fatal("waitForTopicState did not stop the broker call at the ceiling")
	}
}

func installTopicWaitTestState(t *testing.T, fakeClock clock.Clock, timeout time.Duration, listTopics func(*admin, context.Context, string, ...string) (kadm.TopicDetails, error)) {
	t.Helper()
	previousClock := kafkaTopologyClock
	previousTimeout := kafkaTopologyVisibilityTimeout
	previousListTopics := kafkaTopologyListTopics
	kafkaTopologyClock = fakeClock
	if timeout != 0 {
		kafkaTopologyVisibilityTimeout = timeout
	}
	kafkaTopologyListTopics = listTopics
	t.Cleanup(func() {
		kafkaTopologyClock = previousClock
		kafkaTopologyVisibilityTimeout = previousTimeout
		kafkaTopologyListTopics = previousListTopics
	})
}

func visibleTopicDetails(names ...string) kadm.TopicDetails {
	topics := make(kadm.TopicDetails, len(names))
	for _, name := range names {
		topics[name] = kadm.TopicDetail{
			Topic:      name,
			Partitions: kadm.PartitionDetails{0: {}},
		}
	}
	return topics
}
