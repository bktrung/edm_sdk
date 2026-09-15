package f1

import (
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/sched"
)

const (
	retryStormWindow        = 200
	retryStormBacklog       = retryStormWindow + 1
	retryStormTierCount     = 5
	retryStormConcurrency   = 5000
	retryStormDefaultDivide = 2
)

func TestRunnerSchedulerPreservesRetryStormFairness(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	defaults := defaultSubscription()
	subscription := retryStormSubscription()
	runner := &Runner{
		client:       &Client{options: clientOptions{clock: fake}},
		subscription: subscription,
	}
	scheduler, err := newRunnerScheduler(runner)
	if err != nil {
		t.Fatal(err)
	}
	freshIDs, laneIDs := retryStormLaneIDs(subscription)
	if got, want := len(laneIDs), len(subscription.Priorities)*(1+retryTiers(subscription.Retry)); got != want {
		t.Fatalf("scheduler lanes = %d, want %d", got, want)
	}
	for _, id := range laneIDs {
		for range retryStormBacklog {
			if err := scheduler.Enqueue(id, sched.Item{Value: id}); err != nil {
				t.Fatalf("enqueue %s: %v", id, err)
			}
		}
	}
	wantPending := len(laneIDs) * retryStormBacklog
	if got := scheduler.Pending(); got != wantPending {
		t.Fatalf("saturation pending = %d, want %d", got, wantPending)
	}
	for _, id := range laneIDs {
		if got := scheduler.Depth(id); got != retryStormBacklog {
			t.Fatalf("saturation depth for %s = %d, want %d", id, got, retryStormBacklog)
		}
	}

	retrySeen := make(map[string]int)
	freshCount := 0
	for i := range retryStormWindow {
		item, ok := scheduler.Next()
		if !ok {
			t.Fatalf("scheduler emptied during saturation window at %d", i)
		}
		id := item.Value.(string)
		if _, ok := freshIDs[id]; ok {
			freshCount++
			continue
		}
		retrySeen[id]++
	}
	for _, id := range laneIDs {
		if _, fresh := freshIDs[id]; fresh {
			continue
		}
		if retrySeen[id] == 0 {
			t.Fatalf("retry tier %s was not independently dispatched", id)
		}
	}

	freshWeight, totalWeight := retryStormExpectedWeights(defaults)
	numerator := retryStormWindow * freshWeight
	if numerator%totalWeight != 0 {
		t.Fatalf("fresh share is not integral for window %d: %d/%d", retryStormWindow, freshWeight, totalWeight)
	}
	wantFresh := numerator / totalWeight
	if freshCount != wantFresh {
		t.Fatalf("fresh dispatches = %d, want %d from weights %d/%d", freshCount, wantFresh, freshWeight, totalWeight)
	}
}

func retryStormSubscription() Subscription {
	defaults := defaultSubscription()
	fairness := cloneFairness(defaults.Fairness)
	fairness.RetryWeightDivisor = 0
	fairness.DisableAging = true
	retry := cloneRetry(defaults.Retry)
	retry.Tiers = make([]time.Duration, retryStormTierCount)
	return Subscription{
		Topics:      []string{"orders.created"},
		Concurrency: retryStormConcurrency,
		Priorities:  append([]Priority(nil), defaults.Priorities...),
		Fairness:    fairness,
		Retry:       retry,
	}
}

func retryStormLaneIDs(subscription Subscription) (map[string]struct{}, []string) {
	topic := topicFor(subscription.Topics[0])
	freshIDs := make(map[string]struct{}, len(subscription.Priorities))
	laneIDs := make([]string, 0, len(subscription.Priorities)*(1+retryTiers(subscription.Retry)))
	for _, priority := range subscription.Priorities {
		mainID := schedulerLaneID(topic, priority, 0)
		freshIDs[mainID] = struct{}{}
		laneIDs = append(laneIDs, mainID)
		for tier := 1; tier <= retryTiers(subscription.Retry); tier++ {
			laneIDs = append(laneIDs, schedulerLaneID(topic, priority, tier))
		}
	}
	return freshIDs, laneIDs
}

func retryStormExpectedWeights(defaults SubscriptionConfig) (freshWeight, totalWeight int) {
	for _, priority := range defaults.Priorities {
		weight := defaults.Fairness.Weights[priority]
		freshWeight += weight
		totalWeight += weight
		retryWeight := max(weight/retryStormDefaultDivide, 1)
		totalWeight += retryWeight
	}
	return freshWeight, totalWeight
}
