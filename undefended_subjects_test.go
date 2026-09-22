package f1

import (
	"context"
	"errors"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// TestOptionSubsetCopiesOnlyTheRequestedDriverOptions pins the option boundary
// passed to a broker adapter: the prefix is removed, unrelated options stay out,
// and a source map entry is copied rather than renamed in place.
func TestOptionSubsetCopiesOnlyTheRequestedDriverOptions(t *testing.T) {
	options := map[string]string{
		"kafka.fetchMaxWait": "200ms",
		"rabbitmq.queueType": "quorum",
		"fetchMaxWait":       "wrong",
	}
	got := optionSubset(options, "kafka.")
	if len(got) != 1 || got["fetchMaxWait"] != "200ms" {
		t.Fatalf("optionSubset() = %#v, want only kafka.fetchMaxWait as fetchMaxWait=200ms", got)
	}
	if len(options) != 3 || options["kafka.fetchMaxWait"] != "200ms" {
		t.Fatalf("optionSubset() modified source options: %#v", options)
	}
}

// TestRawSubscriptionFromConfigPreservesConfiguredValues pins the seeding
// conversion used while decoding YAML: every caller setting survives in the
// raw shape, including map keys and retry/fairness values.
func TestRawSubscriptionFromConfigPreservesConfiguredValues(t *testing.T) {
	input := SubscriptionConfig{
		Topics: []string{"orders.created"}, Mode: OrderedByKey, Concurrency: 3, Prefetch: 12,
		Priorities: []Priority{PriorityHigh, PriorityLow},
		Fairness: FairnessConfig{
			Weights: map[Priority]int{PriorityHigh: 8}, Budgets: map[Priority]time.Duration{PriorityLow: 2 * time.Second},
			RetryWeightDivisor: 2, PrefetchFactor: 3, DisableDeadlinePromotion: true,
		},
		Retry:          RetryConfig{MaxAttempts: 4, InitialInterval: time.Second, Multiplier: 2, MaxInterval: 30 * time.Second, Tiers: []time.Duration{time.Second}},
		HandlerTimeout: 5 * time.Second, UnmatchedPolicy: DeadLetter,
	}
	got := rawSubscriptionFromConfig(input)
	if len(got.Topics) != 1 || got.Topics[0] != "orders.created" || got.Mode != rawMode(OrderedByKey) || got.Concurrency != 3 || got.Prefetch != 12 {
		t.Fatalf("raw subscription core fields = %#v, want configured values", got)
	}
	if len(got.Priorities) != 2 || got.Priorities[0] != rawPriority(PriorityHigh) || got.Priorities[1] != rawPriority(PriorityLow) {
		t.Fatalf("raw priorities = %#v, want high,low", got.Priorities)
	}
	if got.Fairness.Weights["high"] != 8 || got.Fairness.Budgets["low"] != 2*time.Second || got.Fairness.RetryWeightDivisor != 2 || got.Fairness.PrefetchFactor != 3 || !got.Fairness.DisableDeadlinePromotion {
		t.Fatalf("raw fairness = %#v, want configured fairness", got.Fairness)
	}
	if got.Retry.MaxAttempts != 4 || got.Retry.InitialInterval != time.Second || got.Retry.Multiplier != 2 || got.Retry.MaxInterval != 30*time.Second || len(got.Retry.Tiers) != 1 || got.Retry.Tiers[0] != time.Second {
		t.Fatalf("raw retry = %#v, want configured retry", got.Retry)
	}
	if got.HandlerTimeout != 5*time.Second || got.UnmatchedPolicy != rawPolicy(DeadLetter) {
		t.Fatalf("raw terminal fields = %#v/%v, want configured values", got.HandlerTimeout, got.UnmatchedPolicy)
	}
}

// TestErrorWrappersPreserveUnderlyingText pins the operator-facing text of
// both internal error wrappers when they expose an underlying cause.
func TestErrorWrappersPreserveUnderlyingText(t *testing.T) {
	cause := errors.New("underlying failure")
	if got := (&classifiedError{err: cause}).Error(); got != cause.Error() {
		t.Fatalf("classifiedError.Error() = %q, want %q", got, cause.Error())
	}
	if got := (&detailsError{err: cause}).Error(); got != cause.Error() {
		t.Fatalf("detailsError.Error() = %q, want %q", got, cause.Error())
	}
}

// TestPriorityHintMapsLogicalPrioritiesToWireLevels pins the driver's strict
// priority levels used in every outbound message.
func TestPriorityHintMapsLogicalPrioritiesToWireLevels(t *testing.T) {
	for _, test := range []struct {
		priority Priority
		want     uint8
	}{
		{priority: PriorityHigh, want: 1},
		{priority: PriorityMedium, want: 0},
		{priority: PriorityLow, want: 2},
	} {
		if got := priorityHint(test.priority); got != test.want {
			t.Errorf("priorityHint(%v) = %d, want %d", test.priority, got, test.want)
		}
	}
}

// TestZeroConfigPredicatesDistinguishEmptyAndExplicitOverrides pins the merge
// decision: empty structs are omitted, while each supported override shape is
// treated as present.
func TestZeroConfigPredicatesDistinguishEmptyAndExplicitOverrides(t *testing.T) {
	if !fairnessZero(FairnessConfig{}) {
		t.Fatal("fairnessZero(empty) = false, want true")
	}
	for _, fairness := range []FairnessConfig{
		{Weights: map[Priority]int{PriorityHigh: 1}},
		{Budgets: map[Priority]time.Duration{PriorityLow: time.Second}},
		{RetryWeightDivisor: 2},
		{PrefetchFactor: 2},
		{DisableDeadlinePromotion: true},
	} {
		if fairnessZero(fairness) {
			t.Fatalf("fairnessZero(%#v) = true, want false", fairness)
		}
	}
	if !retryZero(RetryConfig{}) {
		t.Fatal("retryZero(empty) = false, want true")
	}
	for _, retry := range []RetryConfig{
		{MaxAttempts: 2}, {InitialInterval: time.Second}, {Multiplier: 2}, {MaxInterval: time.Second}, {Tiers: []time.Duration{time.Second}},
	} {
		if retryZero(retry) {
			t.Fatalf("retryZero(%#v) = true, want false", retry)
		}
	}
}

// TestDestinationDelaysIncludesOnlyDeclaredPositiveDelays pins the scheduler
// input: immediate destinations are absent, while a declared delay is exact.
func TestDestinationDelaysIncludesOnlyDeclaredPositiveDelays(t *testing.T) {
	got := destinationDelays(driver.TopologySpec{Destinations: []driver.DestinationSpec{
		{Name: "delayed", Delay: 2 * time.Second},
		{Name: "immediate"},
		{Name: "negative", Delay: -time.Second},
	}})
	if len(got) != 1 || got["delayed"] != 2*time.Second {
		t.Fatalf("destinationDelays() = %#v, want delayed=2s only", got)
	}
}

// TestRunnerIsDrainingReadsTheTeardownState pins the branch that keeps a
// cancellation wait open while runner teardown is still in progress.
func TestRunnerIsDrainingReadsTheTeardownState(t *testing.T) {
	runner := &Runner{}
	if runnerIsDraining(runner) {
		t.Fatal("runnerIsDraining(new Runner) = true, want false")
	}
	runner.draining = true
	if !runnerIsDraining(runner) {
		t.Fatal("runnerIsDraining(draining Runner) = false, want true")
	}
}

// TestRunnerOwnerGenerationEndsOnlyAfterSourcesClose pins the drain start
// condition: both outstanding work and a pending opener must be gone.
func TestRunnerOwnerGenerationEndsOnlyAfterSourcesClose(t *testing.T) {
	for _, test := range []struct {
		name        string
		outstanding int
		openPending bool
		wantEnded   bool
	}{
		{name: "empty", wantEnded: true},
		{name: "delivery outstanding", outstanding: 1},
		{name: "consumer open pending", openPending: true},
		{name: "both pending", outstanding: 1, openPending: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner := &runnerOwner{outstanding: test.outstanding, openPending: test.openPending}
			if got := owner.generationEnded(); got != test.wantEnded {
				t.Fatalf("generationEnded() = %t, want %t", got, test.wantEnded)
			}
		})
	}
}

// TestSuccessorAttemptNormalizesMissingAttempts pins the attempt carried by a
// successor event: an absent attempt becomes one, and positive attempts stay.
func TestSuccessorAttemptNormalizesMissingAttempts(t *testing.T) {
	for _, test := range []struct {
		attempt int
		want    int
	}{
		{attempt: 0, want: 1},
		{attempt: 1, want: 1},
		{attempt: 3, want: 3},
	} {
		if got := successorAttempt(Envelope{Attempt: test.attempt}); got != test.want {
			t.Errorf("successorAttempt(%d) = %d, want %d", test.attempt, got, test.want)
		}
	}
}

// TestSuccessorTopicPrefersTheConsumingFamily pins successor routing when an
// event type belongs to another topic, while preserving the historical fallback.
func TestSuccessorTopicPrefersTheConsumingFamily(t *testing.T) {
	client, runner := newRetryBridgeRunner(t, &dispatchProducer{}, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()
	envelope, message := divergentBridgeMessage(t, &dispatchSettler{})
	if got := successorTopic(runner, envelope, message); got != "orders.created" {
		t.Fatalf("successorTopic(consuming family) = %q, want orders.created", got)
	}
	message.Destination = "outside.declared.family"
	if got := successorTopic(runner, envelope, message); got != "payments.charged" {
		t.Fatalf("successorTopic(fallback) = %q, want payments.charged", got)
	}
}
