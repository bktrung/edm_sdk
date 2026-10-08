package f1

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	//nolint:depguard // subscription error-path tests use the deterministic in-memory driver.
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem"
)

func TestSubscribeRejectsMalformedEnvironmentValues(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		value     string
		wantCause func(error) bool
	}{
		{name: "mode", key: "mode", value: "bogus"},
		{name: "concurrency", key: "concurrency", value: "not-an-int", wantCause: hasSubscriptionNumError},
		{name: "prefetch", key: "prefetch", value: "not-an-int", wantCause: hasSubscriptionNumError},
		{name: "priorities", key: "priorities", value: "bogus", wantCause: hasSubscriptionPriorityError},
		{name: "handler timeout", key: "handlerTimeout", value: "not-a-duration", wantCause: hasSubscriptionDurationError},
		{name: "unmatched policy", key: "unmatchedPolicy", value: "bogus"},
		{name: "fairness retry weight divisor", key: "fairness.retryWeightDivisor", value: "not-an-int", wantCause: hasSubscriptionNumError},
		{name: "fairness prefetch factor", key: "fairness.prefetchFactor", value: "not-an-int", wantCause: hasSubscriptionNumError},
		{name: "fairness deadline promotion", key: "fairness.disableDeadlinePromotion", value: "not-a-bool", wantCause: hasSubscriptionNumError},
		{name: "fairness high weight", key: "fairness.weights.high", value: "not-an-int", wantCause: hasSubscriptionNumError},
		{name: "fairness medium weight", key: "fairness.weights.medium", value: "not-an-int", wantCause: hasSubscriptionNumError},
		{name: "fairness low weight", key: "fairness.weights.low", value: "not-an-int", wantCause: hasSubscriptionNumError},
		{name: "fairness high budget", key: "fairness.budgets.high", value: "not-a-duration", wantCause: hasSubscriptionDurationError},
		{name: "fairness medium budget", key: "fairness.budgets.medium", value: "not-a-duration", wantCause: hasSubscriptionDurationError},
		{name: "fairness low budget", key: "fairness.budgets.low", value: "not-a-duration", wantCause: hasSubscriptionDurationError},
		{name: "retry max attempts", key: "retry.maxAttempts", value: "not-an-int", wantCause: hasSubscriptionNumError},
		{name: "retry initial interval", key: "retry.initialInterval", value: "not-a-duration", wantCause: hasSubscriptionDurationError},
		{name: "retry multiplier", key: "retry.multiplier", value: "not-a-float", wantCause: hasSubscriptionNumError},
		{name: "retry max interval", key: "retry.maxInterval", value: "not-a-duration", wantCause: hasSubscriptionDurationError},
		{name: "retry tiers", key: "retry.tiers", value: "not-a-duration", wantCause: hasSubscriptionDurationError},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const subscriptionName = "environmentErrors"
			prefix := subscriptionEnvPrefix(subscriptionName)
			fullKey := envKey(prefix, tc.key)
			t.Setenv(fullKey, tc.value)

			client := newSubscriptionEnvironmentErrorClient(t)
			runner, err := client.Subscribe(context.Background(), Subscription{
				Name:   subscriptionName,
				Topics: []string{"orders.created"},
			})
			if runner != nil {
				t.Fatalf("Subscribe() runner = %#v, want nil", runner)
			}
			if err == nil {
				t.Fatal("Subscribe() error = nil, want environment parse error")
			}
			wantPrefix := "f1: " + fullKey + ": "
			if !strings.HasPrefix(err.Error(), wantPrefix) {
				t.Fatalf("Subscribe() error = %v, want prefix %q", err, wantPrefix)
			}
			if tc.name == "unmatched policy" && !strings.Contains(err.Error(), "unsupported unmatched policy") {
				t.Fatalf("Subscribe() error = %v, want unsupported policy cause", err)
			}
			if tc.wantCause != nil && !tc.wantCause(err) {
				t.Fatalf("Subscribe() error = %v, want wrapped parse cause", err)
			}
		})
	}
}

func newSubscriptionEnvironmentErrorClient(t *testing.T) *Client {
	t.Helper()
	client, err := New(context.Background(), testClientConfig(t), WithDriver(inmem.New()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client
}

func hasSubscriptionNumError(err error) bool {
	var target *strconv.NumError
	return errors.As(err, &target)
}

func hasSubscriptionDurationError(err error) bool {
	return strings.Contains(err.Error(), "time: invalid duration")
}

func hasSubscriptionPriorityError(err error) bool {
	var target *UnrecognisedValueError
	return errors.As(err, &target)
}
