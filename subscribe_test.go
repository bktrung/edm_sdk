package f1

import (
	"context"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestSubscribeRejectsOrderedByKeyWhenUnavailable(t *testing.T) {
	t.Parallel()
	conn := &testConn{caps: driver.Capabilities{}, info: testBrokerInfo()}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&testDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	status := featureStatus(client.Limits(), "ordered_by_key")
	if status.Mode != FeatureUnavailable {
		t.Fatalf("ordered_by_key limit = %v, want unavailable", status.Mode)
	}
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:        "orders",
		Topics:      []string{"orders.created"},
		Mode:        OrderedByKey,
		Concurrency: 2,
	})
	if runner != nil || err == nil || !strings.Contains(err.Error(), "ordered_by_key") {
		t.Fatalf("Subscribe() = runner %v, error %v, want ordered_by_key construction error", runner, err)
	}
}

func TestSubscribeRejectsUnsupportedMode(t *testing.T) {
	client := newPublishClient(t, &recordingProducer{})
	_, err := client.Subscribe(context.Background(), Subscription{
		Name:        "orders",
		Topics:      []string{"orders.created"},
		Mode:        Mode(99),
		Concurrency: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "subscriptions.orders.mode") {
		t.Fatalf("Subscribe() error = %v, want unsupported mode", err)
	}
}

func TestSubscribeAcceptsOrderedByKeyWhenNative(t *testing.T) {
	t.Parallel()
	conn := &testConn{caps: driver.Capabilities{OrderedByKey: true}, info: testBrokerInfo()}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&testDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	status := featureStatus(client.Limits(), "ordered_by_key")
	if status.Mode != FeatureNative {
		t.Fatalf("ordered_by_key limit = %v, want native", status.Mode)
	}
	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:        "orders",
		Topics:      []string{"orders.created"},
		Mode:        OrderedByKey,
		Concurrency: 2,
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v, want ordered mode to be accepted", err)
	}
	if runner == nil || runner.config.Mode != OrderedByKey || runner.config.Concurrency != 2 {
		t.Fatalf("resolved subscription = %#v, want ordered mode with concurrency 2", runner.config)
	}
}

func TestSubscribeRejectsPrefetchBelowLaneCount(t *testing.T) {
	t.Parallel()
	client := newPublishClient(t, &recordingProducer{})
	_, err := client.Subscribe(context.Background(), Subscription{
		Name:     "orders",
		Topics:   []string{"orders.created"},
		Prefetch: 11,
	})
	if err == nil || !strings.Contains(err.Error(), "prefetch 11") || !strings.Contains(err.Error(), "lane count 12") || !strings.Contains(err.Error(), "topics x priorities x (1 + retryTiers)") {
		t.Fatalf("Subscribe() error = %v, want lane-floor validation", err)
	}
}

func TestSubscribeRejectsPrefetchAboveCeilingFromExplicitConfig(t *testing.T) {
	t.Parallel()
	client := newPublishClient(t, &recordingProducer{})
	_, err := client.Subscribe(context.Background(), Subscription{
		Name:     "orders",
		Topics:   []string{"orders.created"},
		Prefetch: 65536,
	})
	want := "f1: subscriptions.orders.prefetch 65536 must be at most 65535"
	if err == nil || err.Error() != want {
		t.Fatalf("Subscribe() error = %v, want %q", err, want)
	}
}

func TestSubscribeRejectsPrefetchAboveCeilingFromEnvironment(t *testing.T) {
	t.Setenv("F1_SUBSCRIPTIONS_ORDERS_PREFETCH", "65536")
	client := newPublishClient(t, &recordingProducer{})
	_, err := client.Subscribe(context.Background(), Subscription{
		Name:   "orders",
		Topics: []string{"orders.created"},
	})
	want := "f1: subscriptions.orders.prefetch 65536 must be at most 65535"
	if err == nil || err.Error() != want {
		t.Fatalf("Subscribe() error = %v, want %q", err, want)
	}
}

func TestSubscribeRejectsUnknownSubscriptionKeyFromEnvironment(t *testing.T) {
	t.Setenv("F1_SUBSCRIPTIONS_ORDERS_HANDLER_TIMEOUT_MS", "5s")
	client := newPublishClient(t, &recordingProducer{})
	_, err := client.Subscribe(context.Background(), Subscription{
		Name:   "orders",
		Topics: []string{"orders.created"},
	})
	if err == nil || !strings.Contains(err.Error(), "F1_SUBSCRIPTIONS_ORDERS_HANDLER_TIMEOUT_MS") {
		t.Fatalf("Subscribe() error = %v, want the unknown variable named", err)
	}
}

func TestSubscribeMergesExplicitGoOverEnvironmentAndYAML(t *testing.T) {
	t.Setenv("F1_SUBSCRIPTIONS_ORDERS_CONCURRENCY", "6")
	cfg, err := LoadConfig(writeConfig(t, `
f1:
  env: test
  service: orders
  broker:
    driver: inmem
  subscriptions:
    orders:
      topics: [orders.created]
      concurrency: 8
      prefetch: 12
`))
	if err != nil {
		t.Fatal(err)
	}
	client, err := New(context.Background(), cfg, WithDriver(&testDriver{conn: &testConn{info: testBrokerInfo()}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	runner, err := client.Subscribe(context.Background(), Subscription{Name: "orders", Concurrency: 4})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := runner.config.Concurrency, 4; got != want {
		t.Fatalf("resolved concurrency = %d, want %d", got, want)
	}
	if got, want := runner.config.Prefetch, 12; got != want {
		t.Fatalf("resolved prefetch = %d, want %d", got, want)
	}
}

func TestApplySubscriptionEnvironmentStopsInCurrentOrderWithPartialMutation(t *testing.T) {
	name := t.Name()
	clearSubscriptionEnvironment(t, name)
	prefix := subscriptionEnvPrefix(name)
	t.Setenv(envKey(prefix, "topics"), "orders.created")
	t.Setenv(envKey(prefix, "mode"), "invalid-mode")
	t.Setenv(envKey(prefix, "concurrency"), "invalid-concurrency")

	cfg := SubscriptionConfig{Mode: OrderedByKey, Concurrency: 9}
	err := applySubscriptionEnvironment(name, &cfg)
	if err == nil || !strings.Contains(err.Error(), envKey(prefix, "mode")) {
		t.Fatalf("applySubscriptionEnvironment() error = %v, want mode error first", err)
	}
	if got, want := cfg.Topics, []string{"orders.created"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("topics after partial mutation = %v, want %v", got, want)
	}
	if cfg.Mode != OrderedByKey || cfg.Concurrency != 9 {
		t.Fatalf("config after partial mutation = %#v, want mode ordered and concurrency 9", cfg)
	}
}

func TestApplySubscriptionEnvironmentAllocatesFairnessMapsLazily(t *testing.T) {
	name := t.Name()
	clearSubscriptionEnvironment(t, name)
	prefix := subscriptionEnvPrefix(name)
	t.Setenv(envKey(prefix, "fairness.weights.high"), "7")

	cfg := SubscriptionConfig{}
	if err := applySubscriptionEnvironment(name, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Fairness.Weights == nil || cfg.Fairness.Weights[PriorityHigh] != 7 {
		t.Fatalf("weights = %#v, want only high weight 7", cfg.Fairness.Weights)
	}
	if cfg.Fairness.Budgets != nil {
		t.Fatalf("budgets = %#v, want nil when no budget env is present", cfg.Fairness.Budgets)
	}

	name = name + "_existing"
	clearSubscriptionEnvironment(t, name)
	prefix = subscriptionEnvPrefix(name)
	t.Setenv(envKey(prefix, "fairness.weights.high"), "11")
	cfg = SubscriptionConfig{Fairness: FairnessConfig{Weights: map[Priority]int{PriorityLow: 3}}}
	if err := applySubscriptionEnvironment(name, &cfg); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg.Fairness.Weights, map[Priority]int{PriorityLow: 3, PriorityHigh: 11}) {
		t.Fatalf("existing weights = %#v, want preserved low and updated high", cfg.Fairness.Weights)
	}
	if cfg.Fairness.Budgets != nil {
		t.Fatalf("existing-case budgets = %#v, want nil", cfg.Fairness.Budgets)
	}

	name = name + "_budget"
	clearSubscriptionEnvironment(t, name)
	prefix = subscriptionEnvPrefix(name)
	t.Setenv(envKey(prefix, "fairness.budgets.medium"), "2s")
	cfg = SubscriptionConfig{}
	if err := applySubscriptionEnvironment(name, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Fairness.Weights != nil {
		t.Fatalf("budget-only weights = %#v, want nil", cfg.Fairness.Weights)
	}
	if cfg.Fairness.Budgets == nil || cfg.Fairness.Budgets[PriorityMedium] != 2*time.Second {
		t.Fatalf("budget-only budgets = %#v, want medium 2s", cfg.Fairness.Budgets)
	}
}

func TestApplySubscriptionEnvironmentRejectsUnknownKey(t *testing.T) {
	name := t.Name()
	clearSubscriptionEnvironment(t, name)
	prefix := subscriptionEnvPrefix(name)
	t.Setenv(envKey(prefix, "concurrency"), "4")
	t.Setenv(prefix+"CONCURENCY", "8")

	cfg := SubscriptionConfig{}
	err := applySubscriptionEnvironment(name, &cfg)
	if err == nil || !strings.Contains(err.Error(), prefix+"CONCURENCY") {
		t.Fatalf("applySubscriptionEnvironment() error = %v, want the unknown variable named", err)
	}
	if got, want := cfg.Concurrency, 4; got != want {
		t.Fatalf("concurrency = %d, want %d from the variable the same call read", got, want)
	}
}

func TestApplySubscriptionEnvironmentNamesReplacementForKeyRenamedSinceConfig(t *testing.T) {
	name := t.Name()
	clearSubscriptionEnvironment(t, name)
	prefix := subscriptionEnvPrefix(name)
	t.Setenv(prefix+"FAIRNESS_AGING_ENABLED", "true")

	cfg := SubscriptionConfig{}
	err := applySubscriptionEnvironment(name, &cfg)
	if err == nil {
		t.Fatal("applySubscriptionEnvironment() error = nil, want the renamed key rejected")
	}
	if !strings.Contains(err.Error(), prefix+"FAIRNESS_AGING_ENABLED") || !strings.Contains(err.Error(), "FAIRNESS_DISABLE_DEADLINE_PROMOTION=false") {
		t.Fatalf("applySubscriptionEnvironment() error = %v, want the variable and the value that replaces it named", err)
	}
	if cfg.Fairness.DisableDeadlinePromotion {
		t.Fatal("DisableDeadlinePromotion = true, want the rejected variable to leave the configuration alone")
	}
}

func TestApplySubscriptionEnvironmentRejectsOldDeadlinePromotionKey(t *testing.T) {
	name := t.Name()
	clearSubscriptionEnvironment(t, name)
	prefix := subscriptionEnvPrefix(name)
	t.Setenv(prefix+"FAIRNESS_DISABLE_AGING", "true")

	err := applySubscriptionEnvironment(name, &SubscriptionConfig{})
	if err == nil {
		t.Fatal("applySubscriptionEnvironment() error = nil, want the old key rejected")
	}
	if !strings.Contains(err.Error(), prefix+"FAIRNESS_DISABLE_AGING") || !strings.Contains(err.Error(), "FAIRNESS_DISABLE_DEADLINE_PROMOTION") {
		t.Fatalf("applySubscriptionEnvironment() error = %v, want the old and replacement keys named", err)
	}
}

func TestApplySubscriptionEnvironmentAcceptsDeadlinePromotion(t *testing.T) {
	name := t.Name()
	clearSubscriptionEnvironment(t, name)
	prefix := subscriptionEnvPrefix(name)
	t.Setenv(prefix+"FAIRNESS_DISABLE_DEADLINE_PROMOTION", "true")

	cfg := SubscriptionConfig{}
	if err := applySubscriptionEnvironment(name, &cfg); err != nil {
		t.Fatal(err)
	}
	if !cfg.Fairness.DisableDeadlinePromotion {
		t.Fatal("DisableDeadlinePromotion = false, want true from the new environment key")
	}
}

func TestApplySubscriptionEnvironmentAcceptsVariableTwoSubscriptionNamesShare(t *testing.T) {
	clearSubscriptionEnvironment(t, "orders")
	clearSubscriptionEnvironment(t, "ordersRetry")
	t.Setenv("F1_SUBSCRIPTIONS_ORDERS_RETRY_MAX_ATTEMPTS", "3")

	orders := SubscriptionConfig{}
	if err := applySubscriptionEnvironment("orders", &orders); err != nil {
		t.Fatalf("applySubscriptionEnvironment(orders) error = %v, want the shared variable accepted", err)
	}
	if got, want := orders.Retry.MaxAttempts, 3; got != want {
		t.Fatalf("orders retry.maxAttempts = %d, want %d", got, want)
	}

	ordersRetry := SubscriptionConfig{}
	if err := applySubscriptionEnvironment("ordersRetry", &ordersRetry); err != nil {
		t.Fatalf("applySubscriptionEnvironment(ordersRetry) error = %v, want the other subscription's variable accepted", err)
	}
	if got := ordersRetry.Retry.MaxAttempts; got != 0 {
		t.Fatalf("ordersRetry retry.maxAttempts = %d, want the variable left to orders", got)
	}
}

func TestApplySubscriptionEnvironmentAcceptsPerPriorityKeys(t *testing.T) {
	name := t.Name()
	clearSubscriptionEnvironment(t, name)
	prefix := subscriptionEnvPrefix(name)
	for _, test := range []struct {
		key  string
		want int
	}{
		{"fairness.weights.high", 5},
		{"fairness.weights.medium", 6},
		{"fairness.weights.low", 7},
	} {
		t.Setenv(envKey(prefix, test.key), strconv.Itoa(test.want))
	}

	cfg := SubscriptionConfig{}
	if err := applySubscriptionEnvironment(name, &cfg); err != nil {
		t.Fatalf("applySubscriptionEnvironment() error = %v, want every per-priority key accepted", err)
	}
	want := map[Priority]int{PriorityHigh: 5, PriorityMedium: 6, PriorityLow: 7}
	if !reflect.DeepEqual(cfg.Fairness.Weights, want) {
		t.Fatalf("weights = %#v, want %#v", cfg.Fairness.Weights, want)
	}
}

func TestApplySubscriptionEnvironmentReportsTheFirstUnknownKeyInNameOrder(t *testing.T) {
	name := t.Name()
	clearSubscriptionEnvironment(t, name)
	prefix := subscriptionEnvPrefix(name)
	first, later := prefix+"AAA_CONCURENCY", prefix+"ZZZ_CONCURENCY"
	t.Setenv(later, "8")
	t.Setenv(first, "9")

	cfg := SubscriptionConfig{}
	err := applySubscriptionEnvironment(name, &cfg)
	if err == nil || !strings.Contains(err.Error(), first) {
		t.Fatalf("applySubscriptionEnvironment() error = %v, want %q reported", err, first)
	}
	if strings.Contains(err.Error(), later) {
		t.Fatalf("applySubscriptionEnvironment() error = %v, want only the name that sorts first", err)
	}
}

func TestApplySubscriptionEnvironmentRejectsDoubledUnderscoreBoundary(t *testing.T) {
	name := t.Name()
	clearSubscriptionEnvironment(t, name)
	prefix := subscriptionEnvPrefix(name)
	t.Setenv(prefix+"_CONCURRENCY", "8")

	cfg := SubscriptionConfig{}
	err := applySubscriptionEnvironment(name, &cfg)
	if err == nil || !strings.Contains(err.Error(), prefix+"_CONCURRENCY") {
		t.Fatalf("applySubscriptionEnvironment() error = %v, want the name no subscription can spell rejected", err)
	}
}

func clearSubscriptionEnvironment(t *testing.T, name string) {
	t.Helper()
	keys := []string{
		"topics", "mode", "concurrency", "prefetch", "priorities",
		"handlerTimeout", "unmatchedPolicy", "fairness.retryWeightDivisor",
		"fairness.prefetchFactor", "fairness.disableDeadlinePromotion",
		"fairness.weights.high", "fairness.weights.medium", "fairness.weights.low",
		"fairness.budgets.high", "fairness.budgets.medium", "fairness.budgets.low",
		"retry.maxAttempts", "retry.initialInterval", "retry.multiplier",
		"retry.maxInterval", "retry.tiers",
	}
	prefix := subscriptionEnvPrefix(name)
	for _, key := range keys {
		name := envKey(prefix, key)
		previous, existed := os.LookupEnv(name)
		_ = os.Unsetenv(name)
		t.Cleanup(func() {
			if existed {
				_ = os.Setenv(name, previous)
			} else {
				_ = os.Unsetenv(name)
			}
		})
	}
}

func TestSubscribeGoZeroCannotOverrideYAMLPolicy(t *testing.T) {
	t.Parallel()
	cfg, err := LoadConfig(writeConfig(t, `
f1:
  env: test
  service: orders
  broker:
    driver: inmem
  subscriptions:
    orders:
      topics: [orders.created]
      unmatchedPolicy: deadletter
`))
	if err != nil {
		t.Fatal(err)
	}
	client, err := New(context.Background(), cfg, WithDriver(&testDriver{conn: &testConn{info: testBrokerInfo()}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	runner, err := client.Subscribe(context.Background(), Subscription{Name: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := runner.config.UnmatchedPolicy, DeadLetter; got != want {
		t.Fatalf("resolved unmatched policy = %v, want %v", got, want)
	}
}

func TestSubscribePreservesExplicitYAMLDisableDeadlinePromotion(t *testing.T) {
	t.Parallel()
	cfg, err := LoadConfig(writeConfig(t, `
f1:
  env: test
  service: orders
  broker:
    driver: inmem
  subscriptions:
    orders:
      topics: [orders.created]
      fairness:
        disableDeadlinePromotion: true
`))
	if err != nil {
		t.Fatal(err)
	}
	client, err := New(context.Background(), cfg, WithDriver(&testDriver{conn: &testConn{info: testBrokerInfo()}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	runner, err := client.Subscribe(context.Background(), Subscription{Name: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if !runner.config.Fairness.DisableDeadlinePromotion {
		t.Fatal("explicit YAML disableDeadlinePromotion: true was replaced by the default")
	}
}

func TestSubscribeMergesGoBuiltRetryOverDefaults(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Env:     "test",
		Service: "orders",
		Broker:  BrokerConfig{Driver: "inmem"},
		Subscriptions: map[string]SubscriptionConfig{
			"orders": {Topics: []string{"orders.created"}, Retry: RetryConfig{MaxAttempts: 7}},
		},
	}
	client, err := New(context.Background(), cfg, WithDriver(&testDriver{conn: &testConn{info: testBrokerInfo()}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	runner, err := client.Subscribe(context.Background(), Subscription{Name: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	want := defaultSubscription().Retry
	want.MaxAttempts = 7
	if got := runner.config.Retry; !reflect.DeepEqual(got, want) {
		t.Fatalf("resolved retry = %+v, want %+v", got, want)
	}
}

func TestSubscribeMergesGoBuiltFairnessOverDefaults(t *testing.T) {
	t.Parallel()
	weights := map[Priority]int{PriorityHigh: 5, PriorityMedium: 2}
	cfg := Config{
		Env:     "test",
		Service: "orders",
		Broker:  BrokerConfig{Driver: "inmem"},
		Subscriptions: map[string]SubscriptionConfig{
			"orders": {Topics: []string{"orders.created"}, Fairness: FairnessConfig{Weights: weights}},
		},
	}
	client, err := New(context.Background(), cfg, WithDriver(&testDriver{conn: &testConn{info: testBrokerInfo()}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	runner, err := client.Subscribe(context.Background(), Subscription{Name: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	want := defaultSubscription().Fairness
	want.Weights = weights
	if got := runner.config.Fairness; !reflect.DeepEqual(got, want) {
		t.Fatalf("resolved fairness = %+v, want %+v", got, want)
	}
}

func TestUnmatchedDiscardReasonIsNotDeathReason(t *testing.T) {
	discarded := discardUnmatched(Envelope{}, nil)
	if discarded.Reason != DiscardUnmatched {
		t.Fatalf("discard reason = %q, want %q", discarded.Reason, DiscardUnmatched)
	}
	if DeathReason(discarded.Reason) == ReasonTerminal {
		t.Fatal("unmatched discard must not be stamped terminal")
	}
}

func TestTerminalNotificationsPreserveBoundedContextAndPanicRecovery(t *testing.T) {
	t.Parallel()
	observed := make(chan bool, 1)
	runner := &Runner{subscription: Subscription{
		OnDeadLetter: func(ctx context.Context, _ DeadLettered) {
			deadline, hasDeadline := ctx.Deadline()
			observed <- hasDeadline && time.Until(deadline) > 0 && time.Until(deadline) <= terminalNotificationTimeout
		},
	}}
	runnerNotifyDeadLetter(runner, context.Background(), DeadLettered{Reason: ReasonTerminal})
	timer := clock.NewReal().Timer(time.Second)
	defer timer.Stop()
	select {
	case got := <-observed:
		if !got {
			t.Fatal("terminal notification context was not bounded")
		}
	case <-timer.C:
		t.Fatal("terminal notification did not start")
	}

	panicStarted := make(chan struct{})
	runner.subscription.OnDeadLetter = func(context.Context, DeadLettered) {
		close(panicStarted)
		panic("notification panic")
	}
	runnerNotifyDeadLetter(runner, context.Background(), DeadLettered{Reason: ReasonTerminal})
	select {
	case <-panicStarted:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("panic notification did not start")
	}
}

func testBrokerInfo() driver.BrokerInfo {
	return driver.BrokerInfo{Kind: "test", Version: "1"}
}
