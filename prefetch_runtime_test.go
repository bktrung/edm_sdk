package f1_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	//nolint:depguard // these tests exercise public admission behavior with the reference driver.
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestAutomaticPrefetchUsesConfiguredWorkers(t *testing.T) {
	for _, workers := range []int{16, 128, 256} {
		t.Run(strconv.Itoa(workers), func(t *testing.T) {
			cfg := schedulingConfig()
			cfg.Broker.DefaultPrefetch = 0
			exercisePrefetchAdmission(t, cfg, f1.Subscription{Concurrency: workers}, workers)
		})
	}
}

func TestPrefetchOverridePrecedenceControlsAdmission(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		broker, loaded, explicit int
		environment              string
		want                     int
	}{
		{name: "automatic-yaml-zero", want: 16},
		{name: "subscription-budget", loaded: 2, want: 2},
		{name: "broker-fallback", broker: 3, want: 3},
		{name: "environment-overrides-yaml", loaded: 2, environment: "4", want: 4},
		{name: "environment-zero-restores-automatic", loaded: 2, environment: "0", want: 16},
		{name: "environment-zero-takes-broker-fallback", broker: 3, loaded: 2, environment: "0", want: 3},
		{name: "go-overrides-environment", loaded: 2, environment: "4", explicit: 5, want: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			content := fmt.Sprintf("f1:\n  env: test\n  service: prefetch\n  broker:\n    driver: inmem\n    defaultPrefetch: %d\n  topology:\n    autoCreate: true\n  subscriptions:\n    prefetch-test:\n      topics: [prefetch.a, prefetch.b]\n      concurrency: 16\n      prefetch: %d\n      priorities: [medium]\n      handlerTimeout: 1s\n", tc.broker, tc.loaded)
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := f1.LoadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			if tc.environment != "" {
				t.Setenv("F1_SUBSCRIPTIONS_PREFETCH_TEST_PREFETCH", tc.environment)
			}
			exercisePrefetchAdmission(t, cfg, f1.Subscription{Concurrency: 16, Prefetch: tc.explicit}, tc.want)
		})
	}
}

func exercisePrefetchAdmission(t *testing.T, cfg f1.Config, sub f1.Subscription, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cfg.Topology.Priorities = []f1.Priority{f1.PriorityHigh, f1.PriorityMedium, f1.PriorityLow}
	client, err := f1.New(ctx, cfg, f1.WithDriver(inmem.New()), f1.WithPublishTopics("prefetch.a", "prefetch.b"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	permits := make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(permits) })
	started := make(chan int, sub.Concurrency*2)
	var active atomic.Int64
	handler := f1.HandlerFunc(func(ctx context.Context, event *f1.Event) error {
		var payload map[string]int
		if err := event.Decode(&payload); err != nil {
			return f1.Terminal(err)
		}
		active.Add(1)
		defer active.Add(-1)
		started <- payload["sequence"]
		select {
		case <-permits:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	sub.Name = "prefetch-test"
	sub.Topics = []string{"prefetch.a", "prefetch.b"}
	sub.Priorities = []f1.Priority{f1.PriorityMedium}
	sub.HandlerTimeout = time.Second
	sub.Handlers = map[string]f1.Handler{"prefetch.a": handler, "prefetch.b": handler}
	runner, err := client.Subscribe(ctx, sub)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	messages := make([]f1.Message, sub.Concurrency*2)
	for i := range messages {
		topic := "prefetch.a"
		if i%2 != 0 {
			topic = "prefetch.b"
		}
		messages[i] = f1.Message{EventType: topic, Payload: map[string]int{"sequence": i}, Opts: []f1.PublishOption{f1.WithTopic(topic), f1.WithPriority(f1.PriorityMedium)}}
	}
	publishMessages(t, client, ctx, messages)
	seen := make(map[int]int, len(messages))
	for range want {
		select {
		case sequence := <-started:
			seen[sequence]++
		case <-ctx.Done():
			t.Fatalf("only part of %d permitted handlers started: %v", want, ctx.Err())
		}
	}
	if got := active.Load(); got != int64(want) {
		t.Fatalf("simultaneous handlers = %d, want %d", got, want)
	}
	timer := clock.NewReal().Timer(20 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-started:
		t.Fatalf("handler admission exceeded %d", want)
	case <-timer.C:
	}
	// Releasing one handler must allow the next delivery through its settlement
	// path; this catches admission limits that never return capacity.
	permits <- struct{}{}
	select {
	case sequence := <-started:
		seen[sequence]++
	case <-ctx.Done():
		t.Fatalf("settlement did not refill admission: %v", ctx.Err())
	}
	release.Do(func() { close(permits) })
	for range len(messages) - want - 1 {
		select {
		case sequence := <-started:
			seen[sequence]++
		case <-ctx.Done():
			t.Fatalf("remaining backlog did not finish: %v", ctx.Err())
		}
	}
	if err := runner.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("runner did not stop: %v", ctx.Err())
	}
	for sequence := range messages {
		if seen[sequence] != 1 {
			t.Fatalf("message %d handled %d times, want once", sequence, seen[sequence])
		}
	}
}
