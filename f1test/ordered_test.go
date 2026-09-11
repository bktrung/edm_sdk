package f1test

import (
	"context"
	"hash/fnv"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
)

func TestOrderedByKeyKeepsEqualKeysSerialAndDifferentKeysConcurrent(t *testing.T) {
	c := NewClient(t, quietLogger())
	ctx, cancel := context.WithCancel(context.Background())
	keys := distinctOrderedKeys()
	const messagesPerKey = 4
	totalMessages := len(keys) * messagesPerKey

	var mu sync.Mutex
	activeByKey := make(map[string]int, len(keys))
	activeTotal := 0
	var overlap []string
	started := make(chan string, totalMessages)
	handled := make(chan string, totalMessages)
	release := make(chan struct{})
	var releaseOnce sync.Once

	runner, err := c.Subscribe(ctx, f1.Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Mode:           f1.OrderedByKey,
		Concurrency:    2,
		Prefetch:       totalMessages,
		Priorities:     []f1.Priority{f1.PriorityMedium},
		Retry:          f1.RetryConfig{MaxAttempts: 1},
		HandlerTimeout: time.Second,
		Handlers: map[string]f1.Handler{
			"orders.created.v1": f1.HandlerFunc(func(_ context.Context, event *f1.Event) error {
				var payload struct {
					Key string
				}
				if err := event.Decode(&payload); err != nil {
					return err
				}
				mu.Lock()
				activeByKey[payload.Key]++
				activeTotal++
				if activeByKey[payload.Key] > 1 {
					overlap = append(overlap, payload.Key)
				}
				mu.Unlock()
				started <- payload.Key
				<-release
				mu.Lock()
				activeByKey[payload.Key]--
				activeTotal--
				mu.Unlock()
				handled <- payload.Key
				return nil
			}),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, runner)
	limits := c.Limits()
	var orderedStatus f1.FeatureStatus
	foundStatus := false
	for _, status := range limits.Features {
		if status.Feature == "ordered_by_key" {
			orderedStatus = status
			foundStatus = true
			break
		}
	}
	require.True(t, foundStatus)
	require.Equal(t, f1.FeatureNative, orderedStatus.Mode)

	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()
	defer func() {
		releaseOnce.Do(func() { close(release) })
		cancel()
		require.NoError(t, <-runDone)
	}()

	messages := make([]f1.Message, 0, totalMessages)
	for _, key := range keys {
		for range messagesPerKey {
			messages = append(messages, f1.Message{
				EventType: "orders.created.v1",
				Payload:   struct{ Key string }{Key: key},
				Opts:      []f1.PublishOption{f1.WithKey(key)},
			})
		}
	}
	result, err := c.Publisher().PublishBatch(ctx, messages)
	require.NoError(t, err)
	require.Len(t, result.Results, totalMessages)
	for _, messageResult := range result.Results {
		require.NoError(t, messageResult.Err)
	}

	seen := make(map[string]bool, len(keys))
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	for len(seen) < len(keys) {
		select {
		case key := <-started:
			seen[key] = true
		case <-waitCtx.Done():
			t.Fatalf("saturation did not start both key lanes: seen %v", seen)
		}
	}
	mu.Lock()
	activeAtSaturation := activeTotal
	mu.Unlock()
	require.Equal(t, len(keys), activeAtSaturation)

	releaseOnce.Do(func() { close(release) })
	for i := range totalMessages {
		select {
		case <-handled:
		case <-waitCtx.Done():
			t.Fatalf("handler completions = %d, want %d", i, totalMessages)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	require.Empty(t, overlap, "equal keys overlapped in the handler")
	require.Zero(t, activeTotal)
}

func distinctOrderedKeys() [2]string {
	first := "key-0"
	firstHash := fnv.New32a()
	_, _ = firstHash.Write([]byte(first))
	for i := 1; i < 256; i++ {
		candidate := "key-" + strconv.Itoa(i)
		hash := fnv.New32a()
		_, _ = hash.Write([]byte(candidate))
		if firstHash.Sum32()%2 != hash.Sum32()%2 {
			return [2]string{first, candidate}
		}
	}
	panic("could not find distinct ordered keys")
}
