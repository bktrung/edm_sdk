package inmem

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// TestHistoryEvictsOldestOnceCapped proves a destination's replay history
// stays bounded at maxDestinationHistory while a consumer keeps it from
// being cleared, evicting the oldest entries first once the cap is
// exceeded.
func TestHistoryEvictsOldestOnceCapped(t *testing.T) {
	ctx := context.Background()
	opened, err := (Driver{}).Open(ctx, driver.Config{})
	require.NoError(t, err)
	impl := opened.(*conn)
	t.Cleanup(func() { _ = opened.Close(ctx) })

	_, err = opened.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: "orders"}},
	})
	require.NoError(t, err)
	producer, err := opened.Producer(ctx, driver.ProducerConfig{})
	require.NoError(t, err)
	consumer, err := opened.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"orders"}, Prefetch: 1})
	require.NoError(t, err)
	// Paused so nothing is ever dispatched: history growth must not depend on
	// whether messages are being drained.
	require.NoError(t, consumer.Pause("orders"))

	const overflow = 5
	total := maxDestinationHistory + overflow
	for i := 0; i < total; i++ {
		require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "orders", Body: []byte("x")}))
	}

	impl.mu.Lock()
	entries := append([]*queuedMessage(nil), impl.history["orders"]...)
	impl.mu.Unlock()
	require.Len(t, entries, maxDestinationHistory, "history must stay capped at maxDestinationHistory")
	require.Equal(t, uint64(overflow+1), entries[0].sequence, "the oldest entries must be evicted first")
	require.Equal(t, uint64(total), entries[len(entries)-1].sequence, "the newest entry must be retained")
}

// TestHistoryClearedOnLastConsumerDetach protects the pre-existing
// invariant that a destination's replay history is discarded once its last
// consumer detaches: a fresh consumer group attaching later must not see
// stale entries left over from before every consumer left.
func TestHistoryClearedOnLastConsumerDetach(t *testing.T) {
	ctx := context.Background()
	opened, err := (Driver{}).Open(ctx, driver.Config{})
	require.NoError(t, err)
	impl := opened.(*conn)
	t.Cleanup(func() { _ = opened.Close(ctx) })

	_, err = opened.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: "orders"}},
	})
	require.NoError(t, err)
	producer, err := opened.Producer(ctx, driver.ProducerConfig{})
	require.NoError(t, err)
	consumer, err := opened.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"orders"}, Prefetch: 1})
	require.NoError(t, err)
	require.NoError(t, consumer.Pause("orders"))

	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "orders", Body: []byte("x")}))

	impl.mu.Lock()
	before := len(impl.history["orders"])
	impl.mu.Unlock()
	require.Equal(t, 1, before)

	require.NoError(t, consumer.Stop(ctx))

	impl.mu.Lock()
	after := impl.history["orders"]
	impl.mu.Unlock()
	require.Nil(t, after, "history must be cleared once the last consumer detaches")
}
