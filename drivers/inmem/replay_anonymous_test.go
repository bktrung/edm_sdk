package inmem

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// TestReplay_NewGroupDoesNotRedeliverToAnonymousConsumer proves a replay made
// for a newly created named group reaches only that group. The ungrouped
// consumer already received the original message, so a replayed copy must not
// be offered to it again, while the new group must still receive m1.
func TestReplay_NewGroupDoesNotRedeliverToAnonymousConsumer(t *testing.T) {
	ctx, opened, producer := openTest(t, clock.NewFake(time.Unix(0, 0)), driver.DestinationSpec{Name: "orders"})
	impl := opened.(*conn)
	anonymous, err := impl.Consumer(ctx, driver.ConsumerConfig{
		Destinations: []string{"orders"}, Prefetch: 1, StartAt: driver.StartEarliest,
	})
	require.NoError(t, err)
	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "orders", Body: []byte("m1")}))
	first := receiveNow(t, impl, anonymous)
	require.Equal(t, "m1", string(first.Body))
	require.NoError(t, first.Settle.Ack(ctx))

	named, err := impl.Consumer(ctx, driver.ConsumerConfig{
		Group: "g", Destinations: []string{"orders"}, Prefetch: 1, StartAt: driver.StartEarliest,
	})
	require.NoError(t, err)
	requireNoDelivery(t, impl, anonymous)
	replayed := receiveNow(t, impl, named)
	require.Equal(t, "m1", string(replayed.Body), "the new group must receive the replayed message")
	require.NoError(t, replayed.Settle.Ack(ctx))
	impl.mu.Lock()
	queued := len(impl.destinations["orders"].messages)
	impl.mu.Unlock()
	require.Zero(t, queued, "the replayed copy must retire once the new group takes it")
	closeTest(t, ctx, impl, producer, anonymous, named)
}
