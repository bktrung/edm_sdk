package inmem

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/testhook"
)

var _ driver.BacklogReader = (*consumer)(nil)

func openEnqueueTest(t *testing.T, fake *clock.Fake, specs ...driver.DestinationSpec) (context.Context, driver.Conn, driver.Producer) {
	t.Helper()
	ctx := context.Background()
	conn, err := testhook.Driver(fake).Open(ctx, driver.Config{})
	require.NoError(t, err)
	_, err = conn.Admin().EnsureTopology(ctx, driver.TopologySpec{Destinations: specs})
	require.NoError(t, err)
	producer, err := conn.Producer(ctx, driver.ProducerConfig{})
	require.NoError(t, err)
	return ctx, conn, producer
}

func TestInboundMessage_EnqueuedAtUsesPublishTime(t *testing.T) {
	publishAt := time.Date(2026, time.January, 2, 3, 4, 5, 6, time.UTC)
	fake := clock.NewFake(publishAt)
	ctx, conn, producer := openEnqueueTest(t, fake, driver.DestinationSpec{Name: "orders"})
	consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"orders"}, Effective: testCaps()})
	require.NoError(t, err)

	require.NoError(t, consumer.Pause("orders"))
	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "orders", Body: []byte("one")}))
	receiveAt := publishAt.Add(time.Minute)
	fake.Advance(time.Minute)
	require.NoError(t, consumer.Resume("orders"))

	message := receiveTest(t, consumer)
	require.Equal(t, publishAt, message.EnqueuedAt)
	require.Equal(t, driver.EnqueueSourceBroker, message.EnqueuedAtSource)
	require.Equal(t, receiveAt, message.ReceivedAt)
	require.NoError(t, message.Settle.Ack(ctx))
	closeTest(t, ctx, conn, producer, consumer)
}

func TestInboundMessage_DeferredKeepsPublishTime(t *testing.T) {
	publishAt := time.Date(2026, time.February, 3, 4, 5, 6, 7, time.UTC)
	fake := clock.NewFake(publishAt)
	ctx, conn, producer := openEnqueueTest(t, fake, driver.DestinationSpec{Name: "orders"})
	consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"orders"}, Effective: testCaps()})
	require.NoError(t, err)

	dueAt := publishAt.Add(time.Hour)
	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{
		Destination: "orders",
		DelayUntil:  dueAt,
		Body:        []byte("later"),
	}))
	fake.BlockUntil(1)
	fake.Advance(time.Hour)

	message := receiveTest(t, consumer)
	require.Equal(t, publishAt, message.EnqueuedAt)
	require.Equal(t, driver.EnqueueSourceBroker, message.EnqueuedAtSource)
	require.Equal(t, dueAt, message.ReceivedAt)
	require.NoError(t, message.Settle.Ack(ctx))
	closeTest(t, ctx, conn, producer, consumer)
}

func TestBacklogReportsLagAndOldestEnqueue(t *testing.T) {
	firstAt := time.Date(2026, time.March, 4, 5, 6, 7, 8, time.UTC)
	fake := clock.NewFake(firstAt)
	ctx, conn, producer := openEnqueueTest(t, fake,
		driver.DestinationSpec{Name: "orders"},
		driver.DestinationSpec{Name: "empty"},
	)
	consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{
		Destinations: []string{"orders", "empty"},
		Effective:    testCaps(),
	})
	require.NoError(t, err)

	require.NoError(t, consumer.Pause())
	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "orders", Body: []byte("first")}))
	fake.Advance(time.Minute)
	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "orders", Body: []byte("second")}))

	lag, err := consumer.Lag(ctx)
	require.NoError(t, err)
	reader := consumer.(driver.BacklogReader)
	backlog, err := reader.Backlog(ctx)
	require.NoError(t, err)
	require.Equal(t, lag["orders"], backlog["orders"].Lag)
	require.Equal(t, int64(2), backlog["orders"].Lag)
	require.Equal(t, firstAt, backlog["orders"].HeadEnqueuedAt)
	require.Equal(t, driver.EnqueueSourceBroker, backlog["orders"].HeadSource)
	require.Equal(t, int64(0), backlog["empty"].Lag)
	require.True(t, backlog["empty"].HeadEnqueuedAt.IsZero())
	require.Equal(t, driver.EnqueueSourceUnknown, backlog["empty"].HeadSource)
	closeTest(t, ctx, conn, producer, consumer)
}
