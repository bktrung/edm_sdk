package inmem

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestRoundTrip_PublishThenConsume(t *testing.T) {
	ctx := context.Background()
	conn, err := (Driver{}).Open(ctx, driver.Config{})
	require.NoError(t, err)
	_, err = conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: "orders"}},
	})
	require.NoError(t, err)
	producer, err := conn.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true})
	require.NoError(t, err)
	consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"orders"}, Prefetch: 1})
	require.NoError(t, err)

	want := driver.OutboundMessage{Destination: "orders", Key: []byte("order-1"), Body: []byte(`{"id":1}`)}
	require.NoError(t, producer.Publish(ctx, want))
	got := <-consumer.Messages()
	require.Equal(t, want.Body, got.Body)
	require.Equal(t, want.Key, got.Key)
	require.NoError(t, got.Settle.Ack(ctx))
	err = got.Settle.Ack(ctx)
	require.ErrorIs(t, err, driver.ErrAlreadySettled)
	_, classified := driver.Classify(err)
	require.True(t, classified)
	require.NoError(t, consumer.Stop(ctx))
	require.NoError(t, producer.Close(ctx))
	require.NoError(t, conn.Close(ctx))
}
