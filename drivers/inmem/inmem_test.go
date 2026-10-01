package inmem

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestRoundTrip_PublishThenConsume(t *testing.T) {
	ctx := context.Background()
	conn, err := (Driver{}).Open(ctx, driver.Config{})
	require.NoError(t, err)
	_, err = conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: "orders"}},
	})
	require.NoError(t, err)
	producer, err := conn.Producer(ctx, driver.ProducerConfig{})
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

func BenchmarkDispatchBacklog(b *testing.B) {
	ctx := context.Background()
	opened, err := (Driver{}).Open(ctx, driver.Config{})
	if err != nil {
		b.Fatal(err)
	}
	impl := opened.(*conn)
	_, err = opened.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: "orders"}},
	})
	if err != nil {
		b.Fatal(err)
	}
	producer, err := opened.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		b.Fatal(err)
	}
	consumer, err := opened.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"orders"}, Prefetch: 1})
	if err != nil {
		b.Fatal(err)
	}
	if err := producer.Publish(ctx, driver.OutboundMessage{Destination: "orders", Body: []byte("held")}); err != nil {
		b.Fatal(err)
	}
	<-consumer.Messages()
	backlog := make([]driver.OutboundMessage, 1024)
	for i := range backlog {
		backlog[i] = driver.OutboundMessage{
			Destination: "orders",
			Key:         []byte("key"),
			Body:        []byte("body"),
		}
	}
	if err := producer.Publish(ctx, backlog...); err != nil {
		b.Fatal(err)
	}

	impl.mu.Lock()
	b.ResetTimer()
	for range b.N {
		impl.dispatchLocked()
	}
	b.StopTimer()
	impl.mu.Unlock()
	if err := consumer.Release(ctx); err != nil {
		b.Fatal(err)
	}
	if err := producer.Close(ctx); err != nil {
		b.Fatal(err)
	}
	if err := opened.Close(ctx); err != nil {
		b.Fatal(err)
	}
}

func TestDispatchDoesNotRescanBlockedBacklog(t *testing.T) {
	ctx, opened, producer := openTest(t, nil, driver.DestinationSpec{Name: "orders"})
	impl := opened.(*conn)
	consumer, err := impl.Consumer(ctx, driver.ConsumerConfig{
		Destinations: []string{"orders"},
		Prefetch:     1,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = consumer.Release(ctx)
		_ = producer.Close(ctx)
		_ = opened.Close(ctx)
	})

	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{
		Destination: "orders",
		Body:        []byte("held"),
	}))
	<-consumer.Messages()
	backlog := make([]driver.OutboundMessage, 256)
	for i := range backlog {
		backlog[i] = driver.OutboundMessage{
			Destination: "orders",
			Key:         []byte("key"),
			Body:        []byte("body"),
		}
	}
	require.NoError(t, producer.Publish(ctx, backlog...))

	impl.mu.Lock()
	dest := impl.destinations["orders"]
	dest.dispatchCursor = 0
	before := dest.dispatchScanned
	for range 4 {
		impl.dispatchLocked()
	}
	after := impl.destinations["orders"].dispatchScanned
	impl.mu.Unlock()
	require.Equal(t, before, after)
}

func BenchmarkDispatchDueBacklog(b *testing.B) {
	b.Run("cursor", func(b *testing.B) {
		benchmarkDispatchDueBacklog(b, false)
	})
	b.Run("no-cursor", func(b *testing.B) {
		benchmarkDispatchDueBacklog(b, true)
	})
}

func benchmarkDispatchDueBacklog(b *testing.B, resetCursor bool) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	ctx := context.Background()
	opened, err := (Driver{clock: fake}).Open(ctx, driver.Config{})
	if err != nil {
		b.Fatal(err)
	}
	impl := opened.(*conn)
	_, err = opened.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: "orders", Delay: time.Hour}},
	})
	if err != nil {
		b.Fatal(err)
	}
	producer, err := opened.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		b.Fatal(err)
	}
	consumer, err := opened.Consumer(ctx, driver.ConsumerConfig{
		Destinations: []string{"orders"},
		Prefetch:     1,
	})
	if err != nil {
		b.Fatal(err)
	}
	backlog := make([]driver.OutboundMessage, 4096)
	for i := range backlog {
		backlog[i] = driver.OutboundMessage{Destination: "orders", Body: []byte("body")}
	}
	if err := producer.Publish(ctx, backlog...); err != nil {
		b.Fatal(err)
	}

	impl.mu.Lock()
	dest := impl.destinations["orders"]
	b.ResetTimer()
	for range b.N {
		if resetCursor {
			dest.dispatchCursor = 0
		}
		impl.dispatchLocked()
	}
	b.StopTimer()
	impl.mu.Unlock()
	if err := consumer.Release(ctx); err != nil {
		b.Fatal(err)
	}
	if err := producer.Close(ctx); err != nil {
		b.Fatal(err)
	}
	if err := opened.Close(ctx); err != nil {
		b.Fatal(err)
	}
}

func TestDispatchCursorSkipsDueBacklog(t *testing.T) {
	start := time.Unix(0, 0)
	fake := clock.NewFake(start)
	ctx, opened, producer := openTest(t, fake, driver.DestinationSpec{Name: "orders", Delay: time.Hour})
	impl := opened.(*conn)
	consumer, err := impl.Consumer(ctx, driver.ConsumerConfig{
		Destinations: []string{"orders"},
		Prefetch:     1,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = consumer.Release(ctx)
		_ = producer.Close(ctx)
		_ = opened.Close(ctx)
	})

	backlog := make([]driver.OutboundMessage, 256)
	for i := range backlog {
		backlog[i] = driver.OutboundMessage{Destination: "orders", Body: []byte("body")}
	}
	require.NoError(t, producer.Publish(ctx, backlog...))

	impl.mu.Lock()
	dest := impl.destinations["orders"]
	require.True(t, dest.hasEligibleConsumer())
	require.Equal(t, uint64(len(backlog)), dest.dispatchScanned)
	before := dest.dispatchScanned
	impl.dispatchLocked()
	after := dest.dispatchScanned
	impl.mu.Unlock()
	require.Equal(t, before, after)
}
