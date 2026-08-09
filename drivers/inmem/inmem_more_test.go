package inmem

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

type countingClock struct {
	clock.Clock
	timers atomic.Int64
}

func (c *countingClock) Timer(d time.Duration) clock.Timer {
	c.timers.Add(1)
	return c.Clock.Timer(d)
}

func testCaps() driver.Capabilities { return (Driver{}).Capabilities() }

func openTest(t *testing.T, clk clock.Clock, specs ...driver.DestinationSpec) (context.Context, driver.Conn, driver.Producer) {
	t.Helper()
	ctx := context.Background()
	conn, err := Driver{Clock: clk}.Open(ctx, driver.Config{})
	require.NoError(t, err)
	_, err = conn.Admin().EnsureTopology(ctx, driver.TopologySpec{Destinations: specs})
	require.NoError(t, err)
	producer, err := conn.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true})
	require.NoError(t, err)
	return ctx, conn, producer
}

func receiveTest(t *testing.T, consumer driver.Consumer) driver.InboundMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case message := <-consumer.Messages():
		return message
	case <-ctx.Done():
		t.Fatal("timed out waiting for in-memory delivery")
		return driver.InboundMessage{}
	}
}

func closeTest(t *testing.T, ctx context.Context, conn driver.Conn, producer driver.Producer, consumers ...driver.Consumer) {
	t.Helper()
	for _, consumer := range consumers {
		require.NoError(t, consumer.Stop(ctx))
	}
	require.NoError(t, producer.Close(ctx))
	require.NoError(t, conn.Close(ctx))
}

func TestDelayedDelivery_WaitsWithoutBurningCPU(t *testing.T) {
	clk := &countingClock{Clock: clock.NewReal()}
	ctx, conn, producer := openTest(t, clk, driver.DestinationSpec{Name: "delayed", Delay: 50 * time.Millisecond})
	consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"delayed"}, Effective: testCaps()})
	require.NoError(t, err)

	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "delayed", Body: []byte("later")}))
	message := receiveTest(t, consumer)
	require.Equal(t, []byte("later"), message.Body)
	require.LessOrEqual(t, clk.timers.Load(), int64(4))
	require.NoError(t, message.Settle.Ack(ctx))
	closeTest(t, ctx, conn, producer, consumer)
}

func TestDispatch_BackedUpDestinationDoesNotStarveAnother(t *testing.T) {
	ctx, conn, producer := openTest(t, clock.NewFake(time.Unix(0, 0)),
		driver.DestinationSpec{Name: "a"}, driver.DestinationSpec{Name: "b"})
	consumerA, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"a"}, Prefetch: 1, Effective: testCaps()})
	require.NoError(t, err)
	consumerB, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"b"}, Prefetch: 1, Effective: testCaps()})
	require.NoError(t, err)

	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "a", Body: []byte("holds the slot")}))
	first := receiveTest(t, consumerA)
	require.NoError(t, producer.Publish(ctx,
		driver.OutboundMessage{Destination: "a", Body: []byte("blocked")},
		driver.OutboundMessage{Destination: "b", Body: []byte("independent")}))
	second := receiveTest(t, consumerB)
	require.Equal(t, []byte("independent"), second.Body)
	require.NoError(t, first.Settle.Ack(ctx))
	blocked := receiveTest(t, consumerA)
	require.Equal(t, []byte("blocked"), blocked.Body)
	require.NoError(t, blocked.Settle.Ack(ctx))
	require.NoError(t, second.Settle.Ack(ctx))
	closeTest(t, ctx, conn, producer, consumerA, consumerB)
}

func TestNackRequeueIncrementsDeliveryCount(t *testing.T) {
	ctx, conn, producer := openTest(t, clock.NewFake(time.Unix(0, 0)), driver.DestinationSpec{Name: "orders"})
	consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"orders"}, Effective: testCaps()})
	require.NoError(t, err)
	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "orders", Body: []byte("retry")}))

	first := receiveTest(t, consumer)
	require.Equal(t, 0, first.DeliveryCount)
	require.NoError(t, first.Settle.Nack(ctx, driver.NackOptions{Requeue: true, CountAsFailure: true}))
	second := receiveTest(t, consumer)
	require.Equal(t, 1, second.DeliveryCount)
	require.NoError(t, second.Settle.Ack(ctx))
	closeTest(t, ctx, conn, producer, consumer)
}

func TestLagPauseAndResume(t *testing.T) {
	ctx, conn, producer := openTest(t, clock.NewFake(time.Unix(0, 0)), driver.DestinationSpec{Name: "orders"})
	consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"orders"}, Effective: testCaps()})
	require.NoError(t, err)
	require.NoError(t, consumer.Pause("orders"))
	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "orders", Body: []byte("paused")}))

	lag, err := consumer.Lag(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), lag["orders"])
	select {
	case <-consumer.Messages():
		t.Fatal("paused consumer received a message")
	default:
	}
	require.NoError(t, consumer.Resume("orders"))
	message := receiveTest(t, consumer)
	require.NoError(t, message.Settle.Ack(ctx))
	closeTest(t, ctx, conn, producer, consumer)
}

func TestDrainStopsNewDeliveries(t *testing.T) {
	ctx, conn, producer := openTest(t, clock.NewFake(time.Unix(0, 0)), driver.DestinationSpec{Name: "orders"})
	consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"orders"}, Effective: testCaps()})
	require.NoError(t, err)
	require.NoError(t, consumer.Drain(ctx))
	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "orders", Body: []byte("held")}))
	select {
	case <-consumer.Messages():
		t.Fatal("draining consumer received a new message")
	default:
	}
	closeTest(t, ctx, conn, producer, consumer)
}

func TestStopRejectsOutstandingUntilSettled(t *testing.T) {
	ctx, conn, producer := openTest(t, clock.NewFake(time.Unix(0, 0)), driver.DestinationSpec{Name: "orders"})
	consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"orders"}, Effective: testCaps()})
	require.NoError(t, err)
	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "orders", Body: []byte("outstanding")}))
	message := receiveTest(t, consumer)
	err = consumer.Stop(ctx)
	kind, classified := driver.Classify(err)
	require.True(t, classified)
	require.Equal(t, driver.KindFatal, kind)
	require.ErrorContains(t, err, "outstanding messages")
	require.NoError(t, message.Settle.Ack(ctx))
	closeTest(t, ctx, conn, producer, consumer)
}

func TestPruneGuards(t *testing.T) {
	ctx, conn, producer := openTest(t, clock.NewFake(time.Unix(0, 0)),
		driver.DestinationSpec{Name: "holds"}, driver.DestinationSpec{Name: "attached"}, driver.DestinationSpec{Name: "empty"})
	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "holds", Body: []byte("retained")}))
	results, err := conn.Admin().Prune(ctx, []string{"holds"})
	require.NoError(t, err)
	require.Equal(t, "holds 1 message", results[0].Reason)

	consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"attached"}, Effective: testCaps()})
	require.NoError(t, err)
	results, err = conn.Admin().Prune(ctx, []string{"attached"})
	require.NoError(t, err)
	require.Equal(t, "consumer attached", results[0].Reason)
	require.NoError(t, consumer.Stop(ctx))

	// Inmem has no auxiliary lane, so an empty destination can be deleted after
	// the other guards pass.
	results, err = conn.Admin().Prune(ctx, []string{"empty"})
	require.NoError(t, err)
	require.True(t, results[0].Deleted)
	_, err = conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"empty"}, Effective: testCaps()})
	require.ErrorIs(t, err, driver.ErrDestinationMissing)
	closeTest(t, ctx, conn, producer)
}

func TestOrphanScanWithAndWithoutScope(t *testing.T) {
	ctx, conn, producer := openTest(t, clock.NewFake(time.Unix(0, 0)),
		driver.DestinationSpec{Name: "owned.old"}, driver.DestinationSpec{Name: "outside"})
	diff, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{Scope: []string{"owned."}})
	require.NoError(t, err)
	require.Len(t, diff.Orphaned, 1)
	require.Equal(t, "owned.old", diff.Orphaned[0].Name)
	require.Empty(t, diff.OrphanScanError)

	diff, err = conn.Admin().EnsureTopology(ctx, driver.TopologySpec{})
	require.NoError(t, err)
	require.Empty(t, diff.Orphaned)
	require.Equal(t, "TopologySpec.Scope is empty: not scanning for orphans", diff.OrphanScanError)
	closeTest(t, ctx, conn, producer)
}

func TestKeyAffinityAcrossTwoConsumers(t *testing.T) {
	ctx, conn, producer := openTest(t, clock.NewFake(time.Unix(0, 0)), driver.DestinationSpec{Name: "orders"})
	consumerA, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"orders"}, Prefetch: 2, Effective: testCaps()})
	require.NoError(t, err)
	consumerB, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"orders"}, Prefetch: 2, Effective: testCaps()})
	require.NoError(t, err)
	require.NoError(t, producer.Publish(ctx,
		driver.OutboundMessage{Destination: "orders", Key: []byte("same"), Body: []byte("one")},
		driver.OutboundMessage{Destination: "orders", Key: []byte("same"), Body: []byte("two")}))

	var first, second driver.InboundMessage
	var owner driver.Consumer
	waitCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	select {
	case first = <-consumerA.Messages():
		owner = consumerA
	case first = <-consumerB.Messages():
		owner = consumerB
	case <-waitCtx.Done():
		t.Fatal("timed out waiting for key-affinity delivery")
	}
	second = receiveTest(t, owner)
	require.Equal(t, []byte("same"), first.Key)
	require.Equal(t, []byte("same"), second.Key)
	require.NoError(t, first.Settle.Ack(ctx))
	require.NoError(t, second.Settle.Ack(ctx))
	closeTest(t, ctx, conn, producer, consumerA, consumerB)
}

func TestExclusiveConsumerRejectsSecondAttachment(t *testing.T) {
	ctx, conn, producer := openTest(t, clock.NewFake(time.Unix(0, 0)), driver.DestinationSpec{Name: "orders"})
	consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"orders"}, Exclusive: true, Effective: testCaps()})
	require.NoError(t, err)
	_, err = conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"orders"}, Exclusive: true, Effective: testCaps()})
	require.Error(t, err)
	kind, classified := driver.Classify(err)
	require.True(t, classified)
	require.Equal(t, driver.KindFatal, kind)
	require.NoError(t, consumer.Stop(ctx))

	consumer, err = conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"orders"}, Exclusive: true, Effective: testCaps()})
	require.NoError(t, err)
	_, err = conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"orders"}, Effective: testCaps()})
	require.Error(t, err)
	kind, classified = driver.Classify(err)
	require.True(t, classified)
	require.Equal(t, driver.KindFatal, kind)
	closeTest(t, ctx, conn, producer, consumer)
}
