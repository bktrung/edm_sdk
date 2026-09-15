package inmem

import (
	"testing"

	"github.com/stretchr/testify/require"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// receiveNow takes one delivery without waiting. An immediate message is
// dispatched by the call that makes it eligible - the publish, the attach, the
// settlement or the release - so a delivery is either already buffered or was
// never made, and the assertions below stay about counts rather than timing.
func receiveNow(t *testing.T, conn *conn, consumer driver.Consumer) driver.InboundMessage {
	t.Helper()
	conn.ReleaseDue()
	select {
	case message, ok := <-consumer.Messages():
		require.True(t, ok, "Messages must stay open while the consumer is live")
		return message
	default:
		t.Fatal("no delivery was made")
		return driver.InboundMessage{}
	}
}

// requireNoDelivery fails when another dispatch pass makes a delivery. It is
// the exact-count half of the tests below: one delivery per group means the
// channel is empty afterwards.
func requireNoDelivery(t *testing.T, conn *conn, consumer driver.Consumer) {
	t.Helper()
	conn.ReleaseDue()
	select {
	case message, ok := <-consumer.Messages():
		if !ok {
			return
		}
		t.Fatalf("unexpected extra delivery of %q", message.Body)
	default:
	}
}

// TestReleaseAfterLaterAckRedeliversEarlierDelivery proves a group's position
// is a creation floor that settlement never moves. A consumer holding two
// messages acknowledges the later one and releases; the earlier, unsettled one
// must come back to the group on rejoin, the acknowledged one must not, and the
// group's lag must return to zero.
func TestReleaseAfterLaterAckRedeliversEarlierDelivery(t *testing.T) {
	ctx, opened, producer := openTest(t, nil, driver.DestinationSpec{Name: "orders"})
	impl := opened.(*conn)
	config := driver.ConsumerConfig{
		Group: "orders.reader", Destinations: []string{"orders"}, Prefetch: 2,
		StartAt: driver.StartEarliest, Effective: driver.Capabilities{LagQueryable: true},
	}
	require.NoError(t, producer.Publish(ctx,
		driver.OutboundMessage{Destination: "orders", Body: []byte("first")},
		driver.OutboundMessage{Destination: "orders", Body: []byte("second")},
	))
	holder, err := impl.Consumer(ctx, config)
	require.NoError(t, err)
	first := receiveNow(t, impl, holder)
	second := receiveNow(t, impl, holder)
	require.Equal(t, "first", string(first.Body))
	require.Equal(t, "second", string(second.Body))
	require.NoError(t, second.Settle.Ack(ctx))
	require.NoError(t, holder.Release(ctx))

	rejoined, err := impl.Consumer(ctx, config)
	require.NoError(t, err)
	redelivered := receiveNow(t, impl, rejoined)
	require.Equal(t, "first", string(redelivered.Body), "the unsettled message must return to the group")
	requireNoDelivery(t, impl, rejoined)
	require.NoError(t, redelivered.Settle.Ack(ctx))
	lag, err := rejoined.Lag(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), lag["orders"], "the released message must not be stranded in the queue")
	closeTest(t, ctx, impl, producer, rejoined)
}

// TestTwoGroupsEachReceiveKeyedMessageOnce proves key affinity is per group: a
// keyed message reaches every group on the destination exactly once, with the
// first group's affinity consumer unable to take a second copy and unable to
// withhold the message from the second group.
func TestTwoGroupsEachReceiveKeyedMessageOnce(t *testing.T) {
	ctx, opened, producer := openTest(t, nil, driver.DestinationSpec{Name: "orders"})
	impl := opened.(*conn)
	consumer := func(group string) driver.Consumer {
		cs, err := impl.Consumer(ctx, driver.ConsumerConfig{
			Group: group, Destinations: []string{"orders"}, Prefetch: 2, StartAt: driver.StartEarliest,
		})
		require.NoError(t, err)
		return cs
	}
	first := consumer("orders.first")
	second := consumer("orders.second")
	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{
		Destination: "orders", Key: []byte("order-1"), Body: []byte("keyed"),
	}))

	delivered := receiveNow(t, impl, first)
	require.Equal(t, "keyed", string(delivered.Body))
	requireNoDelivery(t, impl, first)
	other := receiveNow(t, impl, second)
	require.Equal(t, "keyed", string(other.Body), "the second group must receive the keyed message")
	requireNoDelivery(t, impl, second)
	require.NoError(t, delivered.Settle.Ack(ctx))
	require.NoError(t, other.Settle.Ack(ctx))
	requireNoDelivery(t, impl, first)
	requireNoDelivery(t, impl, second)
	closeTest(t, ctx, impl, producer, first, second)
}

// TestDetachedGroupReceivesMessageAckedWhileAway proves a named group stays a
// member of its destination after it detaches: a message the away group never
// received is not retired by the group that is still attached, and the rejoin
// receives it exactly once.
func TestDetachedGroupReceivesMessageAckedWhileAway(t *testing.T) {
	ctx, opened, producer := openTest(t, nil, driver.DestinationSpec{Name: "orders"})
	impl := opened.(*conn)
	config := driver.ConsumerConfig{
		Group: "orders.reporter", Destinations: []string{"orders"}, Prefetch: 1, StartAt: driver.StartEarliest,
	}
	reader, err := impl.Consumer(ctx, driver.ConsumerConfig{
		Group: "orders.reader", Destinations: []string{"orders"}, Prefetch: 1, StartAt: driver.StartEarliest,
	})
	require.NoError(t, err)
	reporter, err := impl.Consumer(ctx, config)
	require.NoError(t, err)
	require.NoError(t, reporter.Release(ctx))

	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "orders", Body: []byte("while-away")}))
	delivered := receiveNow(t, impl, reader)
	require.Equal(t, "while-away", string(delivered.Body))
	require.NoError(t, delivered.Settle.Ack(ctx))
	requireNoDelivery(t, impl, reader)

	rejoined, err := impl.Consumer(ctx, config)
	require.NoError(t, err)
	back := receiveNow(t, impl, rejoined)
	require.Equal(t, "while-away", string(back.Body), "the message must wait for the group that was away")
	requireNoDelivery(t, impl, rejoined)
	require.NoError(t, back.Settle.Ack(ctx))
	closeTest(t, ctx, impl, producer, reader, rejoined)
}

// TestLatestGroupDoesNotHoldBackEarlierMessage proves retirement ignores a group
// whose floor is above the message: a group created with StartLatest after the
// message was published can never be given it, so the message retires once every
// group that can take it has, and the destination's depth returns to zero
// instead of holding the message for the life of the connection.
func TestLatestGroupDoesNotHoldBackEarlierMessage(t *testing.T) {
	ctx, opened, producer := openTest(t, nil, driver.DestinationSpec{Name: "orders"})
	impl := opened.(*conn)

	earlier, err := impl.Consumer(ctx, driver.ConsumerConfig{
		Group: "orders.earlier", Destinations: []string{"orders"}, Prefetch: 1, StartAt: driver.StartEarliest,
	})
	require.NoError(t, err)
	require.NoError(t, earlier.Pause("orders"))
	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "orders", Body: []byte("earlier")}))

	later, err := impl.Consumer(ctx, driver.ConsumerConfig{
		Group: "orders.later", Destinations: []string{"orders"}, Prefetch: 1, StartAt: driver.StartLatest,
	})
	require.NoError(t, err)

	require.NoError(t, earlier.Resume("orders"))
	message := receiveNow(t, impl, earlier)
	require.Equal(t, "earlier", string(message.Body))
	require.NoError(t, message.Settle.Ack(ctx))
	requireNoDelivery(t, impl, later)

	depth, err := impl.Admin().DescribeTopology(ctx, []string{"orders"})
	require.NoError(t, err)
	require.Equal(t, int64(0), depth.Depth["orders"], "a group created after the message was published must not keep it queued")
	closeTest(t, ctx, impl, producer, earlier, later)
}

// TestPurgeDropsReplayHistory proves Purge empties the destination's replay
// history as well as its queue: a group attaching from earliest afterwards must
// receive nothing, because the purged body is retained nowhere the driver can
// replay it from.
func TestPurgeDropsReplayHistory(t *testing.T) {
	ctx, opened, producer := openTest(t, nil, driver.DestinationSpec{Name: "orders"})
	impl := opened.(*conn)
	maintenance, ok := impl.Admin().(driver.Maintenance)
	require.True(t, ok)

	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "orders", Body: []byte("purged")}))
	purged, err := maintenance.Purge(ctx, "orders")
	require.NoError(t, err)
	require.Equal(t, int64(1), purged, "Purge must report the queued messages it removed")
	late, err := impl.Consumer(ctx, driver.ConsumerConfig{
		Group: "orders.late", Destinations: []string{"orders"}, Prefetch: 1, StartAt: driver.StartEarliest,
	})
	require.NoError(t, err)
	requireNoDelivery(t, impl, late)
	closeTest(t, ctx, impl, producer, late)
}

// TestPruneDropsReplayHistoryAndMembership proves Prune deletes a destination's
// replay history and its place in every group's membership: a recreated
// destination serves a new group nothing, and the group that was attached
// before the delete does not keep the recreated destination's first message
// queued.
//
// It reaches Prune the one way a destination can hold replay history with an
// empty queue and no consumer attached: a body is published after the last
// consumer detached and then purged, which at this driver's base leaves the
// history behind. That is what makes the test red before the fix.
func TestPruneDropsReplayHistoryAndMembership(t *testing.T) {
	ctx, opened, producer := openTest(t, nil, driver.DestinationSpec{Name: "orders"})
	impl := opened.(*conn)
	maintenance, ok := impl.Admin().(driver.Maintenance)
	require.True(t, ok)

	// The reader settles one body and detaches.
	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "orders", Body: []byte("settled")}))
	reader, err := impl.Consumer(ctx, driver.ConsumerConfig{
		Group: "orders.reader", Destinations: []string{"orders"}, Prefetch: 1, StartAt: driver.StartEarliest,
	})
	require.NoError(t, err)
	require.NoError(t, receiveNow(t, impl, reader).Settle.Ack(ctx))
	require.NoError(t, reader.Stop(ctx))

	// Published with nothing attached, then purged: empty queue, history left
	// behind, which is the state Prune has to clean up.
	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "orders", Body: []byte("pruned")}))
	_, err = maintenance.Purge(ctx, "orders")
	require.NoError(t, err)

	results, err := maintenance.Prune(ctx, []string{"orders"})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.True(t, results[0].Deleted, "Prune must delete an empty destination with no consumer attached: %s", results[0].Reason)

	_, err = impl.Admin().EnsureTopology(ctx, driver.TopologySpec{Destinations: []driver.DestinationSpec{{Name: "orders"}}})
	require.NoError(t, err)
	recreated, err := impl.Consumer(ctx, driver.ConsumerConfig{
		Group: "orders.recreated", Destinations: []string{"orders"}, Prefetch: 1,
		StartAt: driver.StartEarliest, Effective: driver.Capabilities{LagQueryable: true},
	})
	require.NoError(t, err)
	requireNoDelivery(t, impl, recreated)

	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "orders", Body: []byte("after-prune")}))
	message := receiveNow(t, impl, recreated)
	require.Equal(t, "after-prune", string(message.Body))
	require.NoError(t, message.Settle.Ack(ctx))
	requireNoDelivery(t, impl, recreated)
	depth, err := impl.Admin().DescribeTopology(ctx, []string{"orders"})
	require.NoError(t, err)
	require.Equal(t, int64(0), depth.Depth["orders"], "a group from before the delete must not keep a message queued")
	lag, err := recreated.Lag(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), lag["orders"])
	closeTest(t, ctx, impl, producer, recreated)
}

// TestRequeueBeforeAnotherGroupReceivesKeepsOneQueuedCopy proves a requeue
// returns a message to the queue only when it is not already there. The first
// group holds the message and requeues it while the second group has not
// received it, so the message is both still queued and being handed back: the
// second group must receive it once, the first group must receive it twice, and
// the destination must end empty.
func TestRequeueBeforeAnotherGroupReceivesKeepsOneQueuedCopy(t *testing.T) {
	ctx, opened, producer := openTest(t, nil, driver.DestinationSpec{Name: "orders"})
	impl := opened.(*conn)
	holder, err := impl.Consumer(ctx, driver.ConsumerConfig{
		Group: "orders.holder", Destinations: []string{"orders"}, Prefetch: 2,
		StartAt: driver.StartEarliest, Effective: driver.Capabilities{LagQueryable: true},
	})
	require.NoError(t, err)
	other, err := impl.Consumer(ctx, driver.ConsumerConfig{
		Group: "orders.other", Destinations: []string{"orders"}, Prefetch: 2, StartAt: driver.StartEarliest,
	})
	require.NoError(t, err)
	// Pausing the second group is what puts the first group in front of it:
	// the message is only dispatched once, to the group that is attached and
	// not paused, and the second group is still a member that has not seen it.
	require.NoError(t, other.Pause("orders"))
	require.NoError(t, producer.Publish(ctx, driver.OutboundMessage{Destination: "orders", Body: []byte("contended")}))

	first := receiveNow(t, impl, holder)
	require.Equal(t, "contended", string(first.Body))
	require.NoError(t, first.Settle.Nack(ctx, driver.NackOptions{Requeue: true}))

	require.NoError(t, other.Resume("orders"))
	redelivered := receiveNow(t, impl, holder)
	require.Equal(t, "contended", string(redelivered.Body), "a requeue must return the message to its group")
	require.NoError(t, redelivered.Settle.Ack(ctx))
	received := receiveNow(t, impl, other)
	require.Equal(t, "contended", string(received.Body), "the other group must receive the requeued message")
	require.NoError(t, received.Settle.Ack(ctx))

	lag, err := holder.Lag(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), lag["orders"], "the requeue must not leave a duplicate queued")
	depth, err := impl.Admin().DescribeTopology(ctx, []string{"orders"})
	require.NoError(t, err)
	require.Equal(t, int64(0), depth.Depth["orders"])
	requireNoDelivery(t, impl, holder)
	requireNoDelivery(t, impl, other)
	closeTest(t, ctx, impl, producer, holder, other)
}
