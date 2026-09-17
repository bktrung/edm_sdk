package conformance

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func init() {
	registerGroup("rebalance", runRebalance)
}

// warmRebalanceTopology pays Kafka's creation and metadata propagation cost before rebalance checks.
func warmRebalanceTopology(group *groupContext) {
	destinations := make([]driver.DestinationSpec, 0, 12)
	for _, name := range []string{
		"rebalance.scale-up",
		"rebalance.reassign",
		"rebalance.in-flight",
		"rebalance.delivery-count",
		"rebalance.prefetch",
		"rebalance.repeat",
		"rebalance.settle",
		"rebalance.key-a",
		"rebalance.key-b",
		"rebalance.affinity",
		"rebalance.join-in-flight",
		"rebalance.idle-join",
	} {
		destinations = append(destinations, driver.DestinationSpec{
			Name: name, Partitions: rebalancePartitionCount,
		})
	}
	warmTopology(group.t, group, driver.TopologySpec{
		Destinations: destinations,
		Effective:    group.effective,
	})
}

func runRebalance(group *groupContext) {
	warmRebalanceTopology(group)
	group.Check("adding a consumer distributes new work to both consumers", func(t *testing.T) {
		producer := newRebalanceProducer(t, group, "rebalance.scale-up", driver.ProducerConfig{Effective: group.effective})
		first := newRebalanceConsumer(t, group, "rebalance.scale-up", 1)
		waitForRebalanceAssignment(t, group, first)
		second := newRebalanceConsumer(t, group, "rebalance.scale-up", 1)
		waitForRebalanceAssignment(t, group, second)
		if err := producer.Publish(group.ctx,
			driver.OutboundMessage{Destination: "rebalance.scale-up", Key: []byte("rebalance-key-0"), Body: []byte("first")},
			driver.OutboundMessage{Destination: "rebalance.scale-up", Key: []byte("rebalance-key-1"), Body: []byte("second")},
		); err != nil {
			t.Fatal(err)
		}
		ackMessage(t, group, receiveMessage(t, group, first))
		ackMessage(t, group, receiveMessage(t, group, second))
		group.vector.Add(BehaviorEvent{ID: "rebalance-scale-up", Outcome: "distributed", FinalDestination: "rebalance.scale-up"})
	})

	group.Check("draining a consumer reassigns new work to a survivor", func(t *testing.T) {
		producer := newRebalanceProducer(t, group, "rebalance.reassign", driver.ProducerConfig{Effective: group.effective})
		departing := newRebalanceConsumer(t, group, "rebalance.reassign", 1)
		waitForRebalanceAssignment(t, group, departing)
		survivor := newRebalanceConsumer(t, group, "rebalance.reassign", 1)
		if err := departing.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		publishRebalanceCount(t, group, producer, "rebalance.reassign", 4)
		ackAll(t, group, survivor, 4)
		waitForStable(t, group, "drained consumer to remain without reassigned work", func() (bool, string) {
			select {
			case message, ok := <-departing.Messages():
				if !ok {
					return false, "Messages channel closed"
				}
				return false, fmt.Sprintf("received %q", message.Destination)
			default:
				return true, "no delivery to drained consumer"
			}
		})
		group.vector.Add(BehaviorEvent{ID: "rebalance-reassign", Outcome: "reassigned", FinalDestination: "rebalance.reassign"})
	})

	group.Check("drained consumer requeues in-flight work to one survivor", func(t *testing.T) {
		producer := newRebalanceProducer(t, group, "rebalance.in-flight", driver.ProducerConfig{Effective: group.effective})
		first := newRebalanceConsumer(t, group, "rebalance.in-flight", 1)
		waitForRebalanceAssignment(t, group, first)
		second := newRebalanceConsumer(t, group, "rebalance.in-flight", 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.in-flight", Body: []byte("once")}); err != nil {
			t.Fatal(err)
		}
		inFlight, departing, survivor := receiveFromEither(t, group, "in-flight message", first, second)
		if err := departing.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		if err := inFlight.Settle.Nack(group.ctx, driver.NackOptions{Requeue: true}); err != nil {
			t.Fatalf("Nack(requeue) error = %v", err)
		}
		redelivered := receiveMessage(t, group, survivor)
		if string(redelivered.Body) != "once" {
			t.Fatalf("redelivered body = %q, want once", redelivered.Body)
		}
		ackMessage(t, group, redelivered)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.in-flight", Body: []byte("next")}); err != nil {
			t.Fatal(err)
		}
		next := receiveMessage(t, group, survivor)
		if string(next.Body) != "next" {
			t.Fatalf("next body = %q, want next", next.Body)
		}
		ackMessage(t, group, next)
		waitFor(t, group, "requeued in-flight message to settle once", func() (bool, string) {
			view := inspectDestination(t, group, "rebalance.in-flight")
			return view.Ready == 0 && view.Unsettled == 0, fmt.Sprintf("view=%+v", view)
		})
		group.vector.Add(BehaviorEvent{ID: "rebalance-in-flight", Outcome: "redelivered", AttemptCount: 2, FinalDestination: "rebalance.in-flight"})
	})

	group.Check("redelivery count advances only when available", func(t *testing.T) {
		producer := newRebalanceProducer(t, group, "rebalance.delivery-count", driver.ProducerConfig{Effective: group.effective})
		firstConsumer := newRebalanceConsumer(t, group, "rebalance.delivery-count", 1)
		waitForRebalanceAssignment(t, group, firstConsumer)
		secondConsumer := newRebalanceConsumer(t, group, "rebalance.delivery-count", 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.delivery-count"}); err != nil {
			t.Fatal(err)
		}
		first, departing, survivor := receiveFromEither(t, group, "delivery-count message", firstConsumer, secondConsumer)
		if err := departing.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		if err := first.Settle.Nack(group.ctx, driver.NackOptions{Requeue: true}); err != nil {
			t.Fatal(err)
		}
		redelivery := receiveMessage(t, group, survivor)
		if group.effective.NativeDeliveryCount {
			if redelivery.DeliveryCount <= first.DeliveryCount {
				t.Fatalf("redelivery DeliveryCount = %d, first = %d", redelivery.DeliveryCount, first.DeliveryCount)
			}
		} else if redelivery.DeliveryCount != -1 {
			t.Fatalf("DeliveryCount = %d, want -1 when unavailable", redelivery.DeliveryCount)
		}
		ackMessage(t, group, redelivery)
		group.vector.Add(BehaviorEvent{ID: "rebalance-delivery-count", Outcome: "redelivered", AttemptCount: 2, FinalDestination: "rebalance.delivery-count"})
	})

	group.Check("each consumer keeps its own prefetch budget after joining", func(t *testing.T) {
		producer := newRebalanceProducer(t, group, "rebalance.prefetch", driver.ProducerConfig{Effective: group.effective})
		const firstPrefetch = 1
		const secondPrefetch = 2
		first := newRebalanceConsumer(t, group, "rebalance.prefetch", firstPrefetch)
		waitForRebalanceAssignment(t, group, first)
		second := newRebalanceConsumer(t, group, "rebalance.prefetch", secondPrefetch)
		publishRebalanceCount(t, group, producer, "rebalance.prefetch", 4)
		waitFor(t, group, "per-consumer prefetch budgets to saturate", func() (bool, string) {
			view := inspectDestination(t, group, "rebalance.prefetch")
			return view.Unsettled == 3 && view.Ready == 1, fmt.Sprintf("view=%+v", view)
		})

		initial := make([]driver.InboundMessage, 0, firstPrefetch+secondPrefetch)
		firstOutstanding, secondOutstanding := 0, 0
		for range firstPrefetch + secondPrefetch {
			message, owner, _ := receiveFromEither(t, group, "prefetch delivery", first, second)
			initial = append(initial, message)
			switch owner {
			case first:
				firstOutstanding++
			case second:
				secondOutstanding++
			default:
				t.Fatalf("prefetch delivery came from an unknown consumer")
			}
		}
		if firstOutstanding > firstPrefetch {
			t.Fatalf("first consumer held %d unsettled messages, prefetch=%d", firstOutstanding, firstPrefetch)
		}
		if secondOutstanding > secondPrefetch {
			t.Fatalf("second consumer held %d unsettled messages, prefetch=%d", secondOutstanding, secondPrefetch)
		}
		// Every published record must be settled, and a record whose ack meets
		// the transfer is proved by its redelivery or by the drain: a revoked
		// acknowledgement is not proof of an unsettled record, because the
		// offset may have committed for the new owner all the same. The
		// redelivery cannot arrive while the destination has no free capacity,
		// since a revoked copy keeps the slot it held until it is settled, so
		// the acks and the redeliveries proceed in one loop.
		settled := 0
		for _, message := range initial {
			if settleBeforeOwnershipTransfer(t, group, message) {
				settled++
			}
		}
		drained := false
		for settled < firstPrefetch+secondPrefetch+1 && !drained {
			message, _, _ := receiveFromEither(t, group, "prefetch delivery or redelivery", first, second)
			if settleBeforeOwnershipTransfer(t, group, message) {
				settled++
			}
			view := inspectDestination(t, group, "rebalance.prefetch")
			drained = view.Ready == 0 && view.Unsettled == 0
		}
		waitFor(t, group, "prefetch destination to drain after all messages are acknowledged", func() (bool, string) {
			view := inspectDestination(t, group, "rebalance.prefetch")
			return view.Ready == 0 && view.Unsettled == 0, fmt.Sprintf("view=%+v", view)
		})
		group.vector.Add(BehaviorEvent{ID: "rebalance-prefetch", Outcome: "bounded", FinalDestination: "rebalance.prefetch"})
	})

	group.Check("repeating a departure leaves redistribution stable", func(t *testing.T) {
		producer := newRebalanceProducer(t, group, "rebalance.repeat", driver.ProducerConfig{Effective: group.effective})
		departing := newRebalanceConsumer(t, group, "rebalance.repeat", 1)
		waitForRebalanceAssignment(t, group, departing)
		survivor := newRebalanceConsumer(t, group, "rebalance.repeat", 1)
		if err := departing.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		if err := departing.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.repeat"}); err != nil {
			t.Fatal(err)
		}
		ackMessage(t, group, receiveMessage(t, group, survivor))
		group.vector.Add(BehaviorEvent{ID: "rebalance-repeat", Outcome: "stable", FinalDestination: "rebalance.repeat"})
	})

	group.Check("delivery made before a membership change remains settleable", func(t *testing.T) {
		producer := newRebalanceProducer(t, group, "rebalance.settle", driver.ProducerConfig{Effective: group.effective})
		first := newRebalanceConsumer(t, group, "rebalance.settle", 1)
		waitForRebalanceAssignment(t, group, first)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.settle", Body: []byte("old")}); err != nil {
			t.Fatal(err)
		}
		old := receiveMessage(t, group, first)
		joining := newRebalanceConsumer(t, group, "rebalance.settle", 1)
		waitForRebalanceAssignment(t, group, joining)

		if settled := settleBeforeOwnershipTransfer(t, group, old); !settled {
			// A revoked acknowledgement is not proof of an unsettled record: the
			// offset may have committed for the new owner all the same, the key
			// may have moved and be redelivered, or the key may have come back
			// with this delivery re-granted. Settle whichever arrives, and let
			// the destination's own drain decide whether anything was left.
			waitFor(t, group, "a revoked pre-change delivery to settle or be redelivered", func() (bool, string) {
				view := inspectDestination(t, group, "rebalance.settle")
				if view.Ready == 0 && view.Unsettled == 0 {
					return true, "destination drained"
				}
				for _, consumer := range []driver.Consumer{first, joining} {
					select {
					case redelivered, ok := <-consumer.Messages():
						if !ok {
							return false, "Messages channel closed"
						}
						if string(redelivered.Body) != "old" {
							t.Fatalf("redelivered body = %q, want old", redelivered.Body)
						}
						ackMessage(t, group, redelivered)
						return true, "redelivered to the owner"
					default:
					}
				}
				return false, "no redelivery and the destination is not drained"
			})
		}
		waitFor(t, group, "pre-change delivery settlement to clear", func() (bool, string) {
			view := inspectDestination(t, group, "rebalance.settle")
			return view.Ready == 0 && view.Unsettled == 0, fmt.Sprintf("view=%+v", view)
		})
		group.vector.Add(BehaviorEvent{ID: "rebalance-settle", Outcome: "bounded", FinalDestination: "rebalance.settle"})
	})

	group.Check("a key remains ordered across bounded ownership transfer", func(t *testing.T) {
		producerA := newRebalanceProducer(t, group, "rebalance.key-a", driver.ProducerConfig{Effective: group.effective})
		producerB := newRebalanceProducer(t, group, "rebalance.key-b", driver.ProducerConfig{Effective: group.effective})
		departing := newRebalanceConsumerFor(t, group, driver.ConsumerConfig{
			Destinations: []string{"rebalance.key-a", "rebalance.key-b"}, Prefetch: 4, Effective: group.effective,
		})
		waitForRebalanceAssignment(t, group, departing)
		if err := producerA.Publish(group.ctx,
			driver.OutboundMessage{Destination: "rebalance.key-a", Key: []byte("K"), Body: []byte("old")},
			driver.OutboundMessage{Destination: "rebalance.key-a", Key: []byte("L"), Body: []byte("old-secondary")},
		); err != nil {
			t.Fatal(err)
		}
		oldMessages := make(map[string]driver.InboundMessage, 2)
		for range 2 {
			message := receiveMessage(t, group, departing)
			body := string(message.Body)
			if body != "old" && body != "old-secondary" {
				t.Fatalf("in-flight body = %q, want old or old-secondary", message.Body)
			}
			if _, exists := oldMessages[body]; exists {
				t.Fatalf("duplicate in-flight body %q", body)
			}
			oldMessages[body] = message
		}
		if err := departing.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		survivor := newRebalanceConsumerFor(t, group, driver.ConsumerConfig{
			Destinations: []string{"rebalance.key-a", "rebalance.key-b"}, Prefetch: 1, Effective: group.effective,
		})
		waitForRebalanceAssignment(t, group, survivor)
		if err := producerA.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.key-a", Key: []byte("K"), Body: []byte("later")}); err != nil {
			t.Fatal(err)
		}
		if err := producerB.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.key-b", Key: []byte("control"), Body: []byte("control")}); err != nil {
			t.Fatal(err)
		}

		transfer := make(map[string]bool, len(oldMessages))
		for _, message := range oldMessages {
			transfer[string(message.Body)] = !settleBeforeOwnershipTransfer(t, group, message)
		}

		expected := 2
		for _, transferred := range transfer {
			if transferred {
				expected++
			}
		}
		seenBodies := make(map[string]int, expected)
		seenLater := false
		for range expected {
			message := receiveMessage(t, group, survivor)
			body := string(message.Body)
			switch body {
			case "old", "old-secondary":
				if !transfer[body] {
					t.Fatalf("body %q was redelivered after settlement won", body)
				}
				if body == "old" && seenLater {
					t.Fatal("old same-key body was redelivered after later same-key work")
				}
			case "later":
				seenLater = true
			case "control":
			default:
				t.Fatalf("unexpected survivor body %q", message.Body)
			}
			seenBodies[body]++
			if seenBodies[body] > 1 {
				t.Fatalf("body %q was delivered more than once", body)
			}
			ackMessage(t, group, message)
		}
		for body, transferred := range transfer {
			if transferred && seenBodies[body] != 1 {
				t.Fatalf("transferred body %q settlement count = %d, want 1", body, seenBodies[body])
			}
			if !transferred && seenBodies[body] != 0 {
				t.Fatalf("settled body %q was redelivered", body)
			}
		}
		if seenBodies["later"] != 1 || seenBodies["control"] != 1 {
			t.Fatalf("later/control settlement counts = later:%d control:%d, want one each", seenBodies["later"], seenBodies["control"])
		}
		waitForStable(t, group, "survivor to remain free of duplicate keyed deliveries", func() (bool, string) {
			select {
			case message, ok := <-survivor.Messages():
				if !ok {
					return false, "Messages channel closed"
				}
				return false, fmt.Sprintf("received duplicate body=%q", message.Body)
			default:
				return true, "no duplicate keyed delivery"
			}
		})
		waitFor(t, group, "bounded key transfer to settle all bodies", func() (bool, string) {
			view := inspectDestination(t, group, "rebalance.key-a")
			control := inspectDestination(t, group, "rebalance.key-b")
			return view.Ready == 0 && view.Unsettled == 0 && control.Ready == 0 && control.Unsettled == 0, fmt.Sprintf("key=%+v control=%+v", view, control)
		})
		group.vector.Add(BehaviorEvent{ID: "rebalance-key-exclusive", Outcome: "bounded", FinalDestination: "rebalance.key-a"})
	})

	group.Check("settled key reassigns after its holder drains", func(t *testing.T) {
		producer := newRebalanceProducer(t, group, "rebalance.affinity", driver.ProducerConfig{Effective: group.effective})
		departing := newRebalanceConsumer(t, group, "rebalance.affinity", 1)
		waitForRebalanceAssignment(t, group, departing)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.affinity", Key: []byte("order")}); err != nil {
			t.Fatal(err)
		}
		ackMessage(t, group, receiveMessage(t, group, departing))
		if err := departing.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		survivor := newRebalanceConsumer(t, group, "rebalance.affinity", 1)
		waitForRebalanceAssignment(t, group, survivor)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "rebalance.affinity", Key: []byte("order")}); err != nil {
			t.Fatal(err)
		}
		ackMessage(t, group, receiveMessage(t, group, survivor))
		group.vector.Add(BehaviorEvent{ID: "rebalance-settled-key", Outcome: "reassigned", FinalDestination: "rebalance.affinity"})
	})

	group.Check("joining consumer handles bounded ownership transfer", func(t *testing.T) {
		producer := newRebalanceProducer(t, group, "rebalance.join-in-flight", driver.ProducerConfig{Effective: group.effective})
		first := newRebalanceConsumer(t, group, "rebalance.join-in-flight", 1)
		waitForRebalanceAssignment(t, group, first)
		oldKey := []byte(nil)
		if group.conn.BrokerInfo().Kind == "kafka" {
			oldKey = []byte("K")
		}
		if err := producer.Publish(group.ctx, driver.OutboundMessage{
			Destination: "rebalance.join-in-flight",
			Key:         oldKey,
			Body:        []byte("old"),
		}); err != nil {
			t.Fatal(err)
		}
		old := receiveMessage(t, group, first)
		if err := first.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		joining := newRebalanceConsumer(t, group, "rebalance.join-in-flight", 1)
		waitForRebalanceAssignment(t, group, joining)
		if err := producer.Publish(group.ctx,
			driver.OutboundMessage{Destination: "rebalance.join-in-flight", Key: []byte("control"), Body: []byte("control")},
			driver.OutboundMessage{Destination: "rebalance.join-in-flight", Key: []byte("K"), Body: []byte("later")},
		); err != nil {
			t.Fatal(err)
		}

		transferred := !settleBeforeOwnershipTransfer(t, group, old)
		expected := 2
		if transferred {
			expected++
		}
		seen := make(map[string]struct{}, expected)
		seenLater := false
		for range expected {
			message := receiveMessage(t, group, joining)
			body := string(message.Body)
			switch body {
			case "old":
				if !transferred {
					t.Fatal("old body was redelivered after settlement won")
				}
				if seenLater {
					t.Fatal("old body was redelivered after later same-key work")
				}
			case "control":
			case "later":
				seenLater = true
			default:
				t.Fatalf("unexpected joining body %q", message.Body)
			}
			if _, exists := seen[body]; exists {
				t.Fatalf("joining received body %q more than once", body)
			}
			seen[body] = struct{}{}
			ackMessage(t, group, message)
		}
		if _, ok := seen["control"]; !ok {
			t.Fatalf("joining settlement bodies = %v, want control and later", seen)
		}
		if _, ok := seen["later"]; !ok {
			t.Fatalf("joining settlement bodies = %v, want control and later", seen)
		}
		if transferred {
			if _, ok := seen["old"]; !ok {
				t.Fatalf("transferred old body was not redelivered: %v", seen)
			}
		} else if _, ok := seen["old"]; ok {
			t.Fatalf("settled old body was redelivered: %v", seen)
		}
		waitFor(t, group, "bounded joining transfer to settle all bodies", func() (bool, string) {
			view := inspectDestination(t, group, "rebalance.join-in-flight")
			return view.Ready == 0 && view.Unsettled == 0, fmt.Sprintf("view=%+v", view)
		})
		group.vector.Add(BehaviorEvent{ID: "rebalance-join-in-flight", Outcome: "bounded", FinalDestination: "rebalance.join-in-flight"})
	})

	group.Check("joining an idle destination receives newly published work", func(t *testing.T) {
		producer := newRebalanceProducer(t, group, "rebalance.idle-join", driver.ProducerConfig{Effective: group.effective})
		departing := newRebalanceConsumer(t, group, "rebalance.idle-join", 1)
		waitForRebalanceAssignment(t, group, departing)
		if err := departing.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		joining := newRebalanceConsumer(t, group, "rebalance.idle-join", 1)
		waitForRebalanceAssignment(t, group, joining)
		publishRebalanceCount(t, group, producer, "rebalance.idle-join", 4)
		ackAll(t, group, joining, 4)
		group.vector.Add(BehaviorEvent{ID: "rebalance-idle-join", Outcome: "joined", FinalDestination: "rebalance.idle-join"})
	})
}

// rebalancePartitionCount is the partition count every rebalance destination is
// declared with. It is also the number of keys placementKey cycles over, and it
// has to be: the checks publish one record per key and require several of them
// outstanding at once, which one partition cannot give.
const rebalancePartitionCount = placementPartitions

func newRebalanceProducer(t *testing.T, group *groupContext, destination string, config driver.ProducerConfig) driver.Producer {
	t.Helper()
	destination = profileDestination(group, destination)
	if _, err := group.conn.Admin().EnsureTopology(group.ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Partitions: rebalancePartitionCount}},
		Effective:    group.effective,
	}); err != nil {
		t.Fatalf("EnsureTopology(%q): %v", destination, err)
	}
	producer, err := group.conn.Producer(group.ctx, config)
	if err != nil {
		t.Fatalf("Producer(%q): %v", destination, err)
	}
	t.Cleanup(func() {
		if err := producer.Close(group.ctx); err != nil {
			t.Errorf("close producer %q: %v", destination, err)
		}
	})
	t.Cleanup(func() {
		if err := purgeIfSupported(group.ctx, group.conn, destination); err != nil {
			t.Errorf("purge destination %q: %v", destination, err)
		}
	})
	return &profileProducer{group: group, producer: producer, scoped: true}
}

// publishRebalanceCount publishes count messages to destination, each keyed so
// that a partition-bound driver places them on distinct partitions. A check that
// requires several of them outstanding at once needs that: a destination's
// partitions, not the sum of its consumers' prefetch budgets, decide how much a
// partition-bound driver may hold unsettled.
func publishRebalanceCount(t *testing.T, group *groupContext, producer driver.Producer, destination string, count int) {
	t.Helper()
	messages := make([]driver.OutboundMessage, count)
	for i := range messages {
		messages[i] = driver.OutboundMessage{
			Destination: destination,
			Key:         []byte(placementKey(i)),
			Body:        fmt.Appendf(nil, "message-%d", i),
		}
	}
	if err := producer.Publish(group.ctx, messages...); err != nil {
		t.Fatalf("Publish(%q, %d messages): %v", destination, count, err)
	}
}

// rebalanceGroupName returns the consumer group one rebalance check owns. Each
// check gets its own group, so a member that fails to leave cannot hold a later
// check behind it; the consumers of a single check pass the same subtest in and
// share its group.
func rebalanceGroupName(group *groupContext, t *testing.T) string {
	return "conformance.rebalance." + group.runID + "." + group.profile.String() + "." + rebalanceCheckScope(t)
}

// rebalanceCheckScope returns the per-check part of a rebalance group name: the
// last element of the running subtest's name, which the harness sets to the
// check name, mapped to the characters a group identifier accepts.
func rebalanceCheckScope(t *testing.T) string {
	name := t.Name()
	if index := strings.LastIndexByte(name, '/'); index >= 0 {
		name = name[index+1:]
	}
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, name)
}

func newRebalanceConsumer(t *testing.T, group *groupContext, destination string, prefetch int) driver.Consumer {
	t.Helper()
	cfg := driver.ConsumerConfig{
		Destinations: []string{destination},
		Group:        rebalanceGroupName(group, t),
		Prefetch:     prefetch,
		Effective:    group.effective,
	}
	cfg, logical := profileConsumerConfig(group, cfg)
	consumer, err := group.conn.Consumer(group.ctx, cfg)
	if err != nil {
		t.Fatalf("Consumer(%v): %v", destination, err)
	}
	wrapped := newProfileConsumer(group, consumer, logical)
	t.Cleanup(func() {
		if err := wrapped.Stop(group.ctx); err != nil {
			t.Errorf("stop consumer %v: %v", destination, err)
		}
	})
	return wrapped
}

func newRebalanceConsumerFor(t *testing.T, group *groupContext, cfg driver.ConsumerConfig) driver.Consumer {
	t.Helper()
	if cfg.Group == "" {
		cfg.Group = rebalanceGroupName(group, t)
	}
	cfg, logical := profileConsumerConfig(group, cfg)
	consumer, err := group.conn.Consumer(group.ctx, cfg)
	if err != nil {
		t.Fatalf("Consumer(%v): %v", cfg.Destinations, err)
	}
	wrapped := newProfileConsumer(group, consumer, logical)
	t.Cleanup(func() {
		if err := wrapped.Stop(group.ctx); err != nil {
			t.Errorf("stop consumer %v: %v", cfg.Destinations, err)
		}
	})
	return wrapped
}

func waitForRebalanceAssignment(t *testing.T, group *groupContext, consumer driver.Consumer) {
	t.Helper()
	if group.conn.BrokerInfo().Kind != "kafka" || group.effective.ConsumerScaling != driver.ScalingPartitionBound {
		return
	}
	ctx, cancel := context.WithTimeout(group.ctx, waitTimeout)
	defer cancel()
	if err := waitForRebalanceAssignmentError(ctx, consumer.Errors()); err != nil {
		t.Fatal(err)
	}
}

func waitForRebalanceAssignmentError(ctx context.Context, errs <-chan error) error {
	for {
		select {
		case err, ok := <-errs:
			if !ok {
				return fmt.Errorf("consumer Errors channel closed before partition assignment")
			}
			if err == nil {
				return fmt.Errorf("consumer emitted nil error before partition assignment")
			}
			kind, classified := driver.Classify(err)
			if classified && kind == driver.KindNotification {
				return nil
			}
			return fmt.Errorf("consumer error before partition assignment: %w", err)
		case <-ctx.Done():
			return fmt.Errorf("partition assignment notification timed out after %s: %w", waitTimeout, ctx.Err())
		}
	}
}

func settleBeforeOwnershipTransfer(t *testing.T, group *groupContext, message driver.InboundMessage) bool {
	t.Helper()
	err := message.Settle.Ack(group.ctx)
	if err == nil {
		return true
	}
	if !isRevokedSettlementError(err) {
		t.Fatalf("Ack() error = %v, want nil or a revocation error", err)
	}
	return false
}

func isRevokedSettlementError(err error) bool {
	if err == nil {
		return false
	}
	kind, classified := driver.Classify(err)
	return classified && kind == driver.KindFatal && strings.Contains(strings.ToLower(err.Error()), "partition assignment revoked")
}

func receiveFromEither(
	t *testing.T,
	group *groupContext,
	what string,
	first, second driver.Consumer,
) (driver.InboundMessage, driver.Consumer, driver.Consumer) {
	t.Helper()
	var received driver.InboundMessage
	var owner, other driver.Consumer
	waitFor(t, group, what, func() (bool, string) {
		select {
		case message, ok := <-first.Messages():
			if !ok {
				return false, "first Messages channel closed"
			}
			received, owner, other = message, first, second
			return true, "received by first consumer"
		case message, ok := <-second.Messages():
			if !ok {
				return false, "second Messages channel closed"
			}
			received, owner, other = message, second, first
			return true, "received by second consumer"
		default:
			return false, "no delivery"
		}
	})
	return received, owner, other
}
