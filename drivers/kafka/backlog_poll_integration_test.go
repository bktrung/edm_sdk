//go:build integration

package kafka

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

const (
	// backlogPollMessages is the backlog one run drains. It is several times
	// the per-destination budget and more than one fetch response, so the poll
	// loop has to refill while the destination is delivered and settled.
	backlogPollMessages = 240

	// backlogPollBudget is the per-destination budget the destination is
	// consumed with. It is small enough that the prefetch pause fires several
	// times over the backlog, which is the condition the ratio below measures.
	backlogPollBudget = 4

	// backlogRequeueTimeout bounds each receive of the requeue test on its own,
	// so a stall names the delivery it stalled on rather than the test.
	backlogRequeueTimeout = 10 * time.Second
)

// backlogBufferCounter counts the records franz-go buffers for the client it is
// hooked onto. A record buffered here is one the client holds and the driver has
// not taken, which is exactly what a topic pause discards: the ratio of this
// count to the records delivered is what tells the difference between a poll
// loop that drains each fetch and one that leaves the read-ahead behind.
type backlogBufferCounter struct {
	mu       sync.Mutex
	buffered int
}

// OnFetchRecordBuffered implements kgo.HookFetchRecordBuffered.
func (c *backlogBufferCounter) OnFetchRecordBuffered(*kgo.Record) {
	c.mu.Lock()
	c.buffered++
	c.mu.Unlock()
}

func (c *backlogBufferCounter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buffered
}

// TestBacklogPollDrainsEachFetchIntoPending holds the poll loop to draining a
// fetch it has taken. Every record of a backlog is delivered exactly once, and
// the records franz-go buffered stay close to the records delivered.
//
// The two are not equal by construction. A poll that takes one record at a time
// leaves the rest of the response buffered in the client, where the prefetch
// pause discards it: franz-go strips a paused topic's buffered records and
// rewinds the cursor, so they are fetched and buffered again. The backlog is
// then read once per prefetch pause rather than once, and the ratio below grows
// to the size of a fetch response per budget. Draining the fetch into the
// driver's own pending list leaves the pause nothing to strip, so the ratio
// stays at about one however many times the pause fires.
func TestBacklogPollDrainsEachFetchIntoPending(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	destination := kafkaTestTopic(t, "backlog-poll")
	group := kafkaTestTopic(t, "backlog-poll-group")
	cleanupKafkaTopics(t, admin, destination)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, destination, 1)
	if _, err := connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination}},
		Effective:    connection.Capabilities(),
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}

	producer, err := connection.Producer(ctx, driver.ProducerConfig{Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(context.Background()) })

	// The backlog is published before the consumer exists, and the consumer
	// starts at the earliest offset, so the whole backlog is what it drains.
	outbound := make([]driver.OutboundMessage, 0, backlogPollMessages)
	for index := range backlogPollMessages {
		outbound = append(outbound, driver.OutboundMessage{
			Destination: destination,
			Body:        fmt.Appendf(nil, "backlog-poll-%d", index),
		})
	}
	if err := producer.Publish(ctx, outbound...); err != nil {
		t.Fatalf("Publish backlog: %v", err)
	}

	// The hook goes onto the connection's options before the consumer is built,
	// and the consumer is the only client that fetches: consumerClientOpts
	// copies these options first, so the counter sees every record franz-go
	// buffers for the subscription under test.
	counter := &backlogBufferCounter{}
	connection.clientOpts = append(connection.clientOpts, kgo.WithHooks(counter))

	value, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group:          group,
		Destinations:   []string{destination},
		Prefetch:       backlogPollBudget,
		PerDestination: map[string]int{destination: backlogPollBudget},
		Effective:      connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer closeKafkaConsumer(value)

	deadline := kafkaNow().Add(2 * time.Minute)
	delivered := make(map[string]int, backlogPollMessages)
	for range backlogPollMessages {
		message := receiveKafkaMessageBefore(t, value, deadline)
		delivered[string(message.Body)]++
		if err := message.Settle.Ack(ctx); err != nil {
			t.Fatalf("Ack(%q): %v", message.Body, err)
		}
	}

	for index := range backlogPollMessages {
		body := string(fmt.Appendf(nil, "backlog-poll-%d", index))
		if got := delivered[body]; got != 1 {
			t.Fatalf("body %q delivered %d times, want exactly once", body, got)
		}
	}
	if len(delivered) != backlogPollMessages {
		t.Fatalf("distinct bodies delivered = %d, want %d", len(delivered), backlogPollMessages)
	}

	buffered := counter.count()
	if buffered > 2*backlogPollMessages {
		t.Fatalf("franz-go buffered %d records for a backlog of %d delivered, a ratio of %.1fx; "+
			"the poll loop is leaving fetched records in the client for the prefetch pause to strip",
			buffered, backlogPollMessages, float64(buffered)/float64(backlogPollMessages))
	}
	t.Logf("buffered=%d delivered=%d ratio=%.2fx", buffered, backlogPollMessages,
		float64(buffered)/float64(backlogPollMessages))
}

// receiveBacklogDelivery receives one delivery within its own timeout, naming
// the delivery it was waiting for when the wait runs out. A stall then reports
// which step of the requeue it stalled on.
func receiveBacklogDelivery(t *testing.T, value driver.Consumer, waitingFor string) driver.InboundMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), backlogRequeueTimeout)
	defer cancel()
	select {
	case message, ok := <-value.Messages():
		if !ok {
			t.Fatalf("waiting for %s: Messages closed", waitingFor)
		}
		return message
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", waitingFor, ctx.Err())
		return driver.InboundMessage{}
	}
}

// TestBacklogPollRequeueBeatsAPendingSuccessor holds a requeue to reaching the
// broker while the successor it has to overtake is already in the driver's
// pending list. Both records are published under one key before the consumer
// exists, so the first fetch carries both and the drain puts the successor in
// pending before the first delivery.
//
// The budget is one, so the successor cannot be delivered until the requeued
// record settles. Refusing it sets the prefetch pause unless a requeue is
// pending, and pausing here forbids the rewound fetch the requeue is waiting
// for: the successor waits for the slot, the slot waits for the redelivery, and
// the redelivery waits for a fetch the pause forbids. The redelivery then never
// arrives and this test reports which delivery it stalled on.
//
// The rewind fetches both records again, so a copy of the successor reaches the
// driver twice. The quiet window at the end is the duplicate check: the ack
// tracker drops a copy whose offset is already outstanding, so the handler sees
// the successor once.
func TestBacklogPollRequeueBeatsAPendingSuccessor(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	destination := kafkaTestTopic(t, "backlog-requeue")
	group := kafkaTestTopic(t, "backlog-requeue-group")
	cleanupKafkaTopics(t, admin, destination)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, destination, 1)
	if _, err := connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination}},
		Effective:    connection.Capabilities(),
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}

	// The ack is required so both records are on the broker before the consumer
	// is built: a publish that returns earlier could leave the first fetch
	// carrying only the first record, which is the case this test exists to
	// distinguish from.
	producer, err := connection.Producer(ctx, driver.ProducerConfig{
		RequireDurableAck: true,
		Effective:         connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(context.Background()) })

	key := []byte("backlog-requeue-key")
	if err := producer.Publish(ctx,
		driver.OutboundMessage{Destination: destination, Key: key, Body: []byte("one")},
		driver.OutboundMessage{Destination: destination, Key: key, Body: []byte("two")},
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	value, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group:          group,
		Destinations:   []string{destination},
		Prefetch:       1,
		PerDestination: map[string]int{destination: 1},
		Effective:      connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer closeKafkaConsumer(value)

	first := receiveBacklogDelivery(t, value, `the first delivery, "one"`)
	if string(first.Body) != "one" {
		t.Fatalf("first delivery body = %q, waiting for %q", first.Body, "one")
	}
	if err := first.Settle.Nack(ctx, driver.NackOptions{Requeue: true}); err != nil {
		t.Fatalf("Nack(requeue): %v", err)
	}

	redelivery := `the redelivery of "one", ahead of the successor already in pending`
	redelivered := receiveBacklogDelivery(t, value, redelivery)
	if string(redelivered.Body) != "one" {
		t.Fatalf("redelivery body = %q, waiting for %q", redelivered.Body, "one")
	}
	if err := redelivered.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(redelivery): %v", err)
	}

	successor := receiveBacklogDelivery(t, value, `the successor "two"`)
	if string(successor.Body) != "two" {
		t.Fatalf("successor body = %q, waiting for %q", successor.Body, "two")
	}
	if err := successor.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack(successor): %v", err)
	}

	expectNoKafkaMessage(t, value, 300*time.Millisecond)
}
