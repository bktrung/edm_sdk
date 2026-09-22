//go:build integration

package kafka

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestKafkaBacklogReportsLagHeadAndPreservesGroup(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	destination := kafkaTestTopic(t, "backlog-head")
	empty := kafkaTestTopic(t, "backlog-empty")
	group := kafkaTestTopic(t, "backlog-head-group")
	cleanupKafkaTopics(t, admin, destination, empty)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, destination, 1)
	createKafkaTopic(t, admin, ctx, empty, 1)

	firstAt := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	records := []*kgo.Record{
		{Topic: destination, Value: []byte("first"), Timestamp: firstAt},
		{Topic: destination, Value: []byte("second"), Timestamp: firstAt.Add(time.Second)},
		{Topic: destination, Value: []byte("third"), Timestamp: firstAt.Add(2 * time.Second)},
	}
	if err := connection.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatalf("ProduceSync: %v", err)
	}

	value, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group:        group,
		Destinations: []string{destination, empty},
		Prefetch:     1,
		StartAt:      driver.StartEarliest,
		Effective:    connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	consumer := value.(*consumer)
	t.Cleanup(func() { _ = value.Release(context.Background()) })
	waitForKafkaConsumerState(t, consumer, "backlog consumer assignment", func() bool {
		consumer.mu.Lock()
		defer consumer.mu.Unlock()
		return len(consumer.owned) == 2
	})
	reader, ok := value.(driver.BacklogReader)
	if !ok {
		t.Fatal("Kafka consumer does not implement driver.BacklogReader")
	}

	beforeFirstProbe := snapshotKafkaGroup(t, admin, group)
	first, err := reader.Backlog(ctx)
	if err != nil {
		t.Fatalf("Backlog before commit: %v", err)
	}
	assertKafkaGroupSnapshotEqual(t, beforeFirstProbe, snapshotKafkaGroup(t, admin, group), "first Backlog")
	if got := first[destination].Lag; got != 3 {
		t.Fatalf("first backlog lag = %d, want 3", got)
	}
	if !first[destination].HeadEnqueuedAt.Equal(firstAt) {
		t.Fatalf("first backlog head = %v, want %v", first[destination].HeadEnqueuedAt, firstAt)
	}
	if first[destination].HeadSource != driver.EnqueueSourceProducer {
		t.Fatalf("first backlog source = %v, want producer", first[destination].HeadSource)
	}
	if first[empty].Lag != 0 || !first[empty].HeadEnqueuedAt.IsZero() || first[empty].HeadSource != driver.EnqueueSourceUnknown {
		t.Fatalf("empty backlog = %+v, want zero lag and unknown head", first[empty])
	}

	beforeSecondProbe := snapshotKafkaGroup(t, admin, group)
	second, err := reader.Backlog(ctx)
	if err != nil {
		t.Fatalf("Backlog without commit: %v", err)
	}
	assertKafkaGroupSnapshotEqual(t, beforeSecondProbe, snapshotKafkaGroup(t, admin, group), "second Backlog")
	if second[destination].Lag != 3 || !second[destination].HeadEnqueuedAt.Equal(firstAt) {
		t.Fatalf("second backlog = %+v, want lag 3 with head %v", second[destination], firstAt)
	}

	message := receiveKafkaMessage(t, value)
	if err := message.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack first record: %v", err)
	}
	beforeThirdProbe := snapshotKafkaGroup(t, admin, group)
	third, err := reader.Backlog(ctx)
	if err != nil {
		t.Fatalf("Backlog after commit: %v", err)
	}
	assertKafkaGroupSnapshotEqual(t, beforeThirdProbe, snapshotKafkaGroup(t, admin, group), "Backlog after commit")
	if third[destination].Lag != 2 {
		t.Fatalf("third backlog lag = %d, want 2", third[destination].Lag)
	}
	if !third[destination].HeadEnqueuedAt.Equal(firstAt.Add(time.Second)) {
		t.Fatalf("third backlog head = %v, want %v", third[destination].HeadEnqueuedAt, firstAt.Add(time.Second))
	}
	if third[destination].HeadSource != driver.EnqueueSourceProducer {
		t.Fatalf("third backlog source = %v, want producer", third[destination].HeadSource)
	}

	secondMessage := receiveKafkaMessage(t, value)
	if err := secondMessage.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack second record: %v", err)
	}
	thirdMessage := receiveKafkaMessage(t, value)
	if err := thirdMessage.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack third record: %v", err)
	}
	if err := value.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

type kafkaGroupSnapshot struct {
	state      string
	protocol   string
	members    []string
	generation int32
	offsets    map[string]map[int32]kadm.OffsetResponse
}

func snapshotKafkaGroup(t *testing.T, admin *kadm.Client, group string) kafkaGroupSnapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	described, err := admin.DescribeGroups(ctx, group)
	if err != nil {
		t.Fatalf("DescribeGroups(%q): %v", group, err)
	}
	detail, ok := described[group]
	if !ok {
		t.Fatalf("DescribeGroups(%q) returned no group", group)
	}
	if detail.Err != nil {
		t.Fatalf("DescribeGroups(%q) group error: %v", group, detail.Err)
	}
	members := make([]string, 0, len(detail.Members))
	for _, member := range detail.Members {
		instanceID := ""
		if member.InstanceID != nil {
			instanceID = *member.InstanceID
		}
		assignment := ""
		if consumerAssignment, ok := member.Assigned.AsConsumer(); ok {
			assignment = fmt.Sprintf("%v", consumerAssignment.Topics)
		}
		members = append(members, fmt.Sprintf("%s/%s/%s", member.MemberID, instanceID, assignment))
	}
	sort.Strings(members)
	offsets, err := admin.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("FetchOffsets(%q): %v", group, err)
	}
	metadata := kafkaGroupMetadata(t, admin, group)
	return kafkaGroupSnapshot{
		state:      detail.State,
		protocol:   detail.Protocol,
		members:    members,
		generation: metadata.Generation,
		offsets:    offsets,
	}
}

func assertKafkaGroupSnapshotEqual(t *testing.T, before, after kafkaGroupSnapshot, operation string) {
	t.Helper()
	if before.state != after.state || before.protocol != after.protocol {
		t.Fatalf("%s changed group state/protocol from %q/%q to %q/%q", operation, before.state, before.protocol, after.state, after.protocol)
	}
	if fmt.Sprint(before.members) != fmt.Sprint(after.members) {
		t.Fatalf("%s changed members from %v to %v", operation, before.members, after.members)
	}
	if before.generation != after.generation {
		t.Fatalf("%s changed generation from %d to %d", operation, before.generation, after.generation)
	}
	if fmt.Sprint(before.offsets) != fmt.Sprint(after.offsets) {
		t.Fatalf("%s changed committed offsets from %v to %v", operation, before.offsets, after.offsets)
	}
}
