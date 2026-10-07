//go:build integration

package kafka

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func cleanupKafkaGroups(t *testing.T, admin *kadm.Client, names ...string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		responses, err := admin.DeleteGroups(ctx, names...)
		if err != nil {
			t.Errorf("DeleteGroups cleanup: %v", err)
			return
		}
		for _, name := range names {
			response, ok := responses[name]
			if ok && response.Err != nil && !errors.Is(response.Err, kerr.GroupIDNotFound) {
				t.Errorf("DeleteGroups cleanup %q: %v", name, response.Err)
			}
		}
	})
}

func TestClassifyTopicDeletionDisabledAsFatal(t *testing.T) {
	err := classifyAdminError("prune", kerr.TopicDeletionDisabled)
	var classified *driver.Error
	if !errors.As(err, &classified) {
		t.Fatalf("classifyAdminError(%v) = %T, want *driver.Error", kerr.TopicDeletionDisabled, err)
	}
	if got := classified.Kind(); got != driver.KindFatal {
		t.Fatalf("classifyAdminError(%v).Kind() = %s, want %s", kerr.TopicDeletionDisabled, got, driver.KindFatal)
	}
}

func TestMaintenanceGateHonorsContext(t *testing.T) {
	_, connection, _ := openKafkaAdminTest(t)
	connection.maintenanceGate <- struct{}{}
	defer func() { <-connection.maintenanceGate }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := connection.acquireMaintenance(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("acquireMaintenance canceled error = %v, want context.Canceled", err)
	}
}

func startKafkaTestConsumer(t *testing.T, topic, group string) *kgo.Client {
	t.Helper()
	assigned := make(chan struct{}, 1)
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaEndpoint),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) {
			select {
			case assigned <- struct{}{}:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatalf("NewClient consumer: %v", err)
	}
	pollCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		_ = consumer.PollFetches(pollCtx)
	}()
	t.Cleanup(func() {
		cancel()
		<-pollDone
		consumer.Close()
	})

	select {
	case <-assigned:
	case <-pollCtx.Done():
		t.Fatalf("consumer was not assigned a partition: %v", pollCtx.Err())
	}
	return consumer
}

func TestPurgeEmptiesAndKeepsTopic(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "purge")
	cleanupKafkaTopics(t, admin, topic)
	createKafkaTopic(t, admin, ctx, topic, 1)
	produceKafkaRecords(t, connection.client, ctx, topic, 5)

	maintenance := connection.Admin().(driver.Maintenance)
	removed, err := maintenance.Purge(ctx, topic)
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if removed != 5 {
		t.Fatalf("Purge removed %d records, want 5", removed)
	}

	starts, err := admin.ListStartOffsets(ctx, topic)
	if err != nil {
		t.Fatalf("ListStartOffsets after Purge: %v", err)
	}
	ends, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatalf("ListEndOffsets after Purge: %v", err)
	}
	depth, present, err := topicDepth(topic, starts, ends)
	if err != nil {
		t.Fatalf("topicDepth after Purge: %v", err)
	}
	if !present || depth != 0 {
		t.Fatalf("topic after Purge present=%t depth=%d, want present and zero depth", present, depth)
	}

	removed, err = maintenance.Purge(ctx, topic)
	if err != nil {
		t.Fatalf("second Purge: %v", err)
	}
	if removed != 0 {
		t.Fatalf("second Purge removed %d records, want 0", removed)
	}
}

func TestConcurrentPurgeCountsRecordsOnce(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "purge-concurrent")
	cleanupKafkaTopics(t, admin, topic)
	createKafkaTopic(t, admin, ctx, topic, 1)
	produceKafkaRecords(t, connection.client, ctx, topic, 5)

	type purgeResult struct {
		removed int64
		err     error
	}
	results := make(chan purgeResult, 2)
	var group sync.WaitGroup
	for range 2 {
		group.Go(func() {
			removed, err := connection.Admin().(driver.Maintenance).Purge(ctx, topic)
			results <- purgeResult{removed: removed, err: err}
		})
	}
	group.Wait()
	close(results)

	var total int64
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent Purge: %v", result.err)
		}
		total += result.removed
	}
	if total != 5 {
		t.Fatalf("concurrent Purge removed %d records in total, want 5", total)
	}
}

func TestConcurrentPurgeAcrossConnectionsDoesNotShareGate(t *testing.T) {
	ctx1, connection1, admin1 := openKafkaAdminTest(t)
	ctx2, connection2, admin2 := openKafkaAdminTest(t)
	topic1 := kafkaTestTopic(t, "purge-separate-1")
	topic2 := kafkaTestTopic(t, "purge-separate-2")
	cleanupKafkaTopics(t, admin1, topic1)
	cleanupKafkaTopics(t, admin2, topic2)
	createKafkaTopic(t, admin1, ctx1, topic1, 1)
	createKafkaTopic(t, admin2, ctx2, topic2, 1)
	produceKafkaRecords(t, connection1.client, ctx1, topic1, 1)
	produceKafkaRecords(t, connection2.client, ctx2, topic2, 1)

	maintenance1 := connection1.Admin().(driver.Maintenance)
	maintenance2 := connection2.Admin().(driver.Maintenance)
	connection1.maintenanceGate <- struct{}{}
	releaseFirst := true
	defer func() {
		if releaseFirst {
			<-connection1.maintenanceGate
		}
	}()

	firstDone := make(chan error, 1)
	go func() {
		_, err := maintenance1.Purge(ctx1, topic1)
		firstDone <- err
	}()
	select {
	case err := <-firstDone:
		t.Fatalf("first Purge completed while its connection gate was held: %v", err)
	default:
	}

	secondCtx, cancel := context.WithTimeout(ctx2, 3*time.Second)
	defer cancel()
	secondDone := make(chan error, 1)
	go func() {
		_, err := maintenance2.Purge(secondCtx, topic2)
		secondDone <- err
	}()

	var secondErr error
	select {
	case secondErr = <-secondDone:
	case <-secondCtx.Done():
		secondErr = secondCtx.Err()
	}
	<-connection1.maintenanceGate
	releaseFirst = false

	if secondErr != nil {
		t.Fatalf("second Purge waited on the first connection gate: %v", secondErr)
	}
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first Purge: %v", err)
		}
	case <-ctx1.Done():
		t.Fatalf("first Purge did not finish after its gate was released: %v", ctx1.Err())
	}
}

func TestPruneRefusesNonEmptyTopic(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "prune-non-empty")
	cleanupKafkaTopics(t, admin, topic)
	createKafkaTopic(t, admin, ctx, topic, 1)
	produceKafkaRecords(t, connection.client, ctx, topic, 3)

	results, err := connection.Admin().(driver.Maintenance).Prune(ctx, []string{topic})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if len(results) != 1 || results[0].Deleted || !strings.Contains(results[0].Reason, "3") {
		t.Fatalf("Prune result = %#v, want refusal naming depth 3", results)
	}
	details, err := admin.ListTopics(kadm.WithAuthorizedOps(ctx), topic)
	if err != nil {
		t.Fatalf("ListTopics after refused Prune: %v", err)
	}
	if detail, ok := details[topic]; !ok || detail.Err != nil {
		t.Fatalf("topic %q after refused Prune = %#v, want present", topic, detail)
	}
}

func TestPruneRefusesAttachedConsumer(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "prune-attached")
	group := kafkaTestTopic(t, "group-attached")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 1)
	consumer := startKafkaTestConsumer(t, topic, group)

	results, err := connection.Admin().(driver.Maintenance).Prune(ctx, []string{topic})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if len(results) != 1 || results[0].Deleted || !strings.Contains(results[0].Reason, "consumer") {
		t.Fatalf("Prune result = %#v, want attached-consumer refusal", results)
	}
	consumer.Close()
	details, err := admin.ListTopics(kadm.WithAuthorizedOps(ctx), topic)
	if err != nil {
		t.Fatalf("ListTopics after refused Prune: %v", err)
	}
	if detail, ok := details[topic]; !ok || detail.Err != nil {
		t.Fatalf("topic %q after refused Prune = %#v, want present", topic, detail)
	}
}

func TestPruneDeletesAfterConsumerCloses(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "prune-closed")
	group := kafkaTestTopic(t, "group-closed")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 1)
	consumer := startKafkaTestConsumer(t, topic, group)
	consumer.Close()
	described, err := admin.DescribeGroups(ctx, group)
	if err != nil {
		t.Fatalf("DescribeGroups after consumer close: %v", err)
	}
	groupDetails, ok := described[group]
	if !ok || groupDetails.Err != nil || groupDetails.State != "Empty" || len(groupDetails.Members) != 0 {
		t.Fatalf("group %q after consumer close = %#v, want Empty with zero members", group, groupDetails)
	}

	results, err := connection.Admin().(driver.Maintenance).Prune(ctx, []string{topic})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if len(results) != 1 || !results[0].Deleted {
		t.Fatalf("Prune result = %#v, want deleted result", results)
	}

	details, err := admin.ListTopics(kadm.WithAuthorizedOps(ctx), topic)
	if err != nil {
		t.Fatalf("ListTopics after Prune: %v", err)
	}
	detail, ok := details[topic]
	switch {
	case !ok,
		errors.Is(detail.Err, kerr.UnknownTopicOrPartition),
		errors.Is(detail.Err, kerr.UnknownTopicID):
	case detail.Err != nil:
		t.Fatalf("ListTopics after Prune returned %q with error: %v", topic, detail.Err)
	default:
		t.Fatalf("topic %q remained visible after deleted Prune result", topic)
	}
}

func TestPruneReportsMissingTopic(t *testing.T) {
	ctx, connection, _ := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "prune-missing")

	results, err := connection.Admin().(driver.Maintenance).Prune(ctx, []string{topic})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if len(results) != 1 || results[0].Deleted || !strings.Contains(results[0].Reason, "does not exist") {
		t.Fatalf("Prune result = %#v, want missing-topic reason", results)
	}
}

func TestPurgeReportsMissingTopic(t *testing.T) {
	ctx, connection, _ := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "purge-missing")

	_, err := connection.Admin().(driver.Maintenance).Purge(ctx, topic)
	if err == nil ||
		!errors.Is(err, driver.ErrDestinationMissing) ||
		!errors.Is(err, kerr.UnknownTopicOrPartition) {
		t.Fatalf("Purge(%q) error = %v, want missing sentinel and Kafka cause", topic, err)
	}
}
