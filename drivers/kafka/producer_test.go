package kafka

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestPublishFailedIndexesUseRecordIdentity(t *testing.T) {
	t.Parallel()

	records := []*kgo.Record{
		{Topic: "first"},
		{Topic: "second"},
		{Topic: "third"},
	}
	cause := errors.New("record failed")
	results := kgo.ProduceResults{
		{Record: records[0]},
		{Record: records[2], Err: cause},
		{Record: records[1]},
	}

	failed, err := failedPublishIndexes(records, results)
	if err != nil {
		t.Fatalf("failedPublishIndexes() error = %v", err)
	}
	if len(failed) != 1 {
		t.Fatalf("failedPublishIndexes() = %#v, want one failed message", failed)
	}
	got, ok := failed[2]
	if !ok {
		t.Fatalf("failedPublishIndexes() = %#v, want failure at input index 2", failed)
	}
	if !errors.Is(got, cause) {
		t.Fatalf("failedPublishIndexes()[2] = %v, want cause %v", got, cause)
	}
	if _, ok := failed[1]; ok {
		t.Fatalf("failedPublishIndexes() = %#v, used result position instead of input index", failed)
	}
}

func TestFailedPublishIndexesRejectsUnaccountedResults(t *testing.T) {
	t.Parallel()

	records := []*kgo.Record{
		{Topic: "first"},
		{Topic: "second"},
	}
	tests := []struct {
		name      string
		results   kgo.ProduceResults
		wantError string
	}{
		{
			name: "unknown record with omitted results",
			results: kgo.ProduceResults{{
				Record: &kgo.Record{Topic: "unknown"},
				Err:    errors.New("record failed"),
			}},
		},
		{
			name:    "missing result",
			results: kgo.ProduceResults{{Record: records[0]}},
		},
		{
			name:    "duplicate result with omitted results",
			results: kgo.ProduceResults{{Record: records[0]}, {Record: records[0]}},
		},
		{
			name: "unknown record alongside complete results",
			results: kgo.ProduceResults{
				{Record: records[0]},
				{Record: records[1]},
				{Record: &kgo.Record{Topic: "ghost"}, Err: errors.New("record failed")},
			},
			wantError: "produce result references an unknown record",
		},
		{
			name: "duplicate result alongside complete results",
			results: kgo.ProduceResults{
				{Record: records[0]},
				{Record: records[0]},
				{Record: records[1]},
			},
			wantError: "duplicate produce result for input record",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			failed, err := failedPublishIndexes(records, tc.results)
			if err == nil {
				t.Fatalf("failedPublishIndexes() error = nil, want accounting error")
			}
			if tc.wantError != "" && !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("failedPublishIndexes() error = %v, want %q", err, tc.wantError)
			}
			if failed != nil {
				t.Fatalf("failedPublishIndexes() failed = %#v, want nil", failed)
			}
		})
	}
}

func TestPublishRejectsEntryPoint(t *testing.T) {
	t.Parallel()

	p := &producer{}
	err := p.Publish(context.Background(), driver.OutboundMessage{
		Destination: "topic",
		EntryPoint:  true,
	})
	if err == nil {
		t.Fatal("Publish() error = nil, want unsupported entry-point error")
	}
	kind, classified := driver.Classify(err)
	if !classified || kind != driver.KindFatal {
		t.Fatalf("Publish() classification = (%v, %t), want (fatal, true)", kind, classified)
	}
}

func TestOversizeClassifiesTooLarge(t *testing.T) {
	t.Parallel()

	wrapped := fmt.Errorf("client rejected record: %w", kerr.MessageTooLarge)
	err := classify("publish", kafkaErrorKind(wrapped), wrapped)
	kind, classified := driver.Classify(err)
	if !classified {
		t.Fatalf("driver.Classify(%v) classified = false, want true", err)
	}
	if kind != driver.KindTooLarge {
		t.Fatalf("driver.Classify(%v) kind = %v, want too_large", err, kind)
	}
}

func TestPublishDurableAck(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "producer-durable")
	cleanupKafkaTopics(t, admin, topic)
	createKafkaTopic(t, admin, ctx, topic, 3)

	producer, err := connection.Producer(ctx, driver.ProducerConfig{
		RequireDurableAck: true,
		Effective:         connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(context.Background()) })

	messages := []driver.OutboundMessage{
		{Destination: topic, Key: []byte("one"), Body: []byte("first")},
		{Destination: topic, Key: []byte("two"), Body: []byte("second")},
		{Destination: topic, Key: []byte("three"), Body: []byte("third")},
	}
	if err := producer.Publish(ctx, messages...); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	starts, err := admin.ListStartOffsets(ctx, topic)
	if err != nil {
		t.Fatalf("ListStartOffsets(%q): %v", topic, err)
	}
	ends, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatalf("ListEndOffsets(%q): %v", topic, err)
	}
	depth, present, err := topicDepth(topic, starts, ends)
	if err != nil {
		t.Fatalf("topicDepth(%q): %v", topic, err)
	}
	if !present {
		t.Fatalf("topicDepth(%q) present = false, want true", topic)
	}
	if depth != int64(len(messages)) {
		t.Fatalf("topicDepth(%q) = %d, want %d acknowledged records", topic, depth, len(messages))
	}
}

func TestPublishPartialFailureNamesInputIndex(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	validTopic := kafkaTestTopic(t, "producer-partial-valid")
	missingTopic := kafkaTestTopic(t, "producer-partial-missing")
	cleanupKafkaTopics(t, admin, validTopic, missingTopic)
	createKafkaTopic(t, admin, ctx, validTopic, 1)

	producer, err := connection.Producer(ctx, driver.ProducerConfig{Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(context.Background()) })

	err = producer.Publish(ctx,
		driver.OutboundMessage{Destination: validTopic, Body: []byte("accepted-before")},
		driver.OutboundMessage{Destination: missingTopic, Body: []byte("rejected")},
		driver.OutboundMessage{Destination: validTopic, Body: []byte("accepted-after")},
	)
	var publishErr *driver.PublishError
	if !errors.As(err, &publishErr) {
		t.Fatalf("Publish() error = %v, want *driver.PublishError", err)
	}
	if len(publishErr.Failed) != 1 {
		t.Fatalf("Publish() Failed = %#v, want exactly one failure", publishErr.Failed)
	}
	failed, ok := publishErr.Failed[1]
	if !ok {
		t.Fatalf("Publish() Failed = %#v, want missing topic at input index 1", publishErr.Failed)
	}
	kind, classified := driver.Classify(failed)
	if !classified || kind != driver.KindNotFound {
		t.Fatalf("failed[1] classification = (%v, %t), want (not_found, true)", kind, classified)
	}
	if !errors.Is(failed, driver.ErrDestinationMissing) {
		t.Fatalf("failed[1] = %v, want ErrDestinationMissing", failed)
	}
	if !errors.Is(failed, kerr.UnknownTopicOrPartition) {
		t.Fatalf("failed[1] = %v, want Kafka unknown-topic cause", failed)
	}
}

func TestPublishOversizeRejected(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "producer-oversize")
	cleanupKafkaTopics(t, admin, topic)

	responses, err := admin.CreateTopics(ctx, 1, -1, map[string]*string{
		"max.message.bytes": kadm.StringPtr("65536"),
	}, topic)
	if err != nil {
		t.Fatalf("CreateTopic(%q): %v", topic, err)
	}
	response, ok := responses[topic]
	if !ok {
		t.Fatalf("CreateTopic(%q) response missing", topic)
	}
	if response.Err != nil {
		t.Fatalf("CreateTopic(%q) response: %v", topic, response.Err)
	}

	producer, err := connection.Producer(ctx, driver.ProducerConfig{Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(context.Background()) })

	body := make([]byte, 96<<10)
	if _, err := rand.Read(body); err != nil {
		t.Fatalf("generate oversized body: %v", err)
	}
	err = producer.Publish(ctx, driver.OutboundMessage{Destination: topic, Body: body})
	kind, classified := driver.Classify(err)
	if !classified || kind != driver.KindTooLarge {
		t.Fatalf("Publish() classification = (%v, %t), want (too_large, true): %v", kind, classified, err)
	}

	starts, err := admin.ListStartOffsets(ctx, topic)
	if err != nil {
		t.Fatalf("ListStartOffsets(%q): %v", topic, err)
	}
	ends, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatalf("ListEndOffsets(%q): %v", topic, err)
	}
	depth, present, err := topicDepth(topic, starts, ends)
	if err != nil {
		t.Fatalf("topicDepth(%q): %v", topic, err)
	}
	if !present {
		t.Fatalf("topicDepth(%q) present = false, want true", topic)
	}
	if depth != 0 {
		t.Fatalf("topicDepth(%q) = %d, want no retained records after rejection", topic, depth)
	}
}
