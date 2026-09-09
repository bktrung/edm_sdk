package kafka

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func openKafkaAdminTest(t *testing.T) (context.Context, *conn, *kadm.Client) {
	t.Helper()
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	opened, err := (Driver{}).Open(ctx, driver.Config{
		Endpoints: []string{kafkaEndpoint},
		ClientID:  "f1-kafka-admin-test",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	connection, ok := opened.(*conn)
	if !ok {
		t.Fatalf("Open() returned %T, want *conn", opened)
	}
	t.Cleanup(func() { _ = connection.Close(context.Background()) })
	return ctx, connection, kadm.NewClient(connection.client)
}

func kafkaTestTopic(t *testing.T, label string) string {
	t.Helper()
	suffix := strings.ReplaceAll(t.Name(), "/", "-")
	if label == "" {
		label = suffix
	}
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		t.Fatalf("generate unique topic suffix: %v", err)
	}
	return fmt.Sprintf("f1-kafka-%s-%s-%s", label, suffix, hex.EncodeToString(random))
}

func cleanupKafkaTopics(t *testing.T, admin *kadm.Client, names ...string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		responses, err := admin.DeleteTopics(ctx, names...)
		if err != nil {
			t.Errorf("DeleteTopics cleanup: %v", err)
			return
		}
		for _, name := range names {
			response, ok := responses[name]
			if ok && response.Err != nil && !errors.Is(response.Err, kerr.UnknownTopicOrPartition) {
				t.Errorf("DeleteTopics cleanup %q: %v", name, response.Err)
			}
		}
	})
}

const (
	kafkaTopicVisibilityTimeout = 5 * time.Second
	kafkaTopicVisibilityPoll    = 10 * time.Millisecond
)

type kafkaTopicVisibilityProbe func(context.Context, *kadm.Client, string) (bool, error)

var kafkaTopicVisibilityCheck kafkaTopicVisibilityProbe = kafkaTopicVisibleFromBroker

func kafkaTopicVisibleFromBroker(ctx context.Context, admin *kadm.Client, name string) (bool, error) {
	details, err := admin.ListTopics(kadm.WithAuthorizedOps(ctx), name)
	if err != nil {
		return false, err
	}
	detail, ok := details[name]
	return ok && detail.Err == nil && len(detail.Partitions) > 0, nil
}

func waitKafkaTopicVisible(t *testing.T, ctx context.Context, admin *kadm.Client, name string) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, kafkaTopicVisibilityTimeout)
	defer cancel()
	ticker := time.NewTicker(kafkaTopicVisibilityPoll) //nolint:forbidigo // broker visibility polling needs a bounded wall-clock retry
	defer ticker.Stop()
	for {
		visible, err := kafkaTopicVisibilityCheck(waitCtx, admin, name)
		if err != nil {
			t.Fatalf("ListTopics(%q) while waiting for visibility: %v", name, err)
		}
		if visible {
			return
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("topic %q did not become visible within %s: %v", name, kafkaTopicVisibilityTimeout, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func createKafkaTopic(t *testing.T, admin *kadm.Client, ctx context.Context, name string, partitions int32) {
	t.Helper()
	response, err := admin.CreateTopic(ctx, partitions, -1, nil, name)
	if err != nil {
		t.Fatalf("CreateTopic(%q): %v", name, err)
	}
	if response.Err != nil {
		t.Fatalf("CreateTopic(%q) response: %v", name, response.Err)
	}
	waitKafkaTopicVisible(t, ctx, admin, name)
}

func produceKafkaRecords(t *testing.T, client *kgo.Client, ctx context.Context, topic string, count int) {
	t.Helper()
	records := make([]*kgo.Record, count)
	for i := range records {
		records[i] = &kgo.Record{Topic: topic, Value: fmt.Appendf(nil, "record-%d", i)}
	}
	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatalf("ProduceSync(%q): %v", topic, err)
	}
}

func namesContainAll(got []string, want ...string) bool {
	seen := make(map[string]struct{}, len(got))
	for _, name := range got {
		seen[name] = struct{}{}
	}
	for _, name := range want {
		if _, ok := seen[name]; !ok {
			return false
		}
	}
	return true
}

func TestCreateKafkaTopicWaitsForVisibility(t *testing.T) {
	ctx, _, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "visibility-wait")
	cleanupKafkaTopics(t, admin, topic)

	readyAt := time.Now().Add(2 * time.Second) //nolint:forbidigo // deterministic mutation keeps the forced state hidden
	originalCheck := kafkaTopicVisibilityCheck
	t.Cleanup(func() { kafkaTopicVisibilityCheck = originalCheck })
	kafkaTopicVisibilityCheck = func(ctx context.Context, admin *kadm.Client, name string) (bool, error) {
		now := time.Now() //nolint:forbidigo // deterministic mutation controls the forced state
		if now.Before(readyAt) {
			return false, nil
		}
		return kafkaTopicVisibleFromBroker(ctx, admin, name)
	}

	createKafkaTopic(t, admin, ctx, topic, 1)
	visible, err := kafkaTopicVisibilityCheck(ctx, admin, topic)
	if err != nil {
		t.Fatalf("topic visibility probe: %v", err)
	}
	if !visible {
		t.Fatalf("topic %q is not visible after createKafkaTopic returned", topic)
	}
}

func TestClassifyAdminErrorKinds(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want driver.Kind
	}{
		{name: "unknown topic", err: kerr.UnknownTopicOrPartition, want: driver.KindNotFound},
		{name: "invalid topic", err: kerr.InvalidTopicException, want: driver.KindFatal},
		{name: "invalid partitions", err: kerr.InvalidPartitions, want: driver.KindFatal},
		{name: "invalid replication factor", err: kerr.InvalidReplicationFactor, want: driver.KindFatal},
		{name: "invalid config", err: kerr.InvalidConfig, want: driver.KindFatal},
		{name: "unmapped leader unavailable", err: kerr.LeaderNotAvailable, want: driver.KindTransient},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotErr := classifyAdminError("test", tc.err)
			var classified *driver.Error
			if !errors.As(gotErr, &classified) {
				t.Fatalf("classifyAdminError(%v) = %T, want *driver.Error", tc.err, gotErr)
			}
			if got := classified.Kind(); got != tc.want {
				t.Fatalf("classifyAdminError(%v).Kind() = %s, want %s", tc.err, got, tc.want)
			}
		})
	}
}

func TestClassifyAdminErrorOffsetLookupMissingDestination(t *testing.T) {
	cases := []struct {
		name     string
		kafkaErr error
	}{
		{
			name:     "unknown topic or partition",
			kafkaErr: kerr.UnknownTopicOrPartition,
		},
		{
			name:     "unknown topic id",
			kafkaErr: kerr.UnknownTopicID,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outerErr := fmt.Errorf("list offsets: %w", tc.kafkaErr)
			err := classifyAdminError("describe_topology", outerErr)
			if !errors.Is(err, driver.ErrDestinationMissing) {
				t.Fatalf("classifyAdminError error = %v, want ErrDestinationMissing", err)
			}
			if !errors.Is(err, tc.kafkaErr) {
				t.Fatalf("classifyAdminError error = %v, want Kafka error %v", err, tc.kafkaErr)
			}
			kind, ok := driver.Classify(err)
			if !ok || kind != driver.KindNotFound {
				t.Fatalf("classifyAdminError classification = (%v, %t), want (%v, true)", kind, ok, driver.KindNotFound)
			}
		})
	}
}

func TestEnsureTopologyCreatesAndIsIdempotent(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	first := kafkaTestTopic(t, "create-first")
	second := kafkaTestTopic(t, "create-second")
	cleanupKafkaTopics(t, admin, first, second)
	spec := driver.TopologySpec{
		Destinations: []driver.DestinationSpec{
			{Name: first},
			{Name: second},
		},
		Effective: connection.Capabilities(),
	}

	firstDiff, err := connection.Admin().EnsureTopology(ctx, spec)
	if err != nil {
		t.Fatalf("first EnsureTopology: %v", err)
	}
	if !namesContainAll(firstDiff.CreatedDestinations, first, second) || len(firstDiff.CreatedDestinations) != 2 {
		t.Fatalf("first CreatedDestinations = %v, want exactly %q and %q", firstDiff.CreatedDestinations, first, second)
	}
	if len(firstDiff.ExistingDestinations) != 0 {
		t.Fatalf("first ExistingDestinations = %v, want empty", firstDiff.ExistingDestinations)
	}

	secondDiff, err := connection.Admin().EnsureTopology(ctx, spec)
	if err != nil {
		t.Fatalf("second EnsureTopology: %v", err)
	}
	if len(secondDiff.CreatedDestinations) != 0 {
		t.Fatalf("second CreatedDestinations = %v, want empty", secondDiff.CreatedDestinations)
	}
	if !namesContainAll(secondDiff.ExistingDestinations, first, second) || len(secondDiff.ExistingDestinations) != 2 {
		t.Fatalf("second ExistingDestinations = %v, want exactly %q and %q", secondDiff.ExistingDestinations, first, second)
	}
}

func TestEnsureTopologyPolicies(t *testing.T) {
	ctx, connection, kafkaAdmin := openKafkaAdminTest(t)
	existing := kafkaTestTopic(t, "policy-existing")
	verifyMissing := kafkaTestTopic(t, "policy-verify-missing")
	noneMissing := kafkaTestTopic(t, "policy-none-missing")
	cleanupKafkaTopics(t, kafkaAdmin, existing, verifyMissing, noneMissing)
	createKafkaTopic(t, kafkaAdmin, ctx, existing, 1)
	produceKafkaRecords(t, connection.client, ctx, existing, 1)

	verifyDiff, err := connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Policy:       driver.TopologyVerify,
		Destinations: []driver.DestinationSpec{{Name: existing}},
		Effective:    connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("TopologyVerify existing: %v", err)
	}
	if len(verifyDiff.CreatedDestinations) != 0 || len(verifyDiff.ExistingDestinations) != 1 || verifyDiff.ExistingDestinations[0] != existing {
		t.Fatalf("TopologyVerify existing diff = %#v, want only existing %q", verifyDiff, existing)
	}

	_, err = connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Policy:       driver.TopologyVerify,
		Destinations: []driver.DestinationSpec{{Name: verifyMissing}},
		Effective:    connection.Capabilities(),
	})
	if err == nil {
		t.Error("TopologyVerify missing returned nil error")
	} else if !errors.Is(err, driver.ErrDestinationMissing) {
		t.Fatalf("TopologyVerify missing error = %v, want ErrDestinationMissing", err)
	}
	verifyDetails, err := kafkaAdmin.ListTopics(kadm.WithAuthorizedOps(ctx), verifyMissing)
	if err != nil {
		t.Fatalf("ListTopics(%q) after verify: %v", verifyMissing, err)
	}
	if detail, ok := verifyDetails[verifyMissing]; ok && detail.Err == nil {
		t.Fatalf("TopologyVerify created missing topic %q", verifyMissing)
	}

	noneSpec := driver.TopologySpec{
		Policy:       driver.TopologyNone,
		Destinations: []driver.DestinationSpec{{Name: noneMissing}},
		Effective:    connection.Capabilities(),
	}
	noneDiff, err := (&admin{}).EnsureTopology(context.Background(), noneSpec)
	if err != nil {
		t.Fatalf("TopologyNone without client: %v", err)
	}
	if len(noneDiff.CreatedExchanges) != 0 ||
		len(noneDiff.CreatedDestinations) != 0 ||
		len(noneDiff.CreatedBindings) != 0 ||
		len(noneDiff.ExistingExchanges) != 0 ||
		len(noneDiff.ExistingDestinations) != 0 ||
		len(noneDiff.ExistingBindings) != 0 ||
		len(noneDiff.Orphaned) != 0 ||
		noneDiff.OrphanScanError != "" ||
		len(noneDiff.Drifted) != 0 {
		t.Fatalf("TopologyNone without client diff = %#v, want zero diff", noneDiff)
	}

	noneDiff, err = connection.Admin().EnsureTopology(ctx, noneSpec)
	if err != nil {
		t.Fatalf("TopologyNone: %v", err)
	}
	if len(noneDiff.CreatedDestinations) != 0 || len(noneDiff.ExistingDestinations) != 0 {
		t.Fatalf("TopologyNone diff = %#v, want zero diff", noneDiff)
	}
	noneDetails, err := kafkaAdmin.ListTopics(kadm.WithAuthorizedOps(ctx), noneMissing)
	if err != nil {
		t.Fatalf("ListTopics(%q) after none: %v", noneMissing, err)
	}
	if detail, ok := noneDetails[noneMissing]; ok && detail.Err == nil {
		t.Fatalf("TopologyNone created missing topic %q", noneMissing)
	}
}

func TestEnsureTopologyOrphanScan(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	prefix := kafkaTestTopic(t, "orphan-scope")
	orphan := prefix + "-orphan"
	outside := kafkaTestTopic(t, "outside-scope")
	cleanupKafkaTopics(t, admin, orphan, outside)
	createKafkaTopic(t, admin, ctx, orphan, 1)
	createKafkaTopic(t, admin, ctx, outside, 1)
	produceKafkaRecords(t, connection.client, ctx, orphan, 3)

	diff, err := connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Scope:     []string{prefix},
		Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("EnsureTopology scoped orphan scan: %v", err)
	}
	if diff.OrphanScanError != "" {
		t.Fatalf("scoped OrphanScanError = %q, want empty", diff.OrphanScanError)
	}
	if len(diff.Orphaned) != 1 || diff.Orphaned[0].Name != orphan || diff.Orphaned[0].Messages != 3 {
		t.Fatalf("scoped Orphaned = %#v, want %q with depth 3", diff.Orphaned, orphan)
	}

	emptyScope, err := connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("EnsureTopology empty orphan scope: %v", err)
	}
	if len(emptyScope.Orphaned) != 0 || emptyScope.OrphanScanError == "" {
		t.Fatalf("empty-scope result = %#v, want empty Orphaned and non-empty OrphanScanError", emptyScope)
	}
}

func TestPruneRefusesConnectionConsumer(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	topic := kafkaTestTopic(t, "prune-consumer")
	cleanupKafkaTopics(t, admin, topic)
	createKafkaTopic(t, admin, ctx, topic, 1)

	consumer, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Destinations: []string{topic},
		Prefetch:     1,
		Effective:    connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Stop(ctx) })

	maintenance, ok := connection.Admin().(driver.Maintenance)
	if !ok {
		t.Fatalf("Admin does not implement driver.Maintenance")
	}
	results, err := maintenance.Prune(ctx, []string{topic})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Prune results = %v, want 1", results)
	}
	result := results[0]
	if result.Deleted || result.Reason == "" {
		t.Fatalf("Prune(%q) = %+v, want refused with a reason", topic, result)
	}
}
