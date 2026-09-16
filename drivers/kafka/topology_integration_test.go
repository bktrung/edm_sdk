//go:build integration

package kafka

import (
	"context"
	"errors"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestPartitionCountResolution(t *testing.T) {
	cases := []struct {
		name       string
		partitions int
		want       int32
	}{
		{name: "unset", want: -1},
		{name: "zero", partitions: 0, want: -1},
		{name: "negative", partitions: -3, want: -1},
		{name: "positive", partitions: 7, want: 7},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := translateDestination(driver.DestinationSpec{Partitions: tc.partitions}).partitions
			if got != tc.want {
				t.Fatalf("translateDestination(%d).partitions = %d, want %d", tc.partitions, got, tc.want)
			}
		})
	}
}

func TestPartitionCountRejectsOutOfRange(t *testing.T) {
	const maxKafkaPartitions = int(math.MaxInt32)
	if got := translateDestination(driver.DestinationSpec{Partitions: maxKafkaPartitions}).partitions; got != int32(maxKafkaPartitions) {
		t.Fatalf("max Kafka partition count translated to %d, want %d", got, maxKafkaPartitions)
	}

	const destination = "out-of-range"
	for _, partitions := range []int{maxKafkaPartitions + 1, 4294967297} {
		t.Run(strconv.Itoa(partitions), func(t *testing.T) {
			_, err := (&admin{}).EnsureTopology(context.Background(), driver.TopologySpec{
				Destinations: []driver.DestinationSpec{{Name: destination, Partitions: partitions}},
			})
			if err == nil {
				t.Fatalf("EnsureTopology(%d) returned nil error", partitions)
			}
			var classified *driver.Error
			if !errors.As(err, &classified) {
				t.Fatalf("EnsureTopology(%d) error = %T, want *driver.Error", partitions, err)
			}
			if got := classified.Kind(); got != driver.KindFatal {
				t.Fatalf("EnsureTopology(%d).Kind() = %s, want %s", partitions, got, driver.KindFatal)
			}
			if !strings.Contains(err.Error(), destination) {
				t.Fatalf("EnsureTopology(%d) error = %q, want destination %q", partitions, err, destination)
			}
		})
	}
}

func TestReplicationFactorIsBrokerDefault(t *testing.T) {
	for _, partitions := range []int{0, 1, 4} {
		got := translateDestination(driver.DestinationSpec{Partitions: partitions}).replicationFactor
		if got != -1 {
			t.Errorf("translateDestination(%d).replicationFactor = %d, want -1", partitions, got)
		}
	}
}

func TestEnsureTopologyWithoutPartitionFloorCreatesRequestedPartitions(t *testing.T) {
	ctx, connection, kafkaAdmin := openKafkaAdminTest(t)
	destination := kafkaTestTopic(t, "floor-disabled")
	cleanupKafkaTopics(t, kafkaAdmin, destination)

	diff, err := connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Partitions: 2}},
		Effective:    connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("EnsureTopology without maxExpectedInstances: %v", err)
	}
	if !slices.Contains(diff.CreatedDestinations, destination) {
		t.Fatalf("CreatedDestinations = %v, want %q", diff.CreatedDestinations, destination)
	}

	details, err := kafkaAdmin.ListTopics(ctx, destination)
	if err != nil {
		t.Fatalf("ListTopics(%q): %v", destination, err)
	}
	detail, ok := details[destination]
	if !ok || detail.Err != nil {
		t.Fatalf("ListTopics(%q) detail = %#v, want an existing topic", destination, detail)
	}
	if got := len(detail.Partitions); got != 2 {
		t.Fatalf("topic %q partition count = %d, want 2", destination, got)
	}
}

func TestEnsureTopologyPartitionFloorAppliesToUnsetAndExistingTopics(t *testing.T) {
	ctx, connection, kafkaAdmin := openKafkaAdminTest(t)
	created := kafkaTestTopic(t, "floor-default")
	existing := kafkaTestTopic(t, "floor-existing")
	cleanupKafkaTopics(t, kafkaAdmin, created, existing)
	connection.driverOptions = map[string]string{"kafka.maxExpectedInstances": "3"}

	diff, err := connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: created}},
		Effective:    connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("EnsureTopology unset partitions with floor: %v", err)
	}
	if !slices.Contains(diff.CreatedDestinations, created) {
		t.Fatalf("CreatedDestinations = %v, want %q", diff.CreatedDestinations, created)
	}
	details, err := kafkaAdmin.ListTopics(ctx, created)
	if err != nil {
		t.Fatalf("ListTopics(%q): %v", created, err)
	}
	if got := len(details[created].Partitions); got != 3 {
		t.Fatalf("topic %q partition count = %d, want floor 3", created, got)
	}

	createKafkaTopic(t, kafkaAdmin, ctx, existing, 2)
	_, err = connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: existing}},
		Effective:    connection.Capabilities(),
	})
	if err == nil || !strings.Contains(err.Error(), existing) || !strings.Contains(err.Error(), "maxExpectedInstances") {
		t.Fatalf("EnsureTopology existing underpartitioned topic error = %v, want floor rejection", err)
	}
	var classified *driver.Error
	if !errors.As(err, &classified) || classified.Kind() != driver.KindFatal {
		t.Fatalf("EnsureTopology existing underpartitioned topic error = %T/%v, want fatal classified error", err, err)
	}
}

func TestBindingsAndExchangesAreIgnored(t *testing.T) {
	plan := translateTopology(driver.TopologySpec{
		Exchanges:    []driver.ExchangeSpec{{Name: "events", Kind: "fanout", Durable: true}},
		Destinations: []driver.DestinationSpec{{Name: "orders"}},
		Bindings:     []driver.BindingSpec{{Source: "events", Destination: "orders"}},
		Effective:    driver.Capabilities{Fanout: driver.FanoutAtConsume},
	})
	if len(plan.exchanges) != 0 {
		t.Fatalf("translated exchanges = %v, want empty", plan.exchanges)
	}
	if len(plan.bindings) != 0 {
		t.Fatalf("translated bindings = %v, want empty", plan.bindings)
	}
	if len(plan.destinations) != 1 || plan.destinations[0].name != "orders" {
		t.Fatalf("translated destinations = %#v, want orders", plan.destinations)
	}
}

func TestEnsureTopologyDrift(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	destination := kafkaTestTopic(t, "drift")
	cleanupKafkaTopics(t, admin, destination)
	createKafkaTopic(t, admin, ctx, destination, 1)
	produceKafkaRecords(t, connection.client, ctx, destination, 1)

	diff, err := connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Policy:       driver.TopologyVerify,
		Destinations: []driver.DestinationSpec{{Name: destination, Partitions: 3}},
		Effective:    connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("TopologyVerify drift: %v", err)
	}
	if len(diff.Drifted) != 1 {
		t.Fatalf("Drifted = %#v, want one entry", diff.Drifted)
	}
	drift := diff.Drifted[0]
	if drift.Name != destination || drift.Argument != "partitions" || drift.Want != "3" || drift.Got != "1" {
		t.Fatalf("Drifted[0] = %#v, want destination partitions 3 versus 1", drift)
	}

	clean, err := connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Policy:       driver.TopologyVerify,
		Destinations: []driver.DestinationSpec{{Name: destination}},
		Effective:    connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("TopologyVerify unset partitions: %v", err)
	}
	if len(clean.Drifted) != 0 {
		t.Fatalf("unset partitions Drifted = %#v, want empty", clean.Drifted)
	}
}

func TestDescribeTopologyDepth(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	destination := kafkaTestTopic(t, "depth")
	missing := kafkaTestTopic(t, "depth-missing")
	cleanupKafkaTopics(t, admin, destination, missing)
	createKafkaTopic(t, admin, ctx, destination, 1)
	produceKafkaRecords(t, connection.client, ctx, destination, 5)

	state, err := connection.Admin().DescribeTopology(ctx, []string{destination})
	if err != nil {
		t.Fatalf("DescribeTopology before truncation: %v", err)
	}
	if got, ok := state.Depth[destination]; !ok || got != 5 {
		t.Fatalf("Depth[%q] = %d, present=%t, want 5", destination, got, ok)
	}
	_, err = connection.Admin().DescribeTopology(ctx, []string{missing})
	if !errors.Is(err, driver.ErrDestinationMissing) {
		t.Fatalf("DescribeTopology(%q) error = %v, want ErrDestinationMissing", missing, err)
	}

	endOffsets, err := admin.ListEndOffsets(ctx, destination)
	if err != nil {
		t.Fatalf("ListEndOffsets(%q): %v", destination, err)
	}
	deleted, err := admin.DeleteRecords(ctx, endOffsets.Offsets())
	if err != nil {
		t.Fatalf("DeleteRecords(%q): %v", destination, err)
	}
	if err := deleted.Error(); err != nil && !errors.Is(err, kerr.UnknownTopicOrPartition) {
		t.Fatalf("DeleteRecords(%q) response: %v", destination, err)
	}

	state, err = connection.Admin().DescribeTopology(ctx, []string{destination})
	if err != nil {
		t.Fatalf("DescribeTopology after truncation: %v", err)
	}
	if got := state.Depth[destination]; got != 0 {
		t.Fatalf("Depth[%q] after truncation = %d, want 0", destination, got)
	}
}

func TestDescribeTopologyReportsMissingDestinationPure(t *testing.T) {
	_, err := evaluateTopicDepths([]string{"pure-missing"}, kadm.ListedOffsets{}, kadm.ListedOffsets{})
	if err == nil {
		t.Fatalf("evaluateTopicDepths missing destination returned nil error, want ErrDestinationMissing")
	}
	if !errors.Is(err, driver.ErrDestinationMissing) {
		t.Fatalf("evaluateTopicDepths missing destination error = %v, want ErrDestinationMissing", err)
	}
	if kind, ok := driver.Classify(err); !ok || kind != driver.KindNotFound {
		t.Fatalf("evaluateTopicDepths classification = (%v,%t), want not_found", kind, ok)
	}
}

func TestTopologyVerifyRejectsUnsupportedDeliveryLimit(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	destination := kafkaTestTopic(t, "verify-delivery-limit")
	cleanupKafkaTopics(t, admin, destination)
	createKafkaTopic(t, admin, ctx, destination, 1)

	_, err := connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Policy:       driver.TopologyVerify,
		Destinations: []driver.DestinationSpec{{Name: destination, DeliveryLimit: 5}},
		Effective:    connection.Capabilities(),
	})
	if err == nil {
		t.Fatalf("TopologyVerify with delivery limit returned clean diff, want unsupported error")
	}
	if !strings.Contains(err.Error(), "delivery limit") {
		t.Fatalf("TopologyVerify error = %v, want delivery limit mentioned", err)
	}
	if kind, ok := driver.Classify(err); !ok || kind != driver.KindFatal {
		t.Fatalf("TopologyVerify classification = (%v,%t), want fatal", kind, ok)
	}
}

// A deferred destination must survive TopologyVerify, and the successful call
// must teach the connection the lane's delay. What survives Verify is the
// produce path's read of it: the producer stamps a due time from the recorded
// delay on a publish that carries none of its own. So this publishes without
// DelayUntil and holds the record to the due time that stamp implies. A
// connection that never learned the delay stamps nothing, and the consumer here
// delivers the record at once instead of at the due time the destination
// declares.
func TestTopologyVerifyPopulatesDeferredDestinationDelay(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	destination := kafkaTestTopic(t, "verify-deferred")
	group := kafkaTestTopic(t, "verify-deferred-group")
	cleanupKafkaTopics(t, admin, destination)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, destination, 1)

	const delay = 2 * time.Second
	if _, err := connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Policy:       driver.TopologyVerify,
		Destinations: []driver.DestinationSpec{{Name: destination, Delay: delay}},
		Effective:    connection.Capabilities(),
	}); err != nil {
		t.Fatalf("TopologyVerify with a deferred destination returned %v, want no error", err)
	}

	producer, err := connection.Producer(ctx, driver.ProducerConfig{Effective: connection.Capabilities()})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(context.Background()) })
	consumer, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{destination}, Prefetch: 1,
		Delays:    map[string]time.Duration{destination: delay},
		Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() { closeKafkaConsumer(consumer) })

	// The publish carries no due time, so the only due time the record can have
	// is the one the producer stamps from the delay Verify recorded. The
	// producer reads its clock after this snapshot, which makes the stamped due
	// time at or after the one asserted below.
	publishedAt := kafkaNow()
	if err := producer.Publish(ctx, driver.OutboundMessage{
		Destination: destination, Body: []byte("deferred"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	due := publishedAt.Add(delay)
	message := receiveKafkaMessageBefore(t, consumer, due.Add(2*time.Second))
	if message.Destination != destination || message.ReceivedAt.Before(due) {
		t.Fatalf("deferred delivery = %+v, want destination %q received at or after %s", message, destination, due)
	}
	if err := message.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

func TestFanoutAtConsumeCreatedDestinations(t *testing.T) {
	ctx, connection, admin := openKafkaAdminTest(t)
	first := kafkaTestTopic(t, "fanout-first")
	second := kafkaTestTopic(t, "fanout-second")
	cleanupKafkaTopics(t, admin, first, second)

	diff, err := connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: first}, {Name: second}},
		Bindings:     []driver.BindingSpec{{Source: "events.exchange", Destination: first}},
		Effective:    driver.Capabilities{Fanout: driver.FanoutAtConsume},
	})
	if err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	if !slices.Contains(diff.CreatedDestinations, first) || !slices.Contains(diff.CreatedDestinations, second) || len(diff.CreatedDestinations) != 2 {
		t.Fatalf("fanout CreatedDestinations = %v, want exactly %q and %q", diff.CreatedDestinations, first, second)
	}
}
