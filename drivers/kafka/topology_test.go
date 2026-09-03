package kafka

import (
	"errors"
	"testing"

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

func TestReplicationFactorIsBrokerDefault(t *testing.T) {
	for _, partitions := range []int{0, 1, 4} {
		got := translateDestination(driver.DestinationSpec{Partitions: partitions}).replicationFactor
		if got != -1 {
			t.Errorf("translateDestination(%d).replicationFactor = %d, want -1", partitions, got)
		}
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

	state, err := connection.Admin().DescribeTopology(ctx, []string{destination, missing})
	if err != nil {
		t.Fatalf("DescribeTopology before truncation: %v", err)
	}
	if got, ok := state.Depth[destination]; !ok || got != 5 {
		t.Fatalf("Depth[%q] = %d, present=%t, want 5", destination, got, ok)
	}
	if _, ok := state.Depth[missing]; ok {
		t.Fatalf("Depth contains missing destination %q, want it absent", missing)
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
