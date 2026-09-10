package kafka

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestLaneBalancerCoverage(t *testing.T) {
	members := laneTestMembers("member-0", "member-1", "member-2")
	lanes := map[string]int32{
		"lane-a": 3,
		"lane-b": 6,
		"lane-c": 12,
		"low":    4,
	}
	assignments := laneAssignments(t, members, lanes)
	for _, lane := range slices.Sorted(maps.Keys(lanes)) {
		partitions := lanes[lane]
		counts := make([]int, 0, len(members))
		for _, member := range members {
			got := assignments[member.MemberID][lane]
			if len(got) == 0 {
				t.Fatalf("member %q holds no partitions for lane %q (wanted %d partitions)", member.MemberID, lane, partitions)
			}
			counts = append(counts, len(got))
		}
		want := make([]int, len(members))
		base := int(partitions) / len(members)
		for memberIndex := range want {
			want[memberIndex] = base
		}
		for remainderIndex := range int(partitions) % len(members) {
			want[remainderIndex]++
		}
		slices.Sort(counts)
		slices.Sort(want)
		if !slices.Equal(counts, want) {
			t.Fatalf("lane %q counts = %v, want exact counts %v", lane, counts, want)
		}
	}

	// Lane-aware assignment computes each lane from that lane alone, so
	// balancing a lane by itself must reproduce its slice of the multi-lane
	// plan. A flat cursor carries its position across lanes and cannot
	// satisfy this; neither can an offset taken from the plan's topic set
	// rather than from the lane.
	independentLanes := map[string]int32{
		"lane-a": 4,
		"lane-b": 6,
		"lane-c": 7,
		"low":    8,
	}
	independentAssignments := laneAssignments(t, members, independentLanes)
	for _, lane := range slices.Sorted(maps.Keys(independentLanes)) {
		alone := laneAssignments(t, members, map[string]int32{lane: independentLanes[lane]})
		for _, member := range members {
			got := alone[member.MemberID][lane]
			want := independentAssignments[member.MemberID][lane]
			if !slices.Equal(got, want) {
				t.Fatalf("lane %q member %q partitions = %v balanced alone, want %v from the multi-lane plan; a lane's assignment must not depend on which other lanes are present", lane, member.MemberID, got, want)
			}
		}
	}

	balancedLanes := map[string]int32{
		"lane-a": 4,
		"lane-b": 4,
		"lane-c": 4,
		"low":    4,
	}
	balancedAssignments := laneAssignments(t, members, balancedLanes)

	repeatedAssignments := laneAssignments(t, members, balancedLanes)
	for _, member := range members {
		for lane := range balancedLanes {
			got := balancedAssignments[member.MemberID][lane]
			repeated := repeatedAssignments[member.MemberID][lane]
			if !slices.Equal(got, repeated) {
				t.Fatalf("lane %q member %q changed across runs: %v then %v", lane, member.MemberID, got, repeated)
			}
		}
	}
	totals := make([]int, len(members))
	for memberIndex, member := range members {
		for lane := range balancedLanes {
			totals[memberIndex] += len(balancedAssignments[member.MemberID][lane])
		}
	}
	minTotal, maxTotal := slices.Min(totals), slices.Max(totals)
	if maxTotal-minTotal > 1 {
		maxMember := 0
		for memberIndex := range totals {
			if totals[memberIndex] > totals[maxMember] {
				maxMember = memberIndex
			}
		}
		t.Fatalf("member %q total=%d, totals=%v; want cross-lane totals within one", members[maxMember].MemberID, maxTotal, totals)
	}
}

func TestLaneBalancerUnderpartitionedLaneLeavesMemberUncovered(t *testing.T) {
	members := laneTestMembers("member-0", "member-1", "member-2")
	const lane = "low"
	assignments := laneAssignments(t, members, map[string]int32{lane: 2})

	for _, member := range members {
		if len(assignments[member.MemberID][lane]) == 0 {
			t.Logf("member %q holds no partitions for lane %q", member.MemberID, lane)
			return
		}
	}
	t.Fatalf("member %q holds a partition for lane %q; expected one member to be uncovered", members[len(members)-1].MemberID, lane)
}

func TestLaneBalancerPartitionFloor(t *testing.T) {
	const (
		topic     = "low"
		instances = "3"
	)
	admin := &admin{conn: &conn{driverOptions: map[string]string{
		"kafka.maxExpectedInstances": instances,
	}}}
	_, err := admin.EnsureTopology(context.Background(), driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: topic, Partitions: 2}},
	})
	if err == nil || !strings.Contains(err.Error(), topic) || !strings.Contains(err.Error(), "maxExpectedInstances") {
		t.Fatalf("EnsureTopology underpartitioned lane error = %v, want topic and maxExpectedInstances", err)
	}
	var classified *driver.Error
	if !errors.As(err, &classified) || classified.Kind() != driver.KindFatal {
		t.Fatalf("EnsureTopology underpartitioned lane error = %T/%v, want fatal classified error", err, err)
	}

	if got, err := resolveMaxExpectedInstances(nil); err != nil || got != 0 {
		t.Fatalf("resolveMaxExpectedInstances(nil) = %d/%v, want 0/nil", got, err)
	}
}

func TestConsumerClientOptsUsesConfiguredBalancer(t *testing.T) {
	connection := &conn{
		clientOpts: []kgo.Opt{kgo.SeedBrokers("localhost:19092")},
		balancer:   kgo.RangeBalancer(),
	}
	options, err := consumerClientOpts(connection, driver.ConsumerConfig{Destinations: []string{"topic"}}, "group", nil)
	if err != nil {
		t.Fatalf("consumerClientOpts error = %v", err)
	}
	client, err := kgo.NewClient(options...)
	if err != nil {
		t.Fatalf("NewClient error = %v", err)
	}
	defer client.Close()

	values := client.OptValues(kgo.Balancers)
	if len(values) != 1 {
		t.Fatalf("kgo.Balancers values = %v, want one configured value", values)
	}
	balancers, ok := values[0].([]kgo.GroupBalancer)
	if !ok || len(balancers) != 1 || balancers[0].ProtocolName() != "range" {
		t.Fatalf("kgo.Balancers value = %#v, want range balancer", values[0])
	}
}

func laneTestMembers(ids ...string) []kmsg.JoinGroupResponseMember {
	metadata := kmsg.NewConsumerMemberMetadata()
	metadata.Topics = []string{"lane-a", "lane-b", "lane-c", "low"}
	encoded := metadata.AppendTo(nil)
	members := make([]kmsg.JoinGroupResponseMember, 0, len(ids))
	for _, id := range ids {
		members = append(members, kmsg.JoinGroupResponseMember{
			MemberID:         id,
			ProtocolMetadata: append([]byte(nil), encoded...),
		})
	}
	return members
}

func laneAssignments(t *testing.T, members []kmsg.JoinGroupResponseMember, topics map[string]int32) map[string]map[string][]int32 {
	t.Helper()
	balancer := &laneBalancer{}
	memberBalancer, _, err := balancer.MemberBalancer(members)
	if err != nil {
		t.Fatalf("MemberBalancer error = %v", err)
	}
	withError, ok := memberBalancer.(kgo.GroupMemberBalancerOrError)
	if !ok {
		t.Fatalf("MemberBalancer returned %T, want GroupMemberBalancerOrError", memberBalancer)
	}
	assignment, err := withError.BalanceOrError(topics)
	if err != nil {
		t.Fatalf("BalanceOrError error = %v", err)
	}
	plan, ok := assignment.(*kgo.BalancePlan)
	if !ok {
		t.Fatalf("BalanceOrError returned %T, want *kgo.BalancePlan", assignment)
	}
	return plan.AsMemberIDMap()
}
