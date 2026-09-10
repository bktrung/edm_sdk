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
	laneCountsExact(t, "first plan", members, lanes, assignments)

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

	// The property has to survive prior ownership too. A claim is per member
	// and per lane, so balancing one lane while every member holds the
	// multi-lane plan must still reproduce that lane's slice of it.
	claimingIndependent := laneMembersClaiming(t, independentLanes, independentAssignments, "member-0", "member-1", "member-2")
	claimingAssignments := laneAssignments(t, claimingIndependent, independentLanes)
	for _, lane := range slices.Sorted(maps.Keys(independentLanes)) {
		alone := laneAssignments(t, claimingIndependent, map[string]int32{lane: independentLanes[lane]})
		for _, member := range claimingIndependent {
			got := alone[member.MemberID][lane]
			want := claimingAssignments[member.MemberID][lane]
			if !slices.Equal(got, want) {
				t.Fatalf("lane %q member %q partitions = %v balanced alone while claiming, want %v from the multi-lane plan; prior ownership must not make a lane's assignment depend on which other lanes are present", lane, member.MemberID, got, want)
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
	laneTotalsWithinOne(t, "balanced lanes", members, balancedLanes, balancedAssignments)

	// A fourth member joins while the others still hold the balanced plan. The
	// count each lane hands out is the target, not what a member already
	// holds: the newcomer takes its share, no lane loses coverage, and the
	// totals stay within one.
	joined := append(
		laneMembersClaiming(t, balancedLanes, balancedAssignments, "member-0", "member-1", "member-2"),
		laneMembersFromSpecs(t, []laneMemberSpec{{
			id:         "member-3",
			generation: laneGeneration,
			topics:     slices.Sorted(maps.Keys(balancedLanes)),
		}})...,
	)
	joinedAssignments := laneAssignments(t, joined, balancedLanes)
	laneCountsExact(t, "member joined", joined, balancedLanes, joinedAssignments)
	laneTotalsWithinOne(t, "member joined", joined, balancedLanes, joinedAssignments)
}

// laneGeneration is the generation a group is at when its members rebalance
// while still holding the assignment of the previous generation.
const laneGeneration = int32(5)

func TestLaneBalancerRebalanceMovesOnlyDepartedPartitions(t *testing.T) {
	lanes := map[string]int32{
		"lane-a": 3,
		"lane-b": 6,
		"lane-c": 12,
		"low":    4,
	}
	ids := []string{"member-0", "member-1", "member-2"}
	first := laneAssignments(t, laneTestMembers(ids...), lanes)

	// An unchanged membership where every member already holds what the plan
	// gave it: nothing has a reason to move.
	unchanged := laneAssignments(t, laneMembersClaiming(t, lanes, first, ids...), lanes)
	for _, id := range ids {
		for lane := range lanes {
			if !slices.Equal(first[id][lane], unchanged[id][lane]) {
				t.Fatalf("unchanged membership: member %q lane %q = %v, want %v; a rebalance without a membership change must not move a partition", id, lane, unchanged[id][lane], first[id][lane])
			}
		}
	}

	// The member at the end of the group's member order leaves. Every survivor
	// still holds what the plan gave it, so only the departed member's
	// partitions are reassigned.
	const departed = "member-2"
	survivors := []string{"member-0", "member-1"}
	after := laneAssignments(t, laneMembersClaiming(t, lanes, first, survivors...), lanes)
	for _, id := range survivors {
		for lane := range lanes {
			held := first[id][lane]
			got := after[id][lane]
			for _, partition := range held {
				if !slices.Contains(got, partition) {
					t.Fatalf("member %q left: lane %q member %q = %v, want it to keep %v; only the departed member's partitions may move", departed, lane, id, got, held)
				}
			}
		}
	}
}

func TestLaneBalancerJoinedMemberTakesFromMembersOverTheirQuota(t *testing.T) {
	lanes := map[string]int32{
		"lane-a": 4,
		"lane-b": 6,
		"lane-c": 12,
		"low":    5,
	}
	ids := []string{"member-0", "member-1", "member-2"}
	first := laneAssignments(t, laneTestMembers(ids...), lanes)

	joined := append(
		laneMembersClaiming(t, lanes, first, ids...),
		laneMembersFromSpecs(t, []laneMemberSpec{{
			id:         "member-3",
			generation: laneGeneration,
			topics:     slices.Sorted(maps.Keys(lanes)),
		}})...,
	)
	after := laneAssignments(t, joined, lanes)
	laneCountsExact(t, "member joined", joined, lanes, after)

	// A member gives up a partition only when it holds more than the lane now
	// gives it, so no survivor loses a partition it is still entitled to.
	for _, id := range ids {
		for lane, partitions := range lanes {
			quota := int(partitions) / len(joined)
			kept := 0
			for _, partition := range first[id][lane] {
				if slices.Contains(after[id][lane], partition) {
					kept++
				}
			}
			if want := min(len(first[id][lane]), quota); kept < want {
				t.Fatalf("member %q joined: lane %q member %q kept %d of %v, want at least %d; a member may lose a partition only when it holds more than its lane quota", "member-3", lane, id, kept, first[id][lane], want)
			}
		}
	}
}

func TestLaneBalancerIgnoresStaleGenerationClaim(t *testing.T) {
	const staleGeneration = int32(3)
	lanes := map[string]int32{"lane-a": 4}
	topics := slices.Sorted(maps.Keys(lanes))
	members := laneMembersFromSpecs(t, []laneMemberSpec{
		// member-0 missed a rebalance and rejoined still claiming the
		// partitions it held back then, which member-1 holds now.
		{id: "member-0", generation: staleGeneration, topics: topics, owned: map[string][]int32{"lane-a": {0, 1}}},
		{id: "member-1", generation: laneGeneration, topics: topics, owned: map[string][]int32{"lane-a": {0, 1}}},
	})
	assignments := laneAssignments(t, members, lanes)

	if got := assignments["member-1"]["lane-a"]; !slices.Equal(got, []int32{0, 1}) {
		t.Fatalf("stale generation claim: member-1 holds %v for lane-a, want [0 1]; member-0's claim at generation %d must be ignored, not pinned", got, staleGeneration)
	}
	if got := assignments["member-0"]["lane-a"]; slices.Contains(got, 0) || slices.Contains(got, 1) {
		t.Fatalf("stale generation claim: member-0 holds %v for lane-a, want partitions it does not claim from generation %d", got, staleGeneration)
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

// laneMemberSpec is one member of a rebalance: the lanes it is interested in,
// the generation it last completed, and the partitions it claims from then.
type laneMemberSpec struct {
	id         string
	generation int32
	topics     []string
	owned      map[string][]int32
}

// laneMembersFromSpecs builds join group members through the balancer's own
// join side, so a test exercises the metadata a real rebalance carries instead
// of metadata the test wrote by hand.
func laneMembersFromSpecs(t *testing.T, specs []laneMemberSpec) []kmsg.JoinGroupResponseMember {
	t.Helper()
	balancer := &laneBalancer{}
	members := make([]kmsg.JoinGroupResponseMember, 0, len(specs))
	for _, spec := range specs {
		members = append(members, kmsg.JoinGroupResponseMember{
			MemberID:         spec.id,
			ProtocolMetadata: balancer.JoinGroupMetadata(spec.topics, spec.owned, spec.generation),
		})
	}
	return members
}

// laneMembersClaiming rebuilds a member list in which every member still holds
// what the given plan gave it, at the group's current generation.
func laneMembersClaiming(t *testing.T, lanes map[string]int32, assignment map[string]map[string][]int32, ids ...string) []kmsg.JoinGroupResponseMember {
	t.Helper()
	topics := slices.Sorted(maps.Keys(lanes))
	specs := make([]laneMemberSpec, 0, len(ids))
	for _, id := range ids {
		specs = append(specs, laneMemberSpec{
			id:         id,
			generation: laneGeneration,
			topics:     topics,
			owned:      assignment[id],
		})
	}
	return laneMembersFromSpecs(t, specs)
}

// laneCountsExact fails unless every member holds at least one partition of
// every lane and each lane's counts are exactly the base plus remainder split
// this balancer promises. Every lane needs at least as many partitions as there
// are members, or the coverage half of this is not a promise.
func laneCountsExact(t *testing.T, label string, members []kmsg.JoinGroupResponseMember, lanes map[string]int32, assignments map[string]map[string][]int32) {
	t.Helper()
	for _, lane := range slices.Sorted(maps.Keys(lanes)) {
		partitions := lanes[lane]
		if int(partitions) < len(members) {
			t.Fatalf("%s: lane %q has %d partitions for %d members; this assertion needs full coverage", label, lane, partitions, len(members))
		}
		counts := make([]int, 0, len(members))
		for _, member := range members {
			got := assignments[member.MemberID][lane]
			if len(got) == 0 {
				// Reported instead of fatal so one run shows every lane that
				// is wrong, the count comparison below included.
				t.Errorf("%s: member %q holds no partitions for lane %q (wanted %d partitions)", label, member.MemberID, lane, partitions)
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
			t.Errorf("%s: lane %q counts = %v, want exact counts %v", label, lane, counts, want)
		}
	}
}

// laneTotalsWithinOne fails unless every member holds the same number of
// partitions across all lanes, give or take one.
func laneTotalsWithinOne(t *testing.T, label string, members []kmsg.JoinGroupResponseMember, lanes map[string]int32, assignments map[string]map[string][]int32) {
	t.Helper()
	totals := make([]int, len(members))
	for memberIndex, member := range members {
		for lane := range lanes {
			totals[memberIndex] += len(assignments[member.MemberID][lane])
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
		t.Fatalf("%s: member %q total=%d, totals=%v; want cross-lane totals within one", label, members[maxMember].MemberID, maxTotal, totals)
	}
}
