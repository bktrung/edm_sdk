package kafka

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// The lane shape below is the one a real subscription declares, not a fixture
// shape invented here. Two core helpers name the destinations, and their
// formats are what this file reproduces:
//
//	publishEntryPoint   "f1.<env>.<topic>.<priority>"                          (publisher.go)
//	retryDestinationFor "f1.<env>.<topic>.<subscription>.<priority>.retry.<tier>" (worker.go)
//
// The number of tiers comes from the default ladder rather than from a choice
// here: defaultSubscription (config.go) ships Retry.MaxAttempts 4 with an empty
// Tiers list, and retryTiers (config.go) turns that into MaxAttempts-1, so three.
// The branch declares three priorities, not four (priority.go; Valid rejects
// anything else), so one topic of a default subscription carries twelve lanes.
const (
	compareEnv          = "test"
	compareTopic        = "orders.created"
	compareSubscription = "orders"
	compareTiers        = 3
)

// comparePriorities are the priorities of the default ladder, in the order
// Priority.String spells them.
var comparePriorities = []string{"high", "medium", "low"}

// compareNewcomerGeneration is the generation a member that has never synced
// advertises. franz-go hands a first join an empty current assignment and the
// zero group generation minus one.
const compareNewcomerGeneration = int32(-1)

// compareRoundBound is how many rebalance rounds a cooperative group is allowed
// before this harness calls the plan unstable. A cooperative rebalance takes
// two rounds, and the ceiling exists so a plan that oscillates fails the run
// instead of looping.
const compareRoundBound = 6

// compareLanes returns one lane topic per priority and per retry tier, each
// with the given partition count, all of them in one subscription's group.
func compareLanes(partitions int32) map[string]int32 {
	lanes := make(map[string]int32, len(comparePriorities)*(1+compareTiers))
	for _, priority := range comparePriorities {
		lanes[fmt.Sprintf("f1.%s.%s.%s", compareEnv, compareTopic, priority)] = partitions
		for tier := 1; tier <= compareTiers; tier++ {
			lanes[fmt.Sprintf("f1.%s.%s.%s.%s.retry.%d", compareEnv, compareTopic, compareSubscription, priority, tier)] = partitions
		}
	}
	return lanes
}

// compareShortName drops the prefix every lane of the shape above shares, so a
// printed line names the priority and tier a lane is rather than the whole
// destination.
func compareShortName(lane string) string {
	return strings.TrimPrefix(lane, fmt.Sprintf("f1.%s.%s.", compareEnv, compareTopic))
}

// compareMemberSpec is one member of a rebalance: the generation it last
// completed and the partitions it claims from then.
type compareMemberSpec struct {
	id         string
	generation int32
	owned      map[string][]int32
}

// compareMembers builds a member list through the stock balancer's own join
// side, so the metadata it carries is the metadata a real rebalance carries.
func compareMembers(t *testing.T, balancer kgo.GroupBalancer, lanes []string, specs []compareMemberSpec) []kmsg.JoinGroupResponseMember {
	t.Helper()
	members := make([]kmsg.JoinGroupResponseMember, 0, len(specs))
	for _, spec := range specs {
		members = append(members, kmsg.JoinGroupResponseMember{
			MemberID:         spec.id,
			ProtocolMetadata: balancer.JoinGroupMetadata(lanes, spec.owned, spec.generation),
		})
	}
	return members
}

// comparePlan drives one balancer through a member list and returns the plan it
// produces. A cooperative balancer has already applied its own final adjustment
// by the time it returns: the client calls AdjustCooperative inside Balance, so
// what arrives here is the assignment a leader would send, in-flight partitions
// included as unassigned.
func comparePlan(t *testing.T, balancer kgo.GroupBalancer, members []kmsg.JoinGroupResponseMember, lanes map[string]int32) map[string]map[string][]int32 {
	t.Helper()
	memberBalancer, _, err := balancer.MemberBalancer(members)
	if err != nil {
		t.Fatalf("MemberBalancer error = %v", err)
	}
	withError, ok := memberBalancer.(kgo.GroupMemberBalancerOrError)
	if !ok {
		t.Fatalf("MemberBalancer returned %T, want GroupMemberBalancerOrError", memberBalancer)
	}
	assignment, err := withError.BalanceOrError(lanes)
	if err != nil {
		t.Fatalf("BalanceOrError error = %v", err)
	}
	plan, ok := assignment.(*kgo.BalancePlan)
	if !ok {
		t.Fatalf("BalanceOrError returned %T, want *kgo.BalancePlan", assignment)
	}
	return plan.AsMemberIDMap()
}

// compareSteadyPlan plans one balancer for a member set that holds nothing:
// the assignment a group settles into, for the coverage table.
func compareSteadyPlan(t *testing.T, balancer kgo.GroupBalancer, lanes map[string]int32, ids ...string) map[string]map[string][]int32 {
	t.Helper()
	specs := make([]compareMemberSpec, 0, len(ids))
	for _, id := range ids {
		specs = append(specs, compareMemberSpec{id: id, generation: compareNewcomerGeneration})
	}
	return comparePlan(t, balancer, compareMembers(t, balancer, slices.Sorted(maps.Keys(lanes)), specs), lanes)
}

// compareRebalance is the round loop a cooperative group runs: the plan of one
// round is what members own at the next one, and the loop ends at the first
// round that leaves no partition unassigned, because a round that leaves one is
// the round that exists only so the member holding it revokes before the next.
// It returns the settled assignment, the number of rounds that took, and how
// many partitions the first round left assigned to nobody, which is the handover
// a cooperative group does in two rounds rather than one.
//
// The claims the loop advertises are the ones franz-go advertises: the previous
// round's assignment at the current generation for members that were in the
// group, and nothing at all for a member that has just joined.
func compareRebalance(t *testing.T, balancer kgo.GroupBalancer, lanes map[string]int32, prior map[string]map[string][]int32, newcomer string, ids []string) (final map[string]map[string][]int32, rounds, pending int) {
	t.Helper()
	names := slices.Sorted(maps.Keys(lanes))
	claims := prior
	var previous map[string]map[string][]int32
	for round := 1; round <= compareRoundBound; round++ {
		specs := make([]compareMemberSpec, 0, len(ids))
		for _, id := range ids {
			spec := compareMemberSpec{id: id, generation: laneGeneration, owned: claims[id]}
			if id == newcomer {
				spec.generation = compareNewcomerGeneration
				spec.owned = nil
			}
			specs = append(specs, spec)
		}
		plan := comparePlan(t, balancer, compareMembers(t, balancer, names, specs), lanes)
		inFlight := compareUnassigned(plan, lanes)
		if round == 1 {
			pending = inFlight
		}
		if inFlight == 0 {
			return plan, round, pending
		}
		if previous != nil && compareSamePlan(plan, previous) {
			t.Fatalf("the plan repeated itself with %d partitions unassigned for members %v", inFlight, ids)
		}
		previous, claims = plan, plan
	}
	t.Fatalf("the plan did not settle within %d rounds for members %v", compareRoundBound, ids)
	return nil, 0, 0
}

// compareSamePlan reports whether two assignments agree member by member, lane
// by lane and partition by partition.
func compareSamePlan(left, right map[string]map[string][]int32) bool {
	if len(left) != len(right) {
		return false
	}
	for member, lanes := range left {
		other, ok := right[member]
		if !ok || len(lanes) != len(other) {
			return false
		}
		for lane, partitions := range lanes {
			if !slices.Equal(partitions, other[lane]) {
				return false
			}
		}
	}
	return true
}

// compareUnassigned counts partitions a plan assigns to no member.
func compareUnassigned(plan map[string]map[string][]int32, lanes map[string]int32) int {
	held := make(map[string]map[int32]struct{}, len(lanes))
	for lane := range lanes {
		held[lane] = make(map[int32]struct{})
	}
	for _, assignments := range plan {
		for lane, partitions := range assignments {
			for _, partition := range partitions {
				held[lane][partition] = struct{}{}
			}
		}
	}
	unassigned := 0
	for lane, partitions := range lanes {
		unassigned += int(partitions) - len(held[lane])
	}
	return unassigned
}

// compareMoved counts the partitions whose owner differs between two
// assignments. A partition that is assigned to nobody in the later assignment
// counts as moved, because it is not where it was.
func compareMoved(before, after map[string]map[string][]int32, lanes map[string]int32) int {
	owners := func(assignment map[string]map[string][]int32) map[string]map[int32]string {
		owned := make(map[string]map[int32]string, len(lanes))
		for lane := range lanes {
			owned[lane] = make(map[int32]string)
		}
		for member, assignments := range assignment {
			for lane, partitions := range assignments {
				for _, partition := range partitions {
					owned[lane][partition] = member
				}
			}
		}
		return owned
	}
	beforeOwners, afterOwners := owners(before), owners(after)
	moved := 0
	for lane, partitions := range lanes {
		for partition := range partitions {
			if beforeOwners[lane][partition] != afterOwners[lane][partition] {
				moved++
			}
		}
	}
	return moved
}

// compareTotals reports the smallest and largest number of partitions a member
// holds across all lanes. The lane balancer decides each lane on its own, so its
// per-lane counts are even while its totals need not be; the stock balancer
// balances totals and lets a lane's counts differ.
func compareTotals(lanes map[string]int32, assignment map[string]map[string][]int32, ids []string) (least, most int) {
	totals := make([]int, 0, len(ids))
	for _, id := range ids {
		total := 0
		for lane := range lanes {
			total += len(assignment[id][lane])
		}
		totals = append(totals, total)
	}
	return slices.Min(totals), slices.Max(totals)
}

// compareCoverage reports how many (member, lane) pairs hold no partition at
// all, and the widest per-lane spread: the largest difference between the
// member holding most of a lane and the member holding fewest, over every lane.
func compareCoverage(lanes map[string]int32, assignment map[string]map[string][]int32, ids []string) (zeroPairs, maxSpread int, spreadLane string) {
	for _, lane := range slices.Sorted(maps.Keys(lanes)) {
		counts := make([]int, 0, len(ids))
		for _, id := range ids {
			count := len(assignment[id][lane])
			if count == 0 {
				zeroPairs++
			}
			counts = append(counts, count)
		}
		spread := slices.Max(counts) - slices.Min(counts)
		if spread > maxSpread {
			maxSpread, spreadLane = spread, compareShortName(lane)
		}
	}
	return zeroPairs, maxSpread, spreadLane
}

// compareExactlyOnce fails unless every partition of every lane is held by
// exactly one member. It is the one fact this file asserts, and it holds for
// both balancers: a plan that drops a partition or hands it to two members is
// wrong whatever the strategy.
func compareExactlyOnce(t *testing.T, label string, lanes map[string]int32, assignment map[string]map[string][]int32, ids []string) {
	t.Helper()
	owners := make(map[string]map[int32]int)
	for _, id := range ids {
		for lane, partitions := range assignment[id] {
			for _, partition := range partitions {
				if owners[lane] == nil {
					owners[lane] = make(map[int32]int)
				}
				owners[lane][partition]++
			}
		}
	}
	for _, lane := range slices.Sorted(maps.Keys(lanes)) {
		for partition := range lanes[lane] {
			switch owners[lane][partition] {
			case 1:
			case 0:
				t.Errorf("%s: lane %s partition %d is assigned to nobody", label, compareShortName(lane), partition)
			default:
				t.Errorf("%s: lane %s partition %d is assigned to %d members", label, compareShortName(lane), partition, owners[lane][partition])
			}
		}
	}
}

// TestLaneCoverageComparison drives both balancers through the same member sets
// and the same lane shapes and prints what each one assigns.
//
// It is a measurement that prints rather than an assertion of an answer: the
// properties it asserts are that a plan assigns every partition exactly once,
// which both strategies owe, and that each column is driven by the balancer it
// claims. The grid is the members 2, 3 and 4 crossed with 3, 4 and 16
// partitions per lane, and every cell is measured for the settled plan, for one
// member joining and for one member leaving. Two more cells answer a question
// the grid cannot: the two lanes the live comparison declares, with two members
// and with three. Two partitions per lane is in the grid because the live
// three-member run at that count is where the lane balancer left a member with
// no partition of any lane at all.
func TestLaneCoverageComparison(t *testing.T) {
	lane := kgo.GroupBalancer(&laneBalancer{})
	sticky := kgo.CooperativeStickyBalancer()

	for _, members := range []int{2, 3, 4} {
		for _, partitions := range []int32{2, 3, 4, 16} {
			compareCell(t, lane, sticky, compareLanes(partitions), members, partitions)
		}
	}
	// The two priority lanes the live comparison declares, at the partition
	// count it gives them, and the same shape with a third member.
	compareCell(t, lane, sticky, compareLiveLanes(4), 2, 4)
	compareCell(t, lane, sticky, compareLiveLanes(4), 3, 4)
	// The underpartitioned live shape exactly: two lanes of two partitions and
	// three members, where the three-member live run left one member with no
	// partition of either lane.
	compareCell(t, lane, sticky, compareLiveLanes(2), 3, 2)
	// The columns are checked after the table rather than before it, so a run
	// driven by the wrong balancer still prints the measurement its reader
	// needs to see, next to the reason it does not count.
	if got := lane.ProtocolName(); got != "lane" {
		t.Errorf("the lane column is driven by protocol %q, want lane", got)
	}
	if got := sticky.ProtocolName(); got != "cooperative-sticky" {
		t.Errorf("the stock column is driven by protocol %q, want cooperative-sticky", got)
	}
}

// TestWholeLaneOwnershipComparison measures the state StickyBalancer's own doc
// comment starts from: two members that each hold a whole topic, a third topic
// free, and a third member gone, which is also the state a generation skews
// into when its members arrived one at a time against a live backlog. The
// comment's answer leaves one member with none of the other's topic; this cell
// asks whether the balancer still does that, and what the lane balancer does
// with the same state.
func TestWholeLaneOwnershipComparison(t *testing.T) {
	lane := kgo.GroupBalancer(&laneBalancer{})
	sticky := kgo.CooperativeStickyBalancer()
	lanes := map[string]int32{"t0": 3, "t1": 3, "t2": 3}
	ids := []string{"member-0", "member-1"}
	prior := map[string]map[string][]int32{
		"member-0": {"t0": {0, 1, 2}},
		"member-1": {"t1": {0, 1, 2}},
	}

	stockPlan, rounds, pending := compareRebalance(t, sticky, lanes, prior, "", ids)
	compareExactlyOnce(t, "cooperative-sticky whole lanes", lanes, stockPlan, ids)
	stockZero, stockSpread, stockLane := compareCoverage(lanes, stockPlan, ids)
	stockLeast, stockMost := compareTotals(lanes, stockPlan, ids)
	t.Logf("plan balancer=cooperative-sticky lanes=%d partitions=3 members=%d zeroPairs=%d maxSpread=%d spreadLane=%s totals=%d:%d rounds=%d pending=%d assignment=%s",
		len(lanes), len(ids), stockZero, stockSpread, stockLane, stockLeast, stockMost, rounds, pending, comparePlanLine(lanes, stockPlan, ids))

	lanePlan := laneAssignments(t, laneMembersClaiming(t, lanes, prior, ids...), lanes)
	compareExactlyOnce(t, "lane whole lanes", lanes, lanePlan, ids)
	laneZero, laneSpread, laneSpreadLane := compareCoverage(lanes, lanePlan, ids)
	laneLeast, laneMost := compareTotals(lanes, lanePlan, ids)
	t.Logf("plan balancer=lane lanes=%d partitions=3 members=%d zeroPairs=%d maxSpread=%d spreadLane=%s totals=%d:%d rounds=1 pending=0 assignment=%s",
		len(lanes), len(ids), laneZero, laneSpread, laneSpreadLane, laneLeast, laneMost, comparePlanLine(lanes, lanePlan, ids))

	if got := lane.ProtocolName(); got != "lane" {
		t.Errorf("the lane column is driven by protocol %q, want lane", got)
	}
	if got := sticky.ProtocolName(); got != "cooperative-sticky" {
		t.Errorf("the stock column is driven by protocol %q, want cooperative-sticky", got)
	}
}

// comparePlanLine renders an assignment as member, lane and partitions, so a
// cell whose lanes are few enough to read can print what it assigned.
func comparePlanLine(lanes map[string]int32, assignment map[string]map[string][]int32, ids []string) string {
	line := ""
	for _, id := range ids {
		line += id + "{"
		for _, lane := range slices.Sorted(maps.Keys(lanes)) {
			if partitions := assignment[id][lane]; len(partitions) > 0 {
				line += fmt.Sprintf("%s%v", compareShortName(lane), partitions)
			}
		}
		line += "} "
	}
	return line
}

// compareLiveLanes is the lane set the live priority comparison declares: the
// high and low lanes of one topic, and nothing else.
func compareLiveLanes(partitions int32) map[string]int32 {
	return map[string]int32{
		fmt.Sprintf("f1.%s.%s.high", compareEnv, compareTopic): partitions,
		fmt.Sprintf("f1.%s.%s.low", compareEnv, compareTopic):  partitions,
	}
}

// compareCell measures one (member set, lane set) cell for both balancers: the
// settled plan each produces, and the partitions each moves when one member
// joins the settled set and when one member leaves it.
func compareCell(t *testing.T, lane kgo.GroupBalancer, sticky kgo.GroupBalancer, lanes map[string]int32, members int, partitions int32) {
	t.Helper()
	ids := compareMemberIDs(members)

	laneSteady := laneAssignments(t, laneMembersFromSpecs(t, compareLaneSpecs(lanes, ids)), lanes)
	stickySteady := compareSteadyPlan(t, sticky, lanes, ids...)

	for _, measured := range []struct {
		name       string
		assignment map[string]map[string][]int32
	}{
		{"lane", laneSteady},
		{"cooperative-sticky", stickySteady},
	} {
		label := fmt.Sprintf("%s lanes=%d partitions=%d members=%d", measured.name, len(lanes), partitions, members)
		compareExactlyOnce(t, label, lanes, measured.assignment, ids)
		zeroPairs, maxSpread, spreadLane := compareCoverage(lanes, measured.assignment, ids)
		least, most := compareTotals(lanes, measured.assignment, ids)
		t.Logf("plan balancer=%s lanes=%d partitions=%d members=%d zeroPairs=%d maxSpread=%d spreadLane=%s totals=%d:%d",
			measured.name, len(lanes), partitions, members, zeroPairs, maxSpread, spreadLane, least, most)
	}

	// One member joins the settled set, then the first member leaves it. The
	// stock balancer plans both changes itself, in as many rounds as it takes;
	// the lane balancer plans them in the single eager round its own helpers
	// give it.
	joinIDs := compareMemberIDs(members + 1)
	newcomer := joinIDs[len(joinIDs)-1]
	survivors := ids[1:]

	stockJoin, stockJoinRounds, stockJoinPending := compareRebalance(t, sticky, lanes, stickySteady, newcomer, joinIDs)
	compareExactlyOnce(t, fmt.Sprintf("stock lanes=%d partitions=%d members=%d joined", len(lanes), partitions, members+1), lanes, stockJoin, joinIDs)
	t.Logf("moves balancer=%s lanes=%d partitions=%d members=%d phase=join moved=%d rounds=%d pending=%d",
		"cooperative-sticky", len(lanes), partitions, members, compareMoved(stickySteady, stockJoin, lanes), stockJoinRounds, stockJoinPending)

	stockLeave, stockLeaveRounds, stockLeavePending := compareRebalance(t, sticky, lanes, compareRestrict(stickySteady, survivors), "", survivors)
	compareExactlyOnce(t, fmt.Sprintf("stock lanes=%d partitions=%d members=%d left", len(lanes), partitions, members-1), lanes, stockLeave, survivors)
	t.Logf("moves balancer=%s lanes=%d partitions=%d members=%d phase=leave moved=%d rounds=%d pending=%d",
		"cooperative-sticky", len(lanes), partitions, members, compareMoved(stickySteady, stockLeave, lanes), stockLeaveRounds, stockLeavePending)

	laneJoinMembers := append(
		laneMembersClaiming(t, lanes, laneSteady, ids...),
		laneMembersFromSpecs(t, []laneMemberSpec{{
			id:         newcomer,
			generation: compareNewcomerGeneration,
			topics:     slices.Sorted(maps.Keys(lanes)),
		}})...,
	)
	laneJoin := laneAssignments(t, laneJoinMembers, lanes)
	compareExactlyOnce(t, fmt.Sprintf("lane lanes=%d partitions=%d members=%d joined", len(lanes), partitions, members+1), lanes, laneJoin, joinIDs)
	t.Logf("moves balancer=%s lanes=%d partitions=%d members=%d phase=join moved=%d rounds=1 pending=0",
		"lane", len(lanes), partitions, members, compareMoved(laneSteady, laneJoin, lanes))

	laneLeave := laneAssignments(t, laneMembersClaiming(t, lanes, laneSteady, survivors...), lanes)
	compareExactlyOnce(t, fmt.Sprintf("lane lanes=%d partitions=%d members=%d left", len(lanes), partitions, members-1), lanes, laneLeave, survivors)
	t.Logf("moves balancer=%s lanes=%d partitions=%d members=%d phase=leave moved=%d rounds=1 pending=0",
		"lane", len(lanes), partitions, members, compareMoved(laneSteady, laneLeave, lanes))
}

// compareMemberIDs names the members of a group of the given size.
func compareMemberIDs(count int) []string {
	ids := make([]string, 0, count)
	for index := range count {
		ids = append(ids, fmt.Sprintf("member-%d", index))
	}
	return ids
}

// compareLaneSpecs builds the lane balancer's own member specs for members that
// hold nothing, which is the state its next plan starts from.
func compareLaneSpecs(lanes map[string]int32, ids []string) []laneMemberSpec {
	specs := make([]laneMemberSpec, 0, len(ids))
	for _, id := range ids {
		specs = append(specs, laneMemberSpec{
			id:         id,
			generation: compareNewcomerGeneration,
			topics:     slices.Sorted(maps.Keys(lanes)),
		})
	}
	return specs
}

// compareRestrict keeps only the members of an assignment that are named, which
// is the state a rebalance starts from when a member leaves without having
// given anything up.
func compareRestrict(assignment map[string]map[string][]int32, ids []string) map[string]map[string][]int32 {
	kept := make(map[string]map[string][]int32, len(ids))
	for _, id := range ids {
		kept[id] = assignment[id]
	}
	return kept
}
