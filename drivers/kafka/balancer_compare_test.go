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
//	publishEntryPoint   "f1.<env>.<topic>.<priority>"                             (publisher.go)
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

// compareGeneration is the generation a group is at when its members rebalance
// while still holding the assignment of the previous generation.
const compareGeneration = int32(5)

// compareRoundBound is how many rebalance rounds a cooperative group is allowed
// before this harness calls the plan unstable. A cooperative rebalance takes
// two rounds, and the ceiling exists so a plan that oscillates fails the run
// instead of looping.
const compareRoundBound = 6

// compareDefaultBalancer resolves the balancer the driver joins with when
// kafka.balancer is unset. Every cell in this file measures that balancer and no
// other: the question the file answers is what a group of this shape is assigned
// by the protocol this driver ships, and a cell driven by a balancer the driver
// would not choose answers a question nobody asked.
func compareDefaultBalancer(t *testing.T) kgo.GroupBalancer {
	t.Helper()
	balancer, err := resolveBalancer(nil)
	if err != nil {
		t.Fatalf("resolveBalancer(nil) error = %v, want the shipped default", err)
	}
	return balancer
}

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

// compareMembers builds a member list through the balancer's own join side, so
// the metadata it carries is the metadata a real rebalance carries.
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

// comparePlan drives the balancer through a member list and returns the plan it
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

// compareSteadyPlan plans the balancer for a member set that holds nothing: the
// assignment a group settles into, which is the state the coverage pin below is
// about.
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
			spec := compareMemberSpec{id: id, generation: compareGeneration, owned: claims[id]}
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
// holds across all lanes. The balancer balances totals, so a member's per-lane
// counts may differ while its totals stay close.
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
// exactly one member. It is the one fact this file asserts of every plan: a plan
// that drops a partition or hands it to two members is wrong whatever the
// strategy, and the coverage and spread figures beside it would be measuring a
// plan that cannot be sent.
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

// TestDefaultBalancerLaneCoverage drives the shipped default balancer through
// the member sets and lane shapes a subscription of this repository declares and
// prints what it assigns.
//
// It is a measurement that prints rather than an assertion of an answer, and it
// pins two things. One is exactly-once assignment, which every plan owes. The
// other is coverage: every member holds a partition of every lane of the settled
// plan whenever a lane has at least as many partitions as there are members, so
// no member is idle while another holds a lane to itself. A lane with fewer
// partitions than members cannot meet that rule, and those cells are printed as
// the observation they are rather than asserted.
//
// The grid is members 2, 3 and 4 crossed with 2, 3, 4 and 16 partitions per
// lane. Two more cells are the lanes the live priority comparison declares, with
// two members and with three, and a third is that comparison's underpartitioned
// shape exactly: two lanes of two partitions and three members, which is the
// smallest lane set on which a lane cannot seat every member.
//
// What the coverage pin protects is the spreading of whichever balancer the
// option resolves to, which is why it holds for every sticky-family balancer and
// not only for the cooperative default: a franz-go release that changed the
// spreading would fail it. The default itself is pinned by the protocol names
// TestResolveBalancerProtocols asserts, and by
// TestPortCooperativeKeepsRetainedPartitionsDelivering, which the protocol the
// driver joins a live group with cannot pass.
func TestDefaultBalancerLaneCoverage(t *testing.T) {
	balancer := compareDefaultBalancer(t)
	t.Logf("balancer=default protocol=%s", balancer.ProtocolName())

	for _, members := range []int{2, 3, 4} {
		for _, partitions := range []int32{2, 3, 4, 16} {
			compareCell(t, balancer, compareLanes(partitions), members, partitions)
		}
	}
	compareCell(t, balancer, compareLiveLanes(4), 2, 4)
	compareCell(t, balancer, compareLiveLanes(4), 3, 4)
	compareCell(t, balancer, compareLiveLanes(2), 3, 2)
}

// TestWholeLaneOwnershipObservation measures the state StickyBalancer's own doc
// comment starts from: two members that each hold a whole topic, a third topic
// free, and a third member gone, which is also the state a generation skews into
// when its members arrived one at a time against a live backlog.
//
// It is an observation only, printed rather than asserted beyond exactly-once:
// the comment's answer leaves one member with none of the other's topic, and
// whether the settled plan here does the same is the thing this cell records,
// not a rule the balancer is held to. The three lanes all have three partitions,
// which is more than the two members, so the coverage rule the other test pins
// does not reach this cell: what it measures is whether a balancer that balances
// totals keeps a member's whole topic together when nothing asks it to move.
func TestWholeLaneOwnershipObservation(t *testing.T) {
	balancer := compareDefaultBalancer(t)
	lanes := map[string]int32{"t0": 3, "t1": 3, "t2": 3}
	ids := []string{"member-0", "member-1"}
	prior := map[string]map[string][]int32{
		"member-0": {"t0": {0, 1, 2}},
		"member-1": {"t1": {0, 1, 2}},
	}

	plan, rounds, pending := compareRebalance(t, balancer, lanes, prior, "", ids)
	compareExactlyOnce(t, "whole lanes", lanes, plan, ids)
	zeroPairs, maxSpread, spreadLane := compareCoverage(lanes, plan, ids)
	least, most := compareTotals(lanes, plan, ids)
	t.Logf("plan balancer=default protocol=%s lanes=%d partitions=3 members=%d zeroPairs=%d maxSpread=%d spreadLane=%s totals=%d:%d rounds=%d pending=%d assignment=%s",
		balancer.ProtocolName(), len(lanes), len(ids), zeroPairs, maxSpread, spreadLane, least, most, rounds, pending, comparePlanLine(lanes, plan, ids))
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

// compareCell measures one (member set, lane set) cell of the shipped balancer:
// the settled plan it produces, and what one member joining the settled set and
// one member leaving it move. The coverage pin is applied where a lane has at
// least as many partitions as there are members.
func compareCell(t *testing.T, balancer kgo.GroupBalancer, lanes map[string]int32, members int, partitions int32) {
	t.Helper()
	ids := compareMemberIDs(members)
	label := fmt.Sprintf("lanes=%d partitions=%d members=%d", len(lanes), partitions, members)

	steady := compareSteadyPlan(t, balancer, lanes, ids...)
	compareExactlyOnce(t, label, lanes, steady, ids)
	zeroPairs, maxSpread, spreadLane := compareCoverage(lanes, steady, ids)
	least, most := compareTotals(lanes, steady, ids)
	t.Logf("plan balancer=default protocol=%s %s zeroPairs=%d maxSpread=%d spreadLane=%s totals=%d:%d",
		balancer.ProtocolName(), label, zeroPairs, maxSpread, spreadLane, least, most)
	if int(partitions) >= members && zeroPairs != 0 {
		t.Errorf("%s: %d uncovered (member, lane) pairs in the settled plan, want 0 while every lane has at least as many partitions as there are members",
			label, zeroPairs)
	}

	// One member joins the settled set, then the first member leaves it. The
	// balancer plans both changes itself, in as many rounds as a cooperative
	// rebalance takes.
	joinIDs := compareMemberIDs(members + 1)
	newcomer := joinIDs[len(joinIDs)-1]
	survivors := ids[1:]

	joined, joinRounds, joinPending := compareRebalance(t, balancer, lanes, steady, newcomer, joinIDs)
	compareExactlyOnce(t, label+" joined", lanes, joined, joinIDs)
	t.Logf("moves balancer=default protocol=%s %s phase=join moved=%d rounds=%d pending=%d",
		balancer.ProtocolName(), label, compareMoved(steady, joined, lanes), joinRounds, joinPending)

	left, leaveRounds, leavePending := compareRebalance(t, balancer, lanes, compareRestrict(steady, survivors), "", survivors)
	compareExactlyOnce(t, label+" left", lanes, left, survivors)
	t.Logf("moves balancer=default protocol=%s %s phase=leave moved=%d rounds=%d pending=%d",
		balancer.ProtocolName(), label, compareMoved(steady, left, lanes), leaveRounds, leavePending)
}

// compareMemberIDs names the members of a group of the given size.
func compareMemberIDs(count int) []string {
	ids := make([]string, 0, count)
	for index := range count {
		ids = append(ids, fmt.Sprintf("member-%d", index))
	}
	return ids
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
