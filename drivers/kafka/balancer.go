package kafka

import (
	"fmt"
	"maps"
	"slices"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var _ kgo.GroupBalancer = (*laneBalancer)(nil)

type laneBalancer struct{}

func (*laneBalancer) ProtocolName() string {
	return "lane"
}

// JoinGroupMetadata advertises the member's lane interest together with the
// partitions it claims from its last completed generation, so the leader can
// leave those partitions where they are. Delegating this to a simple protocol
// discards currentAssignment, and with it every claim this member could make.
// Version 3 is the version the delegating path advertised, and it is the one
// that carries OwnedPartitions and the generation they are valid at.
func (*laneBalancer) JoinGroupMetadata(interests []string, currentAssignment map[string][]int32, generation int32) []byte {
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 3
	meta.Topics = interests
	meta.Generation = generation
	for _, topic := range slices.Sorted(maps.Keys(currentAssignment)) {
		owned := kmsg.NewConsumerMemberMetadataOwnedPartition()
		owned.Topic = topic
		owned.Partitions = currentAssignment[topic]
		meta.OwnedPartitions = append(meta.OwnedPartitions, owned)
	}
	return meta.AppendTo(nil)
}

func (*laneBalancer) ParseSyncAssignment(buf []byte) (map[string][]int32, error) {
	return kgo.ParseConsumerSyncAssignment(buf)
}

func (b *laneBalancer) MemberBalancer(members []kmsg.JoinGroupResponseMember) (kgo.GroupMemberBalancer, map[string]struct{}, error) {
	consumerBalancer, err := kgo.NewConsumerBalancer(b, members)
	if err != nil {
		return nil, nil, err
	}
	return consumerBalancer, consumerBalancer.MemberTopics(), nil
}

func (*laneBalancer) IsCooperative() bool {
	return false
}

func (*laneBalancer) Balance(b *kgo.ConsumerBalancer, topics map[string]int32) kgo.IntoSyncAssignment {
	plan := b.NewPlan()
	generation := laneCurrentGeneration(b)
	for _, topic := range slices.Sorted(maps.Keys(topics)) {
		partitionCount := int(topics[topic])
		if partitionCount <= 0 {
			continue
		}

		members := laneMembersForTopic(b, topic)
		if len(members) == 0 {
			continue
		}

		planLane(plan, topic, partitionCount, members, generation)
	}
	return plan
}

// laneMember is a member interested in one lane, paired with the metadata it
// advertised in the join.
type laneMember struct {
	member   *kmsg.JoinGroupResponseMember
	metadata *kmsg.ConsumerMemberMetadata
}

func laneMembersForTopic(b *kgo.ConsumerBalancer, topic string) []laneMember {
	members := make([]laneMember, 0, len(b.Members()))
	b.EachMember(func(member *kmsg.JoinGroupResponseMember, metadata *kmsg.ConsumerMemberMetadata) {
		if slices.Contains(metadata.Topics, topic) {
			members = append(members, laneMember{member: member, metadata: metadata})
		}
	})
	return members
}

// laneCurrentGeneration is the highest generation any member advertises, which
// is the generation of the last rebalance that every member of the group
// completed. A member that missed a rebalance advertises the older generation
// it synced at, and the partitions it names there may already be consumed by
// someone else, so nothing below this generation counts as ownership.
func laneCurrentGeneration(b *kgo.ConsumerBalancer) int32 {
	generation := int32(-1)
	b.EachMember(func(_ *kmsg.JoinGroupResponseMember, metadata *kmsg.ConsumerMemberMetadata) {
		generation = max(generation, metadata.Generation)
	})
	return generation
}

// planLane assigns every partition of one lane. Each member's count is decided
// first, exactly as it was before ownership was an input, and only then may the
// member keep the partitions it already owns up to that count. Stickiness
// therefore decides which partitions a member gets, never how many a lane gives
// it.
func planLane(plan *kgo.BalancePlan, topic string, partitionCount int, members []laneMember, generation int32) {
	base := partitionCount / len(members)
	remainder := partitionCount % len(members)

	// remaining holds each member's target count, and order is the member
	// sequence those counts are handed out in. A lane whose members own nothing
	// is planned by walking order, which is what this balancer has always done,
	// and the remainder still starts at the same hashed offset.
	remaining := make([]int, len(members))
	order := make([]int, 0, partitionCount)
	for memberIndex := range members {
		remaining[memberIndex] = base
		for range base {
			order = append(order, memberIndex)
		}
	}
	offset := laneRemainderOffset(topic, len(members))
	for remainderIndex := range remainder {
		memberIndex := (offset + remainderIndex) % len(members)
		remaining[memberIndex]++
		order = append(order, memberIndex)
	}

	// A partition is placed once. Two members can claim the same partition at
	// the same generation after a rebalance that failed between join and sync,
	// and the later claimant then releases it like any other over-quota
	// partition.
	placed := make([]bool, partitionCount)
	for memberIndex := range members {
		if remaining[memberIndex] == 0 {
			continue
		}
		for _, partition := range laneOwnedPartitions(members[memberIndex], topic, partitionCount, generation) {
			if remaining[memberIndex] == 0 {
				break
			}
			if placed[partition] {
				continue
			}
			placed[partition] = true
			plan.AddPartition(members[memberIndex].member, topic, partition)
			remaining[memberIndex]--
		}
	}

	next := 0
	for _, memberIndex := range order {
		if remaining[memberIndex] == 0 {
			continue
		}
		for next < partitionCount && placed[next] {
			next++
		}
		if next == partitionCount {
			break
		}
		placed[next] = true
		plan.AddPartition(members[memberIndex].member, topic, int32(next))
		remaining[memberIndex]--
	}
}

// laneOwnedPartitions returns the partitions of one lane that a member claims
// at the group's current generation, sorted and deduplicated. A claim from any
// other generation is treated as absent: the member that advertises it missed a
// rebalance, and honouring the claim would pin a partition to a member that no
// longer holds it.
func laneOwnedPartitions(member laneMember, topic string, partitionCount int, generation int32) []int32 {
	if member.metadata.Generation != generation {
		return nil
	}
	var partitions []int32
	for _, owned := range member.metadata.OwnedPartitions {
		if owned.Topic != topic {
			continue
		}
		for _, partition := range owned.Partitions {
			if partition >= 0 && int(partition) < partitionCount {
				partitions = append(partitions, partition)
			}
		}
	}
	slices.Sort(partitions)
	return slices.Compact(partitions)
}

// laneRemainderOffset hashes the lane name so every member computes the same
// deterministic remainder start without sharing mutable balancer state.
func laneRemainderOffset(lane string, memberCount int) int {
	if memberCount <= 0 {
		return 0
	}
	const (
		fnvOffsetBasis uint32 = 2166136261
		fnvPrime       uint32 = 16777619
	)
	hash := fnvOffsetBasis
	for index := range len(lane) {
		hash ^= uint32(lane[index])
		hash *= fnvPrime
	}
	return int(hash % uint32(memberCount)) //nolint:gosec // memberCount is bounded by the join group.
}

func resolveBalancer(options map[string]string) (kgo.GroupBalancer, error) {
	value, ok := options["kafka.balancer"]
	if !ok {
		value = "lane"
	}
	switch value {
	case "lane":
		return &laneBalancer{}, nil
	case "cooperative-sticky":
		return kgo.CooperativeStickyBalancer(), nil
	case "sticky":
		return kgo.StickyBalancer(), nil
	case "range":
		return kgo.RangeBalancer(), nil
	default:
		return nil, fmt.Errorf("kafka: invalid kafka.balancer %q; supported values are lane, cooperative-sticky, sticky, range", value)
	}
}
