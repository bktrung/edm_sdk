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

func (*laneBalancer) JoinGroupMetadata(interests []string, currentAssignment map[string][]int32, generation int32) []byte {
	return kgo.RoundRobinBalancer().JoinGroupMetadata(interests, currentAssignment, generation)
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
	for _, topic := range slices.Sorted(maps.Keys(topics)) {
		partitionCount := topics[topic]
		if partitionCount <= 0 {
			continue
		}

		members := laneMembersForTopic(b, topic)
		if len(members) == 0 {
			continue
		}

		base := int(partitionCount) / len(members)
		remainder := int(partitionCount) % len(members)
		partition := int32(0)
		for _, member := range members {
			for range base {
				plan.AddPartition(member, topic, partition)
				partition++
			}
		}

		offset := laneRemainderOffset(topic, len(members))
		for remainderIndex := range remainder {
			member := members[(offset+remainderIndex)%len(members)]
			plan.AddPartition(member, topic, partition)
			partition++
		}
	}
	return plan
}

func laneMembersForTopic(b *kgo.ConsumerBalancer, topic string) []*kmsg.JoinGroupResponseMember {
	members := make([]*kmsg.JoinGroupResponseMember, 0, len(b.Members()))
	b.EachMember(func(member *kmsg.JoinGroupResponseMember, metadata *kmsg.ConsumerMemberMetadata) {
		for _, interestedTopic := range metadata.Topics {
			if interestedTopic == topic {
				members = append(members, member)
				return
			}
		}
	})
	return members
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
