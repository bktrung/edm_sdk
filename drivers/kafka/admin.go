package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type admin struct {
	client *kadm.Client
}

var _ driver.Admin = (*admin)(nil)

// EnsureTopology accepts exchanges, bindings, DeadLetter, and DeliveryLimit
// fields, but Kafka does not honor them: routing is FanoutAtConsume, native
// dead lettering is unavailable, and the core retry ladder owns those semantics.
func (a *admin) EnsureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	if err := ctx.Err(); err != nil {
		return driver.TopologyDiff{}, classify("ensure_topology", driver.KindTransient, err)
	}
	return a.ensureTopology(ctx, spec)
}

func (a *admin) DescribeTopology(ctx context.Context, names []string) (driver.TopologyState, error) {
	if err := ctx.Err(); err != nil {
		return driver.TopologyState{}, classify("describe_topology", driver.KindTransient, err)
	}
	return a.describeTopology(ctx, names)
}

func (a *admin) createTopic(ctx context.Context, topic kafkaTopicSpec) (kadm.CreateTopicResponse, error) {
	responses, err := a.client.CreateTopics(ctx, topic.partitions, topic.replicationFactor, nil, topic.name)
	if err != nil {
		return kadm.CreateTopicResponse{}, classifyAdminError("ensure_topology", err)
	}
	response, ok := responses[topic.name]
	if !ok {
		return kadm.CreateTopicResponse{}, classify("ensure_topology", driver.KindFatal, fmt.Errorf("kafka: create topic response missing %q", topic.name))
	}
	return response, nil
}

func (a *admin) listTopics(ctx context.Context, names ...string) (kadm.TopicDetails, error) {
	topics, err := a.client.ListTopics(kadm.WithAuthorizedOps(ctx), names...)
	if err != nil {
		return nil, classifyAdminError("ensure_topology", err)
	}
	return topics, nil
}

func (a *admin) listTopicOffsets(ctx context.Context, operation string, names ...string) (kadm.ListedOffsets, kadm.ListedOffsets, error) {
	starts, err := a.client.ListStartOffsets(ctx, names...)
	if err != nil {
		return nil, nil, classifyAdminError(operation, err)
	}
	ends, err := a.client.ListEndOffsets(ctx, names...)
	if err != nil {
		return nil, nil, classifyAdminError(operation, err)
	}
	return starts, ends, nil
}

func classifyAdminError(operation string, err error) error {
	if err == nil {
		return nil
	}
	kind := kafkaErrorKind(err)
	switch {
	case errors.Is(err, kerr.UnknownTopicOrPartition):
		kind = driver.KindNotFound
	case errors.Is(err, kerr.InvalidTopicException),
		errors.Is(err, kerr.InvalidPartitions),
		errors.Is(err, kerr.InvalidReplicationFactor),
		errors.Is(err, kerr.InvalidConfig):
		kind = driver.KindFatal
	}
	return classify(operation, kind, err)
}
