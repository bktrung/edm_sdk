package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type kafkaTopicSpec struct {
	name              string
	partitions        int32
	replicationFactor int16
}

type kafkaTopologyPlan struct {
	destinations []kafkaTopicSpec
	exchanges    []string
	bindings     []driver.BindingSpec
}

func translateTopology(spec driver.TopologySpec) kafkaTopologyPlan {
	plan := kafkaTopologyPlan{
		destinations: make([]kafkaTopicSpec, 0, len(spec.Destinations)),
	}
	for _, destination := range spec.Destinations {
		plan.destinations = append(plan.destinations, translateDestination(destination))
	}
	return plan
}

func translateDestination(spec driver.DestinationSpec) kafkaTopicSpec {
	partitions := int32(spec.Partitions) //nolint:gosec // Kafka's API represents partition counts as int32.
	if partitions <= 0 {
		partitions = -1
	}
	return kafkaTopicSpec{
		name:              spec.Name,
		partitions:        partitions,
		replicationFactor: -1,
	}
}

func (a *admin) ensureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	if spec.Policy == driver.TopologyNone {
		return driver.TopologyDiff{}, nil
	}
	if spec.Policy == driver.TopologyVerify {
		return a.verifyTopology(ctx, spec)
	}

	var diff driver.TopologyDiff
	plan := translateTopology(spec)
	for _, destination := range plan.destinations {
		if err := ctx.Err(); err != nil {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindTransient, err)
		}
		response, err := a.createTopic(ctx, destination)
		if err != nil {
			return driver.TopologyDiff{}, err
		}
		switch {
		case response.Err == nil:
			diff.CreatedDestinations = append(diff.CreatedDestinations, destination.name)
		case errors.Is(response.Err, kerr.TopicAlreadyExists):
			diff.ExistingDestinations = append(diff.ExistingDestinations, destination.name)
		default:
			return driver.TopologyDiff{}, classifyAdminError("ensure_topology", response.Err)
		}
	}

	// Kafka is FanoutAtConsume: exchanges and bindings are routing concepts
	// from brokers that copy messages at publish time, so these fields are
	// intentionally accepted and ignored here. Native dead lettering and
	// delivery limits are likewise handled by the core retry ladder.
	a.scanOrphans(ctx, spec, &diff)
	return diff, nil
}

func (a *admin) verifyTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	var diff driver.TopologyDiff
	if len(spec.Destinations) == 0 {
		return diff, nil
	}
	topics, err := a.listTopics(ctx, destinationNames(spec.Destinations)...)
	if err != nil {
		return driver.TopologyDiff{}, err
	}
	for _, destination := range spec.Destinations {
		if err := ctx.Err(); err != nil {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindTransient, err)
		}
		if destination.Name == "" {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindFatal, errors.New("destination name is empty"))
		}
		detail, ok := topics[destination.Name]
		if !ok || errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
			return driver.TopologyDiff{}, missingDestinationError(destination.Name)
		}
		if detail.Err != nil {
			return driver.TopologyDiff{}, classifyAdminError("ensure_topology", detail.Err)
		}
		diff.ExistingDestinations = append(diff.ExistingDestinations, destination.Name)
		if destination.Partitions > 0 {
			got := len(detail.Partitions)
			if got != destination.Partitions {
				diff.Drifted = append(diff.Drifted, driver.ArgumentDrift{
					Name:     destination.Name,
					Argument: "partitions",
					Want:     fmt.Sprint(destination.Partitions),
					Got:      fmt.Sprint(got),
				})
			}
		}
	}
	return diff, nil
}

// describeTopology reports retained records, not unconsumed backlog. It has no
// consumer group input, so the phase that introduces consumer groups must decide
// whether this port should expose consumer lag instead.
func (a *admin) describeTopology(ctx context.Context, names []string) (driver.TopologyState, error) {
	state := driver.TopologyState{Depth: make(map[string]int64, len(names))}
	if len(names) == 0 {
		return state, nil
	}
	starts, ends, err := a.listTopicOffsets(ctx, names...)
	if err != nil {
		return driver.TopologyState{}, err
	}
	for _, name := range names {
		depth, present, err := topicDepth(name, starts, ends)
		if err != nil {
			return driver.TopologyState{}, classifyAdminError("describe_topology", err)
		}
		if present {
			state.Depth[name] = depth
		}
	}
	return state, nil
}

func destinationNames(destinations []driver.DestinationSpec) []string {
	names := make([]string, 0, len(destinations))
	for _, destination := range destinations {
		names = append(names, destination.Name)
	}
	return names
}

func missingDestinationError(name string) error {
	return classify("ensure_topology", driver.KindNotFound, fmt.Errorf("destination %q is missing: %w", name, driver.ErrDestinationMissing))
}

// topicDepth computes retained records as end.Offset - start.Offset summed over
// partitions. Missing topics remain absent so callers can distinguish missing
// from an existing topic with zero retained records.
func topicDepth(name string, starts, ends kadm.ListedOffsets) (int64, bool, error) {
	endPartitions, ok := ends[name]
	if !ok {
		return 0, false, nil
	}
	startPartitions, ok := starts[name]
	if !ok {
		return 0, false, nil
	}
	var depth int64
	for partition, end := range endPartitions {
		if end.Err != nil {
			if errors.Is(end.Err, kerr.UnknownTopicOrPartition) {
				return 0, false, nil
			}
			return 0, false, end.Err
		}
		start, ok := startPartitions[partition]
		if !ok {
			return 0, false, nil
		}
		if start.Err != nil {
			if errors.Is(start.Err, kerr.UnknownTopicOrPartition) {
				return 0, false, nil
			}
			return 0, false, start.Err
		}
		depth += end.Offset - start.Offset
	}
	return depth, true, nil
}

func (a *admin) scanOrphans(ctx context.Context, spec driver.TopologySpec, diff *driver.TopologyDiff) {
	if len(spec.Scope) == 0 {
		diff.OrphanScanError = "orphan scan disabled because no scope was supplied"
		return
	}
	topics, err := a.listTopics(ctx)
	if err != nil {
		diff.OrphanScanError = err.Error()
		return
	}
	known := make(map[string]struct{}, len(spec.Destinations))
	for _, destination := range spec.Destinations {
		known[destination.Name] = struct{}{}
	}
	orphanNames := make([]string, 0)
	for name := range topics {
		if _, wanted := known[name]; wanted || !matchesScope(name, spec.Scope) {
			continue
		}
		if detail := topics[name]; detail.Err != nil {
			diff.OrphanScanError = detail.Err.Error()
			return
		}
		orphanNames = append(orphanNames, name)
	}
	sort.Strings(orphanNames)
	if len(orphanNames) == 0 {
		return
	}
	starts, ends, err := a.listTopicOffsets(ctx, orphanNames...)
	if err != nil {
		diff.OrphanScanError = err.Error()
		return
	}
	for _, name := range orphanNames {
		depth, present, depthErr := topicDepth(name, starts, ends)
		if depthErr != nil {
			diff.OrphanScanError = depthErr.Error()
			return
		}
		if present {
			diff.Orphaned = append(diff.Orphaned, driver.OrphanedDestination{Name: name, Messages: depth})
		}
	}
}

func matchesScope(name string, scopes []string) bool {
	for _, scope := range scopes {
		if strings.HasPrefix(name, scope) {
			return true
		}
	}
	return false
}
