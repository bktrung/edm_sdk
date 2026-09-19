package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

type kafkaTopicSpec struct {
	name              string
	partitions        int32
	replicationFactor int16
	err               error
}

type kafkaTopologyPlan struct {
	destinations []kafkaTopicSpec
	exchanges    []string
	bindings     []driver.BindingSpec
}

type topicVisibility uint8

const (
	kafkaTopologyVisibilityPoll                 = 10 * time.Millisecond
	topicMustExist              topicVisibility = iota
	topicMustBeAbsent
)

var (
	kafkaTopologyVisibilityTimeout             = 60 * time.Second
	kafkaTopologyClock             clock.Clock = clock.NewReal()
	kafkaTopologyListTopics                    = func(a *admin, ctx context.Context, operation string, names ...string) (kadm.TopicDetails, error) {
		return a.listTopics(ctx, operation, names...)
	}
)

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
	topic := kafkaTopicSpec{
		name:              spec.Name,
		replicationFactor: -1,
	}
	if spec.Partitions <= 0 {
		topic.partitions = -1
		return topic
	}
	const maxKafkaPartitions = int(1<<31 - 1)
	if spec.Partitions > maxKafkaPartitions {
		topic.err = classify("ensure_topology", driver.KindFatal, fmt.Errorf("destination %q partition count %d exceeds Kafka int32 maximum", spec.Name, spec.Partitions))
		return topic
	}
	topic.partitions = int32(spec.Partitions)
	return topic
}

func resolveMaxExpectedInstances(options map[string]string) (int32, error) {
	value, ok := options["kafka.maxExpectedInstances"]
	if !ok {
		return 0, nil
	}
	instances, err := strconv.ParseInt(value, 10, 32)
	if err != nil || instances < 0 {
		return 0, fmt.Errorf("kafka: invalid maxExpectedInstances %q; must be a non-negative integer", value)
	}
	return int32(instances), nil
}

func (a *admin) maxExpectedInstances() (int32, error) {
	if a.conn == nil {
		return 0, nil
	}
	return resolveMaxExpectedInstances(a.conn.driverOptions)
}

func partitionFloorError(destination string, partitions int, floor int32) error {
	return classify("ensure_topology", driver.KindFatal, fmt.Errorf(
		"destination %q has %d partitions, fewer than maxExpectedInstances %d",
		destination,
		partitions,
		floor,
	))
}

func (a *admin) checkTopicPartitionFloor(ctx context.Context, destination string, floor int32) error {
	if err := a.waitForTopicState(ctx, "ensure_topology", []string{destination}, topicMustExist); err != nil {
		return err
	}
	topics, err := a.listTopics(ctx, "ensure_topology", destination)
	if err != nil {
		return err
	}
	detail, ok := topics[destination]
	if !ok {
		return classify("ensure_topology", driver.KindFatal, fmt.Errorf("destination %q was not returned while checking partition floor", destination))
	}
	if detail.Err != nil {
		return classifyAdminError("ensure_topology", detail.Err)
	}
	if got := len(detail.Partitions); got < int(floor) {
		return partitionFloorError(destination, got, floor)
	}
	return nil
}

func (a *admin) ensureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	if spec.Policy == driver.TopologyNone {
		return driver.TopologyDiff{}, nil
	}
	maxExpectedInstances, err := a.maxExpectedInstances()
	if err != nil {
		return driver.TopologyDiff{}, classify("ensure_topology", driver.KindFatal, err)
	}
	if spec.Policy == driver.TopologyVerify {
		return a.verifyTopology(ctx, spec)
	}

	var diff driver.TopologyDiff
	created := make([]string, 0, len(spec.Destinations))
	plan := translateTopology(spec)
	for i := range plan.destinations {
		destination := plan.destinations[i]
		if destination.err != nil {
			return driver.TopologyDiff{}, destination.err
		}
		if maxExpectedInstances > 0 && destination.partitions < 0 {
			destination.partitions = maxExpectedInstances
		}
		if maxExpectedInstances > 0 && destination.partitions < maxExpectedInstances {
			return driver.TopologyDiff{}, partitionFloorError(
				destination.name,
				int(destination.partitions),
				maxExpectedInstances,
			)
		}
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
			created = append(created, destination.name)
		case errors.Is(response.Err, kerr.TopicAlreadyExists):
			if maxExpectedInstances > 0 {
				if err := a.checkTopicPartitionFloor(ctx, destination.name, maxExpectedInstances); err != nil {
					return driver.TopologyDiff{}, err
				}
			}
			diff.ExistingDestinations = append(diff.ExistingDestinations, destination.name)
		default:
			return driver.TopologyDiff{}, classifyAdminError("ensure_topology", response.Err)
		}
	}
	if err := a.waitForTopicState(ctx, "ensure_topology", created, topicMustExist); err != nil {
		return driver.TopologyDiff{}, err
	}

	// Kafka is FanoutAtConsume: exchanges and bindings are routing concepts
	// from brokers that copy messages at publish time, so these fields are
	// intentionally accepted and ignored here. Native dead lettering and
	// delivery limits are likewise handled by the core retry ladder.
	a.scanOrphans(ctx, spec, &diff)
	return diff, nil
}

func (a *admin) waitForTopicState(ctx context.Context, operation string, names []string, visibility topicVisibility) error {
	if len(names) == 0 {
		return nil
	}
	waitCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	timer := kafkaTopologyClock.Timer(kafkaTopologyVisibilityTimeout)
	defer timer.Stop()
	go func() {
		select {
		case <-timer.C:
			cancel(context.DeadlineExceeded)
		case <-waitCtx.Done():
		}
	}()
	ticker := kafkaTopologyClock.Ticker(kafkaTopologyVisibilityPoll)
	defer ticker.Stop()
	state := "present"
	if visibility == topicMustBeAbsent {
		state = "absent"
	}
	waitError := func() error {
		cause := context.Cause(waitCtx)
		if callerErr := ctx.Err(); callerErr != nil {
			cause = callerErr
		}
		if cause == nil {
			cause = waitCtx.Err()
		}
		return classify(operation, driver.KindTransient, fmt.Errorf("kafka: topics %q did not become %s: %w", names, state, cause))
	}
	for {
		if waitCtx.Err() != nil {
			return waitError()
		}
		topics, err := kafkaTopologyListTopics(a, waitCtx, operation, names...)
		if err != nil {
			if waitCtx.Err() != nil {
				return waitError()
			}
			return err
		}
		visible := true
		for _, name := range names {
			detail, ok := topics[name]
			present, known := false, true
			if ok && detail.Err == nil {
				present = visibility == topicMustBeAbsent || len(detail.Partitions) > 0
			} else if ok && detail.Err != nil {
				switch {
				case errors.Is(detail.Err, kerr.UnknownTopicOrPartition),
					errors.Is(detail.Err, kerr.UnknownTopicID):
					present = false
				case kafkaErrorKind(detail.Err) == driver.KindTransient:
					known = false
				default:
					return classifyAdminError(operation, detail.Err)
				}
			}
			wantPresent := visibility == topicMustExist
			if !known || present != wantPresent {
				visible = false
				break
			}
		}
		if visible {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return waitError()
		case <-ticker.C:
		}
	}
}

func (a *admin) verifyTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	var diff driver.TopologyDiff
	if len(spec.Destinations) == 0 {
		return diff, nil
	}
	maxExpectedInstances, err := a.maxExpectedInstances()
	if err != nil {
		return driver.TopologyDiff{}, classify("ensure_topology", driver.KindFatal, err)
	}
	topics, err := a.listTopics(ctx, "ensure_topology", destinationNames(spec.Destinations)...)
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
			return driver.TopologyDiff{}, missingDestinationError("ensure_topology", destination.Name)
		}
		if detail.Err != nil {
			return driver.TopologyDiff{}, classifyAdminError("ensure_topology", detail.Err)
		}
		diff.ExistingDestinations = append(diff.ExistingDestinations, destination.Name)
		if maxExpectedInstances > 0 && len(detail.Partitions) < int(maxExpectedInstances) {
			return driver.TopologyDiff{}, partitionFloorError(destination.Name, len(detail.Partitions), maxExpectedInstances)
		}
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
		if destination.DeliveryLimit > 0 {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindFatal, fmt.Errorf("destination %q delivery limit verification unsupported: %w", destination.Name, driver.ErrUnsupported))
		}
		if destination.DeadLetter != nil {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindFatal, fmt.Errorf("destination %q dead letter verification unsupported: %w", destination.Name, driver.ErrUnsupported))
		}
		// Delay is deliberately not refused here. It is a port-level marker,
		// not a broker-side argument, so Kafka holds no value to compare and
		// the existence, partition floor and partition count checks above
		// already verify every fact the broker has about a deferred lane.
		// Refusing the destination fails a lane the consume path honours.
	}
	return diff, nil
}

// describeTopology reports retained records, not unconsumed backlog. It has no
// consumer group input, so the phase that introduces consumer groups must decide
// whether this port should expose consumer lag instead.
func (a *admin) describeTopology(ctx context.Context, names []string) (driver.TopologyState, error) {
	if len(names) == 0 {
		return driver.TopologyState{Depth: make(map[string]int64)}, nil
	}
	starts, ends, err := a.listTopicOffsets(ctx, "describe_topology", names...)
	if err != nil {
		return driver.TopologyState{}, err
	}
	return evaluateTopicDepths(names, starts, ends)
}

func evaluateTopicDepths(names []string, starts, ends kadm.ListedOffsets) (driver.TopologyState, error) {
	state := driver.TopologyState{Depth: make(map[string]int64, len(names))}
	for _, name := range names {
		depth, present, err := topicDepth(name, starts, ends)
		if err != nil {
			return driver.TopologyState{}, classifyAdminError("describe_topology", err)
		}
		if !present {
			return driver.TopologyState{}, missingDestinationError("describe_topology", name)
		}
		state.Depth[name] = depth
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

func missingDestinationError(operation, name string) error {
	return classify(operation, driver.KindNotFound, fmt.Errorf("destination %q is missing: %w", name, driver.ErrDestinationMissing))
}

// topicDepth computes retained records as end.Offset - start.Offset summed over
// partitions. Missing topics remain absent so callers can distinguish missing
// from an existing topic with zero retained records.
func topicDepth(name string, starts, ends kadm.ListedOffsets) (int64, bool, error) {
	endPartitions, ok := ends[name]
	if !ok || len(endPartitions) == 0 {
		return 0, false, nil
	}
	startPartitions, ok := starts[name]
	if !ok || len(startPartitions) == 0 {
		return 0, false, nil
	}
	var depth int64
	for partition, end := range endPartitions {
		if end.Err != nil {
			if errors.Is(end.Err, kerr.UnknownTopicOrPartition) || errors.Is(end.Err, kerr.UnknownTopicID) {
				return 0, false, nil
			}
			return 0, false, end.Err
		}
		start, ok := startPartitions[partition]
		if !ok {
			return 0, false, nil
		}
		if start.Err != nil {
			if errors.Is(start.Err, kerr.UnknownTopicOrPartition) || errors.Is(start.Err, kerr.UnknownTopicID) {
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
	topics, err := a.listTopics(ctx, "ensure_topology")
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
	starts, ends, err := a.listTopicOffsets(ctx, "ensure_topology", orphanNames...)
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
