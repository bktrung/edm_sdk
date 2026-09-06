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
	conn   *conn
}

var (
	_ driver.Admin       = (*admin)(nil)
	_ driver.Maintenance = (*admin)(nil)
)

// maintenanceGate serializes destructive metadata sequences. Conn.Admin
// creates a fresh facade for each call, so a receiver mutex would not
// coordinate two Purge calls sharing the same broker client: without
// serialization, both can read the same low watermark and both report the
// same records as removed. Channel acquisition remains context-aware.
var maintenanceGate = make(chan struct{}, 1)

func acquireMaintenance(ctx context.Context) error {
	select {
	case maintenanceGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func releaseMaintenance() {
	<-maintenanceGate
}

func missingPurgeDestinationError(destination string, cause error) error {
	if cause == nil {
		cause = driver.ErrDestinationMissing
	} else {
		cause = errors.Join(driver.ErrDestinationMissing, cause)
	}
	return classify("purge", driver.KindNotFound, fmt.Errorf("destination %q is missing: %w", destination, cause))
}

// Purge advances every partition's low watermark to its current end offset
// and returns the number of records removed. The destination remains present.
func (a *admin) Purge(ctx context.Context, destination string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, classify("purge", driver.KindTransient, err)
	}
	if err := acquireMaintenance(ctx); err != nil {
		return 0, classify("purge", driver.KindTransient, err)
	}
	defer releaseMaintenance()
	starts, ends, err := a.listTopicOffsets(ctx, "purge", destination)
	if err != nil {
		if errors.Is(err, kerr.UnknownTopicOrPartition) {
			return 0, missingPurgeDestinationError(destination, err)
		}
		return 0, err
	}
	startPartitions, ok := starts[destination]
	if !ok {
		return 0, missingPurgeDestinationError(destination, nil)
	}
	endPartitions, ok := ends[destination]
	if !ok {
		return 0, missingPurgeDestinationError(destination, nil)
	}

	offsets := make(kadm.Offsets)
	for partition, end := range endPartitions {
		if end.Err != nil {
			if errors.Is(end.Err, kerr.UnknownTopicOrPartition) {
				return 0, missingPurgeDestinationError(destination, end.Err)
			}
			return 0, classifyAdminError("purge", end.Err)
		}
		start, ok := startPartitions[partition]
		if !ok {
			return 0, classify("purge", driver.KindFatal, fmt.Errorf("kafka: start offset missing for %s[%d]", destination, partition))
		}
		if start.Err != nil {
			if errors.Is(start.Err, kerr.UnknownTopicOrPartition) {
				return 0, missingPurgeDestinationError(destination, start.Err)
			}
			return 0, classifyAdminError("purge", start.Err)
		}
		offsets.AddOffset(destination, partition, end.Offset, end.LeaderEpoch)
	}

	responses, err := a.client.DeleteRecords(ctx, offsets)
	if err != nil {
		return 0, classifyAdminError("purge", err)
	}
	var removed int64
	for partition := range endPartitions {
		response, ok := responses.Lookup(destination, partition)
		if !ok {
			return 0, classify("purge", driver.KindFatal, fmt.Errorf("kafka: delete records response missing for %s[%d]", destination, partition))
		}
		if response.Err != nil {
			return 0, classifyAdminError("purge", response.Err)
		}
		removed += response.LowWatermark - startPartitions[partition].Offset
	}
	return removed, nil
}

// Prune deletes empty, unattached destinations and reports a reason for each
// destination that is missing, non-empty, or still assigned to a consumer.
func (a *admin) Prune(ctx context.Context, names []string) ([]driver.PruneResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("prune", driver.KindTransient, err)
	}
	if err := acquireMaintenance(ctx); err != nil {
		return nil, classify("prune", driver.KindTransient, err)
	}
	defer releaseMaintenance()
	results := make([]driver.PruneResult, 0, len(names))
	for _, name := range names {
		reason, err := a.pruneGuard(ctx, name)
		if err != nil {
			return nil, err
		}
		if reason != "" {
			if reason == "destination does not exist" {
				a.clearDestinationDelay(name)
			}
			results = append(results, driver.PruneResult{Name: name, Reason: reason})
			continue
		}

		// Kafka has no conditional delete. A consumer can attach after the
		// guard and before DeleteTopics, so this check-then-delete window can
		// still destroy a newly attached destination; the port has no atomic
		// compare-and-delete operation with which to close it.
		reason, err = a.pruneGuard(ctx, name)
		if err != nil {
			return nil, err
		}
		if reason != "" {
			if reason == "destination does not exist" {
				a.clearDestinationDelay(name)
			}
			results = append(results, driver.PruneResult{Name: name, Reason: reason})
			continue
		}
		responses, err := a.client.DeleteTopics(ctx, name)
		if err != nil {
			return nil, classifyAdminError("prune", err)
		}
		response, ok := responses[name]
		if !ok {
			return nil, classify("prune", driver.KindFatal, fmt.Errorf("kafka: delete topic response missing %q", name))
		}
		if response.Err != nil {
			if errors.Is(response.Err, kerr.UnknownTopicOrPartition) {
				a.clearDestinationDelay(name)
				results = append(results, driver.PruneResult{Name: name, Reason: "destination does not exist"})
				continue
			}
			return nil, classifyAdminError("prune", response.Err)
		}
		a.clearDestinationDelay(name)
		results = append(results, driver.PruneResult{Name: name, Deleted: true})
	}
	return results, nil
}

func (a *admin) pruneGuard(ctx context.Context, name string) (string, error) {
	starts, ends, err := a.listTopicOffsets(ctx, "prune", name)
	if err != nil {
		return "", err
	}
	depth, present, err := topicDepth(name, starts, ends)
	if err != nil {
		return "", classifyAdminError("prune", err)
	}
	if !present {
		return "destination does not exist", nil
	}
	if depth != 0 {
		return fmt.Sprintf("destination holds %d records", depth), nil
	}
	if a.conn != nil {
		a.conn.mu.RLock()
		for csm := range a.conn.consumers {
			for _, dest := range csm.destinations {
				if dest == name {
					a.conn.mu.RUnlock()
					return "consumer attached", nil
				}
			}
		}
		a.conn.mu.RUnlock()
	}

	groups, err := a.client.ListGroups(ctx)
	if err != nil {
		return "", classifyAdminError("prune", err)
	}
	if len(groups) == 0 {
		return "", nil
	}
	described, err := a.client.DescribeGroups(ctx, groups.Groups()...)
	if err != nil {
		return "", classifyAdminError("prune", err)
	}
	for _, group := range described {
		if group.Err != nil && !errors.Is(group.Err, kerr.GroupIDNotFound) {
			return "", classifyAdminError("prune", group.Err)
		}
		for _, member := range group.Members {
			if joinConsumer, ok := member.Join.AsConsumer(); ok {
				for _, topic := range joinConsumer.Topics {
					if topic == name {
						return "consumer attached", nil
					}
				}
			}
			if assignConsumer, ok := member.Assigned.AsConsumer(); ok {
				for _, t := range assignConsumer.Topics {
					if t.Topic == name {
						return "consumer attached", nil
					}
				}
			}
		}
	}
	return "", nil
}

// EnsureTopology accepts exchanges, bindings, DeadLetter, and DeliveryLimit
// fields, but Kafka does not honor them: routing is FanoutAtConsume, native
// dead lettering is unavailable, and the core retry ladder owns those semantics.
func (a *admin) EnsureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	if err := ctx.Err(); err != nil {
		return driver.TopologyDiff{}, classify("ensure_topology", driver.KindTransient, err)
	}
	diff, err := a.ensureTopology(ctx, spec)
	if err != nil {
		return driver.TopologyDiff{}, err
	}
	if spec.Policy != driver.TopologyNone {
		a.recordDestinationDelays(spec.Destinations)
	}
	return diff, nil
}

func (a *admin) recordDestinationDelays(destinations []driver.DestinationSpec) {
	if a.conn == nil {
		return
	}
	a.conn.mu.Lock()
	defer a.conn.mu.Unlock()
	for _, destination := range destinations {
		a.conn.delays[destination.Name] = destination.Delay
	}
}

func (a *admin) DescribeTopology(ctx context.Context, names []string) (driver.TopologyState, error) {
	if err := ctx.Err(); err != nil {
		return driver.TopologyState{}, classify("describe_topology", driver.KindTransient, err)
	}
	return a.describeTopology(ctx, names)
}

func (a *admin) clearDestinationDelay(destination string) {
	if a.conn == nil {
		return
	}
	a.conn.mu.Lock()
	defer a.conn.mu.Unlock()
	delete(a.conn.delays, destination)
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
	case errors.Is(err, kerr.UnknownTopicOrPartition), errors.Is(err, kerr.UnknownTopicID):
		kind = driver.KindNotFound
	case errors.Is(err, kerr.InvalidTopicException),
		errors.Is(err, kerr.InvalidPartitions),
		errors.Is(err, kerr.InvalidReplicationFactor),
		errors.Is(err, kerr.InvalidConfig),
		errors.Is(err, kerr.TopicDeletionDisabled):
		kind = driver.KindFatal
	}
	return classify(operation, kind, err)
}
