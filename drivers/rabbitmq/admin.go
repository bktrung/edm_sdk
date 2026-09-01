package rabbitmq

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type adminOperations struct {
	conn *conn
	// pruneBeforeDeleteHook is a test-only synchronization seam. It remains nil
	// in production and does not change driver behavior.
	pruneBeforeDeleteHook func(string)
}

type admin struct{ operations *adminOperations }

var (
	_ driver.Admin       = (*admin)(nil)
	_ driver.Maintenance = (*admin)(nil)
	_ driver.Maintenance = (*adminOperations)(nil)
)

var errQueueNotPrunable = errors.New("rabbitmq: queue is no longer prunable")

type queuePruneGuardError struct {
	reason string
}

func (e *queuePruneGuardError) Error() string { return e.reason }

func (e *queuePruneGuardError) Unwrap() error { return errQueueNotPrunable }

func (a *admin) EnsureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	return a.operations.EnsureTopology(ctx, spec)
}

func (a *admin) DescribeTopology(ctx context.Context, names []string) (driver.TopologyState, error) {
	return a.operations.DescribeTopology(ctx, names)
}

func (a *admin) Purge(ctx context.Context, destination string) (int64, error) {
	return a.operations.Purge(ctx, destination)
}

func (a *admin) Prune(ctx context.Context, names []string) ([]driver.PruneResult, error) {
	return a.operations.Prune(ctx, names)
}

// admission is the shared Admin facade gate. Channel-creating internals retain openChannel as a second boundary.
func (a *adminOperations) admission(ctx context.Context, operation string) error {
	if err := ctx.Err(); err != nil {
		return classify(operation, driver.KindTransient, err)
	}
	a.conn.mu.RLock()
	closed := a.conn.closed || a.conn.closing || a.conn.amqp.IsClosed()
	a.conn.mu.RUnlock()
	if closed {
		return classify(operation, driver.KindTransient, amqp.ErrClosed)
	}
	return nil
}

func (a *adminOperations) EnsureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	if err := a.admission(ctx, "ensure_topology"); err != nil {
		return driver.TopologyDiff{}, err
	}
	return a.ensureTopology(ctx, spec)
}

func (a *adminOperations) DescribeTopology(ctx context.Context, names []string) (driver.TopologyState, error) {
	if err := a.admission(ctx, "describe_topology"); err != nil {
		return driver.TopologyState{}, err
	}
	a.conn.mu.RLock()
	deferred := make(map[string]struct{}, len(a.conn.deferred))
	for name := range a.conn.deferred {
		deferred[name] = struct{}{}
	}
	a.conn.mu.RUnlock()
	depth := make(map[string]int64, len(names))
	for _, name := range names {
		ready, err := a.inspectQueue(ctx, name)
		if err != nil {
			if isNotFound(err) {
				return driver.TopologyState{}, classify("describe_topology", driver.KindNotFound, errors.Join(driver.ErrDestinationMissing, fmt.Errorf("destination %q is missing", name)))
			}
			return driver.TopologyState{}, classifyAMQP("describe_topology", driver.KindTransient, err)
		}
		depth[name] = int64(ready)
		if _, ok := deferred[name]; ok {
			parked, parkErr := a.inspectQueue(ctx, name+".park")
			if parkErr != nil && !isNotFound(parkErr) {
				return driver.TopologyState{}, classifyAMQP("describe_topology", driver.KindTransient, parkErr)
			}
			if parkErr == nil {
				depth[name] += int64(parked)
			}
		}
	}
	return driver.TopologyState{Depth: depth}, nil
}

func (a *adminOperations) Purge(ctx context.Context, destination string) (int64, error) {
	if err := a.admission(ctx, "purge"); err != nil {
		return 0, err
	}
	channel, err := a.openChannel(ctx)
	if err != nil {
		return 0, err
	}
	defer channel.Close()
	count, err := channel.QueuePurge(destination, false)
	if err != nil {
		if isNotFound(err) {
			return 0, classify("purge", driver.KindNotFound, errors.Join(driver.ErrDestinationMissing, err))
		}
		return 0, classifyAMQP("purge", driver.KindTransient, err)
	}
	a.conn.mu.RLock()
	_, hasParking := a.conn.deferred[destination]
	a.conn.mu.RUnlock()
	if !hasParking {
		return int64(count), nil
	}
	parked, err := channel.QueuePurge(destination+".park", false)
	if err != nil {
		return int64(count), classifyAMQP("purge", driver.KindTransient, err)
	}
	return int64(count + parked), nil
}

func (a *adminOperations) Prune(ctx context.Context, names []string) ([]driver.PruneResult, error) {
	if err := a.admission(ctx, "prune"); err != nil {
		return nil, err
	}
	queues, err := a.conn.management.listQueues(ctx)
	if err != nil {
		return nil, classify("prune", driver.KindTransient, err)
	}
	results := make([]driver.PruneResult, 0, len(names))
	for _, name := range names {
		result := driver.PruneResult{Name: name}
		main, exists := findQueue(queues, name)
		if !exists {
			result.Reason = "destination does not exist"
			results = append(results, result)
			continue
		}
		parkName := name + ".park"
		_, hasPark := findQueue(queues, parkName)
		mainReady, inspectErr := a.inspectQueue(ctx, name)
		if inspectErr != nil {
			result.Reason = "destination disappeared before deletion"
			results = append(results, result)
			continue
		}
		parkReady := int64(0)
		if hasPark {
			parkReady, inspectErr = a.inspectQueue(ctx, parkName)
			if inspectErr != nil {
				result.Reason = "parking destination disappeared before deletion"
				results = append(results, result)
				continue
			}
		}
		if reason := a.pruneReason(name, main, mainReady, hasPark, parkReady); reason != "" {
			result.Reason = reason
			results = append(results, result)
			continue
		}
		if hasPark {
			queues, err = a.conn.management.listQueues(ctx)
			if err != nil {
				return nil, classify("prune", driver.KindTransient, err)
			}
			main, exists = findQueue(queues, name)
			_, hasPark = findQueue(queues, parkName)
			if !exists {
				result.Reason = "destination disappeared before deletion"
				results = append(results, result)
				continue
			}
			mainReady, inspectErr = a.inspectQueue(ctx, name)
			if inspectErr != nil {
				result.Reason = "destination disappeared before deletion"
				results = append(results, result)
				continue
			}
			parkReady, inspectErr = a.inspectQueue(ctx, parkName)
			if inspectErr != nil {
				result.Reason = "parking destination disappeared before deletion"
				results = append(results, result)
				continue
			}
			if reason := a.pruneReason(name, main, mainReady, hasPark, parkReady); reason != "" {
				result.Reason = reason
				results = append(results, result)
				continue
			}
			if a.pruneBeforeDeleteHook != nil {
				a.pruneBeforeDeleteHook(parkName)
			}
			deleted, deleteErr := a.deleteQueue(ctx, parkName, true)
			if errors.Is(deleteErr, errQueueNotPrunable) {
				result.Reason = pruneDeleteReason(deleteErr, "parking destination is no longer prunable")
				results = append(results, result)
				continue
			}
			if deleteErr != nil {
				return nil, classify("prune", driver.KindTransient, deleteErr)
			}
			if !deleted {
				result.Reason = "parking destination disappeared before deletion"
				results = append(results, result)
				continue
			}
		}
		queues, err = a.conn.management.listQueues(ctx)
		if err != nil {
			return nil, classify("prune", driver.KindTransient, err)
		}
		main, exists = findQueue(queues, name)
		if !exists {
			result.Reason = "destination disappeared before deletion"
			results = append(results, result)
			continue
		}
		mainReady, inspectErr = a.inspectQueue(ctx, name)
		if inspectErr != nil {
			result.Reason = "destination disappeared before deletion"
			results = append(results, result)
			continue
		}
		if reason := a.pruneReason(name, main, mainReady, false, 0); reason != "" {
			result.Reason = reason
			results = append(results, result)
			continue
		}
		if a.pruneBeforeDeleteHook != nil {
			a.pruneBeforeDeleteHook(name)
		}
		deleted, deleteErr := a.deleteQueue(ctx, name, false)
		if errors.Is(deleteErr, errQueueNotPrunable) {
			result.Reason = pruneDeleteReason(deleteErr, "destination is no longer prunable")
			results = append(results, result)
			continue
		}
		if deleteErr != nil {
			return nil, classify("prune", driver.KindTransient, deleteErr)
		}
		if !deleted {
			result.Reason = "destination disappeared before deletion"
			results = append(results, result)
			continue
		}
		result.Deleted = true
		results = append(results, result)
	}
	return results, nil
}

func findQueue(queues []managementQueue, name string) (managementQueue, bool) {
	for _, queue := range queues {
		if queue.Name == name {
			return queue, true
		}
	}
	return managementQueue{}, false
}

func (a *adminOperations) pruneReason(name string, main managementQueue, mainReady int64, hasPark bool, parkReady int64) string {
	if main.Consumers > 0 || a.consumerCount(name) > 0 {
		return fmt.Sprintf("destination %q has consumers attached", name)
	}
	if mainReady > 0 {
		return fmt.Sprintf("destination %q holds %d ready message(s)", name, mainReady)
	}
	if hasPark && parkReady > 0 {
		return fmt.Sprintf("auxiliary %q holds %d ready message(s)", name+".park", parkReady)
	}
	return ""
}

func (a *adminOperations) consumerCount(destination string) int64 {
	a.conn.mu.RLock()
	defer a.conn.mu.RUnlock()
	var count int64
	for consumer := range a.conn.active {
		if _, ok := consumer.byName[destination]; ok {
			count++
		}
	}
	return count
}

func pruneDeleteReason(err error, fallback string) string {
	var guardErr *queuePruneGuardError
	if errors.As(err, &guardErr) {
		return guardErr.reason
	}
	return fallback
}

func queuePruneReason(name string, ready, consumers int64, auxiliary bool) string {
	if consumers > 0 {
		if auxiliary {
			return fmt.Sprintf("auxiliary %q has consumers attached", name)
		}
		return fmt.Sprintf("destination %q has consumers attached", name)
	}
	if ready > 0 {
		if auxiliary {
			return fmt.Sprintf("auxiliary %q holds %d ready message(s)", name, ready)
		}
		return fmt.Sprintf("destination %q holds %d ready message(s)", name, ready)
	}
	return ""
}

// deleteQueue uses broker preconditions for classic queues. Quorum queues
// support no conditional delete, so their final passive-declare recheck leaves
// a window where a newly arrived message can still be destroyed. The recheck
// runs on the channel that performs the delete, so no other channel's traffic
// can be interleaved between them.
func (a *adminOperations) deleteQueue(ctx context.Context, name string, auxiliary bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	channel, err := a.openChannel(ctx)
	if err != nil {
		return false, err
	}
	defer channel.Close()

	if a.conn.queueKind == queueKindQuorum {
		ready, consumers, err := inspectQueueOnChannel(channel, name)
		if err != nil {
			if isNotFound(err) {
				return false, nil
			}
			return false, err
		}
		if reason := queuePruneReason(name, ready, consumers, auxiliary); reason != "" {
			return false, &queuePruneGuardError{reason: reason}
		}
	}
	if a.conn.queueKind == queueKindQuorum {
		_, err = channel.QueueDelete(name, false, false, false)
	} else {
		_, err = channel.QueueDelete(name, true, true, false)
	}
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		if isPreconditionFailed(err) {
			return false, errQueueNotPrunable
		}
		return false, err
	}

	a.conn.mu.Lock()
	delete(a.conn.deferred, name)
	a.conn.mu.Unlock()
	return true, nil
}

func isPreconditionFailed(err error) bool {
	var amqpErr *amqp.Error
	return errors.As(err, &amqpErr) && amqpErr.Code == 406
}

func (a *adminOperations) inspectQueue(ctx context.Context, name string) (int64, error) {
	ready, _, err := a.inspectQueueWithConsumers(ctx, name)
	return ready, err
}

func (a *adminOperations) inspectQueueWithConsumers(ctx context.Context, name string) (int64, int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	channel, err := a.openChannel(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer channel.Close()
	return inspectQueueOnChannel(channel, name)
}

func inspectQueueOnChannel(channel *amqp.Channel, name string) (int64, int64, error) {
	queue, err := channel.QueueDeclarePassive(name, true, false, false, false, nil)
	if err != nil {
		return 0, 0, err
	}
	return int64(queue.Messages), int64(queue.Consumers), nil
}
