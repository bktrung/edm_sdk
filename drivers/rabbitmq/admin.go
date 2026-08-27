package rabbitmq

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type adminOperations struct{ conn *conn }

type admin struct{ operations *adminOperations }

var (
	_ driver.Admin       = (*admin)(nil)
	_ driver.Maintenance = (*admin)(nil)
	_ driver.Maintenance = (*adminOperations)(nil)
)

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
		park, hasPark := findQueue(queues, parkName)
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
		if reason := a.pruneReason(name, main, mainReady, park, hasPark, parkReady); reason != "" {
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
			park, hasPark = findQueue(queues, parkName)
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
			if reason := a.pruneReason(name, main, mainReady, park, hasPark, parkReady); reason != "" {
				result.Reason = reason
				results = append(results, result)
				continue
			}
			deleted, deleteErr := a.deleteQueue(ctx, parkName)
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
		if reason := a.pruneReason(name, main, mainReady, managementQueue{}, false, 0); reason != "" {
			result.Reason = reason
			results = append(results, result)
			continue
		}
		deleted, deleteErr := a.deleteQueue(ctx, name)
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

func (a *adminOperations) pruneReason(name string, main managementQueue, mainReady int64, park managementQueue, hasPark bool, parkReady int64) string {
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

func (a *adminOperations) deleteQueue(ctx context.Context, name string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	a.conn.mu.RLock()
	channel := a.conn.ephemeral[name]
	a.conn.mu.RUnlock()
	owned := channel != nil
	if !owned {
		var err error
		channel, err = a.conn.amqp.Channel()
		if err != nil {
			return false, err
		}
	}
	if !owned {
		defer channel.Close()
	}
	_, err := channel.QueueDelete(name, false, false, false)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	a.conn.mu.Lock()
	delete(a.conn.ephemeral, name)
	delete(a.conn.deferred, name)
	a.conn.mu.Unlock()
	return true, nil
}

func (a *adminOperations) inspectQueue(ctx context.Context, name string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	a.conn.mu.RLock()
	channel := a.conn.ephemeral[name]
	a.conn.mu.RUnlock()
	owned := channel != nil
	if !owned {
		var err error
		channel, err = a.conn.amqp.Channel()
		if err != nil {
			return 0, err
		}
	}
	if !owned {
		defer channel.Close()
	}
	durable, exclusive := !owned, owned
	queue, err := channel.QueueDeclarePassive(name, durable, false, exclusive, false, nil)
	if err != nil {
		return 0, err
	}
	return int64(queue.Messages), nil
}
