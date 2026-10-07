package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"strings"

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

// Error returns the reason the requested queue cannot be pruned.
func (e *queuePruneGuardError) Error() string { return e.reason }

// Unwrap returns the error identifying a queue that no longer meets the prune conditions.
func (e *queuePruneGuardError) Unwrap() error { return errQueueNotPrunable }

// EnsureTopology applies the requested topology policy and returns the resulting diff.
func (a *admin) EnsureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	return a.operations.EnsureTopology(ctx, spec)
}

// DescribeTopology returns the current topology state for the requested names.
func (a *admin) DescribeTopology(ctx context.Context, names []string) (driver.TopologyState, error) {
	return a.operations.DescribeTopology(ctx, names)
}

// Purge removes messages from a destination and its parking queue, and returns the number removed.
func (a *admin) Purge(ctx context.Context, destination string) (int64, error) {
	return a.operations.Purge(ctx, destination)
}

// Prune processes each requested name and reports whether it was deleted or why it was retained.
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

// EnsureTopology applies the requested topology policy and returns the resulting diff.
func (a *adminOperations) EnsureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	if err := a.admission(ctx, "ensure_topology"); err != nil {
		return driver.TopologyDiff{}, err
	}
	return a.ensureTopology(ctx, spec)
}

// DescribeTopology returns the current topology state for the requested names.
func (a *adminOperations) DescribeTopology(ctx context.Context, names []string) (driver.TopologyState, error) {
	if err := a.admission(ctx, "describe_topology"); err != nil {
		return driver.TopologyState{}, err
	}
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
		if parkName, isDeferred := a.conn.parkingOf(name); isDeferred {
			parked, parkErr := a.inspectQueue(ctx, parkName)
			if parkErr != nil && !isNotFound(parkErr) {
				return driver.TopologyState{}, classifyAMQP("describe_topology", driver.KindTransient, parkErr)
			}
			depth[name] += int64(parked)
		}
	}
	return driver.TopologyState{Depth: depth}, nil
}

// Purge removes messages from a destination and its parking queue, and returns the number removed.
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
	parkName, isDeferred := a.conn.parkingOf(destination)
	if !isDeferred {
		return int64(count), nil
	}
	purged, err := channel.QueuePurge(parkName, false)
	if err != nil {
		// A parking queue that is not there holds nothing: under TopologyNone
		// the operator may not have provisioned it yet, and Purge of the
		// destination is still the operation an application calls to empty it.
		if isNotFound(err) {
			return int64(count), nil
		}
		return int64(count), classifyAMQP("purge", driver.KindTransient, err)
	}
	return int64(count) + int64(purged), nil
}

// Prune processes each requested name and reports whether it was deleted or why it was retained.
func (a *adminOperations) Prune(ctx context.Context, names []string) ([]driver.PruneResult, error) {
	if err := a.admission(ctx, "prune"); err != nil {
		return nil, err
	}
	queues, err := a.conn.management.listQueues(ctx)
	if err != nil {
		return nil, classifyManagement("prune", err)
	}
	results := make([]driver.PruneResult, 0, len(names))
	exchangeListLoaded := false
	var exchanges []managementExchange
	bindingsLoaded := false
	var bindings []managementBinding

	for _, name := range names {
		result := driver.PruneResult{Name: name}
		_, exists := findQueue(queues, name)
		if !exists {
			if !exchangeListLoaded {
				exchanges, err = a.conn.management.listExchanges(ctx)
				if err != nil {
					return nil, classifyManagement("prune", err)
				}
				exchangeListLoaded = true
			}
			if _, exists = findExchange(exchanges, name); !exists {
				result.Reason = "destination does not exist"
				results = append(results, result)
				continue
			}
			if isBuiltInExchange(name) {
				result.Reason = fmt.Sprintf("exchange %q is built-in and cannot be deleted", name)
				results = append(results, result)
				continue
			}

			if !bindingsLoaded {
				bindings, err = a.conn.management.listBindings(ctx)
				if err != nil {
					return nil, classifyManagement("prune", err)
				}
				bindingsLoaded = true
			}

			for _, binding := range bindings {
				if binding.Source != name {
					continue
				}
				result.Reason = fmt.Sprintf("exchange %q has binding to destination %q", name, binding.Destination)
				break
			}
			if result.Reason != "" {
				results = append(results, result)
				continue
			}
			deleted, deleteErr := a.conn.management.deleteExchange(ctx, name)
			if deleteErr != nil {
				return nil, classifyManagement("prune", deleteErr)
			}
			if !deleted {
				result.Reason = "exchange disappeared before deletion"
				results = append(results, result)
				continue
			}
			result.Deleted = true
			results = append(results, result)
			continue
		}

		parking := a.existingParkQueues(queues, name)
		if reason := a.pruneGuard(ctx, name, parking); reason != "" {
			result.Reason = reason
			results = append(results, result)
			continue
		}
		if len(parking) > 0 {
			queues, err = a.conn.management.listQueues(ctx)
			if err != nil {
				return nil, classifyManagement("prune", err)
			}
			if _, exists = findQueue(queues, name); !exists {
				result.Reason = "destination disappeared before deletion"
				results = append(results, result)
				continue
			}
			if reason := a.pruneGuard(ctx, name, parking); reason != "" {
				result.Reason = reason
				results = append(results, result)
				continue
			}
			deletedAll := true
			for _, parkName := range parking {
				if a.pruneBeforeDeleteHook != nil {
					a.pruneBeforeDeleteHook(parkName)
				}
				deleted, deleteErr := a.deleteQueue(ctx, parkName, true)
				if errors.Is(deleteErr, errQueueNotPrunable) {
					result.Reason = pruneDeleteReason(deleteErr, "parking destination is no longer prunable")
					deletedAll = false
					break
				}
				if deleteErr != nil {
					return nil, classify("prune", driver.KindTransient, deleteErr)
				}
				if !deleted {
					result.Reason = "parking destination disappeared before deletion"
					deletedAll = false
					break
				}
			}
			if !deletedAll {
				results = append(results, result)
				continue
			}
		}
		queues, err = a.conn.management.listQueues(ctx)
		if err != nil {
			return nil, classifyManagement("prune", err)
		}
		_, exists = findQueue(queues, name)
		if !exists {
			result.Reason = "destination disappeared before deletion"
			results = append(results, result)
			continue
		}
		if reason := a.pruneGuard(ctx, name, nil); reason != "" {
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

func findExchange(exchanges []managementExchange, name string) (managementExchange, bool) {
	for _, exchange := range exchanges {
		if exchange.Name == name {
			return exchange, true
		}
	}
	return managementExchange{}, false
}

func isBuiltInExchange(name string) bool {
	return name == "" || strings.HasPrefix(name, "amq.")
}

// pruneReason reports why a destination may not be deleted yet, or an empty
// string when it may. auxiliary names the one parking queue whose ready count
// was just read, and is empty for the check that runs after the parking queues
// are gone.
func (a *adminOperations) pruneReason(name string, mainConsumers, mainReady int64, auxiliary string, auxiliaryReady int64) string {
	if mainConsumers > 0 || a.consumerCount(name) > 0 {
		return fmt.Sprintf("destination %q has consumers attached", name)
	}
	if mainReady > 0 {
		return fmt.Sprintf("destination %q holds %d ready message(s)", name, mainReady)
	}
	if auxiliary != "" && auxiliaryReady > 0 {
		return fmt.Sprintf("auxiliary %q holds %d ready message(s)", auxiliary, auxiliaryReady)
	}
	return ""
}

// pruneGuard inspects a destination and the parking queues given and returns
// the first reason the destination may not be deleted, or an empty string when
// every inspected queue is there, empty and unattached. An auxiliary list that
// is empty checks the destination alone, which is the check that runs once the
// parking queues are gone, and a destination that has disappeared is reported
// as such rather than as a queue that failed its guard.
func (a *adminOperations) pruneGuard(ctx context.Context, destination string, auxiliary []string) string {
	mainReady, mainConsumers, err := a.inspectQueueWithConsumers(ctx, destination)
	if err != nil {
		return "destination disappeared before deletion"
	}
	if reason := a.pruneReason(destination, mainConsumers, mainReady, "", 0); reason != "" {
		return reason
	}
	for _, parkName := range auxiliary {
		parkReady, parkErr := a.inspectQueue(ctx, parkName)
		if parkErr != nil {
			return "parking destination disappeared before deletion"
		}
		if reason := a.pruneReason(destination, mainConsumers, mainReady, parkName, parkReady); reason != "" {
			return reason
		}
	}
	return ""
}

// existingParkQueues lists the parking queue of a destination when the broker
// has it right now. The name depends only on the destination, so it is checked
// whether or not this connection declared a delay for it: a queue left by an
// earlier declaration still guards the destination. A parking queue that is not
// there holds nothing and needs no guard.
func (a *adminOperations) existingParkQueues(queues []managementQueue, destination string) []string {
	parkName := parkQueueName(destination)
	if _, found := findQueue(queues, parkName); found {
		return []string{parkName}
	}
	return nil
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
// a window where a message published or a consumer attached on another
// channel between the recheck and the delete is destroyed with the queue.
// Running both on one channel orders them against each other, not against
// other channels' traffic.
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
