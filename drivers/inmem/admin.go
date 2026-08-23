package inmem

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type adminOperations struct{ conn *conn }

type admin struct{ operations *adminOperations }

var _ driver.Admin = (*admin)(nil)

func (a *admin) EnsureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	return a.operations.EnsureTopology(ctx, spec)
}

func (a *admin) DescribeTopology(ctx context.Context, names []string) (driver.TopologyState, error) {
	return a.operations.DescribeTopology(ctx, names)
}

func (a *admin) Purge(ctx context.Context, name string) (int64, error) {
	return a.operations.Purge(ctx, name)
}

func (a *admin) Prune(ctx context.Context, names []string) ([]driver.PruneResult, error) {
	return a.operations.Prune(ctx, names)
}

// begin is the Admin facade gate. It holds the connection lock so lifecycle admission and the operation cannot be separated.
func (a *adminOperations) begin(ctx context.Context, operation string) error {
	if err := ctx.Err(); err != nil {
		return classify(operation, driver.KindTransient, err)
	}
	a.conn.mu.Lock()
	if a.conn.closed || a.conn.closing {
		a.conn.mu.Unlock()
		return classify(operation, driver.KindTransient, errors.New("connection closed"))
	}
	return nil
}

func (a *adminOperations) EnsureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	if err := a.begin(ctx, "ensure_topology"); err != nil {
		return driver.TopologyDiff{}, err
	}
	defer a.conn.mu.Unlock()
	if spec.Policy == driver.TopologyNone {
		return driver.TopologyDiff{}, nil
	}
	var diff driver.TopologyDiff
	if spec.Policy == driver.TopologyVerify {
		for _, item := range spec.Destinations {
			stored, ok := a.conn.destinations[item.Name]
			if !ok {
				return diff, classify("ensure_topology", driver.KindNotFound, fmt.Errorf("%s: %w", item.Name, driver.ErrDestinationMissing))
			}
			diff.ExistingDestinations = append(diff.ExistingDestinations, item.Name)
			diff.Drifted = append(diff.Drifted, destinationArgumentDrift(item, stored.spec)...)
		}
		return diff, nil
	}
	for _, item := range spec.Destinations {
		if _, ok := a.conn.destinations[item.Name]; ok {
			diff.ExistingDestinations = append(diff.ExistingDestinations, item.Name)
			continue
		}
		a.conn.destinations[item.Name] = &destination{spec: item, consumers: make(map[*consumer]struct{}), affinity: make(map[string]*consumer)}
		diff.CreatedDestinations = append(diff.CreatedDestinations, item.Name)
	}
	if len(spec.Scope) == 0 {
		diff.OrphanScanError = "TopologySpec.Scope is empty: not scanning for orphans"
	} else {
		wanted := make(map[string]struct{}, len(spec.Destinations))
		for _, item := range spec.Destinations {
			wanted[item.Name] = struct{}{}
		}
		names := make([]string, 0, len(a.conn.destinations))
		for name := range a.conn.destinations {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			item := a.conn.destinations[name]
			if !inScope(name, spec.Scope) {
				continue
			}
			if _, ok := wanted[name]; !ok {
				diff.Orphaned = append(diff.Orphaned, driver.OrphanedDestination{Name: name, Messages: int64(len(item.messages))})
			}
		}
	}
	return diff, nil
}

func destinationArgumentDrift(want, got driver.DestinationSpec) []driver.ArgumentDrift {
	var drifted []driver.ArgumentDrift
	if want.Delay > 0 && want.Delay != got.Delay {
		drifted = append(drifted, driver.ArgumentDrift{
			Name: want.Name, Argument: "delay", Want: want.Delay.String(), Got: got.Delay.String(),
		})
	}
	if want.DeliveryLimit > 0 && want.DeliveryLimit != got.DeliveryLimit {
		gotValue := "<absent>"
		if got.DeliveryLimit > 0 {
			gotValue = fmt.Sprint(got.DeliveryLimit)
		}
		drifted = append(drifted, driver.ArgumentDrift{
			Name: want.Name, Argument: "x-delivery-limit", Want: fmt.Sprint(want.DeliveryLimit), Got: gotValue,
		})
	}
	if want.DeadLetter != nil {
		gotExchange, gotKey := "<absent>", "<absent>"
		if got.DeadLetter != nil {
			gotExchange, gotKey = got.DeadLetter.Exchange, got.DeadLetter.Key
		}
		if want.DeadLetter.Exchange != gotExchange {
			drifted = append(drifted, driver.ArgumentDrift{
				Name: want.Name, Argument: "x-dead-letter-exchange", Want: want.DeadLetter.Exchange, Got: gotExchange,
			})
		}
		if want.DeadLetter.Key != gotKey {
			drifted = append(drifted, driver.ArgumentDrift{
				Name: want.Name, Argument: "x-dead-letter-routing-key", Want: want.DeadLetter.Key, Got: gotKey,
			})
		}
	}
	return drifted
}

func (a *adminOperations) DescribeTopology(ctx context.Context, names []string) (driver.TopologyState, error) {
	if err := a.begin(ctx, "describe_topology"); err != nil {
		return driver.TopologyState{}, err
	}
	defer a.conn.mu.Unlock()
	a.conn.dispatchLocked()
	state := driver.TopologyState{Depth: make(map[string]int64, len(names))}
	for _, name := range names {
		item, ok := a.conn.destinations[name]
		if !ok {
			return state, classify("describe_topology", driver.KindNotFound, driver.ErrDestinationMissing)
		}
		state.Depth[name] = int64(len(item.messages))
	}
	return state, nil
}

func (a *adminOperations) Purge(ctx context.Context, name string) (int64, error) {
	if err := a.begin(ctx, "purge"); err != nil {
		return 0, err
	}
	defer a.conn.mu.Unlock()
	item, ok := a.conn.destinations[name]
	if !ok {
		return 0, classify("purge", driver.KindNotFound, driver.ErrDestinationMissing)
	}
	n := int64(len(item.messages))
	item.messages = nil
	return n, nil
}

func (a *adminOperations) Prune(ctx context.Context, names []string) ([]driver.PruneResult, error) {
	if err := a.begin(ctx, "prune"); err != nil {
		return nil, err
	}
	defer a.conn.mu.Unlock()
	results := make([]driver.PruneResult, 0, len(names))
	for _, name := range names {
		item, ok := a.conn.destinations[name]
		if !ok {
			results = append(results, driver.PruneResult{Name: name, Reason: "destination does not exist"})
			continue
		}
		if len(item.messages) > 0 {
			count := len(item.messages)
			noun := "messages"
			if count == 1 {
				noun = "message"
			}
			results = append(results, driver.PruneResult{Name: name, Reason: fmt.Sprintf("holds %d %s", count, noun)})
			continue
		}
		if len(item.consumers) > 0 {
			results = append(results, driver.PruneResult{Name: name, Reason: "consumer attached"})
			continue
		}
		// This driver has no auxiliary destinations to delete.
		delete(a.conn.destinations, name)
		results = append(results, driver.PruneResult{Name: name, Deleted: true})
	}
	return results, nil
}

func inScope(name string, scopes []string) bool {
	for _, scope := range scopes {
		if len(name) >= len(scope) && name[:len(scope)] == scope {
			return true
		}
	}
	return false
}
