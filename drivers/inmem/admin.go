package inmem

import (
	"context"
	"fmt"
	"sort"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type admin struct{ conn *conn }

var _ driver.Admin = (*admin)(nil)

func (a *admin) EnsureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	if err := ctx.Err(); err != nil {
		return driver.TopologyDiff{}, classify("ensure_topology", driver.KindTransient, err)
	}
	a.conn.mu.Lock()
	defer a.conn.mu.Unlock()
	var diff driver.TopologyDiff
	for _, item := range spec.Destinations {
		if _, ok := a.conn.destinations[item.Name]; ok {
			diff.Existing = append(diff.Existing, item.Name)
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

func (a *admin) DescribeTopology(ctx context.Context, names []string) (driver.TopologyState, error) {
	if err := ctx.Err(); err != nil {
		return driver.TopologyState{}, classify("describe_topology", driver.KindTransient, err)
	}
	a.conn.mu.Lock()
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

func (a *admin) Purge(ctx context.Context, name string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, classify("purge", driver.KindTransient, err)
	}
	a.conn.mu.Lock()
	defer a.conn.mu.Unlock()
	item, ok := a.conn.destinations[name]
	if !ok {
		return 0, classify("purge", driver.KindNotFound, driver.ErrDestinationMissing)
	}
	n := int64(len(item.messages))
	item.messages = nil
	return n, nil
}

func (a *admin) Prune(ctx context.Context, names []string) ([]driver.PruneResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("prune", driver.KindTransient, err)
	}
	a.conn.mu.Lock()
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
		// The in-memory driver has no auxiliary destinations, so an empty
		// destination can be deleted after the other guards pass.
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
