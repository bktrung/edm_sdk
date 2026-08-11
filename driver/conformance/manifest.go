package conformance

import (
	"context"
	"fmt"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type manifestEntry struct {
	name     string
	declared int
}

var groupManifest = []manifestEntry{
	{name: "publish", declared: 14},
	{name: "consume", declared: 18},
	{name: "settle", declared: 16},
	{name: "ordering", declared: 9},
	{name: "deferred", declared: 9},
	{name: "drain", declared: 12},
	{name: "rebalance", declared: 11},
	{name: "failure", declared: 15},
	{name: "topology", declared: 15},
	{name: "capability", declared: 24},
	{name: "lag", declared: 5},
}

// pendingGroups lists groups without registered runners.
var pendingGroups = []string{
	"ordering", "deferred",
	"drain", "rebalance", "failure", "capability", "lag",
}

type groupContext struct {
	t         *testing.T
	ctx       context.Context
	conn      driver.Conn
	inspect   Inspect
	profile   Profile
	effective driver.Capabilities
	vector    BehaviorVector
	checks    int
}

type groupRunner func(*groupContext)

func (g *groupContext) Check(name string, fn func(*testing.T)) {
	if runCheck(g.t, name, fn) {
		g.t.Errorf("conformance check %q was skipped", name)
		return
	}
	g.checks++
}

func runCheck(t *testing.T, name string, fn func(*testing.T)) (skipped bool) {
	t.Helper()
	t.Run(name, func(checkTest *testing.T) {
		defer func() { skipped = checkTest.Skipped() }()
		fn(checkTest)
	})
	return skipped
}

var groupRunners = map[string]groupRunner{}

func registerGroup(name string, runner groupRunner) {
	for _, entry := range groupManifest {
		if entry.name == name {
			if _, exists := groupRunners[name]; exists {
				panic("duplicate conformance group: " + name)
			}
			groupRunners[name] = runner
			return
		}
	}
	panic("conformance group missing from manifest: " + name)
}

func validateGroupCount(name string, declared, observed int) error {
	if observed < declared {
		return fmt.Errorf("conformance group %q ran %d checks; manifest requires at least %d", name, observed, declared)
	}
	return nil
}

func validateManifestState(manifest []manifestEntry, pending []string, runners map[string]groupRunner) error {
	pendingSet := make(map[string]bool, len(pending))
	for _, name := range pending {
		if pendingSet[name] {
			return fmt.Errorf("conformance group %q appears twice in pending list", name)
		}
		pendingSet[name] = true
	}
	for _, entry := range manifest {
		_, implemented := runners[entry.name]
		if implemented == pendingSet[entry.name] {
			return fmt.Errorf("conformance group %q must be exactly one of implemented or pending", entry.name)
		}
	}
	for _, name := range pending {
		found := false
		for _, entry := range manifest {
			if entry.name == name {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("pending conformance group %q is absent from manifest", name)
		}
	}
	for name := range runners {
		found := false
		for _, entry := range manifest {
			if entry.name == name {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("implemented conformance group %q is absent from manifest", name)
		}
	}
	return nil
}
