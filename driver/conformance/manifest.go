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
	{name: "publish", declared: 17},
	{name: "consume", declared: 19},
	{name: "settle", declared: 16},
	{name: "ordering", declared: 8},
	{name: "deferred", declared: 9},
	{name: "drain", declared: 16},
	{name: "rebalance", declared: 11},
	{name: "failure", declared: 23},
	{name: "topology", declared: 19},
	{name: "capability", declared: 27},
	{name: "lag", declared: 5},
}

// pendingGroups lists groups without registered runners.
var pendingGroups []string

type groupContext struct {
	t                   *testing.T
	ctx                 context.Context
	conn                driver.Conn
	inspect             Inspect
	profile             Profile
	effective           driver.Capabilities
	factoryCapabilities driver.Capabilities
	vector              BehaviorVector
	checks              int
	checkNames          map[string]struct{}
	skips               map[string]string
	inject              FaultInjector
	drv                 driver.Driver
	cfg                 driver.Config
	injectFactory       func(driver.Conn) (FaultInjector, error)
	deadline            DeadlineFixture
	report              *Report
}

func (g *groupContext) capability(capability, declared, status, evidence string) {
	g.report.Capabilities = append(g.report.Capabilities, CapabilityResult{
		Profile: g.profile, Capability: capability, Declared: declared, Status: status, Evidence: evidence,
	})
}

type groupRunner func(*groupContext)

func (g *groupContext) Check(name string, fn func(*testing.T)) {
	if g.checkNames == nil {
		g.checkNames = make(map[string]struct{})
	}
	if _, exists := g.checkNames[name]; exists {
		g.t.Errorf("conformance group has duplicate check name %q", name)
		return
	}
	g.checkNames[name] = struct{}{}
	if runCheck(g.t, name, fn) {
		if _, recorded := g.skips[name]; recorded {
			g.checks++
			return
		} else {
			g.t.Errorf("conformance check %q was skipped without an explicit fixture record", name)
			return
		}
	}
	g.checks++
}

// Skip records a fixture-gated check before marking its subtest skipped. It
// distinguishes an intentional unavailable-fixture result from t.Skip used
// to pad a group while still satisfying its manifest count.
func (g *groupContext) Skip(t *testing.T, name, reason string) {
	if g.skips == nil {
		g.skips = make(map[string]string)
	}
	g.skips[name] = reason
	t.Skip(reason)
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

// validateGroupCount requires the observed check count to equal the declared
// one exactly, in both directions. The declared count is the length of the
// group's specification, so a group that has grown past it has grown past the
// specification too. Reconciling the two means re-reading the specification and
// finding out which clause the extra check was serving, not moving the number
// to whatever the group happens to run.
func validateGroupCount(name string, declared, observed int) error {
	if observed != declared {
		return fmt.Errorf("conformance group %q ran %d checks; manifest declares %d", name, observed, declared)
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
