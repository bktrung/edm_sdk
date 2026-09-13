package conformance

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// scopeGroup builds the minimum groupContext profileDestination reads: the
// profile, and the run id that has to separate two runs of that profile.
func scopeGroup(profile Profile, runID string) *groupContext {
	return &groupContext{profile: profile, runID: runID}
}

func TestProfileDestinationIsUniquePerRun(t *testing.T) {
	const destination = "topology.create"
	first := profileDestination(scopeGroup(ProfileFull, "run-one"), destination)
	second := profileDestination(scopeGroup(ProfileFull, "run-two"), destination)
	if first == second {
		t.Fatalf("profileDestination(%q) = %q for run ids run-one and run-two; two runs sharing a broker would create, count and prune one destination", destination, first)
	}
	if !strings.Contains(first, "run-one") || !strings.Contains(second, "run-two") {
		t.Fatalf("scoped names %q and %q do not carry their own run id", first, second)
	}
}

func TestProfileDestinationRoundTripsEveryShape(t *testing.T) {
	logical := []string{
		"name",
		"topology.create",
		"topology.orphan.under.low",
		"consume-earliest-new",
	}
	for _, profile := range []Profile{ProfileFull, ProfileStrictPortability} {
		group := scopeGroup(profile, "run-round-trip")
		for _, destination := range logical {
			scoped := profileDestination(group, destination)
			if got := unprofileDestination(group, scoped); got != destination {
				t.Errorf("profile=%s unprofileDestination(%q) = %q, want the logical %q", profile, scoped, got, destination)
			}
			// A name already carrying the scope must survive a second call, or
			// every helper that scopes a destination it was handed the scoped
			// form of would nest a second run id.
			if again := profileDestination(group, scoped); again != scoped {
				t.Errorf("profile=%s profileDestination(%q) = %q, want the scoped name unchanged", profile, scoped, again)
			}
		}
		scoped, reverse := profileDestinations(group, logical)
		for i, name := range scoped {
			if reverse[name] != logical[i] {
				t.Errorf("profile=%s logical[%q] = %q, want %q", profile, name, reverse[name], logical[i])
			}
		}
	}
}

// useOnlyGroups replaces the registered manifest for one test, so a test can
// run the suite against one group of its own.
func useOnlyGroups(t *testing.T, entries []manifestEntry, runners map[string]groupRunner) {
	t.Helper()
	manifest := groupManifest
	pending := pendingGroups
	registered := groupRunners
	groupManifest = entries
	pendingGroups = nil
	groupRunners = runners
	t.Cleanup(func() {
		groupManifest = manifest
		pendingGroups = pending
		groupRunners = registered
	})
}

// TestRunReclaimsEveryDestinationItCreates is the acceptance criterion of this
// row at the scale a unit test can hold: the destinations a passing run
// creates are gone from the broker when the run returns, measured by what the
// broker still holds and not by whether a cleanup ran.
func TestRunReclaimsEveryDestinationItCreates(t *testing.T) {
	useOnlyGroups(t, []manifestEntry{{name: "reclaim", declared: 1}}, map[string]groupRunner{
		"reclaim": func(group *groupContext) {
			group.Check("a check's destinations outlive neither the check nor the run", func(t *testing.T) {
				destination := profileDestination(group, "reclaim.created")
				producer := newProducer(t, group, destination, driver.ProducerConfig{Effective: group.effective})
				if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "reclaim.created"}); err != nil {
					t.Fatalf("Publish(%q): %v", destination, err)
				}
				consumer := newConsumer(t, group, destination, 1)
				ackMessage(t, group, receiveMessage(t, group, consumer))
				group.vector.Add(BehaviorEvent{ID: "reclaim-created", Outcome: "published", FinalDestination: "reclaim.created"})
			})
		},
	})

	conn := newProbeTestConn()
	Run(t, Suite{Driver: probeTestDriver{conn: conn}, NewInspector: probeTestInspector})

	// The broker started empty and the run is over, so every surviving
	// destination is one this run created and did not reclaim. The count
	// returning to zero is the assertion; an equal count under different names
	// would not be.
	if len(conn.queues) != 0 {
		t.Fatalf("broker holds %d destinations after the run, want the pre-run 0: %v", len(conn.queues), slices.Sorted(maps.Keys(conn.queues)))
	}
}

// orphanScanningConn answers a scoped EnsureTopology the way a real driver
// does, by listing what it holds under the scope and reporting it as orphaned.
// The other fakes ignore Scope, which is fine everywhere except here, where the
// point is that a destination the tracked set never saw is still reclaimed.
type orphanScanningConn struct {
	*probeTestConn
}

// reclaimTestDriver hands every Open call the one connection, the way a broker
// hands every connection the one broker.
type reclaimTestDriver struct {
	conn *orphanScanningConn
}

func (reclaimTestDriver) Name() string                      { return "reclaim-test" }
func (reclaimTestDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (d reclaimTestDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return d.conn, nil
}

func (c *orphanScanningConn) Admin() driver.Admin {
	return orphanScanningAdmin{probeTestAdmin: c.probeTestConn.Admin().(probeTestAdmin)}
}

type orphanScanningAdmin struct {
	probeTestAdmin
}

func (a orphanScanningAdmin) EnsureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	if len(spec.Scope) != 0 && len(spec.Destinations) == 0 {
		var diff driver.TopologyDiff
		for _, name := range slices.Sorted(maps.Keys(a.conn.queues)) {
			for _, scope := range spec.Scope {
				if strings.HasPrefix(name, scope) {
					diff.Orphaned = append(diff.Orphaned, driver.OrphanedDestination{Name: name})
					break
				}
			}
		}
		return diff, nil
	}
	return a.probeTestAdmin.EnsureTopology(ctx, spec)
}

// TestRunReclaimsADestinationItDidNotRecord covers the gap the tracked set
// cannot close: a check that opens its own connection and provisions a
// destination on it registers nothing with the suite, so the run has to find
// that destination on the broker rather than in its own bookkeeping.
func TestRunReclaimsADestinationItDidNotRecord(t *testing.T) {
	useOnlyGroups(t, []manifestEntry{{name: "reclaim", declared: 1}}, map[string]groupRunner{
		"reclaim": func(group *groupContext) {
			group.Check("a destination on a private connection is reclaimed too", func(t *testing.T) {
				recorded := profileDestination(group, "reclaim.recorded")
				producer := newProducer(t, group, recorded, driver.ProducerConfig{Effective: group.effective})
				if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: "reclaim.recorded"}); err != nil {
					t.Fatalf("Publish(%q): %v", recorded, err)
				}
				private, err := group.drv.Open(group.ctx, group.cfg)
				if err != nil {
					t.Fatalf("open private connection: %v", err)
				}
				t.Cleanup(func() { _ = private.Close(context.Background()) })
				unrecorded := profileDestination(group, "reclaim.unrecorded")
				if _, err := private.Admin().EnsureTopology(group.ctx, driver.TopologySpec{
					Destinations: []driver.DestinationSpec{{Name: unrecorded}},
				}); err != nil {
					t.Fatalf("EnsureTopology(%q) on the private connection: %v", unrecorded, err)
				}
				group.vector.Add(BehaviorEvent{ID: "reclaim-unrecorded", Outcome: "published", FinalDestination: "reclaim.unrecorded"})
			})
		},
	})

	conn := &orphanScanningConn{probeTestConn: newProbeTestConn()}
	Run(t, Suite{
		Driver: reclaimTestDriver{conn: conn},
		NewInspector: func(raw driver.Conn) (Inspect, error) {
			wrapped, ok := raw.(*orphanScanningConn)
			if !ok {
				return nil, errors.New("unexpected reclaim test connection")
			}
			return probeTestInspect(wrapped.probeTestConn), nil
		},
	})

	if len(conn.queues) != 0 {
		t.Fatalf("broker holds %d destinations after the run, want the pre-run 0: %v", len(conn.queues), slices.Sorted(maps.Keys(conn.queues)))
	}
}

// TestFailedGroupCleanupDeletesTheGroupDestinations covers the abort path: a
// group that fails mid-check hands its names to cleanup, and cleanup has to
// delete them rather than empty them, because a failing group is the normal
// case and its residue would otherwise accumulate at the run rate forever.
func TestFailedGroupCleanupDeletesTheGroupDestinations(t *testing.T) {
	raw := &runTestConn{
		queues:    make(map[string][]driver.OutboundMessage),
		unsettled: make(map[string]int),
		specs:     make(map[string]driver.DestinationSpec),
	}
	tracked := newTrackedConn(raw)
	ctx := context.Background()
	destination := "reclaim.failed-group"
	tracked.beginGroup()
	if _, err := tracked.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination}},
	}); err != nil {
		t.Fatalf("EnsureTopology(%q): %v", destination, err)
	}
	// Empty at the point of failure: a cleanup that only purges looks correct
	// from a message count and leaves the destination behind.
	if errs := tracked.cleanup(ctx); len(errs) != 0 {
		t.Fatalf("cleanup errors = %v, want none", errs)
	}
	if _, exists := raw.queues[destination]; exists {
		t.Fatalf("destination %q survived the failed group's cleanup", destination)
	}
}
