package conformance

import (
	"context"
	"errors"
	"maps"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// reclaimGroupRunners is the one group these tests run: a single check that
// creates one destination through the tracked connection, publishes to it and
// leaves it to the run's reclaim, the way a passing check does.
func reclaimGroupRunners(destination string) map[string]groupRunner {
	return map[string]groupRunner{
		"reclaim": func(group *groupContext) {
			group.Check("a check creates a destination and leaves it to the run", func(t *testing.T) {
				producer := newProducer(t, group, profileDestination(group, destination), driver.ProducerConfig{Effective: group.effective})
				if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: destination, Body: []byte("reclaim")}); err != nil {
					t.Fatalf("Publish(%q): %v", destination, err)
				}
			})
		},
	}
}

// TestRunReportsADestinationTheReclaimDidNotDelete is the acceptance this row
// exists for: a run that leaves a destination behind fails, and the failure names
// the destination. The connection reports the delete and keeps the destination,
// which is the failure the reclaim's own account cannot see, so nothing but a
// read taken after the reclaim can find it.
func TestRunReportsADestinationTheReclaimDidNotDelete(t *testing.T) {
	output := survivorRunOutput(t)
	for _, want := range []string{"left destinations behind", ".full.survivor", ".strict.survivor"} {
		if !strings.Contains(string(output), want) {
			t.Fatalf("Run output for a surviving destination omitted %q:\n%s", want, output)
		}
	}
}

const survivorRunMode = "CONFORMANCE_SURVIVOR_MODE"

// survivorRunOutput runs the suite in a child process, because a run that leaves
// a destination behind fails the test it was handed, and the message it fails
// with is what these tests are about.
func survivorRunOutput(t *testing.T) []byte {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestSurvivorRunHelper", "-test.v") //nolint:gosec // the harness re-executes its own test binary.
	cmd.Env = append(os.Environ(), survivorRunMode+"=1")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("Run passed with a destination its reclaim did not delete:\n%s", output)
	}
	return output
}

// TestRunReportsFailedDestinationRead is the acceptance for a reclaim
// verification read that fails for a reason other than a missing destination.
func TestRunReportsFailedDestinationRead(t *testing.T) {
	output := failedDestinationReadRunOutput(t)
	text := string(output)
	for _, profile := range []string{"full", "strict"} {
		verification := "conformance " + profile + " profile reclaim verification"
		if !strings.Contains(text, verification) {
			t.Fatalf("Run output omitted failed verification for %s profile:\n%s", profile, output)
		}
		leftBehind := "conformance " + profile + " profile left destinations behind"
		if strings.Contains(text, leftBehind) {
			t.Fatalf("Run counted an unreadable destination as left behind for %s profile:\n%s", profile, output)
		}
	}
	if !strings.Contains(text, "deliberate describe failure") {
		t.Fatalf("Run output omitted the DescribeTopology failure:\n%s", output)
	}
}

const failedDestinationReadRunMode = "CONFORMANCE_FAILED_DESTINATION_READ_MODE"

// failedDestinationReadRunOutput runs the suite in a child process, because a
// failed reclaim verification is reported through the testing package.
func failedDestinationReadRunOutput(t *testing.T) []byte {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestFailedDestinationReadRunHelper", "-test.v") //nolint:gosec // the harness re-executes its own test binary.
	cmd.Env = append(os.Environ(), failedDestinationReadRunMode+"=1")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("Run passed after a failed destination read:\n%s", output)
	}
	return output
}

// TestFailedDestinationReadRunHelper is the child process failedDestinationReadRunOutput drives.
func TestFailedDestinationReadRunHelper(t *testing.T) {
	if os.Getenv(failedDestinationReadRunMode) == "" {
		return
	}
	useOnlyGroups(t, []manifestEntry{{name: "reclaim", declared: 1}}, reclaimGroupRunners("reclaim.read-failure"))
	conn := &recordingAdminConn{
		probeTestConn:   newProbeTestConn(),
		describeErrName: "read-failure",
		describeErr:     errors.New("deliberate describe failure"),
	}
	Run(t, Suite{
		Driver: fixedConnDriver{conn: conn},
		NewInspector: func(raw driver.Conn) (Inspect, error) {
			wrapped, ok := raw.(*recordingAdminConn)
			if !ok {
				return nil, errors.New("unexpected recording test connection")
			}
			return probeTestInspect(wrapped.probeTestConn), nil
		},
	})
	t.Fatal("Run returned after an expected verification failure")
}

// TestSurvivorRunHelper is the child process survivorRunOutput drives.
func TestSurvivorRunHelper(t *testing.T) {
	if os.Getenv(survivorRunMode) == "" {
		return
	}
	useOnlyGroups(t, []manifestEntry{{name: "reclaim", declared: 1}}, reclaimGroupRunners("reclaim.survivor"))
	conn := &keepingConn{probeTestConn: newProbeTestConn(), keep: "survivor"}
	Run(t, Suite{
		Driver: fixedConnDriver{conn: conn},
		NewInspector: func(raw driver.Conn) (Inspect, error) {
			wrapped, ok := raw.(*keepingConn)
			if !ok {
				return nil, errors.New("unexpected keeping test connection")
			}
			return probeTestInspect(wrapped.probeTestConn), nil
		},
	})
	t.Fatal("Run returned after an expected failure")
}

// TestRunKeepsADestinationTheFixtureCarriedBeforeIt covers the other half of the
// acceptance: a fixture with history is normal. The seeded destination shares its
// first component with one the run creates, and the connection answers a scoped
// scan, so the reclaim and the measurement can both see it and it is outside this
// run's scope for one reason only: its name does not carry this run's id. The run
// has to remove its own destination and leave the seeded one alone.
func TestRunKeepsADestinationTheFixtureCarriedBeforeIt(t *testing.T) {
	useOnlyGroups(t, []manifestEntry{{name: "reclaim", declared: 1}}, reclaimGroupRunners("reclaim.fresh"))
	const seeded = "reclaim.seeded.history"
	conn := &orphanScanningConn{probeTestConn: newProbeTestConn()}
	conn.queues[seeded] = nil

	Run(t, Suite{
		Driver: fixedConnDriver{conn: conn},
		NewInspector: func(raw driver.Conn) (Inspect, error) {
			wrapped, ok := raw.(*orphanScanningConn)
			if !ok {
				return nil, errors.New("unexpected orphan scanning test connection")
			}
			return probeTestInspect(wrapped.probeTestConn), nil
		},
	})

	if remaining := slices.Sorted(maps.Keys(conn.queues)); !slices.Equal(remaining, []string{seeded}) {
		t.Fatalf("broker holds %v after the run, want only %q, the destination the run did not create", remaining, seeded)
	}
}

// TestRunLooksForSurvivorsAfterItReclaims pins the ordering this row turns on:
// the run's destinations are gone from the broker before anything reads them
// back. A check registered as a second cleanup, or registered on the wrong side
// of the reclaim, runs first under LIFO and reports every destination the
// reclaim is about to delete.
func TestRunLooksForSurvivorsAfterItReclaims(t *testing.T) {
	useOnlyGroups(t, []manifestEntry{{name: "reclaim", declared: 1}}, reclaimGroupRunners("reclaim.order"))
	conn := &recordingAdminConn{probeTestConn: newProbeTestConn()}

	Run(t, Suite{
		Driver: fixedConnDriver{conn: conn},
		NewInspector: func(raw driver.Conn) (Inspect, error) {
			wrapped, ok := raw.(*recordingAdminConn)
			if !ok {
				return nil, errors.New("unexpected recording test connection")
			}
			return probeTestInspect(wrapped.probeTestConn), nil
		},
	})

	prunes, describes := reclaimOrder(conn.events, ".order")
	// One read per profile: a survival check that read nothing at all would
	// pass every comparison below by doing nothing.
	if len(describes) != 2 {
		t.Fatalf("the run read %d of its own destinations back, want one per profile: events = %v", len(describes), conn.events)
	}
	for name, describe := range describes {
		prune, reclaimed := prunes[name]
		if !reclaimed {
			t.Fatalf("the run read %q for survival without reclaiming it: events = %v", name, conn.events)
		}
		if describe < prune {
			t.Fatalf("the run read %q for survival at event %d, before the reclaim that deletes it at event %d: events = %v", name, describe, prune, conn.events)
		}
	}
}

// reclaimOrder maps each destination the run reclaimed to the position of the
// last prune of it, and to the position of the first read that looks for it
// afterwards. The positions are per destination and not global, because the two
// profiles of one run reclaim and read two different names.
func reclaimOrder(events []string, suffix string) (prunes, describes map[string]int) {
	prunes = make(map[string]int)
	describes = make(map[string]int)
	for index, event := range events {
		kind, name, ok := strings.Cut(event, ":")
		if !ok || !strings.HasSuffix(name, suffix) {
			continue
		}
		switch kind {
		case "prune":
			prunes[name] = index
		case "describe":
			if _, seen := describes[name]; !seen {
				describes[name] = index
			}
		}
	}
	return prunes, describes
}

// fixedConnDriver hands every Open call the one connection, the way a broker
// hands every connection the one broker.
type fixedConnDriver struct{ conn driver.Conn }

func (fixedConnDriver) Name() string                      { return "reclaim-verification-test" }
func (fixedConnDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (d fixedConnDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return d.conn, nil
}

// keepingConn answers Prune for one destination the way a driver that reports a
// deletion it did not perform would: the destination stays on the broker and the
// result says it was deleted. Everything else is the probe connection, so the
// only thing under test is what the run does when a reclaim does not hold.
type keepingConn struct {
	*probeTestConn
	keep string
}

func (c *keepingConn) Admin() driver.Admin {
	return keepingAdmin{probeTestAdmin: c.probeTestConn.Admin().(probeTestAdmin), keep: c.keep}
}

type keepingAdmin struct {
	probeTestAdmin
	keep string
}

func (a keepingAdmin) Prune(ctx context.Context, names []string) ([]driver.PruneResult, error) {
	kept := make([]driver.PruneResult, 0, len(names))
	prunable := make([]string, 0, len(names))
	for _, name := range names {
		if strings.Contains(name, a.keep) {
			kept = append(kept, driver.PruneResult{Name: name, Deleted: true})
			continue
		}
		prunable = append(prunable, name)
	}
	deleted, err := a.probeTestAdmin.Prune(ctx, prunable)
	if err != nil {
		return nil, err
	}
	return append(kept, deleted...), nil
}

// recordingAdminConn records the order of the deletes and the reads the run makes
// of a destination.
type recordingAdminConn struct {
	*probeTestConn
	events          []string
	describeErrName string
	describeErr     error
}

func (c *recordingAdminConn) Admin() driver.Admin {
	return recordingAdmin{
		probeTestAdmin:  c.probeTestConn.Admin().(probeTestAdmin),
		events:          &c.events,
		describeErrName: c.describeErrName,
		describeErr:     c.describeErr,
	}
}

type recordingAdmin struct {
	probeTestAdmin
	events          *[]string
	describeErrName string
	describeErr     error
}

func (a recordingAdmin) DescribeTopology(ctx context.Context, names []string) (driver.TopologyState, error) {
	for _, name := range names {
		*a.events = append(*a.events, "describe:"+name)
		if a.describeErr != nil && strings.Contains(name, a.describeErrName) {
			return driver.TopologyState{}, a.describeErr
		}
	}
	return a.probeTestAdmin.DescribeTopology(ctx, names)
}

func (a recordingAdmin) Prune(ctx context.Context, names []string) ([]driver.PruneResult, error) {
	results, err := a.probeTestAdmin.Prune(ctx, names)
	for _, name := range names {
		*a.events = append(*a.events, "prune:"+name)
	}
	return results, err
}
