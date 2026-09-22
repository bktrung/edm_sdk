package conformance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestRunRejectsZeroInspector(t *testing.T) {
	assertRunFailure(t, "zero-inspector", "ready delta")
}

func TestRunRejectsInspectorFactoryError(t *testing.T) {
	assertRunFailure(t, "factory-error", "admin unavailable")
}

func TestRunRejectsShortRegisteredGroup(t *testing.T) {
	assertRunFailure(t, "short-group", "ran 1 checks; manifest declares 2")
}

func TestRunRejectsLongRegisteredGroup(t *testing.T) {
	assertRunFailure(t, "long-group", "ran 2 checks; manifest declares 1")
}

func TestRunRejectsDuplicateRegisteredCheckName(t *testing.T) {
	assertRunFailure(t, "duplicate-group", "duplicate check name")
}

func TestRunRejectsUnrecordedSkippedCheck(t *testing.T) {
	assertRunFailure(t, "unrecorded-group", "skipped without an explicit fixture record")
}

func TestRunReportsMultipleFailedGroups(t *testing.T) {
	output := runFailureOutput(t, "two-failed-groups")
	for _, want := range []string{
		"conformance group publish failed",
		"conformance group consume failed",
	} {
		if !strings.Contains(string(output), want) {
			t.Fatalf("Run failure omitted %q:\n%s", want, output)
		}
	}
}

func TestRunContinuesAfterProfileFailure(t *testing.T) {
	output := runFailureOutput(t, "profile-failure")
	if !strings.Contains(string(output), "TestRunFailureHelper/strict") {
		t.Fatalf("strict profile did not run after full profile failure:\n%s", output)
	}
}

func TestRunContinuesAfterProfileSetupFailure(t *testing.T) {
	output := runFailureOutput(t, "profile-setup-failure")
	if !strings.Contains(string(output), "TestRunFailureHelper/strict") {
		t.Fatalf("strict profile did not run after full profile setup failure:\n%s", output)
	}
}

func TestRunEmitsReportWhenVectorComparisonFails(t *testing.T) {
	output := runFailureOutput(t, "vector-diff")
	text := string(output)
	if !strings.Contains(text, "full and strict behavior vectors differ") {
		t.Fatalf("vector comparison did not fail:\n%s", text)
	}
	if !strings.Contains(text, "conformance report:") {
		t.Fatalf("conformance report was not emitted after vector comparison failure:\n%s", text)
	}
}

func TestRunValidatesCountAfterFailedGroup(t *testing.T) {
	output := runFailureOutput(t, "failed-short-group")
	if !strings.Contains(string(output), `conformance group "publish" ran 1 checks; manifest declares 2`) {
		t.Fatalf("Run failure omitted count validation:\n%s", output)
	}
}

func TestRunUsesOneConnectionAndInspector(t *testing.T) {
	manifest := groupManifest
	pending := pendingGroups
	runners := groupRunners
	groupManifest = nil
	pendingGroups = nil
	groupRunners = map[string]groupRunner{}
	defer func() {
		groupManifest = manifest
		pendingGroups = pending
		groupRunners = runners
	}()

	opens := 0
	inspectors := 0
	report := Run(t, Suite{
		Driver: runTestDriver{opens: &opens},
		NewInspector: func(conn driver.Conn) (Inspect, error) {
			inspectors++
			return runTestInspector(conn)
		},
	})
	if opens != 1 {
		t.Fatalf("Run opened %d connections; want 1", opens)
	}
	if inspectors != 1 {
		t.Fatalf("Run built %d inspectors; want 1", inspectors)
	}
	if len(report.Profiles) != 2 {
		t.Fatalf("Run returned %d profile reports; want 2", len(report.Profiles))
	}
}

func TestTrackedAdminRecordsMaintenanceDestinations(t *testing.T) {
	raw := &runTestConn{
		queues:    make(map[string][]driver.OutboundMessage),
		unsettled: make(map[string]int),
	}
	raw.queues["tracked.prune.a"] = nil
	raw.queues["tracked.prune.b"] = nil
	tracked := newTrackedConn(raw)
	admin, ok := tracked.Admin().(driver.Maintenance)
	if !ok {
		t.Fatal("tracked admin does not expose driver.Maintenance")
	}
	if _, err := admin.Purge(context.Background(), "tracked.purge"); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	results, err := admin.Prune(context.Background(), []string{"tracked.prune.a", "tracked.prune.b"})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	for _, result := range results {
		if !result.Deleted {
			t.Fatalf("Prune(%q) = %+v, want deleted", result.Name, result)
		}
	}
	tracked.mu.Lock()
	defer tracked.mu.Unlock()
	for _, name := range []string{"tracked.purge", "tracked.prune.a", "tracked.prune.b"} {
		if _, ok := tracked.touched[name]; !ok {
			t.Fatalf("tracked destinations = %v, missing %q", tracked.touched, name)
		}
	}
}

func TestMaintenanceLessDriverSkipsMaintenanceChecksLoudly(t *testing.T) {
	manifest := groupManifest
	pending := pendingGroups
	runners := groupRunners
	groupManifest = []manifestEntry{{name: "topology", declared: 19}}
	pendingGroups = nil
	groupRunners = map[string]groupRunner{"topology": runTopology}
	defer func() {
		groupManifest = manifest
		pendingGroups = pending
		groupRunners = runners
	}()

	report := Run(t, Suite{
		Driver: maintenanceLessDriver{},
		NewInspector: func(raw driver.Conn) (Inspect, error) {
			wrapped, ok := raw.(*maintenanceLessConn)
			if !ok {
				return nil, errors.New("unexpected maintenance-less connection")
			}
			return runTestInspector(wrapped.Conn)
		},
	})
	expected := map[string]struct{}{
		"EnsureTopology creates missing destinations":                                         {},
		"EnsureTopology is idempotent on a repeat call":                                       {},
		"EnsureTopology reports created and existing destinations together in one call":       {},
		"EnsureTopology reports a destination dropped from the spec but still in scope":       {},
		"EnsureTopology does not report a destination outside the requested scope":            {},
		"EnsureTopology scope prefix matches only at a component boundary":                    {},
		"EnsureTopology with an empty scope disables orphan scanning":                         {},
		"EnsureTopology folds deferred messages into an orphaned destination's message count": {},
		"Prune refuses a destination that still holds ready messages":                         {},
		"Prune refuses an empty destination whose park still holds messages":                  {},
		"Prune refuses a destination with an attached consumer":                               {},
		"Prune on an unknown destination reports not-deleted without erroring":                {},
		"Purge empties a destination and keeps it":                                            {},
		"cancelled context prevents topology admin calls without a partial effect":            {},
		"TopologyVerify reports the first missing destination without creating":               {},
		"FanoutAtConsume ignores bindings":                                                    {},
	}
	if len(report.Profiles) != 2 {
		t.Fatalf("maintenance-less report has %d profiles, want 2", len(report.Profiles))
	}
	for _, profile := range report.Profiles {
		if len(profile.Groups) != 1 {
			t.Fatalf("%s profile has %d groups, want 1", profile.Profile, len(profile.Groups))
		}
		group := profile.Groups[0]
		if group.Name != "topology" || group.Declared != 19 || group.Observed != 19 {
			t.Fatalf("%s topology result = %+v, want declared and observed 19", profile.Profile, group)
		}
		if group.Status != "passed-with-skips" {
			t.Fatalf("%s topology status = %q, want passed-with-skips", profile.Profile, group.Status)
		}
		if len(group.Skipped) != len(expected) {
			t.Fatalf("%s skipped %d checks, want %d", profile.Profile, len(group.Skipped), len(expected))
		}
		seen := make(map[string]struct{}, len(group.Skipped))
		for _, skipped := range group.Skipped {
			if _, duplicate := seen[skipped.Name]; duplicate {
				t.Fatalf("%s recorded duplicate skip %q", profile.Profile, skipped.Name)
			}
			seen[skipped.Name] = struct{}{}
			if _, expected := expected[skipped.Name]; !expected {
				t.Fatalf("%s recorded unexpected maintenance skip %q", profile.Profile, skipped.Name)
			}
			if !strings.Contains(skipped.Reason, "driver.Maintenance") {
				t.Fatalf("%s skip %q reason = %q, want missing interface", profile.Profile, skipped.Name, skipped.Reason)
			}
		}
		if len(seen) != len(expected) {
			t.Fatalf("%s recorded %d unique skips, want %d", profile.Profile, len(seen), len(expected))
		}
		if len(profile.Vector) != 3 {
			t.Fatalf("%s vector = %+v, want non-maintenance checks only", profile.Profile, profile.Vector)
		}
		for _, event := range profile.Vector {
			if event.ID != "topology-argument-drift" && event.ID != "topology-none" && event.ID != "topology-describe-depth" {
				t.Fatalf("%s vector recorded skipped check event %+v", profile.Profile, event)
			}
		}
	}
}

func assertRunFailure(t *testing.T, mode, want string) {
	t.Helper()
	output := runFailureOutput(t, mode)
	if !strings.Contains(string(output), want) {
		t.Fatalf("Run failure for %s omitted %q:\n%s", mode, want, output)
	}
}

func runFailureOutput(t *testing.T, mode string) []byte {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestRunFailureHelper", "-test.v") //nolint:gosec // the harness intentionally re-executes its own test binary.
	cmd.Env = append(os.Environ(), "CONFORMANCE_FAILURE_MODE="+mode)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("Run unexpectedly passed for %s:\n%s", mode, output)
	}
	return output
}

func TestRunFailureHelper(t *testing.T) {
	mode := os.Getenv("CONFORMANCE_FAILURE_MODE")
	if mode == "" {
		return
	}

	var factory InspectorFactory
	var runDriver driver.Driver = runTestDriver{}
	switch mode {
	case "zero-inspector":
		factory = func(driver.Conn) (Inspect, error) {
			return func(context.Context, string) (BrokerView, error) {
				return BrokerView{}, nil
			}, nil
		}
	case "factory-error":
		factory = func(driver.Conn) (Inspect, error) {
			return nil, errors.New("admin unavailable")
		}
	case "short-group":
		groupManifest = []manifestEntry{{name: "publish", declared: 2}}
		pendingGroups = nil
		groupRunners = map[string]groupRunner{}
		registerGroup("publish", func(group *groupContext) {
			group.Check("one registered check", func(*testing.T) {})
		})
		factory = runTestInspector
	case "long-group":
		groupManifest = []manifestEntry{{name: "publish", declared: 1}}
		pendingGroups = nil
		groupRunners = map[string]groupRunner{}
		registerGroup("publish", func(group *groupContext) {
			group.Check("declared check", func(*testing.T) {})
			group.Check("undeclared check", func(*testing.T) {})
		})
		factory = runTestInspector
	case "duplicate-group":
		groupManifest = []manifestEntry{{name: "publish", declared: 2}}
		pendingGroups = nil
		groupRunners = map[string]groupRunner{}
		registerGroup("publish", func(group *groupContext) {
			group.Check("same behavior", func(*testing.T) {})
			group.Check("same behavior", func(*testing.T) {})
		})
		factory = runTestInspector
	case "unrecorded-group":
		groupManifest = []manifestEntry{{name: "publish", declared: 1}}
		pendingGroups = nil
		groupRunners = map[string]groupRunner{}
		registerGroup("publish", func(group *groupContext) {
			group.Check("unrecorded skip", func(checkTest *testing.T) {
				checkTest.Skip("fixture pending")
			})
		})
		factory = runTestInspector
	case "two-failed-groups":
		groupManifest = []manifestEntry{{name: "publish", declared: 1}, {name: "consume", declared: 1}}
		pendingGroups = nil
		groupRunners = map[string]groupRunner{}
		registerGroup("publish", func(group *groupContext) {
			group.Check("deliberate publish failure", func(checkTest *testing.T) {
				checkTest.Fatal("deliberate publish failure")
			})
		})
		registerGroup("consume", func(group *groupContext) {
			group.Check("deliberate consume failure", func(checkTest *testing.T) {
				checkTest.Fatal("deliberate consume failure")
			})
		})
		factory = runTestInspector
	case "failed-short-group":
		groupManifest = []manifestEntry{{name: "publish", declared: 2}}
		pendingGroups = nil
		groupRunners = map[string]groupRunner{}
		registerGroup("publish", func(group *groupContext) {
			group.Check("deliberate short failure", func(checkTest *testing.T) {
				checkTest.Fatal("deliberate short failure")
			})
		})
		factory = runTestInspector
	case "profile-failure":
		groupManifest = []manifestEntry{{name: "publish", declared: 1}}
		pendingGroups = nil
		groupRunners = map[string]groupRunner{}
		registerGroup("publish", func(group *groupContext) {
			group.Check("deliberate full profile failure", func(checkTest *testing.T) {
				if group.profile == ProfileFull {
					checkTest.Fatal("deliberate full profile failure")
				}
			})
		})
		factory = runTestInspector
	case "profile-setup-failure":
		groupManifest = []manifestEntry{{name: "publish", declared: 1}}
		pendingGroups = nil
		groupRunners = map[string]groupRunner{}
		registerGroup("publish", func(group *groupContext) {
			group.Check("strict profile setup continued", func(*testing.T) {})
		})
		runDriver = runTestDriver{failFullProfileSetup: true}
		factory = runTestInspector
	case "vector-diff":
		groupManifest = []manifestEntry{{name: "publish", declared: 1}}
		pendingGroups = nil
		groupRunners = map[string]groupRunner{}
		registerGroup("publish", func(group *groupContext) {
			group.Check("profile behavior differs", func(*testing.T) {
				group.vector.Add(BehaviorEvent{ID: "profile", Outcome: group.profile.String()})
			})
		})
		factory = runTestInspector
	default:
		t.Fatalf("unknown conformance failure mode %q", mode)
	}

	Run(t, Suite{Driver: runDriver, NewInspector: factory})
	if mode == "profile-failure" || mode == "profile-setup-failure" {
		if !t.Failed() {
			t.Fatal("Run returned without preserving the profile failure")
		}
		return
	}
	t.Fatal("Run returned after an expected failure")
}

type runTestDriver struct {
	opens                *int
	failFullProfileSetup bool
}

func (runTestDriver) Name() string                      { return "run-test" }
func (runTestDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (d runTestDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	if d.opens != nil {
		(*d.opens)++
	}
	return &runTestConn{
		caps:                 driver.Capabilities{},
		failFullProfileSetup: d.failFullProfileSetup,
		queues:               make(map[string][]driver.OutboundMessage),
		unsettled:            make(map[string]int),
		specs:                make(map[string]driver.DestinationSpec),
	}, nil
}

type maintenanceLessDriver struct{}

func (maintenanceLessDriver) Name() string                      { return "run-test-without-maintenance" }
func (maintenanceLessDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (maintenanceLessDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	conn, err := (runTestDriver{}).Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &maintenanceLessConn{Conn: conn}, nil
}

type maintenanceLessConn struct {
	driver.Conn
}

func (c *maintenanceLessConn) Admin() driver.Admin {
	return maintenanceLessAdmin{Admin: c.Conn.Admin()}
}

type maintenanceLessAdmin struct {
	driver.Admin
}

type runTestConn struct {
	caps                 driver.Capabilities
	failFullProfileSetup bool
	queues               map[string][]driver.OutboundMessage
	unsettled            map[string]int
	specs                map[string]driver.DestinationSpec
}

func (c *runTestConn) Capabilities() driver.Capabilities { return c.caps }
func (c *runTestConn) BrokerInfo() driver.BrokerInfo     { return driver.BrokerInfo{Kind: "run-test"} }
func (c *runTestConn) Admin() driver.Admin               { return runTestAdmin{conn: c} }
func (c *runTestConn) Ping(context.Context) error        { return nil }
func (c *runTestConn) Close(context.Context) error       { return nil }
func (c *runTestConn) Producer(context.Context, driver.ProducerConfig) (driver.Producer, error) {
	return &runTestProducer{conn: c}, nil
}

func (c *runTestConn) Consumer(_ context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	capacity := max(cfg.Prefetch, 1)
	consumer := &runTestConsumer{
		conn:     c,
		messages: make(chan driver.InboundMessage, capacity),
		errs:     make(chan error, 1),
		settlers: make(map[*runTestSettler]struct{}),
	}
	for _, destination := range cfg.Destinations {
		queued := c.queues[destination]
		for len(queued) > 0 && consumer.outstanding < capacity {
			message := queued[0]
			queued = queued[1:]
			settler := &runTestSettler{conn: c, consumer: consumer, destination: destination, message: message}
			consumer.settlers[settler] = struct{}{}
			consumer.messages <- driver.InboundMessage{
				Destination: destination,
				Body:        message.Body,
				Settle:      settler,
			}
			consumer.outstanding++
			c.unsettled[destination]++
		}
		c.queues[destination] = queued
	}
	return consumer, nil
}

func runTestInspector(raw driver.Conn) (Inspect, error) {
	conn, ok := raw.(*runTestConn)
	if !ok {
		return nil, errors.New("unexpected fake connection")
	}
	return func(_ context.Context, destination string) (BrokerView, error) {
		messages, ok := conn.queues[destination]
		if !ok {
			return BrokerView{}, &driver.Error{Driver: "run-test", Op: "inspect", K: driver.KindNotFound, Err: driver.ErrDestinationMissing}
		}
		var ready, auxiliary int64
		for _, message := range messages {
			if !message.DelayUntil.IsZero() {
				auxiliary++
				continue
			}
			ready++
		}
		return BrokerView{Ready: ready, Auxiliary: auxiliary, Unsettled: int64(conn.unsettled[destination])}, nil
	}, nil
}

type runTestProducer struct{ conn *runTestConn }

func (p *runTestProducer) Publish(_ context.Context, messages ...driver.OutboundMessage) error {
	for _, message := range messages {
		if _, exists := p.conn.queues[message.Destination]; !exists {
			return &driver.Error{Driver: "run-test", Op: "publish", K: driver.KindNotFound, Err: driver.ErrDestinationMissing}
		}
	}
	for _, message := range messages {
		p.conn.queues[message.Destination] = append(p.conn.queues[message.Destination], message)
	}
	return nil
}
func (*runTestProducer) Close(context.Context) error { return nil }

type runTestConsumer struct {
	conn        *runTestConn
	messages    chan driver.InboundMessage
	errs        chan error
	outstanding int
	stopped     bool
	settlers    map[*runTestSettler]struct{}
}

func (c *runTestConsumer) Messages() <-chan driver.InboundMessage { return c.messages }
func (c *runTestConsumer) Errors() <-chan error                   { return c.errs }
func (*runTestConsumer) Pause(...string) error                    { return nil }
func (*runTestConsumer) Resume(...string) error                   { return nil }
func (*runTestConsumer) Drain(context.Context) error              { return nil }
func (c *runTestConsumer) Stop(context.Context) error {
	if c.outstanding != 0 {
		return errors.New("fake consumer has unsettled messages")
	}
	if !c.stopped {
		c.stopped = true
		close(c.messages)
		close(c.errs)
	}
	return nil
}

func (c *runTestConsumer) Release(context.Context) error {
	if c.stopped {
		return nil
	}
	for settler := range c.settlers {
		if settler.settled {
			continue
		}
		settler.settled = true
		delete(c.settlers, settler)
		c.outstanding--
		c.conn.unsettled[settler.destination]--
		c.conn.queues[settler.destination] = append([]driver.OutboundMessage{settler.message}, c.conn.queues[settler.destination]...)
	}
	c.stopped = true
	close(c.messages)
	close(c.errs)
	return nil
}

func (*runTestConsumer) Lag(context.Context) (map[string]int64, error) { return nil, nil }

type runTestSettler struct {
	conn        *runTestConn
	consumer    *runTestConsumer
	destination string
	message     driver.OutboundMessage
	settled     bool
}

func (s *runTestSettler) Ack(context.Context) error                      { return s.settle() }
func (s *runTestSettler) Nack(context.Context, driver.NackOptions) error { return s.settle() }
func (s *runTestSettler) settle() error {
	if s.settled {
		return errors.New("already settled")
	}
	s.settled = true
	delete(s.consumer.settlers, s)
	s.consumer.outstanding--
	s.conn.unsettled[s.destination]--
	return nil
}

type runTestAdmin struct{ conn *runTestConn }

func (a runTestAdmin) EnsureTopology(_ context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	var diff driver.TopologyDiff
	if a.conn.failFullProfileSetup && len(spec.Destinations) == 1 && strings.Contains(spec.Destinations[0].Name, ".full.") {
		return diff, errors.New("deliberate full profile setup failure")
	}
	if spec.Policy == driver.TopologyNone {
		return diff, nil
	}
	if spec.Policy == driver.TopologyVerify {
		for _, destination := range spec.Destinations {
			declared, exists := a.conn.specs[destination.Name]
			if !exists {
				return diff, fmt.Errorf("%s: %w", destination.Name, driver.ErrDestinationMissing)
			}
			diff.ExistingDestinations = append(diff.ExistingDestinations, destination.Name)
			if declared.DeliveryLimit != destination.DeliveryLimit {
				diff.Drifted = append(diff.Drifted, driver.ArgumentDrift{
					Name: destination.Name, Argument: "x-delivery-limit",
					Want: fmt.Sprint(destination.DeliveryLimit), Got: fmt.Sprint(declared.DeliveryLimit),
				})
			}
		}
		return diff, nil
	}
	for _, destination := range spec.Destinations {
		if _, exists := a.conn.queues[destination.Name]; exists {
			diff.ExistingDestinations = append(diff.ExistingDestinations, destination.Name)
			continue
		}
		a.conn.queues[destination.Name] = nil
		a.conn.specs[destination.Name] = destination
		diff.CreatedDestinations = append(diff.CreatedDestinations, destination.Name)
	}
	return diff, nil
}

func (a runTestAdmin) DescribeTopology(_ context.Context, names []string) (driver.TopologyState, error) {
	depth := make(map[string]int64, len(names))
	for _, name := range names {
		queue, exists := a.conn.queues[name]
		if !exists {
			return driver.TopologyState{}, &driver.Error{
				Driver: "run-test",
				Op:     "describe",
				K:      driver.KindNotFound,
				Err:    driver.ErrDestinationMissing,
			}
		}
		depth[name] = int64(len(queue))
	}
	return driver.TopologyState{Depth: depth}, nil
}

func (a runTestAdmin) Purge(_ context.Context, destination string) (int64, error) {
	count := int64(len(a.conn.queues[destination]))
	a.conn.queues[destination] = nil
	return count, nil
}

func (a runTestAdmin) Prune(_ context.Context, names []string) ([]driver.PruneResult, error) {
	results := make([]driver.PruneResult, 0, len(names))
	for _, name := range names {
		if _, exists := a.conn.queues[name]; !exists {
			results = append(results, driver.PruneResult{Name: name, Reason: "destination does not exist"})
			continue
		}
		delete(a.conn.queues, name)
		results = append(results, driver.PruneResult{Name: name, Deleted: true})
	}
	return results, nil
}

func TestInspectorSelfCheckRejectsZeroView(t *testing.T) {
	err := validateInspectorDelta(
		BrokerView{},
		BrokerView{},
		BrokerView{},
		BrokerView{},
		2,
	)
	if err == nil {
		t.Fatal("zero inspector passed the state-change self-check")
	}
}

func TestInspectorFactoryErrorFailsBeforeGroups(t *testing.T) {
	err := validateInspectorFactory(nil, errors.New("admin unavailable"))
	if err == nil || !strings.Contains(err.Error(), "admin unavailable") {
		t.Fatalf("factory error was not preserved: %v", err)
	}
}

func TestManifestRejectsCountDrift(t *testing.T) {
	if err := validateGroupCount("publish", 14, 13); err == nil {
		t.Fatal("short group passed the manifest count")
	}
	if err := validateGroupCount("publish", 14, 14); err != nil {
		t.Fatalf("exact manifest count failed: %v", err)
	}
	// A group that outgrew its declared count has outgrown its specification,
	// which is the same defect as falling short of it and needs the same fix.
	if err := validateGroupCount("publish", 14, 15); err == nil {
		t.Fatal("group larger than its declared count passed the manifest count")
	}
}

func TestSkippedCheckDoesNotSatisfyManifestCount(t *testing.T) {
	if !runCheck(t, "skipped", func(checkTest *testing.T) {
		checkTest.Skip("fixture pending")
	}) {
		t.Fatal("skipped check was not detected")
	}
}

func TestRecordedFixtureSkipSatisfiesManifestCount(t *testing.T) {
	group := &groupContext{t: t, skips: make(map[string]string)}
	group.Check("fixture-gated", func(checkTest *testing.T) {
		group.Skip(checkTest, "fixture-gated", "deadline fixture unavailable")
	})
	if group.checks != 1 {
		t.Fatalf("recorded fixture skip counted %d checks; want 1", group.checks)
	}
	if group.skips["fixture-gated"] != "deadline fixture unavailable" {
		t.Fatalf("recorded fixture skip = %#v", group.skips)
	}
}

func TestBehaviorVectorDiffIsOrdered(t *testing.T) {
	want := BehaviorVector{
		{ID: "first", Outcome: "ack", AttemptCount: 1, FinalDestination: "main"},
		{ID: "second", Outcome: "ack", AttemptCount: 1, FinalDestination: "main"},
	}
	got := BehaviorVector{want[1], want[0]}
	if diff := want.Diff(got); diff == "" {
		t.Fatal("reordered behavior vector compared equal")
	}
}

func TestReportIsArchivable(t *testing.T) {
	report := Report{
		Driver:   "inmem",
		Profiles: []ProfileReport{{Profile: ProfileFull}},
		Capabilities: []CapabilityResult{{
			Profile: ProfileFull, Capability: "NativeDelay",
			Declared: "true", Status: "passed", Evidence: "native capability exercised",
		}},
		Pending: []string{"publish"},
	}
	var jsonOutput bytes.Buffer
	if err := report.WriteJSON(&jsonOutput); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(jsonOutput.String(), "NativeDelay") {
		t.Fatal("JSON report omitted capability table")
	}
	var markdownOutput bytes.Buffer
	if err := report.WriteMarkdown(&markdownOutput); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(markdownOutput.String(), "Pending groups") {
		t.Fatal("Markdown report omitted pending groups")
	}
}

func TestManifestRejectsUntrackedState(t *testing.T) {
	manifest := []manifestEntry{{name: "publish", declared: 1}}
	runner := groupRunner(func(*groupContext) {})
	if err := validateManifestState(manifest, nil, map[string]groupRunner{}); err == nil {
		t.Fatal("manifest accepted a group that was neither implemented nor pending")
	}
	if err := validateManifestState(manifest, []string{"publish"}, map[string]groupRunner{"publish": runner}); err == nil {
		t.Fatal("manifest accepted a group that was both implemented and pending")
	}
	if err := validateManifestState(manifest, []string{"missing"}, map[string]groupRunner{}); err == nil {
		t.Fatal("manifest accepted a pending group absent from the manifest")
	}
}

type consumerConfigRecordingConn struct {
	driver.Conn
	mu      sync.Mutex
	configs []driver.ConsumerConfig
}

func (c *consumerConfigRecordingConn) Consumer(ctx context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	c.mu.Lock()
	c.configs = append(c.configs, cfg)
	c.mu.Unlock()
	return c.Conn.Consumer(ctx, cfg)
}

func (c *consumerConfigRecordingConn) recordedConfigs() []driver.ConsumerConfig {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]driver.ConsumerConfig(nil), c.configs...)
}

func TestRunScopedPersistentGroups(t *testing.T) {
	runCheckCalls := func(t *testing.T, profile Profile, runID string) []driver.ConsumerConfig {
		raw := &runTestConn{
			queues:    make(map[string][]driver.OutboundMessage),
			unsettled: make(map[string]int),
			specs:     make(map[string]driver.DestinationSpec),
		}
		inspect, err := runTestInspector(raw)
		if err != nil {
			t.Fatalf("runTestInspector: %v", err)
		}
		recorder := &consumerConfigRecordingConn{Conn: raw}
		group := &groupContext{
			t:          t,
			ctx:        context.Background(),
			conn:       recorder,
			inspect:    inspect,
			profile:    profile,
			checkNames: make(map[string]struct{}),
			skips:      make(map[string]string),
			runID:      runID,
			checkFilter: func(name string) bool {
				return name == "StartEarliest includes retained messages for a new group" ||
					name == "StartAt does not reposition an existing group"
			},
		}
		runConsume(group)
		return recorder.recordedConfigs()
	}

	pass1 := runCheckCalls(t, ProfileFull, "run-alpha")
	pass2 := runCheckCalls(t, ProfileFull, "run-beta")
	pass3 := runCheckCalls(t, ProfileStrictPortability, "run-alpha")

	if len(pass1) != 3 {
		t.Fatalf("pass1 recorded %d consumer configs, want 3", len(pass1))
	}
	if len(pass2) != 3 {
		t.Fatalf("pass2 recorded %d consumer configs, want 3", len(pass2))
	}
	if len(pass3) != 3 {
		t.Fatalf("pass3 recorded %d consumer configs, want 3", len(pass3))
	}

	startEarliest1 := pass1[0].Group
	startAt1_First := pass1[1].Group
	startAt1_Second := pass1[2].Group

	startEarliest2 := pass2[0].Group
	startAt2_First := pass2[1].Group
	startAt2_Second := pass2[2].Group

	startEarliest3 := pass3[0].Group
	startAt3_First := pass3[1].Group
	startAt3_Second := pass3[2].Group

	// 1. the StartEarliest group contains the supplied run ID and changes when the run ID changes:
	if !strings.Contains(startEarliest1, "run-alpha") {
		t.Errorf("StartEarliest group %q does not contain run ID %q", startEarliest1, "run-alpha")
	}
	if !strings.Contains(startEarliest2, "run-beta") {
		t.Errorf("StartEarliest group %q does not contain run ID %q", startEarliest2, "run-beta")
	}
	if startEarliest1 == startEarliest2 {
		t.Errorf("StartEarliest group did not change across run IDs: %q", startEarliest1)
	}

	// 2. both StartAt consumers within one check use the same group:
	if startAt1_First != startAt1_Second {
		t.Errorf("StartAt consumers within pass1 use different groups: %q vs %q", startAt1_First, startAt1_Second)
	}
	if startAt2_First != startAt2_Second {
		t.Errorf("StartAt consumers within pass2 use different groups: %q vs %q", startAt2_First, startAt2_Second)
	}
	if startAt3_First != startAt3_Second {
		t.Errorf("StartAt consumers within pass3 use different groups: %q vs %q", startAt3_First, startAt3_Second)
	}

	// 3. StartAt's group changes when the run ID changes (and contains the run ID):
	if !strings.Contains(startAt1_First, "run-alpha") {
		t.Errorf("StartAt group %q does not contain run ID %q", startAt1_First, "run-alpha")
	}
	if !strings.Contains(startAt2_First, "run-beta") {
		t.Errorf("StartAt group %q does not contain run ID %q", startAt2_First, "run-beta")
	}
	if startAt1_First == startAt2_First {
		t.Errorf("StartAt group did not change across run IDs: %q", startAt1_First)
	}

	// 4. full and strict profiles do not share a group:
	if startEarliest1 == startEarliest3 {
		t.Errorf("StartEarliest group shared across full and strict profiles: %q", startEarliest1)
	}
	if startAt1_First == startAt3_First {
		t.Errorf("StartAt group shared across full and strict profiles: %q", startAt1_First)
	}
}

func TestIsRevokedSettlementError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", want: false},
		{name: "unclassified revocation text", err: errors.New("partition assignment revoked"), want: false},
		{
			name: "classified transient revocation text",
			err:  &driver.Error{Driver: "test", Op: "ack", K: driver.KindTransient, Err: errors.New("partition assignment revoked")},
			want: false,
		},
		{
			name: "classified fatal unrelated text",
			err:  &driver.Error{Driver: "test", Op: "ack", K: driver.KindFatal, Err: errors.New("broker unavailable")},
			want: false,
		},
		{
			name: "classified fatal revocation text",
			err:  &driver.Error{Driver: "test", Op: "ack", K: driver.KindFatal, Err: errors.New("PARTITION ASSIGNMENT REVOKED")},
			want: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isRevokedSettlementError(test.err); got != test.want {
				t.Fatalf("isRevokedSettlementError(%v) = %t, want %t", test.err, got, test.want)
			}
		})
	}
}

func TestWaitForRebalanceAssignmentError(t *testing.T) {
	unexpected := &driver.Error{Driver: "test", Op: "consume", K: driver.KindFatal, Err: errors.New("broker unavailable")}
	tests := []struct {
		name      string
		setup     func() (context.Context, <-chan error)
		wantText  string
		wantCause error
	}{
		{
			name: "assigned notification",
			setup: func() (context.Context, <-chan error) {
				errs := make(chan error, 1)
				errs <- &driver.Error{Driver: "test", Op: "consume", K: driver.KindNotification, Err: errors.New("assignment notification")}
				return context.Background(), errs
			},
		},
		{
			name: "closed channel",
			setup: func() (context.Context, <-chan error) {
				errs := make(chan error)
				close(errs)
				return context.Background(), errs
			},
			wantText: "Errors channel closed",
		},
		{
			name: "nil error",
			setup: func() (context.Context, <-chan error) {
				errs := make(chan error, 1)
				errs <- nil
				return context.Background(), errs
			},
			wantText: "nil error",
		},
		{
			name: "unexpected error",
			setup: func() (context.Context, <-chan error) {
				errs := make(chan error, 1)
				errs <- unexpected
				return context.Background(), errs
			},
			wantText:  "broker unavailable",
			wantCause: unexpected,
		},
		{
			name: "cancelled context",
			setup: func() (context.Context, <-chan error) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, make(chan error)
			},
			wantText:  "timed out",
			wantCause: context.Canceled,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, errs := test.setup()
			err := waitForRebalanceAssignmentError(ctx, errs)
			if test.wantText == "" {
				if err != nil {
					t.Fatalf("waitForRebalanceAssignmentError() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("waitForRebalanceAssignmentError() = nil, want error")
			}
			if !strings.Contains(err.Error(), test.wantText) {
				t.Fatalf("error = %v, want text %q", err, test.wantText)
			}
			if test.wantCause != nil && !errors.Is(err, test.wantCause) {
				t.Fatalf("error = %v, want cause %v", err, test.wantCause)
			}
		})
	}
}

// stalledDeadlineFixture is a DeadlineFixture whose clock stands still: every Now call
// returns the instant it was built at, which is what a descheduled check goroutine sees
// from the fixture's side while wall time keeps moving.
type stalledDeadlineFixture struct{ now time.Time }

func (*stalledDeadlineFixture) Consumer(context.Context, time.Duration, driver.ConsumerConfig) (driver.Consumer, error) {
	return nil, errors.New("stalled deadline fixture has no consumers")
}

func (f *stalledDeadlineFixture) Now() time.Time { return f.now }

func (*stalledDeadlineFixture) Advance(time.Duration) {}

// delayedFixtureConsumer yields one message after a fixed delay, standing in for a driver
// that hands over a delivery the fixture clock has already released. The release channel
// lets the test's cleanup end the delivery goroutine when the receive fails, which is what
// keeps a failing run from leaving the bubble with a goroutine still waiting on its timer.
type delayedFixtureConsumer struct {
	messages chan driver.InboundMessage
	release  chan struct{}
}

func newDelayedFixtureConsumer(t *testing.T, after time.Duration, message driver.InboundMessage) *delayedFixtureConsumer {
	consumer := &delayedFixtureConsumer{
		messages: make(chan driver.InboundMessage, 1),
		release:  make(chan struct{}),
	}
	t.Cleanup(func() { close(consumer.release) })
	go func() {
		select {
		case <-consumer.release:
		case <-time.After(after): //nolint:forbidigo // the delivery is bubble time, ordered by the bubble clock
			consumer.messages <- message
		}
	}()
	return consumer
}

func (c *delayedFixtureConsumer) Messages() <-chan driver.InboundMessage { return c.messages }
func (*delayedFixtureConsumer) Errors() <-chan error                     { return nil }
func (*delayedFixtureConsumer) Pause(...string) error                    { return nil }
func (*delayedFixtureConsumer) Resume(...string) error                   { return nil }
func (*delayedFixtureConsumer) Drain(context.Context) error              { return nil }
func (*delayedFixtureConsumer) Stop(context.Context) error               { return nil }
func (*delayedFixtureConsumer) Release(context.Context) error            { return nil }

func (*delayedFixtureConsumer) Lag(context.Context) (map[string]int64, error) { return nil, nil }

// TestReceiveBeforeWaitsForTheDeliveryAfterAFixtureClockStall reproduces the deferred
// timing failure where nothing is late: the fixture clock stands still while the wait's
// clock spends the whole deferred budget, which is what a long deschedule does to a check
// on a saturated machine. A fixture deadline must then wait on the harness's delivery
// budget, not on the remainder of an instant the fixture clock never spent.
func TestReceiveBeforeWaitsForTheDeliveryAfterAFixtureClockStall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		group := &groupContext{t: t, ctx: context.Background(), deadline: &stalledDeadlineFixture{now: realNow()}}
		consumer := newDelayedFixtureConsumer(t, deferredLateBound+waitInterval, driver.InboundMessage{Body: []byte("released")})
		deadline := group.deadline.Now().Add(deferredLateBound)
		// The deschedule: wall time passes between the deadline being computed and the
		// wait starting, while the fixture clock that produced the deadline stays put.
		time.Sleep(deferredLateBound) //nolint:forbidigo // the stall is bubble time; see the consumer above
		message := receiveBefore(t, group, consumer, deadline, "fixture delivery after a fixture clock stall")
		if string(message.Body) != "released" {
			t.Fatalf("body=%q, want released", message.Body)
		}
	})
}
