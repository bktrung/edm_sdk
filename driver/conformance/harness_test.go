package conformance

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestRunRejectsZeroInspector(t *testing.T) {
	assertRunFailure(t, "zero-inspector", "ready delta")
}

func TestRunRejectsInspectorFactoryError(t *testing.T) {
	assertRunFailure(t, "factory-error", "admin unavailable")
}

func TestRunRejectsShortRegisteredGroup(t *testing.T) {
	assertRunFailure(t, "short-group", "manifest requires at least 2")
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

func assertRunFailure(t *testing.T, mode, want string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestRunFailureHelper", "-test.v") //nolint:gosec // the harness intentionally re-executes its own test binary.
	cmd.Env = append(os.Environ(), "CONFORMANCE_FAILURE_MODE="+mode)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("Run unexpectedly passed for %s:\n%s", mode, output)
	}
	if !strings.Contains(string(output), want) {
		t.Fatalf("Run failure for %s omitted %q:\n%s", mode, want, output)
	}
}

func TestRunFailureHelper(t *testing.T) {
	mode := os.Getenv("CONFORMANCE_FAILURE_MODE")
	if mode == "" {
		return
	}

	var factory InspectorFactory
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
	default:
		t.Fatalf("unknown conformance failure mode %q", mode)
	}

	Run(t, Suite{Driver: runTestDriver{}, NewInspector: factory})
	t.Fatal("Run returned after an expected failure")
}

type runTestDriver struct {
	opens *int
}

func (runTestDriver) Name() string                      { return "run-test" }
func (runTestDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (d runTestDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	if d.opens != nil {
		(*d.opens)++
	}
	return &runTestConn{
		queues:    make(map[string][]driver.OutboundMessage),
		unsettled: make(map[string]int),
	}, nil
}

type runTestConn struct {
	caps      driver.Capabilities
	queues    map[string][]driver.OutboundMessage
	unsettled map[string]int
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
	capacity := cfg.Prefetch
	if capacity < 1 {
		capacity = 1
	}
	consumer := &runTestConsumer{
		conn:     c,
		messages: make(chan driver.InboundMessage, capacity),
		errs:     make(chan error, 1),
	}
	for _, destination := range cfg.Destinations {
		queued := c.queues[destination]
		for len(queued) > 0 && consumer.outstanding < capacity {
			message := queued[0]
			queued = queued[1:]
			settler := &runTestSettler{conn: c, consumer: consumer, destination: destination}
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
		return BrokerView{
			Ready:     int64(len(conn.queues[destination])),
			Unsettled: int64(conn.unsettled[destination]),
		}, nil
	}, nil
}

type runTestProducer struct{ conn *runTestConn }

func (p *runTestProducer) Publish(_ context.Context, messages ...driver.OutboundMessage) error {
	for _, message := range messages {
		p.conn.queues[message.Destination] = append(p.conn.queues[message.Destination], message)
	}
	return nil
}
func (*runTestProducer) Flush(context.Context) error { return nil }
func (*runTestProducer) Close(context.Context) error { return nil }

type runTestConsumer struct {
	conn        *runTestConn
	messages    chan driver.InboundMessage
	errs        chan error
	outstanding int
	stopped     bool
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
func (*runTestConsumer) Lag(context.Context) (map[string]int64, error) { return nil, nil }

type runTestSettler struct {
	conn        *runTestConn
	consumer    *runTestConsumer
	destination string
	settled     bool
}

func (s *runTestSettler) Ack(context.Context) error                      { return s.settle() }
func (s *runTestSettler) Nack(context.Context, driver.NackOptions) error { return s.settle() }
func (s *runTestSettler) settle() error {
	if s.settled {
		return errors.New("already settled")
	}
	s.settled = true
	s.consumer.outstanding--
	s.conn.unsettled[s.destination]--
	return nil
}

type runTestAdmin struct{ conn *runTestConn }

func (a runTestAdmin) EnsureTopology(_ context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	var diff driver.TopologyDiff
	for _, destination := range spec.Destinations {
		if _, exists := a.conn.queues[destination.Name]; exists {
			diff.Existing = append(diff.Existing, destination.Name)
			continue
		}
		a.conn.queues[destination.Name] = nil
		diff.CreatedDestinations = append(diff.CreatedDestinations, destination.Name)
	}
	return diff, nil
}

func (a runTestAdmin) DescribeTopology(_ context.Context, names []string) (driver.TopologyState, error) {
	depth := make(map[string]int64, len(names))
	for _, name := range names {
		depth[name] = int64(len(a.conn.queues[name]))
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
		results = append(results, driver.PruneResult{Name: name})
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

func TestManifestRejectsShortGroup(t *testing.T) {
	if err := validateGroupCount("publish", 14, 13); err == nil {
		t.Fatal("short group passed the manifest count")
	}
	if err := validateGroupCount("publish", 14, 14); err != nil {
		t.Fatalf("exact manifest count failed: %v", err)
	}
	if err := validateGroupCount("publish", 14, 15); err != nil {
		t.Fatalf("larger group failed: %v", err)
	}
}

func TestSkippedCheckDoesNotSatisfyManifestCount(t *testing.T) {
	if !runCheck(t, "skipped", func(checkTest *testing.T) {
		checkTest.Skip("fixture pending")
	}) {
		t.Fatal("skipped check was not detected")
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
