package conformance

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestInspectProbeSurvivesADirtyBroker(t *testing.T) {
	withoutConformanceGroups(t)
	conn := newProbeTestConn()
	for _, profile := range []Profile{ProfileFull, ProfileStrictPortability} {
		conn.queues["conformance.inspect."+profile.String()+".probe"] = []driver.OutboundMessage{{Body: []byte("old residue")}}
	}

	Run(t, Suite{
		Driver:       probeTestDriver{conn: conn},
		NewInspector: probeTestInspector,
	})

	if len(conn.created) != 2 {
		t.Fatalf("created probe destinations = %v, want one per profile", conn.created)
	}
	runIDs := make(map[string]struct{})
	for _, destination := range conn.created {
		parts := strings.Split(destination, ".")
		if len(parts) != 5 || parts[0] != "conformance" || parts[1] != "inspect" || parts[4] != "probe" {
			t.Fatalf("probe destination %q does not contain a run component", destination)
		}
		runIDs[parts[2]] = struct{}{}
	}
	if len(runIDs) != 1 {
		t.Fatalf("probe destinations use run IDs %v, want one shared run ID", runIDs)
	}
	for _, profile := range []Profile{ProfileFull, ProfileStrictPortability} {
		oldDestination := "conformance.inspect." + profile.String() + ".probe"
		if got := len(conn.queues[oldDestination]); got != 1 {
			t.Fatalf("previous-generation destination %q contains %d messages, want 1", oldDestination, got)
		}
	}
}

func TestInspectProbeReclaimsItsDestination(t *testing.T) {
	withoutConformanceGroups(t)
	conn := newProbeTestConn()
	const runID = "reclaim-test-run"
	const destination = "conformance.inspect.reclaim-test-run.full.probe"

	if !t.Run("completed profile", func(t *testing.T) {
		runProfile(t, context.Background(), conn, probeTestInspect(conn), runID, ProfileFull, driver.Capabilities{}, nil, nil, &Report{}, probeTestDriver{conn: conn}, driver.Config{}, nil)
	}) {
		t.Fatal("completed profile failed")
	}
	if _, err := conn.Admin().DescribeTopology(context.Background(), []string{destination}); !errors.Is(err, driver.ErrDestinationMissing) {
		t.Fatalf("DescribeTopology(%q) after profile = %v, want ErrDestinationMissing", destination, err)
	}

	if _, err := conn.Admin().EnsureTopology(context.Background(), driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination}},
	}); err != nil {
		t.Fatalf("EnsureTopology(%q) for teardown path: %v", destination, err)
	}
	conn.events = nil
	consumer := &refusingProbeConsumer{
		events:      &conn.events,
		conn:        conn.runTestConn,
		destination: destination,
		attached:    &conn.attached,
	}
	producer := &recordingProbeProducer{events: &conn.events}
	cleanupProfileErrors(context.Background(), conn, producer, consumer, destination)
	wantEvents := []string{"stop", "release", "purge", "prune", "producer-close"}
	if strings.Join(conn.events, ",") != strings.Join(wantEvents, ",") {
		t.Fatalf("teardown events = %v, want %v", conn.events, wantEvents)
	}
	if _, err := conn.Admin().DescribeTopology(context.Background(), []string{destination}); !errors.Is(err, driver.ErrDestinationMissing) {
		t.Fatalf("DescribeTopology(%q) after refused stop = %v, want ErrDestinationMissing", destination, err)
	}
}

func TestInspectProbeReclaimsItsDestinationDespiteARefusedPrune(t *testing.T) {
	conn := newProbeTestConn()
	conn.pruneRefusals = 2
	const destination = "conformance.inspect.refused-prune.probe"
	if _, err := conn.Admin().EnsureTopology(context.Background(), driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination}},
	}); err != nil {
		t.Fatalf("EnsureTopology(%q): %v", destination, err)
	}

	errs := cleanupProfileErrors(context.Background(), conn, &recordingProbeProducer{events: &conn.events}, nil, destination)
	if len(errs) != 0 {
		t.Fatalf("cleanup errors = %v, want none", errs)
	}
	if want := conn.pruneRefusals + 1; conn.pruneCalls != want {
		t.Fatalf("Prune calls = %d, want %d", conn.pruneCalls, want)
	}
	if _, err := conn.Admin().DescribeTopology(context.Background(), []string{destination}); !errors.Is(err, driver.ErrDestinationMissing) {
		t.Fatalf("DescribeTopology(%q) after refused Prune = %v, want ErrDestinationMissing", destination, err)
	}
}

func TestProbeReclaimSurvivesATransientPruneError(t *testing.T) {
	conn := newProbeTestConn()
	conn.pruneTransientErrors = 2
	const destination = "conformance.inspect.transient.probe"
	if _, err := conn.Admin().EnsureTopology(context.Background(), driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination}},
	}); err != nil {
		t.Fatalf("EnsureTopology(%q): %v", destination, err)
	}

	errs := cleanupProfileErrors(context.Background(), conn, &recordingProbeProducer{events: &conn.events}, nil, destination)
	if len(errs) != 0 {
		t.Fatalf("cleanup errors = %v, want none", errs)
	}
	if want := conn.pruneTransientErrors + 1; conn.pruneCalls != want {
		t.Fatalf("Prune calls = %d, want %d", conn.pruneCalls, want)
	}
	if _, err := conn.Admin().DescribeTopology(context.Background(), []string{destination}); !errors.Is(err, driver.ErrDestinationMissing) {
		t.Fatalf("DescribeTopology(%q) after transient Prune errors = %v, want ErrDestinationMissing", destination, err)
	}
}

func TestProbeReclaimReportsAPermanentPruneError(t *testing.T) {
	conn := newProbeTestConn()
	conn.prunePermanentError = true
	const destination = "conformance.inspect.permanent.probe"
	if _, err := conn.Admin().EnsureTopology(context.Background(), driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination}},
	}); err != nil {
		t.Fatalf("EnsureTopology(%q): %v", destination, err)
	}

	errs := cleanupProfileErrors(context.Background(), conn, nil, nil, destination)
	if len(errs) != 1 {
		t.Fatalf("cleanup errors = %v, want one permanent prune error", errs)
	}
	if !strings.Contains(errs[0].Error(), destination) {
		t.Fatalf("cleanup error = %v, want destination %q", errs[0], destination)
	}
	if conn.pruneCalls != 1 {
		t.Fatalf("Prune calls = %d, want 1", conn.pruneCalls)
	}
	var classified *driver.Error
	if !errors.As(errs[0], &classified) || classified.Retryable() {
		t.Fatalf("cleanup error = %v, want non-retryable driver error", errs[0])
	}
}

func TestInspectProbeTeardownReportsAnUnreclaimedDestination(t *testing.T) {
	conn := newProbeTestConn()
	conn.pruneForever = true
	const destination = "conformance.inspect.unreclaimed.probe"
	if _, err := conn.Admin().EnsureTopology(context.Background(), driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination}},
	}); err != nil {
		t.Fatalf("EnsureTopology(%q): %v", destination, err)
	}

	var loopDeadline time.Time
	ctx := newProbeRetryBudgetContext(&loopDeadline)
	start := realNow()
	errs := cleanupProfileErrors(ctx, conn, nil, nil, destination)
	if len(errs) != 1 {
		t.Fatalf("cleanup errors = %v, want one unreclaimed-destination error", errs)
	}
	if !strings.Contains(errs[0].Error(), "retry budget exhausted") {
		t.Fatalf("cleanup error = %v, want retry budget exhaustion", errs[0])
	}
	if !strings.Contains(errs[0].Error(), "after 5 attempts") {
		t.Fatalf("cleanup error = %v, want five attempts before retry budget exhaustion", errs[0])
	}
	if conn.pruneCalls != 5 {
		t.Fatalf("Prune calls = %d, want 5", conn.pruneCalls)
	}
	if len(conn.pruneDeadlines) != conn.pruneCalls {
		t.Fatalf("Prune deadlines = %d, want one for every call", len(conn.pruneDeadlines))
	}
	for attempt, deadline := range conn.pruneDeadlines {
		if deadline.IsZero() {
			t.Fatalf("Prune attempt %d received a context without a deadline", attempt+1)
		}
	}
	finalDeadline := conn.pruneDeadlines[len(conn.pruneDeadlines)-1]
	if !finalDeadline.After(loopDeadline) {
		t.Fatalf("final Prune deadline = %s, want after retry loop deadline %s", finalDeadline, loopDeadline)
	}
	if elapsed := realNow().Sub(start); elapsed > pruneRetryBudget+pruneAttemptTimeout+pruneRetryDelay {
		t.Fatalf("cleanup elapsed = %s, exceeded loop budget plus one attempt %s", elapsed, pruneRetryBudget+pruneAttemptTimeout)
	}
}

type probeRetryBudgetContext struct {
	context.Context
	loopDeadline *time.Time
	deadline     time.Time
	calls        int
}

func newProbeRetryBudgetContext(loopDeadline *time.Time) *probeRetryBudgetContext {
	return &probeRetryBudgetContext{
		Context:      context.Background(),
		loopDeadline: loopDeadline,
		deadline:     realNow().Add(24 * time.Hour),
	}
}

func (c *probeRetryBudgetContext) Deadline() (time.Time, bool) {
	if c.calls == 0 {
		// Keep a conservative margin while recording the loop deadline relation.
		*c.loopDeadline = realNow().Add(pruneRetryBudget + pruneRetryDelay)
	}
	c.calls++
	return c.deadline, true
}

func TestInspectProbeTeardownSurvivesARefusedStop(t *testing.T) {
	conn := newProbeTestConn()
	const destination = "conformance.inspect.refused-stop.probe"
	conn.queues[destination] = []driver.OutboundMessage{{Body: []byte("unsettled")}}
	consumer := &refusingProbeConsumer{events: &conn.events, conn: conn.runTestConn, destination: destination, attached: &conn.attached}
	producer := &recordingProbeProducer{events: &conn.events}

	errs := cleanupProfileErrors(context.Background(), conn, producer, consumer, destination)
	if len(errs) != 1 || !errors.Is(errs[0], driver.ErrResourcesOutstanding) {
		t.Fatalf("cleanup errors = %v, want the original ErrResourcesOutstanding refusal", errs)
	}
	if !strings.Contains(errs[0].Error(), "stop profile consumer") {
		t.Fatalf("cleanup error = %v, want stop context", errs[0])
	}
	wantEvents := []string{"stop", "release", "purge", "prune", "producer-close"}
	if strings.Join(conn.events, ",") != strings.Join(wantEvents, ",") {
		t.Fatalf("teardown events = %v, want %v", conn.events, wantEvents)
	}
	if _, err := conn.Admin().DescribeTopology(context.Background(), []string{destination}); !errors.Is(err, driver.ErrDestinationMissing) {
		t.Fatalf("DescribeTopology(%q) error = %v, want ErrDestinationMissing", destination, err)
	}
}

func withoutConformanceGroups(t *testing.T) {
	t.Helper()
	manifest := groupManifest
	pending := pendingGroups
	runners := groupRunners
	groupManifest = nil
	pendingGroups = nil
	groupRunners = map[string]groupRunner{}
	t.Cleanup(func() {
		groupManifest = manifest
		pendingGroups = pending
		groupRunners = runners
	})
}

type probeTestDriver struct {
	conn *probeTestConn
}

func (probeTestDriver) Name() string                      { return "probe-test" }
func (probeTestDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (d probeTestDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return d.conn, nil
}

func probeTestInspector(raw driver.Conn) (Inspect, error) {
	conn, ok := raw.(*probeTestConn)
	if !ok {
		return nil, errors.New("unexpected probe test connection")
	}
	return probeTestInspect(conn), nil
}

func probeTestInspect(conn *probeTestConn) Inspect {
	inspect, _ := runTestInspector(conn.runTestConn)
	return inspect
}

type probeTestConn struct {
	*runTestConn
	created              []string
	events               []string
	attached             bool
	pruneRefusals        int
	pruneTransientErrors int
	prunePermanentError  bool
	pruneForever         bool
	pruneCalls           int
	pruneDeadlines       []time.Time
}

func newProbeTestConn() *probeTestConn {
	return &probeTestConn{runTestConn: &runTestConn{
		queues:    make(map[string][]driver.OutboundMessage),
		unsettled: make(map[string]int),
		specs:     make(map[string]driver.DestinationSpec),
	}}
}

func (c *probeTestConn) Admin() driver.Admin {
	return probeTestAdmin{
		conn:                 c.runTestConn,
		created:              &c.created,
		events:               &c.events,
		attached:             &c.attached,
		pruneRefusals:        c.pruneRefusals,
		pruneTransientErrors: c.pruneTransientErrors,
		prunePermanentError:  c.prunePermanentError,
		pruneForever:         c.pruneForever,
		pruneCalls:           &c.pruneCalls,
		pruneDeadlines:       &c.pruneDeadlines,
	}
}

type probeTestAdmin struct {
	conn                 *runTestConn
	created              *[]string
	events               *[]string
	attached             *bool
	pruneRefusals        int
	pruneTransientErrors int
	prunePermanentError  bool
	pruneForever         bool
	pruneCalls           *int
	pruneDeadlines       *[]time.Time
}

func (a probeTestAdmin) EnsureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	diff, err := (runTestAdmin{conn: a.conn}).EnsureTopology(ctx, spec)
	if err != nil {
		return diff, err
	}
	*a.created = append(*a.created, diff.CreatedDestinations...)
	return diff, nil
}

func (a probeTestAdmin) DescribeTopology(ctx context.Context, names []string) (driver.TopologyState, error) {
	for _, name := range names {
		if _, exists := a.conn.queues[name]; !exists {
			return driver.TopologyState{}, &driver.Error{Driver: "probe-test", Op: "describe", K: driver.KindNotFound, Err: driver.ErrDestinationMissing}
		}
	}
	return (runTestAdmin{conn: a.conn}).DescribeTopology(ctx, names)
}

func (a probeTestAdmin) Purge(ctx context.Context, destination string) (int64, error) {
	*a.events = append(*a.events, "purge")
	return (runTestAdmin{conn: a.conn}).Purge(ctx, destination)
}

func (a probeTestAdmin) Prune(ctx context.Context, names []string) ([]driver.PruneResult, error) {
	*a.events = append(*a.events, "prune")
	*a.pruneCalls = *a.pruneCalls + 1
	deadline, _ := ctx.Deadline()
	*a.pruneDeadlines = append(*a.pruneDeadlines, deadline)
	results := make([]driver.PruneResult, 0, len(names))
	for _, name := range names {
		if _, exists := a.conn.queues[name]; !exists {
			results = append(results, driver.PruneResult{Name: name, Reason: "missing"})
			continue
		}
		if a.prunePermanentError {
			return nil, &driver.Error{Driver: "probe-test", Op: "prune", K: driver.KindFatal, Err: errors.New("permanent prune error")}
		}
		if *a.pruneCalls <= a.pruneTransientErrors {
			return nil, &driver.Error{Driver: "probe-test", Op: "prune", K: driver.KindTransient, Err: errors.New("transient prune error")}
		}
		if a.pruneForever {
			<-ctx.Done()
			return nil, &driver.Error{Driver: "probe-test", Op: "prune", K: driver.KindTransient, Err: ctx.Err()}
		}
		if a.pruneRefusals > 0 && *a.pruneCalls <= a.pruneRefusals {
			results = append(results, driver.PruneResult{Name: name, Reason: "consumer attached"})
			continue
		}
		if (a.attached != nil && *a.attached) || a.conn.unsettled[name] != 0 {
			results = append(results, driver.PruneResult{Name: name, Reason: "consumer attached"})
			continue
		}
		delete(a.conn.queues, name)
		results = append(results, driver.PruneResult{Name: name, Deleted: true})
	}
	return results, nil
}

type refusingProbeConsumer struct {
	events      *[]string
	conn        *runTestConn
	destination string
	attached    *bool
}

func (c *refusingProbeConsumer) Messages() <-chan driver.InboundMessage { return nil }
func (c *refusingProbeConsumer) Errors() <-chan error                   { return nil }
func (*refusingProbeConsumer) Pause(...string) error                    { return nil }
func (*refusingProbeConsumer) Resume(...string) error                   { return nil }
func (*refusingProbeConsumer) Drain(context.Context) error              { return nil }
func (c *refusingProbeConsumer) Stop(context.Context) error {
	*c.events = append(*c.events, "stop")
	*c.attached = true
	return driver.ErrResourcesOutstanding
}

func (c *refusingProbeConsumer) Release(context.Context) error {
	*c.events = append(*c.events, "release")
	*c.attached = false
	c.conn.queues[c.destination] = append(c.conn.queues[c.destination], driver.OutboundMessage{Body: []byte("released")})
	return nil
}
func (*refusingProbeConsumer) Lag(context.Context) (map[string]int64, error) { return nil, nil }

type recordingProbeProducer struct {
	events *[]string
}

func (*recordingProbeProducer) Publish(context.Context, ...driver.OutboundMessage) error { return nil }
func (*recordingProbeProducer) Flush(context.Context) error                              { return nil }
func (p *recordingProbeProducer) Close(context.Context) error {
	*p.events = append(*p.events, "producer-close")
	return nil
}
