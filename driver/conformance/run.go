package conformance

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func newRunID() (string, error) {
	var raw [16]byte
	if _, err := cryptorand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

const (
	pruneRetryAttempts  = 100
	pruneRetryDelay     = 200 * time.Millisecond
	pruneRetryBudget    = 10 * time.Second
	pruneAttemptTimeout = 2 * time.Second
)

func realTimer(duration time.Duration) *time.Timer {
	//nolint:forbidigo // conformance cleanup retries require wall-clock timing.
	return time.NewTimer(duration)
}

// deliveryCeiling reports how many deliveries a driver declaring capabilities
// may hold unsettled at once on one destination, given that a check's traffic
// reaches partitions distinct broker-side partitions, and capped at want.
//
// The cap needs both declarations the inference rests on. ScalingPartitionBound
// says one consumer holds one delivery per partition; OrderedByKey says a
// message key reaches one partition, so the key count a caller has is a
// partition count. A driver that denies key ordering has no key-to-partition
// map, so a key count cannot be converted into a partition count for it and it
// keeps the count it was asked for, which is the count the checks asked for
// before the waiver. Reading the partition bound off ScalingPartitionBound
// alone would take a per-partition admission ceiling from a driver that need
// not have partitions at all.
//
// A check derives its count here instead of hard-coding one because the port
// carries no partition count. A driver satisfying both declarations decides a
// destination's outstanding capacity from partitions the check cannot see, and
// a check that asks for more than the placement allows fails on the placement
// rather than on the behaviour it asserts. The result is compared with the
// inspector's view for equality, so partitions must be the number of partitions
// the traffic reaches and at least one.
func deliveryCeiling(capabilities driver.Capabilities, partitions, want int) int {
	if capabilities.OrderedByKey && capabilities.ConsumerScaling == driver.ScalingPartitionBound {
		return min(want, partitions)
	}
	return want
}

// Run opens one connection and runs the registered checks under the full and
// strict profiles described by suite. Suite.Driver and Suite.NewInspector are
// required. It returns the collected report, compares behavior vectors when
// both profiles complete, and reports check failures through t.
func Run(t *testing.T, suite Suite) Report {
	t.Helper()
	if suite.Driver == nil {
		t.Fatalf("conformance: Driver is required")
	}
	if suite.NewInspector == nil {
		t.Fatalf("conformance: NewInspector is required")
	}
	if err := validateManifestState(groupManifest, pendingGroups, groupRunners); err != nil {
		t.Fatalf("conformance: invalid group manifest: %v", err)
	}

	ctx := context.Background()
	factoryCapabilities := suite.Driver.Capabilities()
	conn, err := suite.Driver.Open(ctx, suite.Config)
	if err != nil {
		t.Fatalf("conformance: open driver: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("conformance: close driver: %v", err)
		}
	}()

	inspect, err := suite.NewInspector(conn)
	if validationErr := validateInspectorFactory(inspect, err); validationErr != nil {
		t.Fatalf("conformance: NewInspector: %v", validationErr)
	}
	runID, err := newRunID()
	if err != nil {
		t.Fatalf("conformance: generate run id: %v", err)
	}
	var inject FaultInjector
	if suite.NewFaultInjector != nil {
		inject, err = suite.NewFaultInjector(conn)
		if err != nil {
			t.Fatalf("conformance: NewFaultInjector: %v", err)
		}
		validateFaultInjector(t, ctx, conn, inject, runID)
	}
	var deadline DeadlineFixture
	if suite.NewDeadlineFixture != nil {
		deadline, err = suite.NewDeadlineFixture(conn)
		if err != nil {
			t.Fatalf("conformance: NewDeadlineFixture: %v", err)
		}
		validateDeadlineFixture(t, ctx, conn, deadline, runID)
	}

	report := Report{
		Driver:  suite.Driver.Name(),
		Pending: append([]string(nil), pendingGroups...),
	}
	defer func() {
		if data, err := json.Marshal(report); err == nil {
			t.Logf("conformance report: %s", data)
		} else {
			t.Errorf("conformance: marshal report: %v", err)
		}
	}()
	profileReports := make([]ProfileReport, 0, 2)
	profilesRan := 0
	groupsRan := 0
	for _, profile := range []Profile{ProfileFull, ProfileStrictPortability} {
		var result ProfileReport
		profileStarted := false
		profileCompleted := false
		t.Run(profile.String(), func(profileTest *testing.T) {
			profileStarted = true
			result = runProfile(profileTest, ctx, conn, inspect, runID, profile, factoryCapabilities, inject, deadline, &report, suite.Driver, suite.Config, suite.NewFaultInjector)
			profileCompleted = true
		})
		if profileCompleted {
			profilesRan++
			groupsRan += len(result.Groups)
			profileReports = append(profileReports, result)
		} else if profileStarted {
			t.Logf("conformance: profile=%s did not complete; behavior vector comparison may be skipped", profile)
		} else {
			t.Logf("conformance: profile=%s filtered out", profile)
		}
	}
	report.Profiles = profileReports
	if len(groupRunners) != 0 && groupsRan == 0 {
		t.Logf("conformance: behavior vector comparison skipped: no groups completed")
		t.Fatalf("conformance: filtered run executed no groups")
	}
	if profilesRan == 2 {
		if diff := report.Profiles[0].Vector.Diff(report.Profiles[1].Vector); diff != "" {
			t.Fatalf("conformance: full and strict behavior vectors differ: %s", diff)
		}
	} else {
		t.Logf("conformance: behavior vector comparison skipped: %d of 2 profiles completed", profilesRan)
	}
	for _, pending := range report.Pending {
		t.Logf("conformance: pending group=%s", pending)
	}
	return report
}

func runProfile(
	t *testing.T,
	ctx context.Context,
	conn driver.Conn,
	inspect Inspect,
	runID string,
	profile Profile,
	factoryCapabilities driver.Capabilities,
	inject FaultInjector,
	deadline DeadlineFixture,
	report *Report,
	drv driver.Driver,
	cfg driver.Config,
	injectFactory func(driver.Conn) (FaultInjector, error),
) ProfileReport {
	effective := effectiveCapabilities(conn.Capabilities(), profile)
	inspectScope := "conformance.inspect." + runID + "." + profile.String() + "."
	destination := inspectScope + "probe"
	var producer driver.Producer
	var consumer driver.Consumer
	messages := make([]driver.InboundMessage, 0, 2)
	t.Cleanup(func() {
		cleanupProfile(t, ctx, conn, producer, consumer, destination, messages...)
	})
	_, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination}},
		Scope:        []string{inspectScope},
		Effective:    effective,
	})
	if err != nil {
		t.Fatalf("ensure topology: %v", err)
	}
	producer, err = conn.Producer(ctx, driver.ProducerConfig{
		Effective: effective,
	})
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	before, err := inspect(ctx, destination)
	if err != nil {
		t.Fatalf("inspect baseline: %v", err)
	}
	// A partition-bound driver decides how many records a destination can carry
	// unsettled from a partition count the port does not carry, and this probe
	// creates its destination by name alone, so the count is the driver's and may
	// be one: a single partition cannot admit its next record before the first
	// settles, and the check below requires all n of them outstanding at the same
	// instant, so settling in between is not available either. The plurality
	// check is therefore waived for such a driver, which asks for one outstanding
	// record, and kept for a driver declaring free consumer scaling, whose
	// destinations no partition count bounds. The waiver is a loss and not a
	// simplification: two records also show Ready and Unsettled to be sums over
	// more than one record, and one only shows the arithmetic.
	n := 2
	if factoryCapabilities.ConsumerScaling == driver.ScalingPartitionBound {
		n = 1
	}
	for i := range n {
		if err := producer.Publish(ctx, driver.OutboundMessage{
			Destination: destination,
			Body:        []byte{byte(i)},
		}); err != nil {
			t.Fatalf("publish inspector probe %d: %v", i, err)
		}
	}
	afterPublish, err := inspect(ctx, destination)
	if err != nil {
		t.Fatalf("inspect after publish: %v", err)
	}

	consumer, err = conn.Consumer(ctx, driver.ConsumerConfig{
		Destinations: []string{destination},
		Prefetch:     n,
		Effective:    effective,
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	for i := range n {
		receiveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		select {
		case message := <-consumer.Messages():
			cancel()
			if message.Settle == nil {
				t.Fatalf("inspector probe message %d has nil settler", i)
			}
			messages = append(messages, message)
		case <-receiveCtx.Done():
			cancel()
			t.Fatalf("receive inspector probe %d: %v", i, receiveCtx.Err())
		}
	}
	afterReceive, err := inspect(ctx, destination)
	if err != nil {
		t.Fatalf("inspect after receive: %v", err)
	}
	for i := range messages {
		if err := messages[i].Settle.Ack(ctx); err != nil {
			t.Fatalf("settle inspector probe %d: %v", i, err)
		}
	}
	messages = messages[:0]
	afterSettle, err := inspect(ctx, destination)
	if err != nil {
		t.Fatalf("inspect after settle: %v", err)
	}
	if validationErr := validateInspectorDelta(before, afterPublish, afterReceive, afterSettle, n); validationErr != nil {
		t.Fatalf("Inspect self-check: %v", validationErr)
	}

	result := ProfileReport{Profile: profile, Vector: BehaviorVector{}, Groups: []GroupResult{}}
	profileFailed := false
	tracked := newTrackedConn(conn)
	// Registered before the first check can create anything, and on the
	// profile's own test, so a passing profile, a profile aborted by a fatal
	// assertion and a panic recovered by the testing package all reach it.
	t.Cleanup(func() {
		for _, err := range tracked.reclaimRun(ctx, runID) {
			t.Errorf("conformance %s profile reclaim: %v", profile, err)
		}
		// The measurement runs inside this cleanup, after the reclaim above,
		// and not in a second one registered beside it. t.Cleanup is LIFO, so
		// a cleanup registered after this one runs before the reclaim and
		// reports every destination the reclaim is about to delete; one
		// cleanup also keeps the order from depending on where a later
		// registration happens to land.
		survivors, err := tracked.unreclaimedDestinations(ctx, runID)
		if err != nil {
			t.Errorf("conformance %s profile reclaim verification: %v", profile, err)
		}
		if len(survivors) != 0 {
			t.Errorf("conformance %s profile left destinations behind: %s", profile, strings.Join(survivors, ", "))
		}
	})
	for _, entry := range groupManifest {
		runner, runnerExists := groupRunners[entry.name]
		if !runnerExists {
			continue
		}
		tracked.beginGroup()
		var groupResult *groupContext
		groupSkipped := false
		groupOK := t.Run(entry.name, func(groupTest *testing.T) {
			if entry.name == "failure" && inject == nil {
				groupSkipped = true
				groupTest.Skip("conformance failure fixture is not configured")
			}
			groupResult = &groupContext{
				t: groupTest, ctx: ctx, conn: tracked, inspect: inspect,
				profile: profile, effective: effective, factoryCapabilities: factoryCapabilities, inject: inject, report: report,
				drv: drv, cfg: cfg, injectFactory: injectFactory,
				checkNames: make(map[string]struct{}),
				skips:      make(map[string]string), deadline: deadline,
				runID: runID,
			}
			runner(groupResult)
		})
		groupFailed := !groupOK
		if !groupOK {
			profileFailed = true
			t.Errorf("conformance group %s failed", entry.name)
			for _, cleanupErr := range tracked.cleanup(ctx) {
				profileFailed = true
				t.Errorf("conformance group %s cleanup: %v", entry.name, cleanupErr)
			}
		}
		if groupSkipped {
			result.Groups = append(result.Groups, GroupResult{Name: entry.name, Declared: entry.declared, Status: "skipped"})
			continue
		}
		if groupResult == nil {
			if groupFailed {
				result.Groups = append(result.Groups, GroupResult{Name: entry.name, Declared: entry.declared, Status: "failed"})
			}
			t.Logf("conformance: group=%s filtered out, count not validated", entry.name)
			continue
		}
		observed := groupResult.checks
		if err := validateGroupCount(entry.name, entry.declared, observed); err != nil {
			profileFailed = true
			groupFailed = true
			t.Error(err)
		}
		result.Vector = append(result.Vector, groupResult.vector...)
		status := "passed"
		if groupFailed {
			status = "failed"
		} else if len(groupResult.skips) != 0 {
			status = "passed-with-skips"
		}
		skipped := make([]CheckSkip, 0, len(groupResult.skips))
		for name, reason := range groupResult.skips {
			skipped = append(skipped, CheckSkip{Name: name, Reason: reason})
		}
		sort.Slice(skipped, func(i, j int) bool { return skipped[i].Name < skipped[j].Name })
		result.Groups = append(result.Groups, GroupResult{Name: entry.name, Declared: entry.declared, Observed: observed, Status: status, Skipped: skipped})
	}
	if profileFailed {
		t.Errorf("conformance: %s profile failed", profile)
	}
	return result
}

type trackedConn struct {
	driver.Conn
	mu        sync.Mutex
	producers map[*trackedProducer]struct{}
	consumers map[*trackedConsumer]struct{}
	touched   map[string]struct{}
	// created holds every name touched so far in this profile, where touched
	// holds only the current group's. touched drives a failed group's cleanup;
	// created drives the reclaim the whole profile runs on its way out.
	created map[string]struct{}
}

func newTrackedConn(conn driver.Conn) *trackedConn {
	return &trackedConn{
		Conn:      conn,
		producers: make(map[*trackedProducer]struct{}),
		consumers: make(map[*trackedConsumer]struct{}),
		touched:   make(map[string]struct{}),
		created:   make(map[string]struct{}),
	}
}

func (c *trackedConn) beginGroup() {
	c.mu.Lock()
	c.touched = make(map[string]struct{})
	c.mu.Unlock()
}

func (c *trackedConn) rememberDestination(name string) {
	if name == "" {
		return
	}
	c.mu.Lock()
	c.touched[name] = struct{}{}
	c.created[name] = struct{}{}
	c.mu.Unlock()
}

// touchedDestinations is the names the current group has used so far.
func (c *trackedConn) touchedDestinations() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Sorted(maps.Keys(c.touched))
}

// createdDestinations is every name the profile has used, across all groups.
func (c *trackedConn) createdDestinations() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Sorted(maps.Keys(c.created))
}

func (c *trackedConn) Admin() driver.Admin {
	raw := c.Conn.Admin()
	base := &trackedAdminBase{Admin: raw, owner: c}
	maintenance, ok := raw.(driver.Maintenance)
	if !ok {
		return &trackedAdminWithoutMaintenance{trackedAdminBase: base}
	}
	return &trackedAdmin{trackedAdminBase: base, maintenance: maintenance}
}

func (c *trackedConn) Producer(ctx context.Context, cfg driver.ProducerConfig) (driver.Producer, error) {
	producer, err := c.Conn.Producer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	tracked := &trackedProducer{Producer: producer, owner: c}
	c.mu.Lock()
	c.producers[tracked] = struct{}{}
	c.mu.Unlock()
	return tracked, nil
}

func (c *trackedConn) Consumer(ctx context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	consumer, err := c.Conn.Consumer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	tracked := &trackedConsumer{Consumer: consumer, owner: c}
	for _, destination := range cfg.Destinations {
		c.rememberDestination(destination)
	}
	for destination := range cfg.PerDestination {
		c.rememberDestination(destination)
	}
	c.mu.Lock()
	c.consumers[tracked] = struct{}{}
	c.mu.Unlock()
	return tracked, nil
}

// cleanup reclaims the destinations the current group created, and is called
// before the next group starts when a group fails. Purging alone would leave
// every one of them on the broker, because Purge empties a destination and
// keeps it and Prune is the port's only deletion operation.
func (c *trackedConn) cleanup(ctx context.Context) []error {
	consumers, producers := c.trackedHandles()
	return c.reclaim(ctx, consumers, producers, c.touchedDestinations())
}

// reclaimRun reclaims every destination the profile created, whether the checks
// that created them passed or failed. It runs from a test cleanup registered
// before the first check, so a fatal assertion, a skipped check and a panic
// recovered by the testing package all reach it. A process killed outright is
// the one exit it cannot cover, and no exit inside the process can, because a
// later run must not delete the destinations of a run still using them.
func (c *trackedConn) reclaimRun(ctx context.Context, runID string) []error {
	recorded := c.createdDestinations()
	orphaned, err := c.discoverRunDestinations(ctx, runID, recorded)
	var errs []error
	if err != nil {
		// The prune set is still usable, just not complete, and saying so is
		// the difference between a short reclaim and an unexplained one.
		errs = append(errs, err)
	}
	destinations := slices.Compact(slices.Sorted(slices.Values(append(recorded, orphaned...))))
	consumers, producers := c.trackedHandles()
	return append(errs, c.reclaim(ctx, consumers, producers, destinations)...)
}

// unreclaimedDestinations reports the destinations this profile created that the
// broker still holds. reclaimRun attempts the deletes and cannot tell a driver
// that deleted a destination from one that only said so; a read taken after the
// reclaim is the only thing that separates the two, and this is that read.
//
// The candidates are the names the profile recorded plus the run's orphans under
// their scopes, which is the set reclaimRun prunes. Every one of them carries
// this run's id, so a destination that was on the fixture before the run can be
// neither reclaimed nor reported here: a shared fixture with history is normal,
// and this measurement does not clean up after anyone else.
//
// A driver without driver.Maintenance has no delete operation at all, so no run
// on one can leave the fixture clean. The reclaim prunes nothing there and this
// reports nothing, rather than failing a run for a capability the port makes
// optional.
func (c *trackedConn) unreclaimedDestinations(ctx context.Context, runID string) ([]string, error) {
	if _, ok := c.Conn.Admin().(driver.Maintenance); !ok {
		return nil, nil
	}
	recorded := c.createdDestinations()
	orphaned, err := c.discoverRunDestinations(ctx, runID, recorded)
	candidates := slices.Compact(slices.Sorted(slices.Values(append(recorded, orphaned...))))
	admin := c.Conn.Admin()
	survivors := make([]string, 0, len(candidates))
	for _, destination := range candidates {
		_, describeErr := admin.DescribeTopology(ctx, []string{destination})
		if errors.Is(describeErr, driver.ErrDestinationMissing) {
			continue
		}
		if describeErr != nil {
			// A destination whose existence the broker cannot answer for is
			// not a destination this run can claim to have removed.
			err = errors.Join(err, fmt.Errorf("describe %q after reclaim: %w", destination, describeErr))
			continue
		}
		survivors = append(survivors, destination)
	}
	return survivors, err
}

// discoverRunDestinations reads the run's remaining destinations off the broker
// under the same first name component as a destination this profile recorded.
// The tracked set is filled by the port wrappers, which cannot see a
// destination a check created on a connection it opened for itself, so the
// prune set is completed from the broker rather than trusted to the
// bookkeeping. The scope is derived from each head rather than fixed, because
// the run id sits after a destination's own first component and not at the
// front of it, so no single prefix covers a run.
func (c *trackedConn) discoverRunDestinations(ctx context.Context, runID string, recorded []string) ([]string, error) {
	if runID == "" {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(recorded))
	scopes := make([]string, 0, len(recorded))
	for _, destination := range recorded {
		head, _, ok := strings.Cut(destination, ".")
		if !ok {
			continue
		}
		scope := head + "." + runID + "."
		if _, duplicate := seen[scope]; duplicate {
			continue
		}
		seen[scope] = struct{}{}
		scopes = append(scopes, scope)
	}
	if len(scopes) == 0 {
		return nil, nil
	}
	diff, err := c.Conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Scope:     scopes,
		Effective: c.Capabilities(),
	})
	if err != nil {
		return nil, fmt.Errorf("discover run destinations: %w", err)
	}
	orphaned := make([]string, 0, len(diff.Orphaned))
	for _, orphan := range diff.Orphaned {
		orphaned = append(orphaned, orphan.Name)
	}
	return orphaned, nil
}

func (c *trackedConn) trackedHandles() ([]*trackedConsumer, []*trackedProducer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	consumers := make([]*trackedConsumer, 0, len(c.consumers))
	for consumer := range c.consumers {
		consumers = append(consumers, consumer)
	}
	producers := make([]*trackedProducer, 0, len(c.producers))
	for producer := range c.producers {
		producers = append(producers, producer)
	}
	return consumers, producers
}

func (c *trackedConn) reclaim(ctx context.Context, consumers []*trackedConsumer, producers []*trackedProducer, destinations []string) []error {
	var errs []error
	// Consumers first: a destination with one still attached is refused by
	// Prune, and a refused release is the difference between a destination that
	// comes back later and one that stays on the broker.
	for _, consumer := range consumers {
		if err := consumer.Release(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	// Destinations follow Prune's dependency order: a destination that other
	// destinations are bound to must be pruned after them, so retry refusals
	// after each pass that deletes something.
	pending := slices.Clone(destinations)
	for len(pending) > 0 {
		next := make([]string, 0, len(pending))
		var refused []error
		progressed := false
		for _, destination := range pending {
			if err := purgeAndPruneIfSupported(ctx, c.Conn, destination); err != nil {
				var refusal *pruneRefusalError
				if errors.As(err, &refusal) {
					next = append(next, destination)
					refused = append(refused, err)
					continue
				}
				errs = append(errs, err)
				continue
			}
			progressed = true
		}
		if len(next) == 0 {
			break
		}
		if !progressed {
			errs = append(errs, refused...)
			break
		}
		pending = next
	}
	for _, producer := range producers {
		if err := producer.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (c *trackedConn) removeProducer(producer *trackedProducer) {
	c.mu.Lock()
	delete(c.producers, producer)
	c.mu.Unlock()
}

func (c *trackedConn) removeConsumer(consumer *trackedConsumer) {
	c.mu.Lock()
	delete(c.consumers, consumer)
	c.mu.Unlock()
}

type trackedProducer struct {
	driver.Producer
	owner *trackedConn
}

func (p *trackedProducer) Publish(ctx context.Context, messages ...driver.OutboundMessage) error {
	for _, message := range messages {
		p.owner.rememberDestination(message.Destination)
	}
	return p.Producer.Publish(ctx, messages...)
}

func (p *trackedProducer) Close(ctx context.Context) error {
	err := p.Producer.Close(ctx)
	if err == nil {
		p.owner.removeProducer(p)
	}
	return err
}

type trackedConsumer struct {
	driver.Consumer
	owner *trackedConn
}

func (c *trackedConsumer) Stop(ctx context.Context) error {
	err := c.Consumer.Stop(ctx)
	if err == nil {
		c.owner.removeConsumer(c)
	}
	return err
}

func (c *trackedConsumer) Release(ctx context.Context) error {
	err := c.Consumer.Release(ctx)
	if err == nil {
		c.owner.removeConsumer(c)
	}
	return err
}

type trackedAdminBase struct {
	driver.Admin
	owner *trackedConn
}

func (a *trackedAdminBase) EnsureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	for _, destination := range spec.Destinations {
		a.owner.rememberDestination(destination.Name)
	}
	for _, binding := range spec.Bindings {
		a.owner.rememberDestination(binding.Destination)
	}
	return a.Admin.EnsureTopology(ctx, spec)
}

func (a *trackedAdminBase) DescribeTopology(ctx context.Context, names []string) (driver.TopologyState, error) {
	for _, name := range names {
		a.owner.rememberDestination(name)
	}
	return a.Admin.DescribeTopology(ctx, names)
}

type trackedAdmin struct {
	*trackedAdminBase
	maintenance driver.Maintenance
}

func (a *trackedAdmin) Purge(ctx context.Context, name string) (int64, error) {
	a.owner.rememberDestination(name)
	return a.maintenance.Purge(ctx, name)
}

func (a *trackedAdmin) Prune(ctx context.Context, names []string) ([]driver.PruneResult, error) {
	for _, name := range names {
		a.owner.rememberDestination(name)
	}
	return a.maintenance.Prune(ctx, names)
}

type trackedAdminWithoutMaintenance struct {
	*trackedAdminBase
}

func purgeIfSupported(ctx context.Context, conn driver.Conn, destination string) error {
	maintenance, ok := conn.Admin().(driver.Maintenance)
	if !ok {
		return nil
	}
	_, err := maintenance.Purge(ctx, destination)
	return err
}

func purgeAndPruneIfSupported(ctx context.Context, conn driver.Conn, destination string) error {
	maintenance, ok := conn.Admin().(driver.Maintenance)
	if !ok {
		return nil
	}
	if _, err := maintenance.Purge(ctx, destination); err != nil && !errors.Is(err, driver.ErrDestinationMissing) {
		return err
	}
	return pruneProfileDestination(ctx, maintenance, destination)
}

func runScopedDestination(runID, destination string) string {
	return destination + "." + runID
}

func validateFaultInjector(t *testing.T, ctx context.Context, conn driver.Conn, inject FaultInjector, runID string) {
	t.Helper()
	if inject == nil {
		t.Fatal("conformance: NewFaultInjector returned nil")
	}
	destination := runScopedDestination(runID, "conformance.fault-probe")
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{Destinations: []driver.DestinationSpec{{Name: destination}}}); err != nil {
		t.Fatalf("conformance: fault injector topology: %v", err)
	}
	producer, err := conn.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		_ = purgeAndPruneIfSupported(ctx, conn, destination)
		t.Fatalf("conformance: fault injector producer: %v", err)
	}
	defer func() {
		_ = producer.Close(ctx)
		if err := purgeAndPruneIfSupported(ctx, conn, destination); err != nil {
			t.Errorf("conformance: fault injector purge: %v", err)
		}
	}()
	if err := inject(ctx, FaultPublishFailure); err != nil {
		t.Fatalf("conformance: inject %s: %v", FaultPublishFailure, err)
	}
	err = producer.Publish(ctx, driver.OutboundMessage{Destination: destination})
	kind, classified := driver.Classify(err)
	if err == nil || !classified || kind != driver.KindTransient {
		t.Fatalf("conformance: fault injector %s was not observed as transient publish failure: %v", FaultPublishFailure, err)
	}
	for _, fault := range []FaultKind{FaultConnectionDrop, FaultDeliveryFailure} {
		validateFaultRedelivery(t, ctx, conn, inject, runID, fault)
	}
	validateFaultLaneClose(t, ctx, conn, inject, runID)
	fatalDestination := runScopedDestination(runID, "conformance.fault-fatal-probe")
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{Destinations: []driver.DestinationSpec{{Name: fatalDestination}}}); err != nil {
		t.Fatalf("conformance: fatal fault topology: %v", err)
	}
	fatalProducer, err := conn.Producer(ctx, driver.ProducerConfig{Effective: conn.Capabilities()})
	if err != nil {
		_ = purgeAndPruneIfSupported(ctx, conn, fatalDestination)
		t.Fatalf("conformance: fatal fault producer: %v", err)
	}
	defer func() {
		if err := fatalProducer.Close(ctx); err != nil {
			t.Errorf("conformance: fatal fault producer close: %v", err)
		}
		if err := purgeAndPruneIfSupported(ctx, conn, fatalDestination); err != nil {
			t.Errorf("conformance: fatal fault purge: %v", err)
		}
	}()
	if err := inject(ctx, FaultFatalPublish); err != nil {
		t.Fatalf("conformance: inject %s: %v", FaultFatalPublish, err)
	}
	fatalErr := fatalProducer.Publish(ctx, driver.OutboundMessage{Destination: fatalDestination})
	var fatalClassified driver.ClassifiedError
	if fatalErr == nil || !errors.As(fatalErr, &fatalClassified) || fatalClassified.Kind() != driver.KindFatal {
		t.Fatalf("conformance: fault injector %s was not observed as fatal non-retryable publish failure: %v", FaultFatalPublish, fatalErr)
	}
}

func validateFaultLaneClose(t *testing.T, ctx context.Context, conn driver.Conn, inject FaultInjector, runID string) {
	t.Helper()
	firstDestination := runScopedDestination(runID, "conformance.fault-lane-close.first")
	secondDestination := runScopedDestination(runID, "conformance.fault-lane-close.second")
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{Destinations: []driver.DestinationSpec{
		{Name: firstDestination}, {Name: secondDestination},
	}}); err != nil {
		t.Fatalf("conformance: lane close topology: %v", err)
	}
	producer, err := conn.Producer(ctx, driver.ProducerConfig{Effective: conn.Capabilities()})
	if err != nil {
		_ = purgeAndPruneIfSupported(ctx, conn, firstDestination)
		_ = purgeAndPruneIfSupported(ctx, conn, secondDestination)
		t.Fatalf("conformance: lane close producer: %v", err)
	}
	consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{
		Destinations: []string{firstDestination, secondDestination},
		Prefetch:     2,
		PerDestination: map[string]int{
			firstDestination:  1,
			secondDestination: 1,
		},
		Effective: conn.Capabilities(),
	})
	if err != nil {
		_ = producer.Close(ctx)
		_ = purgeAndPruneIfSupported(ctx, conn, firstDestination)
		_ = purgeAndPruneIfSupported(ctx, conn, secondDestination)
		t.Fatalf("conformance: lane close consumer: %v", err)
	}
	defer func() {
		if err := consumer.Stop(ctx); err != nil {
			t.Errorf("conformance: lane close consumer stop: %v", err)
		}
		if err := producer.Close(ctx); err != nil {
			t.Errorf("conformance: lane close producer close: %v", err)
		}
		for _, destination := range []string{firstDestination, secondDestination} {
			if err := purgeAndPruneIfSupported(ctx, conn, destination); err != nil {
				t.Errorf("conformance: lane close purge %q: %v", destination, err)
			}
		}
	}()
	if err := producer.Publish(ctx, driver.OutboundMessage{Destination: firstDestination, Body: []byte("first-before")}); err != nil {
		t.Fatalf("conformance: lane close first publish: %v", err)
	}
	first := receiveFaultProbe(t, ctx, consumer, FaultLaneChannelClose+" first lane")
	if first.Destination != firstDestination {
		t.Fatalf("conformance: first lane destination=%q, want %q", first.Destination, firstDestination)
	}
	if err := first.Settle.Ack(ctx); err != nil {
		t.Fatalf("conformance: first lane ack: %v", err)
	}
	if err := producer.Publish(ctx, driver.OutboundMessage{Destination: secondDestination, Body: []byte("second-before")}); err != nil {
		t.Fatalf("conformance: lane close second publish: %v", err)
	}
	second := receiveFaultProbe(t, ctx, consumer, FaultLaneChannelClose+" second lane")
	if second.Destination != secondDestination {
		t.Fatalf("conformance: second lane destination=%q, want %q", second.Destination, secondDestination)
	}
	if err := second.Settle.Ack(ctx); err != nil {
		t.Fatalf("conformance: second lane ack: %v", err)
	}
	if err := inject(ctx, FaultLaneChannelClose); err != nil {
		t.Fatalf("conformance: inject %s: %v", FaultLaneChannelClose, err)
	}
	faultErr := receiveFaultErrorProbe(t, ctx, consumer, FaultLaneChannelClose+" error")
	kind, classified := driver.Classify(faultErr)
	if !classified || kind != driver.KindTransient {
		t.Fatalf("conformance: lane close error classification=(%v,%t), want transient", kind, classified)
	}
	if err := producer.Publish(ctx, driver.OutboundMessage{Destination: secondDestination, Body: []byte("second-after")}); err != nil {
		t.Fatalf("conformance: surviving lane publish: %v", err)
	}
	survivor := receiveFaultProbe(t, ctx, consumer, FaultLaneChannelClose+" surviving lane")
	if survivor.Destination != secondDestination {
		t.Fatalf("conformance: surviving lane destination=%q, want %q", survivor.Destination, secondDestination)
	}
	if err := survivor.Settle.Ack(ctx); err != nil {
		t.Fatalf("conformance: surviving lane ack: %v", err)
	}
	openCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	select {
	case message, ok := <-consumer.Messages():
		if !ok {
			t.Fatal("conformance: lane close Messages channel closed before Stop")
		}
		t.Fatalf("conformance: lane close unexpected message from %q", message.Destination)
	case <-openCtx.Done():
	}
	if err := consumer.Stop(ctx); err != nil {
		t.Fatalf("conformance: lane close Stop: %v", err)
	}
}

func validateFaultRedelivery(t *testing.T, ctx context.Context, conn driver.Conn, inject FaultInjector, runID string, fault FaultKind) {
	t.Helper()
	destination := runScopedDestination(runID, "conformance.fault-"+string(fault))
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{Destinations: []driver.DestinationSpec{{Name: destination}}}); err != nil {
		t.Fatalf("conformance: %s topology: %v", fault, err)
	}
	producer, err := conn.Producer(ctx, driver.ProducerConfig{Effective: conn.Capabilities()})
	if err != nil {
		_ = purgeAndPruneIfSupported(ctx, conn, destination)
		t.Fatalf("conformance: %s producer: %v", fault, err)
	}
	consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{
		Destinations: []string{destination}, Prefetch: 1, Effective: conn.Capabilities(),
	})
	if err != nil {
		_ = producer.Close(ctx)
		_ = purgeAndPruneIfSupported(ctx, conn, destination)
		t.Fatalf("conformance: %s consumer: %v", fault, err)
	}
	defer func() {
		if err := consumer.Stop(ctx); err != nil {
			t.Errorf("conformance: %s consumer stop: %v", fault, err)
		}
		if err := producer.Close(ctx); err != nil {
			t.Errorf("conformance: %s producer close: %v", fault, err)
		}
		if err := purgeAndPruneIfSupported(ctx, conn, destination); err != nil {
			t.Errorf("conformance: %s purge: %v", fault, err)
		}
	}()
	if err := producer.Publish(ctx, driver.OutboundMessage{Destination: destination, Body: []byte("fault-probe")}); err != nil {
		t.Fatalf("conformance: %s publish: %v", fault, err)
	}
	first := receiveFaultProbe(t, ctx, consumer, fault+" first delivery")
	if err := inject(ctx, fault); err != nil {
		t.Fatalf("conformance: inject %s: %v", fault, err)
	}
	faultErr := receiveFaultErrorProbe(t, ctx, consumer, fault+" error")
	kind, classified := driver.Classify(faultErr)
	if !classified || kind != driver.KindTransient {
		t.Fatalf("conformance: %s error classification=(%v,%t), want transient", fault, kind, classified)
	}
	second := receiveFaultProbe(t, ctx, consumer, fault+" redelivery")
	if string(second.Body) != string(first.Body) {
		t.Fatalf("conformance: %s redelivery body=%q, want %q", fault, second.Body, first.Body)
	}
	if second.Settle == nil {
		t.Fatalf("conformance: %s redelivery has nil settler", fault)
	}
	if err := second.Settle.Ack(ctx); err != nil {
		t.Fatalf("conformance: %s redelivery ack: %v", fault, err)
	}
}

func receiveFaultProbe(t *testing.T, ctx context.Context, consumer driver.Consumer, what FaultKind) driver.InboundMessage {
	t.Helper()
	receiveCtx, cancel := context.WithTimeout(ctx, waitTimeout)
	defer cancel()
	select {
	case message, ok := <-consumer.Messages():
		if !ok {
			t.Fatalf("conformance: %s: Messages closed", what)
		}
		return message
	case <-receiveCtx.Done():
		t.Fatalf("conformance: %s: %v", what, receiveCtx.Err())
		return driver.InboundMessage{}
	}
}

func receiveFaultErrorProbe(t *testing.T, ctx context.Context, consumer driver.Consumer, what FaultKind) error {
	t.Helper()
	receiveCtx, cancel := context.WithTimeout(ctx, waitTimeout)
	defer cancel()
	for {
		select {
		case err, ok := <-consumer.Errors():
			if !ok {
				t.Fatalf("conformance: %s: Errors closed", what)
			}
			kind, classified := driver.Classify(err)
			if classified && kind == driver.KindNotification {
				continue
			}
			return err
		case <-receiveCtx.Done():
			t.Fatalf("conformance: %s: %v", what, receiveCtx.Err())
			return nil
		}
	}
}

func validateDeadlineFixture(t *testing.T, ctx context.Context, conn driver.Conn, fixture DeadlineFixture, runID string) {
	t.Helper()
	if fixture == nil {
		t.Fatal("conformance: NewDeadlineFixture returned nil")
	}
	destination := runScopedDestination(runID, "conformance.deadline-probe")
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination}},
	}); err != nil {
		t.Fatalf("conformance: deadline fixture topology: %v", err)
	}
	producer, err := conn.Producer(ctx, driver.ProducerConfig{
		Effective: conn.Capabilities(),
	})
	if err != nil {
		_ = purgeAndPruneIfSupported(ctx, conn, destination)
		t.Fatalf("conformance: deadline fixture producer: %v", err)
	}
	consumer, err := fixture.Consumer(ctx, 10*time.Millisecond, driver.ConsumerConfig{
		Destinations: []string{destination},
		Prefetch:     1,
		Effective:    conn.Capabilities(),
	})
	if err != nil {
		_ = producer.Close(ctx)
		_ = purgeAndPruneIfSupported(ctx, conn, destination)
		t.Fatalf("conformance: deadline fixture consumer: %v", err)
	}
	cleanup := func() {
		if err := consumer.Stop(ctx); err != nil {
			t.Errorf("conformance: deadline fixture stop: %v", err)
		}
		if err := producer.Close(ctx); err != nil {
			t.Errorf("conformance: deadline fixture close producer: %v", err)
		}
		if err := purgeAndPruneIfSupported(ctx, conn, destination); err != nil {
			t.Errorf("conformance: deadline fixture purge: %v", err)
		}
	}
	defer cleanup()

	if err := producer.Publish(ctx, driver.OutboundMessage{
		Destination: destination,
		Body:        []byte("deadline-positive-control"),
	}); err != nil {
		t.Fatalf("conformance: deadline fixture publish: %v", err)
	}
	first := receiveDeadlineProbe(t, ctx, consumer, "first delivery")
	fixture.Advance(20 * time.Millisecond)
	second := receiveDeadlineProbe(t, ctx, consumer, "deadline redelivery")
	if string(second.Body) != string(first.Body) {
		t.Fatalf("conformance: deadline fixture redelivery body = %q, want %q", second.Body, first.Body)
	}
	if conn.Capabilities().NativeDeliveryCount && second.DeliveryCount <= first.DeliveryCount {
		t.Fatalf("conformance: deadline fixture redelivery count = %d, first = %d", second.DeliveryCount, first.DeliveryCount)
	}
	if second.Settle == nil {
		t.Fatal("conformance: deadline fixture redelivery has nil settler")
	}
	if err := second.Settle.Ack(ctx); err != nil {
		t.Fatalf("conformance: deadline fixture ack: %v", err)
	}
}

func receiveDeadlineProbe(t *testing.T, ctx context.Context, consumer driver.Consumer, what string) driver.InboundMessage {
	t.Helper()
	receiveCtx, cancel := context.WithTimeout(ctx, waitTimeout)
	defer cancel()
	select {
	case message, ok := <-consumer.Messages():
		if !ok {
			t.Fatalf("conformance: deadline fixture %s: Messages closed", what)
		}
		return message
	case <-receiveCtx.Done():
		t.Fatalf("conformance: deadline fixture %s: %v", what, receiveCtx.Err())
		return driver.InboundMessage{}
	}
}

func cleanupProfile(t *testing.T, ctx context.Context, conn driver.Conn, producer driver.Producer, consumer driver.Consumer, destination string, messages ...driver.InboundMessage) {
	t.Helper()
	for _, err := range cleanupProfileErrors(ctx, conn, producer, consumer, destination, messages...) {
		t.Errorf("%v", err)
	}
}

func cleanupProfileErrors(ctx context.Context, conn driver.Conn, producer driver.Producer, consumer driver.Consumer, destination string, messages ...driver.InboundMessage) []error {
	var errs []error
	for _, message := range messages {
		if message.Settle != nil {
			if err := message.Settle.Nack(ctx, driver.NackOptions{}); err != nil {
				errs = append(errs, fmt.Errorf("nack profile message: %w", err))
			}
		}
	}
	if consumer != nil {
		if err := consumer.Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("stop profile consumer: %w", err))
			if errors.Is(err, driver.ErrResourcesOutstanding) {
				if releaseErr := consumer.Release(ctx); releaseErr != nil {
					errs = append(errs, fmt.Errorf("release profile consumer: %w", releaseErr))
				}
			}
		}
	}
	if maintenance, ok := conn.Admin().(driver.Maintenance); ok {
		if _, err := maintenance.Purge(ctx, destination); err != nil && !errors.Is(err, driver.ErrDestinationMissing) {
			errs = append(errs, fmt.Errorf("purge profile destination: %w", err))
		}
		if err := pruneProfileDestination(ctx, maintenance, destination); err != nil {
			errs = append(errs, err)
		}
	}
	if producer != nil {
		if err := producer.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("close profile producer: %w", err))
		}
	}
	return errs
}

type pruneRefusalError struct {
	destination string
	reason      string
}

func (e *pruneRefusalError) Error() string {
	return fmt.Sprintf("prune profile destination %q refused: %s", e.destination, e.reason)
}

func pruneProfileDestination(ctx context.Context, maintenance driver.Maintenance, destination string) error {
	retryCtx, cancel := context.WithTimeout(ctx, pruneRetryBudget)
	defer cancel()

	var last driver.PruneResult
	for attempt := 1; attempt <= pruneRetryAttempts; attempt++ {
		if err := retryCtx.Err(); err != nil {
			return pruneRetryBudgetError(destination, attempt-1, err)
		}

		attemptCtx, attemptCancel := context.WithTimeout(ctx, pruneAttemptTimeout)
		results, err := maintenance.Prune(attemptCtx, []string{destination})
		attemptCancel()
		if err != nil {
			if errors.Is(err, driver.ErrDestinationMissing) {
				return nil
			}
			if !transientPruneError(err) {
				return fmt.Errorf("prune profile destination %q: %w", destination, err)
			}
			if attempt == pruneRetryAttempts {
				break
			}
			if err := waitForPruneRetry(retryCtx, destination, attempt); err != nil {
				return err
			}
			continue
		}

		found := false
		for _, result := range results {
			if result.Name == destination {
				last = result
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("prune profile destination %q returned no result", destination)
		}
		if last.Deleted || pruneResultMissing(last) {
			return nil
		}
		if last.Reason == "" {
			return fmt.Errorf("prune profile destination %q reported not deleted without a reason", destination)
		}
		if !transientPruneReason(last.Reason) {
			return &pruneRefusalError{destination: destination, reason: last.Reason}
		}
		if attempt == pruneRetryAttempts {
			break
		}
		if err := waitForPruneRetry(retryCtx, destination, attempt); err != nil {
			return err
		}
	}
	return fmt.Errorf("prune profile destination %q was not deleted after %d attempts: %s", destination, pruneRetryAttempts, last.Reason)
}

func transientPruneError(err error) bool {
	var classified *driver.Error
	return errors.As(err, &classified) && classified.Retryable()
}

func waitForPruneRetry(ctx context.Context, destination string, attempt int) error {
	timer := realTimer(pruneRetryDelay)
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		return pruneRetryBudgetError(destination, attempt, ctx.Err())
	}
}

func pruneRetryBudgetError(destination string, attempts int, err error) error {
	return fmt.Errorf("prune profile destination %q retry budget exhausted after %d attempts: %w", destination, attempts, err)
}

func pruneResultMissing(result driver.PruneResult) bool {
	reason := strings.ToLower(strings.TrimSpace(result.Reason))
	return reason == "missing" || strings.Contains(reason, "does not exist") || strings.Contains(reason, "not found")
}

func transientPruneReason(reason string) bool {
	reason = strings.ToLower(strings.TrimSpace(reason))
	return (strings.Contains(reason, "consumer") && strings.Contains(reason, "attached")) ||
		strings.Contains(reason, "disappeared before deletion") ||
		strings.Contains(reason, "no longer prunable")
}

// WriteJSON writes the report as indented JSON followed by a newline and
// returns any encoding or writer error.
func (r Report) WriteJSON(w io.Writer) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(r)
}

// WriteMarkdown writes the driver name, capability results, and pending groups
// as Markdown, and returns any writer error.
func (r Report) WriteMarkdown(w io.Writer) error {
	if _, err := fmt.Fprintf(w, "# Conformance report: %s\n\n", r.Driver); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "| Profile | Capability | Declared | Status | Evidence |\n|---|---|---|---|---|"); err != nil {
		return err
	}
	for _, capability := range r.Capabilities {
		if _, err := fmt.Fprintf(w, "| %s | %s | %s | %s | %s |\n",
			capability.Profile, capability.Capability, capability.Declared,
			capability.Status, capability.Evidence); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(w, "\n## Pending groups"); err != nil {
		return err
	}
	for _, pending := range r.Pending {
		if _, err := fmt.Fprintf(w, "- %s\n", pending); err != nil {
			return err
		}
	}
	return nil
}
