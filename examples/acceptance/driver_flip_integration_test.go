//go:build integration

package acceptance_test

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver/conformance"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/kafka"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/rabbitmq"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// The run is gated rather than skipped silently: make test-driver-flip sets
// this variable and F1_REQUIRE_*, so a missing broker fails the target instead
// of reporting success for a suite that never ran. The integration build tag
// alone is not enough, because the other integration suites need one broker
// where this one needs two.
const flipGateEnv = "F1_DRIVER_FLIP"

const (
	flipCorpusEnv = "F1_DRIVER_FLIP_CORPUS"
	flipOutEnv    = "F1_DRIVER_FLIP_OUT"

	// flipStepTimeout bounds readiness and shutdown, which are seconds of work.
	// flipCorpusTimeout bounds the corpus itself: a message is published only
	// after the broker confirms the previous one.
	flipStepTimeout   = 2 * time.Minute
	flipCorpusTimeout = 20 * time.Minute

	flipRetryTiers = 1

	// A prune can be refused while a consumer group still holds membership, so
	// teardown retries a few times before reporting what it could not delete.
	flipPruneAttempts = 5
	flipPruneWait     = time.Second

	// The subscription deployment is polled at this interval while the
	// consumer's runner declares it.
	flipTopologyPoll = 100 * time.Millisecond

	// A corpus run that records no terminal event for this long has stopped.
	// flipProgressPoll bounds how often the harness looks.
	flipStallTimeout = 60 * time.Second
	flipProgressPoll = time.Second
)

// flipClock is used only for the bounded wait between prune attempts; the test
// holds no fake clock because the fixtures are real brokers.
var flipClock = clock.NewReal()

// TestDriverFlipAcceptance runs the acceptance publisher and consumer, unchanged,
// against each driver with the same corpus, then diffs the two runs.
//
// The vector is ordered by corpus position rather than arrival order. Arrival
// order is not a declared behaviour: with the default subscription mode neither
// driver promises cross-message order, and a partition-bound consumer
// legitimately interleaves partitions that a single broker queue does not. The
// observed arrival order is recorded and reported next to the vector, so an
// ordering difference is visible rather than silently sorted away.
func TestDriverFlipAcceptance(t *testing.T) {
	if os.Getenv(flipGateEnv) != "1" {
		t.Skipf("driver-flip acceptance: set %s=1, or run make test-driver-flip, to perform the run", flipGateEnv)
	}

	corpus := flipEnvInt(flipCorpusEnv, 10000)
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	outDir := flipEnv(flipOutEnv, filepath.Join(repoRoot, ".cache", "driver-flip"))
	if err := os.MkdirAll(outDir, 0o750); err != nil {
		t.Fatalf("create output directory %s: %v", outDir, err)
	}

	binDir := t.TempDir()
	for _, service := range []string{"publisher", "consumer"} {
		buildAcceptanceService(t, repoRoot, binDir, service)
	}
	image := binaryDigests(t, binDir)
	t.Logf("DRIVER-FLIP image (built once, run by both drivers)\n%s", image)

	// The corpus namespace is unique per invocation, so a repeated run cannot
	// read a previous run's messages or a previous Kafka group's committed
	// offsets. The two driver runs below share it, which is what makes their
	// inputs comparable.
	namespace := flipNamespace(t)
	topic := "acceptance.flip." + namespace
	subscription := "acceptance-flip-" + namespace

	specs := []flipSpec{
		{name: "kafka", endpoint: flipEnv("F1_KAFKA_ENDPOINT", "localhost:19092"), driver: kafka.Driver{}},
		{name: "rabbitmq", endpoint: flipEnv("F1_RABBITMQ_ENDPOINT", "amqp://guest:guest@localhost:5672/"), driver: rabbitmq.Driver{}},
	}
	env := "acceptance"
	for i := range specs {
		// The comparison uses the connection's capabilities, not the driver's
		// ceiling: a connection may reduce what the driver advertises, and it is
		// the reduced set the run's Client.Limits() reports.
		specs[i].caps = flipConnCapabilities(t, specs[i])
		spec := specs[i]
		topology := flipTopologyFor(spec.caps, env, topic, subscription)
		t.Cleanup(func() { purgeFlipDestinations(t, spec, topology.destinations) })
	}

	runs := make([]flipRun, 0, len(specs))
	for _, spec := range specs {
		manifestPath := filepath.Join(outDir, "business-tree-"+spec.name+".txt")
		run := runFlipDriver(t, spec, flipRunConfig{
			binDir:       binDir,
			outDir:       outDir,
			repoRoot:     repoRoot,
			env:          env,
			topic:        topic,
			subscription: subscription,
			corpus:       corpus,
			topology:     flipTopologyFor(spec.caps, env, topic, subscription),
		})
		writeArtifact(t, manifestPath, run.manifest)
		runs = append(runs, run)
	}

	first, second := runs[0], runs[1]

	// Assertion 1: the business package tree is byte-identical between the two
	// runs. The diff is run, not asserted about, and its empty output is the
	// evidence.
	firstManifest := filepath.Join(outDir, "business-tree-"+first.spec.name+".txt")
	secondManifest := filepath.Join(outDir, "business-tree-"+second.spec.name+".txt")
	treeDiff, treeErr := exec.Command("diff", "-u", firstManifest, secondManifest).CombinedOutput() //nolint:gosec // both paths are harness-written artifacts under the configured output directory.
	if treeErr != nil {
		t.Errorf("business package tree differs between the two runs\n$ diff -u %s %s\n%s", firstManifest, secondManifest, treeDiff)
	} else {
		t.Logf("DRIVER-FLIP assertion 1: business package tree byte-identical\n$ diff -u %s %s\n(output empty, %d bytes hashed per run)", firstManifest, secondManifest, len(first.manifest))
	}

	// Assertion 2: behaviour vectors identical.
	vectorDiff := first.vector.Diff(second.vector)
	if vectorDiff != "" {
		t.Errorf("behaviour vectors differ: %s\n%s", vectorDiff, strings.Join(vectorFieldDiff(first.vector, second.vector), "\n"))
	}
	t.Logf("DRIVER-FLIP assertion 2: conformance.BehaviorVector.Diff(%s, %s) = %q", first.spec.name, second.spec.name, vectorDiff)

	// Assertion 3: Client.Limits() differs, and every difference is declared.
	assertLimitsDeclared(t, first, second)

	reportArrivalOrder(t, first, second)

	t.Logf("DRIVER-FLIP summary corpus=%d %s=%s %s=%s", corpus, first.spec.name, first.summary, second.spec.name, second.summary)
}

type flipSpec struct {
	name     string
	endpoint string
	driver   driver.Driver
	// caps is the connection's capability set, filled once per run before the
	// services start.
	caps driver.Capabilities
}

// flipLimits mirrors the JSON LIMITS line the consumer prints from
// Client.Limits(), which is the only form the harness can compare.
type flipLimits struct {
	Driver   string              `json:"driver"`
	Broker   string              `json:"broker"`
	Features []flipFeatureStatus `json:"features"`
}

type flipFeatureStatus struct {
	Feature string `json:"feature"`
	Mode    string `json:"mode"`
	Detail  string `json:"detail,omitempty"`
}

type flipRunConfig struct {
	binDir       string
	outDir       string
	repoRoot     string
	env          string
	topic        string
	subscription string
	corpus       int
	topology     flipTopology
}

type flipRun struct {
	spec         flipSpec
	config       flipRunConfig
	vector       conformance.BehaviorVector
	arrivalOrder []string
	limits       *flipLimits
	summary      string
	manifest     []byte
}

// runFlipDriver deploys one driver's pair of services and records what they did.
func runFlipDriver(t *testing.T, spec flipSpec, config flipRunConfig) flipRun {
	t.Helper()
	childEnv := append(os.Environ(),
		"F1_ACCEPTANCE_DRIVER="+spec.name,
		"F1_ACCEPTANCE_ENDPOINT="+spec.endpoint,
		"F1_ACCEPTANCE_ENV="+config.env,
		"F1_ACCEPTANCE_TOPIC="+config.topic,
		"F1_ACCEPTANCE_SUBSCRIPTION="+config.subscription,
		"F1_ACCEPTANCE_COUNT="+strconv.Itoa(config.corpus),
	)

	manifest, err := businessTreeManifest(config.repoRoot)
	if err != nil {
		t.Fatalf("%s: hash the business package tree: %v", spec.name, err)
	}

	// The consumer starts first, and publishing waits for the destinations its
	// subscription declares: Subscribe returning is not the destinations
	// existing, and a publish that arrives before them is refused with
	// ErrDestinationMissing on a broker that fans out at publish time, so the
	// readiness line alone is not a barrier.
	consumer := startService(t, spec.name+" consumer", filepath.Join(config.binDir, "consumer"), childEnv, config.corpus,
		"CONSUMER_READY ", filepath.Join(config.outDir, "consumer-"+spec.name+".log"))
	waitForService(t, consumer, "CONSUMER_READY", flipStepTimeout)
	waitForDeployment(t, spec, config.topology, flipStepTimeout)

	publishCorpus(t, spec, config, childEnv)

	waitForCorpus(t, consumer, flipCorpusTimeout)
	stopService(t, consumer, flipStepTimeout)

	vector, arrival, limits, summary := parseConsumerRun(t, consumer.lines(), config.corpus)
	assertCorpusOutcomes(t, spec.name, summary, config.corpus)

	run := flipRun{
		spec:         spec,
		config:       config,
		vector:       vector,
		arrivalOrder: arrival,
		limits:       limits,
		summary:      summary,
		manifest:     manifest,
	}
	writeArtifact(t, filepath.Join(config.outDir, "vector-"+spec.name+".txt"), formatVector(run.vector))
	writeArtifact(t, filepath.Join(config.outDir, "arrival-"+spec.name+".txt"), []byte(strings.Join(run.arrivalOrder, "\n")+"\n"))
	return run
}

// serviceProcess is one acceptance service under test, with its output read
// concurrently so a full pipe cannot block it.
type serviceProcess struct {
	name  string
	cmd   *exec.Cmd
	ready string

	mu        sync.Mutex
	recorded  []string
	terminals int
	corpus    int
	readySeen bool
	readyCh   chan struct{}

	done    chan struct{}
	waitErr error
	// readyOnce guards the readiness close.
	readyOnce sync.Once
}

func startService(t *testing.T, name, path string, env []string, corpus int, readyPrefix, logPath string) *serviceProcess {
	t.Helper()
	cmd := exec.Command(path)
	cmd.Env = env
	cmd.Dir = filepath.Dir(path)
	stderr := &strings.Builder{}
	cmd.Stderr = stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("%s: open stdout pipe: %v", name, err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("%s: start: %v", name, err)
	}
	process := &serviceProcess{
		name:    name,
		cmd:     cmd,
		ready:   readyPrefix,
		corpus:  corpus,
		readyCh: make(chan struct{}),
		done:    make(chan struct{}),
	}
	go func() {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			process.observe(scanner.Text())
		}
	}()
	go func() {
		process.waitErr = cmd.Wait()
		if stderr.Len() > 0 {
			process.observe("STDERR " + strings.TrimSpace(stderr.String()))
		}
		close(process.done)
	}()
	t.Cleanup(func() {
		select {
		case <-process.done:
		default:
			_ = cmd.Process.Kill()
			<-process.done
		}
		// The log is written from cleanup so a failed run still leaves what the
		// services printed, which is the first thing a failure needs.
		if err := os.WriteFile(logPath, []byte(strings.Join(process.lines(), "\n")+"\n"), 0o600); err != nil {
			t.Logf("%s: write log %s: %v", name, logPath, err)
		}
	})
	return process
}

func (p *serviceProcess) observe(line string) {
	p.mu.Lock()
	p.recorded = append(p.recorded, line)
	isReady := strings.HasPrefix(line, p.ready)
	switch {
	case isReady:
		p.readySeen = true
	case strings.HasPrefix(line, "HANDLED "), strings.HasPrefix(line, "DEAD_LETTER "):
		p.terminals++
	}
	p.mu.Unlock()
	if isReady {
		p.readyOnce.Do(func() { close(p.readyCh) })
	}
}

func (p *serviceProcess) lines() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.recorded)
}

func (p *serviceProcess) countTerminals() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.terminals
}

func (p *serviceProcess) output() string {
	return strings.Join(p.lines(), "\n")
}

// waitForService blocks until the named line appears, the process exits, or
// timeout elapses.
func waitForService(t *testing.T, p *serviceProcess, condition string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	select {
	case <-p.readyCh:
	case <-p.done:
		t.Fatalf("%s exited before %s\n%s", p.name, condition, p.output())
	case <-ctx.Done():
		t.Fatalf("%s did not reach %s within %s\n%s", p.name, condition, timeout, p.output())
	}
}

func stopService(t *testing.T, p *serviceProcess, timeout time.Duration) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Errorf("%s: SIGTERM: %v", p.name, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	select {
	case <-p.done:
	case <-ctx.Done():
		_ = p.cmd.Process.Kill()
		t.Fatalf("%s did not exit within %s of SIGTERM\n%s", p.name, timeout, p.output())
	}
	if p.waitErr != nil {
		t.Errorf("%s exited with %v\n%s", p.name, p.waitErr, p.output())
	}
}

// waitForCorpus blocks until the consumer has recorded one terminal event per
// corpus message. It fails on a stall as well as on the overall timeout: a
// consumer that stops delivering partway through looks identical to a slow one
// from the outside, and waiting out the whole timeout for every stalled run
// makes the check unusable.
func waitForCorpus(t *testing.T, p *serviceProcess, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	progress := flipClock.Now()
	seen := 0
	for {
		terminals := p.countTerminals()
		if terminals >= p.corpus {
			return
		}
		if terminals > seen {
			seen, progress = terminals, flipClock.Now()
		} else if flipClock.Since(progress) > flipStallTimeout {
			t.Fatalf("%s consumed %d of %d messages and stopped: no terminal event for %s\n%s",
				p.name, terminals, p.corpus, flipStallTimeout, p.output())
		}
		if err := flipClock.Sleep(ctx, flipProgressPoll); err != nil {
			t.Fatalf("%s consumed %d of %d messages within %s\n%s", p.name, terminals, p.corpus, timeout, p.output())
		}
	}
}

// waitForDeployment blocks until the consumer's subscription is deployed: every
// destination it declares exists, and on a broker that routes at publish time
// every publish lane is bound to its entry point.
//
// Subscribe returns before the runner declares its subscription topology, so
// the readiness line alone is not a deployment barrier. The binding check
// matters because the driver declares destinations first and bindings after a
// management-API listing, so a publish that starts as soon as the destinations
// exist still reaches an entry point with no queue bound to it. Verification is
// read-only: the consumer's own declaration does the creating.
func waitForDeployment(t *testing.T, spec flipSpec, topology flipTopology, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	conn, err := spec.driver.Open(ctx, driver.Config{
		Endpoints:      []string{spec.endpoint},
		ClientID:       "f1-driver-flip-topology",
		ConnectTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("%s: open to wait for the subscription deployment: %v", spec.name, err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Logf("%s: close deployment connection: %v", spec.name, err)
		}
	}()
	admin := conn.Admin()
	if admin == nil {
		t.Fatalf("%s: driver has no admin, so the subscription deployment cannot be observed", spec.name)
	}
	waitForCondition(t, ctx, spec.name, "subscription destinations "+strings.Join(topology.destinations, ", "), func(ctx context.Context) (bool, error) {
		state, err := admin.DescribeTopology(ctx, topology.destinations)
		if err != nil {
			if errors.Is(err, driver.ErrDestinationMissing) {
				return false, nil
			}
			return false, err
		}
		return len(state.Depth) == len(topology.destinations), nil
	})
	bindings := topology.publishLaneBindings()
	if len(bindings) == 0 {
		return
	}
	verify := driver.TopologySpec{Policy: driver.TopologyVerify, Effective: spec.caps, Bindings: bindings}
	waitForCondition(t, ctx, spec.name, "publish lane bindings", func(ctx context.Context) (bool, error) {
		if _, err := admin.EnsureTopology(ctx, verify); err != nil {
			if errors.Is(err, driver.ErrDestinationMissing) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	})
}

// waitForCondition polls condition until it holds, an error surfaces, or ctx
// expires.
func waitForCondition(t *testing.T, ctx context.Context, driverName, condition string, check func(context.Context) (bool, error)) {
	t.Helper()
	for {
		ok, err := check(ctx)
		if err != nil {
			t.Fatalf("%s: waiting for %s: %v", driverName, condition, err)
		}
		if ok {
			return
		}
		if err := flipClock.Sleep(ctx, flipTopologyPoll); err != nil {
			t.Fatalf("%s: %s did not arrive in time", driverName, condition)
		}
	}
}

// publishCorpus runs the publisher service once and requires the whole corpus.
// Deployment readiness was established before it started, so a partial or empty
// run is a result, not a race.
func publishCorpus(t *testing.T, spec flipSpec, config flipRunConfig, env []string) {
	t.Helper()
	logPath := filepath.Join(config.outDir, "publisher-"+spec.name+".log")
	publisher := startService(t, spec.name+" publisher", filepath.Join(config.binDir, "publisher"), env, 0, "PUBLISH_SUMMARY ", logPath)
	waitForService(t, publisher, "PUBLISH_SUMMARY", flipCorpusTimeout)
	stopService(t, publisher, flipStepTimeout)
	published, failures := publishOutcome(t, publisher)
	if published != config.corpus {
		t.Fatalf("%s publisher published %d of %d messages\n%s", spec.name, published, config.corpus, strings.Join(failures, "\n"))
	}
}

func publishOutcome(t *testing.T, publisher *serviceProcess) (int, []string) {
	t.Helper()
	var summary string
	var failures []string
	for _, line := range publisher.lines() {
		switch {
		case strings.HasPrefix(line, "PUBLISH_SUMMARY "):
			summary = line
		case strings.HasPrefix(line, "PUBLISH_ERROR "):
			failures = append(failures, line)
		}
	}
	if summary == "" {
		t.Fatalf("%s printed no PUBLISH_SUMMARY\n%s", publisher.name, publisher.output())
	}
	fields := parseFields(strings.TrimPrefix(summary, "PUBLISH_SUMMARY "))
	published, err := strconv.Atoi(fields["published"])
	if err != nil {
		t.Fatalf("%s: PUBLISH_SUMMARY has no published count: %s", publisher.name, summary)
	}
	return published, failures
}

// parseConsumerRun turns the consumer's stdout into the behaviour vector and the
// observations the report quotes. The vector is ordered by corpus position.
func parseConsumerRun(t *testing.T, lines []string, corpus int) (conformance.BehaviorVector, []string, *flipLimits, string) {
	t.Helper()
	bySequence := make(map[int]conformance.BehaviorEvent, corpus)
	arrival := make([]string, 0, corpus)
	var limits *flipLimits
	var summary string
	for _, line := range lines {
		switch {
		case strings.HasPrefix(line, "LIMITS "):
			var report flipLimits
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "LIMITS ")), &report); err != nil {
				t.Fatalf("LIMITS line is not valid JSON: %v\n%s", err, line)
			}
			limits = &report
		case strings.HasPrefix(line, "CONSUMER_SUMMARY "):
			summary = line
		case strings.HasPrefix(line, "HANDLED "):
			recordTerminalEvent(t, bySequence, &arrival, "handled", strings.TrimPrefix(line, "HANDLED "), line)
		case strings.HasPrefix(line, "DEAD_LETTER "):
			recordTerminalEvent(t, bySequence, &arrival, "dead_lettered", strings.TrimPrefix(line, "DEAD_LETTER "), line)
		}
	}
	vector := make(conformance.BehaviorVector, 0, corpus)
	for sequence := 1; sequence <= corpus; sequence++ {
		event, ok := bySequence[sequence]
		if !ok {
			t.Errorf("corpus message %d has no terminal event", sequence)
			continue
		}
		vector = append(vector, event)
	}
	if limits == nil {
		t.Error("consumer printed no LIMITS line")
	}
	if summary == "" {
		t.Error("consumer printed no CONSUMER_SUMMARY line")
	}
	return vector, arrival, limits, summary
}

func recordTerminalEvent(t *testing.T, bySequence map[int]conformance.BehaviorEvent, arrival *[]string, outcome, rest, line string) {
	t.Helper()
	fields := parseFields(rest)
	sequence, err := strconv.Atoi(fields["sequence"])
	if err != nil {
		t.Fatalf("terminal event has no sequence: %s", line)
	}
	attempt, err := strconv.Atoi(fields["attempt"])
	if err != nil {
		t.Fatalf("terminal event has no attempt: %s", line)
	}
	if _, duplicate := bySequence[sequence]; duplicate {
		t.Errorf("corpus message %d has more than one terminal event: %s", sequence, line)
	}
	bySequence[sequence] = conformance.BehaviorEvent{
		ID:               fields["key"],
		Outcome:          outcome,
		AttemptCount:     attempt,
		FinalDestination: fields["destination"],
	}
	*arrival = append(*arrival, fields["key"])
}

// parseFields reads key=value fields from a service line. A field without "=" or
// the error field ends the parse, because an error value contains spaces.
func parseFields(line string) map[string]string {
	fields := make(map[string]string, 8)
	for _, field := range strings.Fields(line) {
		key, value, ok := strings.Cut(field, "=")
		if !ok || key == "error" {
			break
		}
		fields[key] = value
	}
	return fields
}

// assertCorpusOutcomes proves the corpus applied the pressure the vector claims:
// the retry path ran, the dead-letter path ran, and every other message was
// handled. Without this a run that delivered nothing in an identical way to both
// drivers would pass assertion 2.
func assertCorpusOutcomes(t *testing.T, name, summary string, corpus int) {
	t.Helper()
	fields := parseFields(strings.TrimPrefix(summary, "CONSUMER_SUMMARY "))
	handled, err := strconv.Atoi(fields["handled"])
	if err != nil {
		t.Fatalf("%s: CONSUMER_SUMMARY has no handled count: %s", name, summary)
	}
	retried, err := strconv.Atoi(fields["retried"])
	if err != nil {
		t.Fatalf("%s: CONSUMER_SUMMARY has no retried count: %s", name, summary)
	}
	deadLettered, err := strconv.Atoi(fields["dead_lettered"])
	if err != nil {
		t.Fatalf("%s: CONSUMER_SUMMARY has no dead_lettered count: %s", name, summary)
	}
	if handled != corpus-1 || retried != 1 || deadLettered != 1 {
		t.Errorf("%s: summary %q, want handled=%d retried=1 dead_lettered=1", name, summary, corpus-1)
	}
}

// vectorFieldDiff names every differing message and field, because
// BehaviorVector.Diff reports only the first difference.
func vectorFieldDiff(a, b conformance.BehaviorVector) []string {
	var lines []string
	if len(a) != len(b) {
		lines = append(lines, fmt.Sprintf("length differs: %d vs %d", len(a), len(b)))
	}
	for i := range min(len(a), len(b)) {
		left, right := a[i], b[i]
		if left == right {
			continue
		}
		var differences []string
		if left.ID != right.ID {
			differences = append(differences, fmt.Sprintf("id %q vs %q", left.ID, right.ID))
		}
		if left.Outcome != right.Outcome {
			differences = append(differences, fmt.Sprintf("outcome %q vs %q", left.Outcome, right.Outcome))
		}
		if left.AttemptCount != right.AttemptCount {
			differences = append(differences, fmt.Sprintf("attempt %d vs %d", left.AttemptCount, right.AttemptCount))
		}
		if left.FinalDestination != right.FinalDestination {
			differences = append(differences, fmt.Sprintf("destination %q vs %q", left.FinalDestination, right.FinalDestination))
		}
		lines = append(lines, fmt.Sprintf("message %d (%s): %s", i+1, left.ID, strings.Join(differences, ", ")))
	}
	return lines
}

// assertLimitsDeclared checks assertion 3: the two runs report different limits,
// and each differing feature is explained by a declared capability rather than
// by an unexplained one.
func assertLimitsDeclared(t *testing.T, first, second flipRun) {
	t.Helper()
	if first.limits == nil || second.limits == nil {
		t.Fatal("both runs must report Client.Limits()")
	}
	t.Logf("DRIVER-FLIP assertion 3: %s limits driver=%s broker=%s\n%s", first.spec.name, first.limits.Driver, first.limits.Broker, formatLimits(first.limits))
	t.Logf("DRIVER-FLIP assertion 3: %s limits driver=%s broker=%s\n%s", second.spec.name, second.limits.Driver, second.limits.Broker, formatLimits(second.limits))
	if first.limits.Driver == second.limits.Driver {
		t.Errorf("both runs report driver %q, so the flip did not select two drivers", first.limits.Driver)
	}
	if first.limits.Broker == second.limits.Broker {
		t.Errorf("both runs report broker %q, so the flip did not reach two brokers", first.limits.Broker)
	}
	firstCaps := first.spec.caps
	secondCaps := second.spec.caps
	secondByName := make(map[string]flipFeatureStatus, len(second.limits.Features))
	for _, feature := range second.limits.Features {
		secondByName[feature.Feature] = feature
	}
	for _, feature := range first.limits.Features {
		other, ok := secondByName[feature.Feature]
		if !ok {
			t.Errorf("feature %s is absent from the %s limits", feature.Feature, second.spec.name)
			continue
		}
		if feature.Mode == other.Mode && feature.Detail == other.Detail {
			continue
		}
		declaration, differs := declaredCapabilityDifference(feature.Feature, firstCaps, secondCaps)
		t.Logf("DRIVER-FLIP assertion 3: feature %s is %s/%s on %s and %s/%s on %s; %s",
			feature.Feature, feature.Mode, feature.Detail, first.spec.name, other.Mode, other.Detail, second.spec.name, declaration)
		if !differs {
			t.Errorf("feature %s differs (%s/%s on %s, %s/%s on %s) with no declared capability difference: %s",
				feature.Feature, feature.Mode, feature.Detail, first.spec.name, other.Mode, other.Detail, second.spec.name, declaration)
		}
	}
	writeArtifact(t, filepath.Join(first.config.outDir, "limits-"+first.spec.name+".json"), []byte(formatLimits(first.limits)))
	writeArtifact(t, filepath.Join(second.config.outDir, "limits-"+second.spec.name+".json"), []byte(formatLimits(second.limits)))
}

// declaredCapabilityDifference maps one reported feature to the capability field
// that declares it, and reports whether the two drivers differ on that field.
func declaredCapabilityDifference(feature string, a, b driver.Capabilities) (string, bool) {
	switch feature {
	case "per_message_ack":
		return fmt.Sprintf("declared PerMessageAck %t vs %t", a.PerMessageAck, b.PerMessageAck), a.PerMessageAck != b.PerMessageAck
	case "ordered_by_key":
		return fmt.Sprintf("declared OrderedByKey %t vs %t", a.OrderedByKey, b.OrderedByKey), a.OrderedByKey != b.OrderedByKey
	case "native_delay":
		// The limits detail for native_delay carries the accuracy the driver
		// declares, so the feature differs when either the capability or the
		// bound on lateness differs. Comparing only NativeDelay reports a
		// difference the drivers declared as undeclared.
		return fmt.Sprintf("declared NativeDelay %t vs %t, DelayAccuracy %+v vs %+v",
				a.NativeDelay, b.NativeDelay, a.DelayAccuracy, b.DelayAccuracy),
			a.NativeDelay != b.NativeDelay || a.DelayAccuracy != b.DelayAccuracy
	case "delivery_count":
		return fmt.Sprintf("declared NativeDeliveryCount %t vs %t", a.NativeDeliveryCount, b.NativeDeliveryCount), a.NativeDeliveryCount != b.NativeDeliveryCount
	case "dlq_backstop":
		return fmt.Sprintf("declared NativeDLQ %t vs %t", a.NativeDLQ, b.NativeDLQ), a.NativeDLQ != b.NativeDLQ
	case "lag_metrics":
		return fmt.Sprintf("declared LagQueryable %t vs %t", a.LagQueryable, b.LagQueryable), a.LagQueryable != b.LagQueryable
	case "consumer_scaling":
		return fmt.Sprintf("declared ConsumerScaling %s vs %s", a.ConsumerScaling, b.ConsumerScaling), a.ConsumerScaling != b.ConsumerScaling
	case "priority_fairness":
		return "the core emulates priority_fairness for every driver, so it has no declaring capability", false
	default:
		return fmt.Sprintf("feature %q has no declared capability", feature), false
	}
}

// TestDeclaredCapabilityDifferenceNativeDelay pins the native_delay declaration
// check without a broker. The feature's limits detail carries the accuracy the
// driver declares, so a difference in either the capability or the accuracy is
// a difference the drivers declared. Without the accuracy arm, a run of two
// drivers where one declares a bound and the other does not reports a declared
// difference as undeclared, and the acceptance run fails on its own bookkeeping
// rather than on a divergence.
func TestDeclaredCapabilityDifferenceNativeDelay(t *testing.T) {
	declared := driver.DelayAccuracy{Floor: 500 * time.Millisecond, Relative: 1, MaxDelay: 64 * time.Second}
	tests := []struct {
		name        string
		a           driver.Capabilities
		b           driver.Capabilities
		wantDiffers bool
	}{
		{
			name: "equal on both fields",
			a:    driver.Capabilities{NativeDelay: true, DelayAccuracy: declared},
			b:    driver.Capabilities{NativeDelay: true, DelayAccuracy: declared},
		},
		{
			name:        "native delay differs",
			a:           driver.Capabilities{NativeDelay: true, DelayAccuracy: declared},
			b:           driver.Capabilities{NativeDelay: false, DelayAccuracy: declared},
			wantDiffers: true,
		},
		{
			name:        "delay accuracy differs",
			a:           driver.Capabilities{NativeDelay: false},
			b:           driver.Capabilities{NativeDelay: false, DelayAccuracy: declared},
			wantDiffers: true,
		},
		{
			name:        "both differ",
			a:           driver.Capabilities{NativeDelay: true},
			b:           driver.Capabilities{NativeDelay: false, DelayAccuracy: declared},
			wantDiffers: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			declaration, differs := declaredCapabilityDifference("native_delay", test.a, test.b)
			if differs != test.wantDiffers {
				t.Errorf("declaredCapabilityDifference(native_delay, ...) = %t, want %t: %s", differs, test.wantDiffers, declaration)
			}
			// The declaration is a failure's only account of what was compared,
			// so it names both fields the feature reports on.
			for _, field := range []string{"NativeDelay", "DelayAccuracy"} {
				if !strings.Contains(declaration, field) {
					t.Errorf("declaration %q does not name %s", declaration, field)
				}
			}
		})
	}
}

func reportArrivalOrder(t *testing.T, first, second flipRun) {
	t.Helper()
	if slices.Equal(first.arrivalOrder, second.arrivalOrder) {
		t.Logf("DRIVER-FLIP arrival order identical: %s and %s recorded the corpus in the same order", first.spec.name, second.spec.name)
		return
	}
	for i := range min(len(first.arrivalOrder), len(second.arrivalOrder)) {
		if first.arrivalOrder[i] != second.arrivalOrder[i] {
			t.Logf("DRIVER-FLIP arrival order differs at position %d: %s has %s, %s has %s (vector comparison is by corpus position, not arrival order)",
				i+1, first.spec.name, first.arrivalOrder[i], second.spec.name, second.arrivalOrder[i])
			return
		}
	}
	t.Logf("DRIVER-FLIP arrival order differs in length: %s has %d events, %s has %d", first.spec.name, len(first.arrivalOrder), second.spec.name, len(second.arrivalOrder))
}

// businessTreeManifest hashes every file of the two acceptance service packages.
func businessTreeManifest(repoRoot string) ([]byte, error) {
	var manifest strings.Builder
	for _, service := range []string{"publisher", "consumer"} {
		root := filepath.Join(repoRoot, "examples", "acceptance", service)
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			relative, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			digest, err := fileDigest(path)
			if err != nil {
				return err
			}
			fmt.Fprintf(&manifest, "%s  %s\n", digest, filepath.ToSlash(relative))
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return []byte(manifest.String()), nil
}

func binaryDigests(t *testing.T, dir string) string {
	t.Helper()
	var digests strings.Builder
	for _, service := range []string{"publisher", "consumer"} {
		digest, err := fileDigest(filepath.Join(dir, service))
		if err != nil {
			t.Fatalf("hash %s: %v", service, err)
		}
		fmt.Fprintf(&digests, "%s  %s\n", digest, service)
	}
	return digests.String()
}

func fileDigest(path string) (string, error) {
	//nolint:gosec // the paths are the harness's own build outputs and the repository's example sources.
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:]), nil
}

func buildAcceptanceService(t *testing.T, repoRoot, binDir, service string) {
	t.Helper()
	//nolint:gosec // the harness builds the two services it is about to deploy, from this repository.
	cmd := exec.Command("go", "build", "-o", filepath.Join(binDir, service), "./examples/acceptance/"+service)
	cmd.Dir = repoRoot
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s service: %v\n%s", service, err, output)
	}
}

func writeArtifact(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func formatVector(vector conformance.BehaviorVector) []byte {
	var formatted strings.Builder
	for _, event := range vector {
		fmt.Fprintf(&formatted, "%s outcome=%s attempt=%d destination=%s\n", event.ID, event.Outcome, event.AttemptCount, event.FinalDestination)
	}
	return []byte(formatted.String())
}

func formatLimits(report *flipLimits) []byte {
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return []byte(fmt.Sprintf("limits unencodable: %v", err))
	}
	return append(encoded, '\n')
}

// flipConnCapabilities reads the capability set the connection actually
// reports. Client.Limits() is built from this, so comparing the driver's
// advertised ceiling instead would call a declared reduction unexplained.
func flipConnCapabilities(t *testing.T, spec flipSpec) driver.Capabilities {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), flipStepTimeout)
	defer cancel()
	conn, err := spec.driver.Open(ctx, driver.Config{
		Endpoints:      []string{spec.endpoint},
		ClientID:       "f1-driver-flip-capabilities",
		ConnectTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("%s: open to read declared capabilities: %v", spec.name, err)
	}
	caps := conn.Capabilities()
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("%s: close capabilities connection: %v", spec.name, err)
	}
	return caps
}

// flipPriorities are the delivery lanes the acceptance application configures.
var flipPriorities = []string{"high", "medium", "low"}

// flipTopology names every destination and publish entry point one run uses, so
// the harness can wait for and later delete exactly what the run made. It
// mirrors the core's naming because the port has no scope-wide enumerate or
// delete operation.
type flipTopology struct {
	// entryPoints are the names a publisher addresses, one per lane.
	entryPoints []string
	// destinations are the names the subscription declares, one per lane first.
	destinations []string
}

func flipTopologyFor(caps driver.Capabilities, env, topic, subscription string) flipTopology {
	var topology flipTopology
	deadLetter := fmt.Sprintf("f1.%s.%s.dlq.%s", env, topic, subscription)
	for _, priority := range flipPriorities {
		entryPoint := fmt.Sprintf("f1.%s.%s.%s", env, topic, priority)
		main := entryPoint
		if caps.Fanout == driver.FanoutAtPublish {
			main = fmt.Sprintf("f1.%s.%s.%s.%s", env, topic, subscription, priority)
		}
		topology.entryPoints = append(topology.entryPoints, entryPoint)
		topology.destinations = append(topology.destinations, main)
	}
	topology.destinations = append(topology.destinations,
		deadLetter,
		fmt.Sprintf("f1.%s.unknown.dlq.%s", env, subscription),
	)
	if caps.NativeDLQ {
		topology.destinations = append(topology.destinations, deadLetter+".backstop")
	}
	for _, priority := range flipPriorities {
		for tier := 1; tier <= flipRetryTiers; tier++ {
			topology.destinations = append(topology.destinations, fmt.Sprintf("f1.%s.%s.%s.%s.retry.%d", env, topic, subscription, priority, tier))
		}
	}
	return topology
}

// publishLaneBindings pairs each lane's entry point with the destination it must
// reach. It is empty when the broker delivers from a shared destination and no
// binding exists, which is the case where the lanes are the same name.
func (topology flipTopology) publishLaneBindings() []driver.BindingSpec {
	if len(topology.entryPoints) == 0 || topology.entryPoints[0] == topology.destinations[0] {
		return nil
	}
	bindings := make([]driver.BindingSpec, 0, len(topology.entryPoints))
	for i := range topology.entryPoints {
		bindings = append(bindings, driver.BindingSpec{Source: topology.entryPoints[i], Destination: topology.destinations[i]})
	}
	return bindings
}

// purgeFlipDestinations empties and deletes the destinations one run created.
// A destination that was never created is skipped rather than reported: the run
// did not make it.
func purgeFlipDestinations(t *testing.T, spec flipSpec, destinations []string) {
	t.Helper()
	// Cleanup runs after the test context is canceled, so this probe carries its
	// own: the destinations still have to come out when the run before it failed.
	ctx, cancel := context.WithTimeout(context.Background(), flipStepTimeout)
	defer cancel()
	conn, err := spec.driver.Open(ctx, driver.Config{
		Endpoints:      []string{spec.endpoint},
		ClientID:       "f1-driver-flip-cleanup",
		ConnectTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Logf("cleanup %s: open: %v", spec.name, err)
		return
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Logf("cleanup %s: close: %v", spec.name, err)
		}
	}()
	maintenance, ok := conn.Admin().(driver.Maintenance)
	if !ok {
		t.Logf("cleanup %s: admin does not implement driver.Maintenance", spec.name)
		return
	}
	existing := make([]string, 0, len(destinations))
	for _, destination := range destinations {
		if _, err := maintenance.Purge(ctx, destination); err != nil {
			if !errors.Is(err, driver.ErrDestinationMissing) {
				t.Logf("cleanup %s: purge %s: %v", spec.name, destination, err)
			}
			continue
		}
		existing = append(existing, destination)
	}
	if len(existing) == 0 {
		return
	}
	for attempt := 0; attempt < flipPruneAttempts && len(existing) > 0; attempt++ {
		results, err := maintenance.Prune(ctx, existing)
		if err != nil {
			t.Logf("cleanup %s: prune: %v", spec.name, err)
			return
		}
		remaining := make([]string, 0, len(existing))
		for _, result := range results {
			if result.Deleted {
				continue
			}
			// A consumer group can still hold membership for a moment after the
			// process exits, and the prune guard refuses until it clears.
			remaining = append(remaining, result.Name)
		}
		existing = remaining
		if len(existing) == 0 {
			return
		}
		if err := flipClock.Sleep(ctx, flipPruneWait); err != nil {
			break
		}
	}
	if len(existing) > 0 {
		t.Logf("cleanup %s: still present after %d prune attempts: %s", spec.name, flipPruneAttempts, strings.Join(existing, ", "))
	}
}

func flipNamespace(t *testing.T) string {
	t.Helper()
	return strings.ToLower(filepath.Base(filepath.Dir(t.TempDir())))
}

func flipEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func flipEnvInt(key string, fallback int) int {
	value, err := strconv.Atoi(flipEnv(key, strconv.Itoa(fallback)))
	if err != nil || value < 1 {
		return fallback
	}
	return value
}
