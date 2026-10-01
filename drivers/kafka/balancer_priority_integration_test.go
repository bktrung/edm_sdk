//go:build integration

// This file measures the priority lanes on the balancer this driver ships: what
// a group of two or more members assigns them, how long a backed-up high lane
// takes to drain when slots are scarce, and what a join costs in first
// deliveries and duplicates.
//
// There is one arm, and the balancer is written into the client configuration
// rather than left unset so a printed line names the protocol its numbers came
// from.
//
// Everything here reads the broker through the public SDK (the priority runs)
// or through the driver port (the move runs). No package state is read, which is
// what keeps these numbers a description of what a caller observes rather than
// of what one implementation happens to do.
package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"

	kafka "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/kafka"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

const (
	// measureBalancer is the balancer every run in this file configures. It is
	// the value the driver defaults to, and it is written out rather than left
	// unset so the run's printed line says which protocol produced its numbers.
	measureBalancer = "cooperative-sticky"

	// measureRuns is how many times a shape runs, so a reading has its own
	// repeats beside it.
	measureRuns = 3

	// measureHandlerWork is the per-message handler cost, so slots are scarce
	// rather than the broker being the limit.
	measureHandlerWork = 5 * time.Millisecond
	// measureConcurrency is the subscription's worker count, per member.
	measureConcurrency = 2
	// measureBacklog is how many messages each lane carries before the members
	// start, so the drain is the measurement rather than the publish.
	measureBacklog = 2000
	// measurePublishBatchSize is how many messages one publish call carries.
	// The publish instant recorded for a message is its batch's, so this bounds
	// the error the "publish to handler start" wait carries.
	measurePublishBatchSize = 100

	// measureEventType is the type every message of the corpus carries. The
	// topic is supplied per publish, so this value only has to name a handler.
	measureEventType = "measure.lane.priority.v1"

	// measureAssignmentBound bounds the wait for the assignment to settle and
	// cover every partition.
	measureAssignmentBound = 60 * time.Second
	// measureAssignmentQuiet is how long the assignment log must go without a
	// member delivering from a partition it has not delivered from before,
	// which is how this harness recognises that a rebalance has finished.
	measureAssignmentQuiet = 750 * time.Millisecond
	// measureCompletionBound bounds a run's drain.
	measureCompletionBound = 120 * time.Second
	// measureLateAssignment is how long after the measured window a partition
	// may be first delivered before the run counts as having rebalanced under
	// the measurement.
	measureLateAssignment = time.Second

	// measureMoveCorpus is how many records the move runs publish. Two members
	// hold one unsettled delivery per partition when the join lands, and the
	// rest of the corpus is what they drain afterwards.
	measureMoveCorpus = 60
)

// measureLane is one lane a shape declares: the priority name the destination
// carries and the priority the publisher stamps on its messages.
type measureLane struct {
	name     string
	priority f1.Priority
}

// measureShape is one lane set and member count to measure.
type measureShape struct {
	name              string
	lanes             []measureLane
	partitionsPerLane int32
	members           int
}

// measureSpecifiedShape is the shape the ruling names: two members, a high and
// a low lane, four partitions each, so every member holds a share of each lane.
func measureSpecifiedShape() measureShape {
	return measureShape{
		name:              "specified",
		lanes:             []measureLane{{name: "high", priority: f1.PriorityHigh}, {name: "low", priority: f1.PriorityLow}},
		partitionsPerLane: 4,
		members:           2,
	}
}

// TestBalancerPriorityCost measures the priority cost of the shipped balancer on
// the shape a subscription ships with: two members, two lanes, four partitions
// per lane, a backlog on both lanes, and two workers per member. It runs three
// times, and each run asserts that every member holds a partition of every lane,
// which is the property a lane is meant to buy.
func TestBalancerPriorityCost(t *testing.T) {
	requirePortBroker(t)
	for run := 1; run <= measureRuns; run++ {
		measurePriorityRun(t, measureSpecifiedShape(), run)
	}
}

// measurePriorityRun is one weighted run: a fresh topic, a fresh group, a
// pre-published backlog on every lane, and one member per client.
//
// The members start after the backlog is published, so no message is handled
// before the group exists, and the measured window opens when every partition
// is known to be held by a member that is delivering from it. A partition first
// delivered more than measureLateAssignment after that is a rebalance landing
// inside the measurement, and the run is marked void.
func measurePriorityRun(t *testing.T, shape measureShape, run int) {
	t.Helper()
	ctx := t.Context()
	label := fmt.Sprintf("balancer=%s shape=%s members=%d partitions=%d run=%d", measureBalancer, shape.name, shape.members, shape.partitionsPerLane, run)

	topic := portUniqueName(t, "lane-priority")
	group := portUniqueName(t, "lane-priority-group")
	admin := newMeasureKafkaAdmin(t)
	t.Cleanup(func() { measureCleanup(t, admin, group, topic, shape) })
	for _, lane := range shape.lanes {
		measureCreateTopic(t, admin, ctx, measureLaneDestination(topic, lane.name), shape.partitionsPerLane)
	}

	members := measureMemberNames(shape.members)
	assignments := newMeasureAssignments()
	bookkeeping := newMeasureBookkeeping(2*measureBacklog, measureHandlerWork)
	clients := make([]*measureClient, 0, len(members))
	for _, member := range members {
		client, err := f1.New(ctx, measurePriorityConfig(member, shape),
			f1.WithDriver(&measureDriver{Driver: kafka.Driver{}, member: member, assignments: assignments}),
			f1.WithTopology(f1.TopologyDeclare),
			f1.WithPublishTopics(topic),
		)
		if err != nil {
			t.Fatalf("%s: New(%s): %v", label, member, err)
		}
		opened := &measureClient{client: client}
		clients = append(clients, opened)
		t.Cleanup(func() { opened.close(t) })
	}

	published := measurePublishBacklog(t, ctx, clients[0].client, topic, shape, bookkeeping)
	if len(published) != 2*measureBacklog {
		t.Fatalf("%s: published %d of %d messages", label, len(published), 2*measureBacklog)
	}

	runs := make([]chan error, 0, len(members))
	for index, member := range members {
		runner, err := clients[index].client.Subscribe(ctx, measureSubscription(group, topic, bookkeeping))
		if err != nil {
			t.Fatalf("%s: Subscribe(%s): %v", label, member, err)
		}
		done := make(chan error, 1)
		runs = append(runs, done)
		go func() { done <- runner.Run(ctx) }()
	}

	lanes := make([]string, 0, len(shape.lanes))
	for _, lane := range shape.lanes {
		lanes = append(lanes, measureLaneDestination(topic, lane.name))
	}
	windowStart, held := assignments.awaitSettled(lanes, shape.partitionsPerLane, members, measureAssignmentQuiet, measureAssignmentBound)
	void := ""
	if !held {
		void = "no member held every lane within the assignment bound"
	}
	// What the coordinator handed out at that instant. The delivery log says
	// which member delivered what, which cannot tell a member that was assigned
	// nothing from one that was assigned work it never got to.
	broker := measureBrokerAssignment(t, ctx, admin, group, topic, shape)
	drained := bookkeeping.awaitCompletion(measureCompletionBound)
	if !drained && void == "" {
		void = "the drain did not finish within the completion bound"
	}
	if void == "" {
		if late := assignments.lateEntries(windowStart, measureLateAssignment); len(late) > 0 {
			void = "rebalanced inside the window: " + late
		}
	}

	report := bookkeeping.report(windowStart)
	uncovered := assignments.uncovered(shape, windowStart)
	t.Logf("%s window=%dms preWindow=%d zeroPairs=%d assignment=%s broker=%s highWaitP50=%dms highWaitP99=%dms highDrain=%dms lowDrain=%dms settle=%dms end=%dms highDone=%d lowDone=%d perSecond=%.0f unmatched=%d void=%s",
		label, report.window.Milliseconds(), report.preWindow, len(uncovered), assignments.line(shape, windowStart), broker,
		report.highP50.Milliseconds(), report.highP99.Milliseconds(),
		report.highDrain.Milliseconds(), report.lowDrain.Milliseconds(), report.settle.Milliseconds(), report.end.Milliseconds(),
		report.highDone, report.lowDone, report.perSecond, report.unmatched, voidOrNone(void))
	// Every lane of this shape carries more partitions than there are members,
	// so every member is entitled to one of each. A member that holds none of a
	// lane is a member whose handlers never see that priority, which is the
	// property a lane exists to buy and the one thing this run asserts about the
	// assignment rather than printing.
	if len(uncovered) > 0 {
		t.Errorf("%s: %d (member, lane) pairs hold no partition at all: %v; every lane here has %d partitions and there are %d members, so each member is entitled to a partition of each lane",
			label, len(uncovered), uncovered, shape.partitionsPerLane, shape.members)
	}

	for _, opened := range clients {
		opened.close(t)
	}
	for _, done := range runs {
		select {
		case <-done:
		case <-time.After(portCloseTimeout): //nolint:forbidigo // a bounded wait for a runner to stop after Close
			t.Errorf("%s: Runner.Run did not stop after Close", label)
		}
	}
}

// measureBrokerAssignment reads the group's assignment from the coordinator,
// keyed by each member's instance identity, which is the label this file gave
// it. It is the broker's own answer, so a member that was handed nothing is
// reported as a member with no partitions rather than inferred from silence.
func measureBrokerAssignment(t *testing.T, ctx context.Context, admin *kadm.Client, group, topic string, shape measureShape) string {
	t.Helper()
	described, err := admin.DescribeGroups(ctx, group)
	if err != nil {
		return "unreadable: " + err.Error()
	}
	line := ""
	for _, member := range described[group].Members {
		label := member.MemberID
		if member.InstanceID != nil && *member.InstanceID != "" {
			label = *member.InstanceID
		}
		assignment, ok := member.Assigned.AsConsumer()
		if !ok {
			line += label + "{no-consumer-assignment}"
			continue
		}
		assigned := make(map[string][]int32, len(assignment.Topics))
		for _, topicAssignment := range assignment.Topics {
			assigned[topicAssignment.Topic] = topicAssignment.Partitions
		}
		line += label + "{"
		for _, lane := range shape.lanes {
			partitions := assigned[measureLaneDestination(topic, lane.name)]
			if len(partitions) == 0 {
				continue
			}
			line += fmt.Sprintf("%s%v", lane.name, partitions)
		}
		line += "}"
	}
	return line
}

// voidOrNone spells an empty void reason as none, so a printed line always says
// whether the run counted.
func voidOrNone(void string) string {
	if void == "" {
		return "none"
	}
	return void
}

// measureClient is one member's client with a once-only close, so a run's
// failure path and its success path cannot both close it.
type measureClient struct {
	client *f1.Client
	closed bool
}

func (c *measureClient) close(t *testing.T) {
	t.Helper()
	if c.closed {
		return
	}
	c.closed = true
	ctx, cancel := context.WithTimeout(context.Background(), portCloseTimeout)
	defer cancel()
	if err := c.client.Close(ctx); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// measureLaneDestination is the Kafka topic a lane of one run declares: the
// entry point the core derives for the topic and priority, which is
// publishEntryPoint's "f1.<env>.<topic>.<priority>".
func measureLaneDestination(topic, lane string) string {
	return fmt.Sprintf("f1.test.%s.%s", topic, lane)
}

// measurePriorityConfig is one member's client configuration: the same
// subscription and the same broker, with an instance identity of its own, since
// the kafka driver joins a classic group as a static member by default.
func measurePriorityConfig(member string, shape measureShape) f1.Config {
	priorities := make([]f1.Priority, 0, len(shape.lanes))
	for _, lane := range shape.lanes {
		priorities = append(priorities, lane.priority)
	}
	return f1.Config{
		Env:        "test",
		Service:    "lane-priority",
		InstanceID: member,
		Broker: f1.BrokerConfig{
			Driver:          "kafka",
			Endpoints:       []string{portEndpoint()},
			ConnectTimeout:  10 * time.Second,
			DefaultPrefetch: 64,
			DriverOptions:   map[string]string{"kafka.balancer": measureBalancer},
		},
		Topology: f1.TopologyConfig{Priorities: priorities},
		Codec: f1.CodecConfig{
			Default:        "json",
			MaxHeaderBytes: f1.CoreMaxHeaderBytes,
			MaxBodyBytes:   1 << 20,
		},
		Lifecycle: f1.LifecycleConfig{
			DrainTimeout:          30 * time.Second,
			HandlerGrace:          2 * time.Second,
			CloseTimeout:          10 * time.Second,
			RebalanceDrainTimeout: 5 * time.Second,
		},
	}
}

// measureSubscription is the subscription every member of a run joins: one
// destination per lane, two workers, and a handler that records what the
// delivery cost.
func measureSubscription(group, topic string, bookkeeping *measureBookkeeping) f1.Subscription {
	return f1.Subscription{
		Name:           group,
		Topics:         []string{topic},
		Concurrency:    measureConcurrency,
		Prefetch:       2 * measureConcurrency * 2,
		Priorities:     []f1.Priority{f1.PriorityHigh, f1.PriorityLow},
		Retry:          f1.RetryConfig{MaxAttempts: 1},
		HandlerTimeout: 10 * time.Second,
		Handlers: map[string]f1.Handler{
			measureEventType: f1.HandlerFunc(func(_ context.Context, event *f1.Event) error {
				bookkeeping.handle(event.Priority().String(), event.ID())
				return nil
			}),
		},
	}
}

// measurePublishBacklog publishes the whole backlog on every lane before any
// member starts, and returns the publish instant of each message by its ID.
func measurePublishBacklog(t *testing.T, ctx context.Context, client *f1.Client, topic string, shape measureShape, bookkeeping *measureBookkeeping) map[string]time.Time {
	t.Helper()
	published := make(map[string]time.Time, 2*measureBacklog)
	for _, lane := range shape.lanes {
		for base := 0; base < measureBacklog; base += measurePublishBatchSize {
			count := min(measurePublishBatchSize, measureBacklog-base)
			messages := make([]f1.Message, 0, count)
			for index := range count {
				messages = append(messages, f1.Message{
					EventType: measureEventType,
					Payload:   measurePayload{Lane: lane.name, Sequence: base + index},
					Opts:      []f1.PublishOption{f1.WithTopic(topic), f1.WithPriority(lane.priority)},
				})
			}
			at := time.Now() //nolint:forbidigo // the publish instant the wait is measured from
			result, err := client.Publisher().PublishBatch(ctx, messages)
			if err != nil {
				t.Fatalf("PublishBatch(%s, %d messages): %v", lane.name, count, err)
			}
			for _, message := range result.Results {
				if message.Err != nil {
					t.Fatalf("PublishBatch(%s): %v", lane.name, message.Err)
				}
				published[message.ID] = at
			}
		}
	}
	bookkeeping.setPublished(published)
	return published
}

// measurePayload is what one message of the corpus carries. The handler reads
// the lane from the envelope, so this only has to be a payload the codec can
// encode; the sequence number makes a message identifiable in the broker.
type measurePayload struct {
	Lane     string `json:"lane"`
	Sequence int    `json:"sequence"`
}

// measureBookkeeping records what the handlers of one run observed: the wait
// from a message's publish to the start of its handler, and when each lane's
// last handler returned.
type measureBookkeeping struct {
	work        time.Duration
	want        int
	finished    chan struct{}
	done        sync.Once
	mu          sync.Mutex
	published   map[string]time.Time
	waits       map[string][]measureSample
	completed   map[string]int
	completedAt []time.Time
	last        map[string]time.Time
	overall     time.Time
	first       time.Time
	unmatched   int
}

// measureSample is one message's wait from its publish to the start of its
// handler, kept with that start so a report can take only the messages the
// measured window covers.
type measureSample struct {
	at   time.Time
	wait time.Duration
}

// measureReport is one run's numbers, relative to the window the measurement
// opened at.
type measureReport struct {
	window    time.Duration
	highP50   time.Duration
	highP99   time.Duration
	highDrain time.Duration
	lowDrain  time.Duration
	settle    time.Duration
	end       time.Duration
	highDone  int
	lowDone   int
	perSecond float64
	unmatched int
	preWindow int
}

func newMeasureBookkeeping(want int, work time.Duration) *measureBookkeeping {
	return &measureBookkeeping{
		work:      work,
		want:      want,
		finished:  make(chan struct{}),
		waits:     make(map[string][]measureSample),
		completed: make(map[string]int),
		last:      make(map[string]time.Time),
	}
}

func (b *measureBookkeeping) setPublished(published map[string]time.Time) {
	b.mu.Lock()
	b.published = published
	for _, at := range published {
		if b.first.IsZero() || at.Before(b.first) {
			b.first = at
		}
	}
	b.mu.Unlock()
}

// handle runs on the runner's worker: it measures the wait first, then spends
// the handler time, so the wait it records is publish to handler start.
func (b *measureBookkeeping) handle(lane, id string) {
	start := time.Now() //nolint:forbidigo // the handler start is the far end of the wait
	b.mu.Lock()
	at, known := b.published[id]
	if !known {
		b.unmatched++
	}
	b.mu.Unlock()
	if known {
		b.mu.Lock()
		b.waits[lane] = append(b.waits[lane], measureSample{at: start, wait: start.Sub(at)})
		b.mu.Unlock()
	}
	time.Sleep(b.work)     //nolint:forbidigo // the handler cost is real work, which is what makes slots scarce
	finished := time.Now() //nolint:forbidigo // the handler return is what the drain is measured to
	b.mu.Lock()
	b.completed[lane]++
	b.completedAt = append(b.completedAt, finished)
	b.last[lane] = finished
	b.overall = finished
	total := 0
	for _, count := range b.completed {
		total += count
	}
	b.mu.Unlock()
	if total >= b.want {
		b.done.Do(func() { close(b.finished) })
	}
}

// awaitCompletion blocks until every message of the corpus has been handled.
func (b *measureBookkeeping) awaitCompletion(bound time.Duration) bool {
	timer := time.NewTimer(bound) //nolint:forbidigo // a drain is a wall-clock quantity, so its bound is too
	defer timer.Stop()
	select {
	case <-b.finished:
		return true
	case <-timer.C:
		return false
	}
}

// report computes one run's numbers. A wait is measured from its message's
// publish to the start of its handler, and a lane's drain from the measured
// window to that lane's last handler return.
func (b *measureBookkeeping) report(windowStart time.Time) measureReport {
	b.mu.Lock()
	defer b.mu.Unlock()
	report := measureReport{
		highDone:  b.completed["high"],
		lowDone:   b.completed["low"],
		unmatched: b.unmatched,
	}
	// A window is a length of time, so on its own it cannot say where in the run
	// it sat. These two place it: when the assignment settled, counted from the
	// first publish, and when the last handler of the corpus returned. The same
	// remaining drain is cheaper for the arm that starts it later, so the settle
	// is what makes two arms comparable at all. A run whose window never opened
	// has no settle, and reports zero rather than a distance from the zero
	// instant.
	if !b.first.IsZero() {
		if !windowStart.IsZero() {
			report.settle = windowStart.Sub(b.first)
		}
		report.end = b.overall.Sub(b.first)
	}
	for _, at := range b.completedAt {
		if at.Before(windowStart) {
			report.preWindow++
		}
	}
	report.highP50 = measurePercentile(measureWindowed(b.waits["high"], windowStart), 0.50)
	report.highP99 = measurePercentile(measureWindowed(b.waits["high"], windowStart), 0.99)
	// A window that never opened has nothing to be relative to: the run is
	// void, and a drain measured from the zero instant would be hours.
	if windowStart.IsZero() {
		return report
	}
	if last, ok := b.last["high"]; ok {
		report.highDrain = last.Sub(windowStart)
	}
	if last, ok := b.last["low"]; ok {
		report.lowDrain = last.Sub(windowStart)
	}
	if !b.overall.IsZero() && b.overall.After(windowStart) {
		report.window = b.overall.Sub(windowStart)
		if report.window > 0 {
			report.perSecond = float64(report.highDone+report.lowDone) / report.window.Seconds()
		}
	}
	return report
}

// measureWindowed keeps the waits of the messages whose handlers started inside
// the measured window. A message handled before it was handled while the group
// was still forming, which is what the pre-window count reports separately.
func measureWindowed(samples []measureSample, windowStart time.Time) []time.Duration {
	waits := make([]time.Duration, 0, len(samples))
	for _, sample := range samples {
		if !sample.at.Before(windowStart) {
			waits = append(waits, sample.wait)
		}
	}
	return waits
}

// measurePercentile returns the p-th percentile of the samples, nearest rank.
func measurePercentile(samples []time.Duration, p float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sorted := slices.Clone(samples)
	slices.Sort(sorted)
	index := int(p * float64(len(sorted)))
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

// measurePartition is one (destination, partition) pair.
type measurePartition struct {
	destination string
	partition   int32
}

// measureAssignments is the assignment log of one run: per member, the first
// instant each partition of each lane was delivered to it. A partition a member
// never delivered from is one it did not hold, which is the fact this file
// measures the priority lanes by.
type measureAssignments struct {
	mu      sync.Mutex
	seen    map[string]map[measurePartition]time.Time
	last    map[string]map[measurePartition]time.Time
	changes uint64
}

func newMeasureAssignments() *measureAssignments {
	return &measureAssignments{
		seen: make(map[string]map[measurePartition]time.Time),
		last: make(map[string]map[measurePartition]time.Time),
	}
}

func (a *measureAssignments) record(member, destination string, partition int32, at time.Time) {
	a.mu.Lock()
	held := a.seen[member]
	if held == nil {
		held = make(map[measurePartition]time.Time)
		a.seen[member] = held
	}
	latest := a.last[member]
	if latest == nil {
		latest = make(map[measurePartition]time.Time)
		a.last[member] = latest
	}
	key := measurePartition{destination: destination, partition: partition}
	if _, known := held[key]; !known {
		held[key] = at
		// Only a member delivering from a partition it has never delivered
		// from before is a change in the assignment: a further delivery from a
		// partition already in the log is the corpus draining, which is what
		// the measurement is for.
		a.changes++
	}
	latest[key] = at
	a.mu.Unlock()
}

// changeCount reports how many (member, partition) pairs the log has gained, so
// a wait for a settled assignment can tell whether it moved while the wait was
// not looking.
func (a *measureAssignments) changeCount() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.changes
}

// awaitSettled blocks until the assignment has stopped changing and every
// partition of every lane is being delivered from by some member, and returns
// the instant it decided so.
//
// It waits on the assignment log rather than on a plan because a member that
// holds nothing of a lane is exactly what this file measures: the condition is
// that every partition has an owner and that every member has one, not that
// every member has a lane. A member arriving changes which (member, partition)
// pairs exist, so quiet means no member has delivered from a partition it had
// not delivered from before, which is only true once a rebalance has stopped
// moving partitions.
//
// A change in the log is what it waits for and nothing publishes an event for
// it, so it re-checks on a fixed interval that is short against the run and
// bounded by bound.
func (a *measureAssignments) awaitSettled(destinations []string, partitions int32, members []string, quiet, bound time.Duration) (time.Time, bool) {
	deadline := time.NewTimer(bound) //nolint:forbidigo // the settle bound is a wall-clock bound
	defer deadline.Stop()
	probe := time.NewTicker(quiet) //nolint:forbidigo // quiet is a wall-clock interval with no event to wait on
	defer probe.Stop()
	armed := a.changeCount()
	for {
		select {
		case <-probe.C:
			if current := a.changeCount(); current != armed || !a.covered(destinations, partitions, members) {
				armed = current
				continue
			}
			return time.Now(), true //nolint:forbidigo // the settle instant is a wall-clock instant
		case <-deadline.C:
			return time.Time{}, false
		}
	}
}

func (a *measureAssignments) covered(destinations []string, partitions int32, members []string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, member := range members {
		if len(a.seen[member]) == 0 {
			return false
		}
	}
	for _, destination := range destinations {
		for partition := range partitions {
			found := false
			for _, held := range a.seen {
				if _, ok := held[measurePartition{destination: destination, partition: partition}]; ok {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}

// lateEntries reports every partition first delivered after the window opened,
// with the slack the caller allows. It names the members and partitions,
// because a rebalance inside the window is what makes a run's numbers a
// measurement of a rebalance rather than of a lane.
func (a *measureAssignments) lateEntries(windowStart time.Time, slack time.Duration) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var late []string
	for member, held := range a.seen {
		for key, at := range held {
			if at.After(windowStart.Add(slack)) {
				late = append(late, fmt.Sprintf("%s+%s/%d", member, key.destination, key.partition))
			}
		}
	}
	slices.Sort(late)
	if len(late) == 0 {
		return ""
	}
	return fmt.Sprintf("%v", late)
}

// line renders the assignment the measured window saw the way the plan
// comparison prints one: member, lane, and the partitions it delivered from
// while the window was open. A partition a member only held before the window
// was one it gave up on the way in, so reading the window is what makes this
// line the assignment rather than the history of two of them.
func (a *measureAssignments) line(shape measureShape, windowStart time.Time) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	line := ""
	for _, member := range measureMemberNames(shape.members) {
		line += member + "{"
		for _, lane := range shape.lanes {
			partitions := a.partitionsLocked(member, lane.name, windowStart)
			if len(partitions) == 0 {
				continue
			}
			line += fmt.Sprintf("%s%v", lane.name, partitions)
		}
		line += "}"
	}
	return line
}

// uncovered lists the (member, lane) pairs a run left with no partition at all,
// as "member/lane", which is the coverage figure the plan comparison reports and
// the one fact a run on a well-provisioned shape asserts.
func (a *measureAssignments) uncovered(shape measureShape, windowStart time.Time) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var pairs []string
	for _, member := range measureMemberNames(shape.members) {
		for _, lane := range shape.lanes {
			if len(a.partitionsLocked(member, lane.name, windowStart)) == 0 {
				pairs = append(pairs, member+"/"+lane.name)
			}
		}
	}
	return pairs
}

// partitionsLocked lists the partitions of one lane a member delivered from at
// or after windowStart.
func (a *measureAssignments) partitionsLocked(member, lane string, windowStart time.Time) []int32 {
	suffix := "." + lane
	partitions := make([]int32, 0, 4)
	for key, at := range a.last[member] {
		if strings.HasSuffix(key.destination, suffix) && !at.Before(windowStart) {
			partitions = append(partitions, key.partition)
		}
	}
	slices.Sort(partitions)
	return partitions
}

// measureMemberNames names the members of a run.
func measureMemberNames(count int) []string {
	names := make([]string, 0, count)
	for index := range count {
		names = append(names, fmt.Sprintf("member-%d", index))
	}
	return names
}

// measureDriver wraps the Kafka driver and tees each consumer's deliveries into
// the run's assignment log. The core does not put the broker partition on the
// message a handler receives, so a run that measured the lanes from handler
// calls alone could not say which partitions a member held, only that it saw
// work.
type measureDriver struct {
	driver.Driver
	member      string
	assignments *measureAssignments
}

func (d *measureDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	opened, err := (kafka.Driver{}).Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &measureConn{Conn: opened, member: d.member, assignments: d.assignments}, nil
}

type measureConn struct {
	driver.Conn
	member      string
	assignments *measureAssignments
}

func (c *measureConn) Consumer(ctx context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	consumer, err := c.Conn.Consumer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	tee := &measureConsumer{Consumer: consumer, member: c.member, assignments: c.assignments, messages: make(chan driver.InboundMessage, cfg.Prefetch)}
	go tee.forward()
	return tee, nil
}

type measureConsumer struct {
	driver.Consumer
	member      string
	assignments *measureAssignments
	messages    chan driver.InboundMessage
}

func (c *measureConsumer) Messages() <-chan driver.InboundMessage { return c.messages }

// forward copies every delivery into the log and then on to the core, so the
// wrapper changes when a delivery is observed and nothing else.
func (c *measureConsumer) forward() {
	defer close(c.messages)
	for message := range c.Consumer.Messages() {
		c.assignments.record(c.member, message.Destination, message.Ref.Partition, time.Now()) //nolint:forbidigo // a delivery is logged at the wall-clock instant it happens
		c.messages <- message
	}
}

// newMeasureKafkaAdmin opens the broker client this file uses for the fixture
// facts the port does not express: creating a destination with a known
// partition count, and deleting the destination and group a run owns.
func newMeasureKafkaAdmin(t *testing.T) *kadm.Client {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(portEndpoint()))
	if err != nil {
		t.Fatalf("kgo.NewClient(%s): %v", portEndpoint(), err)
	}
	t.Cleanup(client.Close)
	return kadm.NewClient(client)
}

func measureCreateTopic(t *testing.T, admin *kadm.Client, ctx context.Context, name string, partitions int32) {
	t.Helper()
	responses, err := admin.CreateTopics(ctx, partitions, 1, nil, name)
	if err != nil {
		t.Fatalf("CreateTopics %q: %v", name, err)
	}
	if response, ok := responses[name]; ok && response.Err != nil && !errors.Is(response.Err, kerr.TopicAlreadyExists) {
		t.Fatalf("CreateTopics %q: %v", name, response.Err)
	}
	// Creation returns before every partition has a leader; a publish in that
	// window fails with UNKNOWN_TOPIC_OR_PARTITION.
	destination := &portFixture{ctx: ctx, clock: clock.NewReal(), admin: admin, topic: name, partitions: int(partitions)}
	destination.awaitDestination(t) //nolint:contextcheck // the fixture carries ctx
}

// measureCleanup deletes what one priority run created: the group, then every
// lane topic and the two auxiliary destinations the core declares for a
// subscription. All of them are bounded, and a topic a run never got as far as
// creating is the state the cleanup wanted.
func measureCleanup(t *testing.T, admin *kadm.Client, group, topic string, shape measureShape) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), portCloseTimeout)
	defer cancel()
	if responses, err := admin.DeleteGroups(ctx, group); err != nil {
		t.Errorf("DeleteGroups %q: %v", group, err)
	} else if response, ok := responses[group]; ok && response.Err != nil && !errors.Is(response.Err, kerr.GroupIDNotFound) {
		t.Errorf("DeleteGroups %q: %v", group, response.Err)
	}
	names := make([]string, 0, len(shape.lanes)+2)
	for _, lane := range shape.lanes {
		names = append(names, measureLaneDestination(topic, lane.name))
	}
	names = append(names, fmt.Sprintf("f1.test.%s.dlq.%s", topic, group), fmt.Sprintf("f1.test.unknown.dlq.%s", group))
	responses, err := admin.DeleteTopics(ctx, names...)
	if err != nil {
		t.Errorf("DeleteTopics %v: %v", names, err)
		return
	}
	for _, name := range names {
		if response, ok := responses[name]; ok && response.Err != nil && !errors.Is(response.Err, kerr.UnknownTopicOrPartition) {
			t.Errorf("DeleteTopics %q: %v", name, response.Err)
		}
	}
}

// measureMoveGateEnv opts in to TestBalancerMoveCost. The test only prints its
// readings and asserts no bound on them, so it is a measurement rather than a
// gate: without the variable it skips, and make test-infra does not pay three
// runs on the shared fixture for numbers nothing checks.
const measureMoveGateEnv = "F1_KAFKA_MEASURE_MOVE"

// TestBalancerMoveCost measures what a join costs the group: how long after the
// joining member's assignment its first delivery arrives, and how many records
// the group delivered twice while the membership changed.
func TestBalancerMoveCost(t *testing.T) {
	if os.Getenv(measureMoveGateEnv) != "1" {
		t.Skipf("balancer move cost: set %s=1, or run make measure-kafka-move, to perform the measurement", measureMoveGateEnv)
	}
	requirePortBroker(t)
	for run := 1; run <= measureRuns; run++ {
		measureMoveRun(t, run)
	}
}

// measureMoveRun is one move measurement, at the driver port.
//
// The first member takes the whole corpus and holds it unsettled; the second
// member joins and the corpus is released. Whether a delivery is refused
// because its ownership moved, and how many records come back, is then the
// driver's own account of the rebalance rather than the test's.
func measureMoveRun(t *testing.T, run int) {
	t.Helper()
	ctx := t.Context()
	clk := clock.NewReal()
	label := fmt.Sprintf("move balancer=%s run=%d", measureBalancer, run)

	topic := portUniqueName(t, "lane-move")
	group := portUniqueName(t, "lane-move-group")
	admin := newMeasureKafkaAdmin(t)
	t.Cleanup(func() { measureMoveCleanup(t, admin, group, topic) })
	measureCreateTopic(t, admin, ctx, topic, portPartitions)

	holderConnection := measureOpenConnection(t, ctx, "member-0")
	joinerConnection := measureOpenConnection(t, ctx, "member-1")
	producer, err := holderConnection.Producer(ctx, driver.ProducerConfig{Effective: holderConnection.Capabilities()})
	if err != nil {
		t.Fatalf("%s: Producer(): %v", label, err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), portCloseTimeout)
		defer cancel()
		if err := producer.Close(closeCtx); err != nil {
			t.Errorf("%s: Close producer: %v", label, err)
		}
	})

	ledger := newPortLedger(clk)
	holder := newMeasureHolder(ctx, clk, ledger)
	holderSubscriber := measureSubscribe(t, clk, holderConnection, group, topic)
	holderSubscriber.notifications.awaitAssignment(t, portAssignmentTimeout, label+": the first member's assignment")
	measurePublishCorpus(t, ctx, producer, topic)

	holderPump := holderSubscriber.pump(func(message driver.InboundMessage) {
		ledger.received(portSequenceOf(message))
		holder.take(message)
	})
	defer holderPump.stop()
	holder.awaitPartitions(t, portPartitions, label+": the first member holding every partition")

	joinerSubscriber := measureSubscribe(t, clk, joinerConnection, group, topic)
	assignedAt := joinerSubscriber.notifications.awaitAssignment(t, portAssignmentTimeout, label+": the joining member's assignment")
	first := portAwaitDelivery(t, clk, joinerSubscriber, portDeliveryTimeout, label+": the joining member's first delivery")
	firstDelivery := clk.Now().Sub(assignedAt)
	ledger.received(portSequenceOf(first))
	ledger.acknowledge(ctx, first)

	holder.release()
	joinerPump := joinerSubscriber.pump(func(message driver.InboundMessage) {
		ledger.received(portSequenceOf(message))
		ledger.acknowledge(ctx, message)
	})
	defer joinerPump.stop()

	sequences := measureMoveSequences()
	portAwaitCorpus(t, ledger, len(sequences), sequences, portSettleQuietWindow, portSettleTimeout, label+": the corpus after the join")
	ledger.assertNoFailures(t)
	ledger.metrics(t, label, sequences, firstDelivery)
}

// measureSubscribe opens one driver-port consumer on a connection and registers
// its release, which runs before the connection is closed.
func measureSubscribe(t *testing.T, clk clock.Clock, connection driver.Conn, group, topic string) *portSubscriber {
	t.Helper()
	consumer, err := connection.Consumer(t.Context(), driver.ConsumerConfig{
		Group:          group,
		Destinations:   []string{topic},
		Prefetch:       portPartitions,
		PerDestination: map[string]int{topic: portPartitions},
		StartAt:        driver.StartEarliest,
		Effective:      connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer(%q): %v", group, err)
	}
	t.Cleanup(func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), portCloseTimeout)
		defer cancel()
		if err := consumer.Release(releaseCtx); err != nil {
			t.Errorf("Release consumer %q: %v", group, err)
		}
	})
	return &portSubscriber{Consumer: consumer, notifications: watchRebalanceNotifications(clk, consumer)}
}

// measureOpenConnection opens one driver connection with the balancer under
// measurement and an instance identity of its own, since static membership is
// the kafka driver's default and two members sharing one identity evict each
// other instead of sharing the group.
func measureOpenConnection(t *testing.T, ctx context.Context, instance string) driver.Conn {
	t.Helper()
	connection, err := (kafka.Driver{}).Open(ctx, driver.Config{
		Endpoints:             []string{portEndpoint()},
		ClientID:              "f1-kafka-lane-move",
		InstanceID:            instance,
		RebalanceDrainTimeout: 2 * time.Second,
		DriverOptions:         map[string]string{"kafka.balancer": measureBalancer},
	})
	if err != nil {
		t.Fatalf("Open(%s, balancer=%s): %v", portEndpoint(), measureBalancer, err)
	}
	t.Cleanup(func() { //nolint:contextcheck // cleanup runs after the test context is cancelled, so the close needs a context of its own.
		closeCtx, cancel := context.WithTimeout(context.Background(), portCloseTimeout)
		defer cancel()
		if err := connection.Close(closeCtx); err != nil {
			t.Errorf("Close connection: %v", err)
		}
	})
	return connection
}

// measurePublishCorpus publishes the corpus the move runs share, one record per
// sequence with a key that puts it on a known partition.
func measurePublishCorpus(t *testing.T, ctx context.Context, producer driver.Producer, topic string) {
	t.Helper()
	sequences := measureMoveSequences()
	//nolint:contextcheck // the key probe owns its fixture context and takes none.
	keys := portKeyedSequences(sequences, portKeysByPartition(t))
	messages := make([]driver.OutboundMessage, 0, len(sequences))
	for index, sequence := range sequences {
		messages = append(messages, driver.OutboundMessage{Destination: topic, Body: []byte(sequence), Key: []byte(keys[index])})
	}
	if err := producer.Publish(ctx, messages...); err != nil {
		t.Fatalf("Publish(%d messages to %q): %v", len(messages), topic, err)
	}
}

// measureMoveSequences is the numbered corpus the move runs publish.
func measureMoveSequences() []string { return portSequences(measureMoveCorpus) }

// measureHolder keeps the first member's deliveries unsettled until the test
// releases them, and settles everything afterwards. Holding them is what gives
// the join a delivery to move: a member that had already settled everything
// would be handing over nothing.
type measureHolder struct {
	ctx    context.Context
	clk    clock.Clock
	ledger *portLedger

	mu       sync.Mutex
	held     []driver.InboundMessage
	released bool
	notify   chan struct{}
}

func newMeasureHolder(ctx context.Context, clk clock.Clock, ledger *portLedger) *measureHolder {
	return &measureHolder{ctx: ctx, clk: clk, ledger: ledger, notify: make(chan struct{}, 1)}
}

// take records one delivery. After release it settles the delivery instead of
// holding it, and it takes the delivery out of the held set under the same lock
// that release drains, so a delivery is settled exactly once.
func (h *measureHolder) take(message driver.InboundMessage) {
	h.mu.Lock()
	if h.released {
		h.mu.Unlock()
		h.ledger.acknowledge(h.ctx, message)
		return
	}
	h.held = append(h.held, message)
	h.mu.Unlock()
	select {
	case h.notify <- struct{}{}:
	default:
	}
}

// release settles every delivery taken so far and stops holding.
func (h *measureHolder) release() {
	h.mu.Lock()
	h.released = true
	held := h.held
	h.held = nil
	h.mu.Unlock()
	for _, message := range held {
		h.ledger.acknowledge(h.ctx, message)
	}
}

// awaitPartitions blocks until the first member has taken a delivery from count
// distinct partitions, which is it holding every partition of the destination.
func (h *measureHolder) awaitPartitions(t *testing.T, count int, what string) {
	t.Helper()
	timer := time.NewTimer(portAssignmentTimeout) //nolint:forbidigo // a broker assignment is a wall-clock wait
	defer timer.Stop()
	for {
		h.mu.Lock()
		partitions := make(map[int32]struct{}, len(h.held))
		for _, message := range h.held {
			partitions[message.Ref.Partition] = struct{}{}
		}
		h.mu.Unlock()
		if len(partitions) >= count {
			return
		}
		select {
		case <-h.notify:
		case <-timer.C:
			t.Fatalf("%s: only %d of %d partitions had a living delivery within %s", what, len(partitions), count, portAssignmentTimeout)
		}
	}
}

// measureMoveCleanup deletes the group and the destination a move run created.
func measureMoveCleanup(t *testing.T, admin *kadm.Client, group, topic string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), portCloseTimeout)
	defer cancel()
	if responses, err := admin.DeleteGroups(ctx, group); err != nil {
		t.Errorf("DeleteGroups %q: %v", group, err)
	} else if response, ok := responses[group]; ok && response.Err != nil && !errors.Is(response.Err, kerr.GroupIDNotFound) {
		t.Errorf("DeleteGroups %q: %v", group, response.Err)
	}
	responses, err := admin.DeleteTopics(ctx, topic)
	if err != nil {
		t.Errorf("DeleteTopics %q: %v", topic, err)
		return
	}
	if response, ok := responses[topic]; ok && response.Err != nil && !errors.Is(response.Err, kerr.UnknownTopicOrPartition) {
		t.Errorf("DeleteTopics %q: %v", topic, response.Err)
	}
}
