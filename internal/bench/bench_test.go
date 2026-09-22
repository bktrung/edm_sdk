package bench

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// stubDriver is a driver value nothing here opens. A test that asks the
// harness what it would subscribe with needs a value of the right type and
// nothing behind it: the harness reaches the driver only from a measurement,
// and these tests do not run one.
type stubDriver struct{}

func (stubDriver) Name() string { return "stub" }

func (stubDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }

func (stubDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return nil, errors.New("bench: stub driver does not open")
}

// TestNewRefusesNegativePrefetch proves the harness refuses an in-flight budget
// that cannot mean anything. A negative prefetch is not a shape any
// subscription can have, so a caller who passes one has a defect, and
// measuring a budget the caller did not configure would put a wrong shape
// beside a real number.
func TestNewRefusesNegativePrefetch(t *testing.T) {
	t.Parallel()
	_, err := New(stubDriver{}, Config{
		Namespace:  "bench-prefetch-negative",
		Messages:   1,
		Publishers: 1,
		Prefetch:   -1,
		Timeout:    time.Second,
	})
	if err == nil {
		t.Fatal("bench.New accepted a negative prefetch")
	}
}

// TestSubscriptionCarriesConfiguredPrefetch proves the setting reaches the
// subscription the harness builds, which is the only place it can act: the
// core resolves the subscription's own budget, caps it by the subscription's
// lane capacities, and hands the driver what it resolved.
//
// The zero case is the other half of the same claim. A harness that passed the
// value the package ships with would keep measuring that value after it
// changed, so a measurement that configures nothing has to pass nothing.
func TestSubscriptionCarriesConfiguredPrefetch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		prefetch int
		want     int
	}{
		{name: "configured", prefetch: 100, want: 100},
		{name: "unset keeps the shipped default", prefetch: 0, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			harness, err := New(stubDriver{}, Config{
				Namespace:   "bench-prefetch-carry",
				Messages:    1,
				Publishers:  1,
				Concurrency: 16,
				Prefetch:    tc.prefetch,
				Timeout:     time.Second,
			})
			if err != nil {
				t.Fatalf("bench.New: %v", err)
			}
			sub := (&run{h: harness, m: modeConsume}).subscription()
			if sub.Prefetch != tc.want {
				t.Fatalf("subscription prefetch = %d, want %d", sub.Prefetch, tc.want)
			}
		})
	}
}

// TestBacklogGateHoldsDeliveriesOutOfTheCore proves the gate a backlog
// measurement publishes its corpus behind: while the run holds its deliveries
// back, no delivery reaches the core, and when the run opens the gate every
// held delivery does.
//
// It is what makes a backlog measurement measure the drain rather than the
// load. A window that opened with deliveries already inside the core would
// contain settlements of a corpus it did not publish, and its rate would be the
// publisher's again, which is the failure this test exists to catch.
//
// The consumer comes from the connection wrapper a measurement builds its own
// consumer through, so the hold and the gate the run sets on its driver are the
// ones the pump reads: a wrapper that dropped either field would fail here
// rather than under a benchmark, where a gate that never shut reads as a
// plausible number.
func TestBacklogGateHoldsDeliveriesOutOfTheCore(t *testing.T) {
	t.Parallel()
	// What the pump did is observed inside the bubble and judged after it: a
	// failure inside would leave the pump blocked on a send nobody reads, and
	// synctest reports that deadlock beside a result that already said what was
	// wrong.
	var handedOverWhileHeld, handedOverAfterRelease bool
	synctest.Test(t, func(t *testing.T) {
		var hold atomic.Bool
		gate := make(chan struct{})
		messages := make(chan driver.InboundMessage)
		conn := &countingConn{
			Conn: stubConn{consumer: &stubConsumer{messages: messages}},
			tr:   newTracker(clock.NewReal(), 1, 1),
			hold: &hold,
			gate: gate,
		}
		consumer, err := conn.Consumer(context.Background(), driver.ConsumerConfig{})
		if err != nil {
			t.Fatalf("counting connection handed no consumer: %v", err)
		}
		hold.Store(true)
		messages <- driver.InboundMessage{Body: []byte(corpusBody(0))}
		synctest.Wait()
		select {
		case <-consumer.Messages():
			handedOverWhileHeld = true
		default:
		}
		close(gate)
		synctest.Wait()
		select {
		case <-consumer.Messages():
			handedOverAfterRelease = true
		default:
		}
		close(messages)
		if err := consumer.Stop(context.Background()); err != nil {
			t.Errorf("stopping the wrapped consumer: %v", err)
		}
	})
	if handedOverWhileHeld {
		t.Fatal("the pump handed a delivery to the core while the run was holding its deliveries back")
	}
	if !handedOverAfterRelease {
		t.Fatal("the pump did not hand the held delivery over after the gate opened")
	}
}

// stubConn hands out the consumer a test put behind it, so a test can take one
// from the same wrapper chain a measurement uses. Every other method is the
// embedded interface's, and nothing here opens a connection.
type stubConn struct {
	driver.Conn
	consumer driver.Consumer
}

func (c stubConn) Consumer(context.Context, driver.ConsumerConfig) (driver.Consumer, error) {
	return c.consumer, nil
}

// stubConsumer is a consumer the gate test drives: it yields the deliveries a
// test puts on its channel and answers nothing else. The methods the pump does
// not call are the embedded interface's.
type stubConsumer struct {
	driver.Consumer
	messages chan driver.InboundMessage
}

func (c *stubConsumer) Messages() <-chan driver.InboundMessage { return c.messages }

func (c *stubConsumer) Stop(context.Context) error { return nil }

func (c *stubConsumer) Release(context.Context) error { return nil }

// stubSettler answers the settlement calls a test asks it about and nothing
// else: Ack reports the error the test put on it, and Nack is the embedded
// interface's business.
type stubSettler struct {
	err error
}

func (s stubSettler) Ack(context.Context) error { return s.err }

func (s stubSettler) Nack(context.Context, driver.NackOptions) error { return nil }

// TestFailedAckIsNotASettlement proves the harness counts a delivery as settled
// only when its Ack returns nil, which is the reason it wraps the driver at the
// port instead of counting deliveries where they arrive: a rate counted a step
// early is a bigger number about a different thing, and a settlement the broker
// refused is not throughput the consume path reached.
//
// The pair is the test. An accounting that never counts passes the refusing
// half on its own, so one accepted delivery settles beside the refused one and
// the window is asked for exactly one settlement.
func TestFailedAckIsNotASettlement(t *testing.T) {
	t.Parallel()
	// The counts and the samples are read inside the bubble, where the tracker
	// lives, and asserted after it: a failure inside would leave the pump
	// blocked on a channel nobody reads, and synctest reports that as a deadlock
	// beside a result that already said what was wrong. The samples matter
	// because a refused ack that left one behind would move the p50 and p99 the
	// sweep prints.
	var afterRefused, afterAccepted int
	var samplesRefused, samplesAccepted int
	synctest.Test(t, func(t *testing.T) {
		messages := make(chan driver.InboundMessage)
		tr := newTracker(clock.NewReal(), 2, 1)
		consumer := &countingConsumer{
			Consumer: &stubConsumer{messages: messages},
			tr:       tr,
			out:      make(chan driver.InboundMessage),
			stopped:  make(chan struct{}),
			drained:  make(chan struct{}),
		}
		go consumer.pump()
		tr.arm()
		messages <- driver.InboundMessage{
			Body:   []byte(corpusBody(0)),
			Settle: stubSettler{err: errors.New("bench: the broker refused the ack")},
		}
		synctest.Wait()
		refused := <-consumer.Messages()
		if err := refused.Settle.Ack(context.Background()); err == nil {
			t.Error("the refused ack reported no error")
		}
		afterRefused = tr.settledTotal()
		settleLatencies, _ := tr.settledLatencies()
		samplesRefused = len(settleLatencies)
		messages <- driver.InboundMessage{Body: []byte(corpusBody(1)), Settle: stubSettler{}}
		synctest.Wait()
		accepted := <-consumer.Messages()
		if err := accepted.Settle.Ack(context.Background()); err != nil {
			t.Errorf("the accepted ack reported %v", err)
		}
		afterAccepted = tr.settledTotal()
		settleLatencies, _ = tr.settledLatencies()
		samplesAccepted = len(settleLatencies)
		close(messages)
		<-consumer.drained
	})
	if afterRefused != 0 {
		t.Fatalf("settlements after a refused ack = %d, want 0", afterRefused)
	}
	if samplesRefused != 0 {
		t.Fatalf("settle samples after a refused ack = %d, want 0", samplesRefused)
	}
	if afterAccepted != 1 {
		t.Fatalf("settlements after an accepted ack = %d, want 1", afterAccepted)
	}
	if samplesAccepted != 1 {
		t.Fatalf("settle samples after an accepted ack = %d, want 1", samplesAccepted)
	}
}

// recordingDriver records the connection config the client opened it with and
// returns an injected connection when one is set; otherwise it refuses to open.
type recordingDriver struct {
	name string
	conn driver.Conn

	mu     sync.Mutex
	opened []driver.Config
}

func (d *recordingDriver) Name() string { return d.name }

func (d *recordingDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }

func (d *recordingDriver) Open(_ context.Context, cfg driver.Config) (driver.Conn, error) {
	d.mu.Lock()
	d.opened = append(d.opened, cfg)
	d.mu.Unlock()
	if d.conn != nil {
		return d.conn, nil
	}
	return nil, errors.New("bench: recording driver does not open")
}

// lastOpened returns the last connection config the client handed this driver.
func (d *recordingDriver) lastOpened() (driver.Config, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.opened) == 0 {
		return driver.Config{}, false
	}
	return d.opened[len(d.opened)-1], true
}

type cleanupConn struct {
	driver.Conn
	admin driver.Admin
}

func (c cleanupConn) Admin() driver.Admin { return c.admin }

func (c cleanupConn) Close(context.Context) error { return nil }

type cleanupAdmin struct {
	driver.Admin

	mu     sync.Mutex
	purges []string
	prunes [][]string
	refuse map[string]bool
}

func (a *cleanupAdmin) EnsureTopology(context.Context, driver.TopologySpec) (driver.TopologyDiff, error) {
	return driver.TopologyDiff{}, nil
}

func (a *cleanupAdmin) Purge(_ context.Context, name string) (int64, error) {
	a.mu.Lock()
	a.purges = append(a.purges, name)
	a.mu.Unlock()
	return 0, nil
}

func (a *cleanupAdmin) Prune(_ context.Context, names []string) ([]driver.PruneResult, error) {
	a.mu.Lock()
	a.prunes = append(a.prunes, append([]string(nil), names...))
	a.mu.Unlock()
	results := make([]driver.PruneResult, len(names))
	for index, name := range names {
		results[index] = driver.PruneResult{Name: name, Deleted: !a.refuse[name]}
	}
	return results, nil
}

func (a *cleanupAdmin) calls() (purges []string, prunes [][]string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	purges = append([]string(nil), a.purges...)
	prunes = make([][]string, len(a.prunes))
	for index := range a.prunes {
		prunes[index] = append([]string(nil), a.prunes[index]...)
	}
	return purges, prunes
}

type immediateCleanupClock struct{}

func (immediateCleanupClock) Now() time.Time { return time.Time{} }

func (immediateCleanupClock) Since(time.Time) time.Duration { return 0 }

func (immediateCleanupClock) Timer(time.Duration) clock.Timer { return clock.Timer{} }

func (immediateCleanupClock) Ticker(time.Duration) clock.Ticker { return clock.Ticker{} }

func (immediateCleanupClock) Sleep(context.Context, time.Duration) error { return nil }

func newCleanupHarness(t *testing.T, refuse map[string]bool) (*Harness, *cleanupAdmin) {
	t.Helper()
	admin := &cleanupAdmin{refuse: refuse}
	driverValue := &recordingDriver{
		name: "rabbitmq",
		conn: cleanupConn{admin: admin},
	}
	harness, err := New(driverValue, Config{
		Namespace:  "bench-cleanup-test",
		Messages:   1,
		Publishers: 1,
		Timeout:    time.Second,
	})
	if err != nil {
		t.Fatalf("bench.New: %v", err)
	}
	harness.clk = immediateCleanupClock{}
	wrapped := &countingAdmin{Admin: admin, onSpec: harness.noteDeclared}
	_, err = wrapped.EnsureTopology(context.Background(), driver.TopologySpec{
		Exchanges: []driver.ExchangeSpec{
			{Name: "f1.bench.exchange"},
			{Name: "outside.exchange"},
		},
		Destinations: []driver.DestinationSpec{
			{Name: "f1.bench.queue"},
			{Name: "outside.queue"},
		},
	})
	if err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	return harness, admin
}

// TestHarnessCloseRoutesDeclaredNames proves exchanges bypass Purge, destinations
// are purged before Prune, names outside the benchmark namespace are ignored,
// and an undeletable exchange is reported without failing Close while an
// undeletable destination remains an error.
func TestHarnessCloseRoutesDeclaredNames(t *testing.T) {
	const (
		exchange    = "f1.bench.exchange"
		destination = "f1.bench.queue"
	)

	t.Run("deletes both declared names", func(t *testing.T) {
		harness, admin := newCleanupHarness(t, nil)
		if err := harness.Close(context.Background()); err != nil {
			t.Fatalf("Harness.Close: %v", err)
		}
		purges, prunes := admin.calls()
		if want := []string{destination}; !slices.Equal(purges, want) {
			t.Fatalf("Purge names = %v, want %v", purges, want)
		}
		if len(prunes) != 1 {
			t.Fatalf("Prune calls = %d, want 1", len(prunes))
		}
		if want := []string{destination, exchange}; !slices.Equal(prunes[0], want) {
			t.Fatalf("Prune names = %v, want %v", prunes[0], want)
		}
	})

	t.Run("exchange refusal is reported", func(t *testing.T) {
		harness, _ := newCleanupHarness(t, map[string]bool{exchange: true})
		if err := harness.Close(context.Background()); err != nil {
			t.Fatalf("Harness.Close: %v, want nil for an undeletable exchange", err)
		}
	})

	t.Run("destination refusal is an error", func(t *testing.T) {
		harness, _ := newCleanupHarness(t, map[string]bool{destination: true})
		err := harness.Close(context.Background())
		if err == nil || !strings.Contains(err.Error(), destination) {
			t.Fatalf("Harness.Close error = %v, want an error naming %q", err, destination)
		}
	})
}

// TestQueueTypeReachesTheDriver proves the queue kind a measurement configures
// reaches the driver the way a deployment's own setting does: as the driver's
// documented option in the connection config the client opens it with. The
// harness does not interpret the value, so a run of one kind asks the broker
// for that kind instead of measuring whatever the driver defaults to.
//
// The zero case is the other half of the same claim. A harness that invented a
// kind, or passed one it was not given, would measure a shape no caller asked
// for, which is the failure a cell's queue type is read against.
func TestQueueTypeReachesTheDriver(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		queueType string
		want      string
	}{
		{name: "configured", queueType: "classic", want: "classic"},
		{name: "unset passes none", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			drv := &recordingDriver{name: "rabbitmq"}
			harness, err := New(drv, Config{
				Namespace:  "bench-queue-type",
				Endpoint:   "amqp://localhost:1/",
				Messages:   1,
				Publishers: 1,
				QueueType:  tc.queueType,
				Timeout:    time.Second,
			})
			if err != nil {
				t.Fatalf("bench.New: %v", err)
			}
			// The measurement reaches the driver and ends on its refusal, which
			// is what makes this the connection the client built rather than a
			// configuration a test restated.
			if _, err := harness.measure(context.Background(), modeConsume); err == nil {
				t.Fatal("a measurement over a driver that does not open reported no error")
			}
			opened, ok := drv.lastOpened()
			if !ok {
				t.Fatal("the client never opened the driver")
			}
			if got := opened.DriverOptions["rabbitmq.queueType"]; got != tc.want {
				t.Fatalf("driver option rabbitmq.queueType = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestSubscriptionCarriesConfiguredPriorities proves how many destinations a
// measurement opens, which is what the priority set decides: the core derives
// one destination per topic and priority, and the client config a measurement
// runs on declares the same set, so the destinations it consumes are the ones
// its topology covers.
//
// The zero case is the other half of the same claim, for the reason the
// prefetch test gives: a harness that named a set of its own would keep
// measuring it after the package's own default changed.
func TestSubscriptionCarriesConfiguredPriorities(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		priorities []f1.Priority
		want       []f1.Priority
	}{
		{
			name:       "configured",
			priorities: []f1.Priority{f1.PriorityHigh, f1.PriorityLow},
			want:       []f1.Priority{f1.PriorityHigh, f1.PriorityLow},
		},
		{name: "unset keeps one lane", want: []f1.Priority{lane}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			harness, err := New(stubDriver{}, Config{
				Namespace:  "bench-priorities",
				Messages:   1,
				Publishers: 1,
				Priorities: tc.priorities,
				Timeout:    time.Second,
			})
			if err != nil {
				t.Fatalf("bench.New: %v", err)
			}
			sub := (&run{h: harness, m: modeConsume}).subscription()
			if !slices.Equal(sub.Priorities, tc.want) {
				t.Fatalf("subscription priorities = %v, want %v", sub.Priorities, tc.want)
			}
			if got := harness.clientConfig().Topology.Priorities; !slices.Equal(got, tc.want) {
				t.Fatalf("declared topology priorities = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestNewRefusesDuplicatePriority proves the harness refuses a priority set
// that would ask for one destination twice: the core derives a destination per
// entry, and every adapter refuses a duplicate destination, so the shape is a
// defect wherever it comes from and a measurement must not reach a broker with
// it.
func TestNewRefusesDuplicatePriority(t *testing.T) {
	t.Parallel()
	_, err := New(stubDriver{}, Config{
		Namespace:  "bench-priorities-duplicate",
		Messages:   1,
		Publishers: 1,
		Priorities: []f1.Priority{f1.PriorityHigh, f1.PriorityHigh},
		Timeout:    time.Second,
	})
	if err == nil {
		t.Fatal("bench.New accepted a priority set naming one priority twice")
	}
}

// TestPublishSpansCoverTheCorpusExactlyOnce proves the invariants a chunked
// loader rests on: every corpus sequence belongs to exactly one publisher's
// span, and no span is empty. A gap would leave a sequence unpublished, which
// the tracker reports as a short corpus rather than as a wrong rate; an overlap
// would publish it twice, which the tracker counts as a duplicate; and an empty
// span would start a goroutine with nothing to do.
func TestPublishSpansCoverTheCorpusExactlyOnce(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		messages   int
		publishers int
	}{
		{name: "evenly divisible", messages: 64, publishers: 8},
		{name: "remainder", messages: 65, publishers: 8},
		{name: "fewer messages than publishers", messages: 3, publishers: 8},
		{name: "one message", messages: 1, publishers: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			seen := make(map[int]int, tc.messages)
			spans := publishSpans(tc.messages, tc.publishers)
			for _, span := range spans {
				if span[1] <= span[0] {
					t.Fatalf("span %v covers no sequence", span)
				}
				for seq := span[0]; seq < span[1]; seq++ {
					seen[seq]++
				}
			}
			if len(seen) != tc.messages {
				t.Fatalf("spans cover %d of %d sequences", len(seen), tc.messages)
			}
			for seq, count := range seen {
				if count != 1 {
					t.Fatalf("sequence %d is covered %d times", seq, count)
				}
			}
		})
	}
}

// TestTrackerSettleLatencyWindow proves which settlements a measured window's
// settle latencies are drawn from. Only the settlements that finish inside the
// window count: the warm-up a measurement settles before the clock opens would
// otherwise be one of the samples, and a settlement that arrives after the
// clock stopped is evidence that the corpus finished rather than part of the
// interval a percentile describes.
func TestTrackerSettleLatencyWindow(t *testing.T) {
	t.Parallel()
	tr := newTracker(clock.NewReal(), 2, 1)
	// The warm-up settles one message outside the window.
	tr.noteSettled(corpusBody(0), time.Second)
	if got := tr.preludeSettlements(); got != 1 {
		t.Fatalf("prelude settlements = %d, want 1", got)
	}
	tr.arm()
	tr.noteSettled(corpusBody(0), 3*time.Millisecond)
	tr.noteSettled(corpusBody(1), 5*time.Millisecond)
	// Both corpus messages have settled, so the clock has stopped, and this
	// last settlement is one the window does not describe.
	tr.noteSettled(corpusBody(1), 9*time.Millisecond)
	if !tr.hasStopped() {
		t.Fatal("the tracker did not stop when the corpus settled")
	}
	got, gotSequences := tr.settledLatencies()
	want := []time.Duration{3 * time.Millisecond, 5 * time.Millisecond}
	wantSequences := []int{0, 1}
	if len(got) != len(want) || len(gotSequences) != len(wantSequences) {
		t.Fatalf("settle latencies = %v with sequences %v, want %v with sequences %v", got, gotSequences, want, wantSequences)
	}
	for index := range want {
		if got[index] != want[index] || gotSequences[index] != wantSequences[index] {
			t.Fatalf("settle latencies = %v with sequences %v, want %v with sequences %v", got, gotSequences, want, wantSequences)
		}
	}
}

// TestResultSettlePercentileForPriority proves the accessor selects samples by
// the publish sequence's modulo priority mapping while the aggregate accessor
// continues to see every settle latency.
func TestResultSettlePercentileForPriority(t *testing.T) {
	t.Parallel()
	priorities := []f1.Priority{f1.PriorityHigh, f1.PriorityLow}
	result := Result{
		SettleLatencies: []time.Duration{
			time.Millisecond, 9 * time.Millisecond,
			2 * time.Millisecond, 10 * time.Millisecond,
			3 * time.Millisecond, 11 * time.Millisecond,
		},
		SettleSequences: []int{0, 1, 2, 3, 4, 5},
	}
	if got, want := result.SettlePercentileForPriority(f1.PriorityHigh, priorities, 50), 2*time.Millisecond; got != want {
		t.Fatalf("high settle p50 = %s, want %s", got, want)
	}
	if got, want := result.SettlePercentileForPriority(f1.PriorityLow, priorities, 50), 10*time.Millisecond; got != want {
		t.Fatalf("low settle p50 = %s, want %s", got, want)
	}
	if got, want := result.SettlePercentile(50), 3*time.Millisecond; got != want {
		t.Fatalf("aggregate settle p50 = %s, want %s", got, want)
	}
}
