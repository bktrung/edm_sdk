package bench

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

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
		samplesRefused = len(tr.settledLatencies())
		messages <- driver.InboundMessage{Body: []byte(corpusBody(1)), Settle: stubSettler{}}
		synctest.Wait()
		accepted := <-consumer.Messages()
		if err := accepted.Settle.Ack(context.Background()); err != nil {
			t.Errorf("the accepted ack reported %v", err)
		}
		afterAccepted = tr.settledTotal()
		samplesAccepted = len(tr.settledLatencies())
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
	got := tr.settledLatencies()
	want := []time.Duration{3 * time.Millisecond, 5 * time.Millisecond}
	if len(got) != len(want) {
		t.Fatalf("settle latencies = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("settle latencies = %v, want %v", got, want)
		}
	}
}
