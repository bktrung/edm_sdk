package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestConsumerStopReleasesParkedForwarder(t *testing.T) {
	c := newForwarderTestConsumer()
	c.messages <- driver.InboundMessage{}
	delivered := &settler{owner: c}
	c.settlers[delivered] = struct{}{}
	c.outstanding = 1

	lane := &lane{
		owner:       c,
		destination: "stop-forwarder",
		pending:     make(chan laneDelivery, 1),
	}
	lane.pending <- laneDelivery{Delivery: amqp.Delivery{DeliveryTag: 1, Body: []byte("parked")}}
	c.forward.Add(1)
	go c.emitMessages(lane)

	deadline := time.NewTimer(time.Second) //nolint:forbidigo // bound a direct goroutine fixture
	defer deadline.Stop()
	for {
		c.mu.Lock()
		parked := c.outstanding == 2
		c.mu.Unlock()
		if parked {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("forwarder did not register the parked delivery")
		default:
			time.Sleep(time.Millisecond) //nolint:forbidigo // synchronize the direct goroutine fixture
		}
	}

	result := make(chan error, 1)
	go func() { result <- c.Stop(context.Background()) }()

	select {
	case err := <-result:
		if !errors.Is(err, driver.ErrResourcesOutstanding) {
			t.Fatalf("Stop() error = %v, want outstanding-resource refusal", err)
		}
		c.mu.Lock()
		outstanding := c.outstanding
		c.mu.Unlock()
		if outstanding != 1 {
			t.Fatalf("outstanding after parked forwarder escaped = %d, want 1", outstanding)
		}
	case <-time.After(100 * time.Millisecond): //nolint:forbidigo // the regression is an uncancelable deadlock
		close(c.stoppedC)
		<-result
		t.Fatal("Stop() blocked waiting for a forwarder released only after Stop")
	}
}

func TestConsumerStoppedForwarderReleasesSettler(t *testing.T) {
	c := newForwarderTestConsumer()
	c.messages <- driver.InboundMessage{}

	lane := &lane{
		owner:       c,
		destination: "stopped-forwarder",
		pending:     make(chan laneDelivery, 1),
	}
	lane.pending <- laneDelivery{Delivery: amqp.Delivery{DeliveryTag: 2, Body: []byte("stopped")}}
	c.forward.Add(1)
	go c.emitMessages(lane)
	waitForwarderState(t, "settler registration", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.outstanding == 1
	})

	close(c.stoppedC)
	waitForwarderDone(t, c)
	c.mu.Lock()
	outstanding := c.outstanding
	settlers := len(c.settlers)
	c.mu.Unlock()
	if outstanding != 0 || settlers != 0 {
		t.Fatalf("stopped forwarder accounting = outstanding %d, settlers %d, want 0, 0", outstanding, settlers)
	}
}

func TestConsumerPausedForwarderDecrementsEmission(t *testing.T) {
	c := newForwarderTestConsumer()
	lane := &lane{
		owner:       c,
		destination: "paused-forwarder",
		pending:     make(chan laneDelivery, 1),
		paused:      true,
		resume:      make(chan struct{}),
	}
	lane.pending <- laneDelivery{Delivery: amqp.Delivery{DeliveryTag: 3, Body: []byte("paused")}}
	c.forward.Add(1)
	go c.emitMessages(lane)
	waitForwarderState(t, "paused emission", func() bool {
		lane.mu.Lock()
		defer lane.mu.Unlock()
		return lane.emitting == 1
	})

	close(c.stoppedC)
	waitForwarderDone(t, c)
	lane.mu.Lock()
	emitting := lane.emitting
	lane.mu.Unlock()
	if emitting != 0 {
		t.Fatalf("paused forwarder emitting = %d, want 0", emitting)
	}
}

func newForwarderTestConsumer() *consumer {
	return &consumer{
		conn:           &conn{},
		clock:          clock.NewReal(),
		messages:       make(chan driver.InboundMessage, 1),
		errors:         make(chan error, 1),
		stoppedC:       make(chan struct{}),
		forwarderStopC: make(chan struct{}),
		settlers:       make(map[*settler]struct{}),
	}
}

func waitForwarderState(t *testing.T, what string, ready func() bool) {
	t.Helper()
	deadline := time.NewTimer(time.Second) //nolint:forbidigo // bound a direct goroutine fixture
	defer deadline.Stop()
	for {
		if ready() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", what)
		default:
			time.Sleep(time.Millisecond) //nolint:forbidigo // synchronize the direct goroutine fixture
		}
	}
}

func waitForwarderDone(t *testing.T, c *consumer) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		c.forward.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second): //nolint:forbidigo // bound a direct goroutine fixture
		t.Fatal("forwarder did not stop")
	}
}

// TestSecondReleaseWaitsForTheReleaseInProgress pins that a Release retried
// while the first one is still tearing the consumer down does not report
// success early: the consumer is still registered on its connection until the
// first finishes, and a caller told otherwise would close a connection that
// refuses to close.
func TestSecondReleaseWaitsForTheReleaseInProgress(t *testing.T) {
	c := newForwarderTestConsumer()
	c.forward.Add(1) // a forwarder that has not exited yet holds the first Release
	first := make(chan error, 1)
	go func() { first <- c.Release(context.Background()) }()
	waitForwarderState(t, "the first Release to start", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.stopped
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := c.Release(ctx); err == nil {
		t.Fatal("second Release() = nil while the first was still releasing")
	}

	c.forward.Done()
	if err := <-first; err != nil {
		t.Fatalf("first Release() = %v", err)
	}
	if err := c.Release(context.Background()); err != nil {
		t.Fatalf("Release() after the first finished = %v, want nil", err)
	}
}

type admissionAcknowledger struct{}

func (admissionAcknowledger) Ack(uint64, bool) error        { return nil }
func (admissionAcknowledger) Nack(uint64, bool, bool) error { return nil }
func (admissionAcknowledger) Reject(uint64, bool) error     { return nil }

func TestConsumerAdmissionRefillsOnlyAfterSettlement(t *testing.T) {
	for _, test := range []struct {
		name    string
		total   int
		windows []int
		want    int
	}{
		{name: "aggregate", total: 2, windows: []int{2, 2}, want: 2},
		{name: "destination", total: 3, windows: []int{1}, want: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newForwarderTestConsumer()
			c.cfg.Prefetch = test.total
			c.messages = make(chan driver.InboundMessage, 8)
			c.byName = make(map[string]*lane)
			for index, window := range test.windows {
				destination := string(rune('a' + index))
				l := &lane{
					owner: c, destination: destination,
					pending:        make(chan laneDelivery, 4),
					admissionLimit: window, admissionWake: make(chan struct{}, 1),
				}
				c.lanes = append(c.lanes, l)
				c.byName[destination] = l
				for tag := range 4 {
					l.pending <- laneDelivery{Delivery: amqp.Delivery{DeliveryTag: uint64(tag + 1), Acknowledger: admissionAcknowledger{}}}
				}
			}
			t.Cleanup(func() {
				c.stopSignals()
				waitForwarderDone(t, c)
			})
			for _, l := range c.lanes {
				c.forward.Add(1)
				go c.emitMessages(l)
			}
			read := func() driver.InboundMessage {
				select {
				case message := <-c.Messages():
					return message
				case <-time.After(time.Second): //nolint:forbidigo // bound the direct goroutine fixture
					t.Fatal("admission did not progress")
					return driver.InboundMessage{}
				}
			}
			held := make([]driver.InboundMessage, test.want)
			for index := range held {
				held[index] = read()
			}
			assertFull := func() {
				t.Helper()
				select {
				case message := <-c.Messages():
					t.Fatalf("admission exceeded cap: %q", message.Destination)
				case <-time.After(10 * time.Millisecond): //nolint:forbidigo // observe a blocked admission attempt
				}
			}
			assertFull()
			if err := held[0].Settle.Ack(context.Background()); err != nil {
				t.Fatal(err)
			}
			refill := read()
			assertFull()
			if err := held[0].Settle.Ack(context.Background()); !errors.Is(err, driver.ErrAlreadySettled) {
				t.Fatalf("duplicate Ack = %v, want ErrAlreadySettled", err)
			}
			assertFull()
			c.stopSignals()
			waitForwarderDone(t, c)
			if err := refill.Settle.Ack(context.Background()); err != nil {
				t.Fatalf("settlement after forwarder stop: %v", err)
			}
		})
	}
}

// TestConsumerReattachReleasesCancelledAdmission holds the one delivery a
// window of 1 admits, as a handler past the broker's consumer timeout does, and
// then attaches a replacement consumer the way a broker cancel does. The broker
// took the held delivery back, so the replacement's first delivery must be
// admitted without waiting for the held handler, a delivery of the cancelled
// consumer still in the lane buffer must pass without taking a place, and the
// held delivery's late settlement must not give back a place it no longer has.
func TestConsumerReattachReleasesCancelledAdmission(t *testing.T) {
	c := newForwarderTestConsumer()
	c.cfg.Prefetch = 1
	c.messages = make(chan driver.InboundMessage, 8)
	l := &lane{
		owner: c, destination: "cancelled",
		pending:        make(chan laneDelivery, 4),
		admissionLimit: 1, admissionWake: make(chan struct{}, 1),
		generation: 1,
	}
	c.lanes = []*lane{l}
	c.byName = map[string]*lane{l.destination: l}
	t.Cleanup(func() {
		c.stopSignals()
		waitForwarderDone(t, c)
	})
	c.forward.Add(1)
	go c.emitMessages(l)
	push := func(tag, generation uint64) {
		l.pending <- laneDelivery{Delivery: amqp.Delivery{DeliveryTag: tag, Acknowledger: admissionAcknowledger{}}, generation: generation}
	}
	read := func(what string) driver.InboundMessage {
		t.Helper()
		select {
		case message := <-c.Messages():
			return message
		case <-time.After(time.Second): //nolint:forbidigo // bound the direct goroutine fixture
			t.Fatalf("%s was not admitted", what)
			return driver.InboundMessage{}
		}
	}
	counts := func() (int, int) {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.admitted, l.admitted
	}

	push(1, 1)
	held := read("the first delivery")
	c.attachReplacement(l, make(chan amqp.Delivery))

	push(2, 2)
	fresh := read("the replacement's first delivery")
	push(3, 1)
	stale := read("a cancelled consumer's buffered delivery")
	if total, lane := counts(); total != 1 || lane != 1 {
		t.Fatalf("admitted after re-attach = %d total, %d lane, want 1, 1: only the replacement's delivery holds a place", total, lane)
	}
	for _, message := range []driver.InboundMessage{held, stale} {
		if err := message.Settle.Ack(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if total, lane := counts(); total != 1 || lane != 1 {
		t.Fatalf("admitted after the cancelled deliveries settled = %d total, %d lane, want 1, 1", total, lane)
	}
	if err := fresh.Settle.Ack(context.Background()); err != nil {
		t.Fatal(err)
	}
	if total, lane := counts(); total != 0 || lane != 0 {
		t.Fatalf("admitted after every settlement = %d total, %d lane, want 0, 0", total, lane)
	}
}
