package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
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
		pending:     make(chan amqp.Delivery, 1),
	}
	lane.pending <- amqp.Delivery{DeliveryTag: 1, Body: []byte("parked")}
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
		pending:     make(chan amqp.Delivery, 1),
	}
	lane.pending <- amqp.Delivery{DeliveryTag: 2, Body: []byte("stopped")}
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
		pending:     make(chan amqp.Delivery, 1),
		paused:      true,
		resume:      make(chan struct{}),
	}
	lane.pending <- amqp.Delivery{DeliveryTag: 3, Body: []byte("paused")}
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
