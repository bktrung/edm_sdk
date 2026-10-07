package rabbitmq

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
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

// closedTeardownChannel uses only public AMQP APIs and an in-memory peer. A
// zero Channel panics on the old Stop's direct Close; this real, already-closed
// channel makes both old and new teardown paths safe to unwind without a broker.
func closedTeardownChannel(t *testing.T) *amqp.Channel {
	t.Helper()
	client, peer := net.Pipe()
	serverDone := make(chan struct{})
	endPeer := make(chan struct{})
	var endOnce sync.Once
	closePeer := func() { endOnce.Do(func() { close(endPeer) }) }
	t.Cleanup(func() {
		closePeer()
		_ = client.Close()
		_ = peer.Close()
		select {
		case <-serverDone:
		case <-time.After(time.Second): //nolint:forbidigo // join the in-memory protocol fixture on every exit
			t.Error("in-memory AMQP peer did not exit")
		}
	})
	if err := peer.SetDeadline(clock.NewReal().Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	server := make(chan error, 1)
	go func() {
		defer close(serverDone)
		defer peer.Close()
		readMethod := func(class, method uint16) error {
			var header [7]byte
			if _, err := io.ReadFull(peer, header[:]); err != nil {
				return err
			}
			payload := make([]byte, int(binary.BigEndian.Uint32(header[3:]))+1)
			if _, err := io.ReadFull(peer, payload); err != nil {
				return err
			}
			if header[0] != 1 || len(payload) < 5 || binary.BigEndian.Uint16(payload) != class ||
				binary.BigEndian.Uint16(payload[2:]) != method || payload[len(payload)-1] != 0xce {
				return errors.New("unexpected in-memory AMQP method")
			}
			return nil
		}
		writeMethod := func(channel, class, method uint16, args []byte) error {
			frame := []byte{1}
			frame = binary.BigEndian.AppendUint16(frame, channel)
			frame = binary.BigEndian.AppendUint32(frame, uint32(4+len(args))) //nolint:gosec // fixed internal handshake arguments, never external input
			frame = binary.BigEndian.AppendUint16(frame, class)
			frame = binary.BigEndian.AppendUint16(frame, method)
			frame = append(frame, args...)
			frame = append(frame, 0xce)
			_, err := peer.Write(frame)
			return err
		}
		var protocol [8]byte
		_, err := io.ReadFull(peer, protocol[:])
		if err == nil {
			err = writeMethod(0, 10, 10, []byte{0, 9, 0, 0, 0, 0, 0, 0, 0, 5, 'P', 'L', 'A', 'I', 'N', 0, 0, 0, 5, 'e', 'n', '_', 'U', 'S'})
		}
		if err == nil {
			err = readMethod(10, 11)
		}
		if err == nil {
			err = writeMethod(0, 10, 30, []byte{0, 0, 0, 2, 0, 0, 0, 0})
		}
		if err == nil {
			err = readMethod(10, 31)
		}
		if err == nil {
			err = readMethod(10, 40)
		}
		if err == nil {
			err = writeMethod(0, 10, 41, []byte{0})
		}
		if err == nil {
			err = readMethod(20, 10)
		}
		if err == nil {
			err = writeMethod(1, 20, 11, []byte{0, 0, 0, 0})
		}
		server <- err
		if err == nil {
			<-endPeer
		}
	}()
	connection, err := amqp.Open(client, amqp.Config{
		SASL: []amqp.Authentication{&amqp.PlainAuth{Username: "guest", Password: "guest"}},
	})
	if err != nil {
		t.Fatalf("open in-memory AMQP connection: %v", err)
	}
	channel, err := connection.Channel()
	if err != nil {
		t.Fatalf("open in-memory AMQP channel: %v", err)
	}
	if err := <-server; err != nil {
		t.Fatalf("in-memory AMQP peer: %v", err)
	}
	closePeer()
	waitForwarderState(t, "in-memory AMQP channel shutdown", channel.IsClosed)
	return channel
}

func assertTeardownState(t *testing.T, c *consumer, complete bool) {
	t.Helper()
	c.conn.mu.RLock()
	_, registered := c.conn.active[c]
	c.conn.mu.RUnlock()
	if registered == complete {
		t.Errorf("consumer registered = %v, want %v", registered, !complete)
	}
	if closed := messagesClosed(c); closed != complete {
		t.Errorf("Messages closed = %v, want %v", closed, complete)
	}
	select {
	case _, ok := <-c.Errors():
		if ok || !complete {
			t.Errorf("Errors ready with open = %v, want closed = %v", ok, complete)
		}
	default:
		if complete {
			t.Error("Errors still open after successful teardown")
		}
	}
}

func teardownResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second): //nolint:forbidigo // bound a direct teardown fixture
		t.Fatal("teardown call did not return")
		return nil
	}
}

func startTeardownCall(t *testing.T, c *consumer, calls *sync.WaitGroup, call func() error) <-chan error {
	result := make(chan error, 1)
	calls.Go(func() {
		err := call()
		if err == nil {
			assertTeardownState(t, c, true)
		}
		result <- err
	})
	return result
}

func joinTeardownFixture(t *testing.T, c *consumer, calls *sync.WaitGroup) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		calls.Wait()
		c.conn.detachWatch.Wait()
		done <- nil
	}()
	_ = teardownResult(t, done)
}

// teardownWaitContext signals arrival at a context-aware wait without depending
// on renamed private lifecycle fields. With the fixture owner blocked, the new
// implementation reaches Done in its shared-attempt select. The old Stop may
// reach Done in a local join instead, but must then fail on its premature nil.
type teardownWaitContext struct {
	context.Context
	arrived chan struct{}
	once    sync.Once
	expired chan struct{}
}

func newTeardownWaitContext() *teardownWaitContext {
	return &teardownWaitContext{
		Context: context.Background(),
		arrived: make(chan struct{}),
		expired: make(chan struct{}),
	}
}

func (ctx *teardownWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.arrived) })
	return ctx.expired
}

func (ctx *teardownWaitContext) Err() error {
	select {
	case <-ctx.expired:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

// postDrainDeadlineContext expires at the check after Stop's drain and joins;
// the earlier checks at teardown entry and Drain still see a live context.
type postDrainDeadlineContext struct {
	*teardownWaitContext
	checks int
}

func (ctx *postDrainDeadlineContext) Err() error {
	ctx.checks++
	if ctx.checks == 3 {
		close(ctx.expired)
	}
	return ctx.teardownWaitContext.Err()
}

func TestConsumerStopFinishesAfterSuccessfulDrainDeadline(t *testing.T) {
	c := newForwarderTestConsumer()
	c.conn.active = map[*consumer]struct{}{c: {}}
	ctx := &postDrainDeadlineContext{teardownWaitContext: newTeardownWaitContext()}

	if err := c.Stop(ctx); err != nil {
		t.Errorf("Stop() = %v, want nil after successful drain", err)
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("Stop context did not expire before the teardown claim")
	}
	assertTeardownState(t, c, true)
}

func awaitTeardownWait(t *testing.T, ctx *teardownWaitContext, result <-chan error) bool {
	t.Helper()
	select {
	case <-ctx.arrived:
		return true
	case err := <-result:
		t.Errorf("competitor returned %v while teardown incomplete before entering its wait", err)
		return false
	case <-time.After(time.Second): //nolint:forbidigo // bound scheduling of the context-aware wait
		t.Fatal("competitor did not reach its context-aware wait")
		return false
	}
}

// blockedStopFixture installs the synthetic lane only after Drain snapshots the
// empty lanes. Holding channelMu and freezing the clock wedges both the old
// lockBefore and the shared closeLanesWithContext, not merely the new close hook.
func blockedStopFixture(t *testing.T) (*consumer, <-chan error, *sync.WaitGroup, func(), *atomic.Int32) {
	t.Helper()
	c := newForwarderTestConsumer()
	c.conn.active = map[*consumer]struct{}{c: {}}
	fake := clock.NewFake(time.Unix(0, 0))
	c.clock = fake
	l := &lane{owner: c, channel: closedTeardownChannel(t), destination: "shared-stop"}
	l.channelMu.Lock()
	var unlockOnce, forwardOnce sync.Once
	var starts atomic.Int32
	c.channelCloseHook = func(*lane) error {
		starts.Add(1)
		return nil
	}
	var calls sync.WaitGroup
	c.forward.Add(1)
	unblock := func() {
		forwardOnce.Do(c.forward.Done)
		unlockOnce.Do(func() {
			l.channelMu.Unlock()
			fake.Advance(closeLockWait)
		})
	}
	t.Cleanup(func() {
		unblock()
		joinTeardownFixture(t, c, &calls)
	})
	first := startTeardownCall(t, c, &calls, func() error { return c.Stop(context.Background()) })
	waitForwarderState(t, "Stop's empty Drain snapshot", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.draining
	})
	c.mu.Lock()
	c.lanes = []*lane{l}
	c.mu.Unlock()
	forwardOnce.Do(c.forward.Done)
	waitForwarderState(t, "Stop waiting on the lane RPC lock", func() bool { return fake.NumWaiters() > 0 })
	assertTeardownState(t, c, false)
	return c, first, &calls, unblock, &starts
}

func TestConsumerStopWaitsForSharedTeardown(t *testing.T) {
	c, first, calls, unblock, starts := blockedStopFixture(t)
	secondCtx, releaseCtx := newTeardownWaitContext(), newTeardownWaitContext()
	second := startTeardownCall(t, c, calls, func() error { return c.Stop(secondCtx) })
	release := startTeardownCall(t, c, calls, func() error { return c.Release(releaseCtx) })
	secondWaiting := awaitTeardownWait(t, secondCtx, second)
	releaseWaiting := awaitTeardownWait(t, releaseCtx, release)
	if !secondWaiting {
		second = nil
	}
	if !releaseWaiting {
		release = nil
	}
	pending := []struct {
		name   string
		result <-chan error
	}{
		{name: "first Stop", result: first},
		{name: "second Stop", result: second},
		{name: "Release", result: release},
	}
	for index := range pending {
		if pending[index].result == nil {
			continue
		}
		select {
		case err := <-pending[index].result:
			if err == nil {
				t.Errorf("%s returned nil while teardown incomplete", pending[index].name)
			} else {
				t.Errorf("%s returned %v while teardown incomplete, want no return", pending[index].name, err)
			}
			pending[index].result = nil
		case <-time.After(20 * time.Millisecond): //nolint:forbidigo // bound the early-success observation while the fake clock stays frozen
		}
		assertTeardownState(t, c, false)
	}
	unblock()
	for _, call := range pending {
		if call.result == nil {
			continue
		}
		if err := teardownResult(t, call.result); err != nil {
			t.Errorf("%s = %v, want nil", call.name, err)
		}
		assertTeardownState(t, c, true)
	}
	if got := starts.Load(); got != 1 {
		t.Errorf("lane close starts = %d, want 1", got)
	}
	for _, call := range []func() error{
		func() error { return c.Stop(context.Background()) },
		func() error { return c.Release(context.Background()) },
	} {
		if err := teardownResult(t, startTeardownCall(t, c, calls, call)); err != nil {
			t.Errorf("repeated teardown = %v, want nil", err)
		}
		assertTeardownState(t, c, true)
	}
}

func TestSecondStopBoundsItsWaitForTeardown(t *testing.T) {
	c, first, calls, unblock, starts := blockedStopFixture(t)
	ctx := newTeardownWaitContext()
	second := startTeardownCall(t, c, calls, func() error { return c.Stop(ctx) })
	var err error
	if awaitTeardownWait(t, ctx, second) {
		select {
		case err = <-second:
		case <-time.After(20 * time.Millisecond): //nolint:forbidigo // keep the context live while the baseline's already-finished joins return
			close(ctx.expired)
			err = teardownResult(t, second)
		}
	}
	if err == nil {
		t.Error("second Stop returned nil while teardown incomplete")
	} else {
		if !errors.Is(err, driver.ErrDrainTimeout) || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("second Stop = %v, want ErrDrainTimeout and context.DeadlineExceeded", err)
		}
		if kind, ok := driver.Classify(err); !ok || kind != driver.KindTransient {
			t.Errorf("second Stop = %v, want transient classification", err)
		}
	}
	assertTeardownState(t, c, false)
	select {
	case err := <-first:
		t.Fatalf("first Stop returned %v while teardown incomplete", err)
	default:
	}
	unblock()
	if err := teardownResult(t, first); err != nil {
		t.Errorf("first Stop = %v, want nil", err)
	}
	assertTeardownState(t, c, true)
	if err := teardownResult(t, startTeardownCall(t, c, calls, func() error { return c.Stop(context.Background()) })); err != nil {
		t.Errorf("Stop after completion = %v, want nil", err)
	}
	assertTeardownState(t, c, true)
	if got := starts.Load(); got != 1 {
		t.Errorf("lane close starts = %d, want 1", got)
	}
}

func blockedReleaseFixture(t *testing.T) (*consumer, <-chan error, *sync.WaitGroup, context.CancelFunc, func(), *atomic.Int32) {
	t.Helper()
	c := newForwarderTestConsumer()
	c.clock = clock.NewFake(time.Unix(0, 0))
	c.conn.active = map[*consumer]struct{}{c: {}}
	c.settlers[&settler{owner: c}] = struct{}{}
	c.outstanding = 1
	c.lanes = []*lane{{owner: c, channel: closedTeardownChannel(t), destination: "release-first"}}
	started := make(chan struct{})
	resume := make(chan struct{})
	var once sync.Once
	var starts atomic.Int32
	c.channelCloseHook = func(*lane) error {
		if starts.Add(1) == 1 {
			close(started)
		}
		<-resume
		return nil
	}
	unblock := func() { once.Do(func() { close(resume) }) }
	ctx, cancel := context.WithCancel(context.Background())
	var calls sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		unblock()
		joinTeardownFixture(t, c, &calls)
	})
	first := startTeardownCall(t, c, &calls, func() error { return c.Release(ctx) })
	waitForwarderState(t, "Release's broker close", func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	})
	assertTeardownState(t, c, false)
	return c, first, &calls, cancel, unblock, &starts
}

func TestStopWaitsForReleaseTeardown(t *testing.T) {
	c, release, calls, _, unblock, starts := blockedReleaseFixture(t)
	ctx := newTeardownWaitContext()
	stop := startTeardownCall(t, c, calls, func() error { return c.Stop(ctx) })
	returned := !awaitTeardownWait(t, ctx, stop)
	if !returned {
		select {
		case err := <-stop:
			t.Errorf("Stop returned %v while Release teardown incomplete", err)
			returned = true
		case <-time.After(20 * time.Millisecond): //nolint:forbidigo // bound the early-success observation after confirmed arrival
		}
	}
	assertTeardownState(t, c, false)
	unblock()
	if err := teardownResult(t, release); err != nil {
		t.Errorf("Release = %v, want nil", err)
	}
	assertTeardownState(t, c, true)
	if !returned {
		if err := teardownResult(t, stop); err != nil {
			t.Errorf("Stop after Release = %v, want nil despite abandoned settlers", err)
		}
		assertTeardownState(t, c, true)
	}
	if got := starts.Load(); got != 1 {
		t.Errorf("lane close starts = %d, want 1", got)
	}
}

func awaitTeardownSignal(t *testing.T, signal <-chan struct{}, result <-chan error, what string) {
	t.Helper()
	select {
	case <-signal:
	case err := <-result:
		t.Fatalf("%s: teardown returned %v before the signal", what, err)
	case <-time.After(time.Second): //nolint:forbidigo // bound a wedged teardown fixture, not its interleaving
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestConsumerStopBoundsFailedReleaseTakeover(t *testing.T) {
	for _, retry := range []string{"stop", "release"} {
		t.Run(retry, func(t *testing.T) {
			c := newForwarderTestConsumer()
			c.clock = clock.NewFake(time.Unix(0, 0))
			c.conn.active = map[*consumer]struct{}{c: {}}
			l := &lane{owner: c, channel: closedTeardownChannel(t), destination: "retained-close"}
			c.lanes = []*lane{l}
			closeStarted, allowClose := make(chan struct{}), make(chan struct{})
			var starts atomic.Int32
			c.channelCloseHook = func(*lane) error {
				starts.Add(1)
				close(closeStarted)
				<-allowClose
				return nil
			}
			var closeOnce sync.Once
			unblock := func() { closeOnce.Do(func() { close(allowClose) }) }
			releaseCtx, cancel := context.WithCancel(context.Background())
			var calls sync.WaitGroup
			t.Cleanup(func() {
				cancel()
				unblock()
				joinTeardownFixture(t, c, &calls)
			})
			release := startTeardownCall(t, c, &calls, func() error { return c.Release(releaseCtx) })
			awaitTeardownSignal(t, closeStarted, release, "Release's lane close")
			cancel()
			if err := teardownResult(t, release); !errors.Is(err, context.Canceled) {
				t.Fatalf("Release = %v, want context.Canceled", err)
			}
			assertTeardownState(t, c, false)
			l.closeMu.Lock()
			retained := l.closeOutcome
			l.closeMu.Unlock()
			if retained == nil {
				t.Fatal("failed Release did not retain its lane close")
			}

			ctx := newTeardownWaitContext()
			stop := startTeardownCall(t, c, &calls, func() error { return c.Stop(ctx) })
			awaitTeardownSignal(t, ctx.arrived, stop, "Stop's retained close wait")
			close(ctx.expired)
			err := teardownResult(t, stop)
			if !errors.Is(err, driver.ErrDrainTimeout) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Stop = %v, want ErrDrainTimeout and context.DeadlineExceeded", err)
			}
			var classified, nested *driver.Error
			if !errors.As(err, &classified) || classified.Op != "stop" || classified.K != driver.KindTransient {
				t.Fatalf("Stop = %v, want stop/transient driver.Error", err)
			}
			if errors.As(classified.Err, &nested) {
				t.Fatalf("Stop has nested driver.Error: %v", nested)
			}
			assertTeardownState(t, c, false)
			c.mu.Lock()
			stopped, active, finished := c.stopped, c.tearingDown, c.teardownFinished
			c.mu.Unlock()
			if !stopped || active || finished {
				t.Fatalf("failed Stop state = stopped %v, active %v, finished %v", stopped, active, finished)
			}
			l.closeMu.Lock()
			sameOutcome := l.closeOutcome == retained
			l.closeMu.Unlock()
			if !sameOutcome || starts.Load() != 1 {
				t.Fatal("takeover replaced or restarted the retained lane close")
			}

			unblock()
			call := func() error { return c.Stop(context.Background()) }
			if retry == "release" {
				call = func() error { return c.Release(context.Background()) }
			}
			if err := teardownResult(t, startTeardownCall(t, c, &calls, call)); err != nil {
				t.Fatalf("retry %s = %v, want nil", retry, err)
			}
			assertTeardownState(t, c, true)
			if got := starts.Load(); got != 1 {
				t.Fatalf("lane close starts = %d, want 1", got)
			}
			if err := c.Stop(context.Background()); err != nil {
				t.Fatalf("completed Stop = %v", err)
			}
			if err := c.Release(context.Background()); err != nil {
				t.Fatalf("completed Release = %v", err)
			}
		})
	}
}

// beforeDrainClaimContext holds Stop before Drain takes mu, so Release can claim
// teardown after Stop's entry inspection without relying on goroutine timing.
type beforeDrainClaimContext struct {
	*teardownWaitContext
	checks      atomic.Int32
	beforeDrain chan struct{}
	allowDrain  chan struct{}
}

func (ctx *beforeDrainClaimContext) Err() error {
	if ctx.checks.Add(1) == 2 {
		close(ctx.beforeDrain)
		<-ctx.allowDrain
	}
	return ctx.teardownWaitContext.Err()
}

func TestConsumerStopResumesAfterReleaseClaimsBeforeDrain(t *testing.T) {
	c := newForwarderTestConsumer()
	c.clock = clock.NewFake(time.Unix(0, 0))
	c.conn.active = map[*consumer]struct{}{c: {}}
	deliveries := make(chan amqp.Delivery)
	first := &lane{owner: c, channel: closedTeardownChannel(t), destination: "first-close"}
	second := &lane{
		owner: c, channel: closedTeardownChannel(t), destination: "reader-left-open",
		deliveries: deliveries, generation: 1, pending: make(chan laneDelivery),
	}
	c.lanes = []*lane{first, second}
	firstStarted, secondStarted, allowFirst := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var firstStarts, secondStarts atomic.Int32
	var firstOnce, deliveriesOnce, drainOnce sync.Once
	unblockFirst := func() { firstOnce.Do(func() { close(allowFirst) }) }
	closeDeliveries := func() { deliveriesOnce.Do(func() { close(deliveries) }) }
	c.channelCloseHook = func(l *lane) error {
		if l == first {
			firstStarts.Add(1)
			close(firstStarted)
			<-allowFirst
		} else {
			secondStarts.Add(1)
			close(secondStarted)
			closeDeliveries()
		}
		return nil
	}
	ctx := &beforeDrainClaimContext{
		teardownWaitContext: newTeardownWaitContext(),
		beforeDrain:         make(chan struct{}),
		allowDrain:          make(chan struct{}),
	}
	allowDrain := func() { drainOnce.Do(func() { close(ctx.allowDrain) }) }
	releaseCtx, cancel := context.WithCancel(context.Background())
	var calls sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		allowDrain()
		unblockFirst()
		closeDeliveries()
		joinTeardownFixture(t, c, &calls)
		c.readers.Wait()
	})
	c.readers.Add(1)
	go c.readDeliveries(second)
	stop := startTeardownCall(t, c, &calls, func() error { return c.Stop(ctx) })
	awaitTeardownSignal(t, ctx.beforeDrain, stop, "Stop before Drain")
	release := startTeardownCall(t, c, &calls, func() error { return c.Release(releaseCtx) })
	awaitTeardownSignal(t, firstStarted, release, "Release's first close")
	allowDrain()
	awaitTeardownSignal(t, ctx.arrived, stop, "Stop waiting for the Release claim")
	cancel()
	if err := teardownResult(t, release); !errors.Is(err, context.Canceled) {
		t.Fatalf("Release = %v, want context.Canceled", err)
	}
	if got := secondStarts.Load(); got != 0 {
		t.Fatalf("second lane close starts before takeover = %d, want 0", got)
	}
	assertTeardownState(t, c, false)
	unblockFirst()
	awaitTeardownSignal(t, secondStarted, stop, "Stop's remaining lane close")
	if err := teardownResult(t, stop); err != nil {
		t.Fatalf("Stop takeover = %v, want nil", err)
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("Stop used its deadline rather than taking over: %v", err)
	}
	assertTeardownState(t, c, true)
	if firstStarts.Load() != 1 || secondStarts.Load() != 1 {
		t.Fatalf("lane close starts = %d, %d, want 1, 1", firstStarts.Load(), secondStarts.Load())
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("completed Stop = %v", err)
	}
	if err := c.Release(context.Background()); err != nil {
		t.Fatalf("completed Release = %v", err)
	}
}

func TestConsumerStopKeepsSuccessfulDrainCloseUnbounded(t *testing.T) {
	c := newForwarderTestConsumer()
	c.clock = clock.NewFake(time.Unix(0, 0))
	c.conn.active = map[*consumer]struct{}{c: {}}
	forwarderExiting, allowExit := make(chan struct{}), make(chan struct{})
	closeStarted, allowClose := make(chan struct{}), make(chan struct{})
	var exitOnce, closeOnce sync.Once
	unblockExit := func() { exitOnce.Do(func() { close(allowExit) }) }
	unblockClose := func() { closeOnce.Do(func() { close(allowClose) }) }
	c.forwarderExitHook = func() {
		close(forwarderExiting)
		<-allowExit
	}
	c.channelCloseHook = func(*lane) error {
		close(closeStarted)
		<-allowClose
		return nil
	}
	ctx := &postDrainDeadlineContext{teardownWaitContext: newTeardownWaitContext()}
	var calls sync.WaitGroup
	t.Cleanup(func() {
		unblockExit()
		unblockClose()
		joinTeardownFixture(t, c, &calls)
		c.forward.Wait()
	})
	c.forward.Add(1)
	go c.emitMessages(&lane{owner: c, pending: make(chan laneDelivery)})
	stop := startTeardownCall(t, c, &calls, func() error { return c.Stop(ctx) })
	awaitTeardownSignal(t, forwarderExiting, stop, "Drain stopping the forwarder")
	// Drain closes forwarderStopC and snapshots lanes under mu. The forwarder
	// reaches its exit hook before Done; taking mu here waits out that snapshot.
	// Installing the lane now bypasses Cancel and isolates the final Close RPC.
	c.mu.Lock()
	c.lanes = []*lane{{owner: c, channel: closedTeardownChannel(t), destination: "post-drain-close"}}
	c.mu.Unlock()
	unblockExit()
	awaitTeardownSignal(t, closeStarted, stop, "successful-drain final close")
	select {
	case <-ctx.expired:
	default:
		t.Fatal("final lane close started before the post-drain deadline")
	}
	unblockClose()
	if err := teardownResult(t, stop); err != nil {
		t.Fatalf("Stop = %v, want nil after successful drain", err)
	}
	assertTeardownState(t, c, true)
}
