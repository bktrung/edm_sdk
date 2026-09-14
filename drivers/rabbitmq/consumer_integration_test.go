//go:build integration

package rabbitmq

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestConsumerPauseResume(t *testing.T) {
	requireBroker(t)
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("raw broker connection: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("raw channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })

	const queueA = "rabbitmq-driver-consumer-a"
	const queueB = "rabbitmq-driver-consumer-b"
	for _, name := range []string{queueA, queueB} {
		_, _ = rawChannel.QueueDelete(name, false, false, false)
		if _, err := rawChannel.QueueDeclare(name, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
			t.Fatalf("QueueDeclare(%q): %v", name, err)
		}
		t.Cleanup(func() { _, _ = rawChannel.QueueDelete(name, false, false, false) })
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	sdkConsumer, err := conn.Consumer(ctx, driver.ConsumerConfig{
		Destinations:   []string{queueA, queueB},
		Prefetch:       2,
		PerDestination: map[string]int{queueA: 1, queueB: 1},
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}

	if err := sdkConsumer.Pause(queueA); err != nil {
		t.Fatalf("Pause(a): %v", err)
	}
	if err := sdkConsumer.Pause(queueA); err != nil {
		t.Fatalf("repeated Pause(a): %v", err)
	}
	publish := func(queue, body string) {
		t.Helper()
		if err := rawChannel.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         []byte(body),
		}); err != nil {
			t.Fatalf("Publish(%q): %v", queue, err)
		}
	}
	publish(queueA, "a-1")
	publish(queueB, "b-1")

	receiveCtx, receiveCancel := context.WithTimeout(ctx, 5*time.Second)
	defer receiveCancel()
	select {
	case message := <-sdkConsumer.Messages():
		if message.Destination != queueB || string(message.Body) != "b-1" {
			t.Fatalf("received while a paused = %q/%q, want b-1", message.Destination, message.Body)
		}
		if err := message.Settle.Ack(ctx); err != nil {
			t.Fatalf("Ack(b-1): %v", err)
		}
	case <-receiveCtx.Done():
		t.Fatal("timed out waiting for positive control from destination b")
	}

	publish(queueB, "b-2")
	select {
	case message := <-sdkConsumer.Messages():
		if message.Destination != queueB || string(message.Body) != "b-2" {
			t.Fatalf("second active delivery = %q/%q, want b-2", message.Destination, message.Body)
		}
		if err := message.Settle.Ack(ctx); err != nil {
			t.Fatalf("Ack(b-2): %v", err)
		}
	case <-receiveCtx.Done():
		t.Fatal("timed out waiting for second active delivery")
	}

	select {
	case message := <-sdkConsumer.Messages():
		t.Fatalf("received paused destination = %q/%q", message.Destination, message.Body)
	case <-time.After(100 * time.Millisecond): //nolint:forbidigo // test timeout is intentionally wall-clock based
	}

	if err := sdkConsumer.Resume(queueA); err != nil {
		t.Fatalf("Resume(a): %v", err)
	}
	for _, want := range []string{"a-1"} {
		select {
		case message := <-sdkConsumer.Messages():
			if message.Destination != queueA || string(message.Body) != want {
				t.Fatalf("resumed delivery = %q/%q, want a/%q", message.Destination, message.Body, want)
			}
			if err := message.Settle.Ack(ctx); err != nil {
				t.Fatalf("Ack(%q): %v", want, err)
			}
		case <-receiveCtx.Done():
			t.Fatalf("timed out waiting for resumed delivery %q", want)
		}
	}
	if err := sdkConsumer.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsumerDrainAfterCreationContextCancellation(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const queue = "rabbitmq-driver-consumer-cancel-drain"

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}

	consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{queue}, Prefetch: 1})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	cancel()

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	if err := consumer.Drain(drainCtx); err != nil {
		t.Fatalf("Drain after creation context cancellation: %v", err)
	}
	if err := consumer.Stop(drainCtx); err != nil {
		t.Fatalf("Stop after Drain: %v", err)
	}
}

func TestStopReleasesReaderBlockedOnFullLane(t *testing.T) {
	queue := consumerAdmissionQueue(t)
	connection := openConsumerAdmissionConn(t, queue)
	maintenance, ok := connection.Admin().(driver.Maintenance)
	if !ok {
		t.Fatal("Admin does not implement Maintenance")
	}
	if _, err := maintenance.Purge(context.Background(), queue); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	var blockedSends atomic.Int32
	installConsumerConstructionHook(t, func(candidate *consumer, phase consumerConstructionPhase) {
		if phase != consumerConstructionStarted {
			return
		}
		candidate.readerSendHook = func(candidateLane *lane) {
			if len(candidateLane.pending) == cap(candidateLane.pending) {
				blockedSends.Add(1)
			}
		}
	})

	sdkConsumer, err := connection.Consumer(context.Background(), driver.ConsumerConfig{
		Destinations: []string{queue},
		Prefetch:     2,
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	built := sdkConsumer.(*consumer)
	consumerLane := built.lanes[0]
	t.Cleanup(func() { _ = sdkConsumer.Release(context.Background()) })

	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("raw broker connection: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("raw channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })
	publish := func(body string) {
		t.Helper()
		if err := rawChannel.PublishWithContext(context.Background(), "", queue, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         []byte(body),
		}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	if err := sdkConsumer.Pause(queue); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	for range cap(built.messages) {
		built.messages <- driver.InboundMessage{}
	}
	for deliveryTag := range cap(consumerLane.pending) {
		consumerLane.pending <- amqp.Delivery{DeliveryTag: uint64(deliveryTag + 1)}
	}
	waitForwarderState(t, "paused forwarder", func() bool {
		consumerLane.mu.Lock()
		defer consumerLane.mu.Unlock()
		return consumerLane.emitting == 1
	})

	readerDone := make(chan struct{})
	go func() {
		built.readers.Wait()
		close(readerDone)
	}()
	publish("blocked-1")
	publish("blocked-2")
	waitForwarderState(t, "blocked reader send", func() bool {
		return blockedSends.Load() == 1 &&
			len(built.messages) == cap(built.messages) &&
			len(consumerLane.pending) == cap(consumerLane.pending)
	})
	select {
	case <-readerDone:
		t.Fatal("reader exited before Stop released its blocked send")
	default:
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := sdkConsumer.Stop(stopCtx); err != nil {
		if errors.Is(err, driver.ErrDrainTimeout) {
			t.Fatalf("Stop returned drain timeout: %v", err)
		}
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-readerDone:
	case <-time.After(time.Second): //nolint:forbidigo // bound the reader shutdown assertion
		t.Fatal("reader did not exit after Stop")
	}
}

func TestDrainJoinsForwardersBeforeReturning(t *testing.T) {
	queue := consumerAdmissionQueue(t)
	connection := openConsumerAdmissionConn(t, queue)
	maintenance, ok := connection.Admin().(driver.Maintenance)
	if !ok {
		t.Fatal("Admin does not implement Maintenance")
	}
	if _, err := maintenance.Purge(context.Background(), queue); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	var forwardersExited atomic.Int32
	forwarderExitStarted := make(chan struct{})
	releaseForwarderExit := make(chan struct{})
	installConsumerConstructionHook(t, func(candidate *consumer, phase consumerConstructionPhase) {
		if phase != consumerConstructionStarted {
			return
		}
		candidate.forwarderExitHook = func() {
			close(forwarderExitStarted)
			<-releaseForwarderExit
			forwardersExited.Add(1)
		}
	})
	sdkConsumer, err := connection.Consumer(context.Background(), driver.ConsumerConfig{
		Destinations: []string{queue},
		Prefetch:     1,
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	built := sdkConsumer.(*consumer)
	t.Cleanup(func() { _ = sdkConsumer.Release(context.Background()) })

	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("raw broker connection: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("raw channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })
	built.messages <- driver.InboundMessage{}
	if err := rawChannel.PublishWithContext(context.Background(), "", queue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte("blocked"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	waitForwarderState(t, "blocked forwarder", func() bool {
		built.mu.Lock()
		defer built.mu.Unlock()
		return built.outstanding == 1 && len(built.messages) == cap(built.messages)
	})

	drainCtx := context.Background()
	drainDone := make(chan error, 1)
	go func() { drainDone <- sdkConsumer.Drain(drainCtx) }()
	select {
	case <-forwarderExitStarted:
	case <-time.After(time.Second): //nolint:forbidigo // bound the forwarder exit synchronization
		t.Fatal("forwarder did not begin exiting")
	}
	select {
	case err := <-drainDone:
		close(releaseForwarderExit)
		t.Fatalf("Drain returned before forwarder exit: %v", err)
	case <-time.After(100 * time.Millisecond): //nolint:forbidigo // prove Drain remains joined
	}
	close(releaseForwarderExit)
	if err := <-drainDone; err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if got := forwardersExited.Load(); got != 1 {
		t.Fatalf("forwarders exited after Drain = %d, want 1", got)
	}
}

func TestStopAfterFailedDrainLeavesTheConsumerRetryable(t *testing.T) {
	queue := consumerAdmissionQueue(t)
	connection := openConsumerAdmissionConn(t, queue)
	maintenance, ok := connection.Admin().(driver.Maintenance)
	if !ok {
		t.Fatal("Admin does not implement Maintenance")
	}
	if _, err := maintenance.Purge(context.Background(), queue); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	cancelStarted := make(chan struct{})
	releaseCancel := make(chan struct{})
	var cancelReleaseOnce sync.Once
	releaseCancelHook := func() {
		cancelReleaseOnce.Do(func() { close(releaseCancel) })
	}
	var cancelCalls atomic.Int32
	installConsumerConstructionHook(t, func(candidate *consumer, phase consumerConstructionPhase) {
		if phase != consumerConstructionStarted {
			return
		}
		candidate.cancelHook = func() {
			if cancelCalls.Add(1) != 1 {
				return
			}
			close(cancelStarted)
			<-releaseCancel
		}
	})
	t.Cleanup(releaseCancelHook)
	sdkConsumer, err := connection.Consumer(context.Background(), driver.ConsumerConfig{
		Destinations: []string{queue},
		Prefetch:     1,
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() { _ = sdkConsumer.Release(context.Background()) })

	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("raw broker connection: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("raw channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })
	if err := rawChannel.PublishWithContext(context.Background(), "", queue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte("retryable"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	receiveCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var message driver.InboundMessage
	select {
	case message = <-sdkConsumer.Messages():
	case <-receiveCtx.Done():
		t.Fatalf("receive: %v", receiveCtx.Err())
	}

	done := make(chan struct{})
	close(done)
	stopCtx := &failedDrainContext{
		Context: context.Background(),
		done:    done,
	}
	stopErr := sdkConsumer.Stop(stopCtx)
	if stopErr == nil {
		t.Fatal("Stop after failed Drain = nil, want error")
	}
	if !errors.Is(stopErr, driver.ErrDrainTimeout) || !errors.Is(stopErr, context.DeadlineExceeded) {
		t.Fatalf("Stop after failed Drain = %v, want drain timeout and deadline", stopErr)
	}
	waitForConsumerSignal(t, cancelStarted, "first channel cancellation")
	if got := stopCtx.errCalls.Load(); got != 3 {
		t.Fatalf("stop context Err calls = %d, want 3", got)
	}
	built := sdkConsumer.(*consumer)
	built.mu.Lock()
	outstanding := built.outstanding
	stopped := built.stopped
	built.mu.Unlock()
	if outstanding != 1 {
		t.Fatalf("outstanding after failed Drain = %d, want 1", outstanding)
	}
	if stopped {
		t.Fatal("consumer stopped after failed Drain")
	}
	select {
	case <-built.stoppedC:
		t.Fatal("stopped signal closed after failed Drain")
	default:
	}
	if built.lanes[0].channel.IsClosed() {
		t.Fatal("lane channel closed after failed Drain")
	}
	select {
	case _, ok := <-built.messages:
		if !ok {
			t.Fatal("messages channel closed after failed Drain")
		}
		t.Fatal("unexpected message buffered after failed Drain")
	default:
	}
	select {
	case _, ok := <-built.errors:
		if !ok {
			t.Fatal("errors channel closed after failed Drain")
		}
		t.Fatal("unexpected error buffered after failed Drain")
	default:
	}
	if err := message.Settle.Ack(context.Background()); err != nil {
		t.Fatalf("Ack after failed Drain: %v", err)
	}
	retryDone := make(chan error, 1)
	go func() { retryDone <- sdkConsumer.Stop(context.Background()) }()
	select {
	case err := <-retryDone:
		releaseCancelHook()
		t.Fatalf("retry Stop returned while first channel cancellation held: %v", err)
	case <-time.After(100 * time.Millisecond): //nolint:forbidigo // bounded cancellation serialization assertion
	}
	releaseCancelHook()
	select {
	case err := <-retryDone:
		if err != nil {
			t.Fatalf("retry Stop: %v", err)
		}
	case <-time.After(3 * time.Second): //nolint:forbidigo // bounded cancellation retry assertion
		t.Fatal("retry Stop did not complete after releasing first cancellation")
	}
	if got := cancelCalls.Load(); got != 2 {
		t.Fatalf("channel cancellation hook calls = %d, want 2", got)
	}
}

func TestReleaseDoesNotWaitForInFlightCancellation(t *testing.T) {
	queue := consumerAdmissionQueue(t)
	connection := openConsumerAdmissionConn(t, queue)
	maintenance, ok := connection.Admin().(driver.Maintenance)
	if !ok {
		t.Fatal("Admin does not implement Maintenance")
	}
	if _, err := maintenance.Purge(context.Background(), queue); err != nil {
		t.Fatalf("Purge: %v", err)
	}

	cancelStarted := make(chan struct{})
	releaseCancel := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseCancel) })
	}
	t.Cleanup(release)
	installConsumerConstructionHook(t, func(candidate *consumer, phase consumerConstructionPhase) {
		if phase != consumerConstructionStarted {
			return
		}
		candidate.cancelHook = func() {
			close(cancelStarted)
			<-releaseCancel
		}
	})
	sdkConsumer, err := connection.Consumer(context.Background(), driver.ConsumerConfig{
		Destinations: []string{queue},
		Prefetch:     1,
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() {
		release()
		_ = sdkConsumer.Release(context.Background())
	})

	done := make(chan struct{})
	close(done)
	stopCtx := &failedDrainContext{
		Context: context.Background(),
		done:    done,
	}
	stopErr := sdkConsumer.Stop(stopCtx)
	if stopErr == nil {
		t.Fatal("Stop with expiring drain deadline = nil, want error")
	}
	if !errors.Is(stopErr, driver.ErrDrainTimeout) || !errors.Is(stopErr, context.DeadlineExceeded) {
		t.Fatalf("Stop with expiring drain deadline = %v, want drain timeout and deadline", stopErr)
	}
	waitForConsumerSignal(t, cancelStarted, "in-flight channel cancellation")

	releaseDone := make(chan error, 1)
	go func() { releaseDone <- sdkConsumer.Release(context.Background()) }()
	select {
	case err := <-releaseDone:
		if err != nil {
			t.Fatalf("Release: %v", err)
		}
	case <-time.After(2 * time.Second): //nolint:forbidigo // bound Release while cancellation is held
		t.Fatal("Release did not return while cancellation was held")
	}
	release()
}

type failedDrainContext struct {
	context.Context
	done     <-chan struct{}
	errCalls atomic.Int32
}

func (c *failedDrainContext) Done() <-chan struct{} {
	return c.done
}

func (c *failedDrainContext) Err() error {
	if c.errCalls.Add(1) <= 2 {
		return nil
	}
	return context.DeadlineExceeded
}

func TestNewConsumerRollsBackAfterLateFailure(t *testing.T) {
	queue := consumerAdmissionQueue(t)
	connection := openConsumerAdmissionConn(t, queue)
	maintenance, ok := connection.Admin().(driver.Maintenance)
	if !ok {
		t.Fatal("Admin does not implement Maintenance")
	}
	if _, err := maintenance.Purge(context.Background(), queue); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	missing := queue + ".missing"
	_, _ = maintenance.Prune(context.Background(), []string{missing})

	laneReady := make(chan struct{})
	builtC := make(chan *consumer, 1)
	releaseConstruction := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseConstruction) }) }
	var blockedSends atomic.Int32
	installConsumerConstructionHook(t, func(built *consumer, phase consumerConstructionPhase) {
		if phase == consumerConstructionStarted {
			built.readerSendHook = func(candidate *lane) {
				if len(candidate.pending) == cap(candidate.pending) {
					blockedSends.Add(1)
				}
			}
			return
		}
		if phase != consumerConstructionLaneReady {
			return
		}
		builtC <- built
		close(laneReady)
		<-releaseConstruction
	})

	resultC := make(chan consumerResult, 1)
	t.Cleanup(func() {
		release()
		stopIfPresent(resultC)
	})
	go func() {
		sdkConsumer, err := connection.Consumer(context.Background(), driver.ConsumerConfig{
			Destinations: []string{queue, missing},
			Prefetch:     2,
		})
		resultC <- consumerResult{consumer: sdkConsumer, err: err}
	}()
	waitForConsumerSignal(t, laneReady, "first lane construction")
	built := <-builtC
	firstLane := built.lanes[0]
	firstLane.mu.Lock()
	firstLane.paused = true
	firstLane.mu.Unlock()
	for range cap(built.messages) {
		built.messages <- driver.InboundMessage{}
	}
	firstLane.pending <- amqp.Delivery{DeliveryTag: 1001}
	waitForwarderState(t, "paused first-lane forwarder", func() bool {
		firstLane.mu.Lock()
		defer firstLane.mu.Unlock()
		return firstLane.emitting == 1
	})
	firstLane.pending <- amqp.Delivery{DeliveryTag: 1002}

	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("raw broker connection: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("raw channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })
	if err := rawChannel.PublishWithContext(context.Background(), "", queue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte("rollback"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	waitForwarderState(t, "blocked first-lane reader", func() bool {
		return blockedSends.Load() == 1 && len(firstLane.pending) == cap(firstLane.pending)
	})
	release()

	result := waitConsumerResult(t, resultC, "late construction failure")
	if result.consumer != nil {
		t.Fatal("Consumer returned a consumer after late construction failure")
	}
	if result.err == nil {
		t.Fatal("Consumer succeeded with a missing destination")
	}
	waitConsumerWaitGroupDone(t, &built.readers, "reader rollback")
	waitConsumerWaitGroupDone(t, &built.forward, "forwarder rollback")
	waitConsumerWaitGroupDone(t, &built.events, "close watcher rollback")
	if !firstLane.channel.IsClosed() {
		t.Fatal("first lane channel remained open after construction rollback")
	}
	built.mu.Lock()
	stopped := built.stopped
	built.mu.Unlock()
	if !stopped {
		t.Fatal("failed consumer was not marked stopped")
	}
}

func TestNewConsumerRejectsDuplicateDestinationWithoutLeaking(t *testing.T) {
	queue := consumerAdmissionQueue(t)
	connection := openConsumerAdmissionConn(t, queue)
	var hookCalls atomic.Int32
	installConsumerConstructionHook(t, func(_ *consumer, _ consumerConstructionPhase) {
		hookCalls.Add(1)
	})
	beforeSequence := consumerSequence.Load()
	sdkConsumer, err := connection.Consumer(context.Background(), driver.ConsumerConfig{
		Destinations: []string{queue, queue},
	})
	if sdkConsumer != nil {
		t.Fatal("Consumer returned a consumer for duplicate destinations")
	}
	if err == nil {
		t.Fatal("Consumer accepted duplicate destinations")
	}
	if hookCalls.Load() != 0 {
		t.Fatalf("construction hook calls = %d, want 0", hookCalls.Load())
	}
	if afterSequence := consumerSequence.Load(); afterSequence != beforeSequence {
		t.Fatalf("consumer sequence changed from %d to %d on duplicate rejection", beforeSequence, afterSequence)
	}
	connection.mu.RLock()
	active := len(connection.active)
	connection.mu.RUnlock()
	if active != 0 {
		t.Fatalf("active consumers after duplicate rejection = %d, want 0", active)
	}
	if err := connection.Ping(context.Background()); err != nil {
		t.Fatalf("Ping after duplicate rejection: %v", err)
	}
}

// TestConsumerReestablishesAfterServerCancel drives the broker into cancelling
// a consumer and proves the driver recovers instead of going quiet.
//
// The queue is declared here with a one-second x-consumer-timeout rather than
// through the driver, so the cancel is the broker's own decision and not an
// artifact of this driver's declaration. That also makes the test fail for the
// right reason when the cancel handling is removed: the deliveries channel
// closes, nothing is reported, and the lane never delivers again, which is
// exactly the silent stall this change exists to remove.
func TestConsumerReestablishesAfterServerCancel(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const queue = "rabbitmq-driver-consumer-server-cancel"
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("raw broker connection: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("raw channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })
	_, _ = rawChannel.QueueDelete(queue, false, false, false)
	t.Cleanup(func() { _, _ = rawChannel.QueueDelete(queue, false, false, false) })
	if _, err := rawChannel.QueueDeclare(queue, true, false, false, false, amqp.Table{
		"x-queue-type":       "quorum",
		"x-consumer-timeout": int32(1000),
	}); err != nil {
		t.Fatalf("QueueDeclare(%q): %v", queue, err)
	}

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	sdkConsumer, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{queue}, Prefetch: 1})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	// Release rather than Stop: the delivery held below is never settled, and
	// its ack deadline is the setup, not a leak.
	t.Cleanup(func() { _ = sdkConsumer.Release(context.Background()) })

	publish := func(body string) {
		t.Helper()
		if err := rawChannel.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         []byte(body),
		}); err != nil {
			t.Fatalf("Publish(%q): %v", body, err)
		}
	}

	// Positive control: the lane delivers before any cancel.
	publish("held")
	firstCtx, firstCancel := context.WithTimeout(ctx, 5*time.Second)
	defer firstCancel()
	select {
	case message := <-sdkConsumer.Messages():
		if message.Destination != queue || string(message.Body) != "held" {
			t.Fatalf("first delivery = %q/%q, want the held message", message.Destination, message.Body)
		}
		// Deliberately unsettled: the broker cancels a consumer that holds a
		// delivery past x-consumer-timeout.
	case <-firstCtx.Done():
		t.Fatal("timed out waiting for the first delivery")
	}

	// The cancel lands about a second after that delivery. It must be reported
	// as a notification and must not be reported as a failure: a cancel that
	// failed the runner would be a worse defect than the stall being fixed.
	reportCtx, reportCancel := context.WithTimeout(ctx, 15*time.Second)
	defer reportCancel()
	select {
	case err := <-sdkConsumer.Errors():
		kind, classified := driver.Classify(err)
		if !classified || kind != driver.KindNotification {
			t.Fatalf("consumer error after the broker cancel = %v (kind %v, classified %t), want a notification", err, kind, classified)
		}
		if !strings.Contains(err.Error(), queue) {
			t.Fatalf("cancel notification = %q, want it to name destination %q", err, queue)
		}
	case <-reportCtx.Done():
		t.Fatal("timed out waiting for the cancel notification")
	}

	// The re-established consumer must deliver again. The held message was
	// requeued when the broker cancelled its consumer, so it can arrive first.
	publish("after")
	afterCtx, afterCancel := context.WithTimeout(ctx, 15*time.Second)
	defer afterCancel()
	for {
		select {
		case message := <-sdkConsumer.Messages():
			if err := message.Settle.Ack(ctx); err != nil {
				t.Fatalf("Ack(%q): %v", message.Body, err)
			}
			if string(message.Body) == "after" {
				return
			}
		case <-afterCtx.Done():
			t.Fatal("timed out waiting for a delivery after the broker cancel")
		}
	}
}

// TestConsumerLaneEndsWhenChannelDies proves a lane still finishes when its
// channel dies outside a server cancel. A cancel is the one close that earns a
// replacement; a dead channel ends the lane as it always did, and a reader that
// waited for a replacement instead would park there until the consumer was torn
// down.
func TestConsumerLaneEndsWhenChannelDies(t *testing.T) {
	queue := consumerAdmissionQueue(t)
	connection := openConsumerAdmissionConn(t, queue)
	sdkConsumer, err := connection.Consumer(context.Background(), driver.ConsumerConfig{
		Destinations: []string{queue},
		Prefetch:     1,
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	built := sdkConsumer.(*consumer)
	t.Cleanup(func() { _ = sdkConsumer.Release(context.Background()) })

	if err := built.lanes[0].channel.Close(); err != nil {
		t.Fatalf("close lane channel: %v", err)
	}
	waitConsumerWaitGroupDone(t, &built.readers, "reader after channel death")
	waitConsumerWaitGroupDone(t, &built.forward, "forwarder after channel death")
}

func waitConsumerWaitGroupDone(t *testing.T, group *sync.WaitGroup, what string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second): //nolint:forbidigo // bound construction rollback assertions
		t.Fatalf("%s did not finish", what)
	}
}
