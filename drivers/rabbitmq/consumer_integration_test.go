//go:build integration

package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
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
	t.Cleanup(func() {
		management, err := newManagementClient(defaultEndpoint, driver.Config{})
		if err != nil {
			t.Errorf("cleanup %q: newManagementClient: %v", queue, err)
			return
		}
		if _, err := management.deleteQueue(context.Background(), queue); err != nil {
			t.Errorf("cleanup %q: delete queue: %v", queue, err)
		}
	})

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
		consumerLane.pending <- laneDelivery{Delivery: amqp.Delivery{DeliveryTag: uint64(deliveryTag + 1)}, generation: 1}
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
	results, err := maintenance.Prune(context.Background(), []string{missing})
	if err != nil {
		t.Logf("cleanup %q: prune: %v", missing, err)
	}
	for _, result := range results {
		if !result.Deleted {
			t.Logf("cleanup %q: prune kept %q: %s", missing, result.Name, result.Reason)
		}
	}

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
	firstLane.pending <- laneDelivery{Delivery: amqp.Delivery{DeliveryTag: 1001}, generation: 1}
	waitForwarderState(t, "paused first-lane forwarder", func() bool {
		firstLane.mu.Lock()
		defer firstLane.mu.Unlock()
		return firstLane.emitting == 1
	})
	firstLane.pending <- laneDelivery{Delivery: amqp.Delivery{DeliveryTag: 1002}, generation: 1}

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

	// RabbitMQ 4.3.4's quorum-queue source schedules an absolute
	// consumer-timeout deadline. Twenty runs under two concurrent test-fast
	// suites measured 1.000-1.011s (median 1.005s); 10x the worst is below
	// the retained 15s floor, so the finite wait remains 15s.
	cancelStarted := time.Now() //nolint:forbidigo // the broker notification latency is the measurement under test.
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
		t.Fatalf("timed out waiting for the cancel notification %s after delivery", time.Since(cancelStarted)) //nolint:forbidigo // the broker notification latency is the measurement under test.
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

// rabbitRawChannel opens the broker directly for a test that deletes or
// declares its own queues outside the driver under test.
func rabbitRawChannel(t *testing.T) *amqp.Channel {
	t.Helper()
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("raw broker connection: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	channel, err := raw.Channel()
	if err != nil {
		t.Fatalf("raw channel: %v", err)
	}
	t.Cleanup(func() { _ = channel.Close() })
	return channel
}

// rabbitActiveConsumer returns the one consumer the driver currently has open,
// which is how a test reads the physical destinations the core resolved.
func rabbitActiveConsumer(t *testing.T, capture *captureRabbitDriver) *consumer {
	t.Helper()
	for _, opened := range capture.connections() {
		connection, ok := opened.(*conn)
		if !ok {
			continue
		}
		if live := activeRabbitConsumer(connection); live != nil {
			return live
		}
	}
	t.Fatal("no active RabbitMQ consumer")
	return nil
}

// rabbitLaneFor returns the consumer lane whose destination names topic, so a
// test can delete exactly the queue the core resolved for that topic and order
// the deletion against the lane's reattach.
func rabbitLaneFor(t *testing.T, live *consumer, topic string) *lane {
	t.Helper()
	destinations := make([]string, 0, len(live.lanes))
	for _, lane := range live.lanes {
		if strings.Contains(lane.destination, topic) {
			return lane
		}
		destinations = append(destinations, lane.destination)
	}
	t.Fatalf("no consumer lane names topic %q; destinations = %v", topic, destinations)
	return nil
}

// TestConsumerReattachFailureIsAGenerationEndingSignal proves what a lane reports
// when its replacement consumer cannot attach: a transient error, which is the
// one consumer signal the core turns into a rebuild, carrying the broker's own
// not-found diagnosis. The lane's channel lock is held across the queue delete
// so the reattach can only run against a queue that is already gone.
//
// T1 of the task's three proofs; the core's rebuild and its failure outcome are
// proven end-to-end by the two core tests below.
func TestConsumerReattachFailureIsAGenerationEndingSignal(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const queue = "f1.test.rabbitmq-driver-reattach-signal"
	rawChannel := rabbitRawChannel(t)
	_, _ = rawChannel.QueueDelete(queue, false, false, false)
	t.Cleanup(func() { _, _ = rawChannel.QueueDelete(queue, false, false, false) })
	if _, err := rawChannel.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
		t.Fatalf("QueueDeclare: %v", err)
	}

	connection, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	sdkConsumer, err := connection.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{queue}, Prefetch: 1})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() {
		_ = sdkConsumer.Release(context.Background())
		_ = connection.Close(context.Background())
	})
	built := sdkConsumer.(*consumer)

	lane := built.lanes[0]
	lane.channelMu.Lock()
	if _, err := rawChannel.QueueDelete(queue, false, false, false); err != nil {
		lane.channelMu.Unlock()
		t.Fatalf("QueueDelete: %v", err)
	}
	// Deleting the queue makes the broker cancel the consumer, so the cancel
	// notification is reported while the reattach is still blocked.
	if err := receiveRabbitConsumerError(ctx, sdkConsumer); err != nil {
		kind, classified := driver.Classify(err)
		if !classified || kind != driver.KindNotification {
			lane.channelMu.Unlock()
			t.Fatalf("first report after the queue delete = %v (kind %v, classified %t), want the broker cancel notification", err, kind, classified)
		}
	} else {
		lane.channelMu.Unlock()
		t.Fatal("no consumer error after the queue delete")
	}
	lane.channelMu.Unlock()

	// The failed reattach and the channel close it causes are both reported, in
	// an order the two watchers do not fix, so scan the stream for the reattach
	// report itself.
	scanCtx, scanCancel := context.WithTimeout(ctx, 15*time.Second)
	defer scanCancel()
	var failure error
	for failure == nil {
		err := receiveRabbitConsumerError(scanCtx, sdkConsumer)
		if err == nil {
			t.Fatal("no reattach failure reported")
		}
		if strings.Contains(err.Error(), "re-establishing destination") {
			failure = err
		}
	}
	kind, classified := driver.Classify(failure)
	if !classified || kind != driver.KindTransient {
		t.Fatalf("reattach failure = %v (kind %v, classified %t), want a classified transient error", failure, kind, classified)
	}
	if !errors.Is(failure, driver.ErrDestinationMissing) {
		t.Fatalf("reattach failure = %v, want it to reach ErrDestinationMissing", failure)
	}
	var amqpErr *amqp.Error
	if !errors.As(failure, &amqpErr) || amqpErr.Code != 404 || !strings.Contains(amqpErr.Reason, "no queue") {
		t.Fatalf("reattach failure = %v, want the broker's 404 for the missing queue reachable", failure)
	}
	if !strings.Contains(failure.Error(), queue) || !strings.Contains(failure.Error(), amqpErr.Reason) {
		t.Fatalf("reattach failure = %v, want the destination %q and the broker reason %q in the text", failure, queue, amqpErr.Reason)
	}
	// The port keeps Messages open across a lane failure; the core, not the
	// driver, decides what happens next.
	select {
	case message, ok := <-sdkConsumer.Messages():
		if ok {
			t.Fatalf("delivery %q after a failed reattach, want none", message.Body)
		}
	default:
	}

	// The reopen the core performs must work once the destination is back.
	if _, err := rawChannel.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
		t.Fatalf("QueueDeclare(recreate): %v", err)
	}
	reopened, err := connection.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{queue}, Prefetch: 1})
	if err != nil {
		t.Fatalf("Consumer after recreating %q: %v", queue, err)
	}
	t.Cleanup(func() { _ = reopened.Release(context.Background()) })
	if err := rawChannel.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte("after-reattach"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case message := <-reopened.Messages():
		if string(message.Body) != "after-reattach" {
			t.Fatalf("reopened delivery = %q, want the published message", message.Body)
		}
	case <-time.After(10 * time.Second): //nolint:forbidigo // live broker wait
		t.Fatal("reopened consumer delivered nothing")
	}
}

func receiveRabbitConsumerError(ctx context.Context, sdkConsumer driver.Consumer) error {
	select {
	case err, ok := <-sdkConsumer.Errors():
		if !ok {
			return nil
		}
		return err
	case <-ctx.Done():
		return nil
	}
}

// TestRabbitMQCoreRebuildsTheConsumerWhenAReattachedDestinationReturns proves the
// first core outcome: a lane that ends on a failed reattach ends its generation,
// the runner rebuilds the consumer, and the subscription consumes again from that
// destination once the rebuild's topology declaration has recreated it, with the
// sibling lane resumed on the same consumer.
func TestRabbitMQCoreRebuildsTheConsumerWhenAReattachedDestinationReturns(t *testing.T) {
	requireBroker(t)
	sequence := rabbitReconnectSequence.Add(1)
	deleted := fmt.Sprintf("reattach.return.%d.%d", os.Getpid(), sequence)
	sibling := fmt.Sprintf("reattach.sibling.%d.%d", os.Getpid(), sequence)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := f1.Config{
		Env:        "test",
		Service:    "reattach",
		InstanceID: fmt.Sprintf("%d", sequence),
		Broker: f1.BrokerConfig{
			Driver:               "rabbitmq",
			Endpoints:            []string{defaultEndpoint},
			ConnectTimeout:       5 * time.Second,
			MaxReconnectAttempts: 3,
			DefaultPrefetch:      1,
		},
		Topology: f1.TopologyConfig{AutoCreate: true},
		Lifecycle: f1.LifecycleConfig{
			DrainTimeout: 5 * time.Second,
			HandlerGrace: 500 * time.Millisecond,
			CloseTimeout: 5 * time.Second,
		},
	}
	driverCapture := &captureRabbitDriver{base: Driver{}}
	client, err := f1.New(context.Background(), cfg,
		f1.WithDriver(driverCapture),
		f1.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close(context.Background())
		cleanupRabbitTestQueues(t, driverCapture, "f1.test."+deleted, "f1.test."+sibling)
	})

	var handledDeleted, handledSibling atomic.Int32
	runner, err := client.Subscribe(context.Background(), f1.Subscription{
		Name:           "reattach-return",
		Topics:         []string{deleted, sibling},
		Prefetch:       1,
		HandlerTimeout: 2 * time.Second,
		Handlers: map[string]f1.Handler{
			deleted: f1.HandlerFunc(func(context.Context, *f1.Event) error {
				handledDeleted.Add(1)
				return nil
			}),
			sibling: f1.HandlerFunc(func(context.Context, *f1.Event) error {
				handledSibling.Add(1)
				return nil
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(context.Background()) }()
	publish := func(topic string) {
		t.Helper()
		if _, err := client.Publisher().Publish(ctx, topic, map[string]string{"source": "reattach"}); err != nil {
			t.Fatalf("Publish(%q): %v", topic, err)
		}
	}

	// Positive control: both lanes are attached and consuming before the delete.
	live := waitRabbitActiveConsumer(t, driverCapture)
	publish(deleted)
	publish(sibling)
	waitRabbit(t, func() bool { return handledDeleted.Load() == 1 && handledSibling.Load() == 1 })

	deletedLane := rabbitLaneFor(t, live, deleted)
	rawChannel := rabbitRawChannel(t)
	// Hold the lane's channel lock across the delete so the reattach can only run
	// against a finished deletion. Released while the delete is in flight, the
	// reattach races it and the broker reports the teardown on the lane's channel
	// instead, a failure the core already recovers from, which would leave this
	// test proving nothing about the reattach report.
	deletedLane.channelMu.Lock()
	if _, err := rawChannel.QueueDelete(deletedLane.destination, false, false, false); err != nil {
		deletedLane.channelMu.Unlock()
		t.Fatalf("QueueDelete(%q): %v", deletedLane.destination, err)
	}
	deletedLane.channelMu.Unlock()

	// The reattach fails with the queue gone, the generation ends, and the
	// rebuild replaces the consumer without replacing the connection.
	waitRabbitConsumerReplacement(t, driverCapture, live)
	if got := len(driverCapture.connections()); got != 1 {
		t.Fatalf("driver Open count = %d, want 1: the repair must rebuild the consumer, not the connection", got)
	}

	publish(deleted)
	publish(sibling)
	waitRabbit(t, func() bool { return handledDeleted.Load() == 2 && handledSibling.Load() == 2 })
	if err := client.Health(ctx); err != nil {
		t.Fatalf("Health after the consumer repair = %v, want a healthy client", err)
	}
	// The subscription stayed up through the repair rather than ending.
	select {
	case err := <-runDone:
		t.Fatalf("runner Run() = %v, want it still running after the consumer repair", err)
	default:
	}
}

// TestRabbitMQCoreFailsTheSubscriptionWhenTheReattachedDestinationStaysGone
// proves the second core outcome: with the destination still missing, the rebuild
// reaches the broker and the subscription fails with the broker's own not-found
// text, which Health reports.
//
// The client runs with TopologyNone so the rebuild's own topology pass declares
// nothing and the reopen reaches the broker; under Declare the rebuild recreates
// the destination instead (the test above), and under Verify it fails at the
// declaration with the driver's own message rather than the broker's.
func TestRabbitMQCoreFailsTheSubscriptionWhenTheReattachedDestinationStaysGone(t *testing.T) {
	requireBroker(t)
	sequence := rabbitReconnectSequence.Add(1)
	topic := fmt.Sprintf("reattach.gone.%d.%d", os.Getpid(), sequence)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const subscriptionName = "reattach-gone"

	newConfig := func() f1.Config {
		return f1.Config{
			Env:        "test",
			Service:    "reattach",
			InstanceID: fmt.Sprintf("%d", sequence),
			Broker: f1.BrokerConfig{
				Driver:               "rabbitmq",
				Endpoints:            []string{defaultEndpoint},
				ConnectTimeout:       5 * time.Second,
				MaxReconnectAttempts: 3,
				DefaultPrefetch:      1,
			},
			Lifecycle: f1.LifecycleConfig{
				DrainTimeout: 5 * time.Second,
				HandlerGrace: 500 * time.Millisecond,
				CloseTimeout: 5 * time.Second,
			},
		}
	}
	subscription := func() f1.Subscription {
		return f1.Subscription{
			Name:           subscriptionName,
			Topics:         []string{topic},
			Prefetch:       1,
			HandlerTimeout: 2 * time.Second,
			Handlers: map[string]f1.Handler{
				topic: f1.HandlerFunc(func(context.Context, *f1.Event) error { return nil }),
			},
		}
	}

	// A declaring client first, only to create the topology and report the
	// physical destination the core resolves for the topic.
	declaringCapture := &captureRabbitDriver{base: Driver{}}
	declaringConfig := newConfig()
	declaringConfig.Topology = f1.TopologyConfig{AutoCreate: true}
	declaringClient, err := f1.New(context.Background(), declaringConfig,
		f1.WithDriver(declaringCapture),
		f1.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = declaringClient.Close(context.Background())
		cleanupRabbitTestQueues(t, declaringCapture, "f1.test."+topic)
	})
	declaringRunner, err := declaringClient.Subscribe(context.Background(), subscription())
	if err != nil {
		t.Fatal(err)
	}
	declaringDone := make(chan error, 1)
	go func() { declaringDone <- declaringRunner.Run(context.Background()) }()
	waitRabbit(t, func() bool { return rabbitActiveConsumerOrNil(declaringCapture) != nil })
	queue := rabbitLaneFor(t, rabbitActiveConsumer(t, declaringCapture), topic).destination
	if err := declaringClient.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-declaringDone

	// The tested client declares nothing, so a destination that stays gone
	// reaches the broker and decides the subscription's fate.
	driverCapture := &captureRabbitDriver{base: Driver{}}
	client, err := f1.New(context.Background(), newConfig(),
		f1.WithDriver(driverCapture),
		f1.WithTopology(driver.TopologyNone),
		f1.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	runner, err := client.Subscribe(ctx, subscription())
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(context.Background()) }()
	live := waitRabbitActiveConsumer(t, driverCapture)
	goneLane := rabbitLaneFor(t, live, topic)

	rawChannel := rabbitRawChannel(t)
	// As in the return test, the delete is ordered against the reattach so the
	// lane's own report is what carries the deleted destination.
	goneLane.channelMu.Lock()
	if _, err := rawChannel.QueueDelete(queue, false, false, false); err != nil {
		goneLane.channelMu.Unlock()
		t.Fatalf("QueueDelete(%q): %v", queue, err)
	}
	goneLane.channelMu.Unlock()

	var runErr error
	select {
	case runErr = <-runDone:
	case <-time.After(30 * time.Second): //nolint:forbidigo // bound the rebuild that must fail a subscription
		t.Fatal("runner Run did not return after the destination disappeared")
	}
	if runErr == nil {
		t.Fatal("runner Run() = nil, want the reopen failure")
	}
	if kind, classified := driver.Classify(runErr); !classified || kind != driver.KindNotFound {
		t.Fatalf("runner Run() = %v (kind %v, classified %t), want a not-found error", runErr, kind, classified)
	}
	if !errors.Is(runErr, driver.ErrDestinationMissing) {
		t.Fatalf("runner Run() = %v, want it to reach ErrDestinationMissing", runErr)
	}
	var amqpErr *amqp.Error
	if !errors.As(runErr, &amqpErr) || amqpErr.Code != 404 || !strings.Contains(amqpErr.Reason, "no queue") {
		t.Fatalf("runner Run() = %v, want the broker's 404 for the missing queue reachable", runErr)
	}
	health := client.Health(ctx)
	if health == nil {
		t.Fatal("Health() = nil, want the failed subscription")
	}
	for _, want := range []string{subscriptionName, queue, amqpErr.Reason} {
		if !strings.Contains(health.Error(), want) {
			t.Fatalf("Health() = %v, want it to name %q", health, want)
		}
	}
}

// rabbitActiveConsumerOrNil reports the driver's active consumer without failing
// the caller, for a wait loop.
func rabbitActiveConsumerOrNil(capture *captureRabbitDriver) *consumer {
	for _, opened := range capture.connections() {
		connection, ok := opened.(*conn)
		if !ok {
			continue
		}
		if live := activeRabbitConsumer(connection); live != nil {
			return live
		}
	}
	return nil
}

// waitRabbitActiveConsumer waits for the driver to report one active consumer,
// which is the point the core has declared the subscription topology and opened
// its lanes.
func waitRabbitActiveConsumer(t *testing.T, capture *captureRabbitDriver) *consumer {
	t.Helper()
	waitRabbit(t, func() bool { return rabbitActiveConsumerOrNil(capture) != nil })
	return rabbitActiveConsumer(t, capture)
}

// TestReleaseRacingDrainCancellationAlwaysReturns proves that a Release whose
// channel close overlaps a Drain's basic.cancel on the same channel returns.
// AMQP does not correlate replies, so an unserialized Close could have its
// close-ok taken by the Cancel and then wait forever.
func TestReleaseRacingDrainCancellationAlwaysReturns(t *testing.T) {
	queue := consumerAdmissionQueue(t)
	connection := openConsumerAdmissionConn(t, queue)
	const rounds = 100
	for round := range rounds {
		sdkConsumer, err := connection.Consumer(context.Background(), driver.ConsumerConfig{
			Destinations: []string{queue},
			Prefetch:     1,
		})
		if err != nil {
			t.Fatalf("round %d: Consumer: %v", round, err)
		}
		drainDone := make(chan struct{})
		go func() {
			defer close(drainDone)
			_ = sdkConsumer.Drain(context.Background())
		}()
		releaseDone := make(chan error, 1)
		go func() { releaseDone <- sdkConsumer.Release(context.Background()) }()
		select {
		case err := <-releaseDone:
			if err != nil {
				t.Fatalf("round %d: Release: %v", round, err)
			}
		case <-time.After(5 * time.Second): //nolint:forbidigo // bound Release racing a drain cancellation
			t.Fatalf("round %d: Release did not return while a drain cancellation raced its channel close", round)
		}
		select {
		case <-drainDone:
		case <-time.After(5 * time.Second): //nolint:forbidigo // bound Drain after Release
			t.Fatalf("round %d: Drain did not return after Release", round)
		}
	}
}

// TestConsumerLaneCloseMeasurements records broker events separately from the
// driver's watchers so a cancel already processed by watchCancel is not missed.
func TestConsumerLaneCloseMeasurements(t *testing.T) {
	requireBroker(t)
	for _, candidate := range []string{"delete_queue", "delete_then_ack", "delete_then_qos", "revoke_read", "revoke_read_then_delete", "delete_user"} {
		t.Run(candidate, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			name := fmt.Sprintf("lane-measure.%d.%d", os.Getpid(), rabbitReconnectSequence.Add(1))
			guest, err := newManagementClient(defaultEndpoint, driver.Config{})
			if err != nil {
				t.Fatal(err)
			}
			userPath := "/api/users/" + name
			permissionPath := "/api/permissions/" + url.PathEscape(guest.vhost) + "/" + name
			t.Cleanup(func() {
				managementRequest(t, guest, http.MethodDelete, userPath, "")
				managementRequest(t, guest, http.MethodDelete, "/api/queues/"+url.PathEscape(guest.vhost)+"/"+name, "")
			})
			managementRequest(t, guest, http.MethodPut, userPath, `{"password":"lane-measure","tags":""}`)
			managementRequest(t, guest, http.MethodPut, permissionPath, `{"configure":".*","write":".*","read":".*"}`)
			raw := rabbitRawChannel(t)
			if _, err := raw.QueueDeclare(name, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
				t.Fatal(err)
			}
			endpoint, err := url.Parse(defaultEndpoint)
			if err != nil {
				t.Fatal(err)
			}
			endpoint.User = url.UserPassword(name, "lane-measure")
			connection, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{endpoint.String()}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = connection.Close(context.Background()) })
			sdkConsumer, err := connection.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{name}, Prefetch: 1})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = sdkConsumer.Release(context.Background()) })
			built := sdkConsumer.(*consumer)
			lane := built.lanes[0]
			cancels := lane.channel.NotifyCancel(make(chan string, 1))
			closes := lane.channel.NotifyClose(make(chan *amqp.Error, 1))
			publish := func(body string) driver.InboundMessage {
				t.Helper()
				if err := raw.PublishWithContext(ctx, "", name, false, false, amqp.Publishing{Body: []byte(body)}); err != nil {
					t.Fatal(err)
				}
				select {
				case message := <-sdkConsumer.Messages():
					if string(message.Body) != body {
						t.Fatalf("delivery = %q, want %q", message.Body, body)
					}
					return message
				case <-ctx.Done():
					t.Fatal("lane did not deliver")
					return driver.InboundMessage{}
				}
			}
			message := publish("before")
			// Block re-Consume until deletion and the optional settlement/Qos
			// have finished. Otherwise its 404 can obscure those operations.
			lane.channelMu.Lock()
			locked := true
			defer func() {
				if locked {
					lane.channelMu.Unlock()
				}
			}()
			if strings.HasPrefix(candidate, "revoke_read") {
				managementRequest(t, guest, http.MethodPut, permissionPath, `{"configure":".*","write":".*","read":"^$"}`)
			}
			if strings.HasPrefix(candidate, "delete_") && candidate != "delete_user" || candidate == "revoke_read_then_delete" {
				if _, err := raw.QueueDelete(name, false, false, false); err != nil {
					t.Fatal(err)
				}
			}
			switch candidate {
			case "delete_then_ack":
				if err := message.Settle.Ack(ctx); err != nil {
					t.Fatalf("Ack after deletion: %v", err)
				}
			case "delete_then_qos":
				if err := lane.channel.Qos(1, 0, false); err != nil {
					t.Fatalf("Qos after deletion: %v", err)
				}
			case "delete_user":
				managementRequest(t, guest, http.MethodDelete, userPath, "")
			case "revoke_read":
				if err := message.Settle.Ack(ctx); err != nil {
					t.Fatal(err)
				}
				if err := lane.channel.Qos(1, 0, false); err != nil {
					t.Fatal(err)
				}
				if err := publish("after").Settle.Ack(ctx); err != nil {
					t.Fatal(err)
				}
			}
			lane.channelMu.Unlock()
			locked = false
			if candidate == "revoke_read" {
				select {
				case event := <-closes:
					t.Fatalf("read revocation closed the channel: %v", event)
				case tag := <-cancels:
					t.Fatalf("read revocation cancelled the consumer: %q", tag)
				case <-time.After(300 * time.Millisecond): //nolint:forbidigo // bounded live-broker observation after positive delivery control
				}
				t.Log("cancel_first=false close=none lane=continues delivering after read revocation; Ack and Qos succeeded")
				return
			}
			var closeErr *amqp.Error
			select {
			case closeErr = <-closes:
			case <-ctx.Done():
				t.Fatal("no channel close")
			}
			if closeErr == nil {
				t.Fatal("channel closed without broker diagnostic")
			}
			// NotifyCancel is buffered before the action. amqp091 dispatches
			// it synchronously, so a value after NotifyClose proves the cancel
			// was received before shutdown, independent of watcher scheduling.
			tag, cancelled := <-cancels
			cancelled = cancelled && tag != ""
			wantCode := 404
			if candidate == "delete_user" {
				wantCode = 320
			}
			if closeErr.Code != wantCode || cancelled != (candidate != "delete_user") {
				t.Fatalf("cancel_first=%t close=%v, want cancel_first=%t code=%d", cancelled, closeErr, candidate != "delete_user", wantCode)
			}
			var recovery error
			for recovery == nil {
				report := receiveRabbitConsumerError(ctx, sdkConsumer)
				if report == nil {
					t.Fatal("no generation-ending lane report")
				}
				if kind, ok := driver.Classify(report); ok && kind == driver.KindTransient {
					recovery = report
				}
			}
			if !strings.Contains(recovery.Error(), closeErr.Reason) {
				t.Fatalf("recovery report %v lost broker text %q", recovery, closeErr.Reason)
			}
			t.Logf("cancel_first=%t close=%d text=%q lane=reports transient recovery: %v", cancelled, closeErr.Code, closeErr.Reason, recovery)
		})
	}
}
