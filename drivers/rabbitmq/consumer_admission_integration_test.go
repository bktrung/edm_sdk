//go:build integration

package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type consumerResult struct {
	consumer driver.Consumer
	err      error
}

func installConsumerConstructionHook(t *testing.T, hook func(*consumer, consumerConstructionPhase)) {
	t.Helper()
	previous := consumerConstructionHook
	consumerConstructionHook = hook
	t.Cleanup(func() { consumerConstructionHook = previous })
}

func consumerAdmissionQueue(t *testing.T) string {
	t.Helper()
	return "f1.test.consumer-admission." + strings.ReplaceAll(t.Name(), "/", ".")
}

func openConsumerAdmissionConn(t *testing.T, queue string) *conn {
	t.Helper()
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	public, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	connection := public.(*conn)
	if _, err := connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
	}); err != nil {
		_ = connection.Close(context.Background())
		t.Fatalf("EnsureTopology: %v", err)
	}
	t.Cleanup(func() { cleanupConsumerAdmissionConn(connection, queue) })
	return connection
}

func cleanupConsumerAdmissionConn(connection *conn, queue string) {
	connection.mu.RLock()
	closed := connection.closed
	connection.mu.RUnlock()
	if closed {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		public, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
		if err == nil {
			fresh := public.(*conn)
			freshAdmin, ok := fresh.Admin().(driver.Maintenance)
			if ok {
				_, _ = freshAdmin.Prune(ctx, []string{queue})
			}
			_ = fresh.Close(context.Background())
		}
		cancel()
	}
	if maintenance, ok := connection.Admin().(driver.Maintenance); ok {
		_, _ = maintenance.Prune(context.Background(), []string{queue})
	}
	_ = connection.Close(context.Background())
}

func waitConsumerResult(t *testing.T, done <-chan consumerResult, what string) consumerResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(3 * time.Second): //nolint:forbidigo // bounded admission-test wait
		t.Fatalf("timed out waiting for %s", what)
		return consumerResult{}
	}
}

func waitForConsumerSignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second): //nolint:forbidigo // bounded admission-test wait
		t.Fatalf("timed out waiting for %s", what)
	}
}

func assertAdmissionReturns(t *testing.T, done <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second): //nolint:forbidigo // bounded admission-test wait
		t.Fatalf("%s did not progress while consumer construction was blocked", what)
		return nil
	}
}

func stopIfPresent(done <-chan consumerResult) {
	select {
	case result := <-done:
		if result.consumer != nil {
			_ = result.consumer.Stop(context.Background())
		}
	default:
	}
}

func TestConsumerConstructionDoesNotHoldConnectionLock(t *testing.T) {
	queue := consumerAdmissionQueue(t)
	connection := openConsumerAdmissionConn(t, queue)
	setupStarted := make(chan struct{})
	setupRelease := make(chan struct{})
	var setupCalls atomic.Int32
	installConsumerConstructionHook(t, func(_ *consumer, phase consumerConstructionPhase) {
		if phase != consumerConstructionStarted || setupCalls.Add(1) != 1 {
			return
		}
		close(setupStarted)
		<-setupRelease
	})

	firstDone := make(chan consumerResult, 1)
	secondDone := make(chan consumerResult, 1)
	var releaseFirstOnce sync.Once
	releaseFirst := func() { releaseFirstOnce.Do(func() { close(setupRelease) }) }
	t.Cleanup(func() {
		releaseFirst()
		stopIfPresent(firstDone)
		stopIfPresent(secondDone)
		cleanupConsumerAdmissionConn(connection, queue)
	})

	go func() {
		consumer, err := connection.Consumer(context.Background(), driver.ConsumerConfig{Destinations: []string{queue}})
		firstDone <- consumerResult{consumer: consumer, err: err}
	}()
	waitForConsumerSignal(t, setupStarted, "first consumer setup")

	pingDone := make(chan error, 1)
	go func() { pingDone <- connection.Ping(context.Background()) }()
	if err := assertAdmissionReturns(t, pingDone, "Ping"); err != nil {
		t.Fatalf("Ping() = %v, want nil", err)
	}

	producerDone := make(chan error, 1)
	go func() {
		producer, err := connection.Producer(context.Background(), driver.ProducerConfig{})
		if err == nil {
			err = producer.Close(context.Background())
		}
		producerDone <- err
	}()
	if err := assertAdmissionReturns(t, producerDone, "Producer admission"); err != nil {
		t.Fatalf("Producer admission = %v, want nil", err)
	}

	go func() {
		consumer, err := connection.Consumer(context.Background(), driver.ConsumerConfig{Destinations: []string{queue}})
		secondDone <- consumerResult{consumer: consumer, err: err}
	}()
	second := waitConsumerResult(t, secondDone, "second consumer admission")
	if second.err != nil {
		t.Fatalf("second Consumer() = %v, want nil", second.err)
	}
	if err := connection.Close(context.Background()); !errors.Is(err, driver.ErrResourcesOutstanding) {
		t.Fatalf("Close() with second consumer = %v, want ErrResourcesOutstanding", err)
	}
	if err := second.consumer.Stop(context.Background()); err != nil {
		t.Fatalf("second consumer Stop = %v", err)
	}

	releaseFirst()
	first := waitConsumerResult(t, firstDone, "first consumer admission")
	if first.err != nil {
		t.Fatalf("first Consumer() = %v, want nil", first.err)
	}
	if err := first.consumer.Stop(context.Background()); err != nil {
		t.Fatalf("first consumer Stop = %v", err)
	}
}

func TestConcurrentConflictingExclusiveConsumersInstallOne(t *testing.T) {
	runConsumerConflictTest(t, true, false, "exclusive consumer refused: destination %q already has a consumer")
}

func TestConcurrentNonexclusiveAndExclusiveConsumersRemainSymmetric(t *testing.T) {
	t.Run("exclusive existing and nonexclusive new", func(t *testing.T) {
		runConsumerConflictTest(t, true, false, "exclusive consumer refused: destination %q already has a consumer")
	})
	t.Run("nonexclusive existing and exclusive new", func(t *testing.T) {
		runConsumerConflictTest(t, false, true, "exclusive consumer refused: destination %q already has an exclusive consumer")
	})
}

func runConsumerConflictTest(t *testing.T, firstExclusive, secondExclusive bool, want string) {
	t.Helper()
	queue := consumerAdmissionQueue(t)
	connection := openConsumerAdmissionConn(t, queue)
	firstReady := make(chan struct{})
	firstBuilt := make(chan *consumer, 1)
	firstRelease := make(chan struct{})
	var setupCalls atomic.Int32
	installConsumerConstructionHook(t, func(built *consumer, phase consumerConstructionPhase) {
		if phase != consumerConstructionReady || setupCalls.Add(1) != 1 {
			return
		}
		firstBuilt <- built
		close(firstReady)
		<-firstRelease
	})

	firstDone := make(chan consumerResult, 1)
	secondDone := make(chan consumerResult, 1)
	var releaseFirstOnce sync.Once
	releaseFirst := func() { releaseFirstOnce.Do(func() { close(firstRelease) }) }
	t.Cleanup(func() {
		releaseFirst()
		stopIfPresent(firstDone)
		stopIfPresent(secondDone)
		cleanupConsumerAdmissionConn(connection, queue)
	})

	go func() {
		consumer, err := connection.Consumer(context.Background(), driver.ConsumerConfig{
			Destinations: []string{queue},
			Exclusive:    firstExclusive,
		})
		firstDone <- consumerResult{consumer: consumer, err: err}
	}()
	waitForConsumerSignal(t, firstReady, "first conflicting consumer setup")
	builtFirst := <-firstBuilt
	if len(builtFirst.lanes) != 1 {
		t.Fatalf("first built lanes = %d, want 1", len(builtFirst.lanes))
	}
	_ = builtFirst.lanes[0].channel.Close()

	go func() {
		consumer, err := connection.Consumer(context.Background(), driver.ConsumerConfig{
			Destinations: []string{queue},
			Exclusive:    secondExclusive,
		})
		secondDone <- consumerResult{consumer: consumer, err: err}
	}()
	second := waitConsumerResult(t, secondDone, "second conflicting consumer")
	if second.err != nil {
		t.Fatalf("second Consumer() = %v, want winner: %v", second.err, want)
	}

	releaseFirst()
	first := waitConsumerResult(t, firstDone, "first conflicting consumer")
	if first.consumer != nil {
		t.Fatalf("first Consumer() returned a consumer after losing conflict")
	}
	if first.err == nil || !strings.Contains(first.err.Error(), fmt.Sprintf(want, queue)) {
		t.Fatalf("first Consumer() = %v, want %q", first.err, fmt.Sprintf(want, queue))
	}
	if kind, ok := driver.Classify(first.err); !ok || kind != driver.KindFatal {
		t.Fatalf("first Consumer() classification = %v, %t, want fatal, true", kind, ok)
	}

	connection.mu.RLock()
	active := len(connection.active)
	connection.mu.RUnlock()
	if active != 1 {
		t.Fatalf("active consumers after conflict = %d, want 1", active)
	}
	winner := second.consumer.(*consumer)
	if len(winner.lanes) != 1 || winner.lanes[0].channel.IsClosed() {
		t.Fatalf("winning consumer lane state = lanes %d closed %v, want one open lane", len(winner.lanes), winner.lanes[0].channel.IsClosed())
	}
	if err := second.consumer.Stop(context.Background()); err != nil {
		t.Fatalf("winning consumer Stop = %v", err)
	}
	if !winner.lanes[0].channel.IsClosed() {
		t.Fatal("winning consumer lane stayed open after Stop")
	}
}

func TestConsumerCloseWinsBeforeInstallAndCleansBuiltLanes(t *testing.T) {
	queue := consumerAdmissionQueue(t)
	connection := openConsumerAdmissionConn(t, queue)
	ready := make(chan struct{})
	builtC := make(chan *consumer, 1)
	installRelease := make(chan struct{})
	var readyOnce sync.Once
	installConsumerConstructionHook(t, func(built *consumer, phase consumerConstructionPhase) {
		if phase != consumerConstructionReady {
			return
		}
		builtC <- built
		readyOnce.Do(func() { close(ready) })
		<-installRelease
	})

	consumerDone := make(chan consumerResult, 1)
	closeDone := make(chan error, 1)
	var releaseInstallOnce sync.Once
	releaseInstall := func() { releaseInstallOnce.Do(func() { close(installRelease) }) }
	t.Cleanup(func() {
		releaseInstall()
		stopIfPresent(consumerDone)
		cleanupConsumerAdmissionConn(connection, queue)
	})

	go func() {
		consumer, err := connection.Consumer(context.Background(), driver.ConsumerConfig{Destinations: []string{queue}})
		consumerDone <- consumerResult{consumer: consumer, err: err}
	}()
	waitForConsumerSignal(t, ready, "built consumer before install")
	built := <-builtC

	go func() { closeDone <- connection.Close(context.Background()) }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() while built consumer waits for install = %v", err)
		}
	case <-time.After(2 * time.Second): //nolint:forbidigo // bounded admission-test wait
		t.Fatal("Close() did not progress while the built consumer waited for install")
	}

	releaseInstall()
	result := waitConsumerResult(t, consumerDone, "rejected built consumer")
	if result.consumer != nil {
		t.Fatal("Consumer() returned a built consumer after Close won")
	}
	if result.err == nil {
		t.Fatal("Consumer() succeeded after Close won")
	}
	connection.mu.RLock()
	active := len(connection.active)
	connection.mu.RUnlock()
	if active != 0 {
		t.Fatalf("active consumers after close-winning construction = %d, want 0", active)
	}
	if len(built.lanes) != 1 || !built.lanes[0].channel.IsClosed() {
		t.Fatalf("rejected consumer lane state = lanes %d closed %v, want one closed lane", len(built.lanes), built.lanes[0].channel.IsClosed())
	}
}
