package rabbitmq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func setupPruneTest(t *testing.T, kind queueKind, specs ...driver.DestinationSpec) (context.Context, *conn, *admin) {
	t.Helper()
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	management, err := newManagementClient(defaultEndpoint, driver.Config{})
	if err != nil {
		cancel()
		t.Fatalf("newManagementClient: %v", err)
	}
	names := make([]string, 0, len(specs)*(len(parkRungs)+2))
	for _, spec := range specs {
		names = append(names, spec.Name)
		if spec.Delay > 0 {
			names = append(names, parkQueueNames(spec.Name)...)
		}
	}
	for _, name := range names {
		if _, err := management.deleteQueue(ctx, name); err != nil {
			cancel()
			t.Fatalf("deleteQueue(%q): %v", name, err)
		}
	}

	rabbitConn, err := (Driver{}).Open(ctx, driver.Config{
		Endpoints: []string{defaultEndpoint},
		DriverOptions: map[string]string{
			"rabbitmq.queueType": string(kind),
		},
	})
	if err != nil {
		cancel()
		t.Fatalf("Open: %v", err)
	}
	facade := rabbitConn.Admin().(*admin)
	if _, err := facade.EnsureTopology(ctx, driver.TopologySpec{Destinations: specs}); err != nil {
		_ = rabbitConn.Close(context.Background())
		cancel()
		t.Fatalf("EnsureTopology: %v", err)
	}
	t.Cleanup(func() {
		_ = rabbitConn.Close(context.Background())
		for _, name := range names {
			_, _ = management.deleteQueue(context.Background(), name)
		}
		cancel()
	})
	return ctx, rabbitConn.(*conn), facade
}

func openPrunePublisher(t *testing.T) (*amqp.Channel, <-chan amqp.Confirmation) {
	t.Helper()
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	channel, err := raw.Channel()
	if err != nil {
		_ = raw.Close()
		t.Fatalf("Channel: %v", err)
	}
	if err := channel.Confirm(false); err != nil {
		_ = channel.Close()
		_ = raw.Close()
		t.Fatalf("Confirm: %v", err)
	}
	confirmations := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	t.Cleanup(func() {
		_ = channel.Close()
		_ = raw.Close()
	})
	return channel, confirmations
}

func publishPruneMessage(ctx context.Context, channel *amqp.Channel, confirmations <-chan amqp.Confirmation, destination string) error {
	if err := channel.PublishWithContext(ctx, "", destination, true, false, amqp.Publishing{
		Body: []byte("raced"),
	}); err != nil {
		return err
	}
	select {
	case confirmation, ok := <-confirmations:
		if !ok {
			return fmt.Errorf("publish confirmations channel closed")
		}
		if !confirmation.Ack {
			return fmt.Errorf("publish was negatively acknowledged")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// attachPruneConsumer attaches a raw consumer to destination and returns its
// channel and consumer tag. The tag is returned so a caller can detach with
// Cancel: the broker acknowledges a consumer cancel only once the queue has
// processed it, while a channel close returns before that for quorum queues.
func attachPruneConsumer(t *testing.T, ctx context.Context, destination string) (*amqp.Channel, string) {
	t.Helper()
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial consumer: %v", err)
	}
	channel, err := raw.Channel()
	if err != nil {
		_ = raw.Close()
		t.Fatalf("consumer Channel: %v", err)
	}
	tag := "rabbitmq-driver-prune-consumer-" + strings.ReplaceAll(t.Name(), "/", "-")
	if _, err := channel.Consume(destination, tag, false, false, false, false, nil); err != nil {
		_ = channel.Close()
		_ = raw.Close()
		t.Fatalf("Consume(%q): %v", destination, err)
	}
	probe, err := raw.Channel()
	if err != nil {
		_ = channel.Cancel(tag, false)
		_ = channel.Close()
		_ = raw.Close()
		t.Fatalf("probe Channel: %v", err)
	}
	t.Cleanup(func() {
		_ = channel.Cancel(tag, false)
		_ = channel.Close()
		_ = probe.Close()
		_ = raw.Close()
	})

	for {
		queue, err := probe.QueueDeclarePassive(destination, true, false, false, false, nil)
		if err != nil {
			t.Fatalf("QueueDeclarePassive(%q): %v", destination, err)
		}
		if queue.Consumers > 0 {
			return channel, tag
		}
		select {
		case <-ctx.Done():
			t.Fatalf("consumer for %q was not registered: %v", destination, ctx.Err())
		case <-time.After(10 * time.Millisecond): //nolint:forbidigo // bounded polling interval for broker registration
		}
	}
}

func waitForPruneManagementConsumer(t *testing.T, ctx context.Context, facade *admin, destination string) {
	t.Helper()
	for {
		queues, err := facade.operations.conn.management.listQueues(ctx)
		if err != nil {
			t.Fatalf("listQueues: %v", err)
		}
		queue, ok := findQueue(queues, destination)
		if ok && queue.Consumers > 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("management snapshot for %q did not report a consumer: %v", destination, ctx.Err())
		case <-time.After(10 * time.Millisecond): //nolint:forbidigo // bounded polling interval for broker statistics
		}
	}
}

func assertPruneQueuePresent(t *testing.T, ctx context.Context, facade *admin, name string, want bool) {
	t.Helper()
	queues, err := facade.operations.conn.management.listQueues(ctx)
	if err != nil {
		t.Fatalf("listQueues: %v", err)
	}
	_, present := findQueue(queues, name)
	if present != want {
		t.Fatalf("queue %q present = %t, want %t (queues = %+v)", name, present, want, queues)
	}
}

func TestPruneDoesNotDeleteQueueThatGainedAMessage(t *testing.T) {
	for _, kind := range []queueKind{queueKindClassic, queueKindQuorum} {
		t.Run(string(kind), func(t *testing.T) {
			destination := "rabbitmq-driver-prune-precondition-message-" + string(kind)
			ctx, _, facade := setupPruneTest(t, kind, driver.DestinationSpec{Name: destination, Durable: true})
			publisher, confirmations := openPrunePublisher(t)
			published := false
			facade.operations.pruneBeforeDeleteHook = func(name string) {
				if name != destination || published {
					return
				}
				if err := publishPruneMessage(ctx, publisher, confirmations, destination); err != nil {
					t.Errorf("publishPruneMessage: %v", err)
				}
				published = true
			}

			results, err := facade.Prune(ctx, []string{destination})
			if err != nil {
				t.Fatalf("Prune: %v", err)
			}
			if !published {
				t.Fatal("prune delete hook did not publish a message")
			}
			if len(results) != 1 || results[0].Deleted || results[0].Reason == "" {
				t.Fatalf("Prune result = %+v, want a non-deleted destination with a reason", results)
			}
			assertPruneQueuePresent(t, ctx, facade, destination, true)
		})
	}
}

func TestPruneDoesNotDeleteQueueThatGainedAConsumer(t *testing.T) {
	for _, kind := range []queueKind{queueKindClassic, queueKindQuorum} {
		t.Run(string(kind), func(t *testing.T) {
			destination := "rabbitmq-driver-prune-precondition-consumer-" + string(kind)
			ctx, _, facade := setupPruneTest(t, kind, driver.DestinationSpec{Name: destination, Durable: true})
			attached := false
			facade.operations.pruneBeforeDeleteHook = func(name string) {
				if name != destination || attached {
					return
				}
				_, _ = attachPruneConsumer(t, ctx, destination)
				attached = true
			}

			results, err := facade.Prune(ctx, []string{destination})
			if err != nil {
				t.Fatalf("Prune: %v", err)
			}
			if !attached {
				t.Fatal("prune delete hook did not attach a consumer")
			}
			wantReason := map[queueKind]string{
				queueKindClassic: "destination is no longer prunable",
				queueKindQuorum:  fmt.Sprintf("destination %q has consumers attached", destination),
			}[kind]
			if len(results) != 1 || results[0].Deleted || results[0].Reason != wantReason {
				t.Fatalf("Prune result = %+v, want a non-deleted destination with reason %q", results, wantReason)
			}
			assertPruneQueuePresent(t, ctx, facade, destination, true)
		})
	}
}

func TestPruneDeletesQueueWhoseConsumerJustDetached(t *testing.T) {
	for _, kind := range []queueKind{queueKindClassic, queueKindQuorum} {
		t.Run(string(kind), func(t *testing.T) {
			destination := "rabbitmq-driver-prune-detached-consumer-" + string(kind)
			ctx, _, facade := setupPruneTest(t, kind, driver.DestinationSpec{Name: destination, Durable: true})
			consumerChannel, tag := attachPruneConsumer(t, ctx, destination)
			waitForPruneManagementConsumer(t, ctx, facade, destination)

			// Detach by cancelling the consumer, not by closing its channel.
			// The guard reads the queue's own consumer count, and a quorum
			// queue keeps counting a consumer whose channel was closed until
			// the queue process handles the cancellation; closing the channel
			// raced that window and made prune refuse a queue whose consumer
			// was gone. Cancel is acknowledged only once the count no longer
			// includes the consumer, which is the detach this test means.
			if err := consumerChannel.Cancel(tag, false); err != nil {
				t.Fatalf("Cancel consumer: %v", err)
			}
			queues, err := facade.operations.conn.management.listQueues(ctx)
			if err != nil {
				t.Fatalf("listQueues after detach: %v", err)
			}
			queue, ok := findQueue(queues, destination)
			if !ok || queue.Consumers == 0 {
				t.Fatalf("management snapshot after detach for %q = %+v, want Consumers > 0", destination, queue)
			}

			results, err := facade.Prune(ctx, []string{destination})
			if err != nil {
				t.Fatalf("Prune: %v", err)
			}
			if len(results) != 1 || !results[0].Deleted || results[0].Reason != "" {
				t.Fatalf("Prune result = %+v, want a deleted destination without a reason", results)
			}
			assertPruneQueuePresent(t, ctx, facade, destination, false)
		})
	}
}

func TestPruneRefusesDestinationWithALiveConsumer(t *testing.T) {
	for _, kind := range []queueKind{queueKindClassic, queueKindQuorum} {
		t.Run(string(kind), func(t *testing.T) {
			destination := "rabbitmq-driver-prune-live-consumer-" + string(kind)
			ctx, _, facade := setupPruneTest(t, kind, driver.DestinationSpec{Name: destination, Durable: true})
			_, _ = attachPruneConsumer(t, ctx, destination)
			waitForPruneManagementConsumer(t, ctx, facade, destination)

			results, err := facade.Prune(ctx, []string{destination})
			if err != nil {
				t.Fatalf("Prune: %v", err)
			}
			wantReason := fmt.Sprintf("destination %q has consumers attached", destination)
			if len(results) != 1 || results[0].Deleted || results[0].Reason != wantReason {
				t.Fatalf("Prune result = %+v, want a non-deleted destination with reason %q", results, wantReason)
			}
		})
	}
}

func TestPrunePreconditionFailureDoesNotAbortRemainingNames(t *testing.T) {
	for _, kind := range []queueKind{queueKindClassic, queueKindQuorum} {
		t.Run(string(kind), func(t *testing.T) {
			first := "rabbitmq-driver-prune-precondition-first-" + string(kind)
			second := "rabbitmq-driver-prune-precondition-second-" + string(kind)
			ctx, _, facade := setupPruneTest(t, kind,
				driver.DestinationSpec{Name: first, Durable: true},
				driver.DestinationSpec{Name: second, Durable: true},
			)
			publisher, confirmations := openPrunePublisher(t)
			published := false
			facade.operations.pruneBeforeDeleteHook = func(name string) {
				if name != first || published {
					return
				}
				if err := publishPruneMessage(ctx, publisher, confirmations, first); err != nil {
					t.Errorf("publishPruneMessage: %v", err)
				}
				published = true
			}

			results, err := facade.Prune(ctx, []string{first, second})
			if err != nil {
				t.Fatalf("Prune: %v", err)
			}
			if !published {
				t.Fatal("prune delete hook did not publish a message")
			}
			if len(results) != 2 {
				t.Fatalf("Prune returned %d results, want 2: %+v", len(results), results)
			}
			if results[0].Name != first || results[0].Deleted || results[0].Reason == "" {
				t.Fatalf("first Prune result = %+v, want a non-deleted destination with a reason", results[0])
			}
			if results[1].Name != second || !results[1].Deleted || results[1].Reason != "" {
				t.Fatalf("second Prune result = %+v, want a deleted destination", results[1])
			}
			assertPruneQueuePresent(t, ctx, facade, first, true)
			assertPruneQueuePresent(t, ctx, facade, second, false)
		})
	}
}

func TestPruneDoesNotDeleteParkingQueueThatGainedAMessage(t *testing.T) {
	for _, kind := range []queueKind{queueKindClassic, queueKindQuorum} {
		t.Run(string(kind), func(t *testing.T) {
			destination := "rabbitmq-driver-prune-parking-message-" + string(kind)
			parking := destination + ".park"
			ctx, _, facade := setupPruneTest(t, kind, driver.DestinationSpec{
				Name:    destination,
				Durable: true,
				Delay:   time.Hour,
			})
			publisher, confirmations := openPrunePublisher(t)
			published := false
			facade.operations.pruneBeforeDeleteHook = func(name string) {
				if name != parking || published {
					return
				}
				if err := publishPruneMessage(ctx, publisher, confirmations, parking); err != nil {
					t.Errorf("publishPruneMessage: %v", err)
				}
				published = true
			}

			results, err := facade.Prune(ctx, []string{destination})
			if err != nil {
				t.Fatalf("Prune: %v", err)
			}
			if !published {
				t.Fatal("prune delete hook did not publish a parking message")
			}
			wantReason := map[queueKind]string{
				queueKindClassic: "parking destination is no longer prunable",
				queueKindQuorum:  fmt.Sprintf("auxiliary %q holds 1 ready message(s)", parking),
			}[kind]
			if len(results) != 1 || results[0].Deleted || results[0].Reason != wantReason {
				t.Fatalf("Prune result = %+v, want a non-deleted destination with reason %q", results, wantReason)
			}
			assertPruneQueuePresent(t, ctx, facade, destination, true)
			assertPruneQueuePresent(t, ctx, facade, parking, true)
		})
	}
}

// TestPruneRefusesEveryParkingRung proves the prune guard reads the whole
// ladder: a message parked in a rung queue, with the destination's own queue
// empty, refuses the deletion and the refusal names the queue holding it.
// Before the ladder there was one parking queue, so a guard that reads only the
// beyond-the-ladder queue would delete a destination whose deferred work is
// sitting in a rung, and those messages would dead-letter into a destination
// that no longer exists.
func TestPruneRefusesEveryParkingRung(t *testing.T) {
	for _, rung := range parkRungs {
		t.Run(parkRungTags[rung], func(t *testing.T) {
			destination := "rabbitmq-driver-prune-rung-" + parkRungTags[rung]
			ctx, rabbitConn, facade := setupPruneTest(t, queueKindQuorum, driver.DestinationSpec{
				Name:    destination,
				Durable: true,
				Delay:   rung,
			})
			producer, err := rabbitConn.Producer(ctx, driver.ProducerConfig{RequireDurableAck: true})
			if err != nil {
				t.Fatalf("Producer: %v", err)
			}
			defer func() { _ = producer.Close(context.Background()) }()
			due := time.Now().Add(rung) //nolint:forbidigo // a live delayed publish needs a future due time
			if err := producer.Publish(ctx, driver.OutboundMessage{
				Destination: destination,
				DelayUntil:  due,
				Body:        []byte("parked"),
			}); err != nil {
				t.Fatalf("Publish: %v", err)
			}

			// Which queue holds it is read from the broker rather than assumed:
			// the rung is a function of the delay left when the publish lands,
			// and a mis-routed message has to fail here rather than pass by
			// refusing for the wrong queue.
			held := make([]string, 0, 1)
			for _, parkName := range parkQueueNames(destination) {
				ready, err := facade.operations.inspectQueue(ctx, parkName)
				if err != nil {
					t.Fatalf("inspectQueue(%q): %v", parkName, err)
				}
				if ready > 0 {
					held = append(held, parkName)
				}
			}
			want := parkQueueName(destination, rung)
			if len(held) != 1 || held[0] != want {
				t.Fatalf("parking queues holding a message = %v, want exactly [%q]", held, want)
			}

			results, err := facade.Prune(ctx, []string{destination})
			if err != nil {
				t.Fatalf("Prune: %v", err)
			}
			wantReason := fmt.Sprintf("auxiliary %q holds 1 ready message(s)", want)
			if len(results) != 1 || results[0].Deleted || results[0].Reason != wantReason {
				t.Fatalf("Prune result = %+v, want a refusal with reason %q", results, wantReason)
			}
			assertPruneQueuePresent(t, ctx, facade, destination, true)
			assertPruneQueuePresent(t, ctx, facade, want, true)
		})
	}
}

func TestPruneQuorumKeepsTheConnectionAlive(t *testing.T) {
	const destination = "rabbitmq-driver-prune-quorum-connection"
	ctx, rabbitConn, facade := setupPruneTest(t, queueKindQuorum, driver.DestinationSpec{Name: destination, Durable: true})
	publisher, confirmations := openPrunePublisher(t)
	published := false
	facade.operations.pruneBeforeDeleteHook = func(name string) {
		if name != destination || published {
			return
		}
		if err := publishPruneMessage(ctx, publisher, confirmations, destination); err != nil {
			t.Errorf("publishPruneMessage: %v", err)
		}
		published = true
	}

	results, err := facade.Prune(ctx, []string{destination})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if len(results) != 1 || results[0].Deleted || !strings.Contains(results[0].Reason, "holds 1 ready message(s)") {
		t.Fatalf("Prune result = %+v, want a non-deleted destination with a message reason", results)
	}
	if !published {
		t.Fatal("prune delete hook did not publish a message")
	}
	state, err := facade.DescribeTopology(ctx, []string{destination})
	if err != nil {
		t.Fatalf("DescribeTopology after Prune: %v", err)
	}
	if state.Depth[destination] != 1 {
		t.Fatalf("Depth[%q] after Prune = %d, want 1", destination, state.Depth[destination])
	}
	if err := rabbitConn.Ping(ctx); err != nil {
		t.Fatalf("Ping after Prune: %v", err)
	}
}

func TestPruneSurvivesAPreconditionFailure(t *testing.T) {
	const destination = "rabbitmq-driver-prune-precondition-nondurable"
	ctx, rabbitConn, facade := setupPruneTest(t, queueKindClassic, driver.DestinationSpec{Name: destination})
	publisher, err := rabbitConn.amqp.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	if err := publisher.Confirm(false); err != nil {
		_ = publisher.Close()
		t.Fatalf("Confirm: %v", err)
	}
	confirmations := publisher.NotifyPublish(make(chan amqp.Confirmation, 1))
	t.Cleanup(func() { _ = publisher.Close() })
	published := false
	facade.operations.pruneBeforeDeleteHook = func(name string) {
		if name != destination || published {
			return
		}
		if err := publishPruneMessage(ctx, publisher, confirmations, destination); err != nil {
			t.Errorf("publishPruneMessage: %v", err)
		}
		published = true
	}

	firstResults, err := facade.Prune(ctx, []string{destination})
	if err != nil {
		t.Fatalf("first Prune: %v", err)
	}
	if len(firstResults) != 1 || firstResults[0].Deleted || !strings.Contains(firstResults[0].Reason, "no longer prunable") {
		t.Fatalf("first Prune result = %+v, want a non-deleted destination with a precondition reason", firstResults)
	}

	secondResults, err := facade.Prune(ctx, []string{destination})
	if err != nil {
		t.Fatalf("second Prune: %v", err)
	}
	if len(secondResults) != 1 || secondResults[0].Deleted || !strings.Contains(secondResults[0].Reason, "holds 1 ready message(s)") {
		t.Fatalf("second Prune result = %+v, want the existing message to remain observable", secondResults)
	}
}

func TestConcurrentAdminOperationsDoNotCrossReplies(t *testing.T) {
	const (
		firstDestination       = "rabbitmq-driver-admin-concurrent-first"
		secondDestination      = "rabbitmq-driver-admin-concurrent-second"
		firstMessages          = int64(2)
		secondMessages         = int64(5)
		operationsPerIteration = 8
		iterations             = 200
	)
	ctx, rabbitConn, facade := setupPruneTest(t, queueKindClassic,
		driver.DestinationSpec{Name: firstDestination, Delay: time.Hour},
		driver.DestinationSpec{Name: secondDestination, Delay: time.Hour},
	)
	publisher, err := rabbitConn.amqp.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	if err := publisher.Confirm(false); err != nil {
		_ = publisher.Close()
		t.Fatalf("Confirm: %v", err)
	}
	confirmations := publisher.NotifyPublish(make(chan amqp.Confirmation, 1))
	t.Cleanup(func() { _ = publisher.Close() })
	for i := range firstMessages {
		if err := publishPruneMessage(ctx, publisher, confirmations, firstDestination); err != nil {
			t.Fatalf("publish first message %d: %v", i, err)
		}
	}
	for i := range secondMessages {
		if err := publishPruneMessage(ctx, publisher, confirmations, secondDestination); err != nil {
			t.Fatalf("publish second message %d: %v", i, err)
		}
	}

	type describeResult struct {
		state driver.TopologyState
		err   error
	}
	names := []string{firstDestination, secondDestination}
	reverseNames := []string{secondDestination, firstDestination}
	for iteration := range iterations {
		start := make(chan struct{})
		ready := make(chan struct{}, operationsPerIteration)
		results := make(chan describeResult, operationsPerIteration)
		var operations sync.WaitGroup
		operations.Add(operationsPerIteration)
		for operation := range operationsPerIteration {
			targets := names
			if operation%2 == 1 {
				targets = reverseNames
			}
			go func(targets []string) {
				defer operations.Done()
				ready <- struct{}{}
				<-start
				state, err := facade.DescribeTopology(ctx, targets)
				results <- describeResult{state: state, err: err}
			}(targets)
		}
		for range operationsPerIteration {
			<-ready
		}
		close(start)
		operations.Wait()
		close(results)

		for result := range results {
			if result.err != nil {
				t.Fatalf("iteration %d DescribeTopology: %v", iteration, result.err)
			}
			if result.state.Depth[firstDestination] != firstMessages {
				t.Fatalf("iteration %d Depth[%q] = %d, want %d", iteration, firstDestination, result.state.Depth[firstDestination], firstMessages)
			}
			if result.state.Depth[secondDestination] != secondMessages {
				t.Fatalf("iteration %d Depth[%q] = %d, want %d", iteration, secondDestination, result.state.Depth[secondDestination], secondMessages)
			}
		}
	}
}

func TestNonDurableDestinationOutlivesTheCallThatDeclaredIt(t *testing.T) {
	const destination = "rabbitmq-driver-admin-nondurable-lifetime"
	ctx, rabbitConn, facade := setupPruneTest(t, queueKindClassic, driver.DestinationSpec{Name: destination})
	publisher, err := rabbitConn.amqp.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	if err := publisher.Confirm(false); err != nil {
		_ = publisher.Close()
		t.Fatalf("Confirm: %v", err)
	}
	confirmations := publisher.NotifyPublish(make(chan amqp.Confirmation, 1))
	t.Cleanup(func() { _ = publisher.Close() })

	queue, err := publisher.QueueDeclarePassive(destination, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclarePassive(%q): %v", destination, err)
	}
	if queue.Messages != 0 {
		t.Fatalf("QueueDeclarePassive(%q) messages = %d, want 0", destination, queue.Messages)
	}
	if err := publishPruneMessage(ctx, publisher, confirmations, destination); err != nil {
		t.Fatalf("publish message: %v", err)
	}
	state, err := facade.DescribeTopology(ctx, []string{destination})
	if err != nil {
		t.Fatalf("DescribeTopology(%q): %v", destination, err)
	}
	if state.Depth[destination] != 1 {
		t.Fatalf("Depth[%q] = %d, want 1", destination, state.Depth[destination])
	}
}

func TestRabbitMQAdminPruneRefusesNonEmptyParking(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	admin := conn.Admin()
	maintenance, ok := admin.(driver.Maintenance)
	if !ok {
		_ = conn.Close(ctx)
		t.Fatal("Admin does not implement driver.Maintenance")
	}
	const destination = "rabbitmq-driver-admin-prune-parking"
	if _, err := admin.EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: time.Hour}},
	}); err != nil {
		_ = conn.Close(ctx)
		t.Fatalf("EnsureTopology: %v", err)
	}
	producer, err := conn.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		_ = conn.Close(ctx)
		t.Fatalf("Producer: %v", err)
	}
	if err := producer.Publish(ctx, driver.OutboundMessage{
		Destination: destination,
		Body:        []byte("parked"),
	}); err != nil {
		_ = producer.Close(ctx)
		_ = conn.Close(ctx)
		t.Fatalf("Publish: %v", err)
	}
	if err := producer.Close(ctx); err != nil {
		_ = conn.Close(ctx)
		t.Fatalf("Close producer: %v", err)
	}
	results, err := maintenance.Prune(ctx, []string{destination})
	if err != nil {
		_, _ = maintenance.Purge(ctx, destination)
		_ = conn.Close(ctx)
		t.Fatalf("Prune: %v", err)
	}
	if len(results) != 1 || results[0].Deleted || !strings.Contains(results[0].Reason, ".park") {
		_, _ = maintenance.Purge(ctx, destination)
		_ = conn.Close(ctx)
		t.Fatalf("Prune result = %+v, want a named non-empty parking auxiliary refusal", results)
	}
	state, err := admin.DescribeTopology(ctx, []string{destination})
	if err != nil {
		_, _ = maintenance.Purge(ctx, destination)
		_ = conn.Close(ctx)
		t.Fatalf("DescribeTopology: %v", err)
	}
	if state.Depth[destination] != 1 {
		_, _ = maintenance.Purge(ctx, destination)
		_ = conn.Close(ctx)
		t.Fatalf("Depth[%q] = %d, want 1 while the park holds the message", destination, state.Depth[destination])
	}
	purged, err := maintenance.Purge(ctx, destination)
	if err != nil {
		_ = conn.Close(ctx)
		t.Fatalf("Purge: %v", err)
	}
	if purged != 1 {
		_ = conn.Close(ctx)
		t.Fatalf("Purge count = %d, want 1", purged)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
