package rabbitmq

import (
	"context"
	"fmt"
	"strings"
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
	names := make([]string, 0, len(specs)*2)
	for _, spec := range specs {
		names = append(names, spec.Name)
		if spec.Delay > 0 {
			names = append(names, spec.Name+".park")
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

func attachPruneConsumer(t *testing.T, ctx context.Context, destination string) {
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
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("consumer for %q was not registered: %v", destination, ctx.Err())
		case <-time.After(10 * time.Millisecond): //nolint:forbidigo // bounded polling interval for broker registration
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
				attachPruneConsumer(t, ctx, destination)
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

func TestPruneSurvivesAPreconditionFailureOnAReusedChannel(t *testing.T) {
	const destination = "rabbitmq-driver-prune-precondition-ephemeral"
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
	rabbitConn.mu.RLock()
	_, retained := rabbitConn.ephemeral[destination]
	rabbitConn.mu.RUnlock()
	if retained {
		t.Fatal("ephemeral channel remained cached after a precondition failure")
	}

	secondResults, err := facade.Prune(ctx, []string{destination})
	if err != nil {
		t.Fatalf("second Prune: %v", err)
	}
	if len(secondResults) != 1 || secondResults[0].Deleted || !strings.Contains(secondResults[0].Reason, "holds 1 ready message(s)") {
		t.Fatalf("second Prune result = %+v, want the existing message to remain observable", secondResults)
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
