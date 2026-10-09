//go:build integration

package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// deleteQueue removes one queue through the management API, leaving a queue
// that is already gone alone. The driver deletes queues only through Prune,
// which uses its own AMQP channel and guards every precondition, so this lives
// with the tests that declare fixtures directly and have to clean up after
// themselves.
func (m *managementClient) deleteQueue(ctx context.Context, name string) (bool, error) {
	response, err := m.do(ctx, http.MethodDelete, m.queuePath(name))
	if err != nil {
		return false, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return false, fmt.Errorf("management API DELETE queue %q: %s: %s", name, response.Status, strings.TrimSpace(string(body)))
	}
	return true, nil
}

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
			names = append(names, parkQueueName(spec.Name))
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

func setupPruneExchangeTest(t *testing.T, spec driver.TopologySpec) (context.Context, *conn, *admin) {
	t.Helper()
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	management, err := newManagementClient(defaultEndpoint, driver.Config{})
	if err != nil {
		cancel()
		t.Fatalf("newManagementClient: %v", err)
	}
	for _, destination := range spec.Destinations {
		if _, err := management.deleteQueue(ctx, destination.Name); err != nil {
			cancel()
			t.Fatalf("deleteQueue(%q): %v", destination.Name, err)
		}
	}
	for _, exchange := range spec.Exchanges {
		if _, err := management.deleteExchange(ctx, exchange.Name); err != nil {
			cancel()
			t.Fatalf("deleteExchange(%q): %v", exchange.Name, err)
		}
	}
	rabbitConn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		cancel()
		t.Fatalf("Open: %v", err)
	}
	facade := rabbitConn.Admin().(*admin)
	if _, err := facade.EnsureTopology(ctx, spec); err != nil {
		_ = rabbitConn.Close(context.Background())
		cancel()
		t.Fatalf("EnsureTopology: %v", err)
	}
	t.Cleanup(func() {
		_ = rabbitConn.Close(context.Background())
		for _, destination := range spec.Destinations {
			_, _ = management.deleteQueue(context.Background(), destination.Name)
		}
		for _, exchange := range spec.Exchanges {
			_, _ = management.deleteExchange(context.Background(), exchange.Name)
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
		deadline := time.Now().Add(3 * time.Second) //nolint:forbidigo // the close deadline bounds cleanup around a live broker.
		// Closing the connection also closes its channels, while the deadline
		// prevents a blocked broker from wedging test cleanup.
		_ = raw.CloseDeadline(deadline)
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

func assertPruneExchangePresent(t *testing.T, ctx context.Context, facade *admin, name string, want bool) {
	t.Helper()
	exchanges, err := facade.operations.conn.management.listExchanges(ctx)
	if err != nil {
		t.Fatalf("listExchanges: %v", err)
	}
	_, present := findExchange(exchanges, name)
	if present != want {
		t.Fatalf("exchange %q present = %t, want %t (exchanges = %+v)", name, present, want, exchanges)
	}
}

func TestPruneDeletesUnboundExchange(t *testing.T) {
	const exchange = "rabbitmq-driver-prune-unbound-exchange"
	ctx, _, facade := setupPruneExchangeTest(t, driver.TopologySpec{
		Exchanges: []driver.ExchangeSpec{{Name: exchange, Kind: "fanout", Durable: true}},
	})
	assertPruneExchangePresent(t, ctx, facade, exchange, true)

	results, err := facade.Prune(ctx, []string{exchange})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if len(results) != 1 || results[0].Name != exchange || !results[0].Deleted || results[0].Reason != "" {
		t.Fatalf("Prune result = %+v, want a deleted exchange without a reason", results)
	}
	assertPruneExchangePresent(t, ctx, facade, exchange, false)
}

func TestPruneRefusesBoundExchange(t *testing.T) {
	const exchange = "rabbitmq-driver-prune-bound-exchange"
	const destination = "rabbitmq-driver-prune-bound-exchange-queue"
	const otherExchange = "rabbitmq-driver-prune-unrelated-exchange"
	const otherDestination = "rabbitmq-driver-prune-unrelated-exchange-queue"
	ctx, _, facade := setupPruneExchangeTest(t, driver.TopologySpec{
		Exchanges: []driver.ExchangeSpec{
			{Name: exchange, Kind: "fanout", Durable: true},
			{Name: otherExchange, Kind: "fanout", Durable: true},
		},
		Destinations: []driver.DestinationSpec{
			{Name: destination, Durable: true},
			{Name: otherDestination, Durable: true},
		},
		Bindings: []driver.BindingSpec{
			{Source: exchange, Destination: destination},
			{Source: otherExchange, Destination: otherDestination},
		},
	})
	assertPruneExchangePresent(t, ctx, facade, exchange, true)

	results, err := facade.Prune(ctx, []string{exchange})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	wantReason := fmt.Sprintf("exchange %q has binding to destination %q", exchange, destination)
	if len(results) != 1 || results[0].Name != exchange || results[0].Deleted || results[0].Reason != wantReason {
		t.Fatalf("Prune result = %+v, want refusal with reason %q", results, wantReason)
	}
	assertPruneExchangePresent(t, ctx, facade, exchange, true)
}

func TestPruneReportsUnknownBuiltInNameAsMissing(t *testing.T) {
	const name = "amq.not-created-by-rabbitmq-driver"
	ctx, _, facade := setupPruneExchangeTest(t, driver.TopologySpec{})

	results, err := facade.Prune(ctx, []string{name})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if len(results) != 1 || results[0].Name != name || results[0].Deleted || results[0].Reason != "destination does not exist" {
		t.Fatalf("Prune result = %+v, want missing destination reason", results)
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
			parking := parkQueueName(destination)
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
	// 1,600 describes through the management API can outlast the 15 s setup
	// budget on a busy machine, so the loop has its own bound. The test checks
	// that concurrent replies are not crossed, not how fast they arrive.
	loopCtx, cancelLoop := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelLoop()
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
				state, err := facade.DescribeTopology(loopCtx, targets)
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
	const destination = "rabbitmq-driver-admin-prune-parking"
	management, err := newManagementClient(defaultEndpoint, driver.Config{})
	if err != nil {
		t.Fatalf("newManagementClient: %v", err)
	}
	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("cleanup connection: %v", err)
		}
		for _, name := range []string{destination, parkQueueName(destination)} {
			if _, err := management.deleteQueue(context.Background(), name); err != nil {
				t.Errorf("cleanup deleteQueue(%q): %v", name, err)
			}
		}
	})
	admin := conn.Admin()
	maintenance, ok := admin.(driver.Maintenance)
	if !ok {
		t.Fatal("Admin does not implement driver.Maintenance")
	}
	if _, err := admin.EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: time.Hour}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	producer, err := conn.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() {
		if err := producer.Close(context.Background()); err != nil {
			t.Errorf("cleanup producer: %v", err)
		}
	})
	if err := producer.Publish(ctx, driver.OutboundMessage{
		Destination: destination,
		Body:        []byte("parked"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := producer.Close(ctx); err != nil {
		t.Fatalf("Close producer: %v", err)
	}
	results, err := maintenance.Prune(ctx, []string{destination})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if len(results) != 1 || results[0].Deleted || !strings.Contains(results[0].Reason, ".park") {
		t.Fatalf("Prune result = %+v, want a named non-empty parking auxiliary refusal", results)
	}
	state, err := admin.DescribeTopology(ctx, []string{destination})
	if err != nil {
		t.Fatalf("DescribeTopology: %v", err)
	}
	if state.Depth[destination] != 1 {
		t.Fatalf("Depth[%q] = %d, want 1 while the park holds the message", destination, state.Depth[destination])
	}
	purged, err := maintenance.Purge(ctx, destination)
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if purged != 1 {
		t.Fatalf("Purge count = %d, want 1", purged)
	}
}

func TestPruneRefusesParkingQueueWithAMessage(t *testing.T) {
	const destination = "rabbitmq-driver-prune-fixed-queue"
	spec := driver.DestinationSpec{Name: destination, Durable: true, Delay: 5 * time.Second}
	parkQueue := parkQueueName(destination)
	ctx, rabbitConn, facade := setupPruneTest(t, queueKindQuorum, spec)
	producer, err := rabbitConn.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	defer func() { _ = producer.Close(context.Background()) }()
	if err := producer.Publish(ctx, driver.OutboundMessage{
		Destination: destination,
		Body:        []byte("parked"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ready, err := facade.operations.inspectQueue(ctx, parkQueue)
	if err != nil {
		t.Fatalf("inspectQueue(%q): %v", parkQueue, err)
	}
	if ready != 1 {
		t.Fatalf("parking queue %q ready = %d, want 1", parkQueue, ready)
	}
	results, err := facade.Prune(ctx, []string{destination})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	wantReason := fmt.Sprintf("auxiliary %q holds 1 ready message(s)", parkQueue)
	if len(results) != 1 || results[0].Deleted || results[0].Reason != wantReason {
		t.Fatalf("Prune result = %+v, want a refusal with reason %q", results, wantReason)
	}
	assertPruneQueuePresent(t, ctx, facade, destination, true)
	assertPruneQueuePresent(t, ctx, facade, parkQueue, true)
}

func TestParkingQueueCountsInDescribeAndPurge(t *testing.T) {
	const destination = "rabbitmq-driver-fixed-describe-purge"
	spec := driver.DestinationSpec{Name: destination, Durable: true, Delay: 5 * time.Second}
	parkQueue := parkQueueName(destination)
	ctx, rabbitConn, facade := setupPruneTest(t, queueKindQuorum, spec)
	producer, err := rabbitConn.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	defer func() { _ = producer.Close(context.Background()) }()
	if err := producer.Publish(ctx, driver.OutboundMessage{
		Destination: destination,
		Body:        []byte("parked"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ready, err := facade.operations.inspectQueue(ctx, parkQueue)
	if err != nil {
		t.Fatalf("inspectQueue(%q): %v", parkQueue, err)
	}
	if ready != 1 {
		t.Fatalf("parking queue %q ready = %d, want 1", parkQueue, ready)
	}
	state, err := facade.DescribeTopology(ctx, []string{destination})
	if err != nil {
		t.Fatalf("DescribeTopology: %v", err)
	}
	if state.Depth[destination] != 1 {
		t.Fatalf("Depth[%q] = %d, want 1", destination, state.Depth[destination])
	}
	purged, err := facade.Purge(ctx, destination)
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if purged != 1 {
		t.Fatalf("Purge count = %d, want 1", purged)
	}
	after, err := facade.operations.inspectQueue(ctx, parkQueue)
	if err != nil {
		t.Fatalf("inspectQueue(%q): %v", parkQueue, err)
	}
	if after != 0 {
		t.Fatalf("parking queue %q ready = %d, want 0 after Purge", parkQueue, after)
	}
}

// managementRequest issues one management API request with the fixture's own
// credentials. The driver's management client creates nothing and sends no
// request body, and the permission test below needs both a user and its
// permission to exist. A 404 is left to the caller's own check: it is how a
// cleanup that has nothing to delete ends.
func managementRequest(t *testing.T, client *managementClient, method, path, body string) {
	t.Helper()
	var payload io.Reader
	if body != "" {
		payload = strings.NewReader(body)
	}
	request, err := http.NewRequestWithContext(context.Background(), method, client.baseURL+path, payload)
	if err != nil {
		t.Errorf("management %s %s: %v", method, path, err)
		return
	}
	if body != "" {
		request.Header.Set("content-type", "application/json")
	}
	request.SetBasicAuth(client.username, client.password)
	response, err := client.client.Do(request)
	if err != nil {
		t.Errorf("management %s %s: %v", method, path, err)
		return
	}
	defer response.Body.Close()
	if response.StatusCode >= http.StatusBadRequest && response.StatusCode != http.StatusNotFound {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		t.Errorf("management %s %s: %s: %s", method, path, response.Status, strings.TrimSpace(string(detail)))
	}
}

// TestPruneRefusesQueueDeleteWithoutConfigurePermission covers a queue delete
// the broker answers with 403 because the connected user may not configure the
// destination. Only the delete reports that refusal: a passive declare needs no
// configure permission, so the prune guard passes the destination and the
// failure has to stay a permission error rather than a transient one the core
// would retry.
func TestPruneRefusesQueueDeleteWithoutConfigurePermission(t *testing.T) {
	requireBroker(t)
	const (
		user        = "t089-prune-403-user"
		password    = "t089-prune-403-password" //nolint:gosec // fixture credentials this test creates and removes
		destination = "t089-prune-403-queue"
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	guest, err := newManagementClient(defaultEndpoint, driver.Config{})
	if err != nil {
		t.Fatalf("newManagementClient: %v", err)
	}
	// The restricted connection is the fixture endpoint with another identity,
	// so the test follows the fixture's scheme, host and vhost.
	restricted, err := url.Parse(defaultEndpoint)
	if err != nil {
		t.Fatalf("url.Parse(%q): %v", defaultEndpoint, err)
	}
	restricted.User = url.UserPassword(user, password)
	vhost := url.PathEscape(guest.vhost)
	managementRequest(t, guest, http.MethodPut, "/api/users/"+user,
		fmt.Sprintf(`{"password":%q,"tags":"monitoring"}`, password))
	// The user may configure only names under the allowed prefix, so the queue
	// below stays visible and declarable to it but not deletable by it.
	managementRequest(t, guest, http.MethodPut, "/api/permissions/"+vhost+"/"+user,
		`{"configure":"^t089-prune-403-allowed","write":".*","read":".*"}`)
	t.Cleanup(func() {
		managementRequest(t, guest, http.MethodDelete, "/api/permissions/"+vhost+"/"+user, "")
		managementRequest(t, guest, http.MethodDelete, "/api/users/"+user, "")
		managementRequest(t, guest, http.MethodDelete, "/api/queues/"+vhost+"/"+destination, "")
	})

	owner, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open as the fixture user: %v", err)
	}
	t.Cleanup(func() { _ = owner.Close(context.Background()) })
	if _, err := owner.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}

	maintenanceConn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{restricted.String()}})
	if err != nil {
		t.Fatalf("Open as the restricted user: %v", err)
	}
	t.Cleanup(func() { _ = maintenanceConn.Close(context.Background()) })
	maintenance, ok := maintenanceConn.Admin().(driver.Maintenance)
	if !ok {
		t.Fatal("Admin does not implement driver.Maintenance")
	}
	_, err = maintenance.Prune(ctx, []string{destination})
	kind, classified := driver.Classify(err)
	if !classified || kind != driver.KindPermission {
		t.Fatalf("Prune = %v (kind %v, classified %t), want a permission refusal", err, kind, classified)
	}
	var portErr *driver.Error
	var amqpErr *amqp.Error
	if !errors.As(err, &portErr) || portErr.Op != "prune" || !errors.As(err, &amqpErr) || amqpErr.Code != 403 {
		t.Fatalf("Prune = %v, want the broker's 403 reachable under a prune error", err)
	}
}

// TestOpenStillConnectsWhenEarlierAttemptsFail covers what endpoint failover and
// the refusal budget are for: a failed attempt must not end the connect while a
// remaining endpoint, or the same endpoint after its refusal clears, can still
// serve it.
func TestOpenStillConnectsWhenEarlierAttemptsFail(t *testing.T) {
	requireBroker(t)
	cases := []struct {
		name      string
		endpoints func(*testing.T) []string
	}{
		{
			name: "transient failure first",
			endpoints: func(t *testing.T) []string {
				return []string{unreachableLoopbackEndpoint(t), defaultEndpoint}
			},
		},
		{
			name: "refused credentials first",
			endpoints: func(t *testing.T) []string {
				refusing, _ := credentialRefusingEndpoint(t)
				return []string{refusing, defaultEndpoint}
			},
		},
		{
			name: "refusal that clears",
			endpoints: func(t *testing.T) []string {
				return []string{clearingEndpoint(t, 2)}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			conn, err := (Driver{}).Open(ctx, driver.Config{
				Endpoints:      tc.endpoints(t),
				ConnectTimeout: 10 * time.Second,
			})
			if err != nil {
				t.Fatalf("Open() = %v, want a connection", err)
			}
			if err := conn.Close(context.Background()); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

// clearingEndpoint starts a broker stand-in that refuses the first refusals
// connections the way rejected credentials are refused, and forwards every later
// connection to the fixture broker. It is how a test watches a refusal clear
// while the budget that would end the connect is still far from spent, with the
// real broker answering the handshake that follows.
func clearingEndpoint(t *testing.T, refusals int) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	broker := brokerAddress(defaultEndpoint)
	go func() {
		refused := 0
		for {
			inbound, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			if refused < refusals {
				refused++
				go refuseAfterStartOk(inbound)
				continue
			}
			go proxyToBroker(inbound, broker)
		}
	}()
	return standInEndpoint(t, listener.Addr().String())
}

// standInEndpoint points the fixture's own endpoint at a local listener, so a
// stand-in answers with the credentials and vhost the fixture is configured with
// rather than with defaults. The stand-in stays plaintext loopback, which the
// driver accepts without TLS settings, and the fixture publishes no amqps port
// for it to relay to, so an amqps fixture endpoint is not supported by this
// case.
func standInEndpoint(t *testing.T, address string) string {
	t.Helper()
	fixture, err := url.Parse(defaultEndpoint)
	if err != nil {
		t.Fatalf("url.Parse(%q): %v", defaultEndpoint, err)
	}
	fixture.Scheme = "amqp"
	fixture.Host = address
	return fixture.String()
}

// proxyToBroker relays one connection to the fixture broker, so a stand-in can
// stop refusing without having to answer an AMQP handshake itself.
func proxyToBroker(inbound net.Conn, broker string) {
	defer inbound.Close()
	outbound, err := net.Dial("tcp", broker)
	if err != nil {
		return
	}
	defer outbound.Close()
	relayed := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(outbound, inbound)
		relayed <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(inbound, outbound)
		relayed <- struct{}{}
	}()
	<-relayed
}
