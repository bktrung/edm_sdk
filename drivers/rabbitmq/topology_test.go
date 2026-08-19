package rabbitmq

import (
	"context"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestEnsureTopologyDeclaresFanoutAndBinding(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const exchange = "rabbitmq-driver-topology-exchange"
	const queue = "rabbitmq-driver-topology-queue"
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })
	_, _ = rawChannel.QueueDelete(queue, false, false, false)
	_ = rawChannel.ExchangeDelete(exchange, false, false)
	t.Cleanup(func() {
		_, _ = rawChannel.QueueDelete(queue, false, false, false)
		_ = rawChannel.ExchangeDelete(exchange, false, false)
	})

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	spec := driver.TopologySpec{
		Exchanges:    []driver.ExchangeSpec{{Name: exchange, Kind: "fanout", Durable: true}},
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
		Bindings:     []driver.BindingSpec{{Source: exchange, Destination: queue}},
	}
	first, err := conn.Admin().EnsureTopology(ctx, spec)
	if err != nil {
		t.Fatalf("first EnsureTopology: %v", err)
	}
	if !containsString(first.CreatedExchanges, exchange) {
		t.Fatalf("first CreatedExchanges = %v, want %q", first.CreatedExchanges, exchange)
	}
	if !containsString(first.CreatedDestinations, queue) {
		t.Fatalf("first CreatedDestinations = %v, want %q", first.CreatedDestinations, queue)
	}
	if !containsString(first.CreatedBindings, queue) {
		t.Fatalf("first CreatedBindings = %v, want %q", first.CreatedBindings, queue)
	}

	second, err := conn.Admin().EnsureTopology(ctx, spec)
	if err != nil {
		t.Fatalf("second EnsureTopology: %v", err)
	}
	if containsString(second.CreatedExchanges, exchange) || containsString(second.CreatedDestinations, queue) || containsString(second.CreatedBindings, queue) {
		t.Fatalf("second diff reports creation: %#v", second)
	}
	if !containsString(second.Existing, exchange) || !containsString(second.Existing, queue) || !containsString(second.Existing, queue) {
		t.Fatalf("second Existing = %v, want exchange, queue, and binding", second.Existing)
	}

	deliveries, err := rawChannel.Consume(queue, "topology-test", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if err := rawChannel.PublishWithContext(ctx, exchange, "", false, false, amqp.Publishing{Body: []byte("routed")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case delivery := <-deliveries:
		if string(delivery.Body) != "routed" {
			t.Fatalf("routed body = %q, want routed", delivery.Body)
		}
		if err := delivery.Ack(false); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("waiting for routed message: %v", ctx.Err())
	}
}

func TestVerifyTopologyReportsDeletedBinding(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const exchange = "rabbitmq-driver-verify-binding-exchange"
	const queue = "rabbitmq-driver-verify-binding-queue"
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })
	_, _ = rawChannel.QueueDelete(queue, false, false, false)
	_ = rawChannel.ExchangeDelete(exchange, false, false)
	t.Cleanup(func() {
		_, _ = rawChannel.QueueDelete(queue, false, false, false)
		_ = rawChannel.ExchangeDelete(exchange, false, false)
	})

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	spec := driver.TopologySpec{
		Exchanges:    []driver.ExchangeSpec{{Name: exchange, Kind: "fanout", Durable: true}},
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
		Bindings:     []driver.BindingSpec{{Source: exchange, Destination: queue}},
	}
	if _, err := conn.Admin().EnsureTopology(ctx, spec); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	if err := rawChannel.QueueUnbind(queue, "", exchange, nil); err != nil {
		t.Fatalf("QueueUnbind: %v", err)
	}

	spec.Policy = driver.TopologyVerify
	_, err = conn.Admin().EnsureTopology(ctx, spec)
	if err == nil {
		t.Fatal("TopologyVerify() error = nil after binding deletion")
	}
	message := err.Error()
	if !strings.Contains(message, "binding") || !strings.Contains(message, exchange) || !strings.Contains(message, queue) {
		t.Fatalf("TopologyVerify() error = %q, want binding and endpoint names", message)
	}

	spec.Policy = driver.TopologyDeclare
	repaired, err := conn.Admin().EnsureTopology(ctx, spec)
	if err != nil {
		t.Fatalf("EnsureTopology after binding deletion: %v", err)
	}
	if !containsString(repaired.CreatedBindings, queue) {
		t.Fatalf("EnsureTopology after binding deletion CreatedBindings = %v, want %q", repaired.CreatedBindings, queue)
	}
}

func TestVerifyTopologyReportsArgumentDrift(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const drifted = "rabbitmq-driver-verify-drift-queue"
	const clean = "rabbitmq-driver-verify-drift-clean-queue"
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })
	_, _ = rawChannel.QueueDelete(drifted, false, false, false)
	_, _ = rawChannel.QueueDelete(clean, false, false, false)
	t.Cleanup(func() {
		_, _ = rawChannel.QueueDelete(drifted, false, false, false)
		_, _ = rawChannel.QueueDelete(clean, false, false, false)
	})

	// Declare the queue out of band with a delivery limit the spec below does
	// not agree with. QueueDeclarePassive would not catch this: it checks the
	// queue's name only. The management-API-backed drift check is what must
	// catch it.
	if _, err := rawChannel.QueueDeclare(drifted, true, false, false, false, amqp.Table{
		"x-queue-type":     "quorum",
		"x-delivery-limit": int32(3),
	}); err != nil {
		t.Fatalf("out-of-band QueueDeclare(%q): %v", drifted, err)
	}

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	driftedSpec := driver.TopologySpec{
		Policy:       driver.TopologyVerify,
		Destinations: []driver.DestinationSpec{{Name: drifted, Durable: true, DeliveryLimit: 7}},
	}
	diff, err := conn.Admin().EnsureTopology(ctx, driftedSpec)
	if err != nil {
		t.Fatalf("EnsureTopology(verify) for drifted queue: %v", err)
	}
	if len(diff.Drifted) == 0 {
		t.Fatalf("Drifted = %v, want an entry naming x-delivery-limit", diff.Drifted)
	}
	found := false
	for _, d := range diff.Drifted {
		if d.Name == drifted && d.Argument == "x-delivery-limit" {
			found = true
			if d.Want != "7" || d.Got != "3" {
				t.Fatalf("Drifted entry = %+v, want Want=7 Got=3", d)
			}
		}
	}
	if !found {
		t.Fatalf("Drifted = %v, want an entry for %q naming x-delivery-limit", diff.Drifted, drifted)
	}

	// A queue declared to match the spec must report no drift, so the check
	// is not simply always-fail.
	cleanSpec := driver.TopologySpec{
		Policy:       driver.TopologyDeclare,
		Destinations: []driver.DestinationSpec{{Name: clean, Durable: true, DeliveryLimit: 7}},
	}
	if _, err := conn.Admin().EnsureTopology(ctx, cleanSpec); err != nil {
		t.Fatalf("EnsureTopology(declare) for clean queue: %v", err)
	}
	cleanSpec.Policy = driver.TopologyVerify
	cleanDiff, err := conn.Admin().EnsureTopology(ctx, cleanSpec)
	if err != nil {
		t.Fatalf("EnsureTopology(verify) for clean queue: %v", err)
	}
	if len(cleanDiff.Drifted) != 0 {
		t.Fatalf("Drifted = %v, want empty for a correctly declared queue", cleanDiff.Drifted)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
