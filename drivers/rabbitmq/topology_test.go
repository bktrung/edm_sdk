package rabbitmq

import (
	"context"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestEnsureTopologyDeclaresFanoutAndBinding(t *testing.T) {
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
	defer func() { _ = conn.Close(context.Background()) }()

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

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
