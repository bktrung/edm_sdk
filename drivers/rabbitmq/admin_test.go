package rabbitmq

import (
	"context"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestRabbitMQAdminPruneRefusesNonEmptyParking(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	admin := conn.Admin()
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
	results, err := admin.Prune(ctx, []string{destination})
	if err != nil {
		_, _ = admin.Purge(ctx, destination)
		_ = conn.Close(ctx)
		t.Fatalf("Prune: %v", err)
	}
	if len(results) != 1 || results[0].Deleted || !strings.Contains(results[0].Reason, ".park") {
		_, _ = admin.Purge(ctx, destination)
		_ = conn.Close(ctx)
		t.Fatalf("Prune result = %+v, want a named non-empty parking auxiliary refusal", results)
	}
	state, err := admin.DescribeTopology(ctx, []string{destination})
	if err != nil {
		_, _ = admin.Purge(ctx, destination)
		_ = conn.Close(ctx)
		t.Fatalf("DescribeTopology: %v", err)
	}
	if state.Depth[destination] != 1 {
		_, _ = admin.Purge(ctx, destination)
		_ = conn.Close(ctx)
		t.Fatalf("Depth[%q] = %d, want 1 while the park holds the message", destination, state.Depth[destination])
	}
	purged, err := admin.Purge(ctx, destination)
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
