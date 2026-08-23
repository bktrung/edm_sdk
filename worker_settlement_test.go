package f1

import (
	"context"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
)

func newSettlementOrderingRunner(t *testing.T) (*Client, *Runner, *dispatchConsumer) {
	t.Helper()
	producer := &dispatchProducer{}
	consumer := newDispatchConsumer()
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: &dispatchConn{producer: producer, consumer: consumer}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	runner := &Runner{
		client: client,
		subscription: Subscription{
			Name:       "orders",
			Topics:     []string{"orders.created"},
			Priorities: []Priority{PriorityHigh},
		},
		inflight: newInflightRegistry(),
	}
	runner.consumer = consumer
	runner.accounting = lifecycle.NewAccounting(runner.inflight.registry)
	return client, runner, consumer
}

// TestDrainSetupKeepsInstalledSettlementContextLive pins the context
// ordering between deferred delivery cleanup and drain setup. The cleanup of
// an abandoned delivery builds a settlement context the first time it needs
// one, and the drain setup may build one at the same moment. Whichever
// context the cleanup captured must stay live: cancelling it underneath a
// settlement call that is about to reach the driver turns that call into a
// transient context failure before the driver ever sees it, and the
// delivery's remaining cleanup budget is spent recovering from a cancellation
// the drain itself caused.
func TestDrainSetupKeepsInstalledSettlementContextLive(t *testing.T) {
	_, runner, consumer := newSettlementOrderingRunner(t)

	fallback, cancelFallback := context.WithCancel(context.Background())
	cancelFallback()

	installed := runnerSettlementContext(runner, fallback)

	if err := fetchRunnerAfterCancel(runner, fallback, consumer.Messages(), make(chan delivery, 1)); err != nil {
		t.Fatalf("fetchRunnerAfterCancel() = %v", err)
	}

	if err := installed.Err(); err != nil {
		t.Fatalf("settlement context the cleanup captured has error %v after drain setup; the drain must keep an already-installed settlement context live instead of cancelling and replacing it", err)
	}
}
