package f1

import (
	"context"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// TestPublishPriorityReachesDriver proves that the public publish path carries
// each logical priority to the driver's exact wire level.
func TestPublishPriorityReachesDriver(t *testing.T) {
	producer := &recordingProducer{}
	client := newPublishClient(t, producer)
	for _, test := range []struct {
		name      string
		priority  Priority
		wireLevel uint8
	}{
		{name: "high", priority: PriorityHigh, wireLevel: 1},
		{name: "medium", priority: PriorityMedium, wireLevel: 0},
		{name: "low", priority: PriorityLow, wireLevel: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := client.Publisher().Publish(
				context.Background(), "orders.created", "payload", WithPriority(test.priority),
			); err != nil {
				t.Fatal(err)
			}
			producer.mu.Lock()
			messages := append([]driver.OutboundMessage(nil), producer.messages...)
			producer.mu.Unlock()
			if len(messages) != 1 {
				t.Fatalf("driver messages = %d, want 1", len(messages))
			}
			if got := messages[0].Priority; got != test.wireLevel {
				t.Fatalf("wire priority = %d, want %d for %s", got, test.wireLevel, test.priority)
			}
		})
	}
}

// TestDispatchSettlesHandlerAfterParentCancellationDuringDrain proves that a
// caller entering the drain state does not turn an already-running handler
// into a stuck delivery when its parent context ends.
func TestDispatchSettlesHandlerAfterParentCancellationDuringDrain(t *testing.T) {
	producer := &dispatchProducer{}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: &dispatchConn{producer: producer}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	started := make(chan struct{})
	release := make(chan struct{})
	settled := make(chan struct{})
	settler := &dispatchSettler{onSettle: func() { close(settled) }}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseHandler)

	runner := &Runner{
		client: client,
		subscription: Subscription{
			Name:           "orders",
			Topics:         []string{"orders.created"},
			Priorities:     []Priority{PriorityMedium},
			Retry:          RetryConfig{MaxAttempts: 1},
			HandlerTimeout: time.Second,
			Handlers: map[string]Handler{
				"orders.created": HandlerFunc(func(context.Context, *Event) error {
					close(started)
					<-release
					return nil
				}),
			},
		},
	}
	envelope := Envelope{SpecVersion: "1.0", ID: "drain-parent-cancel", Source: "/test/orders", Type: "orders.created", Attempt: 1}
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	message := driver.InboundMessage{
		Destination: "f1.test.orders.created.medium",
		Headers:     headerSlice(headers),
		Body:        []byte(`{}`),
		Settle:      settler,
	}
	done := make(chan bool, 1)
	go func() { done <- dispatchMessage(runner, parent, message, &Envelope{}, new(bool), &deliveryState{}) }()

	wait := client.options.clock.Timer(time.Second)
	defer wait.Stop()
	select {
	case <-started:
	case <-wait.C:
		t.Fatal("handler did not start")
	}

	runner.mu.Lock()
	runner.draining = true
	runner.mu.Unlock()
	cancel()
	releaseHandler()

	dispatchWait := client.options.clock.Timer(time.Second)
	defer dispatchWait.Stop()
	select {
	case handled := <-done:
		if !handled {
			t.Fatal("dispatchMessage returned false, want the handler result to settle")
		}
	case <-dispatchWait.C:
		t.Fatal("dispatchMessage did not finish after releasing the handler")
	}
	settleWait := client.options.clock.Timer(time.Second)
	defer settleWait.Stop()
	select {
	case <-settled:
	case <-settleWait.C:
		t.Fatal("handler result did not settle the delivery")
	}
	settler.mu.Lock()
	acked, nacked := settler.acked, settler.nacked
	settler.mu.Unlock()
	if acked != true || nacked != false {
		t.Fatalf("settlement = acked %t nacked %t, want acked true and nacked false", acked, nacked)
	}
}
