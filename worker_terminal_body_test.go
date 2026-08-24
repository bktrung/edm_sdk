package f1

import (
	"context"
	"errors"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func waitForTerminalBody(t *testing.T, done <-chan struct{}, got *[]byte, want []byte, kind string) {
	t.Helper()
	timer := clock.NewReal().Timer(time.Second)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		t.Fatalf("%s callback did not finish", kind)
	}
	if string(*got) != string(want) {
		t.Fatalf("%s.Body = %q, want %q", kind, *got, want)
	}
}

// terminalBodyMessage builds an InboundMessage carrying body, encoding
// envelope into headers unless envelope is nil, in which case headers is used
// verbatim so a malformed decode can be exercised.
func terminalBodyMessage(t *testing.T, envelope *Envelope, headers map[string]string, body []byte, settler driver.Settler) driver.InboundMessage {
	t.Helper()
	if envelope != nil {
		encoded, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
		if err != nil {
			t.Fatal(err)
		}
		headers = encoded
	}
	return driver.InboundMessage{
		Destination: "test/orders",
		Headers:     headerSlice(headers),
		Body:        body,
		Settle:      settler,
	}
}

// TestTerminalCallbackBodiesCarryExactPayload exercises all four worker.go
// call sites that construct a DeadLettered or Discarded payload and asserts
// the callback observes the exact body bytes the driver delivered.
func TestTerminalCallbackBodiesCarryExactPayload(t *testing.T) {
	t.Run("unmatched", func(t *testing.T) {
		producer := &dispatchProducer{}
		client, runner := newRetryBridgeRunner(t, producer, "orders.unmatched")
		defer func() { _ = client.Close(context.Background()) }()
		var got []byte
		done := make(chan struct{})
		runner.subscription.OnDiscarded = func(_ context.Context, d Discarded) {
			got = d.Body
			close(done)
		}
		body := []byte("unmatched-payload")
		envelope := Envelope{SpecVersion: "1.0", ID: "unmatched", Source: "/test/orders", Type: "orders.unmatched.v1", Priority: PriorityHigh, Attempt: 1}
		message := terminalBodyMessage(t, &envelope, nil, body, &dispatchSettler{})
		if !dispatchMessage(runner, context.Background(), message, &Envelope{}, new(bool)) {
			t.Fatal("unmatched message was not settled")
		}
		waitForTerminalBody(t, done, &got, body, "Discarded")
	})

	t.Run("dropped", func(t *testing.T) {
		producer := &dispatchProducer{}
		client, runner := newRetryBridgeRunner(t, producer, "orders.dropped")
		defer func() { _ = client.Close(context.Background()) }()
		runner.subscription.Handlers = map[string]Handler{
			"orders.dropped.v1": HandlerFunc(func(context.Context, *Event) error {
				return Drop(errors.New("no longer needed"))
			}),
		}
		var got []byte
		done := make(chan struct{})
		runner.subscription.OnDiscarded = func(_ context.Context, d Discarded) {
			got = d.Body
			close(done)
		}
		body := []byte("dropped-payload")
		envelope := Envelope{SpecVersion: "1.0", ID: "dropped", Source: "/test/orders", Type: "orders.dropped.v1", Priority: PriorityHigh, Attempt: 1}
		message := terminalBodyMessage(t, &envelope, nil, body, &dispatchSettler{})
		if !dispatchMessage(runner, context.Background(), message, &Envelope{}, new(bool)) {
			t.Fatal("dropped message was not settled")
		}
		waitForTerminalBody(t, done, &got, body, "Discarded")
	})

	t.Run("decode failure", func(t *testing.T) {
		producer := &dispatchProducer{}
		client, runner := newRetryBridgeRunner(t, producer, "orders.decode")
		defer func() { _ = client.Close(context.Background()) }()
		var got []byte
		done := make(chan struct{})
		runner.subscription.OnDeadLetter = func(_ context.Context, d DeadLettered) {
			got = d.Body
			close(done)
		}
		body := []byte("decode-failure-payload")
		message := terminalBodyMessage(t, nil, map[string]string{"time": "not-a-valid-time"}, body, &dispatchSettler{})
		if !dispatchMessage(runner, context.Background(), message, &Envelope{}, new(bool)) {
			t.Fatal("decode-failure message was not settled")
		}
		waitForTerminalBody(t, done, &got, body, "DeadLettered")
	})

	t.Run("ordinary dead letter", func(t *testing.T) {
		producer := &dispatchProducer{}
		client, runner := newRetryBridgeRunner(t, producer, "orders.runaway")
		defer func() { _ = client.Close(context.Background()) }()
		var got []byte
		done := make(chan struct{})
		runner.subscription.OnDeadLetter = func(_ context.Context, d DeadLettered) {
			got = d.Body
			close(done)
		}
		body := []byte("runaway-payload")
		envelope := Envelope{SpecVersion: "1.0", ID: "runaway", Source: "/test/orders", Type: "orders.runaway.v1", Priority: PriorityHigh, Attempt: 15}
		message := terminalBodyMessage(t, &envelope, nil, body, &dispatchSettler{})
		if !dispatchMessage(runner, context.Background(), message, &Envelope{}, new(bool)) {
			t.Fatal("runaway message was not settled")
		}
		waitForTerminalBody(t, done, &got, body, "DeadLettered")
	})
}

// TestTerminalCallbackBodyIsCopiedNotAliased proves the Body recorded on a
// terminal callback is an independent copy: mutating the driver's delivery
// buffer after settlement completes must not change what the callback
// observed.
func TestTerminalCallbackBodyIsCopiedNotAliased(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.runaway")
	defer func() { _ = client.Close(context.Background()) }()

	var got []byte
	done := make(chan struct{})
	runner.subscription.OnDeadLetter = func(_ context.Context, d DeadLettered) {
		got = d.Body
		close(done)
	}

	body := []byte("original-payload")
	envelope := Envelope{SpecVersion: "1.0", ID: "runaway", Source: "/test/orders", Type: "orders.runaway.v1", Priority: PriorityHigh, Attempt: 15}

	settler := &dispatchSettler{}
	message := terminalBodyMessage(t, &envelope, nil, body, settler)

	if !dispatchMessage(runner, context.Background(), message, &Envelope{}, new(bool)) {
		t.Fatal("runaway message was not settled")
	}
	waitForTerminalBody(t, done, &got, []byte("original-payload"), "DeadLettered")
	if !settler.acked {
		t.Fatal("settlement never completed")
	}

	// Mutate the driver's delivery buffer after settlement, as a driver is
	// free to recycle it once the message is settled.
	for i := range body {
		body[i] = 'X'
	}

	if string(got) == string(body) {
		t.Fatalf("DeadLettered.Body aliases the driver buffer: got %q after mutation", got)
	}
	if string(got) != "original-payload" {
		t.Fatalf("DeadLettered.Body = %q, want the original payload unchanged", got)
	}
}
