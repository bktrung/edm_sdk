package f1

import (
	"context"
	"errors"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func newRetryBridgeRunner(t *testing.T, producer *dispatchProducer, topic string) (*Client, *Runner) {
	t.Helper()
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: &dispatchConn{producer: producer}}))
	if err != nil {
		t.Fatal(err)
	}
	runner := &Runner{
		client: client,
		subscription: Subscription{
			Name:           "orders",
			Topics:         []string{topic},
			Priorities:     []Priority{PriorityHigh},
			Retry:          RetryConfig{MaxAttempts: 3, Tiers: []time.Duration{time.Second, 2 * time.Second}, Jitter: 0.2},
			HandlerTimeout: time.Second,
		},
		metrics: newDeliveryMetrics([]string{topic}),
	}
	return client, runner
}

func retryBridgeMessage(t *testing.T, envelope Envelope, settler driver.Settler) driver.InboundMessage {
	t.Helper()
	headers, err := envelope.EncodeHeaders(CoreMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	return driver.InboundMessage{
		Destination: publishEntryPoint("/test/orders", topicFor(envelope.Type), envelope.Priority),
		Headers:     headerSlice(headers),
		Body:        []byte("{}"),
		Settle:      settler,
	}
}

func TestDispatchDeadLettersRunawayCounter(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()

	settler := &dispatchSettler{}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "runaway",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     15,
	}
	message := retryBridgeMessage(t, envelope, settler)
	if !dispatchMessage(runner, context.Background(), message, &Envelope{}, new(bool)) {
		t.Fatal("runaway message was not settled")
	}
	if !settler.acked {
		t.Fatal("runaway message was not settled after the poison hand-off")
	}
	if len(producer.messages) != 1 {
		t.Fatalf("poison copies = %d, want 1", len(producer.messages))
	}
	if got := headerValue(producer.messages[0].Headers, "f1deathreason"); got != ReasonPoison.String() {
		t.Fatalf("death reason = %q, want %q", got, ReasonPoison)
	}
}

func TestRetryAfterClampRecordsMetricAndCarriesTier(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.retry.created")
	defer func() { _ = client.Close(context.Background()) }()

	settler := &dispatchSettler{}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "clamped",
		Source:      "/test/orders",
		Type:        "orders.retry.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	message := retryBridgeMessage(t, envelope, settler)
	if !retryAndSettle(runner, context.Background(), message, envelope, RetryAfter(errors.New("slow"), 5*time.Minute)) {
		t.Fatal("retry successor was not published and settled")
	}
	if got := runner.metrics.sampleRetryAfterClamped()["orders.retry.created"]; got != 1 {
		t.Fatalf("clamp samples = %d, want 1", got)
	}
	retryDestination := producer.messages[0].Destination
	runner.retryDestinationTiers = map[string]int{retryDestination: 2}
	inbound := driver.InboundMessage{
		Destination: retryDestination,
		Headers:     producer.messages[0].Headers,
	}
	if got, want := deliveryLane(runner, inbound), schedulerLaneID("orders.retry.created", PriorityHigh, 2); got != want {
		t.Fatalf("delivery lane = %q, want %q", got, want)
	}
	runner.retryDestinationTiers[retryDestination] = 1
	if got, want := deliveryLane(runner, inbound), schedulerLaneID("orders.retry.created", PriorityHigh, 1); got != want {
		t.Fatalf("wrong destination tier lane = %q, want %q", got, want)
	}
}

func TestRetryAndSettleIncrementsAttempt(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.retry.created")
	defer func() { _ = client.Close(context.Background()) }()

	settler := &dispatchSettler{}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "ordinary-retry",
		Source:      "/test/orders",
		Type:        "orders.retry.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	message := retryBridgeMessage(t, envelope, settler)
	if !retryAndSettle(runner, context.Background(), message, envelope, errors.New("temporary")) {
		t.Fatal("ordinary retry was not published and settled")
	}
	if len(producer.messages) != 1 {
		t.Fatalf("retry copies = %d, want 1", len(producer.messages))
	}
	got, err := DecodeHeaders(inboundHeaders(producer.messages[0].Headers))
	if err != nil {
		t.Fatal(err)
	}
	if got.Attempt != 2 {
		t.Fatalf("retry attempt = %d, want 2", got.Attempt)
	}
}

func TestOpenRunnerConsumerBuildsRetryDestinationTiers(t *testing.T) {
	consumer := newDispatchConsumer()
	producer := &dispatchProducer{}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: &dispatchConn{producer: producer, consumer: consumer, admin: &dispatchAdmin{}}}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()
	runner := &Runner{
		client: client,
		subscription: Subscription{
			Name:       "orders",
			Topics:     []string{"orders.created"},
			Priorities: []Priority{PriorityHigh},
			Retry:      RetryConfig{MaxAttempts: 3},
		},
		config: SubscriptionConfig{Prefetch: 1},
	}
	if _, err := openRunnerConsumer(runner, context.Background()); err != nil {
		t.Fatal(err)
	}
	wantDestination := retryDestinationFor(client.source, "orders.created", PriorityHigh, 2, "orders")
	if got := runner.retryDestinationTiers[wantDestination]; got != 2 {
		t.Fatalf("retry tier for %q = %d, want 2", wantDestination, got)
	}
}

func TestRetryDestinationTierMapMatchesSubscriptionDestinations(t *testing.T) {
	source := "/test/orders"
	sub := Subscription{
		Name:       "orders",
		Topics:     []string{"orders.created", "payments.created"},
		Priorities: []Priority{PriorityHigh, PriorityNormal},
		Retry: RetryConfig{
			Tiers: []time.Duration{time.Second, 2 * time.Second},
		},
	}
	effective := driver.Capabilities{Fanout: driver.FanoutAtConsume}
	destinations := subscriptionDestinations(effective, source, sub)

	mainDestinations := make(map[string]struct{})
	for _, topic := range sub.Topics {
		logical := topicFor(topic)
		for _, priority := range sub.Priorities {
			mainDestinations[consumeDestination(effective, source, logical, priority, sub.Name)] = struct{}{}
		}
	}
	wantRetry := make(map[string]struct{})
	for _, destination := range destinations {
		if _, ok := mainDestinations[destination]; !ok {
			wantRetry[destination] = struct{}{}
		}
	}

	gotRetry := retryDestinationTierMap(source, sub)
	if len(gotRetry) != len(wantRetry) {
		t.Fatalf("retry destination count = %d, want %d", len(gotRetry), len(wantRetry))
	}
	for destination := range wantRetry {
		if _, ok := gotRetry[destination]; !ok {
			t.Errorf("retry destination %q is missing from tier map", destination)
		}
	}
	for destination := range gotRetry {
		if _, ok := wantRetry[destination]; !ok {
			t.Errorf("tier map contains non-retry destination %q", destination)
		}
	}
}

type retryBridgeFailingProducer struct{}

func (*retryBridgeFailingProducer) Publish(context.Context, ...driver.OutboundMessage) error {
	return errors.New("successor publish failed")
}

func (*retryBridgeFailingProducer) Flush(context.Context) error { return nil }

func (*retryBridgeFailingProducer) Close(context.Context) error { return nil }

type retryBridgeSettler struct {
	acks  int
	nacks []driver.NackOptions
}

func (s *retryBridgeSettler) Ack(context.Context) error {
	s.acks++
	return nil
}

func (s *retryBridgeSettler) Nack(_ context.Context, options driver.NackOptions) error {
	s.nacks = append(s.nacks, options)
	return nil
}

func TestFailedSuccessorHandoffsDoNotBareRequeue(t *testing.T) {
	cases := []struct {
		name    string
		handOff func(*Runner, context.Context, driver.InboundMessage, Envelope, *deliveryState) bool
	}{
		{
			name: "retry",
			handOff: func(r *Runner, ctx context.Context, message driver.InboundMessage, envelope Envelope, state *deliveryState) bool {
				return retryAndSettle(r, ctx, message, envelope, errors.New("temporary"), state)
			},
		},
		{
			name: "dead-letter",
			handOff: func(r *Runner, ctx context.Context, message driver.InboundMessage, envelope Envelope, state *deliveryState) bool {
				return deadLetterAndSettle(r, ctx, message, envelope, ReasonTerminal, errors.New("terminal"), state)
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			producer := &dispatchProducer{}
			client, runner := newRetryBridgeRunner(t, producer, "orders.created")
			defer func() { _ = client.Close(context.Background()) }()
			client.producerHandle = &retryBridgeFailingProducer{}
			settler := &retryBridgeSettler{}
			envelope := Envelope{
				SpecVersion: "1.0",
				ID:          "handoff-failure",
				Source:      "/test/orders",
				Type:        "orders.created.v1",
				Priority:    PriorityHigh,
				Attempt:     1,
			}
			message := retryBridgeMessage(t, envelope, settler)
			state := &deliveryState{}
			if testCase.handOff(runner, context.Background(), message, envelope, state) {
				t.Fatal("failed successor hand-off was reported as successful")
			}
			if len(settler.nacks) != 1 {
				t.Fatalf("Nack calls = %d, want 1", len(settler.nacks))
			}
			if settler.nacks[0].Requeue || !settler.nacks[0].CountAsFailure {
				t.Fatalf("fallback Nack options = %+v, want non-requeue failure", settler.nacks[0])
			}
			if settler.acks != 0 {
				t.Fatalf("Ack calls = %d, want 0", settler.acks)
			}
		})
	}
}
