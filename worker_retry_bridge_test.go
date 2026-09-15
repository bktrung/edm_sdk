package f1

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func newRetryBridgeRunner(t *testing.T, producer *dispatchProducer, topic string, options ...Option) (*Client, *Runner) {
	t.Helper()
	options = append([]Option{WithDriver(&dispatchDriver{conn: &dispatchConn{producer: producer}})}, options...)
	client, err := New(context.Background(), testClientConfig(t), options...)
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

type missingRouteProducer struct {
	attempts int
	kindOnly bool
}

func (p *missingRouteProducer) Publish(_ context.Context, messages ...driver.OutboundMessage) error {
	p.attempts += len(messages)
	failed := make(map[int]error, len(messages))
	for i := range messages {
		cause := error(driver.ErrDestinationMissing)
		if p.kindOnly {
			cause = errors.New("dead-letter route missing")
		}
		failed[i] = &driver.Error{Driver: "test", Op: "publish", K: driver.KindNotFound, Err: cause}
	}
	return &driver.PublishError{Failed: failed}
}

func (*missingRouteProducer) Close(context.Context) error { return nil }

func noRoutePoisonRunner(t *testing.T, producer driver.Producer, options ...Option) (*Client, *Runner) {
	t.Helper()
	options = append([]Option{WithDriver(&dispatchDriver{conn: &dispatchConn{producerOverride: producer}})}, options...)
	client, err := New(context.Background(), testClientConfig(t), options...)
	if err != nil {
		t.Fatal(err)
	}
	runner := &Runner{
		client: client,
		subscription: Subscription{
			Name:           "orders",
			Topics:         []string{"orders.created"},
			Priorities:     []Priority{PriorityHigh},
			Retry:          RetryConfig{MaxAttempts: 3, Tiers: []time.Duration{time.Second}},
			HandlerTimeout: time.Second,
		},
	}
	return client, runner
}

func poisonEnvelope() Envelope {
	return Envelope{
		SpecVersion: "1.0",
		ID:          "poison-no-route",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     15,
	}
}

func TestDispatchDropsPoisonWithoutDLQRoute(t *testing.T) {
	producer := &missingRouteProducer{}
	recorder := newErrorHandlerRecorder()
	client, runner := noRoutePoisonRunner(t, producer, WithErrorHandler(recorder.handle))
	defer func() { _ = client.Close(context.Background()) }()

	settler := &dispatchSettler{}
	message := retryBridgeMessage(t, poisonEnvelope(), settler)
	if !dispatchMessage(runner, context.Background(), message, &Envelope{}, new(bool)) {
		t.Fatal("poison message was not settled as handled")
	}
	if !settler.acked || settler.nacked {
		t.Fatalf("poison settlement = acked %t nacked %t, want ack only", settler.acked, settler.nacked)
	}
	if producer.attempts != maxSuccessorPublishAttempts {
		t.Fatalf("missing-route publish attempts = %d, want %d", producer.attempts, maxSuccessorPublishAttempts)
	}
	recorder.waitForCall(t, time.Second)
	if got := recorder.count(); got != 1 {
		t.Fatalf("poison error handler calls = %d, want 1", got)
	}
	recorder.mu.Lock()
	call := recorder.calls[0]
	recorder.mu.Unlock()
	if call.event == nil || call.event.ID() != poisonEnvelope().ID || call.event.Attempt() != poisonEnvelope().Attempt {
		t.Fatalf("poison error event = %#v, want id %q attempt %d", call.event, poisonEnvelope().ID, poisonEnvelope().Attempt)
	}
	for _, want := range []string{poisonEnvelope().ID, message.Destination, "attempt=15", "no dead-letter route available"} {
		if !strings.Contains(call.err.Error(), want) {
			t.Fatalf("poison error = %q, missing %q", call.err, want)
		}
	}
}

func TestDispatchDropsPoisonForClassifiedMissingRoute(t *testing.T) {
	producer := &missingRouteProducer{kindOnly: true}
	client, runner := noRoutePoisonRunner(t, producer)
	defer func() { _ = client.Close(context.Background()) }()

	settler := &dispatchSettler{}
	if !dispatchMessage(runner, context.Background(), retryBridgeMessage(t, poisonEnvelope(), settler), &Envelope{}, new(bool)) {
		t.Fatal("classified missing-route poison was not settled as handled")
	}
	if !settler.acked || settler.nacked {
		t.Fatalf("classified missing-route settlement = acked %t nacked %t, want ack only", settler.acked, settler.nacked)
	}
}

func TestDispatchPoisonNoRouteDoesNotRedeliver(t *testing.T) {
	producer := &missingRouteProducer{}
	client, runner := noRoutePoisonRunner(t, producer)
	defer func() { _ = client.Close(context.Background()) }()

	settler := &dispatchSettler{}
	message := retryBridgeMessage(t, poisonEnvelope(), settler)
	deliveries := 0
	for deliveries < maxSuccessorPublishAttempts+1 {
		deliveries++
		if dispatchMessage(runner, context.Background(), message, &Envelope{}, new(bool)) {
			break
		}
	}
	if deliveries != 1 || !settler.acked {
		t.Fatalf("poison deliveries = %d acked=%t, want one handled delivery", deliveries, settler.acked)
	}
}

func TestDispatchPoisonNoRouteUsesLastResortLogger(t *testing.T) {
	var logs logSink
	producer := &missingRouteProducer{}
	client, runner := noRoutePoisonRunner(t, producer, WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	defer func() { _ = client.Close(context.Background()) }()

	if !dispatchMessage(runner, context.Background(), retryBridgeMessage(t, poisonEnvelope(), &dispatchSettler{}), &Envelope{}, new(bool)) {
		t.Fatal("poison message was not settled as handled")
	}
	output := logs.String()
	for _, want := range []string{"poison message dropped", poisonEnvelope().ID, "destination", "attempt", "no dead-letter route"} {
		if !strings.Contains(output, want) {
			t.Fatalf("last-resort poison log = %q, missing %q", output, want)
		}
	}
}

func TestDispatchDiscardedDeathDetailsUsesConfiguredLogger(t *testing.T) {
	defaultOutput := captureProcessDefault(t)
	var configuredOutput logSink
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created",
		WithLogger(slog.New(slog.NewTextHandler(&configuredOutput, nil))),
	)
	defer func() { _ = client.Close(context.Background()) }()

	innerDetails := map[string]string{"tenant": "acme"}
	outerDetails := make(map[string]string, 5)
	for i := range 5 {
		innerDetails[fmt.Sprintf("BAD%02d", i)] = fmt.Sprintf("secret-%02d", i)
		outerDetails[fmt.Sprintf("BAD%02d", i+5)] = fmt.Sprintf("secret-%02d", i+5)
	}
	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error {
			err := WithDetails(errors.New("terminal failure"), innerDetails)
			return Terminal(WithDetails(err, outerDetails))
		}),
	}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "discarded-details-configured",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	if !dispatchMessage(runner, context.Background(), retryBridgeMessage(t, envelope, &dispatchSettler{}), &Envelope{}, new(bool)) {
		t.Fatal("terminal message was not settled")
	}

	output := configuredOutput.String()
	if !strings.Contains(output, "f1 death details discarded") {
		t.Fatalf("configured logger output = %q, want discard warning", output)
	}
	if strings.Count(output, "f1 death details discarded") != 1 {
		t.Fatalf("configured logger output = %q, want one discard warning", output)
	}
	for i := range maxLoggedDeathDetailKeys {
		if !strings.Contains(output, fmt.Sprintf("BAD%02d", i)) {
			t.Fatalf("configured logger output = %q, missing logged key BAD%02d", output, i)
		}
	}
	for i := maxLoggedDeathDetailKeys; i < 10; i++ {
		if strings.Contains(output, fmt.Sprintf("BAD%02d", i)) {
			t.Fatalf("configured logger output = %q, includes uncapped key BAD%02d", output, i)
		}
	}
	for i := range 10 {
		if strings.Contains(output, fmt.Sprintf("secret-%02d", i)) {
			t.Fatalf("configured logger output = %q, logged detail value", output)
		}
	}
	for _, want := range []string{"subscription=orders", "event_id=discarded-details-configured", "count=10"} {
		if !strings.Contains(output, want) {
			t.Fatalf("configured logger output = %q, missing %q", output, want)
		}
	}
	if got := defaultOutput.String(); got != "" {
		t.Fatalf("process default output = %q, want empty", got)
	}
}

func TestDispatchDiscardedDeathDetailsUsesProcessDefaultWithoutLogger(t *testing.T) {
	defaultOutput := captureProcessDefault(t)
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()

	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error {
			return Terminal(WithDetails(errors.New("terminal failure"), map[string]string{"BAD_KEY": "secret"}))
		}),
	}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "discarded-details-default",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	if !dispatchMessage(runner, context.Background(), retryBridgeMessage(t, envelope, &dispatchSettler{}), &Envelope{}, new(bool)) {
		t.Fatal("terminal message was not settled")
	}

	output := defaultOutput.String()
	for _, want := range []string{
		"f1 death details discarded",
		"subscription=orders",
		"event_id=discarded-details-default",
		"keys=[BAD_KEY]",
		"count=1",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("process default output = %q, missing %q", output, want)
		}
	}
	if strings.Contains(output, "secret") {
		t.Fatalf("process default output = %q, logged detail value", output)
	}
}

func TestDispatchCustomDeathDetailsValidatesKeys(t *testing.T) {
	var configuredOutput logSink
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created",
		WithLogger(slog.New(slog.NewTextHandler(&configuredOutput, nil))),
	)
	defer func() { _ = client.Close(context.Background()) }()

	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error {
			return Terminal(customDeathDetailError{details: map[string]string{
				"tenant":  "acme",
				"BAD_KEY": "secret",
			}})
		}),
	}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "custom-discarded-details",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	if !dispatchMessage(runner, context.Background(), retryBridgeMessage(t, envelope, &dispatchSettler{}), &Envelope{}, new(bool)) {
		t.Fatal("custom carrier message was not settled")
	}
	if got := headerValue(producer.messages[0].Headers, "f1detailtenant"); got != "acme" {
		t.Fatalf("custom detail tenant = %q, want acme", got)
	}
	if got := headerValue(producer.messages[0].Headers, "f1detailBAD_KEY"); got != "" {
		t.Fatalf("custom invalid detail = %q, want omitted", got)
	}
	output := configuredOutput.String()
	for _, want := range []string{"f1 death details discarded", "keys=[BAD_KEY]", "count=1"} {
		if !strings.Contains(output, want) {
			t.Fatalf("configured logger output = %q, missing %q", output, want)
		}
	}
	if strings.Contains(output, "secret") {
		t.Fatalf("configured logger output = %q, logged detail value", output)
	}
}

func TestDispatchValidDeathDetailsDoNotWarn(t *testing.T) {
	var configuredOutput logSink
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created",
		WithLogger(slog.New(slog.NewTextHandler(&configuredOutput, nil))),
	)
	defer func() { _ = client.Close(context.Background()) }()

	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error {
			return Terminal(WithDetails(errors.New("terminal failure"), map[string]string{"tenant": "acme"}))
		}),
	}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "valid-details-no-warning",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	if !dispatchMessage(runner, context.Background(), retryBridgeMessage(t, envelope, &dispatchSettler{}), &Envelope{}, new(bool)) {
		t.Fatal("terminal message was not settled")
	}
	if output := configuredOutput.String(); strings.Contains(output, "f1 death details discarded") {
		t.Fatalf("configured logger output = %q, want no discard warning", output)
	}
}

func TestDispatchLadderExhaustionKeepsMaxAttemptsReason(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()

	envelope := poisonEnvelope()
	envelope.ID = "ladder-exhausted"
	envelope.Attempt = 3
	envelope.MaxAttempts = 3
	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error { return errors.New("temporary") }),
	}
	if !dispatchMessage(runner, context.Background(), retryBridgeMessage(t, envelope, &dispatchSettler{}), &Envelope{}, new(bool)) {
		t.Fatal("ladder-exhausted message was not settled")
	}
	if got := headerValue(producer.messages[0].Headers, "f1deathreason"); got != ReasonMaxAttempts.String() {
		t.Fatalf("ladder exhaustion reason = %q, want %q", got, ReasonMaxAttempts)
	}
}

type deadLetterTypedError struct{}

func (*deadLetterTypedError) Error() string { return "typed terminal failure" }

func TestDispatchTerminalCarriesDetailsAndUntouchedCallbackError(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()

	handlerErr := &deadLetterTypedError{}
	deadLetters := make(chan DeadLettered, 1)
	runner.subscription.OnDeadLetter = func(_ context.Context, dead DeadLettered) {
		deadLetters <- dead
	}
	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error {
			return Terminal(WithDetails(handlerErr, map[string]string{"tenant": "acme"}))
		}),
	}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "terminal-details",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	if !dispatchMessage(runner, context.Background(), retryBridgeMessage(t, envelope, &dispatchSettler{}), &Envelope{}, new(bool)) {
		t.Fatal("terminal message was not settled")
	}
	if got := headerValue(producer.messages[0].Headers, "f1deathreason"); got != ReasonTerminal.String() {
		t.Fatalf("terminal death reason = %q, want %q", got, ReasonTerminal)
	}
	if got := headerValue(producer.messages[0].Headers, "f1detailtenant"); got != "acme" {
		t.Fatalf("terminal detail tenant = %q, want acme", got)
	}

	timer := clock.NewReal().Timer(time.Second)
	defer timer.Stop()
	select {
	case dead := <-deadLetters:
		var got *deadLetterTypedError
		if !errors.As(dead.LastErr, &got) || got != handlerErr {
			t.Fatalf("DeadLettered.LastErr = %T %v, want original typed error", dead.LastErr, dead.LastErr)
		}
	case <-timer.C:
		t.Fatal("DeadLettered callback did not run")
	}
}

func TestDispatchMaxAttemptsCarriesHandlerDetails(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()

	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error {
			return WithDetails(errors.New("temporary"), map[string]string{"tenant": "acme"})
		}),
	}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "max-attempts-details",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
		MaxAttempts: 1,
	}
	if !dispatchMessage(runner, context.Background(), retryBridgeMessage(t, envelope, &dispatchSettler{}), &Envelope{}, new(bool)) {
		t.Fatal("max-attempts message was not settled")
	}
	if got := headerValue(producer.messages[0].Headers, "f1deathreason"); got != ReasonMaxAttempts.String() {
		t.Fatalf("max-attempts death reason = %q, want %q", got, ReasonMaxAttempts)
	}
	if got := headerValue(producer.messages[0].Headers, "f1detailtenant"); got != "acme" {
		t.Fatalf("max-attempts detail tenant = %q, want acme", got)
	}
}

func TestDispatchRetryCapUsesSubscriptionCeiling(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()

	runner.subscription.Retry.MaxAttempts = 2
	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error {
			return errors.New("temporary")
		}),
	}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "retry-cap-ceiling",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     2,
		MaxAttempts: 100,
	}
	if !dispatchMessage(runner, context.Background(), retryBridgeMessage(t, envelope, &dispatchSettler{}), &Envelope{}, new(bool)) {
		t.Fatal("retry-cap ceiling message was not settled")
	}
	if got := headerValue(producer.messages[0].Headers, "f1deathreason"); got != ReasonMaxAttempts.String() {
		t.Fatalf("retry-cap ceiling death reason = %q, want %q", got, ReasonMaxAttempts)
	}
}

func TestDispatchRetryCapCanLowerSubscriptionPolicy(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()

	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error {
			return errors.New("temporary")
		}),
	}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "retry-cap-lowering",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
		MaxAttempts: 1,
	}
	if !dispatchMessage(runner, context.Background(), retryBridgeMessage(t, envelope, &dispatchSettler{}), &Envelope{}, new(bool)) {
		t.Fatal("retry-cap lowering message was not settled")
	}
	if got := headerValue(producer.messages[0].Headers, "f1deathreason"); got != ReasonMaxAttempts.String() {
		t.Fatalf("retry-cap lowering death reason = %q, want %q", got, ReasonMaxAttempts)
	}
}

func TestDispatchRetryCopyCarriesEffectiveRetryCap(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()

	runner.subscription.Retry.MaxAttempts = 2
	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error {
			return errors.New("temporary")
		}),
	}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "retry-cap-propagation",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
		MaxAttempts: 100,
	}
	if !dispatchMessage(runner, context.Background(), retryBridgeMessage(t, envelope, &dispatchSettler{}), &Envelope{}, new(bool)) {
		t.Fatal("retry-cap propagation message was not settled")
	}
	if got := headerValue(producer.messages[0].Headers, "f1maxattempts"); got != "2" {
		t.Fatalf("retry-copy max attempts = %q, want 2", got)
	}
}

func TestDispatchHandlerSeesSubscriptionRetryCap(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()

	runner.subscription.Retry.MaxAttempts = 2
	var got int
	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(_ context.Context, event *Event) error {
			got = event.MaxAttempts()
			return nil
		}),
	}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "handler-retry-cap",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
		MaxAttempts: 100,
	}
	var dispatched Envelope
	if !dispatchMessage(runner, context.Background(), retryBridgeMessage(t, envelope, &dispatchSettler{}), &dispatched, new(bool)) {
		t.Fatal("handler retry-cap message was not settled")
	}
	if got != 2 {
		t.Fatalf("handler max attempts = %d, want 2", got)
	}
	if dispatched.MaxAttempts != 100 {
		t.Fatalf("dispatch envelope max attempts = %d, want raw value 100", dispatched.MaxAttempts)
	}
}

func TestDispatchHandlerSeesLowerEventRetryCap(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()

	runner.subscription.Retry.MaxAttempts = 3
	var got int
	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(_ context.Context, event *Event) error {
			got = event.MaxAttempts()
			return nil
		}),
	}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "handler-lower-retry-cap",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
		MaxAttempts: 1,
	}
	if !dispatchMessage(runner, context.Background(), retryBridgeMessage(t, envelope, &dispatchSettler{}), &Envelope{}, new(bool)) {
		t.Fatal("lower handler retry-cap message was not settled")
	}
	if got != 1 {
		t.Fatalf("handler max attempts = %d, want 1", got)
	}
}

func TestDispatchHandlerUsesPolicyWhenEventRetryCapAbsent(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()

	runner.subscription.Retry.MaxAttempts = 3
	var got int
	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(_ context.Context, event *Event) error {
			got = event.MaxAttempts()
			return nil
		}),
	}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "handler-default-retry-cap",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	if !dispatchMessage(runner, context.Background(), retryBridgeMessage(t, envelope, &dispatchSettler{}), &Envelope{}, new(bool)) {
		t.Fatal("absent handler retry-cap message was not settled")
	}
	if got != 3 {
		t.Fatalf("handler max attempts = %d, want 3", got)
	}
}

func TestEventFromDeliveryUsesEffectiveRetryCap(t *testing.T) {
	producer := &dispatchProducer{}
	recorder := newErrorHandlerRecorder()
	client, runner := newRetryBridgeRunner(t, producer, "orders.created", WithErrorHandler(recorder.handle))
	defer func() { _ = client.Close(context.Background()) }()
	client.producerHandle = &retryBridgeFailingProducer{}
	runner.subscription.Retry.MaxAttempts = 2

	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "error-handler-retry-cap",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
		MaxAttempts: 100,
	}
	message := retryBridgeMessage(t, envelope, &retryBridgeSettler{})
	if retryAndSettle(runner, context.Background(), message, envelope, errors.New("temporary")) {
		t.Fatal("failed retry successor hand-off was reported as successful")
	}
	recorder.waitForCall(t, time.Second)
	recorder.mu.Lock()
	call := recorder.calls[0]
	recorder.mu.Unlock()
	if call.event == nil {
		t.Fatal("error handler event = nil, want failed delivery event")
	}
	if got := call.event.MaxAttempts(); got != 2 {
		t.Fatalf("error handler max attempts = %d, want 2", got)
	}
}

func TestRetryExhaustionCarriesHandlerDetails(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()
	runner.subscription.Retry.MaxAttempts = 1
	runner.subscription.Retry.Tiers = nil

	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "retry-exhaustion-details",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	err := WithDetails(errors.New("temporary"), map[string]string{"tenant": "acme"})
	if !retryAndSettle(runner, context.Background(), retryBridgeMessage(t, envelope, &dispatchSettler{}), envelope, err) {
		t.Fatal("retry-exhaustion message was not settled")
	}
	if got := headerValue(producer.messages[0].Headers, "f1deathreason"); got != ReasonMaxAttempts.String() {
		t.Fatalf("retry-exhaustion death reason = %q, want %q", got, ReasonMaxAttempts)
	}
	if got := headerValue(producer.messages[0].Headers, "f1detailtenant"); got != "acme" {
		t.Fatalf("retry-exhaustion detail tenant = %q, want acme", got)
	}
}

func TestDispatchPanicDoesNotCarryHandlerDetails(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()

	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error {
			panic(WithDetails(errors.New("panic value"), map[string]string{"tenant": "acme"}))
		}),
	}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "panic-details",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	if !dispatchMessage(runner, context.Background(), retryBridgeMessage(t, envelope, &dispatchSettler{}), &Envelope{}, new(bool)) {
		t.Fatal("panic message was not settled")
	}
	if got := headerValue(producer.messages[0].Headers, "f1deathreason"); got != ReasonPanic.String() {
		t.Fatalf("panic death reason = %q, want %q", got, ReasonPanic)
	}
	assertNoDeathDetailHeaders(t, producer.messages[0].Headers)
}

func TestDispatchNonHandlerDeathsDoNotCarryDetails(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T, *Runner, *dispatchProducer)
	}{
		{
			name: "decode",
			run: func(t *testing.T, runner *Runner, producer *dispatchProducer) {
				envelope := Envelope{
					SpecVersion:  "1.0",
					ID:           "decode-details",
					Source:       "/test/orders",
					Type:         "orders.created.v1",
					Priority:     PriorityHigh,
					Attempt:      1,
					DeathDetails: map[string]string{"tenant": "acme"},
				}
				message := retryBridgeMessage(t, envelope, &dispatchSettler{})
				for i := range message.Headers {
					if message.Headers[i].Key == "time" {
						message.Headers[i].Value = []byte("not-a-time")
					}
				}
				if !dispatchMessage(runner, context.Background(), message, &Envelope{}, new(bool)) {
					t.Fatal("decode message was not settled")
				}
				assertNoDeathDetailHeaders(t, producer.messages[0].Headers)
			},
		},
		{
			name: "expired",
			run: func(t *testing.T, runner *Runner, producer *dispatchProducer) {
				expiry := time.Unix(1, 0).UTC()
				envelope := Envelope{
					SpecVersion:  "1.0",
					ID:           "expired-details",
					Source:       "/test/orders",
					Type:         "orders.created.v1",
					Priority:     PriorityHigh,
					Attempt:      1,
					Expiry:       &expiry,
					DeathDetails: map[string]string{"tenant": "acme"},
				}
				if !dispatchMessage(runner, context.Background(), retryBridgeMessage(t, envelope, &dispatchSettler{}), &Envelope{}, new(bool)) {
					t.Fatal("expired message was not settled")
				}
				assertNoDeathDetailHeaders(t, producer.messages[0].Headers)
			},
		},
		{
			name: "unmatched",
			run: func(t *testing.T, runner *Runner, producer *dispatchProducer) {
				runner.subscription.UnmatchedPolicy = DeadLetter
				envelope := Envelope{
					SpecVersion:  "1.0",
					ID:           "unmatched-details",
					Source:       "/test/orders",
					Type:         "orders.unknown.v1",
					Priority:     PriorityHigh,
					Attempt:      1,
					DeathDetails: map[string]string{"tenant": "acme"},
				}
				if !dispatchMessage(runner, context.Background(), retryBridgeMessage(t, envelope, &dispatchSettler{}), &Envelope{}, new(bool)) {
					t.Fatal("unmatched message was not settled")
				}
				assertNoDeathDetailHeaders(t, producer.messages[0].Headers)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			producer := &dispatchProducer{}
			client, runner := newRetryBridgeRunner(t, producer, "orders.created")
			defer func() { _ = client.Close(context.Background()) }()
			test.run(t, runner, producer)
		})
	}
}

func TestDispatchPlainErrorPreservesDeathError(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()

	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(context.Context, *Event) error {
			return errors.New("plain temporary failure")
		}),
	}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "plain-error",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
		MaxAttempts: 1,
	}
	if !dispatchMessage(runner, context.Background(), retryBridgeMessage(t, envelope, &dispatchSettler{}), &Envelope{}, new(bool)) {
		t.Fatal("plain-error message was not settled")
	}
	if got := headerValue(producer.messages[0].Headers, "f1deatherror"); got != "plain temporary failure" {
		t.Fatalf("plain death error = %q, want unchanged error", got)
	}
}

func assertNoDeathDetailHeaders(t *testing.T, headers []driver.Header) {
	t.Helper()
	for _, header := range headers {
		if strings.HasPrefix(header.Key, "f1detail") {
			t.Fatalf("unexpected death detail header %q", header.Key)
		}
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

func TestRetryAfterClampCarriesTier(t *testing.T) {
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

func TestRetryCopyDropsDeathDetails(t *testing.T) {
	producer := &dispatchProducer{}
	client, runner := newRetryBridgeRunner(t, producer, "orders.retry.created")
	defer func() { _ = client.Close(context.Background()) }()

	envelope := Envelope{
		SpecVersion:  "1.0",
		ID:           "details-retry",
		Source:       "/test/orders",
		Type:         "orders.retry.created.v1",
		Priority:     PriorityHigh,
		Attempt:      1,
		DeathDetails: map[string]string{"tenant": "acme"},
	}
	if !retryAndSettle(runner, context.Background(), retryBridgeMessage(t, envelope, &dispatchSettler{}), envelope, errors.New("temporary")) {
		t.Fatal("ordinary retry was not published and settled")
	}
	if len(producer.messages) != 1 {
		t.Fatalf("retry copies = %d, want 1", len(producer.messages))
	}
	headers := inboundHeaders(producer.messages[0].Headers)
	if _, ok := headers["f1detailtenant"]; ok {
		t.Fatal("retry copy retained death details")
	}
	got, err := DecodeHeaders(headers)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.DeathDetails) != 0 {
		t.Fatalf("retry copy death details = %#v, want empty", got.DeathDetails)
	}
}

func TestRunWithClockTimeoutZeroMeansNoTimeout(t *testing.T) {
	want := errors.New("completed without a deadline")
	got := runWithClockTimeout(context.Background(), nil, 0, "flush", func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			t.Fatal("zero timeout canceled the phase")
		default:
		}
		return want
	})
	if !errors.Is(got, want) {
		t.Fatalf("runWithClockTimeout() error = %v, want %v", got, want)
	}
}

func TestRunWithClockTimeoutRejectsNegativeTimeout(t *testing.T) {
	called := false
	err := runWithClockTimeout(context.Background(), nil, -time.Second, "flush", func(context.Context) error {
		called = true
		return nil
	})
	if err == nil || called {
		t.Fatalf("runWithClockTimeout() error = %v, called = %v; want negative timeout rejection", err, called)
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
		Priorities: []Priority{PriorityHigh, PriorityMedium},
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

// TestFailedSuccessorHandoffsDoNotBareRequeue is a preservation guard: it
// asserts a failed retry or dead-letter hand-off never gives the original
// back to the broker uncounted (a "bare requeue", Requeue: true with no
// failure accounting - the one sanctioned bare requeue is the drain phase,
// not this path). It says nothing about whether the original is
// discarded, which is a separate invariant covered by
// TestFailedSuccessorHandoffLeavesOriginalUnsettled.
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
			for _, nack := range settler.nacks {
				if nack.Requeue {
					t.Fatalf("bare requeue occurred: %+v", nack)
				}
			}
		})
	}
}

// TestFailedSuccessorHandoffLeavesOriginalUnsettled proves the consume-side
// ordering invariant: the original is only released once every
// possible successor is confirmed durable. When the successor publish keeps
// failing, the original must stay completely unsettled - no ack, no nack of
// any kind, including the requeue=false form the SDK previously used - and
// the runner's consumer must be closed so the broker redelivers the
// still-unacked delivery.
func TestFailedSuccessorHandoffLeavesOriginalUnsettled(t *testing.T) {
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
			consumer := newDispatchConsumer()
			runner.consumer = consumer
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
			if settler.acks != 0 {
				t.Fatalf("Ack calls = %d, want 0: original must stay unsettled", settler.acks)
			}
			if len(settler.nacks) != 0 {
				t.Fatalf("Nack calls = %d, want 0: original must stay unsettled, not released via requeue=false", len(settler.nacks))
			}
			if !consumer.stopped {
				t.Fatal("consumer was not stopped after the successor publish exhausted its budget")
			}
		})
	}
}

func TestRunnerReleasesDeliveryAfterSuccessorPublishExhaustsBudget(t *testing.T) {
	producer := &dispatchProducer{}
	consumer := newDispatchConsumer()
	consumer.releasedMessages = make(chan driver.InboundMessage, 1)
	conn := &dispatchConn{producer: producer, consumer: consumer, admin: &dispatchAdmin{}}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: conn}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	settler := &retryBridgeSettler{}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "release-on-handoff-failure",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	message := retryBridgeMessage(t, envelope, settler)
	consumer.open = 1
	consumer.outstanding = []driver.InboundMessage{message}
	consumer.messages <- message

	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:       "orders",
		Topics:     []string{"orders.created"},
		Priorities: []Priority{PriorityHigh},
		Retry:      RetryConfig{MaxAttempts: 3, Tiers: []time.Duration{time.Second}},
		Handlers: map[string]Handler{
			"orders.created.v1": HandlerFunc(func(context.Context, *Event) error {
				return errors.New("temporary handler failure")
			}),
		},
		HandlerTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	client.producerHandle = &retryBridgeFailingProducer{}

	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(context.Background()) }()

	releaseTimer := clock.NewReal().Timer(time.Second)
	defer releaseTimer.Stop()
	select {
	case released := <-consumer.releasedMessages:
		got, err := DecodeHeaders(inboundHeaders(released.Headers))
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != envelope.ID {
			t.Fatalf("released delivery ID = %q, want %q", got.ID, envelope.ID)
		}
	case <-releaseTimer.C:
		t.Fatal("delivery was not released back to the broker after successor publish exhaustion")
	}

	runTimer := clock.NewReal().Timer(time.Second)
	defer runTimer.Stop()
	select {
	case err := <-runDone:
		if err == nil {
			t.Fatal("runner stopped without reporting the successor handoff failure")
		}
	case <-runTimer.C:
		t.Fatal("runner continued consuming after releasing the failed successor delivery")
	}
	if !consumer.stopped {
		t.Fatal("consumer was not closed after Release")
	}
}

type unsupportedReleaseConsumer struct {
	stopCalls int
}

func (*unsupportedReleaseConsumer) Messages() <-chan driver.InboundMessage { return nil }
func (*unsupportedReleaseConsumer) Errors() <-chan error                   { return nil }
func (*unsupportedReleaseConsumer) Pause(...string) error                  { return nil }
func (*unsupportedReleaseConsumer) Resume(...string) error                 { return nil }
func (*unsupportedReleaseConsumer) Drain(context.Context) error            { return nil }
func (c *unsupportedReleaseConsumer) Stop(context.Context) error {
	c.stopCalls++
	return nil
}

func (*unsupportedReleaseConsumer) Release(context.Context) error {
	return driver.ErrUnsupported
}

func (*unsupportedReleaseConsumer) Lag(context.Context) (map[string]int64, error) {
	return nil, driver.ErrUnsupported
}

func TestReleaseRunnerConsumerSurfacesUnsupportedWithoutStop(t *testing.T) {
	consumer := &unsupportedReleaseConsumer{}
	runner := &Runner{consumer: consumer}

	err := releaseRunnerConsumer(runner, context.Background())
	if !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("releaseRunnerConsumer() error = %v, want ErrUnsupported", err)
	}
	if consumer.stopCalls != 0 {
		t.Fatalf("Stop calls = %d, want 0", consumer.stopCalls)
	}
}

func TestDeliveryLaneMalformedHeadersUsesDefaultLane(t *testing.T) {
	client, runner := newRetryBridgeRunner(t, &dispatchProducer{}, "orders.created")
	defer func() { _ = client.Close(context.Background()) }()
	message := driver.InboundMessage{
		Destination: consumeDestination(runner.client.effective, runner.client.source, "orders.created", PriorityLow, runner.subscription.Name),
		Headers: headerSlice(map[string]string{
			"specversion": "1.0",
			"source":      "/test/orders",
			"type":        "orders.created.v1",
			"f1priority":  "low",
		}),
	}
	want := schedulerLaneID("orders.created", runner.subscription.Priorities[0], 0)
	if got := deliveryLane(runner, message); got != want {
		t.Fatalf("delivery lane = %q, want malformed-header fallback %q", got, want)
	}
}
