package f1

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// scriptedNackSettler models the port contract for a failed negative
// acknowledgement: a transient failure returns before the driver records the
// settlement, so the driver still owns the delivery until a Nack returns nil.
type scriptedNackSettler struct {
	mu          sync.Mutex
	failFirst   int
	nackCalls   int
	ackCalls    int
	outstanding bool
	onAck       func()
	onNack      func()
}

func (s *scriptedNackSettler) Ack(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ackCalls++
	if s.onAck != nil {
		s.onAck()
	}
	return nil
}

func (s *scriptedNackSettler) Nack(context.Context, driver.NackOptions) error {
	s.mu.Lock()
	s.nackCalls++
	hook := s.onNack
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nackCalls <= s.failFirst {
		s.outstanding = true
		return &driver.Error{Driver: "test", Op: "nack", K: driver.KindTransient, Err: errors.New("injected transient nack failure")}
	}
	s.outstanding = false
	return nil
}

func (s *scriptedNackSettler) state() (nacks, acks int, outstanding bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nackCalls, s.ackCalls, s.outstanding
}

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
	return client, runner, consumer
}

// TestRetriedDeliveryDoesNotCountAsHandled pins the flag the reconnect
// decision reads: only a delivery the handler completed counts the
// generation as having one, so a delivery that settles by publishing a retry
// copy must leave it false.
func TestRetriedDeliveryDoesNotCountAsHandled(t *testing.T) {
	newRunner := func(t *testing.T, handler Handler) (*dispatchProducer, *Runner) {
		t.Helper()
		producer := &dispatchProducer{}
		client, err := New(context.Background(), testClientConfig(t), WithDriver(&dispatchDriver{conn: &dispatchConn{producer: producer}}))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close(context.Background()) })
		return producer, &Runner{
			client: client,
			subscription: Subscription{
				Name:           "orders",
				Topics:         []string{"orders.created"},
				Priorities:     []Priority{PriorityHigh},
				Retry:          RetryConfig{MaxAttempts: 2, InitialInterval: time.Second},
				HandlerTimeout: time.Second,
				Handlers:       map[string]Handler{"orders.created.v1": handler},
			},
			inflight: newInflightRegistry(),
		}
	}
	envelope := func(id string) Envelope {
		return Envelope{SpecVersion: "1.0", ID: id, Source: "/test/orders", Type: "orders.created.v1", Priority: PriorityHigh, Attempt: 1}
	}

	t.Run("retried delivery", func(t *testing.T) {
		producer, runner := newRunner(t, HandlerFunc(func(context.Context, *Event) error {
			return errors.New("handler failed")
		}))
		settler := &dispatchSettler{}
		message := retryBridgeMessage(t, envelope("retried"), settler)
		id := runner.inflight.Add(message)
		processDelivery(runner, context.Background(), delivery{id: id, message: message})

		if got := len(producer.messages); got != 1 {
			t.Fatalf("publishes after an ordinary handler error = %d, want one retry copy", got)
		}
		if destination := producer.messages[0].Destination; !strings.Contains(destination, "."+retryDestinationSegment+".") {
			t.Fatalf("retry successor destination = %q, want the retry destination", destination)
		}
		if !settler.acked || settler.nacked {
			t.Fatalf("retry settlement = acked %t nacked %t, want the original acked once its retry copy was published", settler.acked, settler.nacked)
		}
		if runner.successfulDelivery {
			t.Fatal("a generation whose only delivery was retried counted it as handled")
		}
	})

	t.Run("handled delivery", func(t *testing.T) {
		producer, runner := newRunner(t, HandlerFunc(func(context.Context, *Event) error {
			return nil
		}))
		settler := &dispatchSettler{}
		message := retryBridgeMessage(t, envelope("handled"), settler)
		id := runner.inflight.Add(message)
		processDelivery(runner, context.Background(), delivery{id: id, message: message})

		if got := len(producer.messages); got != 0 {
			t.Fatalf("publishes after a handled delivery = %d, want none", got)
		}
		if !settler.acked || settler.nacked {
			t.Fatalf("handled settlement = acked %t nacked %t, want one ack", settler.acked, settler.nacked)
		}
		if !runner.successfulDelivery {
			t.Fatal("a handled delivery did not count the generation as having one")
		}
	})
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

// TestGenerationStartReplacesExpiredSettlementContext spans two generations
// and crosses the settlement budget. Generation 1 is abandoned for a
// reconnect, which cancels its run context; the first settlement that runs
// after that installs the runner's settlement context with a full drain
// budget starting then. The runner lives longer than that budget, so by the
// time a later generation drains at Close the inherited context is already
// dead - and first-wins kept it, so every drain settlement failed on an
// expired context without reaching the driver. Starting a generation must
// retire the previous generation's settlement context so the next install
// carries a fresh, live budget.
func TestGenerationStartReplacesExpiredSettlementContext(t *testing.T) {
	_, runner, consumer := newSettlementOrderingRunner(t)

	runner.client.config.Lifecycle.DrainTimeout = 30 * time.Millisecond

	fallback, cancelFallback := context.WithCancel(context.Background())
	cancelFallback()

	generationOne := runnerSettlementContext(runner, fallback)
	if err := generationOne.Err(); err != nil {
		t.Fatalf("installed err right after install: %v", err)
	}

	deadline := clock.NewReal().Timer(5 * time.Second)
	defer deadline.Stop()
	for generationOne.Err() == nil {
		select {
		case <-deadline.C:
			t.Fatal("settlement context did not expire within 5s of a 30ms budget")
		default:
			_ = clock.NewReal().Sleep(context.Background(), 5*time.Millisecond)
		}
	}
	if !errors.Is(generationOne.Err(), context.DeadlineExceeded) {
		t.Fatalf("generation one settlement context ended with %v, want context deadline exceeded", generationOne.Err())
	}

	cancelGenerationTwo := func() {}
	beginRunnerGeneration(runner, fallback, cancelGenerationTwo)

	if err := fetchRunnerAfterCancel(runner, fallback, consumer.Messages(), make(chan delivery, 1)); err != nil {
		t.Fatalf("fetchRunnerAfterCancel() = %v", err)
	}

	runner.mu.Lock()
	handedOut := runner.settleCtx
	runner.mu.Unlock()
	if handedOut == nil {
		t.Fatal("drain setup installed no settlement context")
	}
	if handedOut == generationOne {
		t.Fatal("drain setup kept the previous generation's expired settlement context")
	}
	if err := handedOut.Err(); err != nil {
		t.Fatalf("STALE: drain settlement context already expired: %v", err)
	}
}

// TestGenerationStartDoesNotCancelCapturedSettlementContext pins the way the
// per-generation reset retires the settlement context: it drops the field,
// never calls the old cancel. A cleanup goroutine from the previous
// generation captured the context value, not the field, and may still be
// about to reach the driver on it; cancelling it there would reintroduce the
// exact defect first-wins exists to prevent, one level up. The reset must
// retire both fields together: a stale cancel left behind would be invoked
// by finishRunner at teardown, cancelling a straggler's context there
// instead.
func TestGenerationStartDoesNotCancelCapturedSettlementContext(t *testing.T) {
	_, runner, _ := newSettlementOrderingRunner(t)

	fallback, cancelFallback := context.WithCancel(context.Background())
	cancelFallback()

	installed := runnerSettlementContext(runner, fallback)

	cancelNextGeneration := func() {}
	beginRunnerGeneration(runner, fallback, cancelNextGeneration)

	if err := installed.Err(); err != nil {
		t.Fatalf("captured settlement context from the previous generation ended with %v when the next generation started; starting a generation must drop the old context, not cancel it", err)
	}

	finishRunner(runner)

	if err := installed.Err(); err != nil {
		t.Fatalf("captured settlement context ended with %v at teardown; a generation start that retires the context must retire its cancel with it", err)
	}
}

// TestCleanupKeepsUnsettledDeliveryAccountedUntilSettled pins the settlement
// oracle: the inflight registry may only report zero for a delivery the
// driver no longer owns. A delivery whose cleanup nacks fail transiently is
// retained by the driver on purpose; the deferred cleanup must keep retrying
// within its bounded budget, and the registry entry must stay until a
// settlement call actually succeeds.
func TestCleanupKeepsUnsettledDeliveryAccountedUntilSettled(t *testing.T) {
	_, runner, _ := newSettlementOrderingRunner(t)

	// The first nack runs while the driver still owns the delivery, which is
	// the moment a drain must not read zero. Record what the registry says
	// there: the entry is present, and WaitZero reports the cancelled context
	// instead of completing.
	var lenDuringFirstNack int
	var waitErrDuringFirstNack error
	settler := &scriptedNackSettler{failFirst: 2}
	settler.onNack = func() {
		settler.mu.Lock()
		first := settler.nackCalls == 1
		settler.mu.Unlock()
		if !first {
			return
		}
		lenDuringFirstNack = runner.inflight.Len()
		waitCtx, cancelWait := context.WithCancel(context.Background())
		cancelWait()
		waitErrDuringFirstNack = runner.inflight.WaitZero(waitCtx)
	}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "unsettled-cleanup",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	message := retryBridgeMessage(t, envelope, settler)
	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(ctx context.Context, _ *Event) error {
			<-ctx.Done()
			return ctx.Err()
		}),
	}

	ctx, cancelRun := context.WithCancel(context.Background())
	cancelRun()

	id := runner.inflight.Add(message)
	processDelivery(runner, ctx, delivery{id: id, message: message})

	nacks, acks, outstanding := settler.state()
	if acks != 0 {
		t.Fatalf("Ack calls = %d, want 0: an abandoned delivery settles by requeue", acks)
	}
	if nacks < 3 {
		t.Fatalf("Nack calls = %d, want at least 3: two transient failures must not exhaust the cleanup budget", nacks)
	}
	if outstanding {
		if runner.inflight.Len() == 0 {
			t.Fatal("inflight registry reported zero while the driver still owned the unsettled delivery")
		}
		t.Fatal("delivery was never settled despite remaining cleanup budget")
	}
	if lenDuringFirstNack != 1 {
		t.Fatalf("inflight length during the first failed nack = %d, want 1: the registry entry must stay while the driver still owns the delivery", lenDuringFirstNack)
	}
	if !errors.Is(waitErrDuringFirstNack, context.Canceled) {
		t.Fatalf("WaitZero during the failed settlement = %v, want context canceled: a wait must not report zero while a delivery is unsettled", waitErrDuringFirstNack)
	}
	if got := runner.inflight.Len(); got != 0 {
		t.Fatalf("inflight length after the delivery settled = %d, want 0", got)
	}
	if err := runner.inflight.WaitZero(context.Background()); err != nil {
		t.Fatalf("WaitZero() after the delivery settled = %v, want nil", err)
	}
}

// TestCleanupRemovesDeliveryOnlyWhenSettlementBudgetExhausts pins the other
// side of the cleanup bound: when every settlement attempt within the
// bounded budget fails transiently, the delivery is removed rather than
// retried forever, because a drain must terminate. The registry entry stays
// for the whole retry budget and goes only when the budget does, so the test
// reads the registry inside the budget as well as at the end.
func TestCleanupRemovesDeliveryOnlyWhenSettlementBudgetExhausts(t *testing.T) {
	_, runner, _ := newSettlementOrderingRunner(t)

	settler := &scriptedNackSettler{failFirst: 99}
	// Every attempt inside the budget must still see the delivery: an entry
	// removed early would let a drain read zero while the broker still owns
	// the message.
	var lenDuringBudget []int
	settler.onNack = func() {
		lenDuringBudget = append(lenDuringBudget, runner.inflight.Len())
	}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "budget-exhausted",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	message := retryBridgeMessage(t, envelope, settler)
	runner.subscription.Handlers = map[string]Handler{
		"orders.created.v1": HandlerFunc(func(ctx context.Context, _ *Event) error {
			<-ctx.Done()
			return ctx.Err()
		}),
	}

	ctx, cancelRun := context.WithCancel(context.Background())
	cancelRun()

	id := runner.inflight.Add(message)
	processDelivery(runner, ctx, delivery{id: id, message: message})

	nacks, _, outstanding := settler.state()
	if nacks != 1+settlementRetryAttempts {
		t.Fatalf("Nack calls = %d, want %d: the initial attempt plus the bounded retry rounds", nacks, 1+settlementRetryAttempts)
	}
	if !outstanding {
		t.Fatal("settler reported the delivery settled, want retained")
	}
	if len(lenDuringBudget) != 1+settlementRetryAttempts {
		t.Fatalf("registry reads inside the budget = %d, want %d: one per settlement attempt", len(lenDuringBudget), 1+settlementRetryAttempts)
	}
	for attempt, length := range lenDuringBudget {
		if length != 1 {
			t.Fatalf("inflight length during settlement attempt %d = %d, want 1: the entry stays for the whole retry budget", attempt+1, length)
		}
	}
	if got := runner.inflight.Len(); got != 0 {
		t.Fatalf("inflight length = %d, want 0 once the settlement budget is exhausted", got)
	}
	if err := runner.inflight.WaitZero(context.Background()); err != nil {
		t.Fatalf("WaitZero() after the budget was exhausted = %v, want nil: a drain must terminate", err)
	}
}
