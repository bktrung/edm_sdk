package f1

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
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

type settleCallerKey struct{}

type settlementContextObserver struct {
	consumeRecordingObserver
	next context.Context
}

func (o *settlementContextObserver) Start(ctx context.Context, event StartEvent) (context.Context, Token) {
	_, token := o.consumeRecordingObserver.Start(ctx, event)
	return o.next, token
}

type contextCheckingSettler struct {
	scriptedNackSettler
	ctx     context.Context
	options driver.NackOptions
}

func (s *contextCheckingSettler) Ack(ctx context.Context) error {
	s.ctx = ctx
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.scriptedNackSettler.Ack(ctx)
}

func (s *contextCheckingSettler) Nack(ctx context.Context, options driver.NackOptions) error {
	s.ctx = ctx
	s.options = options
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.scriptedNackSettler.Nack(ctx, options)
}

func TestObserverSettleCanceledContextDoesNotControlBrokerCalls(t *testing.T) {
	for _, operation := range []SettleOperation{SettleAck, SettleNack} {
		t.Run(string(operation), func(t *testing.T) {
			client, runner, _ := newSettlementOrderingRunner(t)
			canceled, cancel := context.WithCancel(context.Background())
			cancel()
			observer := &settlementContextObserver{next: canceled}
			client.observer = observer
			ctx, cancelCaller := context.WithTimeout(context.WithValue(t.Context(), settleCallerKey{}, "caller"), time.Minute)
			defer cancelCaller()
			settler := &contextCheckingSettler{}
			message := driver.InboundMessage{Destination: "orders.in", Settle: settler}
			state := &deliveryState{}
			options := driver.NackOptions{Requeue: true}
			if operation == SettleAck {
				if !ackDeliveryAs(runner, ctx, message, false, state) {
					t.Fatal("Ack did not settle with live caller context")
				}
			} else if err := nackDelivery(runner, ctx, message, options, state); err != nil {
				t.Fatalf("Nack did not settle with live caller context: %v", err)
			}
			if !state.attempted || !state.settled {
				t.Fatalf("settlement state = %#v, want attempted and settled", state)
			}
			nacks, acks, _ := settler.state()
			if (operation == SettleAck && (acks != 1 || nacks != 0)) ||
				(operation == SettleNack && (nacks != 1 || acks != 0 || settler.options != options)) {
				t.Fatalf("accepted calls = Ack %d Nack %d options %#v", acks, nacks, settler.options)
			}
			deadline, _ := ctx.Deadline()
			gotDeadline, ok := settler.ctx.Deadline()
			if settler.ctx.Err() != nil || settler.ctx.Value(settleCallerKey{}) != "caller" || !ok || gotDeadline != deadline {
				t.Fatal("settler lost the live caller's cancellation, values, or deadline")
			}
			starts, finishes := observer.settlePairs()
			if len(starts) != 1 || len(finishes) != 1 || finishes[0].Outcome != ObserverOutcomeOK {
				t.Fatalf("settle pairs = %#v / %#v, want one successful pair", starts, finishes)
			}
		})
	}
}

func TestObserverLiveContextDoesNotOverrideCallerCancellation(t *testing.T) {
	t.Run("publish", func(t *testing.T) {
		observer := &settlementContextObserver{next: context.Background()}
		producer := &recordingProducer{}
		client := newPublishClient(t, producer, WithObserver(observer))
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := client.Publisher().Publish(ctx, "orders.created.v1", "payload")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Publish error = %v, want caller cancellation", err)
		}
		if len(producer.messages) != 0 {
			t.Fatal("canceled caller published a message")
		}
	})
	t.Run("ack", func(t *testing.T) {
		client, runner, _ := newSettlementOrderingRunner(t)
		client.observer = &settlementContextObserver{next: context.Background()}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		settler := &contextCheckingSettler{}
		state := &deliveryState{}
		if ackDeliveryAs(runner, ctx, driver.InboundMessage{Settle: settler}, false, state) {
			t.Fatal("Ack settled despite caller cancellation")
		}
		if !state.attempted || state.settled || !errors.Is(settler.ctx.Err(), context.Canceled) {
			t.Fatalf("Ack state = %#v, context error = %v, want unsettled caller cancellation", state, settler.ctx.Err())
		}
		_, acks, _ := settler.state()
		if acks != 0 {
			t.Fatal("canceled caller acknowledged a message")
		}
	})
}

// TestRetriedDeliveryDoesNotCountAsHandled pins the report the reconnect
// decision reads: only a delivery the handler completed makes the delivery
// path report a handled delivery, so a delivery that settles by publishing a
// retry copy must report nothing.
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
			events: make(chan runnerEvent, 8),
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
		id := runner.inflight.Add()
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
		if reportedHandledDeliveries(runner) != 0 {
			t.Fatal("a generation whose only delivery was retried reported it as handled")
		}
	})

	t.Run("handled delivery", func(t *testing.T) {
		producer, runner := newRunner(t, HandlerFunc(func(context.Context, *Event) error {
			return nil
		}))
		settler := &dispatchSettler{}
		message := retryBridgeMessage(t, envelope("handled"), settler)
		id := runner.inflight.Add()
		processDelivery(runner, context.Background(), delivery{id: id, message: message})

		if got := len(producer.messages); got != 0 {
			t.Fatalf("publishes after a handled delivery = %d, want none", got)
		}
		if !settler.acked || settler.nacked {
			t.Fatalf("handled settlement = acked %t nacked %t, want one ack", settler.acked, settler.nacked)
		}
		if reportedHandledDeliveries(runner) != 1 {
			t.Fatal("a handled delivery did not report the generation as having one")
		}
	})
}

// reportedHandledDeliveries counts the handled-delivery reports a runner's
// delivery path has sent to its owner, without waiting for any that have not
// arrived.
func reportedHandledDeliveries(runner *Runner) int {
	reported := 0
	for {
		select {
		case event := <-runner.events:
			if event.kind == runnerEventHandledDelivery {
				reported++
			}
		default:
			return reported
		}
	}
}

// TestDrainSetupKeepsInstalledSettlementContextLive pins the context
// ordering between deferred delivery cleanup and drain setup. A settlement
// call that runs on a context which is already finished is given the runner's
// own settlement context instead, and the drain setup may build one at the
// same moment. Whichever context the cleanup captured must stay live:
// cancelling it underneath a settlement call that is about to reach the
// driver turns that call into a transient context failure before the driver
// ever sees it, and the delivery's remaining cleanup budget is spent
// recovering from a cancellation the runner itself caused.
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

// TestExpiredSettlementWindowIsReplaced pins what a runner does with a
// settlement window whose budget has run out. The runner lives on after it,
// and every settlement that reaches the driver on the expired window fails at
// once without the driver seeing the call, so the next settlement that needs
// the runner's own context is given a fresh one.
func TestExpiredSettlementWindowIsReplaced(t *testing.T) {
	_, runner, _ := newSettlementOrderingRunner(t)

	runner.client.config.Lifecycle.DrainTimeout = 30 * time.Millisecond

	fallback, cancelFallback := context.WithCancel(context.Background())
	cancelFallback()

	first := runnerSettlementContext(runner, fallback)
	if err := first.Err(); err != nil {
		t.Fatalf("settlement window right after install = %v", err)
	}

	deadline := clock.NewReal().Timer(5 * time.Second)
	defer deadline.Stop()
	for first.Err() == nil {
		select {
		case <-deadline.C:
			t.Fatal("settlement window did not expire within 5s of a 30ms budget")
		default:
			_ = clock.NewReal().Sleep(context.Background(), 5*time.Millisecond)
		}
	}
	if !errors.Is(first.Err(), context.DeadlineExceeded) {
		t.Fatalf("settlement window ended with %v, want context deadline exceeded", first.Err())
	}

	second := runnerSettlementContext(runner, fallback)
	if second == first {
		t.Fatal("the runner handed out a settlement window that had already expired, so every settlement on it fails without reaching the driver")
	}
	if err := second.Err(); err != nil {
		t.Fatalf("replacement settlement window = %v, want a live budget", err)
	}
}

// TestReplacingALiveWindowDoesNotCancelIt pins how the replacement is made:
// the window it replaces is dropped, never cancelled. A cleanup goroutine
// captured the context value, not the field, and may still be about to reach
// the driver on it; cancelling it there would turn that call into a context
// failure the runner caused, one level up from the defect the window exists to
// avoid. The window is live when the drain's own installation replaces it, so
// the difference between dropping and cancelling is observable here: a
// cancelled window reads Canceled at once, a dropped one keeps running to its
// own deadline. Replacing must also retire the old cancel, so teardown releases
// the live window rather than one nothing holds any more.
func TestReplacingALiveWindowDoesNotCancelIt(t *testing.T) {
	_, runner, _ := newSettlementOrderingRunner(t)

	runner.client.config.Lifecycle.DrainTimeout = time.Minute

	finished, cancelFinished := context.WithCancel(context.Background())
	cancelFinished()

	// A settlement whose own context has finished is rescued onto the runner's
	// window, and that window is live.
	retired := runnerSettlementContext(runner, finished)
	if retired == finished {
		t.Fatal("the rescue installed no settlement window")
	}
	if err := retired.Err(); err != nil {
		t.Fatalf("rescued settlement window = %v, want a live budget", err)
	}

	// The drain computes its own window when it starts, which replaces the
	// rescued one.
	runner.beginSettlementWindow(context.Background())
	current := runnerSettlementContext(runner, finished)
	if current == retired {
		t.Fatal("the drain's window did not replace the rescued one")
	}
	if err := current.Err(); err != nil {
		t.Fatalf("replacement settlement window = %v, want a live budget", err)
	}
	if err := retired.Err(); err != nil {
		t.Fatalf("the retired settlement window ended with %v; replacing a live window must drop it, not cancel it", err)
	}

	finishRunner(runner)
	if err := current.Err(); err == nil {
		t.Fatal("teardown left the settlement window it installed live")
	}
	if err := retired.Err(); err != nil {
		t.Fatalf("teardown ended a retired settlement window with %v; teardown releases only the window it installed", err)
	}
}

// TestDrainWindowIsComputedWhenTheDrainStarts pins when the settlement budget
// begins: at the drain, not at generation start. A runner that has been
// healthy for longer than DrainTimeout still drains on a live window, so its
// settlements reach the driver instead of failing on a budget that expired
// while nothing was draining.
func TestDrainWindowIsComputedWhenTheDrainStarts(t *testing.T) {
	_, runner, _ := newSettlementOrderingRunner(t)

	runner.client.config.Lifecycle.DrainTimeout = 30 * time.Millisecond
	if err := clock.NewReal().Sleep(context.Background(), 50*time.Millisecond); err != nil {
		t.Fatalf("sleep for longer than the drain budget = %v", err)
	}

	runner.beginSettlementWindow(context.Background())
	window := runnerSettlementContext(runner, context.Background())
	if err := window.Err(); err != nil {
		t.Fatalf("the drain window was already done when the drain started: %v; the budget begins at the drain and not at the generation", err)
	}
	if _, ok := window.Deadline(); !ok {
		t.Fatal("drain settlement window carries no deadline")
	}
}

// TestGenerationStartLeavesTheDrainWindowAlone pins that neither a generation
// boundary nor teardown retires the settlement window a drain installed. The
// window is the drain's own value: starting the next generation neither
// replaces it nor cancels it, and nothing the runner tears down is a context
// the runner did not create.
func TestGenerationStartLeavesTheDrainWindowAlone(t *testing.T) {
	_, runner, _ := newSettlementOrderingRunner(t)

	captured := t.Context()

	runner.beginSettlementWindow(captured)
	window := runnerSettlementContext(runner, captured)
	if window == captured {
		t.Fatal("the drain installed no settlement window")
	}

	beginRunnerGeneration(runner, func() {})
	if got := runnerSettlementContext(runner, captured); got != window {
		t.Fatal("starting a generation replaced the drain's settlement window")
	}
	if err := window.Err(); err != nil {
		t.Fatalf("starting a generation ended the drain's settlement window: %v", err)
	}

	finishRunner(runner)
	if err := captured.Err(); err != nil {
		t.Fatalf("the context a settlement captured ended with %v; nothing the runner tears down may cancel a context it did not create", err)
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

	id := runner.inflight.Add()
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

	id := runner.inflight.Add()
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

// ackRetrySettler fails its first Ack and every Nack, so a cleanup that
// retries the ack in kind settles it and one that keeps nacking cannot.
type ackRetrySettler struct {
	mu        sync.Mutex
	ackCalls  int
	nackCalls int
	acked     bool
}

func (s *ackRetrySettler) Ack(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ackCalls++
	if s.ackCalls == 1 {
		return &driver.Error{Driver: "test", Op: "ack", K: driver.KindTransient, Err: errors.New("injected transient ack failure")}
	}
	s.acked = true
	return nil
}

func (s *ackRetrySettler) Nack(context.Context, driver.NackOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nackCalls++
	return &driver.Error{Driver: "test", Op: "nack", K: driver.KindTransient, Err: errors.New("injected transient nack failure")}
}

// TestCancelPathCleanupRequeuesUntilTheNackSettles proves the delivery a
// cancellation path takes back off the channel runs the same bounded cleanup as
// a dispatched one: a nack that fails once is retried, and the registry entry
// stays until the driver reports the delivery settled.
func TestCancelPathCleanupRequeuesUntilTheNackSettles(t *testing.T) {
	_, runner, _ := newSettlementOrderingRunner(t)
	settler := &scriptedNackSettler{failFirst: 1}
	// Every attempt must still see the delivery: the entry may not leave while
	// the broker still owns the message.
	var lenDuringBudget []int
	settler.onNack = func() {
		lenDuringBudget = append(lenDuringBudget, runner.inflight.Len())
	}
	envelope := Envelope{
		SpecVersion: "1.0",
		ID:          "cancel-requeue",
		Source:      "/test/orders",
		Type:        "orders.created.v1",
		Priority:    PriorityHigh,
		Attempt:     1,
	}
	message := retryBridgeMessage(t, envelope, settler)

	ctx, cancelRun := context.WithCancel(context.Background())
	cancelRun()

	if enqueueDelivery(runner, ctx, make(chan delivery), message) {
		t.Fatal("enqueueDelivery() accepted a delivery the cancelled context refused")
	}

	nacks, acks, outstanding := settler.state()
	if acks != 0 {
		t.Fatalf("Ack calls = %d, want 0: the cancel path requeues", acks)
	}
	if nacks != 2 {
		t.Fatalf("Nack calls = %d, want 2: the failed requeue must be retried within the cleanup budget", nacks)
	}
	if outstanding {
		t.Fatal("settler reported the delivery settled, want it retained until a Nack succeeded")
	}
	if len(lenDuringBudget) != 2 {
		t.Fatalf("nack hooks = %d, want 2", len(lenDuringBudget))
	}
	for attempt, length := range lenDuringBudget {
		if length != 1 {
			t.Fatalf("inflight length during nack %d = %d, want 1: the entry stays while the driver still owns the delivery", attempt+1, length)
		}
	}
	if got := runner.inflight.Len(); got != 0 {
		t.Fatalf("inflight length = %d, want 0 once the delivery settled", got)
	}
	if err := runner.inflight.WaitZero(context.Background()); err != nil {
		t.Fatalf("WaitZero() after the delivery settled = %v, want nil", err)
	}
}

func TestCleanupRetriesAFailedAckInKindAfterTheRequeueFallback(t *testing.T) {
	_, runner, _ := newSettlementOrderingRunner(t)
	settler := &ackRetrySettler{}
	envelope := Envelope{SpecVersion: "1.0", ID: "ack-retry", Source: "/test/orders", Type: "orders.created.v1", Priority: PriorityHigh, Attempt: 1}
	message := retryBridgeMessage(t, envelope, settler)
	state := &deliveryState{attempted: true, operation: settlementOperationAck}

	retryDeliverySettlement(runner, context.Background(), message, state)

	settler.mu.Lock()
	defer settler.mu.Unlock()
	if !settler.acked || !state.settled {
		t.Fatalf("acked = %v, settled = %v after Ack calls = %d and Nack calls = %d, want the ack retried in kind", settler.acked, state.settled, settler.ackCalls, settler.nackCalls)
	}
}

// discardAckSettler adds transient Ack failures to the existing Nack script.
// Hooks run without the mutex so a held Ack can be inspected by its test.
type discardAckSettler struct {
	scriptedNackSettler
	failAcks int
	options  driver.NackOptions
}

func (s *discardAckSettler) Ack(context.Context) error {
	s.mu.Lock()
	s.ackCalls++
	call := s.ackCalls
	hook := s.onAck
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if call <= s.failAcks {
		s.outstanding = true
		return &driver.Error{Driver: "test", Op: "ack", K: driver.KindTransient, Err: errors.New("injected transient ack failure")}
	}
	s.outstanding = false
	return nil
}

func (s *discardAckSettler) Nack(ctx context.Context, options driver.NackOptions) error {
	s.mu.Lock()
	s.options = options
	s.mu.Unlock()
	return s.scriptedNackSettler.Nack(ctx, options)
}

func TestDiscardCallbackIsSuppressedAfterRequeueFallback(t *testing.T) {
	for _, test := range []struct {
		name      string
		failNacks int
		wantAcks  int
		wantNacks int
	}{
		{name: "requeue succeeds", wantAcks: 2, wantNacks: 1},
		{name: "settlement budget exhausted", failNacks: settlementRetryAttempts, wantAcks: 1 + settlementRetryAttempts, wantNacks: settlementRetryAttempts},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				_, runner, _ := newSettlementOrderingRunner(t)
				cause := errors.New("discard this event")
				runner.subscription.Handlers = map[string]Handler{
					"orders.created.v1": HandlerFunc(func(context.Context, *Event) error { return Drop(cause) }),
				}
				callbacks := 0
				runner.subscription.OnDiscarded = func(context.Context, Discarded) { callbacks++ }
				settler := &discardAckSettler{
					scriptedNackSettler: scriptedNackSettler{failFirst: test.failNacks, outstanding: true},
					failAcks:            1 + settlementRetryAttempts,
				}
				envelope := Envelope{SpecVersion: "1.0", ID: "discard-requeue", Source: "/test/orders", Type: "orders.created.v1", Priority: PriorityHigh, Attempt: 1}
				message := retryBridgeMessage(t, envelope, settler)
				processDelivery(runner, t.Context(), delivery{id: runner.inflight.Add(), message: message})
				synctest.Wait()
				nacks, acks, outstanding := settler.state()
				if acks != test.wantAcks || nacks != test.wantNacks || !settler.options.Requeue {
					t.Fatalf("settlement calls: acks=%d nacks=%d options=%+v, want acks=%d nacks=%d requeue", acks, nacks, settler.options, test.wantAcks, test.wantNacks)
				}
				if outstanding != (test.failNacks > 0) || runner.inflight.Len() != 0 {
					t.Fatalf("outstanding=%t inflight=%d, want outstanding=%t inflight=0", outstanding, runner.inflight.Len(), test.failNacks > 0)
				}
				if callbacks != 0 {
					t.Fatalf("OnDiscarded calls = %d, want 0 without a successful Ack", callbacks)
				}
			})
		})
	}
}

func TestDiscardCallbackFollowsDeferredAckSuccessOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, runner, _ := newSettlementOrderingRunner(t)
		cause := errors.New("discard this event")
		runner.subscription.Handlers = map[string]Handler{
			"orders.created.v1": HandlerFunc(func(context.Context, *Event) error { return Drop(cause) }),
		}
		var reports []Discarded
		runner.subscription.OnDiscarded = func(_ context.Context, payload Discarded) { reports = append(reports, payload) }
		started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		settler := &discardAckSettler{failAcks: 1}
		settler.onAck = func() {
			_, acks, _ := settler.state()
			if acks == 2 {
				close(started)
				<-release
			}
		}
		envelope := Envelope{SpecVersion: "1.0", ID: "discard-deferred", Source: "/test/orders", Type: "orders.created.v1", Priority: PriorityHigh, Attempt: 1}
		message := retryBridgeMessage(t, envelope, settler)
		go func() {
			defer close(done)
			processDelivery(runner, t.Context(), delivery{id: runner.inflight.Add(), message: message})
		}()
		<-started
		synctest.Wait()
		beforeSuccess := len(reports)
		close(release)
		<-done
		synctest.Wait()
		if beforeSuccess != 0 {
			t.Errorf("OnDiscarded calls before deferred Ack success = %d, want 0", beforeSuccess)
		}
		if len(reports) != 1 {
			t.Fatalf("OnDiscarded calls after deferred Ack success = %d, want 1", len(reports))
		}
		got := reports[0]
		if got.Envelope.ID != envelope.ID || string(got.Body) != string(message.Body) || got.Reason != DiscardDropped || !errors.Is(got.Err, cause) {
			t.Fatalf("discard payload = %+v, want original envelope/body/drop cause", got)
		}
		nacks, acks, outstanding := settler.state()
		if acks != 2 || nacks != 0 || outstanding || runner.inflight.Len() != 0 {
			t.Fatalf("settlement: acks=%d nacks=%d outstanding=%t inflight=%d, want 2, 0, false, 0", acks, nacks, outstanding, runner.inflight.Len())
		}
	})
}
