# Observer

An observer is how an application watches what F1 is doing. F1 hands out typed
events at every stage of publishing and consuming a message, and the application
decides what to do with them: record metrics, open spans, write logs, or nothing
at all.

The SDK emits neutral events in the core and knows nothing about the tool you
send them to. That is why adapters live outside it. If you want OpenTelemetry,
use the [observability guide](/advanced-topics/observability); this page explains the
model underneath it.

## Three methods, three event shapes

The [`Observer`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/observer.go)
interface is three methods:

```go
type Observer interface {
	Start(context.Context, StartEvent) (context.Context, Token)
	Finish(Token, FinishEvent)
	Record(PointEvent)
}
```

`Start` and `Finish` bracket a stage that takes time. `Record` reports something
that happened at an instant. Every event carries a `Kind` saying which stage it
belongs to, and an `At` timestamp from the client's clock.

Events are passed by value, and a field that does not apply to a kind is left at
its zero value. A drain event has no `EventType`; a publish `Start` is emitted
before the message is resolved, so it has no `Topic` yet. The field comments in
`observer.go` say which kinds set which fields, and the
[observer events reference](/development/observer-events) lists every value.

## Write one

A working observer that counts failures by cause:

```go
type failureCounter struct {
	mu     sync.Mutex
	counts map[f1.ErrorClass]int
}

func (o *failureCounter) Start(ctx context.Context, event f1.StartEvent) (context.Context, f1.Token) {
	return ctx, f1.Token{}
}

func (o *failureCounter) Finish(token f1.Token, event f1.FinishEvent) {
	if event.Outcome != f1.ObserverOutcomeError {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.counts[event.ErrorClass]++
}

func (o *failureCounter) Record(event f1.PointEvent) {}
```

Hand it to the client at construction:

```go
client, err := f1.New(ctx, cfg, f1.WithObserver(&failureCounter{
	counts: make(map[f1.ErrorClass]int),
}))
```

Note what this observer does not do. It returns the context unchanged and a zero
token, because it keeps no per-stage state. It ignores `Record` entirely. An
observer only has to answer the calls it cares about.

## Tokens match a Finish to its Start

An observer that does keep per-stage state needs to find that state again when
`Finish` arrives. That is what the token is for: whatever you return from
`Start`, F1 gives back to `Finish`.

```go
func (o *spanObserver) Start(ctx context.Context, event f1.StartEvent) (context.Context, f1.Token) {
	ctx, span := o.tracer.Start(ctx, string(event.Kind))

	o.mu.Lock()
	o.handle++
	handle := o.handle
	o.spans[handle] = span
	o.mu.Unlock()

	return ctx, f1.Token{Kind: event.Kind, Start: event.At, Handle: handle}
}

func (o *spanObserver) Finish(token f1.Token, event f1.FinishEvent) {
	o.mu.Lock()
	span, ok := o.spans[token.Handle]
	delete(o.spans, token.Handle)
	o.mu.Unlock()

	if !ok {
		return // zero or unknown token, nothing was started
	}
	span.End()
}
```

`Handle` is yours. F1 does not read it. `Kind` and `Start` are filled in for you
and are there so an adapter can work without its own lookup when that is enough.

The zero token means no stage was started, so check for it rather than assuming
a `Finish` always maps to state you hold.

Every started stage gets exactly one `Finish`. If the stage exits without
reporting an outcome, F1's finish guard sends one anyway with
`ObserverOutcomeAbandoned`, so an adapter never leaks a span or a timer. The
guard lives in [`observer_call.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/observer_call.go).

## Paired kinds and point kinds

Five kinds are paired, meaning they arrive as a `Start` and a matching `Finish`:
publish, message building, processing, settlement, and drain. Everything else is
a point kind and arrives only through `Record`.

The kinds are named for where an observation belongs in the lifecycle, not for a
broker API:

- **Publishing.** `ObserverPublish` surrounds an admitted publish call;
  `ObserverMessageBuilt` surrounds building each outbound message inside it.
- **Consuming.** `ObserverDeliveryReceived` marks a delivery admitted to
  dispatch, `ObserverProcess` surrounds the handler, and `ObserverSettle`
  surrounds the ack or nack.
- **Failing.** `ObserverRetryScheduled` fires when a retry successor is
  confirmed. The dead-letter kinds separate the decision from the publish result,
  so you can tell "we decided to dead-letter" from "the dead-letter publish
  failed". `ObserverPoisonRejected` covers a delivery dropped for decode failure,
  poison, expiry, or no matching handler.
- **Connecting.** `ObserverConnectionLost` and `ObserverConnectionRestored`
  report transitions; `ObserverDriverSelected` fires once per client.
- **Scheduling.** `ObserverDrain` surrounds a runner drain,
  `ObserverBacklogSampled` reports a sampled destination, and
  `ObserverDeadlinePromoted` reports a lane promotion, rate limited per lane.

A message does not pass through every stage. One rejected before dispatch never
produces a processing pair, so do not build a dashboard that assumes publish and
process counts line up.

## Outcomes and error classes

`Outcome` on a `FinishEvent` is one of `ok`, `error`, or `abandoned`. When it is
`error`, `ErrorClass` says why in a bounded vocabulary: `f1_retryable`,
`f1_poison`, `driver_transient`, `_OTHER`, and a dozen more.

Bounded is the point. An error string is unbounded, and using one as a metric
label is how a metrics backend falls over. `ErrorClass` is safe to use as a
dimension:

```go
failures.WithLabelValues(
	string(event.Kind),
	string(event.ErrorClass),
).Inc()
```

These strings are a public contract and they surface to operators through the
metric labels the OpenTelemetry adapter emits. The full list is in the
[observer events reference](/development/observer-events); the mapping from a Go
error to a class is [`errorClassOf`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/observer_call.go).

## Rules an observer must follow

**Be safe for concurrent use.** F1 calls every method concurrently for different
messages, subscriptions, and publishers. A `Start` and its own `Finish` may run
on different goroutines. For one message, its `Start` happens before its
`Finish`; between different messages F1 promises no ordering at all.

**Return quickly.** Observer methods run synchronously on the path that emitted
the event. A slow one stalls intake, dispatch, settlement, or drain. Buffer and
flush elsewhere; do not do I/O inline. F1 applies no timeout to an observer call:
the path that emitted the event waits for it, and on the settlement path that
wait can spend the consumer liveness window.

**Panicking is contained but not free.** F1 recovers a panic in an observer so it
cannot change how a message settles, and logs the first one per kind, but the
observation is lost.

**A nil observer is free.** F1 checks for nil before it builds the event, so a
client with no observer pays one call-site comparison and constructs no event
struct.

**Ignore what you do not know.** An adapter must not fail because it does not
recognize a kind or does not read a field. A field that does not apply to a kind
keeps its zero value, and `FinishEvent.Results` must not be retained or mutated
after the call returns, because the core may reuse its storage.

**Use event timestamps, not the wall clock.** Every event carries an `At` from the
client's clock. Derive durations from event timestamps instead of calling
`time.Now`, so a fake-clock test stays exact.

**Keep metric cardinality bounded.** Do not use a physical destination or a
message identity as a metric dimension. Use the logical topic, the subscription
or consumer group, the kind, and the bounded `ErrorClass`.

F1 never calls an observer while holding a client or runner lock, so you cannot
deadlock the SDK from inside one. The contract is stated on `Observer` and pinned
by the tests in [`observer_contract_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/observer_contract_test.go).

## Trace context

Inbound trace context arrives on `StartEvent` as `TraceParent` and `TraceState`,
so reading it needs no extra hook.

Writing it out does. An observer that also implements
[`TraceInjector`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/observer.go)
is asked to serialize the current trace context, and F1 writes the result into
the outgoing envelope:

```go
func (o *spanObserver) InjectTrace(ctx context.Context) (traceParent, traceState string) {
	carrier := propagation.MapCarrier{}
	o.propagator.Inject(ctx, carrier)
	return carrier.Get("traceparent"), carrier.Get("tracestate")
}
```

F1 checks for the interface once and calls it after starting a message build or
a republish. An injector that is missing or panics does not fail the publish.

One boundary worth knowing: the context from `Start` for `ObserverProcess` is the
context the handler receives, but settlement and retry publication can run on a
different drain-time context. Trace context crossing a retry or dead-letter hop
travels on the message, not on a context.

## Test against a recorder

`f1test` ships a recording observer so a test can assert on events instead of
reimplementing one:

```go
recorder := f1test.NewRecorder()
client, err := f1.New(ctx, cfg, f1.WithObserver(recorder))

// ... publish and consume ...

for _, call := range recorder.Calls() {
	t.Log(call)
}
```

`Recorder.Wait` blocks until the recorded calls satisfy a predicate, which is how
you wait for an event without sleeping.

## Continue from here

- [Observability](/advanced-topics/observability) - the OpenTelemetry adapter, metrics, and operator setup;
- [Observer events reference](/development/observer-events) - every kind, outcome, error class, and field; and
- [Consume flow](/development/consume-flow) - where these events are emitted on the consume path.
