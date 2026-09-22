# AGENTS.md

Guidance for anyone changing this repository, human or agent.

## Package overview

`f1` is a broker-agnostic event-driven messaging SDK for Go. The root package is the
public API and the orchestration around it. `driver/` is the stdlib-only port that every
broker adapter implements. `drivers/` holds the adapters: `inmem`, `rabbitmq`, `kafka`.

Read `README.md`, then `ARCHITECTURE.md`, then `docs/development/source-reading-guide.md` before your first
change. Do not start in `worker.go` and do not start in the tests.

## Style

ASCII only, everywhere: source, comments, tests, commit messages. No em-dash.

`gofumpt` and `goimports` are enforced, with this module as the local import prefix.

Every package has a package comment, and every exported symbol has a doc comment that
begins with its own name and states caller-visible behaviour: special cases, the meaning
of the zero value, and concurrency guarantees where they exist. `revive` enforces that the
comments exist. Review enforces that they say something.

Internal comments explain why, not what. A what-comment is almost never useful, the
exception being a genuinely dense block where naming the steps earns its line. A comment
recording the alternative you rejected, or the broker behaviour that forced the shape, is
why the file is still readable a year later.

A comment on a subtle race, in logic or in data, walks through how the race is reached. Do
not only say what the race is. Give the interleaving: which goroutine is where, what it has
already done, and what the other one does in between. "Safe because of the mutex" is not a
walkthrough, and neither is naming the two goroutines without the sequence that puts them in
the wrong order.

## Refactoring threshold

DRY is about logic, not line counts.

Reject a helper that exists to tell its callers apart with a bool flag. That is two
operations sharing infrastructure, not one operation with a knob, and the call site now
reads `doThing(x, true)`, which says nothing. Reject a helper saving fewer than about five
lines per site; the indirection costs more than the duplication did.

Accept a helper that names a genuinely duplicated operation, has one job, and leaves the
call site reading like the thing it is doing.

The exception is correctness. When the same decision is repeated at several call sites and
being wrong at any one of them is a defect, consolidate regardless of line count, and remove
the per-call-site choice rather than fixing one more copy of it. A second correct copy is
how the same defect comes back.

## Never call time or the global logger directly

The linter forbids `time.Now`, `Since`, `Sleep`, `After`, `Tick`, `NewTimer`, `NewTicker`
and `AfterFunc`. Use the `clock.Clock` the surrounding type already holds.

This is not style. Tests drive time through a fake clock, so one direct `time` call makes
the test covering it slow or flaky, usually both, and usually somewhere else.

Output is the same rule: no `fmt.Print*`, no `log.*`. Structured logging goes through the
logger the client was built with.

## The core does not know about brokers

`depguard` denies `drivers/**`, `franz-go` and `amqp091-go` to every package outside
`driver/`, `drivers/`, `f1test/` and `examples/`. `make verify-agnostic` is that check on
its own.

If a change seems to need broker knowledge in the core, the port is wrong. Widen the port.
Do not import the broker.

## Errors that cross the port

A driver returns a `*driver.Error` carrying the driver name, the port operation in `Op`, a
`Kind`, and the broker's own error wrapped in `Err` so `errors.Is` and `errors.As` still
reach it.

An error that does not implement `ClassifiedError` is treated as transient, which is the
retrying default, so an unclassified fatal error becomes an infinite retry loop. Classify
deliberately.

Never put a config struct, credentials, or a URI carrying credentials into an error string
or a log line.

## Broker behaviour is measured, not assumed

The broker is ground truth. Before writing a rule about what a broker accepts, run it
against the fixture. A rule derived from a client library's method signature, or from
documentation, is a hypothesis.

When a broker refuses something it usually says which precondition failed. Carry that
through. Replacing it with a sentinel you invented throws away the whole diagnostic on the
path where it matters most.

## Tests

`make test-fast` is the short race-enabled suite and must stay fast. A unit test taking
more than a second or two is doing I/O it should not, or waiting on real time.

Broker-backed suites need the local fixtures: `make broker-up` for RabbitMQ on 5672,
`make kafka-up` for Kafka on 19092. A test file that needs a broker carries
`//go:build integration` and is named `_integration_test.go`. `make test-fast` compiles none
of them; `make test-infra` compiles all of them and fails when a fixture is unreachable. Kafka
conformance and the driver flip still need their own targets: `make test-kafka-conformance` and
`make test-driver-flip`.

The fixtures are shared. Delete every queue, topic and consumer group your test created,
and never reset or reconfigure a fixture you did not start.

Run with `-race`. Do not weaken, skip, or delete a test to make a gate pass. A failing test
is either a defect or a wrong test, and both are worth saying out loud.

New behaviour needs a test that fails without it. After it passes, break the change again
and watch that named test go red. A test that passes both ways proves nothing, and this is
the single most common way a correct fix ships unprotected.

## Gates

Run all of these before proposing a change:

```
make build
go vet ./...
gofmt -l .
make format-check
make lint
make vulncheck
make verify-agnostic
make verify-self-contained
make check-doc-source-links
make check-observer-events
make check-api-surface
make check-api-surface-f1otel
make check-api-diff
make otlp-boundary
make test-fast
```

Add `make test-rabbitmq` or `make test-kafka` when you touched that driver.

`check-api-diff` failing on a breaking change is not a stale-baseline chore. Changing the
public API is a release decision. Get it approved before recording a new baseline. The target
also fails on an unrecorded compatible change; record it in the baseline with
`make record-api-diff-baseline` in the same commit, with no approval.

`verify-self-contained` fails on citations no reader of this repository can resolve. Design
records live elsewhere. State the rule the citation stands for instead of the reference.

## Commit style

One logical change per commit. Subject is `<scope>: <what changed>`, lowercase, no trailing
period, present tense. Scope is the package or driver: `kafka:`, `worker:`, `docs:`. One or
two lines total. Add a body only when the reason is not obvious, and keep it short.

No AI attribution, no co-author trailers, no plan or ticket identifiers.

## Approach for agents

An audit reports, it does not edit. If you were asked what is wrong, answer that and stop.
Do not fix things on the way past.

Read before you write. Find the existing helper, test utility, and convention, and use it.

Say what you did not do. A gap you name is cheap. The same gap found in review is not.

## Key files

| File | Holds |
| --- | --- |
| `client.go` | client construction, driver connection, topology setup, shutdown |
| `publisher.go` | envelope construction, publish, confirm |
| `subscription.go` | subscription resolution, handler admission, error-handler group |
| `worker.go` | consume loop, retry, settlement, drain. Largest file; use the function map in `docs/development/consume-flow.md` |
| `config.go`, `broker_config.go`, `options.go` | configuration and option resolution |
| `envelope.go` | message identity, headers, body |
| `reconnect.go` | reconnection and lane repair |
| `driver/driver.go` | the port: Driver, Conn, Producer, Consumer, Admin, Maintenance |
| `driver/topology.go` | topology specs, state, diffs, prune results |
| `driver/errors.go` | classification and the standard driver error |
| `driver/conformance/` | the suite every adapter must pass |
| `drivers/inmem/` | the reference adapter. Read it before writing a new one |
| `f1test/` | test helpers for users of the SDK |
| `observer.go` | public observer event types, lifecycle contract, and trace injection |
| `backlog_poll.go` | backlog polling and observer backlog samples |
| `deadline_promotion.go` | deadline promotion observer events and per-lane suppression |
| `f1otel/` | OpenTelemetry observer adapter for metrics, spans, and propagation |
| `internal/` | clock, retry, sched, dispatch, lifecycle |
