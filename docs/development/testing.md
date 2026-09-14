# Testing strategy

F1 uses several test layers because the repository owns two different kinds of
behavior: the public SDK contract and the broker adapter contract. Choose the
lowest layer that can prove the behavior, then add a higher-level test when
the behavior crosses a package or broker boundary.

The practical default is:

- use [`f1test`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/f1test/f1test.go) for handler behavior;
- use the in-memory driver for end-to-end F1 semantics;
- use [driver conformance](/development/driver-conformance) for adapter portability;
- use broker-backed suites for provider-specific behavior; and
- use the repository gates in the [Makefile](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/Makefile) before handing off
  a change.

```mermaid
flowchart TB
    PURE[Pure package and internal tests]
    ROOT[Root-package behavior tests]
    F1TEST[f1test handler tests]
    INMEM[In-memory integration]
    CONF[Driver conformance]
    BROKER[RabbitMQ and Kafka suites]
    GATES[API, import, and self-contained gates]

    PURE --> ROOT
    ROOT --> F1TEST
    ROOT --> INMEM
    INMEM --> CONF
    CONF --> BROKER
    ROOT --> GATES
    CONF --> GATES
    BROKER --> GATES
```

The layers are complementary. A broker test is not a substitute for a
deterministic handler test, and a passing in-memory test is not evidence that a
RabbitMQ or Kafka translation is correct.

## Unit tests for pure packages and internal primitives

Use unit tests when the behavior is an algorithm or a small, broker-independent
contract. These tests should be fast, deterministic, and independent of
network services.

The main pure areas are:

- [`codec/`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/codec) for payload and codec behavior;
- [`internal/clock/`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/internal/clock) for real and fake time;
- [`internal/retry/`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/internal/retry) for classification, retry
  outcomes, and backoff ladders;
- [`internal/sched/`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/internal/sched) for weighted lanes, aging, and
  fairness;
- [`internal/dispatch/`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/internal/dispatch) for worker routing,
  ordered keys, and in-flight accounting; and
- [`internal/lifecycle/`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/internal/lifecycle) for drain state and
  disposition accounting.

An internal test should name the invariant it protects and use the smallest
public surface of that package. It should not reproduce the entire client or
broker setup just to exercise a queue, clock, retry decision, or accounting
transition.

These tests are the first place to look when a failure is isolated to timing,
scheduling, dispatch admission, retry classification, or lifecycle accounting.
The package-local `*_test.go` files are the authority for the exact fixtures and
invariants.

## Root-package behavior tests

Root tests prove behavior owned by the public `f1` package: client construction,
publishing, subscriptions, workers, routing, settlement, retries, topology,
reconnection, and shutdown. They should test through exported objects whenever
the behavior is visible to an application.

Useful entry points include:

- [`publish_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/publish_test.go) and
  [`publisher_topology_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/publisher_topology_test.go) for publish
  admission, routing, topology, and durable publication behavior;
- [`subscribe_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/subscribe_test.go),
  [`worker_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/worker_test.go), and
  [`dispatch_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/dispatch_test.go) for subscription and dispatch
  behavior;
- [`worker_retry_bridge_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/worker_retry_bridge_test.go) and
  [`worker_successor_family_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/worker_successor_family_test.go)
  for retry and dead-letter successor boundaries;
- [`settlement_state_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/settlement_state_test.go),
  [`worker_settlement_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/worker_settlement_test.go), and
  [`registry_settlement_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/registry_settlement_test.go) for
  settlement accounting and settle-last ordering; and
- [`topology_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/topology_test.go),
  [`topology_policy_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/topology_policy_test.go), and
  [`worker_ordering_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/worker_ordering_test.go) for topology and
  ordered delivery behavior.

Root tests may use the in-memory driver as a transport, but the assertion should
be about F1 behavior rather than an in-memory implementation detail. If the
test needs to inspect a broker queue, offset, or provider-specific error, move
it to the appropriate driver suite.

## `f1test` deterministic handler tests

Prefer [`f1test.Client`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/f1test/f1test.go) for application handler tests.
It exposes the normal client, subscription, runner, and handler APIs while
providing deterministic helpers for publishing and observing accepted output.
It is backed by the in-memory driver and a manually advanced clock; the client
is cleaned up through `t.Cleanup`.

Use it to prove decisions such as:

- a handler succeeds and produces the expected application-visible effect;
- a retryable, terminal, or dropped handler result follows the documented
  route;
- retry and dead-letter copies preserve the event identity and relevant
  headers; and
- ordered keys, fanout, and handler concurrency have the expected public
  behavior.

The helper's [`Deliver`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/f1test/f1test.go),
[`Published`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/f1test/f1test.go), [`DLQ`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/f1test/f1test.go), and
[`Advance`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/f1test/f1test.go) methods are observation and timing tools,
not alternate production APIs. The examples in
[`f1test/f1test_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/f1test/f1test_test.go),
[`f1test/fanout_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/f1test/fanout_test.go), and
[`f1test/ordered_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/f1test/ordered_test.go) show the intended
test shape.

When testing a retry path, wait until the retry publication is observable
before advancing the fake clock. The helper's capture signal and explicit
channels are preferable to a real-time sleep.

## In-memory integration tests

Use the in-memory adapter when the test needs the complete F1 path but does not
need a real broker. This is the right layer for interactions among client
configuration, topology preparation, publishing, consumption, dispatch,
settlement, retry routing, ordering, drain, and reconnect behavior.

The adapter is deterministic and provides the reference implementation for the
port. Its integration tests live under
[`drivers/inmem/`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/drivers/inmem), including settlement, drain,
successor-family, topology, and reconnect cases. The in-memory suite is also
the transport used by `f1test`, but direct adapter tests can inspect port-level
state more closely when that is the behavior under test.

Keep the distinction clear:

- `f1test` asks whether a service author sees the right handler-facing result;
- root-package tests ask whether F1 orchestration produces that result; and
- in-memory driver tests ask whether the reference adapter fulfills the port
  while supporting that orchestration.

Do not use the in-memory adapter to make a claim about RabbitMQ queue
semantics, Kafka offsets, partition ownership, management APIs, or broker
durability.

## Driver conformance tests

Use [driver conformance](/development/driver-conformance) when implementing or changing
an adapter's implementation of the broker-independent port. The shared suite
checks the same publish, consume, settlement, topology, lifecycle, capability,
fault, ordering, drain, lag, and rebalance contract against each candidate.

The provider test supplies an inspector and any deterministic fault or deadline
fixtures. The shared package must not import a concrete driver. A conformance
pass proves portability through the port; it does not prove provider-specific
features or production broker configuration.

Read [`driver/driver.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/driver/driver.go) and
[`driver/conformance/`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/driver/conformance) together when a port test
fails. The conformance page explains full and strict profiles, behavior vectors,
pending groups, fixture registration, and provider run paths.

## Broker-backed RabbitMQ and Kafka tests

These tests live in `_integration_test.go` files behind the `//go:build
integration` tag. A test file is an integration file, and is named
`_integration_test.go`, if and only if it requires a process this repository did
not start; the commands for both sides of that line are under "Which targets
contact a broker" below.

Use live broker tests only when the behavior depends on the provider or its
client library. Examples include:

- RabbitMQ acknowledgement, exchange and queue declarations, management
  inspection, deferred queues, ordering, reconnect, TLS, and broker-specific
  admission behavior under [`drivers/rabbitmq/`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/drivers/rabbitmq);
- Kafka producer confirmation, offsets, partitions, classic consumer groups,
  rebalancing, deferred records, lag, TLS, and Kafka error mapping under
  [`drivers/kafka/`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/drivers/kafka); and
- provider-specific fault injectors and inspectors used by the shared
  conformance suite.

The [Makefile](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/Makefile) owns the broker lifecycle and suite targets.
Use its `test-rabbitmq`, `test-kafka`, `test-kafka-conformance`,
`test-infra`, and `test-driver-flip` targets instead of copying fixture setup
into documentation or test scripts. Each passes `-tags integration` and an
explicit endpoint, so an unreachable fixture fails the run rather than turning
that suite into a skip.

`test-driver-flip` is the driver-flip acceptance: it builds the two
`examples/acceptance` services once, runs the identical binaries against Kafka
and RabbitMQ with one corpus, and diffs the behaviour vectors. It needs both
brokers, takes a few minutes, and writes its artifacts to `.cache/driver-flip`.
Size the corpus with `F1_DRIVER_FLIP_CORPUS` (default 10 000). It is not a
required gate: a known Kafka consumer stall can red it at any corpus size, so
run it deliberately and read a red run as evidence about that defect rather than
about broker agnosticism.

### Which targets contact a broker

A test file is an integration file, and is named `_integration_test.go`, if and
only if it requires a process this repository did not start. Scope is irrelevant:
a single-unit test that needs a broker is an integration file, and an untagged
file may cross three packages. The file carries `//go:build integration` for the
same reason, and the guard in `internal/testlayout` fails when the tag and the
name disagree in either direction.

That makes the split a compile-time one, so a run never decides at run time
whether infrastructure is reachable. `make test-fast` and `make test` run every
test that needs no broker; the integration files are not compiled into them at
all, so no test in either run connects to a broker. Those two targets are the
default gate and they stay runnable with nothing listening.

`make test-infra` runs the other half, `go test -count=1 -p 1 -tags integration
./...`, with Kafka and RabbitMQ started first. An unreachable fixture fails the
run; nothing behind the tag skips. Packages run one at a time there because the
broker-backed suites share both fixtures and the machine.

The per-driver targets are the same tag with a narrower package list:
`test-kafka`, `test-rabbitmq`, `test-rabbitmq-driver`,
`test-rabbitmq-conformance`, `test-kafka-conformance`, and `test-driver-flip`.
Each starts the fixtures it needs and passes `-tags integration`, so each one
keeps running the tests it names.

`F1_KAFKA_ENDPOINT` and `F1_RABBITMQ_ENDPOINT` say *where* a fixture is. Unset
means the documented default address, and a test pointed at nothing fails rather
than skipping. Point a run at your own fixture with `F1_KAFKA_ENDPOINT` (for
example `localhost:19131`) and `F1_RABBITMQ_ENDPOINT` (for example
`amqp://guest:guest@localhost:15131/`); the broker-backed targets honour both,
falling back to the port their own `KAFKA_PORT` or `RABBITMQ_PORT` variable
selects.

Tests that build a `kgo` client only to inspect its resolved options, or to drive
a path whose network calls are stubbed out, pass `noDialKafkaOption`. franz-go
dials a seed broker from a background metadata fetch, so a client built from a
compiled-in endpoint reaches whatever happens to be listening on that port, from
a test that never needs a broker.

Keep provider tests isolated from one another. Use the fixture cleanup helpers,
remove queues, topics, and groups created by the test, and do not reset a
shared broker that the test did not start.

## Failure injection and reconnect testing

Failure tests should exercise the recovery contract, not just prove that an
error was returned. A useful failure sequence is:

1. publish or receive a control message;
2. inject a transient connection, lane, delivery, or publication failure;
3. assert the error classification and channel liveness;
4. assert that unsettled work is redelivered and settled exactly once after
   recovery; and
5. assert that a later publish or delivery succeeds without a phantom duplicate
   or lost message.

The shared fault contract is implemented in
[`driver/conformance/failure.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/driver/conformance/failure.go). Core
reconnect and lane-repair behavior is covered by
[`reconnection_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/reconnection_test.go) and
[`lane_repair_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/lane_repair_test.go). Provider-specific recovery
belongs in [`drivers/rabbitmq/reconnect_integration_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/drivers/rabbitmq/reconnect_integration_test.go),
the RabbitMQ fault injector, and the corresponding Kafka or in-memory tests.

Delivery is at least once. Duplicate delivery, uncertain acknowledgement, and
redelivery after a connection or process failure are expected contract paths.
Tests for handlers and side effects must therefore include retry and duplicate
delivery cases and make the effect idempotent, rather than treating a second
delivery as an impossible test artifact.

## Shutdown and settlement testing

Shutdown tests must distinguish admission, drain, settlement, release, and
connection close. The expected behavior is not simply that a goroutine
returns. Assert the public lifecycle result:

- no new delivery is admitted after drain starts;
- already delivered work remains settleable during drain;
- successor publication completes before the original delivery is
  acknowledged;
- an unsettled stop reports the documented refusal or drain timeout;
- release abandons outstanding deliveries for redelivery; and
- successful stop closes the driver message channel at the documented point.

Core lifecycle coverage starts with
[`client_close_sequencing_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/client_close_sequencing_test.go),
[`client_drain_budget_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/client_drain_budget_test.go),
[`client_producer_admission_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/client_producer_admission_test.go),
[`publish_close_barrier_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/publish_close_barrier_test.go),
[`worker_abort_teardown_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/worker_abort_teardown_test.go), and
the settlement tests linked in the root-package section. Driver-level drain,
stop, release, and reconnect behavior is covered by the adapter suites and
the conformance drain group.

Do not make shutdown tests pass by adding a longer sleep. Arrange an explicit
settlement signal, use the lifecycle API under test, and assert the resulting
state or error.

## Fake clocks and deterministic timing

Use [`internal/clock`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/internal/clock) and its fake implementation when
time is part of the behavior: retry delays, deferred delivery, acknowledgement
deadlines, drain budgets, backoff, or scheduler aging. `f1test.Client.Advance`
advances the same fake clock used by the core and in-memory driver, so a test
can release due work without waiting for wall time.

Use explicit synchronization for non-time events. Channels, runner completion,
publication capture signals, and broker readiness checks make the test explain
what it is waiting for. A context deadline is appropriate as a failure bound
around external I/O; it is not a substitute for synchronization.

Avoid `time.Sleep` and polling loops when a fake clock, condition channel,
settlement signal, or test helper can express the event directly. If a real
clock is unavoidable, keep the timeout bounded and document the external
condition that makes it necessary.

## Public behavior versus internal mocking

Assert through the public API when the behavior is visible to a service author,
is part of the driver contract, or is an invariant shared by multiple
implementations. This keeps tests resilient to changes in worker structure,
lane names, helper functions, and broker client choice.

Use a lower-level or fake boundary only when it makes a real invariant
deterministic:

- test a scheduler, retry ladder, clock, or accounting transition directly in
  its package;
- use the in-memory driver or `f1test` for F1 behavior instead of mocking the
  worker's internal queues;
- use a driver conformance fixture to inject a port-level failure instead of
  reaching into core goroutines; and
- use a broker-backed test when the claim is about a broker client or broker
  state that the port intentionally hides.

A mock that only mirrors the implementation can pass while the public behavior
is broken. Prefer a test that publishes, consumes, settles, drains, or observes
the result through the same boundary a caller uses.

## API and repository boundary checks

Tests protect runtime behavior, but repository gates protect the surfaces that
make those tests meaningful:

- API-surface checks compare exported symbols in the root package, `codec`,
  `driver`, and `f1test` with their committed fixtures. The checker lives in
  [`tools/apisurface`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/tools/apisurface), and the owning Makefile
  targets are `check-api-surface` and its package-specific variants.
- API-diff checks compare the current root, driver, and codec surfaces with
  [`testdata/api-diff/`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/testdata/api-diff). An incompatible change fails the
  check and is a release decision, not a baseline-maintenance detail; record it
  only through the Makefile's explicitly approved baseline workflow. An
  addition fails too, until the same commit records it in the baseline with
  `make record-api-diff-baseline`, which puts the addition in the diff a
  reviewer reads. Either half can be turned off for one local run:
  `API_DIFF_ENFORCE=0` for incompatible changes and `API_DIFF_ADDITIONS_ENFORCE=0`
  for additions.
- `verify-agnostic` runs the `depguard` rules in
  [`.golangci.yml`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/.golangci.yml). It prevents core code from importing
  concrete drivers or broker clients and prevents the conformance package from
  importing a driver.
- `verify-self-contained` checks that repository documentation does not rely on
  unresolved design-record identifiers or paths. Its implementation and
  rationale are in the [Makefile](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/Makefile).

These are repository contract checks, not substitutes for a handler or broker
test. Run them when changing exported symbols, package boundaries, driver
interfaces, or maintainer documentation.

## Choosing a test before changing code

Ask where the claimed behavior lives:

| Change | First test layer | Add when needed |
| --- | --- | --- |
| Handler result, retry policy, dead letter, event payload | `f1test` | Root or in-memory integration for orchestration boundaries |
| Root publish, subscribe, dispatch, settlement, topology, lifecycle | Root-package behavior tests | In-memory integration and failure/reconnect cases |
| Clock, retry ladder, scheduler, dispatch, lifecycle accounting | Pure package tests | Root behavior test if the public contract is also at risk |
| Port behavior or driver capability | Shared conformance | Provider-specific broker tests |
| RabbitMQ or Kafka client/broker behavior | Provider suite | Conformance if the port contract changed |
| Exported API or import boundary | Makefile API/boundary gates | Runtime tests when behavior also changed |

When in doubt, begin with a public behavior test that fails without the
change, then add the narrow unit or provider test that explains the underlying
invariant. Keep both only when they protect different boundaries.

## Maintainer route

Read [Architecture](/development/architecture) for package ownership, [Publish flow](/development/publish-flow)
and [Consume flow](/development/consume-flow) for runtime paths, then
[Driver conformance](/development/driver-conformance) for adapter portability. This page
explains where to place and how to shape the tests that protect those paths.

The [Makefile](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/Makefile), test source, API fixtures, and CI definition in
[`.gitlab-ci.yml`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/.gitlab-ci.yml) own the current commands and gate
composition. This page records the strategy and decision rules, not a second
copy of those command implementations.
