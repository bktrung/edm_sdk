# Architecture

What each package owns, how a message moves through them, and the import boundaries that keep it
that way. This file describes structure, not the reasoning behind it: it tells you where code goes
and what may import what, so that a change lands in the right package and the build catches it when
it does not.

## Layers

One-way dependency, application at the top:

```
application code (service-owned handlers)
    -> package f1            public API: Client, Publisher, Consumer, Handler, Event
        -> internal/*        core: codec, dispatch, sched, retry, dedupe, lifecycle, obs
            -> driver        the port: interfaces only, stdlib only
drivers/kafka, drivers/rabbitmq, drivers/inmem
    -> driver                implement the port
    -> their broker's client library
```

The core never has a `switch driver.Name()`. A capability the core needs to branch on belongs in the
capability model, not a type switch.

## Package tree

| Package | Owns |
|---|---|
| `.` (`package f1`) | The public API surface: `Client`, `Publisher`, `Consumer`, `Handler`, `Event`, error taxonomy |
| `driver/` (`package driver`) | The port: the interfaces a broker driver implements. Zero third-party imports |
| `drivers/inmem/` (`package inmem`) | Deterministic, race-free reference driver; the SDK's canonical test fake |
| `codec/` | Payload encode/decode behind a port. Public API, not `internal/` - a third party can implement its own codec |
| `internal/clock/` | Injectable time source; all timing-dependent code goes through it |
| `internal/obs/` | Metric, trace and log emission, including the sampling loop |

`dedupe`, `outbox`, `internal/sched`, `internal/retry`, `internal/lifecycle`, `drivers/kafka`,
`drivers/rabbitmq`, `store/` and `cmd/` are part of the target layout and are not created yet. Add
one only when the code that lives in it arrives, so the tree never advertises a package a reader
cannot open.

## Data flow

Publish: `f1.Publisher` resolves the logical topic to a physical destination, builds the envelope,
and hands it to `driver.Producer`; `Publish` returns only after the broker has durably
acknowledged. Consume: a driver delivers into per-lane buffers, a DWRR scheduler picks the next
message, a worker decodes it, checks the dedupe store, runs the handler, and settles - acking the
original only after any retry or DLQ copy is confirmed, never before.

That ack ordering is the zero-loss guarantee and it is not an implementation detail: a message is
acked only once its retry or dead-letter copy is durable, so a crash at any point leaves the
original redeliverable rather than lost.

## Import boundaries

Three rules, enforced by `depguard` and proven by `make verify-agnostic`:

- `internal/**` and the root package (`f1`) must not import `drivers/**`, or a broker client
  directly.
- `driver` (the port) must not import anything outside stdlib.
- A `drivers/<broker>` package must import stdlib, `driver`, and only its own broker client - not
  another driver's.

Two packages sit inside those paths but are not bound by the rule covering them, and each has its
own `depguard` list:

- `driver/conformance/` is a test suite, not the port. It imports the port and testify, and never a
  driver: it is what every driver is measured against, so it cannot know any of them.
- `cmd/` is this module's composition root. Its binaries register drivers by blank import exactly as
  an application does, so "core must not import drivers" does not apply there - but reaching a
  broker client directly still does.

A violation fails the build; `make verify-agnostic` is that failure made checkable on demand,
independent of the rest of `make lint`.

**When editing these rules:** `depguard` matches import paths by *prefix*. Allowing
`.../driver` also allows `.../drivers/kafka`, because the first is a literal prefix of the second.
Every rule that allows the port therefore carries an explicit `.../drivers` deny, and deny wins over
allow. Removing one of those denies leaves a rule that reads correctly and enforces nothing - change
these only alongside a probe that watches the rule fail.
