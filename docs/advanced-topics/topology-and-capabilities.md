# Topology and capabilities

F1 keeps application code on a logical messaging API while the selected driver
maps that API to a concrete messaging system. Two decisions must stay
separate:

- **Topology policy** decides whether F1 declares, verifies, or assumes the
  required destinations already exist.
- **Capability policy** decides which driver features are available natively,
  emulated by F1, or unavailable for this connection.

The core owns the logical topology contract and application-visible semantics.
The driver owns transport syntax and provider-specific administration. This is
why application handlers should use `f1.Event`, `Publisher`, and
`Subscription`, while driver implementations work through the interfaces under
[`driver/`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/driver).

## Select topology policy

F1 exposes three policies:

| Policy | Meaning | Use when |
| --- | --- | --- |
| `f1.TopologyDeclare` | Create missing required topology and report the diff | Local development, integration tests, or an explicitly delegated provisioning boundary |
| `f1.TopologyVerify` | Check that required topology exists and report drift without creating it | Deployments where infrastructure is provisioned separately but startup verification is valuable |
| `f1.TopologyNone` | Assume topology exists and make no topology administration call | The application or platform owns all provisioning and startup checks |

The effective policy is selected in this order:

1. an explicit [`WithTopology`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/options.go) option;
2. `topology.autoCreate: true` in configuration;
3. `topology.verifyOnStart: true` in configuration; or
4. `TopologyNone` when neither configuration setting is enabled.

The policy selection is implemented by [`topologyPolicy`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/client.go) and
validated in [`config.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/config.go). In the current validation,
`topology.autoCreate` is rejected when `Config.Env` is `prod`; production
provisioning should therefore be explicit rather than an accidental side
effect of starting a service.

Set the policy deliberately at the composition boundary:

```go
client, err := f1.New(
	ctx,
	cfg,
	f1.WithDriver(selectedDriver),
	f1.WithTopology(f1.TopologyVerify),
)
if err != nil {
	return fmt.Errorf("connect F1: %w", err)
}
```

`WithTopology` overrides the config-derived policy for this client. Do not use
`TopologyNone` as a workaround for a missing or incorrect destination: the
first publish or consumer creation will fail later, farther from the actual
configuration mistake.

## Know when topology is checked

Topology checks happen at the resource boundary, not at every message:

- `New` opens the selected driver and, when `WithPublishTopics` is present and
  the policy is not `TopologyNone`, ensures the publisher entry points;
- `Subscribe` validates the subscription and returns a `Runner`; it does not
  open a consumer; and
- `Runner.Run` opens the consumer and ensures the subscription's main, retry,
  and dead-letter destinations when the policy is not `TopologyNone`.

The same checks are repeated for resources rebuilt during reconnect. A startup
or reconnect topology error should be surfaced to the service rather than
hidden by creating a second client or runner.

### Declare publisher topics explicitly when useful

`WithPublishTopics` has two jobs: it names the logical topics whose publish
entry points F1 should prepare during `New`, and it restricts publishing to
those topics. The event type's version suffix is normalized to its logical
topic before the allowlist is checked:

```go
client, err := f1.New(
	ctx,
	cfg,
	f1.WithDriver(selectedDriver),
	f1.WithPublishTopics("orders.placed", "orders.cancelled"),
)
```

Use this option when startup should fail before the first publish if the
publisher topology is unavailable. Omit it when publishing topics are managed
outside this client and the service intentionally relies on an existing
topology. The option contract is owned by [`WithPublishTopics`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/options.go)
and topic validation by [`validatePublishTopic`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/publisher.go).

## What F1 generates

Application configuration describes logical topics, priorities, retry policy,
and subscription names. F1 derives the physical topology from those inputs.
Depending on the effective capabilities, the generated specification can
contain:

- publish entry points for each declared logical topic and priority;
- main subscription destinations;
- one destination for each configured retry tier;
- a dead-letter destination, including a stable destination for malformed
  messages whose event type cannot be recovered; and
- an optional driver-native dead-letter backstop when the capability and
  topology support it.

The core also chooses whether a publish entry point is represented as a
routing point with bindings or as a direct destination based on the effective
fan-out capability. The application does not need to know which physical shape
was selected.

Do not construct physical destination names in application code. Use logical
event types, `WithTopic`, `WithPublishTopics`, and `Subscription.Topics`; the
core naming functions keep publish, consume, retry, and dead-letter families
consistent. The topology builders are [`publisherTopologySpec`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/publisher.go)
and [`subscriptionTopologySpecs`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/worker.go).

## Admin operations are non-destructive by default

The driver's [`Admin`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/driver/driver.go) interface provides:

- `EnsureTopology`, which is idempotent, creates missing resources according to
  the selected policy, and reports changes, drift, and in-scope orphans; and
- `DescribeTopology`, which reads current state for inspection and diff output.

`EnsureTopology` does not delete destinations. An orphan in a topology diff is
an observation that requires an operational decision, not permission to remove
data. Destructive operations are an optional [`Maintenance`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/driver/driver.go)
capability; `Prune` must recheck that a destination and its auxiliary retry or
dead-letter resources are empty and unused before deleting them.

When a driver cannot scan the requested scope, `TopologyDiff.OrphanScanError`
must make that limitation visible. Treat an incomplete orphan report as
incomplete rather than as proof that cleanup is safe. The executable topology
contract is in [`driver/topology.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/driver/topology.go).

## Read capabilities after connecting

Capabilities have a static and a live layer:

1. `Driver.Capabilities()` describes the adapter's ceiling without a live
   connection.
2. `Conn.Capabilities()` reports what the connected messaging system actually
   supports and may reduce the driver's declaration.
3. F1 selects the effective set and passes that set to producer, consumer, and
   topology operations.
4. `Client.Limits()` exposes the connected result to the application.

Inspect `Client.Limits()` after `New` when service behavior depends on a
capability:

```go
limits := client.Limits()
for _, feature := range limits.Features {
	log.Printf("F1 feature=%s mode=%v detail=%s",
		feature.Feature, feature.Mode, feature.Detail)
}
```

`FeatureNative` means the connected driver provides the feature directly;
`FeatureEmulated` means F1 provides the same application contract through its
portable core; and `FeatureUnavailable` means the requested behavior cannot be
provided for that connection. The report is a decision surface, not a hint to
cast the connection to a provider-specific type.

The current public feature report is assembled by [`limitsFor`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/limits.go).
Use the symbol and the connected `Detail` field as the authority instead of
maintaining a hand-written driver support table in service documentation.

### Required semantics versus native optimizations

Capabilities must not change observable F1 semantics. A driver may provide a
native optimization for fan-out, delayed delivery, delivery counts, or
settlement, but F1 still owns the public retry, ordering, dead-letter, and
handler contracts. When a native mechanism is absent, the core uses its
portable path where one exists; when a requested semantic cannot be provided,
the operation is rejected.

`OrderedByKey` is an example of a required capability for a requested
subscription mode: an ordered subscription is rejected when the effective
connection does not advertise the feature. See [Ordering and scheduling](/advanced-topics/ordering-and-scheduling).

Physical constraints are not optimizations. Message and header size limits,
consumer scaling limits, and other connection constraints remain relevant even
when strict portability is enabled.

## Use strict portability deliberately

`WithStrictPortability` withdraws optional native shortcuts from the effective
capability set so tests and deployments can exercise F1's portable behavior:

```go
client, err := f1.New(
	ctx,
	cfg,
	f1.WithDriver(selectedDriver),
	f1.WithStrictPortability(),
)
```

Strict portability is useful for checking that a driver-specific optimization
has not become an application dependency. It does not remove physical limits,
make an unavailable semantic available, or authorize a driver to branch on its
own capability view instead of the effective set passed by F1.

The transformation is defined by [`Capabilities.Strict`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/driver/capability.go).
The core and driver conformance suite compare full-capability and strict
profiles for equivalent observable behavior; see
[`driver/conformance`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/driver/conformance).

## Driver implementation boundary

When adding or maintaining a driver, keep these ownership rules:

- `Driver` opens a connection and reports its capability ceiling;
- `Conn` may reduce capabilities after connecting but must not increase them;
- `Admin` translates `TopologySpec` into provider operations and returns a
  useful `TopologyDiff`;
- producer and consumer implementations branch on `ProducerConfig.Effective`
  and `ConsumerConfig.Effective`, not on an independent capability read;
- a missing native feature must follow the core's portable path when the
  contract allows emulation; and
- driver-specific topology syntax, names, and administration errors stay
  behind the driver interfaces.

The complete interface contract is in [`driver/driver.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/driver/driver.go),
and the topology data model is in [`driver/topology.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/driver/topology.go).
Run the conformance package in both normal and strict profiles before treating
a driver change as portable.

## Test topology and capability decisions

Keep application tests focused on logical behavior:

- verify that the chosen topology policy makes startup create, verify, or skip
  administration as intended;
- use `Client.Limits()` to test capability-dependent decisions such as ordered
  subscriptions;
- assert that missing or drifted topology fails at the intended lifecycle
  boundary; and
- use the in-memory driver for deterministic topology and capability tests
  without importing transport types into handlers.

The policy matrix is covered by [`topology_policy_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/topology_policy_test.go),
publisher and consumer specs by [`publisher_topology_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/publisher_topology_test.go)
and [`topology_test.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/topology_test.go), and capability equivalence by
the [driver conformance package](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/tree/main/driver/conformance).

## Common mistakes

- Assuming `TopologyDeclare` is always the effective policy without checking
  `WithTopology` and configuration precedence.
- Enabling auto-creation in a production environment that should use explicit
  infrastructure ownership.
- Expecting `Subscribe` to validate physical topology before `Runner.Run`
  opens the consumer.
- Omitting `WithPublishTopics` and then assuming publisher topology was checked
  at client startup.
- Hardcoding physical destination names or provider routing syntax in service
  code.
- Treating an orphaned destination in a diff as safe to delete without checking
  message depth, auxiliary destinations, and active consumers.
- Reading `Driver.Capabilities()` after connecting instead of using
  `Conn.Capabilities()` and `Client.Limits()`.
- Treating `FeatureNative` as a different application contract from
  `FeatureEmulated`.
- Using strict portability as a way to bypass an unavailable required feature.

## Continue from here

- [Ordering and scheduling](/advanced-topics/ordering-and-scheduling) - capability-gated
  per-key ordering and fair delivery lanes;
- [Lifecycle and shutdown](/advanced-topics/lifecycle-and-shutdown) - resource ownership,
  reconnect, drain, and close sequencing;
- [Publisher and subscriber](/basics/pubsub) - logical topics and
  subscription boundaries;
- [Driver and capabilities](/drivers-and-capabilities) - port structure
  and adapter ownership; and
- [Getting started](/learn/getting-started) - generic driver selection at
  the application composition root.
