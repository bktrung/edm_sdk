package conformance

import (
	"context"
	"fmt"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// Profile identifies one capability policy used by the suite.
type Profile int

const (
	// ProfileFull runs with the capabilities advertised by the driver.
	ProfileFull Profile = iota
	// ProfileStrictPortability denies native optimization while retaining limits.
	ProfileStrictPortability
)

// String returns the stable report and subtest name for p.
func (p Profile) String() string {
	if p == ProfileStrictPortability {
		return "strict"
	}
	return "full"
}

// BrokerView is the broker-side state for one destination at one instant.
type BrokerView struct {
	Ready     int64
	Unsettled int64
	Auxiliary int64
}

// Inspect reads broker state for one destination.
type Inspect func(ctx context.Context, destination string) (BrokerView, error)

// InspectorFactory binds an inspector to the one connection owned by Run.
type InspectorFactory func(driver.Conn) (Inspect, error)

// FaultKind identifies a port-level condition a conformance fixture can inject.
type FaultKind string

const (
	// FaultPublishFailure makes the next Publish fail with a transient error.
	FaultPublishFailure FaultKind = "publish-failure"
	// FaultConnectionDrop makes current deliveries transiently unavailable and
	// emits a connection error without closing consumer channels.
	FaultConnectionDrop FaultKind = "connection-drop"
	// FaultLaneChannelClose closes one transport lane while leaving the
	// connection and other lanes available.
	FaultLaneChannelClose FaultKind = "lane-channel-close"
	// FaultDeliveryFailure returns current deliveries for redelivery.
	FaultDeliveryFailure FaultKind = "delivery-failure"
	// FaultFatalPublish makes the next Publish fail with a non-retryable error.
	FaultFatalPublish FaultKind = "fatal-publish"
	// FaultCloseFailure makes the next Conn.Close fail after teardown begins.
	// The driver must keep admission closed and accept a retry of Close.
	FaultCloseFailure FaultKind = "close-failure"
)

// FaultInjector applies one deterministic port-level fault to the suite connection.
type FaultInjector func(context.Context, FaultKind) error

// DeadlineFixture creates consumers with a broker-side liveness deadline and
// advances the fixture clock for deterministic pressure checks.
type DeadlineFixture interface {
	Consumer(context.Context, time.Duration, driver.ConsumerConfig) (driver.Consumer, error)
	Now() time.Time
	Advance(time.Duration)
}

// DeadlineFixtureFactory builds a deadline fixture on the suite connection.
// The fixture is conformance-only; the frozen driver port remains unchanged.
type DeadlineFixtureFactory func(driver.Conn) (DeadlineFixture, error)

// DeferralModel names the model a driver implements for a message that carries
// a due time.
type DeferralModel int

const (
	// DeferralExact delivers a deferred message at the due time the message
	// carries. It is the zero value, so a harness that declares no model keeps
	// exact deferral.
	DeferralExact DeferralModel = iota
	// DeferralDestinationDelay delivers a deferred message at its publish instant
	// plus the delay its destination declares, whatever due time the message
	// carries. It is never earlier than that instant, and the messages it deferred
	// within one partition of a destination are delivered in the order they were
	// published.
	DeferralDestinationDelay
)

// Suite describes one driver conformance run. Run executes both profiles.
type Suite struct {
	Driver             driver.Driver
	Config             driver.Config
	NewInspector       InspectorFactory
	NewFaultInjector   func(driver.Conn) (FaultInjector, error)
	NewDeadlineFixture DeadlineFixtureFactory

	// DeferralModel names the model the driver under test implements and selects
	// the deferred checks that model owes. It is a harness declaration rather than
	// a capability, because it describes semantics rather than an optimisation. The
	// zero value is DeferralExact, so a suite that leaves it unset keeps exact
	// deferral.
	DeferralModel DeferralModel
}

// BehaviorEvent is one observable event in a behavior vector.
type BehaviorEvent struct {
	ID               string
	Outcome          string
	AttemptCount     int
	FinalDestination string
}

// BehaviorVector is an ordered, diffable record of observable behavior.
type BehaviorVector []BehaviorEvent

// Add appends one event to the vector in observation order.
func (v *BehaviorVector) Add(event BehaviorEvent) {
	*v = append(*v, event)
}

// ProfileReport contains one profile's vectors and group results.
type ProfileReport struct {
	Profile Profile
	Vector  BehaviorVector
	Groups  []GroupResult
}

// GroupResult records the observed and declared size of a conformance group.
type GroupResult struct {
	Name     string
	Declared int
	Observed int
	Status   string
	Skipped  []CheckSkip
}

// CheckSkip records a check the harness's declared conditions gate before marking its
// subtest skipped. It distinguishes an intentional result, such as an absent
// fixture or a deferral model the check does not apply to, from t.Skip used to
// pad a group while still satisfying its manifest count.
type CheckSkip struct {
	Name   string
	Reason string
}

// CapabilityResult is one capability check in a report.
type CapabilityResult struct {
	Profile    Profile
	Capability string
	Declared   string
	Status     string
	Evidence   string
}

// Report is the result of one Run call.
type Report struct {
	Driver       string
	Profiles     []ProfileReport
	Capabilities []CapabilityResult
	Pending      []string
}

// Diff returns a human-readable vector difference, or an empty string.
func (v BehaviorVector) Diff(other BehaviorVector) string {
	if len(v) != len(other) {
		return fmt.Sprintf("length differs: want %d, got %d", len(v), len(other))
	}
	for i := range v {
		if v[i] != other[i] {
			return fmt.Sprintf("event %d differs: want %+v, got %+v", i, v[i], other[i])
		}
	}
	return ""
}
