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
	// ProfileStrictPortability runs with native optimizations disabled while
	// preserving portable semantics and physical limits.
	ProfileStrictPortability
)

// String returns "strict" for ProfileStrictPortability and "full" otherwise.
func (p Profile) String() string {
	if p == ProfileStrictPortability {
		return "strict"
	}
	return "full"
}

// BrokerView is the broker-side state for one destination at one instant.
type BrokerView struct {
	// Ready is the number of messages available for delivery.
	Ready int64
	// Unsettled is the number of delivered messages not yet settled.
	Unsettled int64
	// Auxiliary is the number of messages held in broker-side auxiliary destinations.
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

// FaultInjector applies one deterministic port-level fault to the suite
// connection and returns an error if it cannot inject the fault.
type FaultInjector func(context.Context, FaultKind) error

// DeadlineFixture creates consumers with a broker-side liveness deadline and
// advances the fixture clock for deterministic pressure checks.
type DeadlineFixture interface {
	// Consumer creates a consumer whose broker-side liveness deadline is timeout.
	Consumer(context.Context, time.Duration, driver.ConsumerConfig) (driver.Consumer, error)
	// Now returns the fixture clock's current time.
	Now() time.Time
	// Advance moves the fixture clock forward by the supplied duration.
	Advance(time.Duration)
}

// DeadlineFixtureFactory builds a deadline fixture on the connection opened by
// Run.
type DeadlineFixtureFactory func(driver.Conn) (DeadlineFixture, error)

// Suite describes one driver conformance run. Run executes both profiles.
type Suite struct {
	// Driver is the adapter to exercise.
	Driver driver.Driver
	// Config is passed to Driver.Open.
	Config driver.Config
	// NewInspector is required and creates an inspector for the connection
	// opened by Run.
	NewInspector InspectorFactory
	// NewFaultInjector optionally creates the fault injector used by Run.
	NewFaultInjector func(driver.Conn) (FaultInjector, error)
	// NewDeadlineFixture optionally creates the deadline fixture used by Run.
	NewDeadlineFixture DeadlineFixtureFactory
}

// BehaviorEvent is one observable event in a behavior vector.
type BehaviorEvent struct {
	// ID identifies the observed behavior.
	ID string
	// Outcome records the result observed by the conformance check.
	Outcome string
	// AttemptCount is the number of attempts observed for the event.
	AttemptCount int
	// FinalDestination is the destination where the event ended.
	FinalDestination string
}

// BehaviorVector is an ordered, diffable record of observable behavior.
type BehaviorVector []BehaviorEvent

// add appends one event to the vector in observation order.
func (v *BehaviorVector) add(event BehaviorEvent) {
	*v = append(*v, event)
}

// ProfileReport contains one profile's vectors and group results.
type ProfileReport struct {
	// Profile is the capability policy used for this report.
	Profile Profile
	// Vector is the ordered behavior observed under this profile.
	Vector BehaviorVector
	// Groups contains the results of the conformance groups that ran.
	Groups []GroupResult
}

// GroupResult records the observed and declared size of a conformance group.
type GroupResult struct {
	// Name identifies the conformance group.
	Name string
	// Declared is the number of checks specified for the group.
	Declared int
	// Observed is the number of checks run for the group.
	Observed int
	// Status records the group's result.
	Status string
	// Skipped contains checks explicitly skipped by the harness.
	Skipped []CheckSkip
}

// CheckSkip records a conformance check skipped because a declared condition
// made it inapplicable, such as a missing fixture or an unsupported deferral
// model.
type CheckSkip struct {
	// Name identifies the skipped check.
	Name string
	// Reason explains why the check was skipped.
	Reason string
}

// CapabilityResult is one capability check in a report.
type CapabilityResult struct {
	// Profile is the capability policy under which the check ran.
	Profile Profile
	// Capability identifies the capability being checked.
	Capability string
	// Declared records the capability value declared by the driver.
	Declared string
	// Status records the check result.
	Status string
	// Evidence describes the observed behavior used by the check.
	Evidence string
}

// Report is the result of one Run call.
type Report struct {
	// Driver is the name of the driver under test.
	Driver string
	// Profiles contains the reports for profiles that completed.
	Profiles []ProfileReport
	// Capabilities contains the capability checks recorded during the run.
	Capabilities []CapabilityResult
	// Pending lists conformance groups declared but not registered by this package.
	Pending []string
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
