package f1

import (
	"fmt"
	"math"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// Limits describes the selected driver's capabilities and the mode used for
// each SDK feature. Its zero value contains no driver or features.
type Limits struct {
	Driver   string
	Broker   string
	Features []FeatureStatus
}

// FeatureStatus describes one feature's availability under the connected
// driver.
type FeatureStatus struct {
	Feature string
	Mode    FeatureMode
	Detail  string
}

// FeatureMode describes whether a feature is native, emulated, or unavailable.
// Its zero value, FeatureNative, means the driver provides the feature directly.
type FeatureMode int

const (
	// FeatureNative means the connected broker provides the feature directly.
	FeatureNative FeatureMode = iota
	// FeatureEmulated means the core provides the feature with fixed semantics.
	FeatureEmulated
	// FeatureUnavailable means the feature cannot be provided by this client.
	FeatureUnavailable
)

// capabilityName is the reported name of one capability entry. limitsFor
// declares each entry with one of these values and the warning decision reads
// the same value, so the name a decision compares against is the name the
// report carries: a rename or a typo is a compile error at one declaration
// instead of a silent detach between two literals.
type capabilityName string

const (
	capabilityPerMessageAck    capabilityName = "per_message_ack"
	capabilityOrderedByKey     capabilityName = "ordered_by_key"
	capabilityPriorityFairness capabilityName = "priority_fairness"
	capabilityNativeDelay      capabilityName = "native_delay"
	capabilityDeliveryCount    capabilityName = "delivery_count"
	capabilityDLQBackstop      capabilityName = "dlq_backstop"
	capabilityLagMetrics       capabilityName = "lag_metrics"
	capabilityConsumerScaling  capabilityName = "consumer_scaling"
)

// capabilityRequired reports whether a capability the connected driver reports
// as unavailable is one the loaded configuration asks for, which is the case
// that logs a warning rather than an information line. Ordered delivery is the
// one: the core emulates it, so a subscription that asked for it runs on a
// promise the driver does not make.
//
// The reported feature is a string because FeatureStatus.Feature is the public
// name a caller reads, so the comparison converts the declared name here, in
// the package that declares it.
//
// It reads the loaded configuration and not the live runners, so a subscription
// passed straight to Subscribe is not counted and a configured subscription that
// is never subscribed is. That is deliberate: the report describes what this
// client's configuration asked of the driver.
func capabilityRequired(c *Client, feature string) bool {
	if c == nil || feature != string(capabilityOrderedByKey) {
		return false
	}
	for _, subscription := range c.config.Subscriptions {
		if subscription.Mode == OrderedByKey {
			return true
		}
	}
	return false
}

func featureModeString(mode FeatureMode) string {
	switch mode {
	case FeatureNative:
		return "native"
	case FeatureEmulated:
		return "emulated"
	default:
		return "unavailable"
	}
}

// maxDelayDuration is the largest delay the port can carry, and the MaxDelay a
// driver declares when no delay a caller can request is above its bound.
const maxDelayDuration = time.Duration(math.MaxInt64)

// undeclaredDelayAccuracy is reported for native_delay when the connected
// driver declares no delay accuracy. It names the gap rather than rendering a
// duration, because a zero bound reads as a promise to deliver at the due time
// exactly, which is the strongest claim the shape can make.
const undeclaredDelayAccuracy = "delay accuracy not declared by this driver"

// delayAccuracyDetail renders a declared delay accuracy as one sentence an
// operator reads without knowing how the driver implements the delay.
func delayAccuracyDetail(accuracy driver.DelayAccuracy) string {
	if accuracy == (driver.DelayAccuracy{}) {
		return undeclaredDelayAccuracy
	}
	bound := "delivered at its due time"
	switch {
	case accuracy.Floor > 0 && accuracy.Relative > 0:
		bound = fmt.Sprintf("late by at most %s, or %s, whichever is larger", relativeDelay(accuracy.Relative), accuracy.Floor)
	case accuracy.Relative > 0:
		bound = "late by at most " + relativeDelay(accuracy.Relative)
	case accuracy.Floor > 0:
		bound = "late by at most " + accuracy.Floor.String()
	}
	if accuracy.MaxDelay >= maxDelayDuration {
		return bound + ", at any requested delay"
	}
	return fmt.Sprintf("%s, for a delay of at most %s; no bound above that", bound, accuracy.MaxDelay)
}

// relativeDelay names a relative lateness bound the way a caller reads it,
// rather than as a fraction of the delay it deferred.
func relativeDelay(relative float64) string {
	if relative == 1 {
		return "the requested delay"
	}
	return fmt.Sprintf("%g times the requested delay", relative)
}

// Limits returns the connected broker's stable capability report.
func (c *Client) Limits() Limits {
	if c == nil {
		return Limits{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	result := c.limits
	result.Features = append([]FeatureStatus(nil), c.limits.Features...)
	return result
}

func limitsFor(driverName string, info driver.BrokerInfo, caps driver.Capabilities) Limits {
	feature := func(name capabilityName, enabled bool, missing FeatureMode) FeatureStatus {
		if enabled {
			return FeatureStatus{Feature: string(name), Mode: FeatureNative}
		}
		return FeatureStatus{Feature: string(name), Mode: missing}
	}
	// native_delay carries the accuracy the connected driver's deferral path
	// delivers, so an application plans against a stated bound rather than
	// against the folklore that a delay is roughly honoured.
	nativeDelay := feature(capabilityNativeDelay, caps.NativeDelay, FeatureEmulated)
	nativeDelay.Detail = delayAccuracyDetail(caps.DelayAccuracy)
	perMessageAck := feature(capabilityPerMessageAck, caps.PerMessageAck, FeatureEmulated)
	if caps.PerMessageAck {
		perMessageAck.Detail = "the broker settles each message independently, so a slow message does not hold its lane's in-flight budget"
	} else {
		perMessageAck.Detail = "the core settles each message itself; settlement order is the core's, so a slow message holds its lane's in-flight budget"
	}
	priorityFairness := FeatureStatus{
		Feature: string(capabilityPriorityFairness),
		Mode:    FeatureEmulated,
		Detail:  "the core's scheduler substitutes weighted lanes, so fairness is per lane and not per broker",
	}
	deliveryCount := feature(capabilityDeliveryCount, caps.NativeDeliveryCount, FeatureEmulated)
	if caps.NativeDeliveryCount {
		deliveryCount.Detail = "the broker supplies a redelivery count to observer events, but handler code reads the core's one-based attempt count instead"
	} else {
		deliveryCount.Detail = "the core counts handler attempts in the envelope; retry copies increment that count, broker redeliveries do not, and a new publish resets it to one"
	}
	dlqBackstop := feature(capabilityDLQBackstop, caps.NativeDLQ, FeatureUnavailable)
	if caps.NativeDLQ {
		dlqBackstop.Detail = "the broker routes an exhausted message to its dead-letter destination; the core also has a successor publish path, but this feature reports only broker-native dead-letter routing"
	} else {
		dlqBackstop.Detail = "the core's dead-letter path publishes a successor and settles the source after publication; this feature reports only broker-native dead-letter routing"
	}
	return Limits{Driver: driverName, Broker: info.Display(), Features: []FeatureStatus{
		perMessageAck,
		feature(capabilityOrderedByKey, caps.OrderedByKey, FeatureUnavailable),
		priorityFairness,
		nativeDelay,
		deliveryCount,
		dlqBackstop,
		feature(capabilityLagMetrics, caps.LagQueryable, FeatureUnavailable),
		{Feature: string(capabilityConsumerScaling), Mode: FeatureNative, Detail: caps.ConsumerScaling.String()},
	}}
}
