package f1

import (
	"fmt"
	"math"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// Limits describes how the connected broker provides each SDK feature.
type Limits struct {
	Driver   string
	Broker   string
	Features []FeatureStatus
}

// FeatureStatus describes one feature under the connected broker.
type FeatureStatus struct {
	Feature string
	Mode    FeatureMode
	Detail  string
}

// FeatureMode describes whether a feature is native, emulated, or unavailable.
type FeatureMode int

const (
	// FeatureNative means the connected broker provides the feature directly.
	FeatureNative FeatureMode = iota
	// FeatureEmulated means the core provides the feature with fixed semantics.
	FeatureEmulated
	// FeatureUnavailable means the feature cannot be provided by this client.
	FeatureUnavailable
)

func capabilityRequired(c *Client, feature string) bool {
	if c == nil || feature != "ordered_by_key" {
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
	feature := func(name string, enabled bool, missing FeatureMode) FeatureStatus {
		if enabled {
			return FeatureStatus{Feature: name, Mode: FeatureNative}
		}
		return FeatureStatus{Feature: name, Mode: missing}
	}
	// native_delay carries the accuracy the connected driver's deferral path
	// delivers, so an application plans against a stated bound rather than
	// against the folklore that a delay is roughly honoured.
	nativeDelay := feature("native_delay", caps.NativeDelay, FeatureEmulated)
	nativeDelay.Detail = delayAccuracyDetail(caps.DelayAccuracy)
	return Limits{Driver: driverName, Broker: info.Display(), Features: []FeatureStatus{
		feature("per_message_ack", caps.PerMessageAck, FeatureEmulated),
		feature("ordered_by_key", caps.OrderedByKey, FeatureUnavailable),
		{Feature: "priority_fairness", Mode: FeatureEmulated},
		nativeDelay,
		feature("delivery_count", caps.NativeDeliveryCount, FeatureEmulated),
		feature("dlq_backstop", caps.NativeDLQ, FeatureUnavailable),
		feature("lag_metrics", caps.LagQueryable, FeatureUnavailable),
		{Feature: "consumer_scaling", Mode: FeatureNative, Detail: caps.ConsumerScaling.String()},
	}}
}
