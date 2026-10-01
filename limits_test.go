package f1

import (
	"context"
	"strings"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// reportedLimits connects a client to a fake driver declaring caps and returns
// the feature report that connection publishes.
func reportedLimits(t *testing.T, caps driver.Capabilities, options ...Option) Limits {
	t.Helper()
	fakeDriver := &testDriver{conn: &testConn{caps: caps, info: driver.BrokerInfo{Kind: "test", Version: "1"}}}
	client, err := New(context.Background(), testClientConfig(t), append([]Option{WithDriver(fakeDriver)}, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(context.Background()); err != nil {
			t.Errorf("close client: %v", err)
		}
	})
	return client.Limits()
}

func TestLimitsReportsCoreFeatureDetails(t *testing.T) {
	t.Parallel()

	const (
		emulatedPerMessageAck  = "the core settles each message itself; settlement order is the core's, so a slow message holds its lane's in-flight budget"
		nativePerMessageAck    = "the broker settles each message independently, so a slow message does not hold its lane's in-flight budget"
		emulatedDeliveryCount  = "the core counts handler attempts in the envelope; retry copies increment that count, broker redeliveries do not, and a new publish resets it to one"
		nativeDeliveryCount    = "the broker supplies a redelivery count to observer events, but handler code reads the core's one-based attempt count instead"
		emulatedDLQBackstop    = "the core's dead-letter path publishes a successor and settles the source after publication; this feature reports only broker-native dead-letter routing"
		nativeDLQBackstop      = "the broker routes an exhausted message to its dead-letter destination; the core also has a successor publish path, but this feature reports only broker-native dead-letter routing"
		priorityFairnessDetail = "the core's scheduler substitutes weighted lanes, so fairness is per lane and not per broker"
		emulatedNativeDelay    = "the driver holds each message on a delayed destination for that destination's delay, in a parking queue or on the consumer"
		nativeNativeDelay      = "the broker holds each message on a delayed destination for that destination's delay"
	)
	tests := []struct {
		name       string
		feature    string
		caps       driver.Capabilities
		wantMode   FeatureMode
		wantDetail string
	}{
		{
			name:       "per message ack emulated",
			feature:    "per_message_ack",
			wantMode:   FeatureEmulated,
			wantDetail: emulatedPerMessageAck,
		},
		{
			name:       "per message ack native",
			feature:    "per_message_ack",
			caps:       driver.Capabilities{PerMessageAck: true},
			wantMode:   FeatureNative,
			wantDetail: nativePerMessageAck,
		},
		{
			name:       "delivery count emulated",
			feature:    "delivery_count",
			wantMode:   FeatureEmulated,
			wantDetail: emulatedDeliveryCount,
		},
		{
			name:       "delivery count native",
			feature:    "delivery_count",
			caps:       driver.Capabilities{NativeDeliveryCount: true},
			wantMode:   FeatureNative,
			wantDetail: nativeDeliveryCount,
		},
		{
			name:       "dead letter backstop unavailable",
			feature:    "dlq_backstop",
			wantMode:   FeatureUnavailable,
			wantDetail: emulatedDLQBackstop,
		},
		{
			name:       "dead letter backstop native",
			feature:    "dlq_backstop",
			caps:       driver.Capabilities{NativeDLQ: true},
			wantMode:   FeatureNative,
			wantDetail: nativeDLQBackstop,
		},
		{
			name:       "native delay emulated",
			feature:    "native_delay",
			wantMode:   FeatureEmulated,
			wantDetail: emulatedNativeDelay,
		},
		{
			name:       "native delay native",
			feature:    "native_delay",
			caps:       driver.Capabilities{NativeDelay: true},
			wantMode:   FeatureNative,
			wantDetail: nativeNativeDelay,
		},
		{
			name:       "priority fairness always emulated",
			feature:    "priority_fairness",
			caps:       driver.Capabilities{PerMessageAck: true, NativeDeliveryCount: true, NativeDLQ: true},
			wantMode:   FeatureEmulated,
			wantDetail: priorityFairnessDetail,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			status := featureStatus(reportedLimits(t, test.caps), test.feature)
			if status.Mode != test.wantMode {
				t.Errorf("%s mode = %v, want %v", test.feature, status.Mode, test.wantMode)
			}
			if status.Detail != test.wantDetail {
				t.Errorf("%s detail = %q, want %q", test.feature, status.Detail, test.wantDetail)
			}
		})
	}
}

func TestStrictPortabilityReportsEmulatedDelay(t *testing.T) {
	t.Parallel()

	// Under strict the core does not use the driver's native delay, so the
	// report names the emulated path even when the driver declares one.
	caps := driver.Capabilities{NativeDelay: true, ConsumerScaling: driver.ScalingFree}
	got := featureStatus(reportedLimits(t, caps, WithStrictPortability()), "native_delay")
	if got.Mode != FeatureEmulated {
		t.Fatalf("strict native_delay mode = %v, want emulated", got.Mode)
	}
	if !strings.Contains(got.Detail, "the driver holds each message") {
		t.Fatalf("strict native_delay detail = %q, want the emulated detail", got.Detail)
	}
}
