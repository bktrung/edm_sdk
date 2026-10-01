package kafka

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestCapabilityDeclarationAndBrokerLimits(t *testing.T) {
	declared := Driver{}.Capabilities()
	if declared.PerMessageAck {
		t.Error("PerMessageAck = true, want false")
	}
	if declared.NativeDeliveryCount {
		t.Error("NativeDeliveryCount = true, want false")
	}
	if declared.ConsumerScaling != driver.ScalingPartitionBound {
		t.Errorf("ConsumerScaling = %v, want partition-bound", declared.ConsumerScaling)
	}
	if !declared.OrderedByKey {
		t.Error("OrderedByKey = false, want true")
	}
	if declared.NativeDLQ {
		t.Error("NativeDLQ = true, want false")
	}
	if declared.Fanout != driver.FanoutAtConsume {
		t.Errorf("Fanout = %v, want consume", declared.Fanout)
	}
	if !declared.LagQueryable {
		t.Error("LagQueryable = false, want true")
	}
	if declared.NativePriority != driver.PriorityNone {
		t.Errorf("NativePriority = %v, want none", declared.NativePriority)
	}
	if declared.NativeDelay {
		t.Error("NativeDelay = true, want false")
	}

	const maxMessageBytes = 123456
	// A connection reports this value, so it may add the broker's own limits
	// and must not turn a capability back on: a connection wider than the
	// factory is a driver claiming something on a connection it does not have.
	connected := brokerCapabilities(maxMessageBytes)
	if connected.PerMessageAck {
		t.Error("connected PerMessageAck = true, want false")
	}
	if connected.NativeDeliveryCount {
		t.Error("connected NativeDeliveryCount = true, want false")
	}
	if connected.ConsumerScaling != driver.ScalingPartitionBound {
		t.Errorf("connected ConsumerScaling = %v, want partition-bound", connected.ConsumerScaling)
	}
	if !connected.OrderedByKey {
		t.Error("connected OrderedByKey = false, want true")
	}
	// Kafka exposes no header limit.
	if connected.MaxHeaderBytes != 0 {
		t.Errorf("connected MaxHeaderBytes = %d, want 0", connected.MaxHeaderBytes)
	}
	if connected.MaxMessageBytes != maxMessageBytes {
		t.Errorf("connected MaxMessageBytes = %d, want %d", connected.MaxMessageBytes, maxMessageBytes)
	}

	strict := connected.Strict()
	wantStrict := driver.Capabilities{
		OrderedByKey:    true,
		Fanout:          driver.FanoutAtConsume,
		ConsumerScaling: driver.ScalingPartitionBound,
		MaxMessageBytes: maxMessageBytes,
	}
	if !reflect.DeepEqual(strict, wantStrict) {
		t.Fatalf("Strict() = %#v, want %#v", strict, wantStrict)
	}
}

func TestEffectiveProducerBatchBytes(t *testing.T) {
	const maxBatchBytes = 1 << 30
	cases := []struct {
		name        string
		brokerLimit int
		want        int
		wantErr     bool
	}{
		{name: "below minimum", brokerLimit: 511, wantErr: true},
		{name: "minimum", brokerLimit: 512, want: 512},
		{name: "broker limit", brokerLimit: 123456, want: 123456},
		{name: "above maximum", brokerLimit: maxBatchBytes + 1, want: maxBatchBytes},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := effectiveProducerBatchBytes(tc.brokerLimit)
			if tc.wantErr {
				if err == nil {
					t.Fatal("effectiveProducerBatchBytes() error = nil, want error")
				}
				if !errors.Is(err, errBrokerConfig) {
					t.Fatalf("effectiveProducerBatchBytes() error = %v, want errBrokerConfig", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("effectiveProducerBatchBytes() error = %v", err)
			}
			if got != tc.want {
				t.Errorf("effectiveProducerBatchBytes() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestKafkaBodyLimitSizing(t *testing.T) {
	cases := []struct {
		name       string
		batchLimit int
		wantBody   int
	}{
		{name: "empty body", batchLimit: 72, wantBody: 0},
		{name: "record length varint first boundary", batchLimit: 131, wantBody: 58},
		{name: "body length varint first boundary", batchLimit: 138, wantBody: 64},
		{name: "record length varint second boundary", batchLimit: 8260, wantBody: 8185},
		{name: "body length varint second boundary", batchLimit: 8268, wantBody: 8192},
		{name: "record length varint third boundary", batchLimit: 1048645, wantBody: 1048568},
		{name: "body length varint third boundary", batchLimit: 1048654, wantBody: 1048576},
		{name: "record length varint fourth boundary", batchLimit: 134217798, wantBody: 134217719},
		{name: "body length varint fourth boundary", batchLimit: 134217808, wantBody: 134217728},
		{name: "maximum batch limit", batchLimit: 1 << 30, wantBody: (1 << 30) - 80},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := maxKafkaBodyBytes(tc.batchLimit)
			if got != tc.wantBody {
				t.Fatalf("maxKafkaBodyBytes(%d) = %d, want %d", tc.batchLimit, got, tc.wantBody)
			}
			if got > 0 {
				if previous := maxKafkaBodyBytes(tc.batchLimit - 1); previous != got-1 {
					t.Fatalf("maxKafkaBodyBytes(%d) = %d, want %d", tc.batchLimit-1, previous, got-1)
				}
			}
			if encoded := kafkaBareRecordBatchBytes(got); encoded > tc.batchLimit {
				t.Fatalf("kafkaBareRecordBatchBytes(%d) = %d, exceeds limit %d", got, encoded, tc.batchLimit)
			}
			if encoded := kafkaBareRecordBatchBytes(got + 1); encoded <= tc.batchLimit {
				t.Fatalf("kafkaBareRecordBatchBytes(%d) = %d, fits limit %d", got+1, encoded, tc.batchLimit)
			}
		})
	}
}

func TestKafkaErrorKind(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want driver.Kind
	}{
		{name: "permission/topic", err: fmt.Errorf("wrapped: %w", kerr.TopicAuthorizationFailed), want: driver.KindPermission},
		{name: "permission/group", err: fmt.Errorf("wrapped: %w", kerr.GroupAuthorizationFailed), want: driver.KindPermission},
		{name: "permission/cluster", err: fmt.Errorf("wrapped: %w", kerr.ClusterAuthorizationFailed), want: driver.KindPermission},
		{name: "permission/transactional-id", err: fmt.Errorf("wrapped: %w", kerr.TransactionalIDAuthorizationFailed), want: driver.KindPermission},
		{name: "permission/delegation-token", err: fmt.Errorf("wrapped: %w", kerr.DelegationTokenAuthorizationFailed), want: driver.KindPermission},
		{name: "too large", err: fmt.Errorf("wrapped: %w", kerr.MessageTooLarge), want: driver.KindTooLarge},
		{name: "not found", err: fmt.Errorf("wrapped: %w", kerr.UnknownTopicOrPartition), want: driver.KindNotFound},
		{name: "fatal/sasl-authentication", err: fmt.Errorf("wrapped: %w", kerr.SaslAuthenticationFailed), want: driver.KindFatal},
		{name: "fatal/unsupported-sasl", err: fmt.Errorf("wrapped: %w", kerr.UnsupportedSaslMechanism), want: driver.KindFatal},
		{name: "fatal/illegal-sasl-state", err: fmt.Errorf("wrapped: %w", kerr.IllegalSaslState), want: driver.KindFatal},
		{name: "fatal/unsupported-version", err: fmt.Errorf("wrapped: %w", kerr.UnsupportedVersion), want: driver.KindFatal},
		{name: "fatal/invalid-request", err: fmt.Errorf("wrapped: %w", kerr.InvalidRequest), want: driver.KindFatal},
		{name: "fatal/security-disabled", err: fmt.Errorf("wrapped: %w", kerr.SecurityDisabled), want: driver.KindFatal},
		{name: "fatal/broker-config", err: fmt.Errorf("wrapped: %w", errBrokerConfig), want: driver.KindFatal},
		{name: "fatal/protocol-response", err: fmt.Errorf("wrapped: %w", errProtocolResponse), want: driver.KindFatal},
		{name: "transient default", err: errors.New("unrecognised Kafka error"), want: driver.KindTransient},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := kafkaErrorKind(tc.err); got != tc.want {
				t.Fatalf("kafkaErrorKind(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestStaticMembershipResolution(t *testing.T) {
	cases := []struct {
		name    string
		options map[string]string
		want    bool
		wantErr bool
	}{
		{name: "default when missing", options: nil, want: true},
		{name: "empty options map", options: map[string]string{}, want: true},
		{name: "explicit true", options: map[string]string{"kafka.staticMembership": "true"}, want: true},
		{name: "explicit 1", options: map[string]string{"kafka.staticMembership": "1"}, want: true},
		{name: "explicit false", options: map[string]string{"kafka.staticMembership": "false"}, want: false},
		{name: "explicit 0", options: map[string]string{"kafka.staticMembership": "0"}, want: false},
		{name: "invalid boolean string", options: map[string]string{"kafka.staticMembership": "invalid"}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveStaticMembership(tc.options)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("resolveStaticMembership(%v) wanted error, got nil", tc.options)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveStaticMembership(%v) unexpected error: %v", tc.options, err)
			}
			if got != tc.want {
				t.Fatalf("resolveStaticMembership(%v) = %v, want %v", tc.options, got, tc.want)
			}
		})
	}
}
