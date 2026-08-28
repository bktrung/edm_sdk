package driver

import (
	"reflect"
	"testing"
)

func TestStrict_ZeroesEveryNegotiableCapability(t *testing.T) {
	t.Parallel()

	got := (Capabilities{
		PerMessageAck:        true,
		OrderedByKey:         true,
		Fanout:               FanoutAtPublish,
		NativePriority:       PriorityStrict,
		NativePriorityLevels: 32,
		NativeDelay:          true,
		NativeDeliveryCount:  true,
		NativeDLQ:            true,
		ConsumerScaling:      ScalingFree,
		MaxMessageBytes:      100,
		MaxHeaderBytes:       20,
		LagQueryable:         true,
	}).Strict()

	value := reflect.ValueOf(got)
	// Constraints survive the strict profile; capabilities do not. A field added
	// here must be a fact about the broker that holds under either profile.
	kept := map[string]bool{
		"Fanout":          true,
		"ConsumerScaling": true,
		"OrderedByKey":    true,
		"MaxMessageBytes": true,
		"MaxHeaderBytes":  true,
	}
	typeOfCapabilities := value.Type()
	for i := range value.NumField() {
		fieldName := typeOfCapabilities.Field(i).Name
		if !kept[fieldName] && !value.Field(i).IsZero() {
			t.Errorf("Strict() field %s = %#v, want zero", fieldName, value.Field(i).Interface())
		}
	}
	if got.MaxMessageBytes != 100 || got.MaxHeaderBytes != 20 {
		t.Fatalf("Strict() lost physical limits: %#v", got)
	}
}

func TestStrict_PreservesConsumerScaling(t *testing.T) {
	t.Parallel()

	// ScalingPartitionBound is the zero value, so a Strict that dropped the
	// field would not withdraw the declaration: it would silently replace a
	// free-scaling broker with a partition-bound one carrying no partitions.
	// Asserting the free case is what makes that substitution visible.
	if got := (Capabilities{ConsumerScaling: ScalingFree}).Strict(); got.ConsumerScaling != ScalingFree {
		t.Errorf("Strict() ConsumerScaling = %v, want %v", got.ConsumerScaling, ScalingFree)
	}
	if got := (Capabilities{ConsumerScaling: ScalingPartitionBound}).Strict(); got.ConsumerScaling != ScalingPartitionBound {
		t.Errorf("Strict() ConsumerScaling = %v, want %v", got.ConsumerScaling, ScalingPartitionBound)
	}
}

func TestStrict_PreservesOrderedByKey(t *testing.T) {
	t.Parallel()

	if got := (Capabilities{OrderedByKey: true}).Strict(); !got.OrderedByKey {
		t.Error("Strict() removed OrderedByKey")
	}
}

func TestPriorityMode_String(t *testing.T) {
	t.Parallel()

	tests := map[PriorityMode]string{
		PriorityNone:     "none",
		PriorityRelative: "relative",
		PriorityStrict:   "strict",
	}
	for input, want := range tests {
		if got := input.String(); got != want {
			t.Errorf("PriorityMode(%d).String() = %q, want %q", input, got, want)
		}
	}
}

func TestScaling_String(t *testing.T) {
	t.Parallel()

	tests := map[Scaling]string{
		ScalingPartitionBound: "partition-bound",
		ScalingFree:           "free",
	}
	for input, want := range tests {
		if got := input.String(); got != want {
			t.Errorf("Scaling(%d).String() = %q, want %q", input, got, want)
		}
	}
}
