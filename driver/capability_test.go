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
		NativePriority:       PriorityStrict,
		NativePriorityLevels: 32,
		NativeDelay:          true,
		NativeDeliveryCount:  true,
		NativeDLQ:            true,
		Transactions:         true,
		ConsumerScaling:      ScalingFree,
		ServerSideFilter:     true,
		MaxMessageBytes:      100,
		MaxHeaderBytes:       20,
		LagQueryable:         true,
	}).Strict()

	value := reflect.ValueOf(got)
	kept := map[string]bool{
		"MaxMessageBytes": true,
		"MaxHeaderBytes":  true,
	}
	typeOfCapabilities := value.Type()
	for i := 0; i < value.NumField(); i++ {
		fieldName := typeOfCapabilities.Field(i).Name
		if !kept[fieldName] && !value.Field(i).IsZero() {
			t.Errorf("Strict() field %s = %#v, want zero", fieldName, value.Field(i).Interface())
		}
	}
	if got.MaxMessageBytes != 100 || got.MaxHeaderBytes != 20 {
		t.Fatalf("Strict() lost physical limits: %#v", got)
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
