package driver

import "testing"

func TestEnqueueSource_String(t *testing.T) {
	t.Parallel()

	tests := map[EnqueueSource]string{
		EnqueueSourceUnknown:  "unknown",
		EnqueueSourceProducer: "producer",
		EnqueueSourceBroker:   "broker",
		EnqueueSource(99):     "unknown",
	}
	for input, want := range tests {
		if got := input.String(); got != want {
			t.Errorf("EnqueueSource(%d).String() = %q, want %q", input, got, want)
		}
	}
}
