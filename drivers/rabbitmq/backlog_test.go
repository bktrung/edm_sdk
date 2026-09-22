package rabbitmq

import (
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestManagementQueueHeadEnqueuedAt(t *testing.T) {
	want := time.Unix(1_724_123_456, 0)
	got, source := managementQueue{HeadMessageTimestamp: want.Unix()}.headEnqueuedAt()
	if !got.Equal(want) {
		t.Fatalf("headEnqueuedAt() = %s, want %s", got, want)
	}
	if source != driver.EnqueueSourceProducer {
		t.Fatalf("head source = %q, want producer", source)
	}

	got, source = (managementQueue{}).headEnqueuedAt()
	if !got.IsZero() {
		t.Fatalf("empty headEnqueuedAt() = %s, want zero", got)
	}
	if source != driver.EnqueueSourceUnknown {
		t.Fatalf("empty head source = %q, want unknown", source)
	}
}
