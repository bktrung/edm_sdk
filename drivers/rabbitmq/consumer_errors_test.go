package rabbitmq

import (
	"errors"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestFailedReattachIsNotCrowdedOutByCancelNotices(t *testing.T) {
	c := &consumer{errors: make(chan error, 8)}
	for range cap(c.errors) {
		c.sendError(classify("consumer", driver.KindNotification, errors.New("broker cancelled the consumer")))
	}
	reattach := classify("consumer", driver.KindNotFound, errors.New("re-establishing destination"))
	c.sendError(reattach)
	transient := classify("consumer", driver.KindTransient, errors.New("channel closed"))
	c.sendError(transient)

	foundReattach, foundTransient := false, false
	for range len(c.errors) {
		queued := <-c.errors
		foundReattach = foundReattach || errors.Is(queued, reattach)
		foundTransient = foundTransient || errors.Is(queued, transient)
	}
	if !foundReattach {
		t.Fatal("a failed re-attach was dropped by a full error buffer, so the stopped lane goes unreported")
	}
	if !foundTransient {
		t.Fatal("a transient close was dropped behind cancel notices, so nothing tells the core to repair the lane")
	}
}

// TestFullBufferDropsTransientBehindAQueuedRecoverySignal pins the other side
// of the buffer rule: once a transient, unclassified or fatal error is queued,
// a later transient error is dropped rather than evicting another entry, since
// one queued recovery signal already makes the core repair the generation.
func TestFullBufferDropsTransientBehindAQueuedRecoverySignal(t *testing.T) {
	tests := []struct {
		name string
		kind driver.Kind
	}{
		{name: "transient", kind: driver.KindTransient},
		{name: "fatal", kind: driver.KindFatal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := &consumer{errors: make(chan error, 2)}
			queued := classify("consumer", test.kind, errors.New("queued recovery signal"))
			c.sendError(classify("consumer", driver.KindNotification, errors.New("broker cancelled the consumer")))
			c.sendError(queued)
			late := classify("consumer", driver.KindTransient, errors.New("channel closed"))
			c.sendError(late)

			got := make([]error, 0, cap(c.errors))
			for len(c.errors) > 0 {
				got = append(got, <-c.errors)
			}
			if len(got) != 2 {
				t.Fatalf("queued %d errors, want the two original entries", len(got))
			}
			for _, item := range got {
				if errors.Is(item, late) {
					t.Fatalf("a later transient displaced the queued %s signal: %v", test.name, got)
				}
			}
			if !errors.Is(got[1], queued) {
				t.Fatalf("queued %v, want the queued %s signal kept", got, test.name)
			}
		})
	}
}

// TestFullBufferEvictsANotificationBeforeASevereReport pins which queued entry
// goes when a transient error takes a full buffer's last slot: the notification
// first, so a severe report the core would otherwise only hear once survives.
// The notification is queued behind the severe report, so evicting the oldest
// entry would still lose the severe report and only the preference keeps it.
func TestFullBufferEvictsANotificationBeforeASevereReport(t *testing.T) {
	c := &consumer{errors: make(chan error, 2)}
	severe := classify("consumer", driver.KindNotFound, errors.New("re-establishing destination"))
	notice := classify("consumer", driver.KindNotification, errors.New("broker cancelled the consumer"))
	c.sendError(severe)
	c.sendError(notice)
	c.sendError(classify("consumer", driver.KindTransient, errors.New("channel closed")))

	got := make([]error, 0, cap(c.errors))
	for len(c.errors) > 0 {
		got = append(got, <-c.errors)
	}
	if len(got) != 2 || !errors.Is(got[0], severe) {
		t.Fatalf("queued %v, want the severe report kept at the head beside the transient", got)
	}
	for _, item := range got {
		if errors.Is(item, notice) {
			t.Fatalf("queued %v, want the notification evicted ahead of the severe report", got)
		}
	}
}

func TestSevereErrorDoesNotDisplaceQueuedFatal(t *testing.T) {
	c := &consumer{errors: make(chan error, 1)}
	fatal := classify("consumer", driver.KindFatal, errors.New("fatal"))
	c.sendError(fatal)
	c.sendError(classify("consumer", driver.KindNotFound, errors.New("re-establishing destination")))
	if got := <-c.errors; !errors.Is(got, fatal) {
		t.Fatalf("queued error = %v, want the fatal error kept", got)
	}
}
