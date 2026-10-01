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
	c.sendError(classify("consumer", driver.KindTransient, errors.New("channel closed")))

	found := false
	for range len(c.errors) {
		if errors.Is(<-c.errors, reattach) {
			found = true
		}
	}
	if !found {
		t.Fatal("a failed re-attach was dropped by a full error buffer, so the stopped lane goes unreported")
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
