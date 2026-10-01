package kafka

import (
	"errors"
	"fmt"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestSevereConsumerErrorDoesNotDisplaceQueuedFatal(t *testing.T) {
	const errorCapacity = 8
	c := &consumer{errors: make(chan error, errorCapacity)}
	c.sendError(classify("consumer", driver.KindFatal, errors.New("fatal consumer failure")))
	for sequence := range errorCapacity - 1 {
		c.sendError(classify("consumer", driver.KindTransient, fmt.Errorf("transient consumer failure %d", sequence)))
	}

	c.sendError(classify("consumer", driver.KindNotFound, errors.New("topic not found")))

	fatalSurvived := false
	for range errorCapacity {
		if kind, classified := driver.Classify(<-c.errors); classified && kind == driver.KindFatal {
			fatalSurvived = true
		}
	}
	if !fatalSurvived {
		t.Fatal("queued fatal error was displaced by a later severe error")
	}
}
