// Package testhook carries the constructors this module's own tests use to run
// the core and the in-memory driver on a fake clock.
//
// Both constructors take an internal/clock.Clock, so neither can be part of an
// exported API: an outside module cannot name that type and could only ever pass
// nil, which is what made the client option and the driver argument unusable.
// Package f1 and package inmem each register their constructor here during init,
// and only this module's own tests and test helpers call them.
//
// The clock state is process-wide and set once per package init, so the hooks
// are not safe to swap while a test runs; a test that needs a different clock
// builds its own client and driver instead.
package testhook

import (
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

var (
	clientOption  func(clock.Clock) any
	driverOnClock func(clock.Clock) driver.Driver
)

// RegisterClientOption records how package f1 builds a client option that runs
// the core on a given clock. It is meant to be called once, from an init
// function.
func RegisterClientOption(construct func(clock.Clock) any) { clientOption = construct }

// RegisterDriver records how package inmem builds an isolated driver on a given
// clock. It is meant to be called once, from an init function.
func RegisterDriver(construct func(clock.Clock) driver.Driver) { driverOnClock = construct }

// ClientOption returns the option that runs the core on c. The caller asserts
// the result to the root package's Option type, which this package cannot name.
// It panics when package f1 registered no constructor, which means the test that
// called it does not link the root package.
func ClientOption(c clock.Clock) any {
	if clientOption == nil {
		panic("testhook: package f1 registered no client clock option")
	}
	return clientOption(c)
}

// Driver returns the in-memory driver that runs on c. It panics when package
// inmem registered no constructor.
func Driver(c clock.Clock) driver.Driver {
	if driverOnClock == nil {
		panic("testhook: package inmem registered no driver constructor")
	}
	return driverOnClock(c)
}
