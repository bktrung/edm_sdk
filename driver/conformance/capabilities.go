package conformance

import "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"

func effectiveCapabilities(caps driver.Capabilities, profile Profile) driver.Capabilities {
	if profile == ProfileStrictPortability {
		return caps.Strict()
	}
	return caps
}
