package conformance

import "github.com/bktrung/edm_sdk/driver"

func effectiveCapabilities(caps driver.Capabilities, profile Profile) driver.Capabilities {
	if profile == ProfileStrictPortability {
		return caps.Strict()
	}
	return caps
}
