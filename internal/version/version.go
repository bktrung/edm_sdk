// Package version reports the SDK release recorded in the running binary's
// build information, so telemetry and broker handshakes name the release that
// emitted them without a hand-edited constant.
package version

import (
	"runtime/debug"
	"sync"
)

// modulePath is the SDK module whose version is reported.
const modulePath = "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"

// devel is reported when the build carries no module version, such as a test
// binary or a build from a working tree.
const devel = "(devel)"

var sdk = sync.OnceValue(func() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return devel
	}
	return fromBuildInfo(info)
})

// SDK returns the SDK module version, such as "v0.1.0", or "(devel)" when the
// build information records none.
func SDK() string {
	return sdk()
}

// fromBuildInfo finds the SDK module in info: the main module when the SDK is
// built directly, otherwise the dependency entry, following a replace.
func fromBuildInfo(info *debug.BuildInfo) string {
	if info.Main.Path == modulePath {
		return orDevel(info.Main.Version)
	}
	for _, dep := range info.Deps {
		if dep.Path != modulePath {
			continue
		}
		if dep.Replace != nil {
			return orDevel(dep.Replace.Version)
		}
		return orDevel(dep.Version)
	}
	return devel
}

func orDevel(version string) string {
	if version == "" {
		return devel
	}
	return version
}
