package version

import (
	"runtime/debug"
	"testing"
)

func TestFromBuildInfo(t *testing.T) {
	for _, test := range []struct {
		name string
		info debug.BuildInfo
		want string
	}{
		{name: "main module", info: debug.BuildInfo{Main: debug.Module{Path: modulePath, Version: "v0.1.0"}}, want: "v0.1.0"},
		{name: "main module without version", info: debug.BuildInfo{Main: debug.Module{Path: modulePath}}, want: devel},
		{name: "dependency", info: debug.BuildInfo{Main: debug.Module{Path: "example.com/app"}, Deps: []*debug.Module{{Path: "other", Version: "v9.9.9"}, {Path: modulePath, Version: "v0.2.1"}}}, want: "v0.2.1"},
		{name: "replaced dependency", info: debug.BuildInfo{Main: debug.Module{Path: "example.com/app"}, Deps: []*debug.Module{{Path: modulePath, Version: "v0.2.1", Replace: &debug.Module{Path: "../sdk"}}}}, want: devel},
		{name: "replaced by a version", info: debug.BuildInfo{Main: debug.Module{Path: "example.com/app"}, Deps: []*debug.Module{{Path: modulePath, Version: "v0.2.1", Replace: &debug.Module{Path: "fork.example/sdk", Version: "v0.2.2"}}}}, want: "v0.2.2"},
		{name: "absent", info: debug.BuildInfo{Main: debug.Module{Path: "example.com/app"}}, want: devel},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := fromBuildInfo(&test.info); got != test.want {
				t.Fatalf("fromBuildInfo() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSDKIsNeverEmpty(t *testing.T) {
	if SDK() == "" {
		t.Fatal("SDK() is empty")
	}
}
