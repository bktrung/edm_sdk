package main

import (
	"path/filepath"
	"testing"
)

func TestCanonicalFileNameUsesGOROOTBoundary(t *testing.T) {
	goroot := filepath.Join(string(filepath.Separator), "toolchain", "go")
	standardLibrary := filepath.Join(goroot, "src", "fmt", "print.go")
	if got := canonicalFileName(standardLibrary, goroot); got != "$GOROOT/src/fmt/print.go" {
		t.Fatalf("standard library path = %q, want $GOROOT/src/fmt/print.go", got)
	}

	sharedPrefix := filepath.Join(goroot+"-other", "src", "fmt", "print.go")
	if got := canonicalFileName(sharedPrefix, goroot); got != "$MODULE/print.go" {
		t.Fatalf("shared-prefix path = %q, want $MODULE/print.go", got)
	}
}
