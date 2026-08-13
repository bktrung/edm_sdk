package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestExtractSurfaceExcludesUnexportedMethods(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "sample")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	source := `package sample

type Public struct{}

func (Public) Exported() {}
func (Public) private() {}

type private struct{}

func (private) Exported() {}
func Top() {}
func privateFunc() {}

const Exported = 1
`
	if err := os.WriteFile(filepath.Join(dir, "sample.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := extractSurface(root, "sample")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"Exported", "Public", "Public.Exported", "Top"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("surface = %v, want %v", got, want)
	}
}
