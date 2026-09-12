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
	moreSource := `package sample

func Additional() {}
`
	if err := os.WriteFile(filepath.Join(dir, "more.go"), []byte(moreSource), 0o644); err != nil {
		t.Fatal(err)
	}
	testSource := `package sample

func TestOnly() {}
`
	if err := os.WriteFile(filepath.Join(dir, "sample_test.go"), []byte(testSource), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := extractSurface(root, "sample")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"Additional", "Exported", "Public", "Public.Exported", "Top"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("surface = %v, want %v", got, want)
	}
}

func TestLoadFixtureReadsDocumentedNotBuilt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixture.json")
	data := []byte(`{"package":"sample","symbols":["Present"],"documented-not-built":["Translator"]}`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	packageName, symbols, gaps, err := loadFixture(path)
	if err != nil {
		t.Fatal(err)
	}
	if packageName != "sample" || !reflect.DeepEqual(symbols, []string{"Present"}) || !reflect.DeepEqual(gaps, []string{"Translator"}) {
		t.Fatalf("fixture = %q, %v, %v; want sample, [Present], [Translator]", packageName, symbols, gaps)
	}
}
func TestSurfaceDifferencesReportFixtureOnlySymbols(t *testing.T) {
	missing, stale, closed := surfaceDifferences(
		[]string{"Present", "New"},
		[]string{"Present", "Stale"},
		[]string{"Gap"},
	)
	if !reflect.DeepEqual(missing, []string{"New"}) {
		t.Fatalf("missing = %v, want [New]", missing)
	}
	if !reflect.DeepEqual(stale, []string{"Stale"}) {
		t.Fatalf("stale = %v, want [Stale]", stale)
	}
	if len(closed) != 0 {
		t.Fatalf("closed = %v, want []", closed)
	}
}
func TestSurfaceDifferencesAllowDocumentedNotBuiltSymbols(t *testing.T) {
	missing, stale, closed := surfaceDifferences(
		[]string{"Present"},
		[]string{"Present"},
		[]string{"Translator"},
	)
	if len(missing) != 0 || len(stale) != 0 || len(closed) != 0 {
		t.Fatalf("absent gap differences = %v, %v, %v; want none", missing, stale, closed)
	}
	missing, stale, closed = surfaceDifferences(
		[]string{"Present"},
		[]string{"Present", "Translator"},
		nil,
	)
	if len(missing) != 0 || !reflect.DeepEqual(stale, []string{"Translator"}) || len(closed) != 0 {
		t.Fatalf("stale gap differences = %v, %v, %v; want stale Translator", missing, stale, closed)
	}
	missing, stale, closed = surfaceDifferences(
		[]string{"Present", "Translator"},
		[]string{"Present"},
		[]string{"Translator"},
	)
	if len(missing) != 0 || len(stale) != 0 || !reflect.DeepEqual(closed, []string{"Translator"}) {
		t.Fatalf("closed gap differences = %v, %v, %v; want closed Translator", missing, stale, closed)
	}
}
