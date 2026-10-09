package main

import (
	"bytes"
	"errors"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func TestWritePairReportsWriterError(t *testing.T) {
	if err := writePair(failingWriter{}, Pair{}); err == nil {
		t.Fatal("writePair returned nil for a failing writer")
	}
}

func TestFixtureProbeClassifiesRedGreenAndNoSubject(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	sample := filepath.Join(dir, "sample.txt")
	contents := strings.Join([]string{
		"testdata/fixture TestDoublingReturnsTwiceTheInput",
		"testdata/fixture TestFormattingDoesNotPanic",
		"testdata/fixture TestUnrelatedCheck",
		"",
	}, "\n")
	if err := os.WriteFile(sample, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	config := Config{
		Root:       root,
		SamplePath: sample,
		CacheDir:   filepath.Join(dir, "cache"),
	}
	if err := runCommand(config, &output, io.Discard); err != nil {
		t.Fatal(err)
	}

	rows := make(map[string]Pair)
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) != 5 {
			t.Fatalf("output line = %q, want five tab-separated fields", line)
		}
		rows[fields[1]] = Pair{Package: fields[0], Test: fields[1], Subject: fields[2], Result: fields[3]}
	}

	red := rows["TestDoublingReturnsTwiceTheInput"]
	if red.Result != resultRed || red.Subject != "testdata/fixture/fixture.go:Doubling" {
		t.Fatalf("defending test = %#v, want red for Doubling", red)
	}
	green := rows["TestFormattingDoesNotPanic"]
	if green.Result != resultGreen || green.Subject != "testdata/fixture/fixture.go:Formatting" {
		t.Fatalf("non-defending test = %#v, want green for Formatting", green)
	}
	noSubject := rows["TestUnrelatedCheck"]
	if noSubject.Result != resultNoSubject || noSubject.Subject != "-" {
		t.Fatalf("unrelated test = %#v, want nosubject", noSubject)
	}
}

func TestSelectSubjectsUsesLeadingWordsAndMethodReceivers(t *testing.T) {
	root := t.TempDir()
	source := []byte("package f1\n")
	if err := os.WriteFile(filepath.Join(root, "fixture.go"), source, 0o644); err != nil {
		t.Fatal(err)
	}

	p := &probe{config: Config{Root: root}}
	subjects, err := p.selectSubjects(sampleEntry{
		Package: ".",
		Test:    "TestLoadConfigRejectsUnknownBrokerOption",
	}, []functionInfo{
		{Name: "LoadConfig", Display: "LoadConfig"},
		{Name: "rawFromConfig", Display: "rawFromConfig"},
		{Name: "config", Display: "rawTopology.config"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(subjects) != 1 || subjects[0].Display != "LoadConfig" {
		t.Fatalf("subjects = %#v, want only LoadConfig", subjects)
	}
}

func TestMutateFileRemovesUnusedNamedImport(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fixture.go")
	source := `package fixture

import amqp "example/amqp"

func target() error {
	return amqp.Err
}
`
	if err := os.WriteFile(path, []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}

	mutated, err := (&probe{}).mutateFile(path, 5, "target")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(mutated), "example/amqp") {
		t.Fatalf("mutated source retained unused named import:\n%s", mutated)
	}
	if _, err := parser.ParseFile(token.NewFileSet(), path, mutated, parser.AllErrors); err != nil {
		t.Fatalf("mutated source does not parse: %v", err)
	}
}
