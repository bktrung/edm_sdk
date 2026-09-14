// Package testlayout holds the guard that keeps the repository's integration build
// tag and its _integration_test.go file-name convention in agreement.
//
// A test file is an integration file exactly when it needs a process this
// repository did not start, whatever layer it otherwise belongs to. The build tag
// is what the toolchain acts on, and the suffix is what a reader sees; a file with
// one and not the other is wrong in a way no compiler will report, because the
// default build never compiles the constrained half of the tree.
package testlayout

import (
	"go/build/constraint"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// integrationSuffix is the file-name half of the convention.
const integrationSuffix = "_integration_test.go"

// TestIntegrationTagMatchesSuffix walks every test file in the module and asserts
// the biconditional: a file carries the integration constraint if and only if its
// name ends in _integration_test.go. It reads the files as text rather than
// importing them, because the compiler ignores the constrained half in the default
// build and this guard is the only always-on check that can see that half.
func TestIntegrationTagMatchesSuffix(t *testing.T) {
	root := moduleRoot(t)
	tree := os.DirFS(root)

	var testFiles, constrained int
	walkErr := fs.WalkDir(tree, ".", func(rel string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			// The walk starts at ".", whose name is not a directory name to judge;
			// the ignore rule applies to the entries below it.
			if rel != "." && toolchainIgnoresDir(entry.Name()) {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		testFiles++

		content, readErr := fs.ReadFile(tree, rel)
		if readErr != nil {
			return readErr
		}
		carries := carriesIntegrationTag(string(content))
		named := strings.HasSuffix(entry.Name(), integrationSuffix)

		switch {
		case carries && !named:
			t.Errorf("%s carries the integration build tag but is not named *_integration_test.go; a test file that needs a broker is constrained and renamed together", rel)
		case !carries && named:
			t.Errorf("%s is named *_integration_test.go but carries no integration build tag; the name would make the file read as broker-backed while the default build still compiles it", rel)
		}
		if carries {
			constrained++
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk test tree: %v", walkErr)
	}
	// A guard that walks the wrong tree passes for the wrong reason, so an empty
	// or unconstrained result is a failure rather than a quiet success.
	if testFiles == 0 || constrained == 0 {
		t.Fatalf("walked %d test files under %s and found %d with the integration tag; the walk did not reach the module", testFiles, root, constrained)
	}
}

// moduleRoot returns the module directory by walking up from the test's working
// directory, which the go command sets to this package's directory.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		} else if !os.IsNotExist(statErr) {
			t.Fatalf("stat go.mod in %s: %v", dir, statErr)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no go.mod above %s", dir)
		}
		dir = parent
	}
}

// toolchainIgnoresDir reports whether the go command ignores a directory by name.
// Mirroring that rule keeps this guard's idea of the test tree the same as the
// build's, so a fixture under testdata is not read as a test file.
func toolchainIgnoresDir(name string) bool {
	if strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") {
		return true
	}
	return name == "testdata" || name == "vendor"
}

// carriesIntegrationTag reports whether the file's build constraint selects it into
// the integration build. Only lines above the package clause are read: a
// //go:build line below it is a comment, not a constraint.
func carriesIntegrationTag(content string) bool {
	for line := range strings.Lines(content) {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "package ") {
			return false
		}
		if !strings.HasPrefix(trimmed, "//") {
			continue
		}
		expr, err := constraint.Parse(trimmed)
		if err != nil {
			continue
		}
		if requiresIntegration(expr) {
			return true
		}
	}
	return false
}

// requiresIntegration reports whether the constraint needs the integration tag, as
// opposed to ignoring it, forbidding it, or naming some other condition. It is
// asked twice, once with every tag available and once with every tag except
// integration, so a conjunction such as `integration && linux` is recognised as
// well as a bare `integration`.
func requiresIntegration(expr constraint.Expr) bool {
	builtWithEverything := expr.Eval(func(string) bool { return true })
	builtWithoutIntegration := expr.Eval(func(tag string) bool { return tag != "integration" })
	return builtWithEverything && !builtWithoutIntegration
}
