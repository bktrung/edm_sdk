// Command apisurface is M1-05c's other half of F-P46's structural finding
// (BLK-10): mq-sdk-docs' tools/apispec can prove its own documents are
// self-consistent, but it never reads this repo's code, so it is
// structurally blind to a symbol that is public here and named in no
// document - seven of them, on a milestone whose exit is "the public API is
// frozen". This tool is the other direction: it lists package f1's actual
// exported surface and asserts it is a SUBSET of mq-sdk-docs' generated
// testdata/public-api.json. Subset, not equality - most of that fixture's
// symbols are for later milestones, and "declared but not yet implemented"
// is expected before freeze.
//
// Granularity matches mq-sdk-docs' extractSurface exactly: go doc level -
// types, top-level funcs, methods (as "Type.Method"), consts, vars. Struct
// fields are explicitly NOT covered here: envelope-attributes.json already
// covers the wire attributes at that level, and extending this tool to
// struct fields would just be a second, differently-shaped copy of that
// check.
package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
)

func main() {
	fixturePath := "testdata/public-api.json"
	for i, arg := range os.Args {
		if arg == "-fixture" && i+1 < len(os.Args) {
			fixturePath = os.Args[i+1]
		}
	}

	repoRoot, err := os.Getwd()
	if err != nil {
		fail("%v", err)
	}
	if _, err := os.Stat(filepath.Join(repoRoot, "go.mod")); err != nil {
		fail("run this from the repo root (expected go.mod at %s): %v", repoRoot, err)
	}

	actual, err := extractSurface(repoRoot, "f1")
	if err != nil {
		fail("extract package f1's surface: %v", err)
	}

	if !filepath.IsAbs(fixturePath) {
		fixturePath = filepath.Join(repoRoot, fixturePath)
	}
	declared, err := loadFixture(fixturePath)
	if err != nil {
		fail("load %s: %v", fixturePath, err)
	}

	known := make(map[string]bool, len(declared))
	for _, s := range declared {
		known[s] = true
	}

	var undeclared []string
	for _, s := range actual {
		if !known[s] {
			undeclared = append(undeclared, s)
		}
	}

	if len(undeclared) > 0 {
		fmt.Fprintln(os.Stderr, "apisurface: package f1 exports symbols mq-sdk-docs does not declare:")
		for _, s := range undeclared {
			fmt.Fprintf(os.Stderr, "  %s\n", s)
		}
		fmt.Fprintln(os.Stderr, "apisurface: document them in mq-sdk-docs (docs/03 or docs/05), regenerate public-api.json with `make check-api`, then re-run `make check-fixture` here.")
		os.Exit(1)
	}

	fmt.Printf("apisurface: package f1's %d exported symbols are all declared in %s\n", len(actual), fixturePath)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "apisurface: "+format+"\n", args...)
	os.Exit(1)
}

type publicAPIFixture struct {
	Symbols []string `json:"symbols"`
}

func loadFixture(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var fixture publicAPIFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		return nil, err
	}
	return fixture.Symbols, nil
}

// extractSurface lists every exported top-level name declared across pkg's
// *.go files directly under moduleDir (test files excluded), methods
// included as "Type.Method". This mirrors mq-sdk-docs' tools/apispec
// extractSurface node-for-node, so the two tools agree on what "exported
// surface" means.
func extractSurface(moduleDir, pkg string) ([]string, error) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, moduleDir, func(fi os.FileInfo) bool {
		return !isTestFile(fi.Name())
	}, 0)
	if err != nil {
		return nil, err
	}

	p, ok := pkgs[pkg]
	if !ok {
		return nil, fmt.Errorf("no package %q found under %s", pkg, moduleDir)
	}

	seen := map[string]bool{}
	add := func(name string) {
		if name != "" && ast.IsExported(name) {
			seen[name] = true
		}
	}
	for _, f := range p.Files {
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil || len(d.Recv.List) == 0 {
					add(d.Name.Name)
					continue
				}
				recv := d.Recv.List[0].Type
				if star, ok := recv.(*ast.StarExpr); ok {
					recv = star.X
				}
				if id, ok := recv.(*ast.Ident); ok && ast.IsExported(id.Name) {
					add(id.Name + "." + d.Name.Name)
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						add(s.Name.Name)
					case *ast.ValueSpec:
						for _, n := range s.Names {
							add(n.Name)
						}
					}
				}
			}
		}
	}

	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

func isTestFile(name string) bool {
	return len(name) > len("_test.go") && name[len(name)-len("_test.go"):] == "_test.go"
}
