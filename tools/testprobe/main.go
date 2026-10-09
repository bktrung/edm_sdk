// Command testprobe reports which sampled tests still pass after a covered
// production function or method is replaced with a zero-value body.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"

	"golang.org/x/tools/go/ast/astutil"
)

const (
	resultRed       = "red"
	resultGreen     = "green"
	resultNoCompile = "nocompile"
	resultTimeout   = "timeout"
	resultSkip      = "skip"
	resultNoSubject = "nosubject"
	resultNotGreen  = "notgreen"
	resultNotRun    = "notrun"
)

// Config controls one probe run. Root is the module directory in which go test
// runs. A zero Timeout uses five minutes per subprocess, and zero PairCap and
// MaxSubjects use 1500 and 5 respectively.
type Config struct {
	Root        string
	SamplePath  string
	CacheDir    string
	Timeout     time.Duration
	PairCap     int
	MaxSubjects int
}

// Pair is one tab-separated probe result. Subject is file:func, or "-" for a
// baseline-only result or a test with no name-matching covered function.
type Pair struct {
	Package string
	Test    string
	Subject string
	Result  string
	Seconds float64
}

type sampleEntry struct {
	Package string
	Test    string
}

type functionInfo struct {
	File        string
	Name        string
	Display     string
	StartLine   int
	StartColumn int
	EndLine     int
	EndColumn   int
	Statements  int
}

type testPlan struct {
	Entry           sampleEntry
	BaselineSeconds float64
	Subjects        []functionInfo
}

type runResult struct {
	Result  string
	Seconds float64
	Output  string
}

type probe struct {
	config        Config
	modulePath    string
	logsDir       string
	importNames   map[string]string
	importExports map[string]map[string]bool
	runNumber     int
	panicReds     int
	timeoutReds   int
}

func main() {
	var config Config
	flag.StringVar(&config.Root, "root", "", "module root in which to run go test")
	flag.StringVar(&config.SamplePath, "sample", "tools/testprobe/sample.txt", "sample file containing package and test names")
	flag.StringVar(&config.CacheDir, "cache", ".cache/testprobe", "directory for raw subprocess output")
	flag.DurationVar(&config.Timeout, "timeout", 5*time.Minute, "maximum duration for one go test subprocess")
	flag.IntVar(&config.PairCap, "pair-cap", 1500, "uncapped pair count at which per-test subject selection starts")
	flag.IntVar(&config.MaxSubjects, "max-subjects", 5, "subjects retained per test after the pair cap")
	flag.Parse()

	if err := runCommand(config, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "testprobe: %v\n", err)
		os.Exit(1)
	}
}

func runCommand(config Config, out, progress io.Writer) error {
	if config.Root == "" {
		var err error
		config.Root, err = os.Getwd()
		if err != nil {
			return err
		}
	}
	absoluteRoot, err := filepath.Abs(config.Root)
	if err != nil {
		return err
	}
	config.Root = absoluteRoot
	if config.Timeout <= 0 {
		config.Timeout = 5 * time.Minute
	}
	if config.PairCap <= 0 {
		config.PairCap = 1500
	}
	if config.MaxSubjects <= 0 {
		config.MaxSubjects = 5
	}
	return run(config, out, progress)
}

func run(config Config, out, progress io.Writer) error {
	modulePath, err := readModulePath(config.Root)
	if err != nil {
		return fmt.Errorf("read module path: %w", err)
	}
	samplePath := config.SamplePath
	if !filepath.IsAbs(samplePath) {
		samplePath = filepath.Join(config.Root, samplePath)
	}
	cacheDir := config.CacheDir
	if !filepath.IsAbs(cacheDir) {
		cacheDir = filepath.Join(config.Root, cacheDir)
	}
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return fmt.Errorf("create cache directory: %w", err)
	}
	logsDir := filepath.Join(cacheDir, "logs")
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		return fmt.Errorf("create log directory: %w", err)
	}
	entries, err := readSample(samplePath)
	if err != nil {
		return err
	}
	fmt.Fprintf(progress, "sample size: %d\n", len(entries))
	for _, entry := range entries {
		if err := validatePackageDir(config.Root, entry.Package); err != nil {
			return err
		}
	}
	config.CacheDir = cacheDir

	p := probe{
		config:        config,
		modulePath:    modulePath,
		logsDir:       logsDir,
		importNames:   make(map[string]string),
		importExports: make(map[string]map[string]bool),
	}
	plans := make([]testPlan, 0, len(entries))
	rows := make([]Pair, 0, len(entries))
	uncappedPairs := 0
	for _, entry := range entries {
		profilePath := filepath.Join(cacheDir, fmt.Sprintf("coverage-%04d.out", p.runNumber+1))
		baseline, err := p.runTest(entry, "baseline", "", profilePath, true, config.Timeout)
		if err != nil {
			return err
		}
		if baseline.Result != resultGreen {
			row := Pair{Package: entry.Package, Test: entry.Test, Subject: "-", Result: baseline.Result, Seconds: baseline.Seconds}
			rows = append(rows, row)
			if err := writePair(out, row); err != nil {
				return err
			}
			continue
		}
		coverage, err := p.coveredFunctions(profilePath, baseline.Output)
		if err != nil {
			return fmt.Errorf("subjects for %s %s: %w", entry.Package, entry.Test, err)
		}
		subjects, err := p.selectSubjects(entry, coverage)
		if err != nil {
			return fmt.Errorf("subjects for %s %s: %w", entry.Package, entry.Test, err)
		}
		if err := os.Remove(profilePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove coverage profile: %w", err)
		}
		if len(subjects) == 0 {
			row := Pair{Package: entry.Package, Test: entry.Test, Subject: "-", Result: resultNoSubject, Seconds: baseline.Seconds}
			rows = append(rows, row)
			if err := writePair(out, row); err != nil {
				return err
			}
			continue
		}
		uncappedPairs += len(subjects)
		plans = append(plans, testPlan{Entry: entry, BaselineSeconds: baseline.Seconds, Subjects: subjects})
	}

	capped := uncappedPairs > config.PairCap
	if capped {
		for i := range plans {
			if len(plans[i].Subjects) > config.MaxSubjects {
				sort.SliceStable(plans[i].Subjects, func(a, b int) bool {
					left, right := plans[i].Subjects[a], plans[i].Subjects[b]
					if left.Statements != right.Statements {
						return left.Statements > right.Statements
					}
					return stableSubjectKey(config.Root, left) < stableSubjectKey(config.Root, right)
				})
				plans[i].Subjects = plans[i].Subjects[:config.MaxSubjects]
			}
		}
	}
	selectedPairs := 0
	for _, plan := range plans {
		selectedPairs += len(plan.Subjects)
	}
	fmt.Fprintf(progress, "pairs before probing: %d\n", uncappedPairs)
	fmt.Fprintf(progress, "pairs after cap: %d\n", selectedPairs)
	if capped {
		fmt.Fprintf(progress, "pair cap applied: at most %d subjects per test, ranked by covered statements\n", config.MaxSubjects)
	} else {
		fmt.Fprintln(progress, "pair cap applied: no")
	}

	for _, plan := range plans {
		for _, subject := range plan.Subjects {
			row, err := p.probePair(plan.Entry, plan.BaselineSeconds, subject)
			if err != nil {
				return err
			}
			rows = append(rows, row)
			if err := writePair(out, row); err != nil {
				return err
			}
		}
	}
	printSummary(progress, rows, p, len(entries), uncappedPairs, selectedPairs)
	return nil
}

func (p *probe) probePair(entry sampleEntry, baselineSeconds float64, subject functionInfo) (Pair, error) {
	mutantDir, err := os.MkdirTemp(p.config.CacheDir, "mutant-")
	if err != nil {
		return Pair{}, fmt.Errorf("create mutant directory: %w", err)
	}
	defer os.RemoveAll(mutantDir)

	source, err := p.mutateFile(subject.File, subject.StartLine, subject.Display)
	if err != nil {
		return Pair{}, fmt.Errorf("mutate %s: %w", subjectKey(subject), err)
	}
	mutantPath := filepath.Join(mutantDir, filepath.Base(subject.File))
	if err := os.WriteFile(mutantPath, source, 0o644); err != nil {
		return Pair{}, fmt.Errorf("write mutant: %w", err)
	}
	overlayPath := filepath.Join(mutantDir, "overlay.json")
	overlay := struct {
		Replace map[string]string `json:"Replace"`
	}{Replace: map[string]string{subject.File: mutantPath}}
	overlayData, err := json.MarshalIndent(overlay, "", "  ")
	if err != nil {
		return Pair{}, fmt.Errorf("encode overlay: %w", err)
	}
	if err := os.WriteFile(overlayPath, append(overlayData, '\n'), 0o644); err != nil {
		return Pair{}, fmt.Errorf("write overlay: %w", err)
	}

	result, err := p.runTest(entry, subjectKey(subject), overlayPath, "", false, pairTimeout(baselineSeconds))
	if err != nil {
		return Pair{}, err
	}
	if result.Result == resultRed && strings.Contains(result.Output, "panic:") {
		p.panicReds++
	}
	if result.Result == resultTimeout {
		p.timeoutReds++
	}
	return Pair{
		Package: entry.Package,
		Test:    entry.Test,
		Subject: fmt.Sprintf("%s:%s", relativeFile(p.config.Root, subject.File), subject.Display),
		Result:  result.Result,
		Seconds: result.Seconds,
	}, nil
}

func pairTimeout(baselineSeconds float64) time.Duration {
	timeout := 30 * time.Second
	scaled := time.Duration(math.Ceil(baselineSeconds * 10 * float64(time.Second)))
	if scaled > timeout {
		return scaled
	}
	return timeout
}

func (p *probe) runTest(entry sampleEntry, label, overlay, profile string, baseline bool, timeout time.Duration) (runResult, error) {
	p.runNumber++
	args := []string{"test", "-count=1", "-v", "-timeout", timeout.String()}
	if baseline {
		args = append(args, "-covermode=set", "-coverprofile", profile)
	}
	if overlay != "" {
		args = append(args, "-overlay", overlay)
	}
	args = append(args, "-run", "^"+entry.Test+"$", packagePattern(entry.Package))

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = p.config.Root
	cmd.Env = os.Environ()
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	started := time.Now()
	if err := cmd.Start(); err != nil {
		return runResult{}, err
	}
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cmd.Wait()
	}()
	timedOut := false
	var err error
	select {
	case err = <-waitDone:
	case <-ctx.Done():
		timedOut = true
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		err = <-waitDone
	}
	seconds := time.Since(started).Seconds()
	text := output.String()
	var contextErr error
	if timedOut {
		contextErr = ctx.Err()
	}
	result := classifyResult(entry.Test, text, err, baseline, contextErr)
	logName := fmt.Sprintf("%04d-%s-%s.log", p.runNumber, sanitize(label), sanitize(entry.Package+"-"+entry.Test))
	if err := os.WriteFile(filepath.Join(p.logsDir, logName), output.Bytes(), 0o644); err != nil {
		return runResult{}, fmt.Errorf("write raw output: %w", err)
	}
	return runResult{Result: result, Seconds: seconds, Output: text}, nil
}

func classifyResult(test, output string, err error, baseline bool, contextErr error) string {
	if contextErr != nil && errors.Is(contextErr, context.DeadlineExceeded) {
		return resultTimeout
	}
	if strings.Contains(output, "test timed out after") {
		return resultTimeout
	}
	if strings.Contains(output, "[build failed]") || strings.Contains(output, "setup failed") || strings.Contains(output, "go: updates to go.mod needed") {
		return resultNoCompile
	}
	if strings.Contains(output, "--- SKIP: "+test+" (") {
		return resultSkip
	}
	if strings.Contains(output, "--- PASS: "+test+" (") && err == nil {
		return resultGreen
	}
	if baseline {
		if strings.Contains(output, "--- FAIL: "+test+" (") || err != nil {
			return resultNotGreen
		}
		return resultNotRun
	}
	if strings.Contains(output, "--- FAIL: "+test+" (") || err != nil {
		return resultRed
	}
	return resultNotRun
}

func (p *probe) coveredFunctions(profilePath, output string) ([]functionInfo, error) {
	data, err := os.ReadFile(profilePath)
	if err != nil {
		return nil, fmt.Errorf("read coverprofile: %w; output: %s", err, oneLine(output))
	}
	blocks, err := parseCoverage(data, p.modulePath, p.config.Root)
	if err != nil {
		return nil, err
	}
	byFile := make(map[string][]coverageBlock)
	for _, block := range blocks {
		if block.Count > 0 {
			byFile[block.File] = append(byFile[block.File], block)
		}
	}
	var functions []functionInfo
	for file, fileBlocks := range byFile {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		decls, err := parseFunctions(file)
		if err != nil {
			return nil, err
		}
		for _, decl := range decls {
			for _, block := range fileBlocks {
				if coveredBy(block, decl) {
					decl.Statements += block.Statements
				}
			}
			if decl.Statements > 0 {
				functions = append(functions, decl)
			}
		}
	}
	return functions, nil
}

type coverageBlock struct {
	File        string
	StartLine   int
	StartColumn int
	EndLine     int
	EndColumn   int
	Statements  int
	Count       int
}

func parseCoverage(data []byte, modulePath, root string) ([]coverageBlock, error) {
	var blocks []coverageBlock
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" || strings.HasPrefix(line, "mode:") {
			continue
		}
		colon := strings.LastIndexByte(line, ':')
		if colon < 0 {
			return nil, fmt.Errorf("malformed coverage line %q", line)
		}
		profileFile := line[:colon]
		fields := strings.Fields(line[colon+1:])
		if len(fields) != 3 {
			return nil, fmt.Errorf("malformed coverage line %q", line)
		}
		positions := strings.Split(fields[0], ",")
		if len(positions) != 2 {
			return nil, fmt.Errorf("malformed coverage position %q", fields[0])
		}
		start := strings.Split(positions[0], ".")
		end := strings.Split(positions[1], ".")
		if len(start) != 2 || len(end) != 2 {
			return nil, fmt.Errorf("malformed coverage position %q", fields[0])
		}
		startLine, err := strconv.Atoi(start[0])
		if err != nil {
			return nil, fmt.Errorf("parse coverage line %q: %w", line, err)
		}
		startColumn, err := strconv.Atoi(start[1])
		if err != nil {
			return nil, fmt.Errorf("parse coverage column %q: %w", line, err)
		}
		endLine, err := strconv.Atoi(end[0])
		if err != nil {
			return nil, fmt.Errorf("parse coverage end line %q: %w", line, err)
		}
		endColumn, err := strconv.Atoi(end[1])
		if err != nil {
			return nil, fmt.Errorf("parse coverage end column %q: %w", line, err)
		}
		statements, err := strconv.Atoi(fields[1])
		if err != nil {
			return nil, fmt.Errorf("parse statement count %q: %w", line, err)
		}
		count, err := strconv.Atoi(fields[2])
		if err != nil {
			return nil, fmt.Errorf("parse execution count %q: %w", line, err)
		}
		file, err := coverageFilePath(profileFile, modulePath, root)
		if err != nil {
			return nil, fmt.Errorf("coverage file %q: %w", profileFile, err)
		}
		blocks = append(blocks, coverageBlock{
			File:        file,
			StartLine:   startLine,
			StartColumn: startColumn,
			EndLine:     endLine,
			EndColumn:   endColumn,
			Statements:  statements,
			Count:       count,
		})
	}
	return blocks, nil
}

func coverageFilePath(profileFile, modulePath, root string) (string, error) {
	file := profileFile
	if !filepath.IsAbs(file) {
		prefix := modulePath + "/"
		if !strings.HasPrefix(file, prefix) {
			return "", fmt.Errorf("coverage file %q is outside module %q", profileFile, modulePath)
		}
		file = filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(file, prefix)))
	}
	file = filepath.Clean(file)
	relative, err := filepath.Rel(root, file)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("coverage file %q escapes module root", profileFile)
	}
	return file, nil
}

func parseFunctions(file string) ([]functionInfo, error) {
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, nil, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("parse covered file %s: %w", file, err)
	}
	functions := make([]functionInfo, 0)
	for _, decl := range parsed.Decls {
		funcDecl, ok := decl.(*ast.FuncDecl)
		if !ok || funcDecl.Body == nil {
			continue
		}
		display := funcDecl.Name.Name
		if funcDecl.Recv != nil && len(funcDecl.Recv.List) > 0 {
			display = receiverName(funcDecl.Recv.List[0].Type) + "." + display
		}
		start := fset.Position(funcDecl.Pos())
		end := fset.Position(funcDecl.End())
		functions = append(functions, functionInfo{
			File:        file,
			Name:        funcDecl.Name.Name,
			Display:     display,
			StartLine:   start.Line,
			StartColumn: start.Column,
			EndLine:     end.Line,
			EndColumn:   end.Column,
		})
	}
	return functions, nil
}

func coveredBy(block coverageBlock, function functionInfo) bool {
	if block.StartLine < function.StartLine || block.StartLine > function.EndLine {
		return false
	}
	if block.StartLine == function.StartLine && block.StartColumn < function.StartColumn {
		return false
	}
	if block.StartLine == function.EndLine && block.StartColumn > function.EndColumn {
		return false
	}
	return true
}

func (p *probe) selectSubjects(entry sampleEntry, functions []functionInfo) ([]functionInfo, error) {
	packageNames, err := packageNames(p.config.Root, entry.Package)
	if err != nil {
		return nil, err
	}
	testWords := testNameWords(entry.Test, packageNames)
	subjects := make([]functionInfo, 0)
	for _, function := range functions {
		lead := functionLeadWord(function)
		if lead != "" && testWords[lead] {
			subjects = append(subjects, function)
		}
	}
	sort.Slice(subjects, func(i, j int) bool {
		return stableSubjectKey(p.config.Root, subjects[i]) < stableSubjectKey(p.config.Root, subjects[j])
	})
	return subjects, nil
}

func packageNames(root, packageDir string) ([]string, error) {
	dir := packageDir
	if dir == "." {
		dir = root
	} else if !filepath.IsAbs(dir) {
		dir = filepath.Join(root, dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, entry.Name()), nil, 0)
		if err != nil {
			return nil, err
		}
		name := strings.TrimSuffix(file.Name.Name, "_test")
		seen[name] = true
	}
	if len(seen) == 0 {
		return nil, fmt.Errorf("no Go package found in %s", dir)
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func (p *probe) mutateFile(file string, startLine int, display string) ([]byte, error) {
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, nil, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	var target *ast.FuncDecl
	for _, decl := range parsed.Decls {
		funcDecl, ok := decl.(*ast.FuncDecl)
		if !ok || funcDecl.Body == nil {
			continue
		}
		current := funcDecl.Name.Name
		if funcDecl.Recv != nil && len(funcDecl.Recv.List) > 0 {
			current = receiverName(funcDecl.Recv.List[0].Type) + "." + current
		}
		if fset.Position(funcDecl.Pos()).Line == startLine && current == display {
			target = funcDecl
			break
		}
	}
	if target == nil {
		return nil, fmt.Errorf("function %s at line %d not found", display, startLine)
	}
	target.Body = zeroBody(target)
	imports := append([]*ast.ImportSpec(nil), parsed.Imports...)
	for _, importSpec := range imports {
		path, err := strconv.Unquote(importSpec.Path.Value)
		if err != nil {
			return nil, fmt.Errorf("unquote import: %w", err)
		}
		if importSpec.Name != nil {
			switch importSpec.Name.Name {
			case "_":
				continue
			case ".":
				exports, err := p.importPackageExports(path)
				if err != nil {
					continue
				}
				if !usesDotImport(parsed, exports) {
					astutil.DeleteImport(fset, parsed, path)
				}
			default:
				if !usesImportAlias(parsed, importSpec.Name.Name) {
					astutil.DeleteNamedImport(fset, parsed, importSpec.Name.Name, path)
				}
			}
			continue
		}
		name, err := p.importPackageName(path)
		if err != nil {
			continue
		}
		if !usesTopName(parsed, name) {
			astutil.DeleteImport(fset, parsed, path)
		}
	}
	var buffer bytes.Buffer
	if err := printer.Fprint(&buffer, fset, parsed); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (p *probe) importPackageName(path string) (string, error) {
	if name, ok := p.importNames[path]; ok {
		return name, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.config.Timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "list", "-f={{.Name}}", path)
	cmd.Dir = p.config.Root
	cmd.Env = os.Environ()
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	name := strings.TrimSpace(string(output))
	if name == "" {
		return "", fmt.Errorf("go list returned no package name for %s", path)
	}
	p.importNames[path] = name
	return name, nil
}

func (p *probe) importPackageExports(path string) (map[string]bool, error) {
	if exports, ok := p.importExports[path]; ok {
		return exports, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.config.Timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "list", "-json", path)
	cmd.Dir = p.config.Root
	cmd.Env = os.Environ()
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	var listing struct {
		Dir      string
		GoFiles  []string
		CgoFiles []string
	}
	if err := json.Unmarshal(output, &listing); err != nil {
		return nil, err
	}
	exports := make(map[string]bool)
	fset := token.NewFileSet()
	files := append(append([]string{}, listing.GoFiles...), listing.CgoFiles...)
	for _, name := range files {
		parsed, err := parser.ParseFile(fset, filepath.Join(listing.Dir, name), nil, 0)
		if err != nil {
			return nil, err
		}
		for _, decl := range parsed.Decls {
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				if decl.Recv == nil && ast.IsExported(decl.Name.Name) {
					exports[decl.Name.Name] = true
				}
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					switch spec := spec.(type) {
					case *ast.TypeSpec:
						if ast.IsExported(spec.Name.Name) {
							exports[spec.Name.Name] = true
						}
					case *ast.ValueSpec:
						for _, name := range spec.Names {
							if ast.IsExported(name.Name) {
								exports[name.Name] = true
							}
						}
					}
				}
			}
		}
	}
	p.importExports[path] = exports
	return exports, nil
}

func usesDotImport(file *ast.File, exports map[string]bool) bool {
	selectorNames := make(map[*ast.Ident]bool)
	ast.Inspect(file, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok {
			selectorNames[selector.Sel] = true
		}
		return true
	})
	used := false
	ast.Inspect(file, func(node ast.Node) bool {
		if ident, ok := node.(*ast.Ident); ok && !selectorNames[ident] && ident.Obj == nil && exports[ident.Name] {
			used = true
			return false
		}
		return !used
	})
	return used
}

func usesTopName(file *ast.File, name string) bool {
	used := false
	ast.Inspect(file, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok {
			if ident, ok := selector.X.(*ast.Ident); ok && ident.Name == name && ident.Obj == nil {
				used = true
				return false
			}
		}
		return !used
	})
	return used
}

func usesImportAlias(file *ast.File, name string) bool {
	used := false
	ast.Inspect(file, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok {
			if ident, ok := selector.X.(*ast.Ident); ok && ident.Name == name && ident.Obj == nil {
				used = true
				return false
			}
		}
		return !used
	})
	return used
}

func zeroBody(decl *ast.FuncDecl) *ast.BlockStmt {
	results := decl.Type.Results
	if results == nil || len(results.List) == 0 {
		return &ast.BlockStmt{}
	}
	allNamed := true
	usedNames := make(map[string]bool)
	recordNames := func(fields *ast.FieldList) {
		if fields == nil {
			return
		}
		for _, field := range fields.List {
			for _, name := range field.Names {
				if name.Name != "_" {
					usedNames[name.Name] = true
				}
			}
		}
	}
	recordNames(decl.Type.TypeParams)
	if decl.Recv != nil {
		ast.Inspect(decl.Recv, func(node ast.Node) bool {
			if ident, ok := node.(*ast.Ident); ok && ident.Name != "_" {
				usedNames[ident.Name] = true
			}
			return true
		})
	}
	recordNames(results)
	for _, field := range results.List {
		if len(field.Names) == 0 {
			allNamed = false
			break
		}
		for _, name := range field.Names {
			if name.Name == "_" {
				allNamed = false
				break
			}
		}
		if !allNamed {
			break
		}
	}
	if allNamed {
		return &ast.BlockStmt{List: []ast.Stmt{&ast.ReturnStmt{}}}
	}
	statements := make([]ast.Stmt, 0, len(results.List)+1)
	values := make([]ast.Expr, 0, len(results.List))
	zeroValue := func(resultType ast.Expr) ast.Expr {
		var name string
		for i := 0; ; i++ {
			name = fmt.Sprintf("__testprobeZero%d", i)
			if !usedNames[name] {
				break
			}
		}
		usedNames[name] = true
		statements = append(statements, &ast.DeclStmt{Decl: &ast.GenDecl{
			Tok: token.VAR,
			Specs: []ast.Spec{&ast.ValueSpec{
				Names: []*ast.Ident{ast.NewIdent(name)},
				Type:  resultType,
			}},
		}})
		return ast.NewIdent(name)
	}
	for _, field := range results.List {
		if len(field.Names) > 0 {
			for _, name := range field.Names {
				if name.Name == "_" {
					values = append(values, zeroValue(field.Type))
				} else {
					values = append(values, ast.NewIdent(name.Name))
				}
			}
			continue
		}
		values = append(values, zeroValue(field.Type))
	}
	statements = append(statements, &ast.ReturnStmt{Results: values})
	return &ast.BlockStmt{List: statements}
}

func receiverName(expr ast.Expr) string {
	for {
		switch value := expr.(type) {
		case *ast.StarExpr:
			expr = value.X
		case *ast.IndexExpr:
			expr = value.X
		case *ast.IndexListExpr:
			expr = value.X
		case *ast.Ident:
			return value.Name
		default:
			return "receiver"
		}
	}
}

func functionLeadWord(function functionInfo) string {
	name := function.Name
	if strings.EqualFold(name, "config") {
		name = strings.TrimSuffix(function.Display, "."+function.Name)
	}
	for _, word := range splitWords(name) {
		if len([]rune(word)) >= 4 {
			return strings.ToLower(word)
		}
	}
	return ""
}

func testNameWords(name string, packages []string) map[string]bool {
	words := splitWords(name)
	packagesSet := make(map[string]bool, len(packages))
	for _, packageName := range packages {
		packagesSet[strings.ToLower(packageName)] = true
	}
	result := make(map[string]bool)
	for i, word := range words {
		lower := strings.ToLower(word)
		if i == 0 && lower == "test" {
			continue
		}
		if packagesSet[lower] {
			continue
		}
		if len([]rune(word)) >= 4 {
			result[lower] = true
		}
	}
	return result
}

func splitWords(input string) []string {
	runes := []rune(input)
	words := make([]string, 0)
	current := make([]rune, 0)
	flush := func() {
		if len(current) > 0 {
			words = append(words, string(current))
			current = current[:0]
		}
	}
	for i, r := range runes {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			flush()
			continue
		}
		if len(current) > 0 && unicode.IsUpper(r) {
			previous := current[len(current)-1]
			if unicode.IsLower(previous) || unicode.IsDigit(previous) || (unicode.IsUpper(previous) && i+1 < len(runes) && unicode.IsLower(runes[i+1])) {
				flush()
			}
		}
		current = append(current, r)
	}
	flush()
	return words
}

func readSample(path string) ([]sampleEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read sample %s: %w", path, err)
	}
	seen := map[string]bool{}
	entries := make([]sampleEntry, 0)
	for lineNumber, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("sample line %d must contain package and test, got %q", lineNumber+1, line)
		}
		packageDir := filepath.Clean(fields[0])
		if packageDir == "" {
			packageDir = "."
		}
		entry := sampleEntry{Package: packageDir, Test: fields[1]}
		key := entry.Package + "\x00" + entry.Test
		if seen[key] {
			continue
		}
		seen[key] = true
		entries = append(entries, entry)
	}
	return entries, nil
}

func validatePackageDir(root, packageDir string) error {
	if filepath.IsAbs(packageDir) {
		return fmt.Errorf("sample package %q must be relative to module root", packageDir)
	}
	candidate := filepath.Join(root, filepath.Clean(packageDir))
	rootReal, err := filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("resolve module root: %w", err)
	}
	candidateReal, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return fmt.Errorf("resolve sample package %q: %w", packageDir, err)
	}
	relative, err := filepath.Rel(rootReal, candidateReal)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("sample package %q escapes module root", packageDir)
	}
	info, err := os.Stat(candidateReal)
	if err != nil {
		return fmt.Errorf("stat sample package %q: %w", packageDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("sample package %q is not a directory", packageDir)
	}
	return nil
}

func readModulePath(root string) (string, error) {
	data, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "module" {
			return fields[1], nil
		}
	}
	return "", fmt.Errorf("module directive not found in %s", filepath.Join(root, "go.mod"))
}

func packagePattern(packageDir string) string {
	if packageDir == "." {
		return "."
	}
	return "./" + filepath.ToSlash(packageDir)
}

func subjectKey(function functionInfo) string {
	return relativeFile("", function.File) + ":" + function.Display
}

func stableSubjectKey(root string, function functionInfo) string {
	return relativeFile(root, function.File) + ":" + function.Display
}

func relativeFile(root, file string) string {
	if root == "" {
		return filepath.ToSlash(file)
	}
	rel, err := filepath.Rel(root, file)
	if err != nil {
		return filepath.ToSlash(file)
	}
	return filepath.ToSlash(rel)
}

func oneLine(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func sanitize(value string) string {
	var builder strings.Builder
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.' || r == '-' {
			builder.WriteRune(r)
		} else {
			builder.WriteByte('_')
		}
	}
	return builder.String()
}

func writePair(out io.Writer, row Pair) error {
	_, err := fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%.3f\n", row.Package, row.Test, row.Subject, row.Result, row.Seconds)
	return err
}

func printSummary(out io.Writer, rows []Pair, p probe, sampleSize, uncappedPairs, selectedPairs int) {
	byResult := make(map[string]int)
	byPackage := make(map[string]map[string]int)
	for _, row := range rows {
		byResult[row.Result]++
		if byPackage[row.Package] == nil {
			byPackage[row.Package] = make(map[string]int)
		}
		byPackage[row.Package][row.Result]++
	}
	fmt.Fprintln(out, "summary by result:")
	for _, result := range []string{resultRed, resultGreen, resultNoCompile, resultTimeout, resultSkip, resultNoSubject, resultNotGreen, resultNotRun} {
		if count := byResult[result]; count > 0 {
			fmt.Fprintf(out, "  %s\t%d\n", result, count)
		}
	}
	fmt.Fprintln(out, "summary by package:")
	packages := make([]string, 0, len(byPackage))
	for packageName := range byPackage {
		packages = append(packages, packageName)
	}
	sort.Strings(packages)
	for _, packageName := range packages {
		counts := byPackage[packageName]
		parts := make([]string, 0, len(counts))
		for _, result := range []string{resultRed, resultGreen, resultNoCompile, resultTimeout, resultSkip, resultNoSubject, resultNotGreen, resultNotRun} {
			if count := counts[result]; count > 0 {
				parts = append(parts, fmt.Sprintf("%s=%d", result, count))
			}
		}
		fmt.Fprintf(out, "  %s\t%s\n", packageName, strings.Join(parts, ","))
	}
	fmt.Fprintf(out, "sample=%d pairs-before=%d pairs-after=%d panic-red=%d timeout-red=%d\n", sampleSize, uncappedPairs, selectedPairs, p.panicReds, p.timeoutReds)
}
