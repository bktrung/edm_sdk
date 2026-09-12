package main

import (
	"bufio"
	"flag"
	"fmt"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/gcexportdata"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: apidiff-normalize INPUT OUTPUT")
	}
	flag.Parse()
	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(2)
	}
	if err := normalize(flag.Arg(0), flag.Arg(1)); err != nil {
		fmt.Fprintf(os.Stderr, "apidiff-normalize: %v\n", err)
		os.Exit(1)
	}
}

func normalize(input, output string) error {
	f, err := os.Open(input)
	if err != nil {
		return err
	}

	reader := bufio.NewReader(f)
	packagePath, err := reader.ReadString('\n')
	if err != nil {
		f.Close()
		return err
	}
	packagePath = strings.TrimSuffix(packagePath, "\n")

	files := token.NewFileSet()
	packages := map[string]*types.Package{}
	pkg, err := gcexportdata.Read(reader, files, packages, packagePath)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}

	goroot, err := resolveGOROOT()
	if err != nil {
		return err
	}

	out, err := os.Create(output)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(out, packagePath); err != nil {
		out.Close()
		return err
	}
	writeErr := gcexportdata.Write(out, canonicalFileSet(files, goroot), pkg)
	closeErr = out.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func canonicalFileSet(files *token.FileSet, goroot string) *token.FileSet {
	canonical := token.NewFileSet()
	files.Iterate(func(file *token.File) bool {
		canonical.AddFile(canonicalFileName(file.Name(), goroot), file.Base(), file.Size())
		return true
	})
	return canonical
}

func resolveGOROOT() (string, error) {
	goPath, err := exec.LookPath("go")
	if err != nil {
		return "", err
	}
	output, err := exec.Command(goPath, "env", "GOROOT").Output()
	if err != nil {
		return "", err
	}
	goroot := strings.TrimSpace(string(output))
	if goroot == "" {
		return "", fmt.Errorf("go env GOROOT returned empty root")
	}
	return goroot, nil
}

func canonicalFileName(name, goroot string) string {
	name = filepath.ToSlash(name)
	if relative, ok := rootedPathSuffix(name, filepath.ToSlash(goroot)); ok {
		return "$GOROOT/" + relative
	}
	if index := strings.Index(name, "/pkg/mod/"); index >= 0 {
		return "$GOMODCACHE/" + name[index+len("/pkg/mod/"):]
	}
	parts := strings.Split(name, "/")
	for i, part := range parts {
		if part == "internal" || part == "driver" || part == "codec" {
			return "$MODULE/" + strings.Join(parts[i:], "/")
		}
	}
	return "$MODULE/" + filepath.Base(name)
}

func rootedPathSuffix(name, root string) (string, bool) {
	if name == root {
		return "", true
	}
	prefix := strings.TrimSuffix(root, "/") + "/"
	if !strings.HasPrefix(name, prefix) {
		return "", false
	}
	return strings.TrimPrefix(name, prefix), true
}
