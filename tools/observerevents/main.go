// Command observerevents renders the public observer reference from observer.go.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
)

const generatedHeader = `# Observer event reference

This page is generated from [` + "`observer.go`" + `](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/observer.go). Run ` + "`make check-observer-events`" + ` to verify it. Edit ` + "`observer.go`" + `; do not edit the generated tables.

<!-- BEGIN GENERATED CONTENT -->
`

const generatedFooter = `<!-- END GENERATED CONTENT -->
`

var enumTypes = []string{
	"ObserverKind",
	"ObserverOutcome",
	"ErrorClass",
	"PublishRoute",
	"SettleOperation",
	"EnqueuedAtSource",
}

var eventTypes = []string{
	"StartEvent",
	"FinishEvent",
	"PointEvent",
	"Token",
	"DrainCounts",
}

type enumConstant struct {
	name        string
	value       string
	description string
	shape       string
}

type enumReference struct {
	name      string
	constants []enumConstant
}

type fieldReference struct {
	name        string
	typeText    string
	description string
}

type eventReference struct {
	name   string
	fields []fieldReference
}

func main() {
	source := flag.String("source", "observer.go", "path to observer.go")
	output := flag.String("output", "", "path for generated markdown; stdout when empty")
	flag.Parse()

	data, err := generate(*source)
	if err != nil {
		fail("generate observer reference: %v", err)
	}
	if *output == "" {
		_, _ = os.Stdout.Write(data)
		return
	}
	if err := os.WriteFile(*output, data, 0o644); err != nil {
		fail("write %s: %v", *output, err)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "observerevents: "+format+"\n", args...)
	os.Exit(1)
}

func generate(path string) ([]byte, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	enums, err := collectEnums(fset, file)
	if err != nil {
		return nil, err
	}
	events, err := collectEvents(fset, file)
	if err != nil {
		return nil, err
	}

	var out bytes.Buffer
	out.WriteString(generatedHeader)
	for _, enum := range enums {
		writeEnum(&out, enum)
	}
	for _, event := range events {
		writeEvent(&out, event)
	}
	out.WriteString(generatedFooter)
	return out.Bytes(), nil
}

func collectEnums(fset *token.FileSet, file *ast.File) ([]enumReference, error) {
	byType := make(map[string][]enumConstant, len(enumTypes))
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		inheritedType := ""
		for _, spec := range gen.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			typeName := inheritedType
			if valueSpec.Type != nil {
				typeName = exprName(valueSpec.Type)
				inheritedType = typeName
			}
			if !isEnumType(typeName) {
				continue
			}
			description := commentText(valueSpec.Doc)
			if description == "" {
				description = commentText(gen.Doc)
			}
			for index, name := range valueSpec.Names {
				if index >= len(valueSpec.Values) {
					return nil, fmt.Errorf("constant %s has no value", name.Name)
				}
				value, err := stringLiteral(fset, valueSpec.Values[index])
				if err != nil {
					return nil, fmt.Errorf("constant %s: %w", name.Name, err)
				}
				constant := enumConstant{
					name:        name.Name,
					value:       value,
					description: description,
				}
				if typeName == "ObserverKind" {
					constant.shape = observerShape(description)
				}
				byType[typeName] = append(byType[typeName], constant)
			}
		}
	}
	result := make([]enumReference, 0, len(enumTypes))
	for _, typeName := range enumTypes {
		constants := byType[typeName]
		if len(constants) == 0 {
			return nil, fmt.Errorf("no constants found for %s", typeName)
		}
		result = append(result, enumReference{name: typeName, constants: constants})
	}
	return result, nil
}

func collectEvents(fset *token.FileSet, file *ast.File) ([]eventReference, error) {
	byName := make(map[string]eventReference, len(eventTypes))
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}
		for _, spec := range gen.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || !isEventType(typeSpec.Name.Name) {
				continue
			}
			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				return nil, fmt.Errorf("%s is not a struct", typeSpec.Name.Name)
			}
			event := eventReference{name: typeSpec.Name.Name}
			for _, field := range structType.Fields.List {
				if len(field.Names) == 0 {
					continue
				}
				description := commentText(field.Doc)
				if description == "" {
					description = commentText(field.Comment)
				}
				typeText, err := nodeText(fset, field.Type)
				if err != nil {
					return nil, fmt.Errorf("field in %s: %w", typeSpec.Name.Name, err)
				}
				for _, name := range field.Names {
					event.fields = append(event.fields, fieldReference{
						name:        name.Name,
						typeText:    typeText,
						description: description,
					})
				}
			}
			byName[event.name] = event
		}
	}

	result := make([]eventReference, 0, len(eventTypes))
	for _, typeName := range eventTypes {
		event, ok := byName[typeName]
		if !ok || len(event.fields) == 0 {
			return nil, fmt.Errorf("no fields found for %s", typeName)
		}
		result = append(result, event)
	}
	return result, nil
}

func writeEnum(out *bytes.Buffer, enum enumReference) {
	fmt.Fprintf(out, "## %s\n\n", enum.name)
	if enum.name == "ObserverKind" {
		out.WriteString("| Constant | Value | Shape | Description |\n| --- | --- | --- | --- |\n")
		for _, constant := range enum.constants {
			fmt.Fprintf(out, "| `%s` | `%s` | %s | %s |\n", constant.name, escapeCell(constant.value), constant.shape, escapeCell(constant.description))
		}
	} else {
		out.WriteString("| Constant | Value | Description |\n| --- | --- | --- |\n")
		for _, constant := range enum.constants {
			fmt.Fprintf(out, "| `%s` | `%s` | %s |\n", constant.name, escapeCell(constant.value), escapeCell(constant.description))
		}
	}
	out.WriteByte('\n')
}

func writeEvent(out *bytes.Buffer, event eventReference) {
	fmt.Fprintf(out, "## %s fields\n\n", event.name)
	out.WriteString("| Field | Type | Description |\n| --- | --- | --- |\n")
	for _, field := range event.fields {
		fmt.Fprintf(out, "| `%s` | `%s` | %s |\n", field.name, escapeCell(field.typeText), escapeCell(field.description))
	}
	out.WriteByte('\n')
}

func isEnumType(name string) bool {
	for _, typeName := range enumTypes {
		if name == typeName {
			return true
		}
	}
	return false
}

func isEventType(name string) bool {
	for _, typeName := range eventTypes {
		if name == typeName {
			return true
		}
	}
	return false
}

func exprName(expr ast.Expr) string {
	ident, ok := expr.(*ast.Ident)
	if !ok {
		return ""
	}
	return ident.Name
}

func stringLiteral(fset *token.FileSet, expr ast.Expr) (string, error) {
	text, err := nodeText(fset, expr)
	if err != nil {
		return "", err
	}
	value, err := strconv.Unquote(text)
	if err != nil {
		return "", fmt.Errorf("expected string literal, got %s: %w", text, err)
	}
	return value, nil
}

func nodeText(fset *token.FileSet, node ast.Node) (string, error) {
	var buf bytes.Buffer
	if err := format.Node(&buf, fset, node); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func commentText(group *ast.CommentGroup) string {
	if group == nil {
		return ""
	}
	return strings.Join(strings.Fields(group.Text()), " ")
}

func observerShape(description string) string {
	lower := strings.ToLower(description)
	switch {
	case strings.Contains(lower, "paired kind"):
		return "paired"
	case strings.Contains(lower, "point kind"):
		return "point"
	default:
		return ""
	}
}

func escapeCell(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "|", "\\|"), "\n", " ")
}
