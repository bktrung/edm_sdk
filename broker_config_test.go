package f1

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// driverOptionsPage is the document the accepted key lists must agree with.
const driverOptionsPage = "docs/user-guide/driver-options.md"

// documentedOptionKey matches the key cell of a row in that page's tables. The
// page writes a key the way an operator writes it, under the broker section it
// belongs to.
var documentedOptionKey = regexp.MustCompile(`^broker\.(kafka|rabbitmq)\.([A-Za-z]+)$`)

// acceptedOptionKeys returns the keys a whitelist function accepts, read from
// the switch that decides it. The switch is the list: a key it names is
// accepted even when no other file in the repository spells it out, so the
// guard cannot be written against the string literals that appear elsewhere in
// the tree.
func acceptedOptionKeys(t *testing.T, function string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "broker_config.go", nil, 0)
	if err != nil {
		t.Fatalf("parse broker_config.go: %v", err)
	}
	var keys []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != function {
			continue
		}
		ast.Inspect(fn, func(node ast.Node) bool {
			clause, ok := node.(*ast.CaseClause)
			if !ok {
				return true
			}
			for _, expr := range clause.List {
				literal, ok := expr.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				key, err := strconv.Unquote(literal.Value)
				if err != nil {
					t.Fatalf("unquote a %s case %s: %v", function, literal.Value, err)
				}
				keys = append(keys, key)
			}
			return true
		})
	}
	if len(keys) == 0 {
		t.Fatalf("%s names no key cases, so the guard is not reading the switch", function)
	}
	return keys
}

// documentedOptionKeys returns the keys the page documents, grouped by driver
// and in the order the page lists them.
func documentedOptionKeys(t *testing.T) map[string][]string {
	t.Helper()
	content, err := os.ReadFile(driverOptionsPage)
	if err != nil {
		t.Fatalf("read %s: %v", driverOptionsPage, err)
	}
	documented := make(map[string][]string)
	seen := make(map[string]bool)
	for line := range strings.SplitSeq(string(content), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "|") {
			continue
		}
		cell, _, _ := strings.Cut(line[1:], "|")
		cell = strings.TrimSpace(strings.ReplaceAll(cell, "`", ""))
		match := documentedOptionKey.FindStringSubmatch(cell)
		if match == nil || seen[cell] {
			continue
		}
		seen[cell] = true
		documented[match[1]] = append(documented[match[1]], match[2])
	}
	return documented
}

// TestDriverOptionsDocumented fails when the page and the whitelist disagree in
// either direction: a key the switch accepts that the page omits, or a key the
// page lists that the switch refuses. Each direction matters on its own, since
// a documented key the driver rejects fails at startup and an accepted key the
// page omits is discoverable only by guessing it.
func TestDriverOptionsDocumented(t *testing.T) {
	documented := documentedOptionKeys(t)
	for _, driver := range []struct {
		name     string
		function string
	}{
		{name: "kafka", function: "validKafkaOption"},
		{name: "rabbitmq", function: "validRabbitMQOption"},
	} {
		t.Run(driver.name, func(t *testing.T) {
			accepted := acceptedOptionKeys(t, driver.function)
			for _, key := range accepted {
				if !slices.Contains(documented[driver.name], key) {
					t.Errorf("broker.%s.%s is accepted by %s and is absent from %s", driver.name, key, driver.function, driverOptionsPage)
				}
			}
			for _, key := range documented[driver.name] {
				if !slices.Contains(accepted, key) {
					t.Errorf("broker.%s.%s is documented in %s and is refused by %s", driver.name, key, driverOptionsPage, driver.function)
				}
			}
		})
	}
}
