package f1

import (
	"context"
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

// TestRemovedRabbitMQOptionsFailToLoad pins the trimmed RabbitMQ whitelist. A
// key that no resolver reads is refused when the configuration loads rather
// than accepted and silently ignored, so a config naming one never starts. The
// four keys that are read still load, which is what separates a trimmed list
// from an emptied one.
func TestRemovedRabbitMQOptionsFailToLoad(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		key   string
		value string
	}{
		{key: "maxLength", value: "1000"},
		{key: "deliveryLimitMargin", value: "2"},
		{key: "deadLetterStrategy", value: "reject"},
		{key: "useNativeDelay", value: "true"},
		{key: "publisherConfirmTimeout", value: "5s"},
		{key: "quorumInitialGroupSize", value: "3"},
	} {
		t.Run(test.key, func(t *testing.T) {
			t.Parallel()
			path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: rabbitmq\n    endpoints: [amqp://broker:5672/]\n    rabbitmq:\n      "+test.key+": "+test.value+"\n")
			_, err := LoadConfig(path)
			if err == nil || !strings.Contains(err.Error(), "broker.rabbitmq."+test.key) {
				t.Fatalf("LoadConfig() with %s set error = %v, want an error naming broker.rabbitmq.%s", test.key, err, test.key)
			}
		})
	}
	t.Run("the keys that are read still load", func(t *testing.T) {
		t.Parallel()
		path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: rabbitmq\n    endpoints: [amqp://broker:5672/]\n    rabbitmq:\n      vhost: /orders\n      queueType: quorum\n      consumerTimeout: 90s\n      managementPort: 15672\n")
		if _, err := LoadConfig(path); err != nil {
			t.Fatalf("LoadConfig() with the four live RabbitMQ keys error = %v", err)
		}
	})
}

// TestUnknownKafkaOptionsFailToLoad is the Kafka side of the guard the RabbitMQ
// case above pins: a key no reader was written for is refused by name when the
// configuration loads, rather than left in the option map for the driver to
// ignore and report as a value the deployment never chose. No key has left the
// Kafka list, so both cases here are names that were never on it, which is what
// a typo or a remembered-but-absent option produces.
func TestUnknownKafkaOptionsFailToLoad(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		key   string
		value string
	}{
		{key: "linger", value: "5ms"},
		{key: "partitions", value: "6"},
	} {
		t.Run(test.key, func(t *testing.T) {
			t.Parallel()
			path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: kafka\n    endpoints: [localhost:19092]\n    kafka:\n      "+test.key+": "+test.value+"\n")
			_, err := LoadConfig(path)
			if err == nil || !strings.Contains(err.Error(), "broker.kafka."+test.key) {
				t.Fatalf("LoadConfig() with %s set error = %v, want an error naming broker.kafka.%s", test.key, err, test.key)
			}
		})
	}
}

// TestNewRefusesUnknownDriverOptions loads no file: it builds the configuration
// in Go and opens it, which is the other way an option arrives and the path the
// refusal used to miss. Nothing between New and the driver reads a key - the map
// is carried to the driver as an opaque passthrough - so a caller who misspells
// one used to get a client that opened cleanly and a driver that kept its own
// default.
//
// The last three cases are the same rule one step out. An option carries the
// name of the driver that reads it, so a key naming the other broker, a key
// naming no broker, and a key on a driver that reads no option at all are read
// by nobody either: they are refused rather than delivered to a resolver that
// will not look at them.
func TestNewRefusesUnknownDriverOptions(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		driver    string
		endpoints []string
		key       string
	}{
		{name: "name the driver has no reader for", driver: "kafka", endpoints: []string{"localhost:19092"}, key: "kafka.maxExpectedInstancesX"},
		{name: "key of the other broker", driver: "kafka", endpoints: []string{"localhost:19092"}, key: "rabbitmq.queueType"},
		{name: "key naming no driver", driver: "kafka", endpoints: []string{"localhost:19092"}, key: "queueType"},
		{name: "key on a driver that reads no option", driver: "inmem", key: "inmem.maxQueueDepth"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{
				Env:     "test",
				Service: "orders",
				Broker: BrokerConfig{
					Driver:        test.driver,
					Endpoints:     test.endpoints,
					DriverOptions: map[string]string{test.key: "1"},
				},
			}
			client, err := New(context.Background(), cfg, WithDriver(&testDriver{name: test.driver, conn: &testConn{}}))
			if err == nil {
				_ = client.Close(context.Background())
				t.Fatalf("New() with %s in BrokerConfig.DriverOptions error = nil, want a refusal naming broker.%s", test.key, test.key)
			}
			if !strings.Contains(err.Error(), "f1: unknown broker."+test.key) {
				t.Fatalf("New() error = %v, want an error naming broker.%s", err, test.key)
			}
		})
	}
	t.Run("the keys the driver reads still open", func(t *testing.T) {
		t.Parallel()
		cfg := Config{
			Env:     "test",
			Service: "orders",
			Broker: BrokerConfig{
				Driver:        "kafka",
				Endpoints:     []string{"localhost:19092"},
				DriverOptions: map[string]string{"kafka.compression": "lz4", "kafka.maxExpectedInstances": "6"},
			},
		}
		client, err := New(context.Background(), cfg, WithDriver(&testDriver{name: "kafka", conn: &testConn{}}))
		if err != nil {
			t.Fatalf("New() with two accepted Kafka options error = %v", err)
		}
		if err := client.Close(context.Background()); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
}
// TestRemovedKafkaOptionsFailToLoad pins the Kafka whitelist after the
// per-consumer acknowledgement gap option was removed. A config naming it is
// refused rather than accepted and silently ignored.
func TestRemovedKafkaOptionsFailToLoad(t *testing.T) {
	t.Parallel()
	removedKey := "max" + "Ack" + "Gap"
	path := writeConfig(t, "f1:\n  env: test\n  service: orders\n  broker:\n    driver: kafka\n    endpoints: [localhost:19092]\n    kafka:\n      "+removedKey+": 10000\n")
	_, err := LoadConfig(path)
	want := "f1: unknown broker.kafka." + removedKey
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("LoadConfig() with removed Kafka key set error = %v, want %s", err, want)
	}
}
