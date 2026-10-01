package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateObserverReferenceIsDeterministic(t *testing.T) {
	source := `package f1

type ObserverKind string
type ObserverOutcome string
type ErrorClass string
type PublishRoute string
type SettleOperation string
type EnqueuedAtSource string

const (
	// ObserverPublish is a paired kind.
	ObserverPublish ObserverKind = "publish"
	// ObserverPoint is a point kind.
	ObserverPoint ObserverKind = "point"
	// ObserverInherited is a point kind.
	ObserverInherited = "inherited"
)
const ObserverOutcomeOK ObserverOutcome = "ok"
const ErrorClassOther ErrorClass = "_OTHER"
const PublishRoutePrimary PublishRoute = "primary"
const SettleAck SettleOperation = "ack"
const EnqueuedAtUnknown EnqueuedAtSource = ""

type StartEvent struct { Kind ObserverKind }
type FinishEvent struct { Outcome ObserverOutcome }
type PointEvent struct { At string }
type Token struct { Handle uint64 }
type DrainCounts struct { Drained int }
`
	path := filepath.Join(t.TempDir(), "observer.go")
	if err := os.WriteFile(path, []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}

	first, err := generate(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := generate(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatal("generator output changed between identical runs")
	}
	output := string(first)
	for _, want := range []string{
		"| `ObserverPublish` | `publish` | paired | ObserverPublish is a paired kind. |",
		"| `ObserverPoint` | `point` | point | ObserverPoint is a point kind. |",
		"| `ObserverInherited` | `inherited` | point | ObserverInherited is a point kind. |",
		"| `EnqueuedAtUnknown` | `` |  |",
		"## StartEvent fields",
		"| `Handle` | `uint64` |  |",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("generated output does not contain %q:\n%s", want, output)
		}
	}
}
