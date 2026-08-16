package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

type terminalError struct{ error }

func (e terminalError) RetryTerminal() bool { return true }
func (e terminalError) Unwrap() error       { return e.error }

type droppedError struct{ error }

func (droppedError) RetryDropped() bool { return true }

type delayedError struct {
	error
	delay time.Duration
}

func (e delayedError) RetryDelay() (time.Duration, bool) { return e.delay, true }

type panicError struct{ error }

func (panicError) RetryPanic() bool { return true }

func TestClassifyOutcomes(t *testing.T) {
	base := errors.New("failure")
	tests := []struct {
		name  string
		err   error
		kind  Kind
		delay time.Duration
	}{
		{name: "nil", kind: Success},
		{name: "drop", err: droppedError{base}, kind: Drop},
		{name: "terminal", err: terminalError{base}, kind: Terminal},
		{name: "retry after", err: delayedError{error: base, delay: 3 * time.Second}, kind: RetryAfter, delay: 3 * time.Second},
		{name: "deadline", err: context.DeadlineExceeded, kind: DeadlineExceeded},
		{name: "panic", err: panicError{base}, kind: Panic},
		{name: "default retry", err: base, kind: Retry},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := Classify(test.err)
			if got.Kind != test.kind || got.Delay != test.delay {
				t.Fatalf("Classify() = %#v, want kind %v delay %s", got, test.kind, test.delay)
			}
		})
	}
}

type allClassificationsError struct{ error }

func (allClassificationsError) RetryTerminal() bool               { return true }
func (allClassificationsError) RetryDropped() bool                { return true }
func (allClassificationsError) RetryDelay() (time.Duration, bool) { return time.Hour, true }
func (allClassificationsError) RetryPanic() bool                  { return true }

func TestClassifyTerminalOutranksEveryOtherDisposition(t *testing.T) {
	if got := Classify(allClassificationsError{errors.New("failure")}); got.Kind != Terminal {
		t.Fatalf("Classify() kind = %v, want Terminal", got.Kind)
	}
}
