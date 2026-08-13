package retry

import (
	"context"
	"errors"
	"time"
)

// Kind identifies the disposition selected for a handler result.
type Kind uint8

const (
	// Success means the handler completed without an error.
	Success Kind = iota
	// Drop means the message is acknowledged without a successor copy.
	Drop
	// Terminal means the message is dead-lettered without another attempt.
	Terminal
	// Unavailable means the message is deferred without advancing its attempt.
	Unavailable
	// RetryAfter means the message is retried with an explicit delay.
	RetryAfter
	// DeadlineExceeded means the handler timed out and should be retried.
	DeadlineExceeded
	// Panic means handler execution panicked and the message is quarantined.
	Panic
	// Retry means an unclassified error receives the safe retry disposition.
	Retry
)

// Outcome is the broker-independent result of classifying an error.
type Outcome struct {
	Kind  Kind
	Delay time.Duration
	Err   error
}

type terminalMarker interface {
	RetryTerminal() bool
}
type droppedMarker interface {
	RetryDropped() bool
}
type unavailableMarker interface {
	RetryUnavailable() bool
}
type delayMarker interface {
	RetryDelay() (time.Duration, bool)
}
type panicMarker interface {
	RetryPanic() bool
}

// Classify maps an error to exactly one retry disposition. Terminal has
// precedence over unavailable when both markers occur in one error chain.
func Classify(err error) Outcome {
	if err == nil {
		return Outcome{Kind: Success}
	}
	var marker terminalMarker
	if errors.As(err, &marker) && marker.RetryTerminal() {
		return Outcome{Kind: Terminal, Err: err}
	}
	var dropped droppedMarker
	if errors.As(err, &dropped) && dropped.RetryDropped() {
		return Outcome{Kind: Drop, Err: err}
	}
	var unavailable unavailableMarker
	if errors.As(err, &unavailable) && unavailable.RetryUnavailable() {
		return Outcome{Kind: Unavailable, Err: err}
	}
	var delay delayMarker
	if errors.As(err, &delay) {
		if value, ok := delay.RetryDelay(); ok {
			if value < 0 {
				value = 0
			}
			return Outcome{Kind: RetryAfter, Delay: value, Err: err}
		}
	}
	var panicErr panicMarker
	if errors.As(err, &panicErr) && panicErr.RetryPanic() {
		return Outcome{Kind: Panic, Err: err}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return Outcome{Kind: DeadlineExceeded, Err: err}
	}
	return Outcome{Kind: Retry, Err: err}
}
