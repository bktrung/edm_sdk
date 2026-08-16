package f1

import (
	"errors"
	"time"
)

// classifiedError carries one handler outcome through wrapped error chains.
type classifiedError struct {
	err        error
	terminal   bool
	dropped    bool
	retryDelay time.Duration
	hasDelay   bool
}

func (e *classifiedError) Error() string {
	return e.err.Error()
}

func (e *classifiedError) Unwrap() error {
	return e.err
}

// Terminal marks err as permanently failed. A terminal error bypasses retries.
func Terminal(err error) error {
	if err == nil {
		return nil
	}
	return &classifiedError{err: err, terminal: true}
}

// RetryAfter marks err as retryable with an explicit delay for this attempt.
func RetryAfter(err error, delay time.Duration) error {
	if err == nil {
		return nil
	}
	return &classifiedError{err: err, retryDelay: delay, hasDelay: true}
}

// Drop marks err as handled and prevents another delivery attempt.
func Drop(err error) error {
	if err == nil {
		return nil
	}
	return &classifiedError{err: err, dropped: true}
}

// IsTerminal reports whether err or an error it wraps is terminal.
func IsTerminal(err error) bool {
	for current := err; current != nil; {
		var classified *classifiedError
		if !errors.As(current, &classified) {
			return false
		}
		if classified.terminal {
			return true
		}
		current = classified.err
	}
	return false
}

// IsDropped reports whether err or an error it wraps is dropped.
func IsDropped(err error) bool {
	var classified *classifiedError
	return errors.As(err, &classified) && classified.dropped
}

// RetryDelay returns an explicit retry delay carried by err.
func RetryDelay(err error) (time.Duration, bool) {
	var classified *classifiedError
	if !errors.As(err, &classified) || !classified.hasDelay {
		return 0, false
	}
	return classified.retryDelay, true
}
