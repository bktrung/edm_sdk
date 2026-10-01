package f1

import (
	"errors"
	"slices"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/wire"
)

// classifiedError carries one handler outcome through wrapped error chains.
type classifiedError struct {
	err      error
	terminal bool
	dropped  bool
}

const (
	maxDeathDetailKeys  = 16
	maxDeathDetailBytes = 1 << 10
)

type detailsError struct {
	err       error
	details   map[string]string
	discarded []string
}
type detailsErrorCarrier interface {
	DeathDetails() map[string]string
	discardedDetails() []string
}

func (e *detailsError) discardedDetails() []string {
	return e.discarded
}

func (e *detailsError) Error() string {
	return e.err.Error()
}

func (e *detailsError) Unwrap() error {
	return e.err
}

func (e *detailsError) DeathDetails() map[string]string {
	return e.details
}

// WithDetails attaches diagnostic key/value pairs to err. When the message
// dies with reason terminal or max_attempts, the core writes each pair to the
// dead-letter copy as the header f1detail<key>. Details never affect routing,
// retry, settlement or ordering.
//
// Keys must be lowercase alphanumeric. An invalid key, or a set exceeding 16
// keys or 1 KiB encoded, is discarded and reported when the message is
// dead-lettered rather than failing the message. Returns nil for a nil err, so
// "return f1.WithDetails(doWork(), d)" is safe on the success path. Composes
// in either direction with Terminal and Drop.
func WithDetails(err error, details map[string]string) error {
	if err == nil {
		return nil
	}
	valid := make(map[string]string, len(details))
	encodedBytes := 0
	var discarded []string
	for key, value := range details {
		if !validDeathDetailKey(key) {
			discarded = append(discarded, key)
			continue
		}
		valid[key] = value
		encodedBytes += len(wire.DetailPrefix) + len(key) + len(value)
	}
	if len(valid) > maxDeathDetailKeys || encodedBytes > maxDeathDetailBytes {
		discarded = make([]string, 0, len(details))
		for key := range details {
			discarded = append(discarded, key)
		}
		return &detailsError{err: err, discarded: discarded}
	}
	if len(valid) == 0 && len(discarded) == 0 {
		return err
	}
	return &detailsError{err: err, details: valid, discarded: discarded}
}

func collectDeathDetails(err error) (map[string]string, []string) {
	if err == nil {
		return nil, nil
	}
	var details map[string]string
	var discarded []string
	var visit func(error)
	visit = func(current error) {
		if current == nil {
			return
		}
		if joined, ok := current.(interface{ Unwrap() []error }); ok {
			for _, child := range joined.Unwrap() {
				visit(child)
			}
		} else if wrapped, ok := current.(interface{ Unwrap() error }); ok {
			visit(wrapped.Unwrap())
		}
		if carrier, ok := current.(detailsErrorCarrier); ok {
			if len(carrier.DeathDetails()) > 0 {
				if details == nil {
					details = make(map[string]string)
				}
				for key, value := range carrier.DeathDetails() {
					details[key] = value
				}
			}
			discarded = append(discarded, carrier.discardedDetails()...)
			return
		}
		if carrier, ok := current.(interface {
			DeathDetails() map[string]string
		}); ok {
			for key, value := range carrier.DeathDetails() {
				if !validDeathDetailKey(key) {
					discarded = append(discarded, key)
					continue
				}
				if details == nil {
					details = make(map[string]string)
				}
				details[key] = value
			}
		}
	}
	visit(err)
	return details, discarded
}

func (e *classifiedError) Error() string {
	return e.err.Error()
}

func (e *classifiedError) Unwrap() error {
	return e.err
}

// Terminal marks err as permanently failed so the message bypasses retries.
// It returns nil when err is nil.
func Terminal(err error) error {
	if err == nil {
		return nil
	}
	return &classifiedError{err: err, terminal: true}
}

// Drop marks err as handled so the message is acknowledged without another
// delivery attempt. It returns nil when err is nil.
func Drop(err error) error {
	if err == nil {
		return nil
	}
	return &classifiedError{err: err, dropped: true}
}

// IsTerminal reports whether any node in err's error tree is terminal.
func IsTerminal(err error) bool {
	if err == nil {
		return false
	}
	var classified *classifiedError
	if errors.As(err, &classified) && classified.terminal {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		return slices.ContainsFunc(joined.Unwrap(), IsTerminal)
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return IsTerminal(wrapped.Unwrap())
	}
	return false
}

// IsDropped reports whether the first classified error in err's error tree is
// marked dropped.
func IsDropped(err error) bool {
	var classified *classifiedError
	return errors.As(err, &classified) && classified.dropped
}
