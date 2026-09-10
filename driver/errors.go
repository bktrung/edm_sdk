package driver

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by drivers for portable failure handling.
var (
	ErrUnsupported          = errors.New("f1/driver: unsupported operation")
	ErrAlreadySettled       = errors.New("f1/driver: message already settled")
	ErrDrainTimeout         = errors.New("f1/driver: drain did not complete in time")
	ErrResourcesOutstanding = errors.New("f1/driver: producers or consumers still open")
	ErrDestinationMissing   = errors.New("f1/driver: destination does not exist")
)

// Kind classifies an error for retry and health logic.
type Kind int

const (
	// KindTransient identifies a retryable network, leader-election, or
	// timeout failure.
	KindTransient Kind = iota
	// KindFatal identifies an authentication or protocol failure.
	KindFatal
	// KindNotFound identifies a missing destination.
	KindNotFound
	// KindTooLarge identifies a message exceeding a broker limit.
	KindTooLarge
	// KindPermission identifies an authorization failure.
	KindPermission
	// KindNotification identifies a routine lifecycle notification rather than a failure.
	KindNotification
)

// String returns the stable wire name of the error kind.
func (k Kind) String() string {
	switch k {
	case KindFatal:
		return "fatal"
	case KindNotFound:
		return "not_found"
	case KindTooLarge:
		return "too_large"
	case KindPermission:
		return "permission"
	case KindNotification:
		return "notification"
	default:
		return "transient"
	}
}

// ClassifiedError classifies errors crossing the driver port. Errors that do
// not implement it are treated as transient by Classify.
type ClassifiedError interface {
	error
	Kind() Kind
}

// Error is the standard ClassifiedError implementation. Drivers may return
// any ClassifiedError; Error does not log.
type Error struct {
	// Driver names the driver so the core can name it in diagnostics.
	Driver string
	// Op names the port operation, such as "publish" or "ensure_topology".
	Op string
	// K is the portable error classification.
	K Kind
	// Err is the broker client's error, reachable through errors.Is and
	// errors.As.
	Err error
}

// Error returns the formatted driver error.
func (e *Error) Error() string {
	return fmt.Sprintf("f1/%s: %s: %v (%s)", e.Driver, e.Op, e.Err, e.K)
}

// Unwrap returns the underlying driver error.
func (e *Error) Unwrap() error {
	return e.Err
}

// Kind returns the portable classification.
func (e *Error) Kind() Kind {
	return e.K
}

// Retryable reports whether the error should be retried.
func (e *Error) Retryable() bool {
	return e.K == KindTransient
}

// Classify returns err's kind and whether it implements ClassifiedError.
// Unclassified errors return KindTransient and false; Classify does not log.
func Classify(err error) (Kind, bool) {
	if classified, ok := errors.AsType[ClassifiedError](err); ok {
		return classified.Kind(), true
	}
	return KindTransient, false
}

// PublishError reports partial publish results. Failed contains the errors for
// messages that were not acknowledged.
type PublishError struct {
	Failed map[int]error
}

// Error returns the number of failed messages.
func (e *PublishError) Error() string {
	if e == nil || len(e.Failed) == 0 {
		return "f1/driver: publish error has no failed messages"
	}
	return fmt.Sprintf("f1/driver: %d of the published messages failed", len(e.Failed))
}

// Unwrap returns the per-message causes for errors.Is and errors.As.
func (e *PublishError) Unwrap() []error {
	if e == nil {
		return nil
	}
	causes := make([]error, 0, len(e.Failed))
	for _, err := range e.Failed {
		if err != nil {
			causes = append(causes, err)
		}
	}
	return causes
}

// Kind returns the most severe classification among failed messages.
func (e *PublishError) Kind() Kind {
	if e == nil || len(e.Failed) == 0 {
		return KindFatal
	}
	worst := KindTransient
	for _, err := range e.Failed {
		kind, classified := Classify(err)
		if !classified {
			kind = KindTransient
		}
		if errorSeverity(kind) > errorSeverity(worst) {
			worst = kind
		}
	}
	return worst
}

// Retryable reports whether every failed message is retryable.
func (e *PublishError) Retryable() bool {
	return e != nil && len(e.Failed) > 0 && e.Kind() == KindTransient
}

func errorSeverity(kind Kind) int {
	switch kind {
	case KindFatal:
		return 5
	case KindPermission:
		return 4
	case KindNotFound:
		return 3
	case KindTooLarge:
		return 2
	default:
		return 1
	}
}
