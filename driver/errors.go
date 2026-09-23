package driver

import (
	"errors"
	"fmt"
)

var (
	// ErrUnsupported reports that a driver does not support the requested operation
	// or feature.
	ErrUnsupported = errors.New("f1/driver: unsupported operation")

	// ErrAlreadySettled reports that a delivery was acknowledged or rejected
	// after it had already been settled.
	ErrAlreadySettled = errors.New("f1/driver: message already settled")

	// ErrDrainTimeout reports that consumer draining or finalization exceeded its
	// context deadline.
	ErrDrainTimeout = errors.New("f1/driver: drain did not complete in time")

	// ErrResourcesOutstanding reports that a producer or consumer operation is
	// blocked by connection resource state, including open resources during close
	// or a closed or closing connection.
	ErrResourcesOutstanding = errors.New("f1/driver: producers or consumers still open")

	// ErrDestinationMissing reports that a requested physical destination does not
	// exist.
	ErrDestinationMissing = errors.New("f1/driver: destination does not exist")
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

// String returns the stable wire name of the error kind. Unknown values return
// "transient".
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
	// Kind returns the portable classification of the error.
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

// Error returns the formatted driver error. A nil *Error, which a driver can
// return by mistake as a typed nil, is safe to call through all methods and
// reads as a transient error.
func (e *Error) Error() string {
	if e == nil {
		return "f1/driver: nil error (transient)"
	}
	return fmt.Sprintf("f1/%s: %s: %v (%s)", e.Driver, e.Op, e.Err, e.K)
}

// Unwrap returns the underlying driver error.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Kind returns the portable classification.
func (e *Error) Kind() Kind {
	if e == nil {
		return KindTransient
	}
	return e.K
}

// Retryable reports whether the error should be retried.
func (e *Error) Retryable() bool {
	return e.Kind() == KindTransient
}

// Classify returns err's kind and whether its error chain contains a
// ClassifiedError. Unclassified errors, including nil, return KindTransient
// and false. It does not log.
func Classify(err error) (Kind, bool) {
	if classified, ok := errors.AsType[ClassifiedError](err); ok {
		return classified.Kind(), true
	}
	return KindTransient, false
}

// PublishError reports the messages a producer did not acknowledge.
type PublishError struct {
	// Failed maps each unacknowledged message index to its error.
	Failed map[int]error
}

// Error returns a summary of the failed-message count and, when present, the
// cause at the lowest index with a non-nil error. Nil causes are skipped, so
// an all-nil Failed map renders only the count. Choosing the lowest index makes
// the text stable across map iteration; the summary reports all failures even
// though the message names at most one cause.
func (e *PublishError) Error() string {
	if e == nil || len(e.Failed) == 0 {
		return "f1/driver: publish error has no failed messages"
	}
	summary := fmt.Sprintf("f1/driver: %d of the published messages failed", len(e.Failed))
	index, cause := e.firstCause()
	if cause == nil {
		return summary
	}
	return fmt.Sprintf("%s; first at index %d: %v", summary, index, cause)
}

// firstCause returns the lowest index in Failed holding a non-nil error
// together with that error, or a nil error when every entry is nil. The lowest
// index wins rather than the first iteration of the map, so the same failure
// renders the same text on every run.
func (e *PublishError) firstCause() (int, error) {
	lowest := -1
	var cause error
	for index, err := range e.Failed {
		if err == nil {
			continue
		}
		if lowest < 0 || index < lowest {
			lowest, cause = index, err
		}
	}
	return lowest, cause
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

// Kind returns the most severe classification among failed messages. This is
// a retry hint, not a report of connection health.
//
// Unclassified causes count as [KindTransient]. A nil or empty PublishError
// returns [KindFatal]. A notification reports a routine lifecycle event rather
// than a failure, so an error whose causes are all notifications reports
// [KindNotification] instead of the retrying default; a single cause that is a
// real failure decides the result on its own.
func (e *PublishError) Kind() Kind {
	if e == nil || len(e.Failed) == 0 {
		return KindFatal
	}
	worst := KindTransient
	onlyNotifications := true
	for _, err := range e.Failed {
		kind, classified := Classify(err)
		if !classified {
			kind = KindTransient
		}
		if kind == KindNotification {
			continue
		}
		onlyNotifications = false
		if errorSeverity(kind) > errorSeverity(worst) {
			worst = kind
		}
	}
	if onlyNotifications {
		return KindNotification
	}
	return worst
}

// Retryable reports whether Failed is non-empty and its aggregate
// classification is [KindTransient]. A nil receiver or an empty Failed map is
// not retryable.
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
