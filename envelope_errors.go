package f1

import (
	"errors"
	"fmt"
)

// Errors returned for invalid envelope headers.
var (
	// ErrEnvelopeTooLarge indicates that encoded headers exceed the configured cap.
	ErrEnvelopeTooLarge = errors.New("f1: envelope headers exceed the size cap")

	// ErrReservedExtension indicates that an extension key is reserved.
	ErrReservedExtension = errors.New("f1: extension key is reserved")

	// ErrInvalidPriority indicates that a Priority is not a declared lane.
	ErrInvalidPriority = errors.New("f1: priority is not a declared lane")
)

// UnrecognisedValueError reports an unsupported value for a closed-set header.
type UnrecognisedValueError struct {
	// Attribute is the header name.
	Attribute string
	// Value is the received value.
	Value string
}

// Error returns a message identifying the unsupported header attribute and
// value.
func (e *UnrecognisedValueError) Error() string {
	return fmt.Sprintf("f1: unrecognised %s value %q", e.Attribute, e.Value)
}
