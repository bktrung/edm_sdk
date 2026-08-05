package f1

import (
	"errors"
	"fmt"
)

// Publish-side rejections (doc 05 §4). All three are programming errors in
// the calling service, not transient conditions: they are returned from
// EncodeHeaders and are never retried, because the same call fails
// identically every time.
var (
	// ErrEnvelopeTooLarge is returned when the encoded headers exceed
	// min(the core's 8 KiB cap, the driver's Capabilities.MaxHeaderBytes).
	// Doc 03 §3.3 is normative. It applies to first publish only: a retry or
	// DLQ copy over the cap sheds diagnostics instead (rule 2), since the DLQ
	// is the one destination a copy that refuses to shrink must still reach.
	// Not yet wired into EncodeHeaders: distinguishing first publish from a
	// retry/DLQ copy needs the caller context M1-10's Publisher has and this
	// package does not (F-P45, hole 2 - declared here so the name exists).
	ErrEnvelopeTooLarge = errors.New("f1: envelope headers exceed the size cap")

	// ErrReservedExtension is returned when an Extensions key collides with a
	// CloudEvents core/optional attribute, an f1 extension, "traceparent" or
	// "tracestate" (doc 03 §3.3 rule 4).
	ErrReservedExtension = errors.New("f1: extension key is reserved")

	// ErrInvalidPriority is returned when a Priority is none of the three
	// declared lanes (doc 05 §1's Valid). The decode-side counterpart is
	// ParsePriority's error; this is the stamping-side one.
	ErrInvalidPriority = errors.New("f1: priority is not a declared lane")
)

// UnrecognisedValueError is returned when a header carries a value doc 03
// §2.3 does not define for an attribute whose value set is closed (currently
// only f1priority - see ParsePriority). It is a plain error: M1-07 has not
// landed yet, so there is no Terminal() to classify it with. The caller at
// the decode site is responsible for that classification once it exists.
type UnrecognisedValueError struct {
	Attribute string
	Value     string
}

func (e *UnrecognisedValueError) Error() string {
	return fmt.Sprintf("f1: unrecognised %s value %q", e.Attribute, e.Value)
}
