package driver

import (
	"errors"
	"fmt"
	"testing"
)

func TestClassify_ReportsWhetherTheErrorCarriedAKind(t *testing.T) {
	t.Parallel()

	if got, classified := Classify(errors.New("network")); got != KindTransient || classified {
		t.Fatalf("Classify(unclassified) = (%v, %t), want (%v, false)", got, classified, KindTransient)
	}

	err := &Error{K: KindTooLarge, Err: errors.New("too large")}
	if got, classified := Classify(err); got != KindTooLarge || !classified {
		t.Fatalf("Classify(classified) = (%v, %t), want (%v, true)", got, classified, KindTooLarge)
	}
}

func TestPublishError_FatalOutranksEveryOtherKind(t *testing.T) {
	t.Parallel()

	err := &PublishError{Failed: map[int]error{
		0: &Error{K: KindNotFound, Err: errors.New("missing")},
		1: &Error{K: KindFatal, Err: errors.New("connection closed")},
		2: &Error{K: KindPermission, Err: errors.New("denied")},
	}}
	if got := err.Kind(); got != KindFatal {
		t.Fatalf("PublishError.Kind() = %v, want %v", got, KindFatal)
	}
	if err.Retryable() {
		t.Fatal("fatal partial publish was marked retryable")
	}
}

func TestPublishError_NotificationOnlyIsNotRetryable(t *testing.T) {
	t.Parallel()

	notification := func() error {
		return &Error{K: KindNotification, Err: errors.New("consumer cancelled")}
	}
	err := &PublishError{Failed: map[int]error{0: notification(), 1: notification()}}
	if got := err.Kind(); got != KindNotification {
		t.Fatalf("notification-only PublishError.Kind() = %v, want %v", got, KindNotification)
	}
	if err.Retryable() {
		t.Fatal("notification-only PublishError was marked retryable")
	}

	mixed := &PublishError{Failed: map[int]error{
		0: notification(),
		1: &Error{K: KindTransient, Err: errors.New("connection reset")},
	}}
	if got := mixed.Kind(); got != KindTransient {
		t.Fatalf("PublishError.Kind() with one transient cause = %v, want %v", got, KindTransient)
	}
	if !mixed.Retryable() {
		t.Fatal("PublishError with one transient cause was not retryable")
	}

	nilCause := &PublishError{Failed: map[int]error{0: nil}}
	if got := nilCause.Kind(); got != KindTransient {
		t.Fatalf("PublishError.Kind() with a nil cause = %v, want %v", got, KindTransient)
	}
	if !nilCause.Retryable() {
		t.Fatal("PublishError with a nil cause was not retryable")
	}
}

func TestPublishError_EmptyFailedIsRejected(t *testing.T) {
	t.Parallel()

	err := &PublishError{}
	if got := err.Kind(); got != KindFatal {
		t.Fatalf("empty PublishError.Kind() = %v, want %v", got, KindFatal)
	}
	if err.Retryable() {
		t.Fatal("empty PublishError was marked retryable")
	}
	if got := err.Error(); got != "f1/driver: publish error has no failed messages" {
		t.Fatalf("empty PublishError.Error() = %q", got)
	}
}

func TestPublishError_ErrorNamesTheCauseAtTheLowestIndex(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		failed map[int]error
		want   string
	}{
		"one failure": {
			failed: map[int]error{0: errors.New("connection reset")},
			want:   "f1/driver: 1 of the published messages failed; first at index 0: connection reset",
		},
		"first cause at a non-zero index": {
			failed: map[int]error{
				2: errors.New("missing destination"),
				3: errors.New("too large"),
				4: errors.New("connection reset"),
				5: errors.New("denied"),
			},
			want: "f1/driver: 4 of the published messages failed; first at index 2: missing destination",
		},
		"nil entry below the first cause": {
			failed: map[int]error{0: nil, 1: errors.New("connection reset"), 4: errors.New("too large")},
			want:   "f1/driver: 3 of the published messages failed; first at index 1: connection reset",
		},
		"every entry nil": {
			failed: map[int]error{0: nil, 3: nil},
			want:   "f1/driver: 2 of the published messages failed",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := &PublishError{Failed: test.failed}
			if got := err.Error(); got != test.want {
				t.Fatalf("PublishError.Error() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestPublishError_ErrorNamesTheCauseThroughARetryWrap(t *testing.T) {
	t.Parallel()

	// A successor publication that fails leaves the retry path wrapping the
	// partial publish in a driver Error, so this is the text an operator reads
	// for a refused retry: the wrapper, then the partial failure with its
	// cause, then the wrapper's kind.
	pe := &PublishError{Failed: map[int]error{
		0: errors.New(`rabbitmq: parking destination "orders" is missing`),
	}}
	err := &Error{Driver: "rabbitmq", Op: "retry", K: KindNotFound, Err: pe}

	const before = "f1/rabbitmq: retry: f1/driver: 1 of the published messages failed (not_found)"
	want := `f1/rabbitmq: retry: f1/driver: 1 of the published messages failed; ` +
		`first at index 0: rabbitmq: parking destination "orders" is missing (not_found)`
	if got := err.Error(); got != want {
		t.Fatalf("retry-wrapped PublishError.Error() = %q, want %q; before it was %q", got, want, before)
	}
}

func TestPublishError_UnwrapsPerMessageCauses(t *testing.T) {
	t.Parallel()

	first := errors.New("first")
	second := errors.New("second")
	err := &PublishError{Failed: map[int]error{0: first, 1: second}}
	if !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatal("PublishError did not unwrap every per-message cause")
	}
}

func TestKind_String(t *testing.T) {
	t.Parallel()

	tests := map[Kind]string{
		KindTransient:    "transient",
		KindFatal:        "fatal",
		KindNotFound:     "not_found",
		KindTooLarge:     "too_large",
		KindPermission:   "permission",
		KindNotification: "notification",
	}
	for input, want := range tests {
		if got := input.String(); got != want {
			t.Errorf("Kind(%d).String() = %q, want %q", input, got, want)
		}
	}
}

func TestError_FormatsUnwrapsAndClassifies(t *testing.T) {
	t.Parallel()

	cause := errors.New("broker closed")
	err := &Error{
		Driver: "rabbitmq",
		Op:     "publish",
		K:      KindTransient,
		Err:    cause,
	}
	if got, want := err.Error(), "f1/rabbitmq: publish: broker closed (transient)"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(err, cause) {
		t.Fatal("Error did not unwrap the broker cause")
	}
	if err.Kind() != KindTransient || !err.Retryable() {
		t.Fatal("transient Error was not classified as retryable")
	}

	for _, kind := range []Kind{KindFatal, KindNotFound, KindTooLarge, KindPermission} {
		err.K = kind
		if err.Kind() != kind || err.Retryable() {
			t.Errorf("Error with kind %v has Kind()=%v Retryable()=%t", kind, err.Kind(), err.Retryable())
		}
	}
}

// TestClassifyToleratesATypedNilError pins that a typed-nil *Error, which a
// driver can return by mistake, classifies as transient instead of panicking
// in the core goroutine that reads it.
func TestClassifyToleratesATypedNilError(t *testing.T) {
	var typed *Error
	err := fmt.Errorf("wrap: %w", typed)
	kind, classified := Classify(err)
	if !classified || kind != KindTransient {
		t.Fatalf("Classify(typed nil) = %v, %t, want transient, true", kind, classified)
	}
	if errors.Is(err, ErrDestinationMissing) {
		t.Fatal("a typed nil matched an unrelated sentinel")
	}
	if !typed.Retryable() || typed.Error() == "" {
		t.Fatal("a typed nil must read as a retryable error with text")
	}
}
