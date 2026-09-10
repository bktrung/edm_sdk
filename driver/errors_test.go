package driver

import (
	"errors"
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
