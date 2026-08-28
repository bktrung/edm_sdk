package f1

import (
	"errors"
	"maps"
	"slices"
	"testing"
)

type customDeathDetailError struct {
	details map[string]string
}

func (e customDeathDetailError) Error() string { return "custom failure" }

func (e customDeathDetailError) DeathDetails() map[string]string { return e.details }

func TestCollectDeathDetails_MergesErrorTrees(t *testing.T) {
	base := errors.New("failure")
	cases := []struct {
		name          string
		err           error
		want          map[string]string
		wantDiscarded []string
	}{
		{
			name: "nested details",
			err:  WithDetails(WithDetails(base, map[string]string{"a": "1"}), map[string]string{"b": "2"}),
			want: map[string]string{"a": "1", "b": "2"},
		},
		{
			name: "outer detail wins collision",
			err:  WithDetails(WithDetails(base, map[string]string{"a": "1"}), map[string]string{"a": "2"}),
			want: map[string]string{"a": "2"},
		},
		{
			name: "terminal inside details",
			err:  Terminal(WithDetails(base, map[string]string{"a": "1"})),
			want: map[string]string{"a": "1"},
		},
		{
			name: "details around terminal",
			err:  WithDetails(Terminal(base), map[string]string{"a": "1"}),
			want: map[string]string{"a": "1"},
		},
		{
			name:          "custom carrier",
			err:           customDeathDetailError{details: map[string]string{"tenant": "acme", "BAD_KEY": "discard"}},
			want:          map[string]string{"tenant": "acme"},
			wantDiscarded: []string{"BAD_KEY"},
		},
		{
			name:          "details discard",
			err:           WithDetails(base, map[string]string{"tenant": "acme", "BAD_KEY": "discard"}),
			want:          map[string]string{"tenant": "acme"},
			wantDiscarded: []string{"BAD_KEY"},
		},
		{
			name: "joined details",
			err: errors.Join(
				WithDetails(errors.New("left"), map[string]string{"tenant": "acme"}),
				WithDetails(errors.New("right"), map[string]string{"step": "charge"}),
			),
			want: map[string]string{"tenant": "acme", "step": "charge"},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			got, discarded := collectDeathDetails(test.err)
			if !maps.Equal(got, test.want) {
				t.Fatalf("collectDeathDetails() = %#v, want %#v", got, test.want)
			}
			slices.Sort(discarded)
			if !slices.Equal(discarded, test.wantDiscarded) {
				t.Fatalf("collectDeathDetails() discarded = %#v, want %#v", discarded, test.wantDiscarded)
			}
		})
	}
}

func TestWithDetailsRecordsDiscardedKeys(t *testing.T) {
	base := errors.New("failure")
	err := WithDetails(base, map[string]string{"tenant": "acme", "BAD_KEY": "discard"})
	detailsErr, ok := errors.AsType[*detailsError](err)
	if !ok {
		t.Fatalf("WithDetails() returned %T, want *detailsError", err)
	}
	if !maps.Equal(detailsErr.details, map[string]string{"tenant": "acme"}) {
		t.Fatalf("detailsError.details = %#v, want attached details", detailsErr.details)
	}
	slices.Sort(detailsErr.discarded)
	if !slices.Equal(detailsErr.discarded, []string{"BAD_KEY"}) {
		t.Fatalf("detailsError.discarded = %#v, want [BAD_KEY]", detailsErr.discarded)
	}
}

func TestWithDetailsOverBoundRecordsEveryKeyAsDiscarded(t *testing.T) {
	base := errors.New("failure")
	input := map[string]string{"tenant": "acme", "BAD_KEY": "discard"}
	for i := range maxDeathDetailKeys {
		input[string(rune('a'+i))] = "value"
	}
	err := WithDetails(base, input)
	detailsErr, ok := errors.AsType[*detailsError](err)
	if !ok {
		t.Fatalf("WithDetails() returned %T, want *detailsError", err)
	}
	if detailsErr.details != nil {
		t.Fatalf("detailsError.details = %#v, want nil for an over-bound set", detailsErr.details)
	}
	slices.Sort(detailsErr.discarded)
	want := make([]string, 0, len(input))
	for key := range input {
		want = append(want, key)
	}
	slices.Sort(want)
	if !slices.Equal(detailsErr.discarded, want) {
		t.Fatalf("detailsError.discarded = %#v, want %#v", detailsErr.discarded, want)
	}
}

func TestCollectDeathDetails_NilErrorStaysNil(t *testing.T) {
	details, discarded := collectDeathDetails(nil)
	if details != nil || discarded != nil {
		t.Fatalf("collectDeathDetails(nil) = %#v, %#v; want nil, nil", details, discarded)
	}
}
