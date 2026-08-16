package f1_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
)

func TestTerminal_SurvivesWrapping(t *testing.T) {
	t.Parallel()

	base := errors.New("bad payload")
	err := fmt.Errorf("handler: %w", f1.Terminal(base))

	require.True(t, f1.IsTerminal(err))
	require.False(t, f1.IsDropped(err))
	require.ErrorIs(t, err, base)
}

func TestRetryDelay_ReadsThroughWrappedChain(t *testing.T) {
	t.Parallel()

	base := errors.New("upstream unavailable")
	delay := 45 * time.Second
	err := fmt.Errorf("handler: %w", f1.RetryAfter(base, delay))

	got, ok := f1.RetryDelay(err)
	require.True(t, ok)
	require.Equal(t, delay, got)
	require.ErrorIs(t, err, base)
}

func TestClassify_TableDriven(t *testing.T) {
	t.Parallel()

	base := errors.New("failure")
	tests := map[string]struct {
		err      error
		terminal bool
		dropped  bool
		delay    time.Duration
		hasDelay bool
	}{
		"nil":         {},
		"plain retry": {err: base},
		"terminal":    {err: f1.Terminal(base), terminal: true},
		"retry after": {
			err: f1.RetryAfter(base, 5*time.Second), delay: 5 * time.Second, hasDelay: true,
		},
		"drop":             {err: f1.Drop(base), dropped: true},
		"wrapped terminal": {err: fmt.Errorf("outer: %w", f1.Terminal(base)), terminal: true},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tt.terminal, f1.IsTerminal(tt.err))
			require.Equal(t, tt.dropped, f1.IsDropped(tt.err))
			got, ok := f1.RetryDelay(tt.err)
			require.Equal(t, tt.hasDelay, ok)
			require.Equal(t, tt.delay, got)
		})
	}
}

func TestClassify_TerminalSurvivesAnOuterClassification(t *testing.T) {
	t.Parallel()

	base := errors.New("permanent failure")
	err := f1.RetryAfter(f1.Terminal(base), 5*time.Second)

	require.True(t, f1.IsTerminal(err))
	require.False(t, f1.IsDropped(err))

	got, ok := f1.RetryDelay(err)
	require.True(t, ok)
	require.Equal(t, 5*time.Second, got)

	err = f1.Terminal(f1.RetryAfter(base, 5*time.Second))
	require.True(t, f1.IsTerminal(err))
	require.False(t, f1.IsDropped(err))
	_, ok = f1.RetryDelay(err)
	require.False(t, ok)

	err = f1.Terminal(f1.Drop(base))
	require.True(t, f1.IsTerminal(err))
	require.False(t, f1.IsDropped(err))
}

func TestClassify_NilErrorStaysNil(t *testing.T) {
	t.Parallel()

	require.Nil(t, f1.Terminal(nil))
	require.Nil(t, f1.RetryAfter(nil, time.Second))
	require.Nil(t, f1.Drop(nil))
}
