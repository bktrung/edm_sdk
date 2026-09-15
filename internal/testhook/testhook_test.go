package testhook

import (
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// TestHooksPanicWhenNoPackageRegistered pins the diagnostic a caller gets when
// the package that owns a constructor is not linked into the test binary: the
// panic has to name that package, because the alternative is a nil call whose
// stack says nothing about which init never ran.
//
// Nothing registers in this package's own test binary, so both hooks are unset
// here by construction.
func TestHooksPanicWhenNoPackageRegistered(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	for _, test := range []struct {
		name string
		want string
		call func()
	}{
		{name: "client option", want: "package f1 registered no client clock option", call: func() { ClientOption(fake) }},
		{name: "driver", want: "package inmem registered no driver constructor", call: func() { Driver(fake) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				recovered := recover()
				message, ok := recovered.(string)
				if !ok || !strings.Contains(message, test.want) {
					t.Fatalf("panic = %v, want a message containing %q", recovered, test.want)
				}
			}()
			test.call()
		})
	}
}
