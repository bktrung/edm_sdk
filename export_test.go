package f1

import (
	"runtime"
	"strings"
)

const modulePath = "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"

// ModuleGoroutineStacks returns the stacks of live goroutines running this
// module's non-test code, or "<none>". The goroutine leak checks in both test
// packages print it when a goroutine outlives the client.
func ModuleGoroutineStacks() string {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	var stacks []string
	for stack := range strings.SplitSeq(string(buf[:n]), "\n\n") {
		lines := strings.Split(stack, "\n")
		for i := 1; i+1 < len(lines); i++ {
			if strings.HasPrefix(lines[i], "created by ") {
				break
			}
			source := strings.TrimSpace(lines[i+1])
			if strings.Contains(lines[i], modulePath) && strings.Contains(source, ".go:") && !strings.Contains(source, "_test.go:") {
				stacks = append(stacks, stack)
				break
			}
		}
	}
	if len(stacks) == 0 {
		return "<none>"
	}
	return strings.Join(stacks, "\n\n")
}
