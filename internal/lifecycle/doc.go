// Package lifecycle owns the SDK's runner shutdown state machine: the states
// a runner moves through between Starting and one of Closed, Aborted or
// Failed, and the transitions between them. Shutdown budgets belong to the
// callers that spend them, not to this package.
package lifecycle
