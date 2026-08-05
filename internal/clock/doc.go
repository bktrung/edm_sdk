// Package clock is the injectable time source every timing-dependent
// component uses, so scheduling, backoff and drain logic are testable in
// milliseconds instead of wall-clock time.
// See docs/11-testing-and-acceptance.md.
package clock
