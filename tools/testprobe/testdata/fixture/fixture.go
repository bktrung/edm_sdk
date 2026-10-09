// Package fixture contains the tiny production surface used by testprobe tests.
package fixture

import "strconv"

// Doubling returns n multiplied by two.
func Doubling(n int) int {
	return n * 2
}

// Formatting returns n as a decimal string.
func Formatting(n int) string {
	return strconv.Itoa(n)
}

// Counting returns the number of bytes in s.
func Counting(s string) int {
	return len(s)
}
