package fixture

import "testing"

func TestDoublingReturnsTwiceTheInput(t *testing.T) {
	if got := Doubling(4); got != 8 {
		t.Fatalf("Doubling(4) = %d, want 8", got)
	}
}

func TestFormattingDoesNotPanic(t *testing.T) {
	_ = Formatting(7)
}

func TestUnrelatedCheck(t *testing.T) {
	if got := Counting("abcd"); got < 0 {
		t.Fatalf("Counting returned a negative length: %d", got)
	}
}
