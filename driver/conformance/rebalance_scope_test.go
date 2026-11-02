package conformance

import "testing"

func TestRebalanceCheckScopeTakesTheLastElement(t *testing.T) {
	// A top-level test name has no '/', so the whole name is the scope. Each
	// capital letter maps to '_' because group identifiers take lower case only.
	if got, want := rebalanceCheckScope(t), "_est_ebalance_heck_cope_akes_he_ast_lement"; got != want {
		t.Fatalf("rebalanceCheckScope(top-level) = %q, want %q", got, want)
	}
	for _, test := range []struct {
		name string
		want string
	}{
		{name: "flat-check", want: "flat_check"},
		{name: "one/level", want: "level"},
		{name: "two/levels/deep-check", want: "deep_check"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := rebalanceCheckScope(t); got != test.want {
				t.Fatalf("rebalanceCheckScope(%q) = %q, want %q", t.Name(), got, test.want)
			}
		})
	}
}
