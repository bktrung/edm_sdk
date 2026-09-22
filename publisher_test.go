package f1

import "testing"

func TestPublishEntryPoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		source   string
		priority Priority
		want     string
	}{
		{name: "slash nested medium", source: "/prod/svc", priority: PriorityMedium, want: "f1.prod.orders.created.medium"},
		{name: "slash nested high", source: "/prod/svc", priority: PriorityHigh, want: "f1.prod.orders.created.high"},
		{name: "slash nested low", source: "/prod/svc", priority: PriorityLow, want: "f1.prod.orders.created.low"},
		{name: "slash nested invalid", source: "/prod/svc", priority: Priority(99), want: "f1.prod.orders.created.invalid"},
		{name: "nested medium", source: "prod/svc", priority: PriorityMedium, want: "f1.prod.orders.created.medium"},
		{name: "nested high", source: "prod/svc", priority: PriorityHigh, want: "f1.prod.orders.created.high"},
		{name: "nested low", source: "prod/svc", priority: PriorityLow, want: "f1.prod.orders.created.low"},
		{name: "nested invalid", source: "prod/svc", priority: Priority(99), want: "f1.prod.orders.created.invalid"},
		{name: "slash environment medium", source: "/prod", priority: PriorityMedium, want: "f1.prod.orders.created.medium"},
		{name: "slash environment high", source: "/prod", priority: PriorityHigh, want: "f1.prod.orders.created.high"},
		{name: "slash environment low", source: "/prod", priority: PriorityLow, want: "f1.prod.orders.created.low"},
		{name: "slash environment invalid", source: "/prod", priority: Priority(99), want: "f1.prod.orders.created.invalid"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := publishEntryPoint(test.source, "orders.created", test.priority); got != test.want {
				t.Errorf("publishEntryPoint(%q, %q, %v) = %q, want %q", test.source, "orders.created", test.priority, got, test.want)
			}
		})
	}
}

func TestPublishEntryPointAllocations(t *testing.T) {
	allocs := testing.AllocsPerRun(100, func() {
		_ = publishEntryPoint("/prod/svc", "orders.created", PriorityHigh)
	})
	if allocs > 1 {
		t.Fatalf("publishEntryPoint allocations = %v, want at most 1", allocs)
	}
}
