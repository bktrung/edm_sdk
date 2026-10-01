package rabbitmq

import (
	"strings"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestResolveBrokerPrefetch(t *testing.T) {
	for _, test := range []struct {
		name    string
		options map[string]string
		want    int
		ok      bool
	}{
		{name: "unset", want: 0, ok: true},
		{name: "positive", options: map[string]string{brokerPrefetchOption: "64"}, want: 64, ok: true},
		{name: "maximum", options: map[string]string{brokerPrefetchOption: "65535"}, want: 65535, ok: true},
		{name: "non-integer", options: map[string]string{brokerPrefetchOption: "many"}},
		{name: "zero", options: map[string]string{brokerPrefetchOption: "0"}},
		{name: "negative", options: map[string]string{brokerPrefetchOption: "-1"}},
		{name: "above maximum", options: map[string]string{brokerPrefetchOption: "65536"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveBrokerPrefetch(test.options)
			if test.ok {
				if err != nil || got != test.want {
					t.Fatalf("resolveBrokerPrefetch() = %d, %v, want %d, nil", got, err, test.want)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), brokerPrefetchOption) {
				t.Fatalf("resolveBrokerPrefetch() error = %v, want an error naming %s", err, brokerPrefetchOption)
			}
		})
	}
}

func TestBrokerPrefetchValidation(t *testing.T) {
	cfg := driver.ConsumerConfig{
		Destinations: []string{"orders", "orders.retry"},
		Prefetch:     10,
	}
	if err := validateBrokerPrefetch(cfg, 4); err == nil || !strings.Contains(err.Error(), "core window 5") {
		t.Fatalf("validateBrokerPrefetch(below window) error = %v, want core window 5", err)
	}
	if err := validateBrokerPrefetch(cfg, 5); err != nil {
		t.Fatalf("validateBrokerPrefetch(equal window) error = %v", err)
	}
	if err := validateBrokerPrefetch(cfg, 6); err != nil {
		t.Fatalf("validateBrokerPrefetch(above window) error = %v", err)
	}
}

func TestBrokerPrefetchOverridesTransportWindows(t *testing.T) {
	cfg := driver.ConsumerConfig{
		Destinations: []string{"orders", "orders.retry"},
		Prefetch:     10,
		PerDestination: map[string]int{
			"orders":       7,
			"orders.retry": 3,
		},
	}
	if got := effectivePrefetch(cfg, "orders", 0, 0); got != 7 {
		t.Fatalf("effectivePrefetch(unset) = %d, want 7", got)
	}
	if got := effectivePrefetch(cfg, "orders", 0, 64); got != 64 {
		t.Fatalf("effectivePrefetch(set) = %d, want 64", got)
	}
}
