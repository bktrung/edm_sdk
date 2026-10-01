package driver

import "testing"

func TestBrokerInfo_DisplayComposesKindAndVersion(t *testing.T) {
	t.Parallel()

	if got := (BrokerInfo{
		Kind: "rabbitmq", Version: "4.3.0",
	}).Display(); got != "rabbitmq 4.3.0" {
		t.Fatalf("Display() = %q", got)
	}
	if got := (BrokerInfo{Kind: "rabbitmq"}).Display(); got != "rabbitmq" {
		t.Fatalf("Display() = %q", got)
	}
	if got := (BrokerInfo{Version: "4.3.0"}).Display(); got != "4.3.0" {
		t.Fatalf("Display() = %q", got)
	}
}

func TestConsumerConfigDestinationPrefetchSplitsTheBudget(t *testing.T) {
	cfg := ConsumerConfig{
		Destinations:   []string{"a", "b", "c", "d"},
		Prefetch:       10,
		PerDestination: map[string]int{"d": 7},
	}
	for index, want := range []int{3, 3, 2, 7} {
		if got := cfg.DestinationPrefetch(index); got != want {
			t.Fatalf("DestinationPrefetch(%d) = %d, want %d", index, got, want)
		}
	}
	if got := (ConsumerConfig{Destinations: []string{"a", "b"}, Prefetch: 1}).DestinationPrefetch(1); got != 1 {
		t.Fatalf("DestinationPrefetch below one share = %d, want 1", got)
	}
}
